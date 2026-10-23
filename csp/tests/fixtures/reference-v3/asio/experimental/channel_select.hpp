// Copyright (c) 2026 Knuth contributors
// Distributed under the Boost Software License, Version 1.0.
#ifndef ASIO_EXPERIMENTAL_CHANNEL_SELECT_HPP
#define ASIO_EXPERIMENTAL_CHANNEL_SELECT_HPP

#include <asio/any_io_executor.hpp>
#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/deferred.hpp>
#include <asio/this_coro.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/system_error.hpp>
#include <array>
#include <unordered_map>
#include <span>
#include <asio/async_result.hpp>
#include <asio/associated_executor.hpp>
#include <asio/associated_cancellation_slot.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/error.hpp>
#include <asio/post.hpp>
#include <asio/defer.hpp>
#include <asio/dispatch.hpp>
#include <asio/steady_timer.hpp>
#include <asio/recycling_allocator.hpp>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <stop_token>
#include <tuple>
#include <type_traits>
#include <variant>
#include <vector>

namespace asio {
ASIO_INLINE_NAMESPACE_BEGIN
namespace experimental {

struct channel_select_options {
  // Default selection shuffles all cases, as opposed to fixed branch priority.
  bool biased = false;
  bool nonblocking = false;
  std::optional<std::uint32_t> random_seed; // Reproducible arbitration when supplied.
  asio::steady_timer::time_point deadline = asio::steady_timer::time_point::max();
};
namespace channel_select_detail {
// A small per-worker generator, following the inexpensive Xorshift approach
// used by Crossbeam. std::shuffle retains unbiased range reduction; the
// explicit-seed path below retains its original Mersenne Twister sequence.
class selection_random {
  std::uint64_t state_;
public:
  using result_type = std::uint64_t;
  selection_random() : state_([] {
    std::random_device source;
    auto seed = (std::uint64_t(source()) << 32) ^ source();
    return seed ? seed : UINT64_C(0x9e3779b97f4a7c15);
  }()) {}
  static constexpr result_type min() { return 0; }
  static constexpr result_type max() { return UINT64_MAX; }
  result_type operator()() {
    auto x = state_;
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    state_ = x;
    return x * UINT64_C(2685821657736338717);
  }
};
// Default arbitration shuffles with the per-worker generator; std::shuffle
// keeps its unbiased range reduction. An explicit seed keeps std::shuffle over
// std::mt19937, so the documented reproducible permutation sequence is unchanged.
inline void shuffle_order(std::span<std::size_t> order, const channel_select_options& options) {
  if (options.random_seed) {
    std::mt19937 seeded(*options.random_seed);
    std::shuffle(order.begin(), order.end(), seeded);
    return;
  }
  thread_local selection_random engine;
  std::shuffle(order.begin(), order.end(), engine);
}
struct operation;
// Execution-context ownership for suspended channel operations. An infinite
// timer is not a lifetime token: constructing one initializes the I/O reactor.
// This service has no worker or scheduler; shutdown only releases its owners.
// A kept operation owns itself through a strong self reference and is linked
// into an intrusive list, so keeping and forgetting allocate nothing.
class pending_operations : public asio::execution_context::service {
  std::mutex mutex_;
  operation* first_ = nullptr;
  inline void shutdown() override;
public:
  inline static asio::execution_context::id id;
  explicit pending_operations(asio::execution_context& context) : service(context) {}
  inline void keep(std::shared_ptr<operation> op);
  inline void forget(operation* op);
};
// Keep references acquired under channel locks alive until AFTER those locks.
struct retired_operations {
  std::array<std::shared_ptr<operation>, 4> local;
  std::size_t used = 0;
  std::vector<std::shared_ptr<operation>> owners;
  std::shared_ptr<operation> acquire(const std::weak_ptr<operation>& weak) {
    if (used < local.size()) { auto& slot = local[used++]; slot = weak.lock(); return slot; }
    // Allocate an empty slot before acquiring ownership. Allocation failure must
    // not release a last temporary owner while a channel lock is still held.
    owners.emplace_back();
    owners.back() = weak.lock();
    return owners.back();
  }
};
struct channel_base {
  std::mutex mutex;
  virtual ~channel_base() = default;
};
// Intrusive wait registrations. A node lives in its owning operation and is
// linked in at most one channel list, under that channel's mutex. The owning
// operation unlinks it under that mutex before it can be destroyed, so a
// linked node always belongs to a live operation; no registration allocates.
struct wait_list;
struct wait_link {
  wait_link* prev = nullptr;
  wait_link* next = nullptr;
  wait_list* list = nullptr;
  const operation* identity = nullptr;
};
struct wait_list {
  wait_link* head = nullptr;
  wait_link* tail = nullptr;
  std::size_t count = 0;
  void push_back(wait_link& node) {
    node.prev = tail; node.next = nullptr; node.list = this;
    (tail ? tail->next : head) = &node;
    tail = &node; ++count;
  }
  void unlink(wait_link& node) {
    (node.prev ? node.prev->next : head) = node.next;
    (node.next ? node.next->prev : tail) = node.prev;
    node.prev = node.next = nullptr; node.list = nullptr; --count;
  }
};
template<class T> struct wait_node : wait_link {
  std::weak_ptr<operation> op;
  std::size_t index = 0;
  std::optional<T>* value = nullptr;
  // Withdrawal by the owning operation; the caller holds the list's mutex.
  // Relinking touches the neighbouring registrations of other operations.
  void unlink(const operation* removing) {
    if (!list) return;
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE)
    if (prev) ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE(prev->identity, removing, false);
    if (next) ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE(next->identity, removing, false);
#else
    (void)removing;
#endif
    list->unlink(*this);
  }
};
struct no_node { void unlink(const operation*) {} };
struct operation : std::enable_shared_from_this<operation> {
  static constexpr std::ptrdiff_t active = -1, waiting = -2, aborted = -3;
  std::atomic<std::ptrdiff_t> winner{active};
  std::atomic<bool> cancellation_requested{false}; // Cancellation ingress only.
  // Serial turn runner, replacing a private strand. The low bit records that a
  // runner is scheduled or running; the other bits are requested turns. At
  // most one runner exists per operation, and before it exits it drains every
  // request made while it ran, so the first start, cancellation, a timer
  // resume and finalization never run concurrently and never nest. A queued
  // runner is a continuation (defer) on the owner executor; a runner holds a
  // strong reference to the operation while it runs.
  static constexpr unsigned scheduled = 1, turn_start = 2, turn_cancel = 4, turn_resume = 8, turn_finalize = 16;
  std::atomic<unsigned> turns{0};
  asio::error_code error;
  asio::any_io_executor owner;
  std::stop_token stop;
  std::optional<asio::steady_timer> timer;
  pending_operations* registration_owner = nullptr;
  operation* registry_prev = nullptr; // Lifetime registry links; registry mutex only.
  operation* registry_next = nullptr;
  std::shared_ptr<operation> self_owner; // Set while kept by the registry.
  channel_select_options options;
  // Common selections need no separate heap allocations for their case order,
  // channel set, or locks. Larger selections retain the same unbounded arity.
  std::array<channel_base*, 4> local_channels;
  std::array<std::size_t, 4> local_order;
  std::vector<channel_base*> extra_channels;
  std::vector<std::size_t> extra_order;
  std::span<channel_base*> channels;
  std::span<std::size_t> order;
  struct cancel_request {
    operation* op;
    void operator()() const { op->request_cancel(); }
  };
  // Declared after the atomics it uses: its destructor, which waits for a
  // callback in progress, runs before they are destroyed.
  std::optional<std::stop_callback<cancel_request>> stop_callback;
  bool finished = false; // Owner only.
  bool initial_turn = true; // Owner only; immediate completions must be posted.
  bool dispatch_completion = false; // Coroutine continuation may dispatch after suspension.

  operation(asio::any_io_executor ex, channel_select_options opt, std::size_t count)
    : owner(std::move(ex)), options(opt) {
    if (count <= local_order.size()) {
      channels = std::span(local_channels).first(count);
      order = std::span(local_order).first(count);
    } else {
      extra_channels.resize(count); extra_order.resize(count);
      channels = extra_channels; order = extra_order;
    }
    std::iota(order.begin(), order.end(), 0);
  }
  virtual ~operation() = default;
  virtual bool attempt(std::size_t, retired_operations&) = 0;
  virtual bool register_case(std::size_t) = 0;
  virtual void unlink_cases() = 0; // Caller holds every participating channel mutex.
  virtual asio::steady_timer::time_point next_timer() const = 0;
  virtual void complete() = 0;

  struct channel_locks {
    std::array<std::unique_lock<std::mutex>, 4> local;
    std::vector<std::unique_lock<std::mutex>> extra;
    explicit channel_locks(std::span<channel_base*> channels) {
      if (channels.size() <= local.size()) {
        for (std::size_t i = 0; i < channels.size(); ++i)
          local[i] = std::unique_lock<std::mutex>(channels[i]->mutex);
      } else {
        extra.reserve(channels.size());
        for (auto* c : channels) extra.emplace_back(c->mutex);
      }
    }
  };
  auto lock_channels() { return channel_locks(channels); }
  void remove() { unlink_cases(); }
  bool claim(std::size_t index) {
    auto expected = waiting;
    return winner.compare_exchange_strong(expected, static_cast<std::ptrdiff_t>(index),
        std::memory_order_acq_rel);
  }
  void commit(std::size_t index, asio::error_code ec = {}) {
    error = ec;
    winner.store(static_cast<std::ptrdiff_t>(index), std::memory_order_release);
  }
  // Request a turn; the caller holds a strong reference. A queued runner is a
  // continuation of the current handler: on a thread that is running the
  // owner's scheduler, Asio queues it on that thread's private queue, with no
  // scheduler lock, no wakeup signal and no migration to another worker. From
  // any other thread it is an ordinary post.
  void request(unsigned turn) {
    if (!(turns.fetch_or(turn | scheduled, std::memory_order_acq_rel) & scheduled))
      asio::defer(owner, [self = shared_from_this()] { self->run_turns(); });
  }
  // Cancellation ingress from a stop callback or a cancellation slot, on any
  // thread, possibly while the operation is being destroyed: the flag is an
  // atomic member and the request needs an owner that still exists.
  void request_cancel() {
    cancellation_requested.store(true, std::memory_order_release);
    if (auto self = weak_from_this().lock()) self->request(turn_cancel);
  }
  void run_turns() {
    for (;;) {
      auto requested = turns.exchange(scheduled, std::memory_order_acq_rel);
      if (requested & turn_start) initial_start();
      if (requested & turn_cancel) cancel(asio::error::operation_aborted);
      if (requested & turn_resume) start(true);
      if (requested & turn_finalize) finalize();
      auto expected = scheduled;
      if (turns.compare_exchange_strong(expected, 0, std::memory_order_acq_rel)) return;
    }
  }
  void initial_start() {
    if (finished) return;
    // Installation and removal share the operation's runner. An already
    // stopped token may invoke request_cancel inline during emplacement.
    if (stop.stop_possible()) stop_callback.emplace(stop, cancel_request{this});
    if (stop.stop_requested() || cancellation_requested.load(std::memory_order_acquire))
      cancel(asio::error::operation_aborted);
    else start();
    initial_turn = false;
  }
  // Wake a committed operation. The completion handler is still dispatched or
  // queued by complete(); nothing of the user's runs under a channel lock.
  void notify() { request(turn_finalize); }
  void finalize() {
    if (finished) return;
    { auto locks = lock_channels(); remove(); }
    finished = true;
    if (timer) timer->cancel();
    stop_callback.reset();
    if (registration_owner) registration_owner->forget(this);
    complete(); // No channel mutex is held while posting user completion.
  }
  void cancel(asio::error_code ec) {
    if (finished) return;
    auto expected = waiting;
    if (winner.compare_exchange_strong(expected, aborted, std::memory_order_acq_rel)
        || (expected == active && (winner.store(aborted), true))) {
      error = ec;
      finalize();
    }
    // A committed peer owns payload transfer and schedules finalize afterwards.
  }
  void start(bool resumed = false) {
    if (finished) return;
    // Thread-local generator: no global selection lock or RNG lock.
    if (!options.biased) shuffle_order(order, options);
    bool ready = false;
    retired_operations retired;
    {
      auto locks = lock_channels();
      if (resumed) {
        auto expected = waiting;
        if (!winner.compare_exchange_strong(expected, active, std::memory_order_acq_rel)) return;
        remove();
      } else if (winner.load(std::memory_order_acquire) != active) return;
      if (options.deadline != asio::steady_timer::time_point::max()
          && options.deadline <= asio::steady_timer::clock_type::now()) {
        error = asio::error::timed_out; winner.store(aborted); ready = true;
      } else {
        for (auto i : order) if (attempt(i, retired)) { ready = true; break; }
        if (!ready) {
          if (options.nonblocking) {
            error = asio::error::would_block; winner.store(aborted); ready = true;
          } else {
            for (auto i : order) if (!register_case(i)) {
              remove(); error = asio::error::no_buffer_space;
              winner.store(aborted); ready = true; break;
            }
            if (!ready) winner.store(waiting, std::memory_order_release);
          }
        }
      }
    }
    if (ready) { finalize(); return; }
    if (!registration_owner) {
      registration_owner = &asio::use_service<pending_operations>(asio::query(owner, asio::execution::context));
      registration_owner->keep(shared_from_this());
    }
    auto expiry = std::min(options.deadline, next_timer());
    if (expiry != asio::steady_timer::time_point::max()) {
      if (!timer) timer.emplace(owner);
      timer->expires_at(expiry);
      timer->async_wait([self = shared_from_this()](asio::error_code ec) {
        if (!ec) self->request(turn_resume);
      });
    }
  }
};
inline void pending_operations::keep(std::shared_ptr<operation> op) {
  auto* identity = op.get();
  identity->self_owner = std::move(op);
  std::lock_guard<std::mutex> lock(mutex_);
  identity->registry_next = first_;
  if (first_) first_->registry_prev = identity;
  first_ = identity;
}
inline void pending_operations::forget(operation* op) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (op->registry_prev) op->registry_prev->registry_next = op->registry_next;
    else if (first_ == op) first_ = op->registry_next;
    if (op->registry_next) op->registry_next->registry_prev = op->registry_prev;
    op->registry_prev = op->registry_next = nullptr;
  }
  // The caller holds a reference; releasing the self reference here cannot
  // destroy the operation under the registry lock.
  op->self_owner.reset();
}
inline void pending_operations::shutdown() {
  operation* retired;
  { std::lock_guard<std::mutex> lock(mutex_); retired = std::exchange(first_, nullptr); }
  // Operation destruction may acquire channel locks. Never do it under the
  // registry lock, and never acquire this lock while holding a channel lock.
  while (retired) {
    auto* next = std::exchange(retired->registry_next, nullptr);
    retired->registry_prev = nullptr;
    retired->self_owner.reset();
    retired = next;
  }
}

template<class T> struct channel_state : channel_base {
  static_assert(std::is_nothrow_move_constructible_v<T>, "payload move must be noexcept");
  bool closed = false;
  std::size_t limit;
  std::vector<std::optional<T>> buffer;
  std::size_t head = 0, size = 0;
  wait_list sends, receives;
  channel_state(std::size_t capacity, std::size_t pending)
    : limit(pending), buffer(capacity) {}

  // Claim the first waiting operation that can still be selected. A node is
  // unlinked first; its memory stays valid while this mutex is held, because
  // the owning operation can only be destroyed after unlinking under it.
  std::shared_ptr<operation> take(wait_list& queue, wait_node<T>*& selected, retired_operations& retired) {
    while (queue.head) {
      auto* node = static_cast<wait_node<T>*>(queue.head);
      queue.unlink(*node);
      if (auto op = retired.acquire(node->op)) if (op->claim(node->index)) {
        selected = node; return op;
      }
    }
    return {};
  }
  void push(std::optional<T>& from) {
    auto slot = head + size;
    if (slot >= buffer.size()) slot -= buffer.size();
    buffer[slot].emplace(std::move(*from));
    from.reset(); ++size;
  }
  void pop(std::optional<T>& to) {
    to.emplace(std::move(*buffer[head])); buffer[head].reset();
    if (++head == buffer.size()) head = 0;
    --size;
  }
  bool send(std::optional<T>& value, asio::error_code& ec, retired_operations& retired) {
    if (closed) { ec = asio::error::broken_pipe; return true; }
    wait_node<T>* w = nullptr;
    if (auto op = take(receives, w, retired)) {
      w->value->emplace(std::move(*value)); value.reset();
      op->error.clear(); op->notify(); ec.clear(); return true;
    }
    if (size < buffer.size()) { push(value); ec.clear(); return true; }
    return false;
  }
  bool receive(std::optional<T>& value, asio::error_code& ec, retired_operations& retired) {
    wait_node<T>* w = nullptr;
    if (size) {
      pop(value);
      if (!closed) if (auto op = take(sends, w, retired)) {
        push(*w->value); op->error.clear(); op->notify();
      }
      ec.clear(); return true;
    }
    if (auto op = take(sends, w, retired)) {
      value.emplace(std::move(**w->value)); w->value->reset();
      op->error.clear(); op->notify(); ec.clear(); return true;
    }
    if (closed) { ec = asio::error::eof; return true; }
    return false;
  }
  void close() {
    retired_operations retired;
    std::lock_guard<std::mutex> lock(this->mutex);
    if (closed) return;
    closed = true;
    wait_node<T>* w = nullptr;
    while (auto op = take(sends, w, retired)) { op->error = asio::error::broken_pipe; op->notify(); }
    while (auto op = take(receives, w, retired)) {
      if (size) { pop(*w->value); op->error.clear(); }
      else op->error = asio::error::eof;
      op->notify();
    }
  }
};
} // namespace channel_select_detail

template<class T> class select_channel {
  std::shared_ptr<channel_select_detail::channel_state<T>> state_;
public:
  using value_type = T;
  explicit select_channel(std::size_t capacity, std::size_t pending_limit = 1024)
    : state_(std::make_shared<channel_select_detail::channel_state<T>>(capacity, pending_limit)) {}
  ~select_channel() { if (state_) state_->close(); }
  select_channel(const select_channel&) = delete;
  select_channel& operator=(const select_channel&) = delete;
  select_channel(select_channel&&) = default;
  select_channel& operator=(select_channel&&) = delete;
  auto state() const { return state_; } // Internal case factory access.
  void close() { state_->close(); }
  // Rejection preserves the caller's payload, including move-only values.
  bool try_send(T&& value) {
    channel_select_detail::retired_operations retired;
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->closed) return false;
    std::optional<T> payload;
    // Check readiness before moving from the caller.
    channel_select_detail::wait_node<T>* w = nullptr;
    if (auto op = state_->take(state_->receives, w, retired)) {
      w->value->emplace(std::move(value)); op->error.clear(); op->notify(); return true;
    }
    if (state_->size == state_->buffer.size()) return false;
    payload.emplace(std::move(value)); state_->push(payload); return true;
  }
  std::optional<T> try_receive(asio::error_code& ec) {
    channel_select_detail::retired_operations retired;
    std::lock_guard<std::mutex> lock(state_->mutex);
    std::optional<T> result;
    if (!state_->receive(result, ec, retired)) ec = asio::error::would_block;
    return result;
  }
  std::size_t pending() const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->sends.count + state_->receives.count;
  }
  std::size_t buffered() const {
    std::lock_guard<std::mutex> lock(state_->mutex); return state_->size;
  }
};

template<class T, bool Sending> struct channel_select_case {
  using node_type = channel_select_detail::wait_node<T>; // Wait registration storage, owned by a suspended operation.
  std::shared_ptr<channel_select_detail::channel_state<T>> channel;
  std::optional<T> value;
  static constexpr bool sending = Sending;
  bool enabled = true;
  channel_select_case&& when(bool condition) && { enabled = condition; return std::move(*this); }
  channel_select_detail::channel_base* base() const { return enabled ? channel.get() : nullptr; }
  std::shared_ptr<channel_select_detail::channel_base> keep() const { return channel; }
  template<class Decision> bool attempt(Decision& op, std::size_t index,
      channel_select_detail::retired_operations& retired) {
    if (!enabled) return false;
    asio::error_code ec;
    bool ready;
    if constexpr (Sending) ready = channel->send(value, ec, retired);
    else ready = channel->receive(value, ec, retired);
    if (ready) op.commit(index, ec);
    return ready;
  }
  bool register_wait(node_type& node, channel_select_detail::operation& op, std::size_t index) {
    if (!enabled) return true;
    if (channel->sends.count + channel->receives.count >= channel->limit) return false;
    node.identity = &op; node.op = op.weak_from_this(); node.index = index; node.value = &value;
    (Sending ? channel->sends : channel->receives).push_back(node);
    return true;
  }
  auto expiry() const { return asio::steady_timer::time_point::max(); }
};
template<class T> auto receive(select_channel<T>& channel) {
  return channel_select_case<T, false>{channel.state(), {}, true};
}
template<class T> auto send(select_channel<T>& channel, T value) {
  return channel_select_case<T, true>{channel.state(), std::optional<T>(std::move(value)), true};
}
struct channel_select_after {
  using node_type = channel_select_detail::no_node;
  asio::steady_timer::time_point time;
  std::optional<std::monostate> value;
  bool enabled = true;
  channel_select_after&& when(bool condition) && { enabled = condition; return std::move(*this); }
  channel_select_detail::channel_base* base() const { return nullptr; }
  std::shared_ptr<channel_select_detail::channel_base> keep() const { return {}; }
  template<class Decision> bool attempt(Decision& op, std::size_t index,
      channel_select_detail::retired_operations&) {
    if (!enabled || time > asio::steady_timer::clock_type::now()) return false;
    value.emplace(); op.commit(index); return true;
  }
  bool register_wait(node_type&, channel_select_detail::operation&, std::size_t) { return true; }
  auto expiry() const { return enabled ? time : asio::steady_timer::time_point::max(); }
};
inline auto after(asio::steady_timer::time_point time) { return channel_select_after{time, {}, true}; }

namespace channel_select_detail {
// A committed case index with the cases it is about to own. Lets a result be
// constructed in place inside its completion tuple with one move of the cases.
template<class... Cases> struct committed { std::size_t index; std::tuple<Cases...>& cases; };
}
template<class... Cases> struct channel_select_result {
  static constexpr std::size_t npos = static_cast<std::size_t>(-1);
  std::size_t index = npos;
  std::tuple<Cases...> cases;
  channel_select_result() = default;
  channel_select_result(std::size_t selected, std::tuple<Cases...>&& owned)
    : index(selected), cases(std::move(owned)) {}
  channel_select_result(channel_select_detail::committed<Cases...> c)
    : index(c.index), cases(std::move(c.cases)) {}
  template<std::size_t I> auto& value() { return std::get<I>(cases).value; }
};
namespace channel_select_detail {
// No operation, strand, timer or registration is needed to commit a ready case.
// The sleeping peer (if any) still owns its usual lifetime and executor wakeup.
struct ready_decision {
  asio::error_code error;
  std::size_t index = static_cast<std::size_t>(-1);
  void commit(std::size_t selected, asio::error_code ec = {}) { index = selected; error = ec; }
};
template<class... Cases>
bool try_ready(std::tuple<Cases...>& cases, const channel_select_options& options,
    std::stop_token stop, bool cancelled, ready_decision& decision) {
  constexpr auto count = sizeof...(Cases);
  std::array<std::size_t, count> order;
  std::iota(order.begin(), order.end(), 0);
  if (!options.biased) shuffle_order(order, options);
  retired_operations retired; // Release acquired peers AFTER channel locks.
  if (cancelled || stop.stop_requested()) {
    decision.error = asio::error::operation_aborted; return true;
  }
  if (options.deadline != asio::steady_timer::time_point::max()
      && options.deadline <= asio::steady_timer::clock_type::now()) {
    decision.error = asio::error::timed_out; return true;
  }
  for (auto index : order) {
    bool ready = std::apply([&](auto&... c) {
      std::size_t current = 0;
      auto attempt = [&](auto& selected) {
        // The probe has no published registrations and can commit only once.
        // One channel at a time suffices; the suspended path still locks its
        // entire channel set in total order before publishing registrations.
        std::unique_lock<std::mutex> lock;
        if (auto* channel = selected.base()) lock = std::unique_lock<std::mutex>(channel->mutex);
        return selected.attempt(decision, index, retired);
      };
      return ((current++ == index && attempt(c)) || ...);
    }, cases);
    if (ready) return true;
  }
  if (options.nonblocking) { decision.error = asio::error::would_block; return true; }
  return false;
}
// Bound uninterrupted ready work on each worker; no readiness polling is used.
inline bool ready_budget_exhausted() {
  thread_local unsigned used = 0;
  if (++used < 64) return false;
  used = 0; return true;
}
template<class Handler, class... Cases> struct select_op : operation {
  Handler handler;
  channel_select_result<Cases...> result;
  std::tuple<typename Cases::node_type...> nodes; // One wait registration per case.
  asio::executor_work_guard<asio::any_io_executor> completion_work;
  select_op(asio::any_io_executor ex, channel_select_options opts, Handler h, std::tuple<Cases...>&& cases)
    : operation(ex, opts, sizeof...(Cases)), handler(std::move(h)),
      result(channel_select_result<Cases...>::npos, std::move(cases)),
      completion_work(asio::any_io_executor(asio::get_associated_executor(handler, ex))) {
    std::size_t used = 0;
    std::apply([&](auto&... c) { ((c.base() ? (channels[used++] = c.base(), void()) : void()), ...); }, result.cases);
    channels = channels.first(used);
    std::sort(channels.begin(), channels.end(), std::less<channel_base*>());
    channels = channels.first(std::unique(channels.begin(), channels.end()) - channels.begin());
  }
  ~select_op() override {
    // Execution-context teardown may destroy the timer handler without running
    // finalize. Remove its weak registrations before destroying case storage.
    // Before completion the case tuple owns every channel. After finalize has
    // withdrawn all registrations, it may move that tuple to the handler and
    // release those channels; a completed destructor must not touch raw bases.
    if (!finished) { auto locks = lock_channels(); remove(); }
  }
  template<class F, std::size_t... I> bool visit(std::size_t index, F f, std::index_sequence<I...>) {
    return ((index == I && f(std::get<I>(result.cases), std::get<I>(nodes))) || ...);
  }
  bool attempt(std::size_t index, retired_operations& retired) override {
    return visit(index, [&](auto& c, auto&) { return c.attempt(*this, index, retired); }, std::index_sequence_for<Cases...>());
  }
  bool register_case(std::size_t index) override {
    return visit(index, [&](auto& c, auto& node) { return c.register_wait(node, *this, index); },
        std::index_sequence_for<Cases...>());
  }
  void unlink_cases() override {
    std::apply([&](auto&... node) { (node.unlink(this), ...); }, nodes);
  }
  asio::steady_timer::time_point next_timer() const override {
    auto time = asio::steady_timer::time_point::max();
    std::apply([&](const auto&... c) { ((time = std::min(time, c.expiry())), ...); }, result.cases);
    return time;
  }
  void complete() override {
    auto w = winner.load(std::memory_order_acquire);
    if (w >= 0) result.index = static_cast<std::size_t>(w);
    auto completion_ex = completion_work.get_executor();
    auto completion = [h = std::move(handler), ec = error, r = std::move(result), work = std::move(completion_work)]() mutable {
      h(ec, std::move(r));
    };
    // A queued completion is a continuation of the current turn, like a
    // peer's wakeup in notify(). On one worker this keeps the order between a
    // peer's withdrawal and this completion as it was with posted wakeups.
    if (dispatch_completion && !initial_turn) asio::dispatch(completion_ex, std::move(completion));
    else asio::defer(completion_ex, std::move(completion));
  }
};
// Suspended startup shared by every completion token: allocate the owned
// operation, connect cancellation ingress, and run its first turn on the owner.
// coroutine_start means the initiating coroutine is already suspended: that
// turn may run inline on the owner and a later completion may dispatch its
// continuation. Generic initiations keep their posted start and completion.
template<class Handler, class... Cases>
void start_suspended(std::tuple<Cases...>&& cases, asio::any_io_executor ex,
    channel_select_options options, std::stop_token stop, Handler handler, bool coroutine_start) {
  // The owned operation is allocated through Asio's per-thread recycling
  // allocator, like Asio's own handler operations.
  auto op = std::allocate_shared<select_op<Handler, Cases...>>(asio::recycling_allocator<void>(),
      ex, options, std::move(handler), std::move(cases));
  op->stop = std::move(stop);
  op->dispatch_completion = coroutine_start;
  // The first turn is scheduled by hand before any cancellation ingress exists.
  op->turns.store(operation::scheduled | operation::turn_start, std::memory_order_relaxed);
  auto slot = asio::get_associated_cancellation_slot(op->handler);
  if (slot.is_connected()) slot.assign([weak = std::weak_ptr<operation>(op)](asio::cancellation_type_t type) {
    if (type != asio::cancellation_type::none) if (auto p = weak.lock()) p->request_cancel();
  });
  auto runner = [op] { op->run_turns(); };
  if (coroutine_start) asio::dispatch(op->owner, std::move(runner));
  else asio::post(op->owner, std::move(runner));
}
// Cancellation already recorded in an awaiting coroutine's cancellation state.
// Other handlers expose no such state; they rely on the stop token and on the
// slot ingress flag that the suspended operation installs.
template<class Handler> bool cancellation_requested(const Handler& handler) noexcept {
  if constexpr (requires { handler.get_cancellation_state().cancelled(); })
    return handler.get_cancellation_state().cancelled() != asio::cancellation_type::none;
  else return false;
}
} // namespace channel_select_detail

// Lazy selection returned for asio::deferred tokens. Awaiting it from an
// asio::awaitable coroutine whose executor equals the explicit owner first
// probes readiness in that coroutine's own frame, before it suspends; a
// committed probe creates no operation, strand, timer or wait registration.
// Otherwise the coroutine suspends and the ordinary owned operation starts on
// the owner. Invoking the object with any other completion token starts the
// same owned operation with the posted turn that generic tokens always had.
// Tuple selects the completion signature: void(tuple<error_code, result>) for
// as_tuple, or void(error_code, result), which throws on error when awaited.
template<bool Tuple, class... Cases> class channel_select_operation {
public:
  using result_type = channel_select_result<Cases...>;
  using completion_type = std::tuple<asio::error_code, result_type>;
  using signature = std::conditional_t<Tuple, void(completion_type), void(asio::error_code, result_type)>;
  // The completion tuple is the operation's only case storage: a committed
  // probe sets its error and index in place, without moving any case.
  channel_select_operation(std::tuple<Cases...>&& cases, asio::any_io_executor&& ex,
      channel_select_options options, std::stop_token&& stop)
    : completion_(asio::error_code{}, channel_select_detail::committed<Cases...>{result_type::npos, cases}),
      executor_(std::move(ex)), options_(options), stop_(std::move(stop)) {}
  template<ASIO_COMPLETION_TOKEN_FOR(signature) CompletionToken>
  auto operator()(CompletionToken&& token) && {
    return asio::async_initiate<CompletionToken, signature>(initiation{std::move(*this)}, token);
  }
  // Ready probe run by the awaiting coroutine on executor current. Returns
  // false, leaving every case untouched, when the probe may not run on that
  // executor, when the worker's ready budget is exhausted (a cooperative yield
  // is then owed), or when no case is ready. A probe runs at most once; after
  // it, awaiting the operation suspends at once.
  template<class Executor>
  bool probe(const Executor& current, asio::cancellation_type_t cancelled) {
    awaited_ = probed_ = true;
    // An explicitly different owner still starts on that owner. Never infer
    // affinity merely from the construction of a channel or I/O object.
    if (!(executor_ == current)) return false;
    if (channel_select_detail::ready_budget_exhausted()) { yield_ = true; return false; }
    channel_select_detail::ready_decision decision;
    auto& result = std::get<1>(completion_);
    if (!channel_select_detail::try_ready(result.cases, options_, stop_,
          cancelled != asio::cancellation_type::none, decision)) return false;
    std::get<0>(completion_) = decision.error;
    result.index = decision.index;
    return true;
  }
  completion_type& completion() noexcept { return completion_; }
  // Immediate completion in the awaiting frame (awaitable_async_op_immediate).
  template<class Frame, class Pointers>
  bool try_immediate(Frame* frame, Pointers& pointers) {
    awaited_ = true;
    if (probed_) return false;
    if (!probe(frame->executor(),
          frame->await_transform(asio::this_coro::cancellation_state).await_resume().cancelled())) return false;
    if constexpr (Tuple) pointers = &completion_;
    else { pointers.disposition_ = &std::get<0>(completion_); pointers.value_ = &std::get<1>(completion_); }
    return true;
  }
private:
  struct initiation {
    channel_select_operation self;
    template<class Handler> void operator()(Handler handler) && { std::move(self).start(std::move(handler)); }
  };
  template<class Handler> static auto adapt(Handler handler) {
    if constexpr (Tuple) return asio::detail::as_tuple_handler<Handler>(std::move(handler));
    else return handler;
  }
  template<class Handler> void start(Handler handler) && {
    if (yield_) {
      // The worker's ready budget is exhausted: already queued work runs first,
      // then the probe repeats on the executor before any registration. The
      // executor is copied before the capture below moves this object.
      auto executor = executor_;
      asio::post(executor, [self = std::move(*this), handler = std::move(handler)]() mutable {
        channel_select_detail::ready_decision decision;
        auto& result = std::get<1>(self.completion_);
        if (channel_select_detail::try_ready(result.cases, self.options_, self.stop_,
              channel_select_detail::cancellation_requested(handler), decision)) {
          std::get<0>(self.completion_) = decision.error;
          result.index = decision.index;
          if constexpr (Tuple) std::move(handler)(std::move(self.completion_));
          else std::move(handler)(decision.error, std::move(result));
        } else std::move(self).suspend(std::move(handler));
      });
      return;
    }
    std::move(*this).suspend(std::move(handler));
  }
  template<class Handler> void suspend(Handler handler) && {
    // Readiness can change after a probe; the suspended path checks again
    // under its channel locks before publishing any wait registrations.
    channel_select_detail::start_suspended(std::move(std::get<1>(completion_).cases), executor_, options_, stop_,
        adapt(std::move(handler)), awaited_);
  }
  completion_type completion_;
  asio::any_io_executor executor_;
  channel_select_options options_;
  std::stop_token stop_;
  bool awaited_ = false; // Initiation runs after the awaiting coroutine suspended.
  bool probed_ = false;  // The ready probe already ran for this operation.
  bool yield_ = false;   // A cooperative executor turn precedes the next probe.
};
} // namespace experimental
namespace detail {
template<bool Tuple, class... Cases>
struct awaitable_async_op_immediate<experimental::channel_select_operation<Tuple, Cases...>> {
  template<class Frame, class Result>
  static bool try_complete(experimental::channel_select_operation<Tuple, Cases...>& op,
      Frame* frame, Result& result) {
    return op.try_immediate(frame, result);
  }
};
} // namespace detail
namespace experimental {

template<class... Cases> class channel_selection {
  static_assert(sizeof...(Cases) > 0, "select requires at least one case");
  std::tuple<Cases...> cases_;
public:
  template<class... Init>
    requires (sizeof...(Init) == sizeof...(Cases) && (std::is_constructible_v<Cases, Init&&> && ...))
  explicit channel_selection(Init&&... cases) : cases_(std::forward<Init>(cases)...) {}
private:
  template<class CompletionToken>
  static auto wait_suspended(std::tuple<Cases...> cases, asio::any_io_executor ex,
      channel_select_options options, std::stop_token stop, CompletionToken&& token) {
    return asio::async_initiate<CompletionToken,
        void(asio::error_code, channel_select_result<Cases...>)>(
      [ex, options, stop, cases = std::move(cases)](auto handler) mutable {
        channel_select_detail::start_suspended(std::move(cases), ex, options, stop, std::move(handler), false);
      }, token);
  }
  // use_awaitable keeps its awaitable<T> return type. Its frame runs the same
  // ready probe as the lazy operation; a suspended selection then awaits that
  // operation, which starts without a further initiation coroutine.
  template<class Executor>
  static asio::awaitable<std::tuple<asio::error_code, channel_select_result<Cases...>>, Executor>
  wait_tuple(std::tuple<Cases...> cases, asio::any_io_executor ex,
      channel_select_options options, std::stop_token stop) {
    channel_select_operation<true, Cases...> op(std::move(cases), std::move(ex), options, std::move(stop));
    if (op.probe(co_await asio::this_coro::executor, (co_await asio::this_coro::cancellation_state).cancelled()))
      co_return std::move(op.completion());
    co_return co_await std::move(op);
  }
  template<class Executor>
  static asio::awaitable<channel_select_result<Cases...>, Executor>
  wait_throwing(std::tuple<Cases...> cases, asio::any_io_executor ex,
      channel_select_options options, std::stop_token stop) {
    channel_select_operation<false, Cases...> op(std::move(cases), std::move(ex), options, std::move(stop));
    if (op.probe(co_await asio::this_coro::executor, (co_await asio::this_coro::cancellation_state).cancelled())) {
      if (auto& ec = std::get<0>(op.completion())) throw asio::system_error(ec);
      co_return std::get<1>(std::move(op.completion()));
    }
    co_return co_await std::move(op);
  }
public:
  template<class CompletionToken>
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, CompletionToken&& token) && {
    return wait_suspended(std::move(cases_), ex, options, stop, std::forward<CompletionToken>(token));
  }
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, asio::deferred_t) && {
    return channel_select_operation<false, Cases...>(std::move(cases_), std::move(ex), options, std::move(stop));
  }
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, asio::as_tuple_t<asio::deferred_t>) && {
    return channel_select_operation<true, Cases...>(std::move(cases_), std::move(ex), options, std::move(stop));
  }
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, asio::partial_as_tuple) && {
    return channel_select_operation<true, Cases...>(std::move(cases_), std::move(ex), options, std::move(stop));
  }
  template<class Executor>
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, asio::as_tuple_t<asio::use_awaitable_t<Executor>>) && {
    return wait_tuple<Executor>(std::move(cases_), std::move(ex), options, std::move(stop));
  }
  template<class Executor>
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, asio::use_awaitable_t<Executor>) && {
    return wait_throwing<Executor>(std::move(cases_), std::move(ex), options, std::move(stop));
  }
};
template<class... Cases> auto make_select(Cases... cases) {
  return channel_selection<Cases...>(std::move(cases)...);
}
} // namespace experimental
ASIO_INLINE_NAMESPACE_END
} // namespace asio
#endif
