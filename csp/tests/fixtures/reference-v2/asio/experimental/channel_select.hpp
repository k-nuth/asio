// Copyright (c) 2026 Knuth contributors
// Distributed under the Boost Software License, Version 1.0.
#ifndef ASIO_EXPERIMENTAL_CHANNEL_SELECT_HPP
#define ASIO_EXPERIMENTAL_CHANNEL_SELECT_HPP

#include <asio/any_io_executor.hpp>
#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
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
#include <asio/dispatch.hpp>
#include <asio/steady_timer.hpp>
#include <asio/strand.hpp>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <list>
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
struct operation;
// Execution-context ownership for suspended channel operations. An infinite
// timer is not a lifetime token: constructing one initializes the I/O reactor.
// This service has no worker or scheduler; shutdown only releases its owners.
class pending_operations : public asio::execution_context::service {
  std::mutex mutex_;
  std::unordered_map<const operation*, std::shared_ptr<operation>> owners_;
  void shutdown() override {
    decltype(owners_) retired;
    { std::lock_guard<std::mutex> lock(mutex_); retired.swap(owners_); }
    // Operation destruction may acquire channel locks. Never do it under the
    // registry lock, and never acquire this lock while holding a channel lock.
  }
public:
  inline static asio::execution_context::id id;
  explicit pending_operations(asio::execution_context& context) : service(context) {}
  void keep(std::shared_ptr<operation> op) {
    auto* identity = op.get();
    std::lock_guard<std::mutex> lock(mutex_);
    owners_.emplace(identity, std::move(op));
  }
  void forget(const operation* identity) {
    std::shared_ptr<operation> retired;
    { std::lock_guard<std::mutex> lock(mutex_);
      auto it = owners_.find(identity);
      if (it != owners_.end()) { retired = std::move(it->second); owners_.erase(it); }
    }
  }
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
  virtual void remove(const operation*) = 0; // Caller holds mutex.
};
struct operation : std::enable_shared_from_this<operation> {
  static constexpr std::ptrdiff_t active = -1, waiting = -2, aborted = -3;
  std::atomic<std::ptrdiff_t> winner{active};
  std::atomic<bool> cancellation_requested{false}; // Cancellation ingress only.
  asio::error_code error;
  asio::strand<asio::any_io_executor> owner;
  std::optional<asio::steady_timer> timer;
  pending_operations* registration_owner = nullptr;
  channel_select_options options;
  // Common selections need no separate heap allocations for their case order,
  // channel set, or locks. Larger selections retain the same unbounded arity.
  std::array<channel_base*, 4> local_channels;
  std::array<std::size_t, 4> local_order;
  std::vector<channel_base*> extra_channels;
  std::vector<std::size_t> extra_order;
  std::span<channel_base*> channels;
  std::span<std::size_t> order;
  std::optional<std::stop_callback<std::function<void()>>> stop_callback;
  bool finished = false; // Owner only.
  bool initial_turn = true; // Owner only; immediate completions must be posted.
  bool dispatch_completion = false; // Coroutine continuation may dispatch after suspension.

  operation(asio::any_io_executor ex, channel_select_options opt, std::size_t count)
    : owner(asio::make_strand(ex)), options(opt) {
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
  void remove() { for (auto* c : channels) c->remove(this); }
  bool claim(std::size_t index) {
    auto expected = waiting;
    return winner.compare_exchange_strong(expected, static_cast<std::ptrdiff_t>(index),
        std::memory_order_acq_rel);
  }
  void commit(std::size_t index, asio::error_code ec = {}) {
    error = ec;
    winner.store(static_cast<std::ptrdiff_t>(index), std::memory_order_release);
  }
  void notify() {
    asio::post(owner, [self = shared_from_this()] { self->finalize(); });
  }
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
    if (!options.biased) {
      // Thread-local generator: no global selection lock or RNG lock.
      if (options.random_seed) {
        std::mt19937 seeded(*options.random_seed); std::shuffle(order.begin(), order.end(), seeded);
      } else {
        thread_local selection_random engine;
        std::shuffle(order.begin(), order.end(), engine);
      }
    }
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
        if (!ec) self->start(true);
      });
    }
  }
};

template<class T> struct channel_state : channel_base {
  static_assert(std::is_nothrow_move_constructible_v<T>, "payload move must be noexcept");
  struct waiter {
    std::weak_ptr<operation> op;
    const operation* identity;
    std::size_t index;
    std::optional<T>* value;
  };
  bool closed = false;
  std::size_t limit;
  std::vector<std::optional<T>> buffer;
  std::size_t head = 0, size = 0;
  std::list<waiter> sends, receives;
  channel_state(std::size_t capacity, std::size_t pending)
    : limit(pending), buffer(capacity) {}

  void remove(const operation* op) override {
    auto matches = [op](const waiter& w) {
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE)
      ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE(w.identity, op, false);
#endif
      return w.identity == op || w.op.expired();
    };
    sends.remove_if(matches); receives.remove_if(matches);
  }
  std::shared_ptr<operation> take(std::list<waiter>& queue, waiter& selected, retired_operations& retired) {
    while (!queue.empty()) {
      auto w = queue.front(); queue.pop_front();
      if (auto op = retired.acquire(w.op)) if (op->claim(w.index)) {
        selected = w; return op;
      }
    }
    return {};
  }
  void push(std::optional<T>& from) {
    buffer[(head + size) % buffer.size()].emplace(std::move(*from));
    from.reset(); ++size;
  }
  void pop(std::optional<T>& to) {
    to.emplace(std::move(*buffer[head])); buffer[head].reset();
    head = (head + 1) % buffer.size(); --size;
  }
  bool send(std::optional<T>& value, asio::error_code& ec, retired_operations& retired) {
    if (closed) { ec = asio::error::broken_pipe; return true; }
    waiter w;
    if (auto op = take(receives, w, retired)) {
      w.value->emplace(std::move(*value)); value.reset();
      op->error.clear(); op->notify(); ec.clear(); return true;
    }
    if (size < buffer.size()) { push(value); ec.clear(); return true; }
    return false;
  }
  bool receive(std::optional<T>& value, asio::error_code& ec, retired_operations& retired) {
    waiter w;
    if (size) {
      pop(value);
      if (!closed) if (auto op = take(sends, w, retired)) {
        push(*w.value); op->error.clear(); op->notify();
      }
      ec.clear(); return true;
    }
    if (auto op = take(sends, w, retired)) {
      value.emplace(std::move(**w.value)); w.value->reset();
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
    waiter w;
    while (auto op = take(sends, w, retired)) { op->error = asio::error::broken_pipe; op->notify(); }
    while (auto op = take(receives, w, retired)) {
      if (size) { pop(*w.value); op->error.clear(); }
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
    while (!state_->receives.empty()) {
      auto w = state_->receives.front(); state_->receives.pop_front();
      if (auto op = retired.acquire(w.op)) if (op->claim(w.index)) {
        w.value->emplace(std::move(value)); op->error.clear(); op->notify(); return true;
      }
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
    return state_->sends.size() + state_->receives.size();
  }
  std::size_t buffered() const {
    std::lock_guard<std::mutex> lock(state_->mutex); return state_->size;
  }
};

template<class T, bool Sending> struct channel_select_case {
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
  bool register_wait(std::shared_ptr<channel_select_detail::operation> op, std::size_t index) {
    if (!enabled) return true;
    if (channel->sends.size() + channel->receives.size() >= channel->limit) return false;
    auto& queue = Sending ? channel->sends : channel->receives;
    queue.push_back({op, op.get(), index, &value}); return true;
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
  bool register_wait(std::shared_ptr<channel_select_detail::operation>, std::size_t) { return true; }
  auto expiry() const { return enabled ? time : asio::steady_timer::time_point::max(); }
};
inline auto after(asio::steady_timer::time_point time) { return channel_select_after{time, {}, true}; }

template<class... Cases> struct channel_select_result {
  static constexpr std::size_t npos = static_cast<std::size_t>(-1);
  std::size_t index = npos;
  std::tuple<Cases...> cases;
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
  if (!options.biased) {
    if (options.random_seed) {
      std::mt19937 seeded(*options.random_seed); std::shuffle(order.begin(), order.end(), seeded);
    } else {
      thread_local selection_random engine;
      std::shuffle(order.begin(), order.end(), engine);
    }
  }
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
  asio::executor_work_guard<asio::any_io_executor> completion_work;
  select_op(asio::any_io_executor ex, channel_select_options opts, Handler h, std::tuple<Cases...> cases)
    : operation(ex, opts, sizeof...(Cases)), handler(std::move(h)),
      result{channel_select_result<Cases...>::npos, std::move(cases)},
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
    return ((index == I && f(std::get<I>(result.cases))) || ...);
  }
  bool attempt(std::size_t index, retired_operations& retired) override {
    return visit(index, [&](auto& c) { return c.attempt(*this, index, retired); }, std::index_sequence_for<Cases...>());
  }
  bool register_case(std::size_t index) override {
    return visit(index, [&](auto& c) { return c.register_wait(this->shared_from_this(), index); },
        std::index_sequence_for<Cases...>());
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
    if (dispatch_completion && !initial_turn) asio::dispatch(completion_ex, std::move(completion));
    else asio::post(completion_ex, std::move(completion));
  }
};
} // namespace channel_select_detail

template<class... Cases> class channel_selection {
  static_assert(sizeof...(Cases) > 0, "select requires at least one case");
  std::tuple<Cases...> cases_;
public:
  explicit channel_selection(Cases... cases) : cases_(std::move(cases)...) {}
private:
  template<class CompletionToken>
  static auto wait_suspended(std::tuple<Cases...> cases, asio::any_io_executor ex,
      channel_select_options options, std::stop_token stop, CompletionToken&& token, bool coroutine_start = false) {
    static_assert(sizeof...(Cases) > 0, "select requires at least one case");
    return asio::async_initiate<CompletionToken,
        void(asio::error_code, channel_select_result<Cases...>)>(
      [ex, options, stop, coroutine_start, cases = std::move(cases)](auto handler) mutable {
        auto op = std::make_shared<channel_select_detail::select_op<decltype(handler), Cases...>>(
            ex, options, std::move(handler), std::move(cases));
        op->dispatch_completion = coroutine_start;
        auto request = [weak = std::weak_ptr<channel_select_detail::operation>(op)] {
          if (auto p = weak.lock()) {
            p->cancellation_requested.store(true, std::memory_order_release);
            asio::post(p->owner, [p] { p->cancel(asio::error::operation_aborted); });
          }
        };
        auto slot = asio::get_associated_cancellation_slot(op->handler);
        if (slot.is_connected()) slot.assign([request](asio::cancellation_type_t type) {
          if (type != asio::cancellation_type::none) request();
        });
        auto start = [op, stop, request] {
          if (op->finished) return;
          // Installation and removal share the operation's owner. An already
          // stopped token may invoke request inline during emplacement.
          if (stop.stop_possible()) op->stop_callback.emplace(stop, request);
          if (stop.stop_requested() || op->cancellation_requested.load(std::memory_order_acquire))
            op->cancel(asio::error::operation_aborted);
          else op->start();
          op->initial_turn = false;
        };
        if (coroutine_start) asio::dispatch(op->owner, std::move(start));
        else asio::post(op->owner, std::move(start));
      }, token);
  }
  template<class Executor>
  static asio::awaitable<std::tuple<asio::error_code, channel_select_result<Cases...>>, Executor>
  wait_tuple(std::tuple<Cases...> cases, asio::any_io_executor ex,
      channel_select_options options, std::stop_token stop,
      asio::as_tuple_t<asio::use_awaitable_t<Executor>> token) {
    auto current = co_await asio::this_coro::executor;
    // An explicitly different owner still starts on that owner. Never infer
    // affinity merely from the construction of a channel or I/O object.
    if (asio::any_io_executor(current) == ex) {
      if (channel_select_detail::ready_budget_exhausted())
        co_await asio::post(current, asio::use_awaitable_t<Executor>{});
      auto cancellation = co_await asio::this_coro::cancellation_state;
      channel_select_detail::ready_decision decision;
      if (channel_select_detail::try_ready(cases, options, stop,
            cancellation.cancelled() != asio::cancellation_type::none, decision)) {
        co_return std::make_tuple(decision.error,
            channel_select_result<Cases...>{decision.index, std::move(cases)});
      }
    }
    // Readiness can change after the probe; the suspended path checks again
    // under its channel locks before publishing any wait registrations.
    co_return co_await wait_suspended(std::move(cases), ex, options, stop, std::move(token), true);
  }
  template<class Executor>
  static asio::awaitable<channel_select_result<Cases...>, Executor>
  wait_throwing(std::tuple<Cases...> cases, asio::any_io_executor ex,
      channel_select_options options, std::stop_token stop, asio::use_awaitable_t<Executor> token) {
    auto [ec, result] = co_await wait_tuple(std::move(cases), ex, options, stop, asio::as_tuple(token));
    if (ec) throw asio::system_error(ec);
    co_return std::move(result);
  }
public:
  template<class CompletionToken>
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, CompletionToken&& token) && {
    return wait_suspended(std::move(cases_), ex, options, stop, std::forward<CompletionToken>(token));
  }
  template<class Executor>
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, asio::as_tuple_t<asio::use_awaitable_t<Executor>> token) && {
    return wait_tuple(std::move(cases_), ex, options, stop, std::move(token));
  }
  template<class Executor>
  auto async_wait(asio::any_io_executor ex, channel_select_options options,
      std::stop_token stop, asio::use_awaitable_t<Executor> token) && {
    return wait_throwing(std::move(cases_), ex, options, stop, std::move(token));
  }
};
template<class... Cases> auto make_select(Cases... cases) {
  return channel_selection<Cases...>(std::move(cases)...);
}
} // namespace experimental
ASIO_INLINE_NAMESPACE_END
} // namespace asio
#endif
