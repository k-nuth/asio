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
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <asio/recycling_allocator.hpp>
#include <asio/any_completion_handler.hpp>
#include <asio/detail/scheduler.hpp>
#include <asio/detail/scheduler_operation.hpp>
#include <asio/detail/throw_exception.hpp>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <optional>
#include <random>
#include <stdexcept>
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
// A small generator, following the inexpensive Xorshift approach used by
// Crossbeam. One per worker seeds one per operation, so a selection draws
// without touching thread-local storage. The explicit-seed path below keeps
// its original Mersenne Twister sequence.
class selection_random {
  std::uint64_t state_;
public:
  using result_type = std::uint64_t;
  selection_random() : state_([] {
    std::random_device source;
    auto seed = (std::uint64_t(source()) << 32) ^ source();
    return seed ? seed : UINT64_C(0x9e3779b97f4a7c15);
  }()) {}
  explicit selection_random(selection_random& source) : state_(source()) {}
  static constexpr result_type min() { return 0; }
  static constexpr result_type max() { return UINT64_MAX; }
  result_type operator()() {
    auto x = state_;
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    state_ = x;
    return x * UINT64_C(2685821657736338717);
  }
  // An exactly uniform draw in [0, bound): multiply-high range reduction with
  // the rejection step of Lemire's method. The rejection branch is taken with
  // probability below bound / 2^32, so the draw is unbiased without the
  // division of std::uniform_int_distribution on every call.
  std::uint32_t below(std::uint32_t bound) {
    auto draw = [this] { return static_cast<std::uint32_t>((*this)() >> 32); };
    auto product = std::uint64_t(draw()) * bound;
    if (static_cast<std::uint32_t>(product) < bound) {
      auto threshold = static_cast<std::uint32_t>(0u - bound) % bound;
      while (static_cast<std::uint32_t>(product) < threshold) product = std::uint64_t(draw()) * bound;
    }
    return static_cast<std::uint32_t>(product >> 32);
  }
};
inline selection_random& worker_random() {
  thread_local selection_random engine;
  return engine;
}
// Default arbitration is a Fisher-Yates shuffle with exactly uniform draws:
// every permutation has the same probability. An explicit seed keeps
// std::shuffle over std::mt19937, so the documented reproducible permutation
// sequence is unchanged.
inline void shuffle_order(std::span<std::size_t> order, const channel_select_options& options,
    selection_random& engine) {
  if (options.random_seed) {
    std::mt19937 seeded(*options.random_seed);
    std::shuffle(order.begin(), order.end(), seeded);
    return;
  }
  for (auto i = order.size(); i > 1; --i)
    std::swap(order[i - 1], order[engine.below(static_cast<std::uint32_t>(i))]);
}
inline void shuffle_order(std::span<std::size_t> order, const channel_select_options& options) {
  shuffle_order(order, options, worker_random());
}
// A cleared error, copied instead of constructed on hot paths: constructing
// or clearing a std::error_code calls system_category() each time.
inline const asio::error_code clear_error{};
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
struct channel_base {
  std::mutex mutex;
  virtual ~channel_base() = default;
};
// Intrusive wait registrations. A node lives in its owning operation and is
// linked in at most one channel list, under that channel's mutex. The owning
// operation unlinks it under that mutex before it can be destroyed, and it is
// strongly owned (by its runner, then by the execution context's registry)
// from registration until withdrawal, so a linked node always belongs to a
// live, owned operation: a peer claims and wakes it through this pointer, and
// no registration allocates or counts references.
// A value modified only under a channel mutex but also read, as a hint,
// without it: relaxed atomic accesses, plain loads and stores on every
// supported target, make such a read well defined.
template<class T> struct hinted {
  std::atomic<T> value;
  constexpr hinted(T v = T()) : value(v) {}
  T get() const { return value.load(std::memory_order_relaxed); }
  void set(T v) { value.store(v, std::memory_order_relaxed); }
  operator T() const { return get(); }
  hinted& operator=(T v) { set(v); return *this; }
  hinted& operator++() { set(get() + 1); return *this; }
  hinted& operator--() { set(get() - 1); return *this; }
};
struct wait_list;
struct wait_link {
  wait_link* prev = nullptr;
  wait_link* next = nullptr;
  hinted<wait_list*> list; // Read without the mutex by the owner to skip withdrawal from a list it left.
  operation* identity = nullptr;
  std::uint64_t generation = 0; // The owning operation's generation when registered.
};
struct wait_list {
  wait_link* head = nullptr;
  wait_link* tail = nullptr;
  std::mutex* mutex = nullptr; // The owning channel's mutex.
  hinted<std::size_t> count;
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
  std::size_t index = 0;
  std::optional<T>* value = nullptr;
  // Withdrawal by the owning operation; the caller holds the list's mutex.
  // Relinking touches the neighbouring registrations of other operations.
  void unlink(const operation* removing) {
    auto* l = list.get();
    if (!l) return;
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE)
    if (prev) ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE(prev->identity, removing, false);
    if (next) ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE(next->identity, removing, false);
#else
    (void)removing;
#endif
    l->unlink(*this);
  }
  // Withdrawal after commitment, taking only this node's mutex. A node that a
  // claiming peer already unlinked stays unlinked, since only its owner links
  // it, so its mutex is not taken at all.
  void withdraw(const operation* removing) {
    auto* l = list.get();
    if (!l) return;
    std::lock_guard<std::mutex> lock(*l->mutex);
    unlink(removing);
  }
};
struct no_node {
  void unlink(const operation*) {}
  void withdraw(const operation*) {}
};
// How a function is handed to an executor: as a continuation of the current
// handler (defer), inline when already on that executor (dispatch), or as an
// ordinary queued item (post).
enum class launch { defer, dispatch, post };
template<class Executor, class F> void submit(launch how, const Executor& ex, F&& f) {
  if (how == launch::dispatch) asio::dispatch(ex, std::forward<F>(f));
  else if (how == launch::post) asio::post(ex, std::forward<F>(f));
  else asio::defer(ex, std::forward<F>(f));
}
// Every submission through any_io_executor builds a type-erased function
// object, allocated per submission, before the target executor sees it. When
// the target is the plain io_context executor, hand the function to it
// directly; any other target keeps the generic path.
template<class Executor, class F> void submit_fast(launch how, const Executor& ex, F&& f) {
  if constexpr (std::is_same_v<Executor, asio::any_io_executor>)
    if (const auto* io = ex.template target<asio::io_context::executor_type>())
      return submit(how, *io, std::forward<F>(f));
  submit(how, ex, std::forward<F>(f));
}
// An operation is also its own scheduler operation: when the owner is the
// plain io_context executor, a runner turn is queued by linking the
// operation itself into that scheduler, with no function object and no
// allocation; the operation owns itself while queued or running.
struct operation : std::enable_shared_from_this<operation>, asio::detail::scheduler_operation {
  static constexpr std::ptrdiff_t active = -1, waiting = -2, aborted = -3;
  std::atomic<std::ptrdiff_t> winner{active};
  // Serial turn runner, replacing a private strand. The low bit records that a
  // runner is scheduled or running; the other bits are requested turns. At
  // most one runner exists per operation, and before it exits it drains every
  // request made while it ran, so the first start, cancellation, a timer
  // resume, finalization and release never run concurrently and never nest.
  // A queued runner is a continuation (defer) on the owner executor; a runner
  // holds a strong reference to the operation while it runs.
  static constexpr unsigned scheduled = 1, turn_start = 2, turn_cancel = 4, turn_resume = 8, turn_finalize = 16,
      turn_release = 32;
  std::atomic<unsigned> turns{0};
  // Generation of the selection this operation currently serves. A one-shot
  // operation serves generation 1; a reusable selector arms the next
  // generation for each wait, after the previous one finished and withdrew
  // every registration. Cancellation, timer and wakeup requests carry the
  // generation they were raised for: the marks below are monotonic, written
  // on the ingress thread and compared with the current generation inside
  // the runner, so nothing raised for a finished selection reaches a later
  // one. Generations never wrap in practice (64 bits).
  std::atomic<std::uint64_t> generation{1};
  std::atomic<std::uint64_t> cancel_generation{0};   // Cancellation ingress, any thread.
  std::atomic<std::uint64_t> resume_generation{0};   // Timer expiry, owner executor.
  std::atomic<std::uint64_t> finalize_generation{0}; // Peer wakeup, under a channel lock.
  std::uint64_t started_generation = 0; // Runner only.
  asio::error_code error;
  asio::any_io_executor owner;
  const asio::io_context::executor_type* owner_io; // The plain io_context executor behind owner, if that is its target.
  asio::detail::scheduler* scheduler; // That executor's scheduler, when owner_io is set.
  std::shared_ptr<operation> runner_ref; // Self-ownership while queued or running as a scheduler operation.
  std::stop_token stop;
  std::optional<asio::steady_timer> timer;
  pending_operations* registration_owner; // Resolved at construction, so keeping a registration cannot fail.
  operation* registry_prev = nullptr; // Lifetime registry links; registry mutex only.
  operation* registry_next = nullptr;
  std::shared_ptr<operation> self_owner; // Set while kept by the registry.
  // A one-shot operation is kept by the registry from registration until
  // finalization. A reusable state stays kept from its first registration
  // until its selector releases it (or the context shuts down), so later
  // generations keep and forget nothing; registered says whether a release
  // is owed, for the selector's destructor on any thread.
  bool persistent_registration = false;
  std::atomic<bool> registered{false};
  channel_select_options options;
  // Common selections need no separate heap allocations for their case order,
  // channel set, or locks. Larger selections retain the same unbounded arity.
  std::array<channel_base*, 4> local_channels;
  std::array<std::size_t, 4> local_order;
  std::vector<channel_base*> extra_channels;
  std::vector<std::size_t> extra_order;
  std::span<channel_base*> channels;
  std::span<std::size_t> order;
  selection_random random{worker_random()}; // Serialized like the order it permutes.
  struct cancel_request {
    operation* op;
    std::uint64_t generation;
    void operator()() const { op->request_cancel(generation); }
    void operator()(asio::cancellation_type_t type) const {
      if (type != asio::cancellation_type::none) op->request_cancel(generation);
    }
  };
  // Declared after the atomics it uses: its destructor, which waits for a
  // callback in progress, runs before they are destroyed.
  std::optional<std::stop_callback<cancel_request>> stop_callback;
  bool finished = false; // Owner only.
  bool initial_turn = true; // Owner only; immediate completions must be posted.
  bool dispatch_completion = false; // Coroutine continuation may dispatch after suspension.
  // The arming task drew a fresh permutation for its probe just before this
  // generation's first start; that start reuses it instead of drawing again.
  bool order_fresh = false;
  // The stored completion runs on the owner itself (an awaiting coroutine
  // whose executor compared equal to it), so it is handed to owner_io.
  bool completion_on_owner = false;

  operation(asio::any_io_executor ex, channel_select_options opt, std::size_t count)
    : scheduler_operation(&operation::run_scheduled),
      owner(std::move(ex)), owner_io(owner.target<asio::io_context::executor_type>()),
      scheduler(owner_io ? &asio::use_service<asio::detail::scheduler>(asio::query(owner, asio::execution::context)) : nullptr),
      registration_owner(&asio::use_service<pending_operations>(asio::query(owner, asio::execution::context))),
      options(opt) {
    if (count <= local_order.size()) {
      channels = std::span(local_channels).first(count);
      order = std::span(local_order).first(count);
    } else {
      extra_channels.resize(count); extra_order.resize(count);
      channels = extra_channels; order = extra_order;
    }
    std::iota(order.begin(), order.end(), 0);
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_OPERATION)
    ASIO_CHANNEL_SELECT_TEST_OBSERVE_OPERATION(this);
#endif
  }
  virtual ~operation() = default;
  virtual bool attempt(std::size_t) = 0; // Caller holds every participating channel mutex.
  virtual bool register_case(std::size_t) = 0;
  virtual void unlink_cases() = 0; // Caller holds every participating channel mutex.
  virtual void withdraw_cases() = 0; // After commitment; takes each remaining node's mutex alone.
  virtual asio::steady_timer::time_point next_timer() const = 0;
  virtual void complete() = 0;
  virtual void discard_completion() = 0; // Context shutdown: drop the completion without running it.

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
  // Commit a case. The error, if any, was written into error by the channel
  // operation that found the case ready; error is clear when a scan starts.
  void commit(std::size_t index) { winner.store(static_cast<std::ptrdiff_t>(index), std::memory_order_release); }
  // Request a turn; the caller holds a strong reference. A queued runner is a
  // continuation of the current handler: on a thread that is running the
  // owner's scheduler, Asio queues it on that thread's private queue, with no
  // scheduler lock, no wakeup signal and no migration to another worker. From
  // any other thread it is an ordinary post. The first start of a generation
  // may instead be dispatched (an awaiting coroutine has already suspended, so
  // the turn runs inline on the owner) or posted (a generic initiation keeps
  // its posted start). Whenever a runner is already scheduled or running, a
  // request only sets its bit and that runner serves it before exiting.
  void request(unsigned turn, launch how = launch::defer) {
    if (turns.fetch_or(turn | scheduled, std::memory_order_acq_rel) & scheduled) return;
    if (scheduler) {
      // Only the requester that set the scheduled bit reaches here, and the
      // scheduler's queue hands the reference to the thread that runs it.
      runner_ref = shared_from_this();
      if (how == launch::dispatch && scheduler->can_dispatch()) {
        auto keep = std::move(runner_ref);
        run_turns();
        return;
      }
      scheduler->post_immediate_completion(this, how != launch::post);
      return;
    }
    submit(how, owner, [self = shared_from_this()] { self->run_turns(); });
  }
  // Scheduler entry: run the turns, or at scheduler shutdown only drop the
  // self-reference, which may destroy the operation as the last action.
  static void run_scheduled(void* owner, asio::detail::scheduler_operation* base, const asio::error_code&, std::size_t) {
    auto* self = static_cast<operation*>(base);
    auto keep = std::move(self->runner_ref);
    if (owner) self->run_turns();
  }
  // Raise a generation mark; marks only grow, so a late request for an earlier
  // generation can never hide a request raised for the current one.
  static void raise(std::atomic<std::uint64_t>& mark, std::uint64_t gen) {
    auto seen = mark.load(std::memory_order_relaxed);
    while (seen < gen && !mark.compare_exchange_weak(seen, gen, std::memory_order_acq_rel)) {}
  }
  // Cancellation ingress from a stop callback or a cancellation slot, on any
  // thread, possibly while the operation is being destroyed: the mark is an
  // atomic member and the request needs an owner that still exists. A request
  // raised for an earlier generation is dropped.
  void request_cancel(std::uint64_t gen) {
    if (gen != generation.load(std::memory_order_acquire)) return;
    raise(cancel_generation, gen);
    if (auto self = weak_from_this().lock()) self->request(turn_cancel);
  }
  void timer_expired(std::uint64_t gen) {
    if (gen != generation.load(std::memory_order_acquire)) return;
    raise(resume_generation, gen);
    request(turn_resume);
  }
  // Memory orders: a request publishes its data with the acquire-release
  // fetch_or above; the runner's exchange acquires it. Releasing the runner
  // publishes the turn's writes to the next runner through its exchange; a
  // failed release only loops back to that exchange. The generation marks are
  // written before their requests, so relaxed loads inside a turn suffice.
  void run_turns() {
    for (;;) {
      auto requested = turns.exchange(scheduled, std::memory_order_acquire);
      auto gen = generation.load(std::memory_order_relaxed);
      if (requested & turn_start) initial_start();
      if ((requested & turn_cancel) && cancel_generation.load(std::memory_order_relaxed) == gen)
        cancel(asio::error::operation_aborted);
      if ((requested & turn_resume) && resume_generation.load(std::memory_order_relaxed) == gen)
        start(true);
      if ((requested & turn_finalize) && finalize_generation.load(std::memory_order_relaxed) == gen)
        finalize();
      if (requested & turn_release) release();
      auto expected = scheduled;
      if (turns.compare_exchange_strong(expected, 0, std::memory_order_release, std::memory_order_relaxed)) return;
    }
  }
  // A persistent registration ends on the runner, so it never races a start.
  void release() {
    if (self_owner) registration_owner->forget(this);
  }
  void initial_start() {
    auto gen = generation.load(std::memory_order_relaxed);
    if (started_generation == gen) return; // Already started, or abandoned.
    started_generation = gen;
    finished = false;
    winner.store(active, std::memory_order_relaxed);
    error = clear_error;
    initial_turn = true;
    // Installation and removal share the operation's runner. An already
    // stopped token may invoke request_cancel inline during emplacement.
    if (stop.stop_possible()) stop_callback.emplace(stop, cancel_request{this, gen});
    if (stop.stop_requested() || cancel_generation.load(std::memory_order_relaxed) == gen)
      cancel(asio::error::operation_aborted);
    else start();
    initial_turn = false;
  }
  // Wake a committed operation of the given generation. The completion handler
  // is still dispatched or queued by complete(); nothing of the user's runs
  // under a channel lock. Exactly one claim exists per generation, so a plain
  // store suffices for its mark.
  void notify(std::uint64_t gen) {
    finalize_generation.store(gen, std::memory_order_relaxed);
    request(turn_finalize);
  }
  void finalize() {
    if (finished) return;
    // The outcome is committed, so no peer can claim a remaining node; each
    // is withdrawn under its own channel mutex, and a claimed one needs none.
    withdraw_cases();
    finished = true;
    if (timer) timer->cancel();
    stop_callback.reset();
    if (self_owner && !persistent_registration) registration_owner->forget(this);
    complete(); // No channel mutex is held while posting user completion.
  }
  // Execution-context shutdown, with no runner able to run: withdraw any
  // registrations and drop the stored completion without invoking it.
  void abandon() {
    started_generation = generation.load(std::memory_order_relaxed);
    if (!finished) { auto locks = lock_channels(); remove(); finished = true; }
    if (timer) timer->cancel();
    stop_callback.reset();
    discard_completion();
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
    // Per-operation generator: no global selection lock or RNG lock. A fresh
    // permutation drawn by the arming task's probe serves the first start.
    if (!options.biased && !order_fresh) shuffle_order(order, options, random);
    order_fresh = false;
    bool ready = false;
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
        for (auto i : order) if (attempt(i)) { ready = true; break; }
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
    if (!self_owner) registration_owner->keep(shared_from_this());
    auto expiry = std::min(options.deadline, next_timer());
    if (expiry != asio::steady_timer::time_point::max()) {
      if (!timer) timer.emplace(owner);
      timer->expires_at(expiry);
      timer->async_wait([self = shared_from_this(), gen = generation.load(std::memory_order_relaxed)](asio::error_code ec) {
        if (!ec) self->timer_expired(gen);
      });
    }
  }
};
inline void pending_operations::keep(std::shared_ptr<operation> op) {
  auto* identity = op.get();
  identity->self_owner = std::move(op);
  identity->registered.store(true, std::memory_order_release);
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
  op->registered.store(false, std::memory_order_release);
  // The caller holds a reference; releasing the self reference here cannot
  // destroy the operation under the registry lock.
  op->self_owner.reset();
}
inline void pending_operations::shutdown() {
  operation* retired;
  { std::lock_guard<std::mutex> lock(mutex_); retired = std::exchange(first_, nullptr); }
  // Withdrawal and destruction may acquire channel locks. Never do them under
  // the registry lock, and never acquire this lock while holding a channel
  // lock. Abandoning drops a stored completion before the owner is released,
  // so a coroutine that owns a reusable selector is unwound by the scheduler
  // instead of keeping its own operation alive through it.
  while (retired) {
    auto* next = std::exchange(retired->registry_next, nullptr);
    retired->registry_prev = nullptr;
    retired->registered.store(false, std::memory_order_release);
    retired->abandon();
    retired->self_owner.reset();
    retired = next;
  }
}

template<class T> struct channel_state : channel_base {
  static_assert(std::is_nothrow_move_constructible_v<T>, "payload move must be noexcept");
  hinted<bool> closed;
  std::size_t limit;
  std::vector<std::optional<T>> buffer;
  std::size_t head = 0;
  hinted<std::size_t> size;
  wait_list sends, receives;
  channel_state(std::size_t capacity, std::size_t pending)
    : limit(pending), buffer(capacity) { sends.mutex = receives.mutex = &this->mutex; }
  // Lock-free readiness hints for a probe. A stale hint either sends a probe
  // under a mutex that finds nothing, or sends the task to the suspended path,
  // which decides again under every channel mutex; neither affects
  // correctness. The hint only spares the mutex when nothing can be ready.
  bool may_receive() const { return size.get() > 0 || sends.count.get() > 0 || closed.get(); }
  bool may_send() const { return closed.get() || receives.count.get() > 0 || size.get() < buffer.size(); }

  // Claim the first waiting operation that can still be selected. A node is
  // unlinked first; its owning operation is alive and owned while this mutex
  // is held (see wait_link), so it is claimed and woken through the pointer.
  operation* take(wait_list& queue, wait_node<T>*& selected) {
    while (queue.head) {
      auto* node = static_cast<wait_node<T>*>(queue.head);
      queue.unlink(*node);
      if (node->identity->claim(node->index)) { selected = node; return node->identity; }
    }
    return nullptr;
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
  // Readiness under the mutex. The error is written only when a ready case
  // carries one; a waiting peer's error is clear while it waits.
  bool send(std::optional<T>& value, asio::error_code& ec) {
    if (closed) { ec = asio::error::broken_pipe; return true; }
    wait_node<T>* w = nullptr;
    if (auto op = take(receives, w)) {
      w->value->emplace(std::move(*value)); value.reset();
      op->notify(w->generation); return true;
    }
    if (size < buffer.size()) { push(value); return true; }
    return false;
  }
  bool receive(std::optional<T>& value, asio::error_code& ec) {
    wait_node<T>* w = nullptr;
    if (size) {
      pop(value);
      if (!closed) if (auto op = take(sends, w)) {
        push(*w->value); op->notify(w->generation);
      }
      return true;
    }
    if (auto op = take(sends, w)) {
      value.emplace(std::move(**w->value)); w->value->reset();
      op->notify(w->generation); return true;
    }
    if (closed) { ec = asio::error::eof; return true; }
    return false;
  }
  void close() {
    std::lock_guard<std::mutex> lock(this->mutex);
    if (closed) return;
    closed = true;
    wait_node<T>* w = nullptr;
    while (auto op = take(sends, w)) { op->error = asio::error::broken_pipe; op->notify(w->generation); }
    while (auto op = take(receives, w)) {
      if (size) pop(*w->value);
      else op->error = asio::error::eof;
      op->notify(w->generation);
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
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->closed) return false;
    std::optional<T> payload;
    // Check readiness before moving from the caller.
    channel_select_detail::wait_node<T>* w = nullptr;
    if (auto op = state_->take(state_->receives, w)) {
      w->value->emplace(std::move(value)); op->notify(w->generation); return true;
    }
    if (state_->size == state_->buffer.size()) return false;
    payload.emplace(std::move(value)); state_->push(payload); return true;
  }
  std::optional<T> try_receive(asio::error_code& ec) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    std::optional<T> result;
    ec = channel_select_detail::clear_error;
    if (!state_->receive(result, ec)) ec = asio::error::would_block;
    return result;
  }
  std::size_t pending() const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->sends.count.get() + state_->receives.count.get();
  }
  std::size_t buffered() const {
    std::lock_guard<std::mutex> lock(state_->mutex); return state_->size.get();
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
  // A send case takes part while it holds a payload; after a transfer, a
  // reusable selector's send case waits for its next payload.
  bool eligible() const { return enabled && (!Sending || value.has_value()); }
  // Lock-free pre-check for a probe; see channel_state::may_receive.
  bool maybe_ready() const { return eligible() && (Sending ? channel->may_send() : channel->may_receive()); }
  template<class Decision> bool attempt(Decision& op, std::size_t index) {
    if (!eligible()) return false;
    bool ready;
    if constexpr (Sending) ready = channel->send(value, op.error);
    else ready = channel->receive(value, op.error);
    if (ready) op.commit(index);
    return ready;
  }
  bool register_wait(node_type& node, channel_select_detail::operation& op, std::size_t index) {
    if (!eligible()) return true;
    if (channel->sends.count.get() + channel->receives.count.get() >= channel->limit) return false;
    node.identity = &op; node.index = index; node.value = &value;
    node.generation = op.generation.load(std::memory_order_relaxed);
    (Sending ? channel->sends : channel->receives).push_back(node);
    return true;
  }
  // A reusable selector clears a receive slot before each wait; a send payload
  // stays until it is transferred.
  void reset_for_wait() { if constexpr (!Sending) value.reset(); }
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
  bool maybe_ready() const { return enabled; }
  template<class Decision> bool attempt(Decision& op, std::size_t index) {
    if (!enabled || time > asio::steady_timer::clock_type::now()) return false;
    value.emplace(); op.commit(index); return true;
  }
  bool register_wait(node_type&, channel_select_detail::operation&, std::size_t) { return true; }
  void reset_for_wait() { value.reset(); }
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
  asio::error_code error = clear_error;
  std::size_t index = static_cast<std::size_t>(-1);
  void commit(std::size_t selected) { index = selected; }
};
template<class... Cases>
bool try_ready(std::tuple<Cases...>& cases, const channel_select_options& options,
    const std::stop_token& stop, bool cancelled, ready_decision& decision, std::span<const std::size_t> order) {
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
        // A case whose hints show nothing to do is passed over without its mutex.
        if (!selected.maybe_ready()) return false;
        std::unique_lock<std::mutex> lock;
        if (auto* channel = selected.base()) lock = std::unique_lock<std::mutex>(channel->mutex);
        return selected.attempt(decision, index);
      };
      return ((current++ == index && attempt(c)) || ...);
    }, cases);
    if (ready) return true;
  }
  if (options.nonblocking) { decision.error = asio::error::would_block; return true; }
  return false;
}
template<class... Cases>
bool try_ready(std::tuple<Cases...>& cases, const channel_select_options& options,
    const std::stop_token& stop, bool cancelled, ready_decision& decision) {
  constexpr auto count = sizeof...(Cases);
  std::array<std::size_t, count> order;
  std::iota(order.begin(), order.end(), 0);
  if (!options.biased) shuffle_order(order, options);
  return try_ready(cases, options, stop, cancelled, decision, order);
}
// Bound uninterrupted ready work on each worker; no readiness polling is used.
inline bool ready_budget_exhausted() {
  thread_local unsigned used = 0;
  if (++used < 64) return false;
  used = 0; return true;
}
// The owned case set and its wait registrations, shared by the one-shot
// operation and the reusable selector state. A one-shot operation locks the
// channels of its enabled cases; a reusable state may enable a case between
// waits, so it locks every case's channel.
template<class... Cases> struct cases_operation : operation {
  std::tuple<Cases...> cases;
  std::tuple<typename Cases::node_type...> nodes; // One wait registration per case.
  cases_operation(asio::any_io_executor ex, channel_select_options opts, std::tuple<Cases...>&& owned,
      bool every_channel = false)
    : operation(std::move(ex), opts, sizeof...(Cases)), cases(std::move(owned)) {
    std::size_t used = 0;
    std::apply([&](auto&... c) {
      auto add = [&](auto& one) { if (auto* b = every_channel ? one.keep().get() : one.base()) channels[used++] = b; };
      (add(c), ...);
    }, cases);
    channels = channels.first(used);
    std::sort(channels.begin(), channels.end(), std::less<channel_base*>());
    channels = channels.first(std::unique(channels.begin(), channels.end()) - channels.begin());
  }
  ~cases_operation() override {
    // Execution-context teardown may destroy the timer handler without running
    // finalize. Withdraw registrations before destroying case storage. The
    // case tuple owns every channel until completion moves it out; a completed
    // destructor must not touch raw channel bases.
    if (!finished) { auto locks = lock_channels(); remove(); }
  }
  template<class F, std::size_t... I> bool visit(std::size_t index, F f, std::index_sequence<I...>) {
    return ((index == I && f(std::get<I>(cases), std::get<I>(nodes))) || ...);
  }
  bool attempt(std::size_t index) override {
    return visit(index, [&](auto& c, auto&) { return c.attempt(*this, index); }, std::index_sequence_for<Cases...>());
  }
  bool register_case(std::size_t index) override {
    return visit(index, [&](auto& c, auto& node) { return c.register_wait(node, *this, index); },
        std::index_sequence_for<Cases...>());
  }
  void unlink_cases() override {
    std::apply([&](auto&... node) { (node.unlink(this), ...); }, nodes);
  }
  void withdraw_cases() override {
    std::apply([&](auto&... node) { (node.withdraw(this), ...); }, nodes);
  }
  asio::steady_timer::time_point next_timer() const override {
    auto time = asio::steady_timer::time_point::max();
    std::apply([&](const auto&... c) { ((time = std::min(time, c.expiry())), ...); }, cases);
    return time;
  }
  // A completion may run inline (dispatch) once the awaiting coroutine has
  // suspended and the first turn is over; otherwise it is queued as a
  // continuation of the current turn, like a peer's wakeup in notify(). On
  // one worker this keeps the order between a peer's withdrawal and this
  // completion as it was with posted wakeups.
  bool completion_inline() const { return dispatch_completion && !initial_turn; }
};
template<class Executor, class Completion>
void schedule_completion(const Executor& ex, bool inline_now, Completion&& completion) {
  submit_fast(inline_now ? launch::dispatch : launch::defer, ex, std::move(completion));
}
template<class Handler, class... Cases> struct select_op : cases_operation<Cases...> {
  Handler handler;
  asio::executor_work_guard<asio::any_io_executor> completion_work;
  select_op(asio::any_io_executor ex, channel_select_options opts, Handler h, std::tuple<Cases...>&& cases)
    : cases_operation<Cases...>(ex, opts, std::move(cases)), handler(std::move(h)),
      completion_work(asio::any_io_executor(asio::get_associated_executor(handler, ex))) {}
  void complete() override {
    auto w = this->winner.load(std::memory_order_acquire);
    channel_select_result<Cases...> result(w >= 0 ? static_cast<std::size_t>(w)
        : channel_select_result<Cases...>::npos, std::move(this->cases));
    auto completion_ex = completion_work.get_executor();
    schedule_completion(completion_ex, this->completion_inline(),
      [h = std::move(handler), ec = this->error, r = std::move(result), work = std::move(completion_work)]() mutable {
        h(ec, std::move(r));
      });
  }
  void discard_completion() override {} // The handler dies with the operation.
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
    if (type != asio::cancellation_type::none) if (auto p = weak.lock()) p->request_cancel(1);
  });
  submit_fast(coroutine_start ? launch::dispatch : launch::post, op->owner, [op] { op->run_turns(); });
}
// Cancellation already recorded in an awaiting coroutine's cancellation state.
// Other handlers expose no such state; they rely on the stop token and on the
// slot ingress flag that the suspended operation installs.
template<class Handler> bool cancellation_requested(const Handler& handler) noexcept {
  if constexpr (requires { handler.get_cancellation_state().cancelled(); })
    return handler.get_cancellation_state().cancelled() != asio::cancellation_type::none;
  else return false;
}
// The handler an awaiting asio::awaitable coroutine hands to an operation: the
// detached coroutine thread and a pointer to its result storage.
template<class Handler> struct is_awaitable_handler : std::false_type {};
template<class Signature, class Executor, class Enable>
struct is_awaitable_handler<asio::detail::awaitable_async_op_handler<Signature, Executor, Enable>> : std::true_type {};
// Any other completion handler of a reusable selector, type-erased on the
// heap together with the executor work it needs until it has run.
struct generic_completion {
  asio::any_completion_handler<void(asio::error_code, std::size_t)> handler;
  asio::executor_work_guard<asio::any_io_executor> work;
};
struct generic_box {
  std::unique_ptr<generic_completion> owned;
  asio::any_io_executor get_executor() const { return owned->work.get_executor(); }
  void operator()(asio::error_code ec, std::size_t index) { std::move(owned->handler)(ec, index); }
};
// In-place storage for the one completion a reusable selector holds while a
// wait is in progress. An awaiting coroutine's handler is two words for any
// executor type and is stored without allocation; it owns the suspended
// coroutine until it runs or is discarded. The slot is written by the task
// arming a wait and consumed inside a runner turn.
struct completion_slot {
  static constexpr std::size_t capacity = 2 * sizeof(void*);
  alignas(void*) unsigned char storage[capacity];
  void (*complete_fn)(void*, asio::error_code, std::size_t, bool, const asio::io_context::executor_type*) = nullptr;
  void (*discard_fn)(void*) = nullptr;
  completion_slot() = default;
  completion_slot(const completion_slot&) = delete;
  completion_slot& operator=(const completion_slot&) = delete;
  ~completion_slot() { discard(); }
  template<class H> static void invoke(H& h, asio::error_code ec, std::size_t index) {
    if constexpr (std::is_invocable_v<H&, asio::error_code, std::size_t>) h(ec, index);
    else h(std::tuple<asio::error_code, std::size_t>(ec, index));
  }
  // The completion runs on the handler's executor; when that is known to be
  // the owner's plain io_context executor it is handed to it directly.
  template<class H> static void complete_with(void* p, asio::error_code ec, std::size_t index, bool inline_now,
      const asio::io_context::executor_type* typed) {
    H h(std::move(*static_cast<H*>(p)));
    static_cast<H*>(p)->~H();
    if (typed) {
      submit(inline_now ? launch::dispatch : launch::defer, *typed,
          [h = std::move(h), ec, index]() mutable { invoke(h, ec, index); });
    } else {
      auto ex = h.get_executor(); // Before the move below.
      schedule_completion(ex, inline_now, [h = std::move(h), ec, index]() mutable { invoke(h, ec, index); });
    }
  }
  template<class H> static void discard_with(void* p) { static_cast<H*>(p)->~H(); }
  template<class H> void emplace(H h) {
    static_assert(sizeof(H) <= capacity && alignof(H) <= alignof(void*), "a stored completion must fit in place");
    new (static_cast<void*>(storage)) H(std::move(h));
    complete_fn = &complete_with<H>; discard_fn = &discard_with<H>;
  }
  void complete(asio::error_code ec, std::size_t index, bool inline_now, const asio::io_context::executor_type* typed) {
    auto fn = std::exchange(complete_fn, nullptr);
    discard_fn = nullptr;
    fn(storage, ec, index, inline_now, typed);
  }
  void discard() {
    if (auto fn = std::exchange(discard_fn, nullptr)) { complete_fn = nullptr; fn(storage); }
  }
};
// Task-owned reusable selection state. It keeps the owning case handles, one
// wait registration per case and the stored completion for the wait in
// progress, and runs every generation through the same serial turn runner.
// A generation is armed only by the owning task, after the previous one has
// withdrawn every registration, cancelled its timer and released its
// completion; its cancellation, timer and wakeup requests carry its number.
template<class... Cases> struct reusable_state : cases_operation<Cases...> {
  using decision_type = std::tuple<asio::error_code, std::size_t>;
  static constexpr std::size_t npos = static_cast<std::size_t>(-1);
  completion_slot completion;
  decision_type decision; // An immediate completion resumes from here.
  bool busy = false; // Owning task only: a wait is being probed or is in progress.
  reusable_state(asio::any_io_executor ex, channel_select_options opts, std::stop_token token,
      std::tuple<Cases...>&& owned)
    : cases_operation<Cases...>(std::move(ex), opts, std::move(owned), true) {
    this->stop = std::move(token);
    this->finished = true; // Idle until a wait is armed.
    this->persistent_registration = true;
  }
  // Begin a wait on the owning task: a second wait while one is in progress
  // is a precondition violation, and receive slots start empty.
  void begin_wait() {
    if (busy) asio::detail::throw_exception(std::logic_error("channel_selector: a wait is already in progress"));
    busy = true;
    std::apply([](auto&... c) { (c.reset_for_wait(), ...); }, this->cases);
  }
  // Ready probe in the awaiting coroutine's frame, under the same rules as the
  // one-shot lazy operation: only on the owner, and never past the budget.
  template<class Executor>
  bool probe(const Executor& current, asio::cancellation_type_t cancelled, bool& yield) {
    this->completion_on_owner = this->owner == current;
    if (!this->completion_on_owner) return false;
    if (ready_budget_exhausted()) { yield = true; return false; }
    return probe_now(cancelled != asio::cancellation_type::none);
  }
  bool probe_now(bool cancelled) {
    // The permutation is drawn into the state's own order, so a start that
    // follows an unsuccessful probe at once reuses it.
    if (!this->options.biased) shuffle_order(this->order, this->options, this->random);
    this->order_fresh = true;
    ready_decision d;
    if (!try_ready(this->cases, this->options, this->stop, cancelled, d, this->order)) return false;
    decision = decision_type(d.error, d.index);
    busy = false;
    return true;
  }
  // Arm the next generation with its completion. Runs on the task's thread,
  // after the coroutine suspended (awaited) or inside a generic initiation.
  template<class Handler> void arm(Handler handler, bool awaited) {
    auto slot = asio::get_associated_cancellation_slot(handler);
    auto gen = this->generation.load(std::memory_order_relaxed) + 1;
    if constexpr (is_awaitable_handler<Handler>::value) {
      completion.emplace(std::move(handler));
      // The awaiting coroutine's slot is cleared when it resumes through the
      // stored completion, and this state outlives the wait, so the slot
      // handler refers to it directly.
      if (slot.is_connected()) slot.assign(operation::cancel_request{this, gen});
    } else {
      auto ex = asio::any_io_executor(asio::get_associated_executor(handler, this->owner));
      completion.emplace(generic_box{std::make_unique<generic_completion>(
          generic_completion{std::move(handler), asio::make_work_guard(ex)})});
      this->completion_on_owner = false;
      // A generic handler's slot belongs to the caller and may outlive this state.
      if (slot.is_connected()) slot.assign([weak = this->weak_from_this(), gen](asio::cancellation_type_t type) {
        if (type != asio::cancellation_type::none) if (auto p = weak.lock()) p->request_cancel(gen);
      });
    }
    this->generation.store(gen, std::memory_order_release);
    this->dispatch_completion = awaited;
    this->request(operation::turn_start, awaited ? launch::dispatch : launch::post);
  }
  void complete() override {
    auto w = this->winner.load(std::memory_order_acquire);
    busy = false; // The task may arm the next wait from inside this completion.
    completion.complete(this->error, w >= 0 ? static_cast<std::size_t>(w) : npos, this->completion_inline(),
        this->completion_on_owner ? this->owner_io : nullptr);
  }
  void discard_completion() override { completion.discard(); }
};
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
      channel_select_detail::submit_fast(channel_select_detail::launch::post, executor,
          [self = std::move(*this), handler = std::move(handler)]() mutable {
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

// One wait of a reusable selector: a lazy operation completing with the error
// and the selected case index (void(error_code, size_t), which throws on
// error when awaited; or the as_tuple form void(tuple<error_code, size_t>)).
// Awaiting it from an asio::awaitable coroutine on the selector's executor
// first probes readiness in the coroutine's frame; a committed probe arms no
// generation and touches no registration. Otherwise the coroutine suspends,
// its handler is stored in the selector in place, and the next generation
// starts on the owner. Any other completion token stores a type-erased
// handler and keeps the posted start of generic initiations.
template<bool Tuple, class... Cases> class channel_selector_wait {
  using state_type = channel_select_detail::reusable_state<Cases...>;
public:
  using decision_type = std::tuple<asio::error_code, std::size_t>;
  using signature = std::conditional_t<Tuple, void(decision_type), void(asio::error_code, std::size_t)>;
  explicit channel_selector_wait(state_type& state) noexcept : state_(&state) {}
  template<ASIO_COMPLETION_TOKEN_FOR(signature) CompletionToken>
  auto operator()(CompletionToken&& token) && {
    return asio::async_initiate<CompletionToken, signature>(initiation{std::move(*this)}, token);
  }
  // Immediate completion in the awaiting frame (awaitable_async_op_immediate).
  template<class Frame, class Pointers>
  bool try_immediate(Frame* frame, Pointers& pointers) {
    awaited_ = true;
    auto& s = *state_;
    s.begin_wait();
    if (!s.probe(frame->executor(),
          frame->await_transform(asio::this_coro::cancellation_state).await_resume().cancelled(), yield_))
      return false;
    if constexpr (Tuple) pointers = &s.decision;
    else { pointers.disposition_ = &std::get<0>(s.decision); pointers.value_ = &std::get<1>(s.decision); }
    return true;
  }
private:
  struct initiation {
    channel_selector_wait self;
    template<class Handler> void operator()(Handler handler) && { std::move(self).start(std::move(handler)); }
  };
  template<class Handler> static auto adapt(Handler handler) {
    if constexpr (Tuple && !channel_select_detail::is_awaitable_handler<Handler>::value)
      return asio::detail::as_tuple_handler<Handler>(std::move(handler));
    else return handler;
  }
  template<class Handler> void start(Handler handler) && {
    auto& s = *state_;
    if (!awaited_) s.begin_wait();
    if (yield_) {
      // The worker's ready budget is exhausted: already queued work runs
      // first, then the probe repeats on the owner before any registration.
      auto owner = s.owner;
      channel_select_detail::submit_fast(channel_select_detail::launch::post, owner,
          [keep = std::static_pointer_cast<state_type>(s.shared_from_this()), handler = std::move(handler)]() mutable {
        auto& state = *keep;
        if (state.probe_now(channel_select_detail::cancellation_requested(handler))) {
          if constexpr (Tuple) std::move(handler)(decision_type(state.decision));
          else std::move(handler)(std::get<0>(state.decision), std::get<1>(state.decision));
        } else state.arm(adapt(std::move(handler)), true);
      });
      return;
    }
    s.arm(adapt(std::move(handler)), awaited_);
  }
  state_type* state_;
  bool awaited_ = false; // Initiation runs after the awaiting coroutine suspended.
  bool yield_ = false;   // A cooperative executor turn precedes the next probe.
};
} // namespace experimental
namespace detail {
template<bool Tuple, class... Cases>
struct awaitable_async_op_immediate<experimental::channel_selector_wait<Tuple, Cases...>> {
  template<class Frame, class Result>
  static bool try_complete(experimental::channel_selector_wait<Tuple, Cases...>& op, Frame* frame, Result& result) {
    return op.try_immediate(frame, result);
  }
};
} // namespace detail
namespace experimental {

// A task-owned reusable selector over a fixed set of cases. It owns its
// channel handles, case values and wait registrations for its whole lifetime,
// so a loop that selects repeatedly creates no operation per iteration: each
// wait reuses the same state and resumes the task with the selected index.
// The selected case's value is read through value<I>(); a send case takes
// part while it holds a payload and consumes it when selected; receive slots
// are cleared when the next wait begins. Cases, options and payloads may be
// changed between waits only. One task uses a selector at a time, and the
// selector outlives its waits; a second wait while one is in progress throws
// std::logic_error. A selector may be destroyed after its execution context
// only while no wait is in progress.
template<class... Cases> class channel_selector {
  static_assert(sizeof...(Cases) > 0, "a selector requires at least one case");
  using state_type = channel_select_detail::reusable_state<Cases...>;
  std::shared_ptr<state_type> state_;
public:
  static constexpr std::size_t npos = static_cast<std::size_t>(-1);
  using decision_type = std::tuple<asio::error_code, std::size_t>;
  template<class... Init>
    requires (sizeof...(Init) == sizeof...(Cases) && (std::is_constructible_v<Cases, Init&&> && ...))
  channel_selector(asio::any_io_executor ex, channel_select_options options, std::stop_token stop, Init&&... cases)
    : state_(std::make_shared<state_type>(std::move(ex), options, std::move(stop),
        std::tuple<Cases...>(std::forward<Init>(cases)...))) {}
  channel_selector(channel_selector&&) noexcept = default;
  channel_selector& operator=(channel_selector&& other) noexcept { release(); state_ = std::move(other.state_); return *this; }
  channel_selector(const channel_selector&) = delete;
  channel_selector& operator=(const channel_selector&) = delete;
  // The execution context's registry may still keep the state; a runner turn
  // on the owner forgets it, serialized with any wait that is in progress.
  ~channel_selector() { release(); }
  // Lazy waits. co_await selector.wait() yields the selected index and throws
  // on error; wait(as_tuple(deferred)) yields tuple<error_code, size_t>.
  auto wait() noexcept { return channel_selector_wait<false, Cases...>(*state_); }
  auto wait(asio::deferred_t) noexcept { return wait(); }
  auto wait(asio::as_tuple_t<asio::deferred_t>) noexcept { return channel_selector_wait<true, Cases...>(*state_); }
  auto wait(asio::partial_as_tuple) noexcept { return channel_selector_wait<true, Cases...>(*state_); }
  template<class CompletionToken> auto wait(CompletionToken&& token) {
    return wait()(std::forward<CompletionToken>(token));
  }
  template<std::size_t I> auto& value() noexcept { return std::get<I>(state_->cases).value; }
  template<std::size_t I> auto& case_at() noexcept { return std::get<I>(state_->cases); }
  channel_select_options& options() noexcept { return state_->options; }
  const asio::any_io_executor& get_executor() const noexcept { return state_->owner; }
private:
  void release() noexcept {
    if (state_ && state_->registered.load(std::memory_order_acquire))
      state_->request(channel_select_detail::operation::turn_release);
  }
};
template<class... Cases> auto make_selector(asio::any_io_executor ex, channel_select_options options,
    std::stop_token stop, Cases... cases) {
  return channel_selector<Cases...>(std::move(ex), options, std::move(stop), std::move(cases)...);
}
} // namespace experimental
ASIO_INLINE_NAMESPACE_END
} // namespace asio
#endif
