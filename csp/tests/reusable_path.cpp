// Mechanism and lifetime controls for the task-owned reusable selector.
//
// A loop that selects repeatedly through one channel_selector constructs one
// owned operation for its whole lifetime and allocates nothing per
// suspension; the one-shot API constructs one owned operation per suspended
// selection. Generation isolation is exercised directly: cancellation,
// timer and wakeup requests raised for a finished generation reach the next
// one neither before nor while it waits. Teardown with a suspended wait,
// simultaneous use, executor work, payload ownership and release of the
// registry registration are checked without timing thresholds.
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <new>
#include <optional>
#include <thread>
#include <vector>
// Test-only observers; operations are constructed on several threads in the
// cross-thread scenario, so they are synchronized here.
std::mutex observer_mutex;
std::vector<void*> constructed_operations;
void observe_operation(void* op) { std::lock_guard<std::mutex> lock(observer_mutex); constructed_operations.push_back(op); }
std::size_t observed_operations() { std::lock_guard<std::mutex> lock(observer_mutex); return constructed_operations.size(); }
void* last_observed_operation() { std::lock_guard<std::mutex> lock(observer_mutex); return constructed_operations.back(); }
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_OPERATION(op) observe_operation(static_cast<void*>(op))
#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
using namespace asio::experimental;
using operation_type = channel_select_detail::operation;
std::atomic<std::size_t> heap_allocations{0};
void* operator new(std::size_t size) {
  ++heap_allocations;
  if (void* p = std::malloc(size ? size : 1)) return p;
  throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
constexpr int rounds = 256;
#if !defined(ONE_SHOT_ONLY)
#define HAVE_REUSABLE 1
#endif

#if defined(HAVE_REUSABLE)
// Producer and consumer suspend alternately on an unbuffered channel: every
// round is a suspended wait on each side.
asio::awaitable<void> reusable_producer(select_channel<int>& out) {
  auto ex = co_await asio::this_coro::executor;
  auto sel = make_selector(ex, {}, {}, send(out, 0));
  for (int i = 1; i <= rounds; ++i) {
    sel.value<0>().emplace(i);
    auto index = co_await sel.wait();
    assert(index == 0 && !sel.value<0>()); // The payload was transferred.
  }
}
asio::awaitable<void> reusable_producer_from_zero(select_channel<int>& out) {
  auto ex = co_await asio::this_coro::executor;
  auto sel = make_selector(ex, {}, {}, send(out, 0));
  for (int i = 0; i < rounds; ++i) {
    sel.value<0>().emplace(i);
    auto index = co_await sel.wait();
    assert(index == 0 && !sel.value<0>());
  }
}
// Counted from the second round on, when both tasks' selectors exist and the
// first registrations have warmed the per-thread caches.
asio::awaitable<void> reusable_consumer(select_channel<int>& in, select_channel<int>& idle,
    std::size_t& operations, std::size_t& allocations) {
  auto ex = co_await asio::this_coro::executor;
  auto sel = make_selector(ex, {}, {}, receive(in), receive(idle));
  auto before_ops = observed_operations(); auto before_allocs = heap_allocations.load();
  for (int i = 1; i <= rounds; ++i) {
    auto [ec, index] = co_await sel.wait(asio::as_tuple(asio::deferred));
    assert(!ec && index == 0 && *sel.value<0>() == i && !sel.value<1>());
    if (i == 1) { before_ops = observed_operations(); before_allocs = heap_allocations.load(); }
  }
  allocations = heap_allocations.load() - before_allocs;
  operations = observed_operations() - before_ops;
}
#endif
asio::awaitable<void> one_shot_producer(select_channel<int>& out) {
  auto ex = co_await asio::this_coro::executor;
  for (int i = 1; i <= rounds; ++i) {
    auto [ec, r] = co_await make_select(send(out, i)).async_wait(ex, {}, {}, asio::as_tuple(asio::deferred));
    assert(!ec && r.index == 0);
  }
}
asio::awaitable<void> one_shot_consumer(select_channel<int>& in, select_channel<int>& idle, std::size_t& operations) {
  auto ex = co_await asio::this_coro::executor;
  auto before = observed_operations();
  for (int i = 1; i <= rounds; ++i) {
    auto [ec, r] = co_await make_select(receive(in), receive(idle)).async_wait(ex, {}, {}, asio::as_tuple(asio::deferred));
    assert(!ec && r.index == 0 && *r.value<0>() == i);
    if (i == 1) before = observed_operations();
  }
  operations = observed_operations() - before;
}
template<class Spawn> void run_pair(Spawn spawn) {
  asio::io_context ctx; std::exception_ptr failure;
  auto done = [&](std::exception_ptr e) { if (e && !failure) failure = e; };
  spawn(ctx, done);
  ctx.run();
  if (failure) std::rethrow_exception(failure);
}
void mechanism() {
  select_channel<int> pipe(0), idle(0);
  std::size_t one_shot_operations = 0;
  run_pair([&](asio::io_context& ctx, auto done) {
    asio::co_spawn(ctx, one_shot_consumer(pipe, idle, one_shot_operations), done);
    asio::co_spawn(ctx, one_shot_producer(pipe), done);
  });
  assert(pipe.pending() == 0 && idle.pending() == 0);
  std::size_t operations = one_shot_operations, allocations = 0;
#if defined(HAVE_REUSABLE)
  run_pair([&](asio::io_context& ctx, auto done) {
    asio::co_spawn(ctx, reusable_consumer(pipe, idle, operations, allocations), done);
    asio::co_spawn(ctx, reusable_producer(pipe), done);
  });
  assert(pipe.pending() == 0 && idle.pending() == 0);
#endif
  std::cout << "rounds=" << rounds << " one_shot_loop_operations=" << one_shot_operations
            << " selector_loop_operations=" << operations << " selector_loop_heap_allocations=" << allocations
            << '\n' << std::flush;
  // Mechanism bounds, not timing: the one-shot loop constructs an owned
  // operation per suspended selection, on whichever side suspends; the
  // selector loop constructs none and allocates nothing from the heap.
  assert(one_shot_operations >= rounds - 2);
  assert(operations == 0);
  assert(allocations == 0);
}
#if defined(HAVE_REUSABLE)
operation_type* last_operation() { return static_cast<operation_type*>(last_observed_operation()); }
// Stale requests from a finished generation must not reach the next one.
asio::awaitable<void> isolation(select_channel<int>& in, bool& done) {
  auto ex = co_await asio::this_coro::executor;
  auto far = asio::steady_timer::clock_type::now() + std::chrono::hours(1);
  auto sel = make_selector(ex, {}, {}, receive(in), after(far));
  auto* op = last_operation();
  auto finished = op->generation.load(); // The generation the first wait will arm.
  asio::post(ex, [&] { assert(in.try_send(1)); });
  auto first = co_await sel.wait();
  assert(first == 0 && *sel.value<0>() == 1 && !sel.value<1>());
  finished = op->generation.load();
  // Raised after that generation finished, before the next one is armed.
  op->request_cancel(finished);
  op->timer_expired(finished);
  op->notify(finished);
  asio::post(ex, [&] { assert(in.try_send(2)); });
  auto second = co_await sel.wait();
  assert(second == 0 && *sel.value<0>() == 2);
  assert(op->generation.load() == finished + 1);
  // Raised while the next generation is registered and waiting, with a
  // second waiter registered behind it. A stale cancellation would abort the
  // wait; a stale wakeup would complete it with no case; a stale timer
  // resume would withdraw and re-register it behind the second waiter and
  // hand it the second value instead of the first.
  bool other_delivered = false;
  asio::post(ex, [&, finished] {
    assert(in.pending() == 1);
    make_select(receive(in)).async_wait(ex, {}, {}, [&](asio::error_code ec, auto r) {
      assert(!ec && *r.template value<0>() == 4); other_delivered = true;
    });
    asio::post(ex, [&, finished] { // Runs after that selection's posted start turn.
      assert(in.pending() == 2);
      op->request_cancel(finished);
      op->timer_expired(finished);
      op->notify(finished);
      // Deferred like a runner turn, so it runs after any turn those requests queued.
      asio::defer(ex, [&] { assert(in.pending() == 2 && in.try_send(3) && in.try_send(4)); });
    });
  });
  auto third = co_await sel.wait();
  assert(third == 0 && *sel.value<0>() == 3);
  while (!other_delivered) co_await asio::post(ex, asio::deferred);
  // A request for the current generation does act: cancellation reaches the wait.
  auto current = op->generation.load() + 1;
  asio::post(ex, [&, current] { assert(in.pending() == 1); op->request_cancel(current); });
  auto [ec, index] = co_await sel.wait(asio::as_tuple(asio::deferred));
  assert(ec == asio::error::operation_aborted && index == sel.npos && in.pending() == 0);
  // The selector is reusable after cancellation, including the timer case.
  sel.case_at<1>().time = asio::steady_timer::clock_type::now();
  auto timed = co_await sel.wait();
  assert(timed == 1 && sel.value<1>() && !sel.value<0>());
  done = true;
}
asio::awaitable<void> second_user(channel_selector<channel_select_case<int, false>>& sel, bool& threw) {
  try { (void)co_await sel.wait(); }
  catch (const std::logic_error&) { threw = true; }
}
void simultaneous_use_and_work() {
  asio::io_context ctx; std::optional<select_channel<int>> channel(std::in_place, 0); auto& in = *channel;
  bool threw = false, first_done = false; std::exception_ptr failure;
  auto sel = std::make_unique<channel_selector<channel_select_case<int, false>>>(ctx.get_executor(), channel_select_options{},
      std::stop_token{}, receive(in));
  asio::co_spawn(ctx, [&]() -> asio::awaitable<void> {
    auto index = co_await sel->wait();
    assert(index == 0 && *sel->value<0>() == 5); first_done = true;
  }, [&](std::exception_ptr e) { if (e) failure = e; });
  ctx.poll();
  assert(!ctx.stopped() && in.pending() == 1); // A suspended wait keeps the context's work.
  asio::co_spawn(ctx, second_user(*sel, threw), [&](std::exception_ptr e) { if (e) failure = e; });
  ctx.poll();
  if (failure) std::rethrow_exception(failure);
  assert(threw && in.pending() == 1 && !first_done);
  assert(in.try_send(5)); ctx.run();
  if (failure) std::rethrow_exception(failure);
  assert(first_done && in.pending() == 0);
  // Release of the persistent registration runs on the owner; the state is
  // then owned by nothing and the channel it held is released with it.
  std::weak_ptr<void> state = in.state();
  ctx.restart(); sel.reset(); ctx.run();
  channel.reset();
  assert(state.expired());
}
void teardown_with_suspended_wait() {
  std::optional<select_channel<int>> channel(std::in_place, 0); auto& a = *channel; bool destroyed = false;
  std::weak_ptr<void> state = a.state();
  struct probe { bool& flag; ~probe() { flag = true; } };
  {
    asio::io_context ctx;
    asio::co_spawn(ctx, [&]() -> asio::awaitable<void> {
      probe p{destroyed};
      auto ex = co_await asio::this_coro::executor;
      auto sel = make_selector(ex, {}, {}, receive(a), after(asio::steady_timer::clock_type::now() + std::chrono::hours(1)));
      co_await sel.wait();
      assert(false && "a wait abandoned at shutdown never resumes");
    }, asio::detached);
    ctx.poll();
    assert(a.pending() == 1 && !destroyed);
  }
  assert(destroyed && a.pending() == 0);
  // A selector destroyed after its context stopped running, then the context.
  {
    asio::io_context ctx;
    auto sel = std::make_unique<channel_selector<channel_select_case<int, false>>>(ctx.get_executor(),
        channel_select_options{}, std::stop_token{}, receive(a));
    bool got = false;
    sel->wait([&](asio::error_code ec, std::size_t index) { assert(!ec && index == 0); got = true; });
    assert(ctx.run_one() == 1 && a.pending() == 1 && a.try_send(8)); ctx.run();
    assert(got);
    sel.reset(); // The release turn is queued on a context nobody runs again.
  }
  channel.reset();
  assert(state.expired());
}
asio::awaitable<void> payloads(bool& done) {
  auto ex = co_await asio::this_coro::executor;
  select_channel<std::unique_ptr<int>> out(1); select_channel<int> in(1);
  channel_select_options biased; biased.biased = true;
  auto sel = make_selector(ex, biased, {}, send(out, std::unique_ptr<int>()), receive(in));
  sel.value<0>().reset(); // A send case without a payload takes no part.
  assert(in.try_send(1));
  auto first = co_await sel.wait();
  assert(first == 1 && *sel.value<1>() == 1 && out.buffered() == 0);
  sel.value<0>() = std::make_unique<int>(2);
  assert(in.try_send(3));
  auto second = co_await sel.wait(); // Biased: the send case is first.
  assert(second == 0 && !sel.value<0>() && out.buffered() == 1 && !sel.value<1>()); // Receive slot cleared.
  asio::error_code e; assert(**out.try_receive(e) == 2 && !e);
  sel.case_at<1>().enabled = false;
  sel.options().nonblocking = true;
  auto [ec, index] = co_await sel.wait(asio::as_tuple(asio::deferred));
  assert(ec == asio::error::would_block && index == sel.npos && in.buffered() == 1);
  sel.options().nonblocking = false; sel.case_at<1>().enabled = true;
  auto third = co_await sel.wait();
  assert(third == 1 && *sel.value<1>() == 3);
  // A moved selector keeps its state; the moved-from one releases nothing twice.
  auto other = std::move(sel);
  assert(in.try_send(4));
  auto fourth = co_await other.wait();
  assert(fourth == 1 && *other.value<1>() == 4);
  std::stop_source stop;
  auto stoppable = make_selector(ex, {}, stop.get_token(), receive(in));
  asio::post(ex, [&] { stop.request_stop(); });
  auto [stop_ec, stop_index] = co_await stoppable.wait(asio::as_tuple(asio::deferred));
  assert(stop_ec == asio::error::operation_aborted && stop_index == stoppable.npos && in.pending() == 0);
  done = true;
}
void cross_thread_wakeups() {
  asio::io_context ctx; select_channel<int> a(0), b(0), c(0); std::exception_ptr failure;
  std::array<int, 3> counts{};
  auto done = [&](std::exception_ptr e) { if (e && !failure) failure = e; };
  asio::co_spawn(ctx, [&]() -> asio::awaitable<void> {
    auto ex = co_await asio::this_coro::executor;
    auto sel = make_selector(ex, {}, {}, receive(a), receive(b), receive(c));
    for (int i = 0; i < 3 * rounds; ++i) {
      auto index = co_await sel.wait();
      assert(index < 3);
      auto value = index == 0 ? *sel.value<0>() : index == 1 ? *sel.value<1>() : *sel.value<2>();
      assert(value == counts[index]); ++counts[index];
    }
  }, done);
  for (auto* ch : {&a, &b, &c}) asio::co_spawn(ctx, reusable_producer_from_zero(*ch), done);
  std::vector<std::thread> workers;
  for (int i = 0; i < 3; ++i) workers.emplace_back([&] { ctx.run(); });
  ctx.run();
  for (auto& t : workers) t.join();
  if (failure) std::rethrow_exception(failure);
  assert(counts[0] == rounds && counts[1] == rounds && counts[2] == rounds);
  assert(a.pending() + b.pending() + c.pending() == 0);
}
#endif
int main() {
  mechanism();
#if defined(HAVE_REUSABLE)
  { asio::io_context ctx; select_channel<int> in(0); bool done = false; std::exception_ptr failure;
    asio::co_spawn(ctx, isolation(in, done), [&](std::exception_ptr e) { failure = e; });
    ctx.run(); if (failure) std::rethrow_exception(failure); assert(done && in.pending() == 0); }
  simultaneous_use_and_work();
  teardown_with_suspended_wait();
  { asio::io_context ctx; bool done = false; std::exception_ptr failure;
    asio::co_spawn(ctx, payloads(done), [&](std::exception_ptr e) { failure = e; });
    ctx.run(); if (failure) std::rethrow_exception(failure); assert(done); }
  cross_thread_wakeups();
  std::cout << "reusable selector: one operation per selector, generation isolation, teardown, "
               "simultaneous-use rejection, payload ownership and release passed\n";
#endif
}
