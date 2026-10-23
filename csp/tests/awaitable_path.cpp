#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
#include <cassert>
#include <future>
#include <iostream>
using namespace asio::experimental;
// Named tuple storage below is required by the independently reproduced
// GCC 16.1 -O1 coroutine structured-binding lifetime issue. All state/payload
// ownership still ends at natural scope exit; weak probes verify destruction.
// Every coroutine scenario runs with both token families: the awaitable<T>
// tokens (as_tuple(use_awaitable), use_awaitable) and the lazy operation tokens
// (as_tuple(deferred), deferred) whose ready path commits in the awaiting frame.
channel_select_options biased_options() { channel_select_options o; o.biased = true; return o; }

struct stop_during_move {
  int value;
  std::stop_source* stop;
  bool* armed;
  stop_during_move(int n, std::stop_source& source, bool& flag) : value(n), stop(&source), armed(&flag) {}
  stop_during_move(stop_during_move&& other) noexcept : value(other.value), stop(other.stop), armed(other.armed) {
    if (*armed) { *armed = false; stop->request_stop(); }
  }
};
template<class TupleToken, class ThrowingToken>
asio::awaitable<void> ready_contracts(std::vector<std::weak_ptr<void>>& released,
    TupleToken tuple_token, ThrowingToken throwing_token) {
  auto ex = co_await asio::this_coro::executor;
  auto opts = biased_options();
  select_channel<int> a(2); select_channel<std::string> b(1);
  released.emplace_back(a.state()); released.emplace_back(b.state());
  assert(a.try_send(1) && a.try_send(2) && b.try_send("kept"));
  auto completion_1 = co_await make_select(receive(a), receive(b)).async_wait(ex, opts, {}, tuple_token);
  auto& [ec, r] = completion_1;
  assert(!ec && r.index == 0 && r.template value<0>() == 1 && !r.template value<1>());
  asio::error_code e; assert(a.try_receive(e) == 2 && !e);
  assert(b.try_receive(e) == "kept" && !e);
  // Aliased cases cannot consume twice or deadlock on their shared channel.
  assert(a.try_send(3) && a.try_send(4));
  auto alias = co_await make_select(receive(a), receive(a)).async_wait(ex, opts, {}, throwing_token);
  assert(alias.index == 0 && alias.template value<0>() == 3 && !alias.template value<1>());
  assert(a.try_receive(e) == 4 && !e);

  select_channel<std::unique_ptr<int>> out(1); released.emplace_back(out.state());
  assert(a.try_send(5));
  auto completion_2 = co_await make_select(receive(a), send(out, std::make_unique<int>(6)))
    .async_wait(ex, opts, {}, tuple_token);
  auto& [mixed_ec, mixed] = completion_2;
  assert(!mixed_ec && mixed.index == 0 && mixed.template value<0>() == 5 && **mixed.template value<1>() == 6);
  assert(out.buffered() == 0);
  auto sent = co_await make_select(send(out, std::make_unique<int>(7)), receive(a))
    .async_wait(ex, opts, {}, throwing_token);
  assert(sent.index == 0 && !sent.template value<0>() && !sent.template value<1>());
  auto payload = out.try_receive(e); assert(!e && **payload == 7);
  out.close();
  auto completion_3 = co_await make_select(send(out, std::make_unique<int>(8)))
    .async_wait(ex, opts, {}, tuple_token);
  auto& [closed_ec, closed] = completion_3;
  assert(closed_ec == asio::error::broken_pipe && **closed.template value<0>() == 8);
  bool threw = false;
  try { (void)co_await make_select(receive(out)).async_wait(ex, opts, {}, throwing_token); }
  catch (const asio::system_error& error) { threw = error.code() == asio::error::eof; }
  assert(threw);

  std::stop_source stop; stop.request_stop(); assert(a.try_send(9));
  auto completion_4 = co_await make_select(receive(a), send(out, std::make_unique<int>(10)))
    .async_wait(ex, opts, stop.get_token(), tuple_token);
  auto& [cancel_ec, cancelled] = completion_4;
  assert(cancel_ec == asio::error::operation_aborted && cancelled.index == cancelled.npos);
  assert(!cancelled.template value<0>() && **cancelled.template value<1>() == 10 && a.buffered() == 1);
  auto timed_opts = opts; timed_opts.deadline = asio::steady_timer::time_point::min();
  auto completion_5 = co_await make_select(receive(a))
    .async_wait(ex, timed_opts, {}, tuple_token);
  auto& [timeout_ec, timeout] = completion_5;
  assert(timeout_ec == asio::error::timed_out && !timeout.template value<0>() && a.buffered() == 1);
  auto completion_6 = co_await make_select(after(asio::steady_timer::time_point::min()), receive(a))
    .async_wait(ex, opts, {}, tuple_token);
  auto& [after_ec, after_result] = completion_6;
  assert(!after_ec && after_result.index == 0 && after_result.template value<0>() && a.buffered() == 1);
  auto disabled_opts = opts; disabled_opts.nonblocking = true;
  auto completion_7 = co_await make_select(receive(a).when(false), send(out, std::make_unique<int>(11)).when(false))
    .async_wait(ex, disabled_opts, {}, tuple_token);
  auto& [disabled_ec, disabled] = completion_7;
  assert(disabled_ec == asio::error::would_block && !disabled.template value<0>() && **disabled.template value<1>() == 11);
  assert(a.try_receive(e) == 9 && !e);
  assert(a.pending() + b.pending() + out.pending() == 0);
  std::stop_source moving_stop; bool armed = false;
  select_channel<stop_during_move> moving(1); released.emplace_back(moving.state());
  assert(moving.try_send(stop_during_move(15, moving_stop, armed))); armed = true;
  auto completion_8 = co_await make_select(receive(moving))
    .async_wait(ex, {}, moving_stop.get_token(), tuple_token);
  auto& [move_ec, moved] = completion_8;
  assert(!move_ec && moved.template value<0>()->value == 15 && moving_stop.stop_requested());
  assert(moving.buffered() == 0 && moving.pending() == 0);
}

template<class TupleToken>
asio::awaitable<void> receive_one(select_channel<int>& channel, asio::any_io_executor owner, bool& delivered,
    TupleToken token) {
  auto completion = co_await make_select(receive(channel)).async_wait(owner, {}, {}, token);
  auto& [ec, r] = completion;
  assert(!ec && r.template value<0>() == 42 && channel.pending() == 0); delivered = true;
}
template<class TupleToken>
asio::awaitable<void> cancelled_pending_send(select_channel<std::unique_ptr<int>>& channel,
    std::stop_token stop, bool& completed, TupleToken token) {
  auto ex = co_await asio::this_coro::executor;
  auto completion = co_await make_select(send(channel, std::make_unique<int>(19)))
    .async_wait(ex, {}, stop, token);
  auto& [ec, r] = completion;
  assert(ec == asio::error::operation_aborted && **r.template value<0>() == 19 && channel.pending() == 0);
  completed = true;
}
template<class TupleToken>
void pending_and_affinity(TupleToken token) {
  asio::io_context ctx; select_channel<int> channel(0); bool delivered = false;
  std::exception_ptr failure;
  asio::co_spawn(ctx, receive_one(channel, ctx.get_executor(), delivered, token), [&](std::exception_ptr e) { failure = e; });
  // The coroutine has already suspended before its inline owner startup.
  assert(ctx.run_one() == 1 && channel.pending() == 1);
  assert(channel.try_send(42)); ctx.run();
  if (failure) std::rethrow_exception(failure);
  assert(delivered && channel.pending() == 0);
  ctx.restart(); std::stop_source stop; select_channel<std::unique_ptr<int>> pending_send(0); bool cancelled = false;
  asio::co_spawn(ctx, cancelled_pending_send(pending_send, stop.get_token(), cancelled, token),
      [&](std::exception_ptr e) { failure = e; });
  assert(ctx.run_one() == 1 && pending_send.pending() == 1);
  stop.request_stop(); ctx.run(); if (failure) std::rethrow_exception(failure);
  assert(cancelled && pending_send.pending() == 0);

  asio::io_context caller, owner; select_channel<int> ready(1); assert(ready.try_send(42)); delivered = false;
  asio::co_spawn(caller, receive_one(ready, owner.get_executor(), delivered, token), [&](std::exception_ptr e) { failure = e; });
  assert(caller.run_one() == 1 && ready.buffered() == 1 && !delivered);
  assert(owner.run_one() == 1 && ready.buffered() == 0 && !delivered);
  caller.run(); owner.run();
  if (failure) std::rethrow_exception(failure);
  assert(delivered);
}
template<class TupleToken>
asio::awaitable<void> cancellation_from_queued_task(select_channel<int>& channel, asio::cancellation_signal& signal,
    bool& observed, TupleToken token) {
  auto ex = co_await asio::this_coro::executor;
  co_await asio::this_coro::throw_if_cancelled(false);
  asio::post(ex, [&] { signal.emit(asio::cancellation_type::all); });
  for (int i = 0; i < 128; ++i) {
    auto [ec, r] = co_await make_select(receive(channel)).async_wait(ex, {}, {}, token);
    if (ec) {
      assert(ec == asio::error::operation_aborted && !r.template value<0>() && channel.buffered() == 1);
      observed = true; co_return;
    }
    assert(r.template value<0>() == 7 && channel.try_send(7));
  }
  assert(false && "ready coroutine monopolized its worker or lost cancellation");
}
void larger_selection_and_completion_lifetime() {
  asio::io_context ctx;
  select_channel<int> a(0), b(0), c(0), d(0), e(0);
  bool delivered = false;
  make_select(receive(a), receive(b), receive(c), receive(d), receive(e))
    .async_wait(ctx.get_executor(), biased_options(), {}, [&](auto ec, auto r) {
      assert(!ec && r.index == 4 && r.template value<4>() == 42);
      assert(!r.template value<0>() && !r.template value<1>() && !r.template value<2>() && !r.template value<3>());
      delivered = true;
    });
  assert(ctx.run_one() == 1);
  assert(a.pending() == 1 && b.pending() == 1 && c.pending() == 1 && d.pending() == 1 && e.pending() == 1);
  assert(e.try_send(42)); ctx.run();
  assert(delivered && a.pending() + b.pending() + c.pending() + d.pending() + e.pending() == 0);

  // Completion may release both the facade and its result's channel ownership
  // before the already-finalized operation itself is destroyed.
  ctx.restart(); auto facade = std::make_unique<select_channel<int>>(0); delivered = false;
  make_select(receive(*facade)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(!ec && r.template value<0>() == 42);
    facade.reset(); std::get<0>(r.cases).channel.reset(); delivered = true;
  });
  assert(ctx.run_one() == 1 && facade->pending() == 1);
  assert(facade->try_send(42)); ctx.run(); assert(delivered);
}
// The lazy operation touches no channel until it is awaited or invoked, and
// any completion token may consume it.
asio::awaitable<void> lazy_operation_contracts(std::vector<std::weak_ptr<void>>& released) {
  auto ex = co_await asio::this_coro::executor;
  select_channel<int> a(1); released.emplace_back(a.state());
  assert(a.try_send(1));
  auto op = make_select(receive(a)).async_wait(ex, biased_options(), {}, asio::as_tuple(asio::deferred));
  asio::error_code e; assert(a.try_receive(e) == 1 && !e && a.pending() == 0 && a.buffered() == 0);
  assert(a.try_send(2));
  auto completion = co_await std::move(op);
  auto& [ec, r] = completion;
  assert(!ec && r.template value<0>() == 2 && a.pending() == 0 && a.buffered() == 0);
  assert(a.try_send(3));
  auto partial = co_await make_select(receive(a)).async_wait(ex, biased_options(), {}, asio::as_tuple);
  auto& [partial_ec, partial_result] = partial;
  assert(!partial_ec && partial_result.template value<0>() == 3 && a.buffered() == 0);
}
void lazy_operation_generic_tokens() {
  asio::io_context ctx; select_channel<int> a(0); auto strand = asio::make_strand(ctx); int calls = 0;
  // Invoked with a callback, the lazy operation starts with the posted turn
  // generic tokens have always used and completes on the bound executor.
  auto op = make_select(receive(a)).async_wait(ctx.get_executor(), {}, {}, asio::deferred);
  std::move(op)(asio::bind_executor(strand, [&](auto ec, auto r) {
    assert(!ec && r.template value<0>() == 5 && strand.running_in_this_thread()); ++calls;
  }));
  assert(a.pending() == 0); // Nothing is registered before the start turn.
  assert(ctx.run_one() == 1 && a.pending() == 1);
  assert(a.try_send(5)); ctx.run(); assert(calls == 1 && a.pending() == 0);
  ctx.restart(); select_channel<int> b(1); assert(b.try_send(6));
  auto future = make_select(receive(b)).async_wait(ctx.get_executor(), {}, {}, asio::as_tuple(asio::deferred))(asio::use_future);
  ctx.run();
  auto completion = future.get();
  auto& [ec, r] = completion;
  assert(!ec && r.template value<0>() == 6 && b.buffered() == 0 && b.pending() == 0);
}
// A coroutine with a concrete executor type instantiates the owner comparison
// and the frame executor accessor for that type, with both token families.
asio::awaitable<void, asio::io_context::executor_type> typed_executor_coroutine(
    select_channel<int>& a, bool& done) {
  auto ex = co_await asio::this_coro::executor;
  assert(a.try_send(21));
  auto lazy = co_await make_select(receive(a)).async_wait(ex, biased_options(), {}, asio::as_tuple(asio::deferred));
  auto& [lazy_ec, lazy_result] = lazy;
  assert(!lazy_ec && lazy_result.template value<0>() == 21 && a.buffered() == 0);
  assert(a.try_send(22));
  auto typed = co_await make_select(receive(a)).async_wait(ex, biased_options(), {},
      asio::as_tuple(asio::use_awaitable_t<asio::io_context::executor_type>{}));
  auto& [typed_ec, typed_result] = typed;
  assert(!typed_ec && typed_result.template value<0>() == 22 && a.buffered() == 0);
  auto thrown = co_await make_select(receive(a).when(false), after(asio::steady_timer::time_point::min()))
    .async_wait(ex, biased_options(), {}, asio::use_awaitable_t<asio::io_context::executor_type>{});
  assert(thrown.index == 1);
  // Suspension and resumption with the typed executor: nothing is ready yet.
  auto pending = make_select(receive(a)).async_wait(ex, {}, {}, asio::deferred);
  asio::post(ex, [&] { assert(a.try_send(23)); });
  auto resumed = co_await std::move(pending);
  assert(resumed.template value<0>() == 23 && a.pending() == 0);
  done = true;
}
void typed_executor() {
  asio::io_context ctx; select_channel<int> a(1); bool done = false; std::exception_ptr failure;
  asio::co_spawn(ctx.get_executor(), typed_executor_coroutine(a, done), [&](std::exception_ptr e) { failure = e; });
  ctx.run(); if (failure) std::rethrow_exception(failure);
  assert(done && a.pending() == 0 && a.buffered() == 0);
}
template<class TupleToken, class ThrowingToken>
void run_token_pair(TupleToken tuple_token, ThrowingToken throwing_token) {
  asio::io_context ctx; std::exception_ptr failure;
  std::vector<std::weak_ptr<void>> released;
  asio::co_spawn(ctx, ready_contracts(released, tuple_token, throwing_token), [&](std::exception_ptr e) { failure = e; });
  ctx.run();
  if (failure) std::rethrow_exception(failure);
  for (const auto& state : released) assert(state.expired());
  pending_and_affinity(tuple_token);
  ctx.restart(); select_channel<int> channel(1); assert(channel.try_send(7));
  asio::cancellation_signal signal; bool observed = false;
  asio::co_spawn(ctx, cancellation_from_queued_task(channel, signal, observed, tuple_token),
    asio::bind_cancellation_slot(signal.slot(), [&](std::exception_ptr e) { failure = e; }));
  ctx.run(); if (failure) std::rethrow_exception(failure); assert(observed && channel.pending() == 0);
}
int main() {
  run_token_pair(asio::as_tuple(asio::use_awaitable), asio::use_awaitable);
  run_token_pair(asio::as_tuple(asio::deferred), asio::deferred);
  larger_selection_and_completion_lifetime();
  asio::io_context ctx; std::exception_ptr failure;
  std::vector<std::weak_ptr<void>> released;
  asio::co_spawn(ctx, lazy_operation_contracts(released), [&](std::exception_ptr e) { failure = e; });
  ctx.run(); if (failure) std::rethrow_exception(failure);
  for (const auto& state : released) assert(state.expired());
  lazy_operation_generic_tokens();
  typed_executor();
  std::cout << "ready coroutine contracts, suspension, owner affinity and cooperative cancellation passed"
               " with awaitable and lazy operation tokens\n";
}
