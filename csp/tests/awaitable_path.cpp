#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
#include <cassert>
#include <iostream>
using namespace asio::experimental;
channel_select_options biased_options() { channel_select_options o; o.biased = true; return o; }

asio::awaitable<void> ready_contracts() {
  auto ex = co_await asio::this_coro::executor;
  auto opts = biased_options();
  select_channel<int> a(2); select_channel<std::string> b(1);
  assert(a.try_send(1) && a.try_send(2) && b.try_send("kept"));
  auto [ec, r] = co_await make_select(receive(a), receive(b)).async_wait(ex, opts, {}, asio::as_tuple(asio::use_awaitable));
  assert(!ec && r.index == 0 && r.value<0>() == 1 && !r.value<1>());
  asio::error_code e; assert(a.try_receive(e) == 2 && !e);
  assert(b.try_receive(e) == "kept" && !e);
  // Aliased cases cannot consume twice or deadlock on their shared channel.
  assert(a.try_send(3) && a.try_send(4));
  auto alias = co_await make_select(receive(a), receive(a)).async_wait(ex, opts, {}, asio::use_awaitable);
  assert(alias.index == 0 && alias.value<0>() == 3 && !alias.value<1>());
  assert(a.try_receive(e) == 4 && !e);

  select_channel<std::unique_ptr<int>> out(1);
  assert(a.try_send(5));
  auto [mixed_ec, mixed] = co_await make_select(receive(a), send(out, std::make_unique<int>(6)))
    .async_wait(ex, opts, {}, asio::as_tuple(asio::use_awaitable));
  assert(!mixed_ec && mixed.index == 0 && mixed.value<0>() == 5 && **mixed.value<1>() == 6);
  assert(out.buffered() == 0);
  auto sent = co_await make_select(send(out, std::make_unique<int>(7)), receive(a))
    .async_wait(ex, opts, {}, asio::use_awaitable);
  assert(sent.index == 0 && !sent.value<0>() && !sent.value<1>());
  auto payload = out.try_receive(e); assert(!e && **payload == 7);
  out.close();
  auto [closed_ec, closed] = co_await make_select(send(out, std::make_unique<int>(8)))
    .async_wait(ex, opts, {}, asio::as_tuple(asio::use_awaitable));
  assert(closed_ec == asio::error::broken_pipe && **closed.value<0>() == 8);
  bool threw = false;
  try { (void)co_await make_select(receive(out)).async_wait(ex, opts, {}, asio::use_awaitable); }
  catch (const asio::system_error& error) { threw = error.code() == asio::error::eof; }
  assert(threw);

  std::stop_source stop; stop.request_stop(); assert(a.try_send(9));
  auto [cancel_ec, cancelled] = co_await make_select(receive(a), send(out, std::make_unique<int>(10)))
    .async_wait(ex, opts, stop.get_token(), asio::as_tuple(asio::use_awaitable));
  assert(cancel_ec == asio::error::operation_aborted && cancelled.index == decltype(cancelled)::npos);
  assert(!cancelled.value<0>() && **cancelled.value<1>() == 10 && a.buffered() == 1);
  auto timed_opts = opts; timed_opts.deadline = asio::steady_timer::time_point::min();
  auto [timeout_ec, timeout] = co_await make_select(receive(a))
    .async_wait(ex, timed_opts, {}, asio::as_tuple(asio::use_awaitable));
  assert(timeout_ec == asio::error::timed_out && !timeout.value<0>() && a.buffered() == 1);
  auto [after_ec, after_result] = co_await make_select(after(asio::steady_timer::time_point::min()), receive(a))
    .async_wait(ex, opts, {}, asio::as_tuple(asio::use_awaitable));
  assert(!after_ec && after_result.index == 0 && after_result.value<0>() && a.buffered() == 1);
  auto disabled_opts = opts; disabled_opts.nonblocking = true;
  auto [disabled_ec, disabled] = co_await make_select(receive(a).when(false), send(out, std::make_unique<int>(11)).when(false))
    .async_wait(ex, disabled_opts, {}, asio::as_tuple(asio::use_awaitable));
  assert(disabled_ec == asio::error::would_block && !disabled.value<0>() && **disabled.value<1>() == 11);
  assert(a.try_receive(e) == 9 && !e);
  assert(a.pending() + b.pending() + out.pending() == 0);
}

asio::awaitable<void> receive_one(select_channel<int>& channel, asio::any_io_executor owner, bool& delivered) {
  auto [ec, r] = co_await make_select(receive(channel)).async_wait(owner, {}, {}, asio::as_tuple(asio::use_awaitable));
  assert(!ec && r.value<0>() == 42 && channel.pending() == 0); delivered = true;
}
void pending_and_affinity() {
  asio::io_context ctx; select_channel<int> channel(0); bool delivered = false;
  std::exception_ptr failure;
  asio::co_spawn(ctx, receive_one(channel, ctx.get_executor(), delivered), [&](std::exception_ptr e) { failure = e; });
  // The coroutine has already suspended before its inline owner startup.
  assert(ctx.run_one() == 1 && channel.pending() == 1);
  assert(channel.try_send(42)); ctx.run();
  if (failure) std::rethrow_exception(failure);
  assert(delivered && channel.pending() == 0);

  asio::io_context caller, owner; select_channel<int> ready(1); assert(ready.try_send(42)); delivered = false;
  asio::co_spawn(caller, receive_one(ready, owner.get_executor(), delivered), [&](std::exception_ptr e) { failure = e; });
  assert(caller.run_one() == 1 && ready.buffered() == 1 && !delivered);
  assert(owner.run_one() == 1 && ready.buffered() == 0 && !delivered);
  caller.run(); owner.run();
  if (failure) std::rethrow_exception(failure);
  assert(delivered);
}
asio::awaitable<void> cancellation_from_queued_task(select_channel<int>& channel, asio::cancellation_signal& signal, bool& observed) {
  auto ex = co_await asio::this_coro::executor;
  co_await asio::this_coro::throw_if_cancelled(false);
  asio::post(ex, [&] { signal.emit(asio::cancellation_type::all); });
  for (int i = 0; i < 128; ++i) {
    auto [ec, r] = co_await make_select(receive(channel)).async_wait(ex, {}, {}, asio::as_tuple(asio::use_awaitable));
    if (ec) {
      assert(ec == asio::error::operation_aborted && !r.value<0>() && channel.buffered() == 1);
      observed = true; co_return;
    }
    assert(r.value<0>() == 7 && channel.try_send(7));
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
int main() {
  asio::io_context ctx; std::exception_ptr failure;
  asio::co_spawn(ctx, ready_contracts(), [&](std::exception_ptr e) { failure = e; }); ctx.run();
  if (failure) std::rethrow_exception(failure);
  pending_and_affinity();
  larger_selection_and_completion_lifetime();
  ctx.restart(); select_channel<int> channel(1); assert(channel.try_send(7));
  asio::cancellation_signal signal; bool observed = false;
  asio::co_spawn(ctx, cancellation_from_queued_task(channel, signal, observed),
    asio::bind_cancellation_slot(signal.slot(), [&](std::exception_ptr e) { failure = e; }));
  ctx.run(); if (failure) std::rethrow_exception(failure); assert(observed && channel.pending() == 0);
  std::cout << "ready coroutine contracts, suspension, owner affinity and cooperative cancellation passed\n";
}
