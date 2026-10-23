// Frozen ready_contracts() from f0ba53bf3a0d5dc8e07558ab227f978832bb4ed0.
// Only the entry point is narrowed to that same complete public scenario.
// Reproduces a 592-byte LeakSanitizer report on the measured GCC 16.1 -O1
// configuration with both the original and optimized channel headers.
#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
#include <cassert>
#include <iostream>
using namespace asio::experimental;
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
  std::stop_source moving_stop; bool armed = false;
  select_channel<stop_during_move> moving(1);
  assert(moving.try_send(stop_during_move(15, moving_stop, armed))); armed = true;
  auto [move_ec, moved] = co_await make_select(receive(moving))
    .async_wait(ex, {}, moving_stop.get_token(), asio::as_tuple(asio::use_awaitable));
  assert(!move_ec && moved.value<0>()->value == 15 && moving_stop.stop_requested());
  assert(moving.buffered() == 0 && moving.pending() == 0);
}

int main() {
  asio::io_context ctx; std::exception_ptr failure;
  asio::co_spawn(ctx, ready_contracts(), [&](std::exception_ptr e) { failure = e; }); ctx.run();
  if (failure) std::rethrow_exception(failure);
}
