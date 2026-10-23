#include <asio.hpp>
#include <asio/detail/reactor.hpp>
#include <asio/experimental/channel_select.hpp>
#include <cassert>
#include <iostream>
using namespace asio::experimental;
int main() {
  asio::io_context ctx;
  select_channel<int> channel(0);
  int delivered = 0;
  make_select(receive(channel)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(!ec && r.template value<0>() == 42); ++delivered;
  });
  assert(ctx.run_one() == 1 && channel.pending() == 1);
  // Channel-only waiting has no I/O deadline. It must not initialize a reactor
  // merely to keep an operation alive. This is a mechanism control, not timing.
  bool reactor_created = asio::has_service<asio::detail::reactor>(ctx);
  std::cout << "pending=" << channel.pending() << " reactor=" << reactor_created << '\n' << std::flush;
  assert(!reactor_created);
  assert(channel.try_send(42));
  ctx.run(); assert(delivered == 1 && channel.pending() == 0);
  // Real deadlines still use the existing Asio timer/reactor and cancellation.
  ctx.restart(); std::stop_source stop;
  make_select(receive(channel), after(asio::steady_timer::clock_type::now() + std::chrono::hours(1)))
    .async_wait(ctx.get_executor(), {}, stop.get_token(), [&](auto ec, auto) {
      assert(ec == asio::error::operation_aborted); ++delivered;
    });
  assert(ctx.run_one() == 1 && channel.pending() == 1);
  assert(asio::has_service<asio::detail::reactor>(ctx));
  stop.request_stop(); ctx.run(); assert(delivered == 2 && channel.pending() == 0);
}
