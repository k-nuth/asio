#include <asio.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <cassert>
#include <iostream>
using channel = asio::experimental::concurrent_channel<void(asio::error_code, int)>;
using namespace asio::experimental::awaitable_operators;
asio::awaitable<void> control(channel& a, channel& b, int& remaining) {
  auto winner = co_await (a.async_receive(asio::as_tuple(asio::use_awaitable)) ||
                         b.async_receive(asio::as_tuple(asio::use_awaitable)));
  assert(winner.index() < 2);
  a.try_receive([&](auto, int) { ++remaining; });
  b.try_receive([&](auto, int) { ++remaining; });
}
int main(int argc, char**) {
  asio::io_context ctx; channel a(ctx, 1), b(ctx, 1); int remaining = 0;
  assert(a.try_send(asio::error_code{}, 1));
  assert(b.try_send(asio::error_code{}, 2));
  auto done = asio::co_spawn(ctx, control(a, b, remaining), asio::use_future);
  ctx.run(); done.get();
  std::cout << "sent=2 returned=1 remaining=" << remaining << '\n';
  // Characterisation of existing operator||, not a claim of contract violation.
  if (argc > 1) return remaining == 1 ? 0 : 1;
  assert(remaining == 0);
}
