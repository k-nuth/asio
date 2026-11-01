#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
#include <iostream>
#include <stdexcept>
#if ASIO_VERSION != 103802
#error Unexpected upstream baseline
#endif
using namespace asio::experimental;
asio::awaitable<int> consume(select_channel<int>& a, select_channel<std::string>& b) {
  auto ex = co_await asio::this_coro::executor;
  auto r = co_await make_select(receive(a), receive(b)).async_wait(ex, {}, {}, asio::use_awaitable);
  if (r.index == 0) co_return *r.template value<0>();
  co_return std::stoi(*r.template value<1>());
}
int main() {
  asio::io_context ctx; select_channel<int> a(1); select_channel<std::string> b(0);
  if (!a.try_send(42)) throw std::runtime_error("send failed");
  int normal = 0;
  asio::steady_timer timer(ctx); timer.expires_at(asio::steady_timer::clock_type::time_point::min());
  timer.async_wait([&](auto ec) { if (!ec) ++normal; });
  auto done = asio::co_spawn(ctx, consume(a, b), asio::use_future);
  ctx.run(); if (done.get() != 42 || normal != 1) return 1;
  std::cout << "ordinary Asio and heterogeneous channel select passed; ASIO_VERSION=" << ASIO_VERSION << '\n';
}
