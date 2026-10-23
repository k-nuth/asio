#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
#include <cassert>
#include <iostream>
using namespace asio::experimental;

asio::awaitable<void> loop(select_channel<int>& a, select_channel<int>& b,
    select_channel<int>& c, int& delivered, bool& other_task_ran) {
  auto ex = co_await asio::this_coro::executor;
  // A ready loop must still yield cooperatively to already queued work.
  asio::post(ex, [&] { other_task_ran = true; });
  for (int i = 0; i < 256; ++i) {
    auto [ec, result] = co_await make_select(receive(a), receive(b), receive(c))
      .async_wait(ex, {}, {}, asio::as_tuple(asio::use_awaitable));
    assert(!ec && result.index < 3);
    auto value = result.index == 0 ? *result.value<0>()
      : result.index == 1 ? *result.value<1>() : *result.value<2>();
    assert(value == 7);
    select_channel<int>* channels[] = {&a, &b, &c};
    assert(channels[result.index]->try_send(7));
    assert(a.buffered() + b.buffered() + c.buffered() == 3);
    if (i == 128) assert(other_task_ran);
    ++delivered;
  }
}
int main() {
  asio::io_context ctx;
  select_channel<int> a(1), b(1), c(1);
  assert(a.try_send(7) && b.try_send(7) && c.try_send(7));
  int delivered = 0;
  bool other_task_ran = false;
  std::exception_ptr failure;
  asio::co_spawn(ctx, loop(a, b, c, delivered, other_task_ran),
      [&](std::exception_ptr e) { failure = e; });
  auto turns = ctx.run();
  if (failure) std::rethrow_exception(failure);
  assert(delivered == 256 && other_task_ran);
  assert(a.pending() + b.pending() + c.pending() == 0);
  std::cout << "delivered=" << delivered << " executor_turns=" << turns << '\n' << std::flush;
  // A semantic control for a ready path: no start/completion round-trip per
  // message. This is deliberately a handler-count bound, not a timing threshold.
  assert(turns < 20);
}
