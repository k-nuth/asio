// Mechanism control for the immediate coroutine path. A ready selection awaited
// through as_tuple(asio::deferred) commits in the awaiting coroutine's frame:
// no executor turn and no heap allocation per selection. Cooperative yielding
// to already queued work is checked in the same loop. Counts are
// deterministic; there is no wall-clock threshold.
#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <new>
using namespace asio::experimental;
std::size_t heap_allocations = 0;
void* operator new(std::size_t size) {
  ++heap_allocations;
  if (void* p = std::malloc(size ? size : 1)) return p;
  throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

asio::awaitable<void> loop(select_channel<int>& a, select_channel<int>& b, select_channel<int>& c,
    int& delivered, bool& other_task_ran, std::size_t& allocations_in_loop) {
  auto ex = co_await asio::this_coro::executor;
  // A ready loop must still yield cooperatively to already queued work.
  asio::post(ex, [&] { other_task_ran = true; });
  auto before = heap_allocations;
  for (int i = 0; i < 256; ++i) {
    auto completion = co_await make_select(receive(a), receive(b), receive(c))
      .async_wait(ex, {}, {}, asio::as_tuple(asio::deferred));
    auto& [ec, result] = completion;
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
  allocations_in_loop = heap_allocations - before;
}
int main() {
  asio::io_context ctx;
  select_channel<int> a(1), b(1), c(1);
  assert(a.try_send(7) && b.try_send(7) && c.try_send(7));
  int delivered = 0;
  bool other_task_ran = false;
  std::size_t allocations = 0;
  std::exception_ptr failure;
  asio::co_spawn(ctx, loop(a, b, c, delivered, other_task_ran, allocations),
      [&](std::exception_ptr e) { failure = e; });
  auto turns = ctx.run();
  if (failure) std::rethrow_exception(failure);
  assert(delivered == 256 && other_task_ran);
  assert(a.pending() + b.pending() + c.pending() == 0);
  std::cout << "delivered=" << delivered << " executor_turns=" << turns
            << " heap_allocations=" << allocations << '\n' << std::flush;
  // Mechanism bounds, not timing: no start/completion round trip per selection,
  // and at most one allocation per cooperative yield (every 64th probe).
  assert(turns < 20);
  assert(allocations <= 256 / 64);
}
