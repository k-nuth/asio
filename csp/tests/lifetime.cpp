// Controlled scheduling around actual registration withdrawal. The observer
// exists only in this test build; it does not replace channel behavior.
#include <atomic>
#include <cstdio>
#include <semaphore>
#include <thread>
void observe_remove(const void*, const void*, bool);
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE(waiter, removing, retained) \
  observe_remove(waiter, removing, retained)
#include <asio/experimental/channel_select.hpp>
#include <asio.hpp>
#include <cassert>

std::atomic<bool> armed{false};
std::binary_semaphore entered{0}, release_remover{0}, peer_context_destroyed{0};
void observe_remove(const void* waiter, const void* removing, bool retained) {
  if (!waiter || waiter == removing || !armed.exchange(false)) return;
  std::printf("peer registration inspected; retained=%d\n", int(retained)); std::fflush(stdout);
  entered.release();
  // On the original implementation the acquired peer reference keeps the peer
  // operation alive until its context has finished destroying its timer owner.
  if (retained) peer_context_destroyed.acquire();
  release_remover.acquire();
}
int main() {
  using namespace asio::experimental;
  select_channel<int> a(0), b(0);
  auto left = std::make_unique<asio::io_context>(), right = std::make_unique<asio::io_context>();
  std::atomic<int> completions{0};
  channel_select_options options; options.biased = true;
  make_select(receive(a), receive(b)).async_wait(left->get_executor(), options, {},
      [&](auto, auto) { ++completions; });
  make_select(receive(a), receive(b)).async_wait(right->get_executor(), options, {},
      [&](auto, auto) { ++completions; });
  assert(left->run_one() == 1 && right->run_one() == 1);
  assert(a.pending() == 2 && b.pending() == 2);
  armed = true;
  std::thread first([&] { left.reset(); });
  entered.acquire();
  std::thread second([&] {
    right.reset();
    std::puts("peer context released"); std::fflush(stdout);
    peer_context_destroyed.release();
  });
  release_remover.release();
  first.join(); second.join();
  assert(completions == 0 && a.pending() == 0 && b.pending() == 0);
  std::puts("concurrent context teardown completed with no registered waits");
}
