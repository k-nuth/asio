// Behavioral regression for the finalize-withdrawal use-after-free.
//
// Observable defect (pre-fix header): a committed selection could be destroyed
// while a peer's channel_state::take() still held a raw pointer to one of its
// wait nodes and was about to dereference the operation to claim it. The peer
// then read freed memory (AddressSanitizer: heap-use-after-free in take/claim;
// ThreadSanitizer: the claim compare-exchange on winner raced the reused
// operation's constructor, the recycling allocator having handed the storage to
// a new operation).
//
// Precise interleaving, reproduced deterministically here:
//
//   1. A consumer O selects receive(X), receive(Z) and suspends, registering a
//      wait node on channel X and on channel Z.
//   2. A producer calls X.try_send(): take(X.receives) pops O's node on X,
//      unlinks it, and is about to call node->identity->claim() -- it holds a
//      raw pointer to O and still has to touch O. (Gated here, between the
//      unlink and the claim, holding channel X's mutex.)
//   3. O is committed on channel Z by another send, so the pending claim on X
//      will fail; O's finalize() runs and must withdraw O's node on X.
//
// Pre-fix finalize withdrew nodes one channel at a time. For O's node on X it
// read the node's `list` pointer WITHOUT channel X's mutex and, finding it
// already unlinked (step 2), returned WITHOUT taking that mutex. So it did not
// serialize with the in-flight take() on X: it ran to completion and destroyed
// O while the producer still held a pointer to O and had not yet claimed it.
//
// The fix withdraws under every participating channel mutex
// (`{ lock_channels(); remove(); }`), so finalize must acquire X's mutex, which
// the producer's take() holds; finalize cannot destroy O until the producer has
// finished claiming and released X. The necessary property is not "lock all
// channels" for its own sake but that the owner acquires each node's channel
// mutex during teardown, making a peer's take() on that channel and the owner's
// self-destruction mutually exclusive. (A per-node scheme that always took the
// node's channel mutex -- rather than reading `list` and returning without it
// -- would also satisfy this; the pre-fix skip path did not.)
//
// Discriminator: a delivered signal, never a timeout, and structural rather
// than temporal. With the producer gated inside X's mutex:
//   - pre-fix finalize needs no mutex for the already-unlinked node, so it
//     completes and O's destructor runs -> OBSERVE_FREE fires while the
//     producer is still gated. RED: the operation was destroyed out from under
//     an in-flight take(). (OBSERVE_LOCKING does not exist in that header.)
//   - fixed finalize announces it is about to take the channel locks
//     (OBSERVE_LOCKING) and then blocks on X until the producer releases it;
//     O's destructor cannot run first. GREEN.
// Neither signal can precede the other in the "wrong" build, so the verdict
// does not depend on scheduling.
//
// Cleanup is join-safe in both verdicts and never touches freed state. The
// claim hook is a predicate: when the control releases the producer after
// observing RED, the hook returns false and take() abandons the popped node
// without dereferencing it (the node lives inside the destroyed operation),
// try_send() returns false, the producer releases X and exits. Every thread is
// joined before the verdict is reported, and no assertion runs while another
// thread is alive. The substitution happens only after the defect has been
// observed; it replaces the crash, not the observation. Building with
// -DFWR_CORROBORATE_DEREFERENCE instead lets the producer perform the real
// dereference (AddressSanitizer then reports heap-use-after-free in take on
// the pre-fix header): corroboration only, never the regression.
//
// Compiles against both the pre-fix and the fixed header; only this test
// defines the observer macros, which are otherwise zero-cost.
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <thread>

namespace detail_fwr {
void on_operation(void* op);
bool on_claim(void* op); // false: take() abandons the popped node untouched
void on_locking(void* op);
void on_free(void* op);
}
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_OPERATION(op) detail_fwr::on_operation(static_cast<void*>(op))
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_CLAIM(op)     detail_fwr::on_claim(static_cast<void*>(op))
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCKING(op)   detail_fwr::on_locking(static_cast<void*>(op))
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_FREE(op)      detail_fwr::on_free(static_cast<void*>(op))
#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
using namespace asio::experimental;

namespace detail_fwr {
std::mutex m;
std::condition_variable cv;
std::atomic<void*> o_ptr{nullptr};    // the single consumer operation O
std::atomic<bool> armed{false};
std::atomic<bool> claim_gated{false}; // one-shot: only the producer's take on X is gated
bool at_claim = false;                // producer reached the gated claim on X
bool go = false;                      // release the gated producer
bool abandon = false;                 // on release: abandon the node instead of claiming
bool freed = false;                   // O's destructor ran (finalize completed) -> defect
bool locking = false;                 // finalize is about to take the channel locks -> safe

void on_operation(void* op) { void* e = nullptr; o_ptr.compare_exchange_strong(e, op); }
bool on_claim(void* op) {
  if (!armed.load() || op != o_ptr.load()) return true;
  if (claim_gated.exchange(true)) return true; // the later take on Z that commits O proceeds
  std::unique_lock<std::mutex> lk(m);
  at_claim = true; cv.notify_all();
  cv.wait(lk, [] { return go; });
  return !abandon;
}
void on_locking(void* op) {
  if (!armed.load() || op != o_ptr.load()) return;
  std::lock_guard<std::mutex> lk(m); locking = true; cv.notify_all();
}
void on_free(void* op) {
  if (!armed.load() || op != o_ptr.load()) return;
  std::lock_guard<std::mutex> lk(m); freed = true; cv.notify_all();
}
void release(bool abandon_node) {
  std::lock_guard<std::mutex> lk(m); abandon = abandon_node; go = true; cv.notify_all();
}
}

int main() {
  using namespace detail_fwr;
  asio::io_context ctx;
  auto work = asio::make_work_guard(ctx);
  select_channel<int> X(0), Z(0);
  channel_select_options biased; biased.biased = true;
  bool o_completed = false; std::size_t o_index = 999; asio::error_code o_ec;

  make_select(receive(X), receive(Z)).async_wait(ctx.get_executor(), biased, {},
      [&](asio::error_code ec, auto r) {
        std::lock_guard<std::mutex> lk(m); o_ec = ec; o_index = r.index; o_completed = true; cv.notify_all();
      });

  // Drive O's start turn until it has registered on X and Z (no sleep, no thread yet).
  while (!(X.pending() == 1 && Z.pending() == 1)) {
    if (ctx.run_one() == 0) { std::printf("INVALID: O never registered\n"); return 2; }
  }
  if (!o_ptr.load()) { std::printf("INVALID: O's operation was not observed\n"); return 2; }
  armed.store(true);

  std::thread io([&] { ctx.run(); });             // runs O's finalize once O is committed
  std::thread producer([&] { X.try_send(42); });  // take(X.receives) pops O's node, gates before claim

  { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [] { return at_claim; }); }

  int verdict;
  bool sent = Z.try_send(7); // commit O on Z; its finalize is now owed
  if (!sent) {
    verdict = 2; // scenario not established; O is alive and unclaimed, let the producer claim it
    release(false);
  } else {
    { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [] { return freed || locking; }); verdict = (freed && !locking) ? 1 : 0; }
#if defined(FWR_CORROBORATE_DEREFERENCE)
    release(false); // corroboration: perform the dangling dereference (AddressSanitizer build only)
#else
    release(verdict == 1); // RED: abandon the node of the destroyed operation; GREEN: claim the live one
#endif
  }

  // Join-safe teardown in every verdict: the producer exits take() either by
  // claiming a live operation or by abandoning the node untouched; O's
  // completion is a delivered signal; then the scheduler thread is stopped.
  producer.join();
  { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return o_completed; }); }
  work.reset();
  ctx.stop();
  io.join();

  const std::size_t expected_index = sent ? 1 : 0;
  if (!(o_completed && o_index == expected_index && !o_ec)) {
    std::printf("INVALID: O completed with index %zu ec=%s (expected %zu, no error)\n",
                o_index, o_ec.message().c_str(), expected_index);
    return 3;
  }
  if (verdict == 2) { std::printf("INVALID: the commit on Z did not claim O; scenario not established\n"); return 2; }
  if (verdict == 1) {
    std::printf("RED: operation %p was destroyed while a peer's take() held one of its "
                "wait nodes and had not yet claimed it (finalize freed a committed "
                "operation out from under an in-flight take); all threads joined\n", o_ptr.load());
    return 1;
  }
  std::printf("GREEN: the owning operation stayed alive throughout the peer's take(); "
              "finalize could not free it until the peer released the channel; all threads joined\n");
  return 0;
}
