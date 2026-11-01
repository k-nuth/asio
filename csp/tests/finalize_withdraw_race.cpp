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
//      will fail; O's finalize() runs on the scheduler thread and must withdraw
//      O's node on X.
//
// Pre-fix finalize withdrew nodes one channel at a time. For O's node on X it
// read the node's `list` pointer WITHOUT channel X's mutex and, finding it
// already unlinked (step 2), returned WITHOUT taking that mutex. So it did not
// serialize with the in-flight take() on X: it ran to completion and destroyed
// O while the producer still held a pointer to O and had not yet claimed it.
//
// Safety property under test: O cannot be destroyed while the peer holds its
// node inside X's mutex, i.e. finalize must serialize with that peer through
// X's mutex. The fix withdraws under every participating channel mutex
// (`{ lock_channels(); remove(); }`); a per-node scheme that always took the
// node's channel mutex would satisfy the same property.
//
// Verdict: derived only from the order of delivered synchronization events,
// observed at the same points on every input (fixed header, pre-fix header and
// any mutant). Observation points (test-only macros, compiled out otherwise):
//   OBSERVE_CLAIM(op)          predicate in take(), after the unlink and before
//                              the claim: gates the peer; returning false makes
//                              take() abandon the node untouched (join-safe
//                              cleanup on RED).
//   OBSERVE_FREE(op)           in ~operation.
//   OBSERVE_LOCK_ATTEMPT(m)    inside the channel-lock primitive, immediately
//                              before std::mutex::lock on channel mutex m.
//   OBSERVE_LOCK_ACQUIRED(m)   inside the same primitive, once m is held.
//   OBSERVE_LOCKING(op)        in finalize before the withdrawal step; recorded
//                              for the log only, never used for the verdict.
// With the producer gated inside X's mutex, exactly one of two events is
// delivered first, and neither involves a timeout:
//   - FREE: O's destructor ran while the producer was still gated. Premature
//     destruction, RED. The producer is then released with the node abandoned,
//     so the control itself never dereferences freed memory.
//   - LOCK_ATTEMPT(X) from a thread other than the producer: finalize entered
//     the acquisition of the mutex the peer holds. The control releases the
//     producer, whose claim fails (O is committed on Z) and which releases X on
//     its way out of try_send. GREEN requires the delivered order
//     attempt(X) < release < acquired(X) < FREE: finalize could not take X until
//     the producer gave it up, and destruction followed the peer's access. Any
//     other order is reported as a failure with the observed sequence.
// A finalize that fires markers but withdraws without X's mutex never produces
// LOCK_ATTEMPT(X) and does produce FREE, so it is RED whatever markers it
// carries; the OBSERVE_LOCKING marker plays no part in the verdict.
//
// Cleanup is join-safe in both verdicts. Every thread is joined and O's
// completion is awaited as a delivered signal before the verdict is reported.
// Building with -DFWR_CORROBORATE_DEREFERENCE instead lets the released
// producer perform the real dereference after a RED observation
// (AddressSanitizer then reports heap-use-after-free in take on the pre-fix
// header): corroboration only, never the regression.
//
// Thread safety of the control itself: every event counter and the producer's
// thread id are written and read only under the control mutex m, which is
// always taken after (never before) any channel mutex; the only atomics are
// the arming flag, the identity of O and the one-shot gate selector.
#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <thread>

namespace detail_fwr {
void on_operation(void* op);
bool on_claim(void* op); // false: take() abandons the popped node untouched
void on_free(void* op);
void on_locking(void* op);
void on_lock_attempt(std::mutex* m);
void on_lock_acquired(std::mutex* m);
}
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_OPERATION(op)    detail_fwr::on_operation(static_cast<void*>(op))
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_CLAIM(op)        detail_fwr::on_claim(static_cast<void*>(op))
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_FREE(op)         detail_fwr::on_free(static_cast<void*>(op))
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCKING(op)      detail_fwr::on_locking(static_cast<void*>(op))
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ATTEMPT(m)  detail_fwr::on_lock_attempt(m)
#define ASIO_CHANNEL_SELECT_TEST_OBSERVE_LOCK_ACQUIRED(m) detail_fwr::on_lock_acquired(m)
#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
using namespace asio::experimental;

namespace detail_fwr {
std::mutex m;                             // guards every field below that is not atomic
std::condition_variable cv;
std::atomic<void*> o_ptr{nullptr};        // the single consumer operation O
std::atomic<bool> armed{false};
std::atomic<bool> claim_gated{false};     // one-shot: only the producer's take on X is gated
std::mutex* x_mutex = nullptr;            // channel X's mutex; set before any thread starts
std::thread::id producer_id;              // the gated producer; written and read under m
unsigned long seq = 0;                    // one counter orders every delivered event
unsigned long seq_gate = 0, seq_locking = 0, seq_attempt_x = 0, seq_release = 0, seq_acquired_x = 0, seq_free = 0;
bool at_claim = false, go = false, abandon = false;

unsigned long next() { return ++seq; }
void on_operation(void* op) { void* e = nullptr; o_ptr.compare_exchange_strong(e, op); }
bool on_claim(void* op) {
  if (!armed.load() || op != o_ptr.load()) return true;
  if (claim_gated.exchange(true)) return true;      // the later take on Z that commits O proceeds
  std::unique_lock<std::mutex> lk(m);
  producer_id = std::this_thread::get_id();
  seq_gate = next(); at_claim = true; cv.notify_all();
  cv.wait(lk, [] { return go; });
  return !abandon;
}
void on_free(void* op) {
  if (!armed.load() || op != o_ptr.load()) return;
  std::lock_guard<std::mutex> lk(m); if (!seq_free) seq_free = next(); cv.notify_all();
}
void on_locking(void* op) {
  if (!armed.load() || op != o_ptr.load()) return;
  std::lock_guard<std::mutex> lk(m); if (!seq_locking) seq_locking = next();
}
void on_lock_attempt(std::mutex* mx) {
  if (!armed.load() || mx != x_mutex) return;
  std::lock_guard<std::mutex> lk(m);
  if (std::this_thread::get_id() == producer_id) return;
  if (!seq_attempt_x) { seq_attempt_x = next(); cv.notify_all(); }
}
void on_lock_acquired(std::mutex* mx) {
  if (!armed.load() || mx != x_mutex) return;
  std::lock_guard<std::mutex> lk(m);
  if (std::this_thread::get_id() == producer_id) return;
  if (!seq_acquired_x) { seq_acquired_x = next(); cv.notify_all(); }
}
void release(bool abandon_node) {
  std::lock_guard<std::mutex> lk(m); abandon = abandon_node; seq_release = next(); go = true; cv.notify_all();
}
}

int main() {
  using namespace detail_fwr;
  asio::io_context ctx;
  auto work = asio::make_work_guard(ctx);
  select_channel<int> X(0), Z(0);
  x_mutex = &X.state()->mutex;
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

  bool sent = Z.try_send(7);                       // commit O on Z; its finalize is now owed
  int verdict = 0;
  if (!sent) {
    verdict = 2; release(false);                   // scenario not established; let the producer claim a live O
  } else {
    bool premature;
    {
      std::unique_lock<std::mutex> lk(m);
      cv.wait(lk, [] { return seq_free || seq_attempt_x; });
      premature = seq_free && (!seq_attempt_x || seq_free < seq_attempt_x);
    }
    if (premature) {
      verdict = 1;                                 // RED: destroyed while the peer held the node
#if defined(FWR_CORROBORATE_DEREFERENCE)
      release(false);                              // corroboration: perform the dangling dereference (sanitizer build only)
#else
      release(true);                               // abandon the node of the destroyed operation
#endif
    } else {
      release(false);                              // finalize is in lock(X): the peer's claim fails on a live O and X is released
      std::unique_lock<std::mutex> lk(m);
      cv.wait(lk, [] { return seq_acquired_x && seq_free; });
      bool order = seq_attempt_x < seq_release && seq_release < seq_acquired_x && seq_acquired_x < seq_free;
      verdict = order ? 0 : 3;
    }
  }

  // Join-safe teardown in every verdict: the producer exits take() either by a
  // failed claim on a live operation or by abandoning the node untouched; O's
  // completion is a delivered signal; then the scheduler thread is stopped.
  producer.join();
  { std::unique_lock<std::mutex> lk(m); cv.wait(lk, [&] { return o_completed; }); }
  work.reset();
  ctx.stop();
  io.join();

  std::printf("events: gate=%lu locking-marker=%lu attempt(X)=%lu release=%lu acquired(X)=%lu free=%lu\n",
              seq_gate, seq_locking, seq_attempt_x, seq_release, seq_acquired_x, seq_free);
  const std::size_t expected_index = sent ? 1 : 0;
  if (!(o_completed && o_index == expected_index && !o_ec)) {
    std::printf("INVALID: O completed with index %zu ec=%s (expected %zu, no error)\n",
                o_index, o_ec.message().c_str(), expected_index);
    return 3;
  }
  if (verdict == 2) { std::printf("INVALID: the commit on Z did not claim O; scenario not established\n"); return 2; }
  if (verdict == 1) {
    std::printf("RED: operation %p was destroyed while a peer's take() held one of its wait nodes and "
                "had not yet claimed it; no lock attempt on the peer's channel preceded the destruction; "
                "all threads joined\n", o_ptr.load());
    return 1;
  }
  if (verdict == 3) { std::printf("FAIL: unexpected event order (see events line); all threads joined\n"); return 3; }
  std::printf("GREEN: finalize attempted the peer's channel mutex while the peer held it, acquired it only "
              "after the peer's release, and the operation was destroyed only afterwards; all threads joined\n");
  return 0;
}
