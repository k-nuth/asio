# Implementation comparison and optimization boundary

The comparison inspected the sources actually built by Cargo.lock: Crossbeam
Channel 0.5.17 and Tokio 1.54.1. These are source references, not a claim that
all runtime/channel contracts are equivalent. The C++ changes implement the
following techniques directly; they do not wrap Rust or add another runtime.

## What the references do

[Crossbeam run_select](https://github.com/crossbeam-rs/crossbeam/blob/crossbeam-channel-0.5.17/crossbeam-channel/src/select.rs)
shuffles the candidate handles and first attempts to commit a ready operation.
Only after that fails does it create/use a waiting context, register operations,
block, and unregister losers. Its bounded
[array channel](https://github.com/crossbeam-rs/crossbeam/blob/crossbeam-channel-0.5.17/crossbeam-channel/src/flavors/array.rs)
reserves stamped ring slots through atomics before moving a message; it includes
CAS retries/backoff. Its rendezvous flavor uses a separate packet/waiter protocol.
Crossbeam blocks OS threads. We keep coroutine suspension and do not copy its
blocking waits or readiness/backoff loop into application tasks.

[Tokio select!](https://github.com/tokio-rs/tokio/blob/tokio-1.54.1/tokio/src/macros/select.rs)
stores futures in the enclosing async frame, picks a random first branch, and
polls until one returns Ready. A ready selection does not spawn another task.
[Tokio MPSC recv](https://github.com/tokio-rs/tokio/blob/tokio-1.54.1/tokio/src/sync/mpsc/chan.rs)
tries the queue before registering a waker, then checks again to close the
registration race. It consumes a cooperative task budget so always-ready loops
yield. Its random starting branch followed by cyclic polling is not the same
as uniformly shuffling every case when only a sparse subset is ready. This C++
implementation retains full shuffling and its existing equal-priority contract.
MPSC has one receiving owner; this C++ channel supports concurrent
receivers as well. Our Rust mixed benchmark reserves capacity before sending,
so the comparison does not gain speed by throwing away a losing send's payload.

[Go select](https://go.dev/src/runtime/select.go) uses compiler/runtime support,
random polling order and ordered channel locking, checks readiness before parking
a goroutine, then withdraws losing waiters. Its wait records and stack lifetime
benefit from the language/runtime integration.

These differences do not explain or justify the original 22-microsecond ready
path. Exactly-one commitment, FIFO and lossless receives are shared requirements.
C++ explicitly retains unsent move-only payloads and supports general completion
executors; that machinery must not be built on every already-ready iteration.

## C++ changes

- `use_awaitable` and `as_tuple(use_awaitable)` first probe on the calling
  coroutine's executor, provided it equals the explicitly requested owner.
  The tuple retains owning references to channels. Each randomized candidate is
  attempted under its own channel lock; the unregistered probe cannot be claimed
  by another task and stops at the first commitment. No speculative receives,
  rollback or waiter insertion occurs. An explicitly different owner uses the
  suspended/deferred path instead.
- After 64 calls on a worker, the next coroutine probe first posts/yields to the
  executor. Cancellation/deadline are checked before commitment after that yield.
  This budget is per worker, not Tokio's per-task budget. Its bounded yield is
  tested; strict scheduler fairness is not claimed.
- If the probe cannot complete, normal startup rechecks under all channel locks
  before publishing waiters. Coroutine startup can dispatch onto its operation
  strand, since the coroutine is already suspended. Notification then finalizes
  on that owner; an asynchronous continuation can dispatch on its completion
  executor. A completion from the initial turn is still posted. Generic tokens
  retain their original deferred startup/completion behavior.
- A small context-owned registry, without threads or scheduling, owns suspended
  operations. The original used an infinite steady_timer for ownership even when
  no deadline existed. That initialized the reactor and made ready executor work
  repeatedly enter it. Finite timers are now created only for actual deadlines.
  Registry and channel locks are never nested; references retire outside both.
  The registry mutex is an explicit primitive-internal synchronization boundary,
  not shared application protocol state.
- Up to four suspended cases use inline storage for channel/order/lock arrays;
  larger selections retain dynamic storage. Acquired peer references have four
  inline retirement slots plus overflow storage. A finalized operation does not
  reaccess channel pointers after handing channel ownership to its completion.

The original header remains unchanged under `reference/` and the immutable tag
`kth-csp-performance-baseline`. Every build produces both C++ engines. Ready and
mixed probes still use the same default randomized arbitration; no biased mode
or refusal to suspend is hidden in the optimized benchmark.

## Evidence and remaining costs

`performance_control.py` compiles the same public ready loop and pending receive
against both headers. The original loop delivers all 256 messages but needs 514
executor handlers; the current path needs fewer than 20 while allowing queued
work to run. The original channel-only wait creates a reactor; the current wait
does not. Both baselines fail the intended assertion and both current controls
pass. These are mechanism regressions, not unreliable wall-clock assertions.

The original macOS CPU sample placed 1,573/1,655 main-thread samples in `kevent`.
This identified the scheduler/reactor path, not an upstream Asio contract defect.
The changes avoid unnecessary work in this extension; existing upstream headers
and scheduler behavior remain unchanged.

The ready path still constructs an Asio awaitable frame, takes channel owning
references, randomizes arbitration and locks a candidate channel. The suspended
path still allocates an operation/strand and wait records, touches the context
registry, and schedules a wakeup. Crossbeam's ring and Tokio's single-consumer
queue have different costs. Performance parity is not an established guarantee;
retain the raw observations and compare each configuration rather than selecting
only a favorable result.
