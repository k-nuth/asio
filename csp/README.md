# Independent channel selection for standalone Asio

This fork's `1.38.3` development branch starts at stable upstream tag
`asio-1-38-2`, commit `8806a6803cde7054c3049d3666d3ec36786568c5` (annotated tag
object `12b52a54a25d1861b037bc5d9810f92a7151d920`). The fork package is
`asio/1.38.3-kth.1@k-nuth/csp`; it is not an upstream 1.38.3 release. Existing
Asio headers, ASIO_VERSION (103802), channels and operator|| remain unchanged,
with one addition: `asio/impl/awaitable.hpp` gains the opt-in
`awaitable_async_op_immediate` trait, whose default keeps every existing
awaited operation on its usual initiate-after-suspension path. Only this
extension's lazy selection operation specializes it.

## API

Include `<asio/experimental/channel_select.hpp>` with C++20. Ordinary Asio usage
retains its existing language requirements. No new runtime is introduced.

```cpp
using namespace asio::experimental;
select_channel<Job> jobs(16);
select_channel<Block> blocks(4);
select_channel<std::unique_ptr<Reply>> replies(0); // Rendezvous.
auto ex = co_await asio::this_coro::executor;
auto completion = co_await make_select(
    receive(jobs),
    receive(blocks),
    send(replies, std::move(reply)),
    after(asio::steady_timer::clock_type::now() + timeout)
).async_wait(ex, {}, stop_token, asio::as_tuple(asio::deferred));
auto& [ec, selected] = completion;
// selected.index identifies one case; selected.value<I>() is its typed optional.
// A losing send retains its payload in selected.value<I>(), including on errors.
```

`asio::deferred` and `as_tuple(asio::deferred)` return a lazy operation that
touches no channel until it is awaited or invoked. Awaited from an
`asio::awaitable` coroutine whose executor equals `ex`, a ready selection
commits in that coroutine's own frame before it suspends: no coroutine frame,
operation, strand, timer, executor turn or wait registration is created.
Invoking the operation with any other completion token (a callback,
`use_future`, ...) starts the ordinary posted operation. `use_awaitable` and
`as_tuple(use_awaitable)` keep returning `awaitable<T>`; their frame runs the
same ready probe and awaits the lazy operation only when it must suspend.

Channels are independent objects, can carry different types, and need no common
executor or strand. Selections can wait on overlapping sets of channels from
different io_context instances running on different threads. A channel does not
borrow any context; it can outlive a completed or destroyed context.

A task that selects repeatedly over the same cases can own a reusable selector
instead of building a selection per iteration:

```cpp
auto sel = make_selector(ex, options, stop_token,
    receive(jobs), receive(blocks), send(replies, Reply{}), after(deadline));
for (;;) {
  sel.value<2>() = next_reply();            // A send case takes part while it holds a payload.
  sel.case_at<3>().time = next_deadline();  // Cases and options change between waits only.
  auto [ec, index] = co_await sel.wait(asio::as_tuple(asio::deferred));
  if (ec) break;
  if (index == 0) consume(*sel.value<0>()); // Values stay in the selector's storage.
}
```

`channel_selector<Cases...>` (`make_selector(ex, options, stop_token, cases...)`)
owns its channel handles, case values, wait registrations and arbitration state
for its whole lifetime; each `wait()` is one lazy operation completing with
`void(error_code, size_t)` (`co_await sel.wait()` returns the index and throws on
error; `wait(as_tuple(asio::deferred))` returns the tuple; any other completion
token is accepted and stores a type-erased handler). Awaited from an
`asio::awaitable` coroutine on the selector's executor, a ready wait commits in
the coroutine's frame exactly like the one-shot operation; a suspended wait
stores the coroutine's handler in the selector in place and arms the next
generation of the same owned operation, so a loop of suspended selections
constructs no operation and allocates nothing per iteration. The selected case's
value is read through `value<I>()`; a selected send case consumes its payload
and waits for the next one; receive slots are cleared when the next wait begins;
`case_at<I>().enabled` and `options()` may be changed between waits. One task
uses a selector at a time and the selector outlives its waits: a second wait
while one is in progress throws `std::logic_error`, and the selector may be
destroyed after its execution context only while no wait is in progress. The
one-shot `make_select(...).async_wait(...)` API and its owning result are
unchanged; the selector is an additional variant with a different result
transport, not a replacement.

`make_select(cases...).async_wait(ex, options, stop_token, token)` consumes its
cases and completes with `void(error_code, channel_select_result<Cases...>)`.
The result owns a tuple of those cases, has an `index` (`npos` if the whole
operation was cancelled, timed out or refused), and `value<I>()` returns the
optional payload of case I. There is exactly one committed case:

* Winning receive: its optional owns the message, or is empty with `eof` if closed.
* Winning send: its optional is empty on success. On `broken_pipe` it retains the
  unsent message.
* Losing send: its optional still owns the original unsent message.
* Losing receive: its optional is empty and its channel has not been consumed.
* Winning `after(time_point)`: its optional contains monostate; the completion is
  successful and the case index identifies the timer branch.

Use `as_tuple(asio::deferred)` or `as_tuple(use_awaitable)` when send payloads
must survive an error. The throwing forms (`asio::deferred` awaited directly,
`use_awaitable`) discard result arguments on error, as Asio normally does.
Cancellation does not silently drop a send payload inside the primitive.

`receive(channel).when(false)`, `send(channel, value).when(false)` and
`after(time).when(false)` disable a branch for this selection. Payload expressions
are still evaluated once when building cases. With every branch disabled the
operation waits for cancellation/deadline, or returns would_block in nonblocking
mode. Empty selections are compile-time errors.

`channel_select_options` provides:

* Default unbiased arbitration: a Fisher-Yates shuffle of all cases with exactly
  uniform draws before inspecting readiness; every permutation has the same
  probability and each ready branch equal pseudorandom priority. As in Go, this
  is not a bound on starvation under adversarial scheduling. `random_seed`
  optionally supplies reproducible arbitration (`std::shuffle` over a seeded
  `std::mt19937`, unchanged across iterations). Each operation draws from its
  own generator, seeded from a per-worker one; no process-wide selection or RNG
  mutex is used.
* `biased=true`: explicit left-to-right priority; may starve later branches.
* `nonblocking=true`: a default branch equivalent, returning `would_block` and
  every unsent payload when none is ready. Readiness is checked first.
* `deadline`: an operation cancellation deadline, returning `timed_out` and all
  uncommitted sends. An expired operation deadline takes priority at startup.
  Use an explicit `after` case when timeout must compete fairly with ready data.

`select_channel<T>(capacity, pending_limit=1024)` supports capacity zero. The
pending limit bounds combined sender/receiver case registrations per channel.
Admission failure completes with `no_buffer_space`, returning every unsent value
and withdrawing partial registrations. Capacity bounds buffered messages; queued
send payloads remain owned by suspended operations. Executor ingress and total
client operations are separately bounded by the application, like other Asio
initiations. Limits are explicit finite-resource admission, not message loss.

`try_send(T&&)` leaves the caller's argument untouched on refusal.
`try_receive(error_code&)` returns optional<T> with would_block/eof when empty.
`buffered()` and `pending()` support observation; `pending()` counts actual
registrations, without hiding stale or already-claimed nodes. By the time a
completion handler runs all its registrations have been removed from every
channel. Buffered messages and pending senders/receivers are FIFO per channel;
selection may withdraw a losing branch without transferring its payload.

`close()` is thread safe. It prevents new sends, completes pending selected sends
with broken_pipe and their payloads, preserves buffered FIFO messages for draining,
and completes empty receives with eof. A closed branch counts as ready. As in
other channel systems, remove/disable it if continuing to select other channels.
Destroying the channel facade closes its shared state; cases keep that state alive
without borrowing the facade. The facade is move-constructible, not copyable;
only destruction is supported on a moved-from facade.

## Ownership, commitment and synchronization boundary

Application protocol state still belongs to its tasks, communicated through
bounded channels. This header is a low-level synchronization primitive, with an
explicit exception for its internal cross-thread channel bookkeeping and
execution-context lifetime registry:

* Each channel's fixed ring buffer and registration queues are protected by that
  channel's mutex. The coroutine ready probe locks one candidate channel at a
  time, commits at most one case, and publishes no registrations. Before taking
  a candidate's mutex it reads that channel's readiness hints (buffered count,
  queued senders and receivers, closed flag): values modified only under the
  mutex, read without it as relaxed atomics. A stale hint either sends the
  probe under a mutex that finds nothing or sends the task to the suspended
  path, which decides again under every participating mutex, so hints affect
  only which mutexes a probe takes. If it must suspend, startup and timer
  resumption take all participating channel mutexes in total address order,
  deduplicating repeated channels. Channel arbitration has no global
  coordinator/domain.
* Each suspended/generic-token selection has one atomic arbitration state. A
  ready coroutine instead owns its decision locally; no peer can access it.
  During suspended startup the operation is active
  and not externally claimable. If nothing is ready, all registrations are
  installed while holding those channel locks, then it becomes waiting. A peer
  can claim waiting -> one case index exactly once. Cancel/timeout can instead
  claim waiting -> aborted. These are the only competing commitment transitions.
* An active selection claims a pending counterpart before moving a payload. Its
  own cancellation cannot execute concurrently with this active turn. Thus a
  rendezvous commits two different selections without half-claiming either one.
  A selection never matches its own send and receive cases.
* Wait registrations are intrusive nodes owned by the suspended operation, one
  per case, linked into a channel's send or receive list under that channel's
  mutex; registering allocates nothing and counts no references. The owning
  operation unlinks its own nodes under the channel mutex before it can be
  destroyed, and it is strongly owned (by its runner, then by the execution
  context's registry, resolved at construction so that keeping a registration
  cannot fail) from registration until withdrawal, so a linked node always
  belongs to a live, owned operation: a matching peer unlinks the node, claims
  the operation through the node's pointer and wakes it through the same
  pointer, under the channel mutex and without acquiring any reference.
  After commitment the owner acquires every participating channel mutex in
  the same total order before withdrawing registrations and releasing its
  lifetime ownership. This includes channels whose node a peer already
  unlinked: that peer can still be using the node's raw operation pointer
  inside `take()`, under the channel mutex. Skipping that mutex because the
  node is unlinked would allow teardown to free the operation too early.
  Destruction and withdrawal preserve the same channel-lock ordering.
* Payloads must have noexcept move construction and must not reenter the primitive
  or perform blocking work in moves/destructors. Buffer storage is preallocated at
  construction; committing a buffer transfer performs no ring allocation. No
  rollback, above-capacity reinsertion, or speculative receive is used.
* Channel locks protect only internal bookkeeping/transfer. They are released
  before user completion handlers execute. Waiting for data/capacity suspends an
  asynchronous operation; no thread waits on a condition variable, polls readiness,
  or sleeps for capacity. A mutex acquisition can briefly contend with another
  transfer; this is an explicit primitive-level exception, not an application
  shared-state concurrency model or a claim of lock-free operation.
* A coroutine ready probe executes on its current executor only when that
  executor equals the explicitly supplied owner. It creates no operation,
  strand, timer or wait registrations. With the lazy `deferred` tokens it runs
  in the awaiting coroutine's frame before suspension (the awaiter's
  `await_ready`); with `use_awaitable` it runs in the returned awaitable's
  frame. A shared per-worker cooperative budget yields after at most 64 such
  calls: the operation then suspends, a posted turn lets queued work run, and
  the probe repeats on the executor before any registration. Generic
  callback/token operations retain deferred startup/completion.
* Suspended startup, cancellation, timer resumption, cleanup and release are
  turns of a private serial runner for EACH operation on its explicitly
  supplied executor: one atomic word records requested turns and whether a
  runner is scheduled or running, at most one runner exists per operation, and
  before it exits it drains every request made while it ran. No turn of one
  operation runs concurrently with, or nested inside, another turn of the same
  operation; this replaces an `asio::strand` object per operation and does not
  restrict the executors of channels or of other selections. When the owner is
  the plain `io_context` executor, the operation is its own scheduler
  operation: a turn links the operation itself into that scheduler's queue,
  with no function object and no allocation, and the operation owns itself
  while queued or running; any other executor receives an ordinary function
  object. Associated completion I/O executors are honored and tracked. A
  suspended coroutine can dispatch its continuation on its associated
  executor; immediate generic-token completion remains queued, never inline.
  Dispatch never runs user code while holding channel locks.
* A committed peer's wakeup and a queued completion are continuations of the
  turn that committed them (`asio::defer`, or the equivalent direct queueing
  for the plain `io_context` executor). On a thread that runs the owner's
  scheduler they go to that thread's private queue: no scheduler lock, no
  wakeup signal, no migration to another worker, and the same order between a
  peer's withdrawal and the committing completion that posted wakeups gave.
  From any other thread, or another context, they are ordinary posts.
  Cancellation ingress and the cooperative yield remain posts.
  Payload moves/destructors can run on the caller, committing peer, cleanup or
  result-owning thread; T must be safe to transfer across those contexts (the
  responsibility represented by Send bounds in Rust). Completion-executor
  affinity does not promise that every payload destructor runs there.
* Every operation serves generations: a one-shot operation exactly one, a
  reusable selector one per wait, armed by the owning task only after the
  previous generation has withdrawn every registration, cancelled its timer,
  removed its stop callback and handed its completion out. Cancellation, timer
  and wakeup requests carry the generation they were raised for in monotonic
  atomic marks; a request raised for a finished generation is dropped at its
  ingress and again in the runner, so nothing from one wait can abort, resume
  or complete the next. The reusable-path control raises exactly such stale
  requests, and the mutation control removes each filter.
* An execution-context service keeps suspended operations alive, replacing the
  original infinite timer used solely for lifetime. It has no worker/scheduler.
  A kept operation owns itself through a strong self reference and is linked
  into the service's intrusive list; keeping and forgetting allocate nothing.
  The service mutex protects only those links, taken after channel locks have
  been released. Forget/shutdown release the self references outside that
  mutex, so an operation destructor can withdraw channel registrations without
  lock inversion. A one-shot operation is kept from registration until
  finalization; a reusable selector's state is kept from its first
  registration until the selector's destructor requests a release turn on the
  owner (serialized with any wait in progress) or the context shuts down, so
  later generations touch the registry lock not at all. Ready probes never do.
  The one-shot operation is allocated through Asio's per-thread recycling
  allocator, like Asio's own handler operations; a selector's state is one
  ordinary allocation for its lifetime. Actual finite deadlines still use
  ordinary Asio timers.
* std::stop_token supplies sticky, thread-safe cancellation even before initiation.
  Associated Asio cancellation slots are supported too; their assignment/emission
  needs the same external synchronization as ordinary Asio slots. Slot and stop
  callbacks record the generation they were raised for and request a
  cancellation turn of the operation's runner. They never mutate channel
  queues. An awaiting coroutine's slot handler refers to the selector's state
  directly, because Asio clears that slot when the coroutine resumes through
  the stored completion; a generic handler's slot belongs to the caller and may
  outlive the state, so it holds a weak reference instead.
* Once claimed, a transfer cannot be revoked, even if cancellation is processed
  during payload movement or before user completion. Cancellation that wins before
  commitment consumes nothing. No payload is returned twice.

The alternatives were a common owner/domain (which restricts composition), racing
ordinary receives (which can consume losers), or a new distributed reservation
protocol/runtime. The first two fail the intended select contract; the last adds
substantial machinery and two-party cancellation problems. The bounded internal
locks plus single-winner state follow the mechanism used by Go's channel runtime,
while preserving asynchronous Asio suspension/completion and owner-routed
cancellation. This exception does not authorize mutex-protected application state.

Stop producers, request cancellation/close as appropriate, and run/join all owned
operations before destroying their referenced application state. Internal wait
registrations are intrusive nodes pointing at their owning operation; the
execution-context service owns pending operations. Context teardown withdraws
registrations, discards a reusable selector's stored completion (which lets
Asio unwind the suspended coroutine, so a coroutine that owns its selector does
not keep itself alive through it) and releases those owners outside the
registry lock, even if no completion handler can run. Finite deadlines
also own ordinary timer waits. Channel-only waiting creates no I/O timer/reactor.
The execution context must outlive its active operations, as required by Asio.
A channel itself can survive context teardown, and so can an idle selector.
Do not destroy a context concurrently with peers that can still commit operations
which target it; cancel and join those operations first.

Allocation failure and throwing user handlers follow normal Asio exception
propagation; recovery from resource-allocation failure is not transactional.
Associated allocators and immediate executors are not customized. Ready coroutine
optimization covers `asio::deferred`, `as_tuple(asio::deferred)`, the partial
`asio::as_tuple` token, `use_awaitable` and `as_tuple(use_awaitable)`; other
token adaptors use the ordinary deferred initiation path. No generic
selection over arbitrary third-party async operations is promised: a wrapped
operation must itself provide safe commitment, just as Go select operates on
channels rather than arbitrary function calls.

## Comparison and evidence

Go [select](https://go.dev/src/runtime/select.go) locks participating channels,
randomizes ready-case order, registers waits and removes unsuccessful cases.
Its [channel runtime](https://go.dev/src/runtime/chan.go) claims a sleeping
selection once. This extension uses the same essential active/waiting distinction,
ordered channel locking, single-winner claim and explicit loser withdrawal, adapted
to asynchronous completions rather than a goroutine scheduler.

Crossbeam [distinguishes committed selection from mere readiness](https://docs.rs/crossbeam-channel/latest/crossbeam_channel/struct.Select.html).
Our results are committed transfers. Tokio [select](https://docs.rs/tokio/latest/tokio/macro.select.html)
requires cancellation-safe branches and randomizes its default polling order.
Tokio [send](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Sender.html) may
lose a cancelled payload unless capacity is reserved; our result explicitly
returns uncommitted sends. Kotlin's [selectUnbiased](https://kotlinlang.org/api/kotlinx.coroutines/kotlinx-coroutines-core/kotlinx.coroutines.selects/select-unbiased.html)
also shuffles clauses, while its normal select is biased. Kotlin's prompt
cancellation can report cancellation even after a send; this extension instead
preserves the committed result, matching the no-loss ownership requirement.
This is a comparison of contracts, not a claim of universal performance
superiority or full API equivalence to those runtimes.

```sh
cmake -S csp -B build-csp -DCMAKE_BUILD_TYPE=Debug
cmake --build build-csp
ctest --test-dir build-csp --output-on-failure
python3 csp/tests/mutation_control.py /tmp/asio-select-mutation
python3 csp/tests/lifetime_control.py /tmp/asio-select-lifetime
python3 csp/tests/finalize_withdraw_control.py /tmp/asio-select-withdraw
c++ -std=c++20 -pthread -DASIO_STANDALONE -Iinclude -g -fno-omit-frame-pointer -fsanitize=address,undefined csp/tests/select.cpp -o /tmp/asio-select-asan
/tmp/asio-select-asan
c++ -std=c++20 -pthread -DASIO_STANDALONE -Iinclude -g -fsanitize=thread csp/tests/select.cpp -o /tmp/asio-select-tsan
/tmp/asio-select-tsan
```

The retained upstream_control intentionally exits 1 with --require-preservation
against pristine upstream headers: two ready receives may both consume under
operator||. This is not an upstream contract violation. The new control uses that
same two-ready scenario, verifies only one consumption and reads the original
losing payload. The upstream_probe also preserves the original deadline/close
observations before cleanup; its ordered single-thread scheduling assertions
characterize that executor/platform, not every possible scheduler.

Controls cover heterogeneous independent channels, mixed send/receive, two-sided
rendezvous, no self-match, ready deadline/message boundary, pending timers,
pre-registration cancellation, registered cancellation, cancellation after claim
and INSIDE a gated move before it returns, closure with buffered and suspended
producers, capacity zero, FIFO, admission limits and default, disabled cases,
move-only payload destruction, context teardown, facade destruction, loser cleanup,
reproducibly shuffled branches, and separate contexts on separate threads.
`reusable_path.cpp` covers the reusable selector: one owned operation and no
heap allocation per suspended loop (counted, against the one-shot loop's
operation per suspension), stale cancellation/timer/wakeup requests injected
between and during waits with a second waiter queued behind the selector
(FIFO must survive), simultaneous-use rejection, executor work while
suspended, teardown with a suspended wait, release after the context stopped,
payload and receive-slot ownership, case and option changes between waits,
stop-token cancellation, and cross-thread wakeups with four workers; its
test-only observers are synchronized because operations are constructed on
several threads there. Concurrent stress supplements controlled
events/barriers. Sanitizer logs must be inspected in full; zero exit alone does
not establish absence of diagnostics.

The lifetime control drives two real pending selections and concurrent context
teardown. Its baseline is the exact reviewed commit 227bfbc08d151f45fec9313883e6da6b53683396.
Only a scheduling observer is inserted after the original weak-reference
acquisition; ownership/channel logic is unchanged. This is an explicitly
instrumented scheduling reproduction, distinct from the pristine upstream RED
control. The observer forces the last temporary reference to retire under the
original lock, reproducing self-deadlock in an isolated child which is killed and
joined. The same public scenario completes and has no registrations on the fix.
The production build has no observer call. The Linux workflow runs a GCC job and
a Clang job; the GCC job also compiles the unmodified reviewed header with
completion_executor.cpp and verifies bad_executor, then executes the same control
successfully with the current header. Clang, like the local macOS compiler,
evaluates the arguments in the other order and cannot expose that choice, so the
RED half of that control is GCC-only.

The withdrawal control preserves the pre-fix working implementation and gates
a real peer, inside a channel's mutex, between unlinking a wait node and
claiming its operation; the operation is then committed on another channel so
its finalize runs while the peer still holds the node. The verdict is the
delivered order of events observed at identical points on every input:
operation destruction, the peer's gate, and the attempt and acquisition of
the held channel mutex inside the channel-lock primitive itself (hooks in
`channel_locks`, and in the frozen input's per-node withdrawal lock as well).
RED is destruction delivered before any attempt on that mutex (the pre-fix
input). GREEN requires attempt, then the peer's release, then acquisition,
then destruction, so a finalize that merely announces locking, or withdraws
without that mutex, is RED whatever markers it carries; a marker before the
withdrawal step is recorded only. No timing threshold is involved; both paths
join every thread, and the RED path releases the peer with the node abandoned
so cleanup never dereferences freed memory. An additional sanitizer
corroboration permits that dereference and reproduces the original
use-after-free; it is separate from the normal regression.


## Local Conan 2

The original recipe is header-only, offers no additional build options, and exports
an exact local source snapshot. It performs no downloads. Recipe revisions identify
exported content; neither Conan Center nor a custom remote is required.

```sh
conan create . --user=k-nuth --channel=csp -s compiler.cppstd=20 --no-remote
```

External conanfile.txt:

```ini
[requires]
asio/1.38.3-kth.1@k-nuth/csp
[generators]
CMakeDeps
CMakeToolchain
```

```sh
conan install . -of build -s compiler.cppstd=20 --build=never --no-remote
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build
./build/consumer
```

Use find_package(asio CONFIG REQUIRED), target asio::asio, includes <asio/...> and
ASIO_STANDALONE from that target. Headers, ipp implementations and upstream
BSL/COPYING are packaged. Windows advertises ws2_32/mswsock; Linux/FreeBSD advertise
pthread. Local sanitizer validation uses macOS/Apple Clang; the Linux workflow
builds the suite and runs every Python control with current GCC (16.2, the
official GCC container image) and Clang (23, from apt.llvm.org), asserting the
compiler versions it runs, and preserves the GCC completion regression.
Other platforms need their own validation. test_package executes normal Asio and heterogeneous
coroutine selection. An external consumer's compiler dependency output must show
all Asio headers coming from this local package, and packaged header bytes must
match the tested checkout.

For future Knuth adoption, replace its Conan reference with the fork reference and
pin the recipe revision, keeping asio::asio. Knuth and its dependencies are not
modified by this work. PR evidence records the exact final tested commit/tree,
package revision and completed review status separately from the baseline.

## Reproducible performance comparison

The separate [benchmark repository](https://github.com/k-nuth/asio-csp-benchmarks)
contains the C++ variants, Go, Tokio and Crossbeam comparisons, locked language
dependencies, build/run scripts, measurements and implementation analysis.
No comparison toolchain, benchmark output or measurement JSON is part of this
Asio repository or its Conan package. Cross-language performance parity is not
claimed; the comparison repository records each scenario and its limitations.

The performance changes and their comparison with pinned Rust/Go sources are
explained in the benchmark repository. Deterministic
`ready_path`/`pending_path` controls distinguish the frozen original mechanisms
from the current ones without timing thresholds; `immediate_path` distinguishes
the preceding deferred-token mechanism (an executor turn and an owned
allocation per ready selection) from the current in-frame commitment.
`awaitable_path` covers the coroutine path's ownership,
cancelled/closed/disabled/deadline cases, explicitly different owner, deferred
suspension, cooperative cancellation, larger selections, and channel destruction
inside completion, with both the `use_awaitable` and the lazy `deferred` token
families, plus the lazy operation's laziness and generic-token invocation.
`reusable_path` distinguishes the preceding one-shot mechanism (an owned
operation per suspended selection, observed through the same construction hook
inserted into the frozen third-iteration header) from the reusable selector
(none), and `mutation_control.py` removes each generation filter in turn.

On the measured GCC 16.1/Linux toolchain, a direct structured binding of a
`co_await` tuple that lives across a later suspension can retain ownership. This
also reproduces with ordinary standard-C++ coroutines and no Asio/channel code.
Use named tuple storage for that pattern (`auto completion = co_await ...;
auto& [ec, result] = completion;`). The retained compiler-only control and full
LeakSanitizer evidence distinguish this toolchain finding from the primitive; see
the benchmark repository. No diagnostic is suppressed.
