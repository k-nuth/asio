# Independent channel selection for standalone Asio

This fork's `1.38.3` development branch starts at stable upstream tag
`asio-1-38-2`, commit `8806a6803cde7054c3049d3666d3ec36786568c5` (annotated tag
object `12b52a54a25d1861b037bc5d9810f92a7151d920`). The fork package is
`asio/1.38.3-kth.1@k-nuth/csp`; it is not an upstream 1.38.3 release. Existing
Asio headers, ASIO_VERSION (103802), channels and operator|| remain unchanged.

## API

Include `<asio/experimental/channel_select.hpp>` with C++20. Ordinary Asio usage
retains its existing language requirements. No new runtime is introduced.

```cpp
using namespace asio::experimental;
select_channel<Job> jobs(16);
select_channel<Block> blocks(4);
select_channel<std::unique_ptr<Reply>> replies(0); // Rendezvous.
auto ex = co_await asio::this_coro::executor;
auto [ec, selected] = co_await make_select(
    receive(jobs),
    receive(blocks),
    send(replies, std::move(reply)),
    after(asio::steady_timer::clock_type::now() + timeout)
).async_wait(ex, {}, stop_token, asio::as_tuple(asio::use_awaitable));
// selected.index identifies one case; selected.value<I>() is its typed optional.
// A losing send retains its payload in selected.value<I>(), including on errors.
```

Channels are independent objects, can carry different types, and need no common
executor or strand. Selections can wait on overlapping sets of channels from
different io_context instances running on different threads. A channel does not
borrow any context; it can outlive a completed or destroyed context.

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

Use `as_tuple(use_awaitable)` when send payloads must survive an error. Ordinary
throwing `use_awaitable` discards result arguments on error, as it normally does.
Cancellation does not silently drop a send payload inside the primitive.

`receive(channel).when(false)`, `send(channel, value).when(false)` and
`after(time).when(false)` disable a branch for this selection. Payload expressions
are still evaluated once when building cases. With every branch disabled the
operation waits for cancellation/deadline, or returns would_block in nonblocking
mode. Empty selections are compile-time errors.

`channel_select_options` provides:

* Default unbiased arbitration: shuffle all cases before inspecting readiness;
  each ready branch has equal pseudorandom priority. As in Go, this is not a bound
  on starvation under adversarial scheduling. `random_seed` optionally supplies
  reproducible arbitration. No process-wide selection or RNG mutex is used.
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
  time, commits at most one case, and publishes no registrations. If it must
  suspend, startup/withdrawal take all participating channel mutexes in total
  address order, deduplicating repeated channels. Channel arbitration has no
  global coordinator/domain.
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
* Registration withdrawal uses non-owning identities and weak expiration checks.
  Matching keeps every acquired operation reference in a retirement collection
  declared outside the channel-lock scope, including failed claims and closure.
  The collection allocates an empty slot before acquiring a weak reference, so
  exception unwinding also releases last owners only after unlocking. An operation
  destructor can therefore deregister without recursively locking a held mutex.
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
  strand, timer or wait registrations. A shared per-worker cooperative budget
  yields after at most 64 such calls, before trying/committing the next case.
  Generic callback/token operations retain deferred startup/completion.
* Suspended cancellation, timer handling and cleanup run on a private strand
  for EACH operation, derived from its explicitly supplied executor. This does not restrict
  the executors of channels or of other selections. Associated completion I/O
  executors are honored and tracked. All losing registrations are removed under
  the ordered channel locks before completion. A suspended coroutine can dispatch
  its continuation on its associated executor; immediate generic-token completion
  remains posted. Dispatch never runs user code while holding channel locks.
  Payload moves/destructors can run on the caller, committing peer, cleanup or
  result-owning thread; T must be safe to transfer across those contexts (the
  responsibility represented by Send bounds in Rust). Completion-executor
  affinity does not promise that every payload destructor runs there.
* An execution-context service keeps suspended operations alive, replacing the
  original infinite timer used solely for lifetime. It has no worker/scheduler.
  Its mutex protects only owning references, acquired after channel locks have
  been released. Forget/shutdown retire references outside that mutex, so an
  operation destructor can withdraw channel registrations without lock inversion.
  Disjoint pending operations in one context share this lifetime-registry lock;
  ready probes do not. Actual finite deadlines still use ordinary Asio timers.
* std::stop_token supplies sticky, thread-safe cancellation even before initiation.
  Associated Asio cancellation slots are supported too; their assignment/emission
  needs the same external synchronization as ordinary Asio slots. Slot callbacks
  record a request in an atomic ingress flag and post cancellation to the operation owner. They never mutate channel queues.
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
registrations are weak references; the execution-context service owns pending
operations. Context teardown releases those owners outside its registry lock and
removes registrations even if no completion handler can run. Finite deadlines
also own ordinary timer waits. Channel-only waiting creates no I/O timer/reactor.
The execution context must outlive its active operations, as required by Asio.
A channel itself can survive context teardown.
Do not destroy a context concurrently with peers that can still commit operations
which target it; cancel and join those operations first.

Allocation failure and throwing user handlers follow normal Asio exception
propagation; recovery from resource-allocation failure is not transactional.
Associated allocators and immediate executors are not customized. Ready coroutine
optimization covers `use_awaitable` and `as_tuple(use_awaitable)`; other token
adaptors use the ordinary deferred initiation path. No generic
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
Concurrent stress supplements controlled events/barriers. Sanitizer logs must be
inspected in full; zero exit alone does not establish absence of diagnostics.

The lifetime control drives two real pending selections and concurrent context
teardown. Its baseline is the exact reviewed commit 227bfbc08d151f45fec9313883e6da6b53683396.
Only a scheduling observer is inserted after the original weak-reference
acquisition; ownership/channel logic is unchanged. This is an explicitly
instrumented scheduling reproduction, distinct from the pristine upstream RED
control. The observer forces the last temporary reference to retire under the
original lock, reproducing self-deadlock in an isolated child which is killed and
joined. The same public scenario completes and has no registrations on the fix.
The production build has no observer call. The GCC workflow also compiles the
unmodified reviewed header with completion_executor.cpp and verifies bad_executor,
then executes the same control successfully with the current header. The local
macOS compiler alone cannot expose that argument-evaluation-order choice.


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
pthread. Local sanitizer validation uses macOS/Apple Clang; a targeted Linux/GCC
workflow checks compiler portability and the preserved completion regression.
Other platforms need their own validation. test_package executes normal Asio and heterogeneous
coroutine selection. An external consumer's compiler dependency output must show
all Asio headers coming from this local package, and packaged header bytes must
match the tested checkout.

For future Knuth adoption, replace its Conan reference with the fork reference and
pin the recipe revision, keeping asio::asio. Knuth and its dependencies are not
modified by this work. PR evidence records the exact final tested commit/tree,
package revision and completed review status separately from the baseline.

## Reproducible performance comparison

See [bench/README.md](bench/README.md) for the frozen original C++ implementation,
current C++, Go, Tokio and Crossbeam benchmarks, locked dependencies, checks,
measurement limits and retained initial observations. Performance parity has
not been achieved by the original implementation.

The performance changes and their comparison with pinned Rust/Go sources are
explained in [bench/IMPLEMENTATION.md](bench/IMPLEMENTATION.md). Deterministic
`ready_path`/`pending_path` controls distinguish the frozen original mechanisms
from the current ones without timing thresholds. `awaitable_path` covers the
optimized coroutine path's ownership, cancelled/closed/disabled/deadline cases,
explicitly different owner, deferred suspension, cooperative cancellation, larger
selections, and channel destruction inside completion.

On the measured GCC 16.1/Linux toolchain, a direct structured binding of a
`co_await` tuple that lives across a later suspension can retain ownership. This
also reproduces with ordinary standard-C++ coroutines and no Asio/channel code.
Use named tuple storage for that pattern (`auto completion = co_await ...;
auto& [ec, result] = completion;`). The retained compiler-only control and full
LeakSanitizer evidence distinguish this toolchain finding from the primitive; see
[bench/IMPLEMENTATION.md](bench/IMPLEMENTATION.md). No diagnostic is suppressed.
