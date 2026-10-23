# Cross-language select benchmark

This harness compares the fork's public coroutine API with Go's built-in
`select`, Rust Crossbeam's `select!`, and Tokio's `select!` over bounded MPSC
channels. All payloads are unsigned 64-bit integers. Each selector has three
branches and uses the implementation's default randomized arbitration.

This is a throughput benchmark, not a proof of semantic equivalence or fairness.
A reported nanoseconds/message figure is elapsed batch time divided by delivered
messages; it is not an individual message's latency or a p99 measurement.

## Scenarios

- `ready`: three capacity-one channels each contain a message. Receive from one
  selected channel and immediately refill that same channel. All three branches
  are ready on every iteration. Check the received value and the final three
  buffered messages.
- `mixed`: receive from the first channel, send to the second, or receive from
  the third. Immediately refill a winning receive or drain a winning send.
  All branches are ready. The final buffers must contain exactly two messages.
  Tokio uses `reserve()` and sends only after winning, preserving ownership when
  the branch loses.
- `fanin`: three producers each send `n` sequential values on separate bounded
  channels; one consumer receives `3*n` values through selection. Every received
  value must equal the next sequence number for its channel. Join all producers
  and verify every channel has delivered exactly `n` messages and is empty.
  Capacities are 0 (rendezvous), 1, and 64. Asio additionally checks that no
  registrations remain after completion.

Go, Asio and Tokio run with one or four execution workers. Crossbeam uses four
OS threads (three producers plus the consumer) for fan-in; its blocking sends
cannot run with this topology on one worker. Tokio bounded MPSC requires capacity
at least one, so it has no capacity-zero result. Crossbeam's ready/mixed cases
use only the calling thread. The async runtimes and their ancillary threads are
not identical implementations.

Asio uses one `make_select(send(...))` operation for each producer send, which is
its current public async sending interface. Go and Rust use their ordinary send
operations. Asio's channel synchronization, executor dispatch, operation
allocation and cancellation machinery are included; checks and immediate
refills/drains are included in every implementation. This compares usable APIs,
not isolated queue instructions.

## Reproduce

The benchmark retains eight engines: `asio-baseline` (frozen original C++),
`asio-v1` and `asio-v1-reuse` (the preceding optimized version, frozen at
`6c107b9dc076532aaeaa5cdd74991afff93ded6a`), `asio` and `asio-reuse` (the new
iteration), `go`, `crossbeam`, and `tokio`. Reuse variants transfer owning case
handles between iterations. The snapshots and hashes are documented in
[reference/README.md](reference/README.md) and
[reference-v1/README.md](reference-v1/README.md). All C++ engines compile the same
benchmark source and optimization flags; only the header overlay and the explicit
reuse macro differ.

Use a release C++20 compiler, Python 3, Go, and Cargo/Rust. `Cargo.lock` pins all
Rust packages. Nothing is added to the normal Asio build or Conan package tests.
From the repository root (use `g++` on Linux or `clang++` on macOS):

```sh
export PATH="$HOME/.cargo/bin:$PATH"
python3 csp/bench/build.py --cxx c++ --go /path/to/go
python3 csp/bench/run.py --n 30000 --repeats 7
```

`build.py` records compiler/runtime versions, build commands, and input source
hashes. No dependencies are fetched from mutable branches; Cargo downloads its
locked registry versions. The script requires an existing Go toolchain and does
not install one globally. `run.py` rejects binaries whose recorded input sources
have changed. Use `--engines asio-v1,asio` for the latest paired C++ comparison,
`--bin-dir /path/to/build` to select build output, and `--out /path/to/results`
to preserve distinct machines or revisions. On Linux, optional `taskset -c 0-3
python3 csp/bench/run.py ...` constrains the runner and inherited workers alike;
recorded metadata includes the actual affinity.

The initial Go toolchain was downloaded from the official Go distribution,
verified against the SHA256 from its release JSON, and extracted under `/tmp`.
It did not replace a global toolchain.

The runner launches one benchmark process at a time, shuffles trials with a
fixed orchestration seed, and retains all observations in `results/raw.json`.
It runs one separate discarded warmup process per configuration to warm system
caches; runtime initialization within each measured process is not warmed by
that process. Runtime/channel/task setup and OS-thread creation are outside the
internal timer. Timed fan-in includes releasing a start gate, executing the
work, and joining the producers/workers. The ready/mixed cases include their
loops and executor driving. Process startup, compilation and printing results
are outside the timer. Threads and producers are joined before inspecting
observations. Each child has a 120-second timeout; an invariant failure or timeout
fails the run rather than contributing a successful sample.

`summary.json` and `table.md` contain median, minimum and maximum across trials.
`metadata.json` records the library commit/tree, source and binary hashes,
compiler/runtime versions, machine and parameters. These are local observations
on one machine without CPU affinity, a fixed frequency, or a dedicated idle host;
repeat on Linux and deployment hardware before extrapolating. No allocation,
CPU-time, memory or tail-latency profile is claimed by these figures.

References: [Go selection](https://go.dev/ref/spec#Select_statements),
[Crossbeam selection](https://docs.rs/crossbeam-channel/0.5.17/crossbeam_channel/macro.select.html),
[Tokio selection](https://docs.rs/tokio/1.54.1/tokio/macro.select.html), and
[Tokio bounded-channel capacity](https://docs.rs/tokio/1.54.1/tokio/sync/mpsc/fn.channel.html).

## Initial local results

Measured library commit: `035b2c7850e26f8d8ecd826899b76ae9bc9db9ae`
(tree `81928be3764bd7f7139911bf1e55b25fb4a7add5`), with the benchmark
sources recorded separately by SHA256 in the results metadata. Apple M6,
macOS 27.0.1 arm64, Apple Clang 21.0.0 (`-O3 -DNDEBUG`), Go 1.27.3,
Rust 1.99.0, Crossbeam Channel 0.5.17 and Tokio 1.54.1. Seven trials per
configuration, 30,000 selections for ready/mixed and 90,000 deliveries for fan-in.

This frozen original implementation is substantially slower in every measured configuration.
For example, the ready path is about 563 times slower than Go and 1,055 times
slower than Tokio. These results do not satisfy a performance-parity objective.
They are a baseline for investigating and changing the implementation, not an
acceptance result. No primitive code was changed during these initial measurements. The table uses
`asio` for that original run; new runs identify this version as `asio-baseline`.

| Engine | Scenario | Capacity | Workers | Median ns/message | Min–max |
|---|---|---:|---:|---:|---:|
| asio | ready | 1 | 1 | 22351.4 | 22047.4–23430.4 |
| asio | mixed | 1 | 1 | 22473.4 | 22084.5–22870.8 |
| asio | fanin | 0 | 1 | 22459.1 | 22365.2–24754.7 |
| asio | fanin | 0 | 4 | 5457.5 | 5440.9–5550.3 |
| asio | fanin | 1 | 1 | 22444.2 | 22366.8–23387.3 |
| asio | fanin | 1 | 4 | 4732.3 | 4534.6–4790.0 |
| asio | fanin | 64 | 1 | 22429.5 | 22365.0–25812.2 |
| asio | fanin | 64 | 4 | 4674.0 | 4135.2–4708.2 |
| go | ready | 1 | 1 | 39.7 | 39.4–42.5 |
| go | mixed | 1 | 1 | 39.7 | 39.3–42.9 |
| go | fanin | 0 | 1 | 101.9 | 100.6–104.6 |
| go | fanin | 0 | 4 | 203.1 | 185.9–210.8 |
| go | fanin | 1 | 1 | 85.9 | 81.4–87.3 |
| go | fanin | 1 | 4 | 157.9 | 155.5–161.7 |
| go | fanin | 64 | 1 | 42.9 | 41.7–45.6 |
| go | fanin | 64 | 4 | 115.2 | 98.5–118.8 |
| crossbeam | ready | 1 | 1 | 20.9 | 20.2–24.1 |
| crossbeam | mixed | 1 | 1 | 21.1 | 20.5–21.2 |
| crossbeam | fanin | 0 | 4 | 1377.8 | 1297.9–1401.0 |
| crossbeam | fanin | 1 | 4 | 128.3 | 86.9–429.9 |
| crossbeam | fanin | 64 | 4 | 54.0 | 51.4–66.0 |
| tokio | ready | 1 | 1 | 21.2 | 20.9–24.3 |
| tokio | mixed | 1 | 1 | 22.5 | 22.0–23.7 |
| tokio | fanin | 1 | 1 | 70.5 | 69.1–70.9 |
| tokio | fanin | 1 | 4 | 263.6 | 260.9–268.2 |
| tokio | fanin | 64 | 1 | 29.1 | 28.6–30.8 |
| tokio | fanin | 64 | 4 | 65.4 | 63.8–66.8 |

A separate macOS `sample` run (2 seconds, 1ms sampling, 300,000 ready
iterations) placed 1,573 of 1,655 main-thread samples in `kevent` beneath
Asio's scheduler/reactor. This locates the dominant sampled path on this
machine; it does not establish a portable root cause or predict the benefit
of a particular change. Profiled execution is excluded from the timing table.

Supplementary short runs of ready, mixed, and fan-in with capacities 0/1/64
passed with C++ ASan+UBSan, C++ TSan and Go's race detector. Sanitizer executions
are excluded from performance measurements. The original observations and metadata are retained in
[observations/macos-original](observations/macos-original/). The raw profiler log
is local evidence rather than a portable regression assertion. Rust was exercised by the ordinary checked harness;
no Rust sanitizer run is claimed.

## Optimized comparison on macOS and Linux

The full six-engine series is retained with every sample, source hash and build
command in [macos-current](observations/macos-current/) and
[linux-current](observations/linux-current/). The measured commits are
`f0ba53bf3a0d5dc8e07558ab227f978832bb4ed0` (macOS) and
`e8ebfc6e1aa069c780b67b4c7020cf0a50246457` (Linux). Every compiled project input
has the same recorded SHA256 in both runs; the later commit adds the compiler
lifetime characterization and named-storage test without changing the primitive
or benchmark inputs. Each run has 301 samples (43 configurations x 7 repetitions).
macOS uses `n=30000`; Linux uses `n=300000` and inherited CPU affinity 0-3 on a
Ryzen 9 9950X3D. The C++ header SHA256 is
`f299765376c3cecca2d5520dac5c7858c8628df3b07b583f39d73f4eba5a3612`.

| Machine/scenario | Original C++ | Current C++ | Go | Tokio | Crossbeam |
|---|---:|---:|---:|---:|---:|
| Mac, ready, 1 worker | 22284.1 | 74.5 | 40.2 | 21.4 | 20.3 |
| Mac, capacity 64, 4 workers | 4877.3 | 417.7 | 118.1 | 64.7 | 53.9 |
| Linux, ready, 1 worker | 674.5 | 79.8 | 58.7 | 41.0 | 28.7 |
| Linux, capacity 64, 4 workers | 2653.0 | 374.2 | 63.5 | 44.5 | 59.2 |

All figures are median ns/message (lower is better), derived from batch elapsed
time; they are not per-message latency percentiles. The improvement is real in
these controls, but the current implementation still does not meet performance
parity with Go or Rust. The complete tables include rendezvous, capacity-one and
one-worker fan-in results; do not infer uniform gains from this compact subset.

A separate Linux `strace -c` observation over 30,000 ready selections counted
60,000 `epoll_wait` calls with the frozen header and none with the current one.
These instrumented executions are excluded from throughput measurements. Their
logs are in the Linux observations directory. The implementation techniques,
guarantees retained, and remaining costs are explained in
[IMPLEMENTATION.md](IMPLEMENTATION.md).

`asio-reuse` is an additional public-API experiment, not a replacement for `asio`.
It moves the owning case handles from each result into the next selection, clears
old receive values, and replenishes a winning send. It introduces no borrowed
channel references or weaker lifetime rules. It tests the hypothesis that
reconstructing handles/refcounts dominates the remaining cost. The first paired
Mac experiment found essentially unchanged ready throughput (75.0 versus 75.2
ns/message); it does not justify attributing the remaining gap to refcounts.
The plain engine remains in every default comparison.

## Build-input integrity control

The build metadata format records all project headers, the Cargo manifest/lock,
benchmark sources, the build script, compiler versions, commands, and executable
hashes. The runner rejects obsolete metadata, changed inputs, or changed binaries
before executing measurements and checks recorded sources again at completion.
Historical observations above retain their original metadata format; rebuild
before collecting new observations with the current harness.

With the same toolchains used for measurement, reproduce the retained original
pipeline defect and the current rejection without editing production sources:

```sh
python3 csp/tests/bench_input_control.py /tmp/asio-bench-input-control \
  --go /path/to/go --cxx c++
```

The output directory must be new. The control compiles real benchmark binaries
in disposable clones, changes Cargo's release optimization level and the C++
optimization flag after building, and runs the actual original/current runners.
The original accepts the stale binaries; the current runner rejects both inputs
and also refuses legacy metadata. This control needs Go/Cargo as well as C++;
the ordinary library CTest suite does not.

The complete paired reuse experiment is retained in
[macos-reuse](observations/macos-reuse/) and
[linux-reuse](observations/linux-reuse/). Linux likewise showed no ready-path
improvement (78.6 versus 82.6 ns/message), while capacity-64/four-worker fan-in
improved in that series (395.7 versus 301.4). These are workload-dependent
observations, not evidence that reuse resolves the remaining gap.

## Second iteration: paired measurements

The new iteration replaces default arbitration's Mersenne Twister with a small
Xorshift64* generator, retaining full `std::shuffle` and the original explicit
seed behavior. It also avoids installing a stop callback for a non-stoppable
token. See [IMPLEMENTATION.md](IMPLEMENTATION.md) for the source comparison.

Seven-engine paired series retain all 357 observations on each host:
[Mac](observations/macos-v2/table.md) and [Labruna](observations/linux-v2/table.md).
Mac uses 100,000 iterations and Linux 300,000, seven repetitions, randomized
process order, the same toolchains and Linux affinity as the preceding series.
The old original engine remains available and its previous measurements are
retained; this paired series focuses on the preceding optimized version.
All 643 recorded build-input hashes agree across hosts. Metadata records the
working-tree patch over `6c107b9` on Mac and `e8ebfc6` on Labruna, rather than
claiming those base commits already contained the new header. The input hashes
identify the measured code.

| Host/scenario | C++ v1 | C++ v2 | Go | Tokio | Crossbeam |
|---|---:|---:|---:|---:|---:|
| Mac, ready | 75.5 | 58.5 | 39.7 | 21.8 | 21.4 |
| Mac, fan-in 64/4 workers | 420.7 | 369.3 | 113.1 | 63.0 | 54.0 |
| Labruna, ready | 78.4 | 75.9 | 59.5 | 41.0 | 27.9 |
| Labruna, fan-in 64/4 workers | 363.6 | 372.4 | 64.6 | 43.7 | 59.1 |

Values are median ns/message. Mac's ready path improves about 22%; Linux's about
3%. Linux's four-worker buffered fan-in does not improve consistently. This is
an architecture-dependent arbitration improvement, not performance parity or a
solution to suspended-operation allocation and scheduler costs. The reuse pairs,
capacity-zero channels and all other scenarios are included in the full tables.
