# Historical regression inputs

These C++ headers preserve the exact pre-change inputs needed by the retained
RED/GREEN mechanism and lifetime controls. Their byte hashes are checked by
`performance_control.py` and `finalize_withdraw_control.py`; test-only
observation overlays are created outside the source tree. They are not
installed by Conan.

`reference-v4` is the uncommitted implementation before the withdrawal
use-after-free fix. Its hash is pinned by `finalize_withdraw_control.py`, which
adds only observation hooks (operation destruction, the peer's claim gate, the
marker before withdrawal, and the attempt/acquisition of each channel mutex in
its lock primitives) and runs the same behavioral scenario against that input
and the current header. The defective input must report that the operation
was destroyed before any attempt on the channel mutex another thread still
held with its wait node; the current implementation must attempt that mutex,
acquire it only after the peer's release, and destroy the operation afterwards.
Both paths join all threads.

The comparison harness, Go/Rust sources and measurement artifacts live in the
separate `k-nuth/asio-csp-benchmarks` repository. These fixtures are library
regression inputs, not benchmark engines.
