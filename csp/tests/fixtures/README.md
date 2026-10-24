# Historical regression inputs

These C++ headers preserve the exact pre-change inputs needed by the retained
RED/GREEN mechanism and lifetime controls. Their byte hashes are checked by
`performance_control.py` and `finalize_withdraw_control.py`; test-only
observation overlays are created outside the source tree. They are not
installed by Conan.

`reference-v4` is the uncommitted implementation before the withdrawal
use-after-free fix. Its hash is pinned by `finalize_withdraw_control.py`, which
adds only observation hooks and runs the same behavioral scenario against
that input and the current header. The defective input must report that the
operation was destroyed while another thread still held its wait node; the
current implementation must keep it alive. Both paths join all threads.

The comparison harness, Go/Rust sources and measurement artifacts live in the
separate `k-nuth/asio-csp-benchmarks` repository. These fixtures are library
regression inputs, not benchmark engines.
