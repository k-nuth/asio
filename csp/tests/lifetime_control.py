#!/usr/bin/env python3
"""Reproduce the reviewed lifetime deadlock with controlled scheduling.

The baseline header comes from the exact reviewed commit. Only a scheduling
observer is inserted after weak_ptr::lock; no ownership or channel logic changes.
The production fix has the same test-only observer at its non-owning inspection.
Both binaries drive public channel selection and concurrent context teardown.
The deadlocked baseline runs in a child process which is killed and joined.
"""
from pathlib import Path
import os
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]).resolve()
base = "227bfbc08d151f45fec9313883e6da6b53683396"
source = subprocess.check_output(["git", "show", base + ":include/asio/experimental/channel_select.hpp"], cwd=root, text=True)
needle = "auto p = w.op.lock(); return !p || p.get() == op;"
assert source.count(needle) == 1
observer = """auto p = w.op.lock();
#if defined(ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE)
      if (p) ASIO_CHANNEL_SELECT_TEST_OBSERVE_REMOVE(p.get(), op, true);
#endif
      return !p || p.get() == op;"""
overlay = out / "baseline/asio/experimental"
overlay.mkdir(parents=True, exist_ok=True)
(overlay / "channel_select.hpp").write_text(source.replace(needle, observer))
compiler = os.environ.get("CXX", "c++")
for mode in ("baseline", "fixed"):
    binary = out / (mode + "-lifetime")
    command = [compiler, "-std=c++20", "-pthread", "-DASIO_STANDALONE"]
    if mode == "baseline":
        # The reviewed commit used the unmodified upstream awaitable implementation.
        command += ["-I" + str(out / "baseline"), "-I" + str(root / "csp/tests/fixtures/reference-upstream")]
    command += ["-I" + str(root / "include"), str(root / "csp/tests/lifetime.cpp"), "-o", str(binary)]
    subprocess.run(command, check=True)
    try:
        run = subprocess.run([str(binary)], capture_output=True, text=True, timeout=8)
    except subprocess.TimeoutExpired as error:
        output = (error.stdout or b"")
        if isinstance(output, bytes):
            output = output.decode()
        (out / (mode + ".log")).write_text(output + "\nchild killed and joined after deadlock timeout\n")
        assert mode == "baseline" and "retained=1" in output and "peer context released" in output
    else:
        (out / (mode + ".log")).write_text(run.stdout + run.stderr)
        assert mode == "fixed" and run.returncode == 0 and "no registered waits" in run.stdout
print("Controlled lifetime baseline RED (deadlock); same public scenario GREEN on fix")
