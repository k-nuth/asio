#!/usr/bin/env python3
"""Reject a faithful losing-consumption mutation in an isolated include overlay.

Usage: python3 csp/tests/mutation_control.py OUTPUT_DIRECTORY
Production source is never modified. Compiler defaults to CXX or c++.
"""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
out = Path(sys.argv[1]).resolve()
header = root / "include/asio/experimental/channel_select.hpp"
original = header.read_bytes()
anchor = "for (auto i : order) if (attempt(i, retired)) { ready = true; break; }"
text = original.decode()
assert text.count(anchor) == 1
text = text.replace(anchor, "for (auto i : order) if (attempt(i, retired)) { ready = true; }")
overlay = out / "asio/experimental"
overlay.mkdir(parents=True, exist_ok=True)
(overlay / header.name).write_text(text)
binary = out / "select-mutated"
subprocess.run([os.environ.get("CXX", "c++"), "-std=c++20", "-pthread",
                "-DASIO_STANDALONE", "-I" + str(out), "-I" + str(root / "include"),
                str(root / "csp/tests/select.cpp"), "-o", str(binary)], check=True)
with (out / "mutation.log").open("w") as log:
    result = subprocess.run([str(binary)], stdout=log, stderr=subprocess.STDOUT, timeout=15)
    log.write(f"\nexit={result.returncode}\n")
assert result.returncode != 0, "control failed to discriminate"
assert header.read_bytes() == original
print("Mutation rejected; unchanged header sha256=" + hashlib.sha256(original).hexdigest())
