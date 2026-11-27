# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Disposable driver: run one phase and retain bounded local streams, without live output."""

import base64
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import sys
import time


class Window:
    def __init__(self, head, tail):
        self.head_limit, self.tail_limit = head, tail
        self.head, self.tail = bytearray(), bytearray()
        self.size = 0

    def add(self, data):
        self.size += len(data)
        split = min(self.head_limit - len(self.head), len(data))
        self.head.extend(data[:split])
        self.tail.extend(data[max(split, len(data) - self.tail_limit):])
        del self.tail[:max(0, len(self.tail) - self.tail_limit)]

    def value(self):
        lost = self.size - len(self.head) - len(self.tail)
        if not lost:
            return bytes(self.head + self.tail)
        first = self.head.rpartition(b"\n")[0]
        last = self.tail.partition(b"\n")[2]
        return bytes(first + f"\n[... {self.size - len(first) - len(last)} bytes omitted ...]\n".encode() + last)


class Stream:
    def __init__(self):
        self.log = Window(0, 128 * 1024)
        self.diagnostics = Window(16 * 1024, 16 * 1024)
        self.pending = b""
        self.discard = False
        self.end = None

    def feed(self, data):
        self.log.add(data)
        lines = (self.pending + data).split(b"\n")
        self.pending = lines.pop()
        for line in lines:
            if not self.discard:
                tag = re.sub(rb"\x1b\[[0-9;]*m", b"", line).strip()
                if tag.startswith(b"=== Databricks failure summary:"):
                    self.end = b"=== End Databricks failure summary ==="
                elif self.end is None and tag == b"=== Databricks GPU diagnostics ===":
                    self.end = b"=== End Databricks GPU diagnostics ==="
                elif self.end is None and re.fullmatch(rb"=+ (?:FAILURES|ERRORS) =+", tag):
                    self.end = b"pytest"
                elif tag.startswith(b"----- First Spark failure records:"):
                    self.end = b"end-of-stream"
                if self.end or tag.startswith((b"[ERROR]", b"Caused by:")):
                    self.diagnostics.add(line + b"\n")
                if tag == self.end or (self.end == b"pytest" and b"short test summary info" in tag):
                    self.end = None
            self.discard = False
        if len(self.pending) > 65536 or self.discard:
            self.pending = b""
            self.discard = True


def capture(destination, command):
    streams = [Stream(), Stream()]
    with subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE) as proc, selectors.DefaultSelector() as ready:
        for pipe, stream in zip((proc.stdout, proc.stderr), streams):
            ready.register(pipe, selectors.EVENT_READ, stream)
        drain_until = None
        while ready.get_map():
            for key, _ in ready.select(0.25):
                data = os.read(key.fd, 65536)
                if data:
                    key.data.feed(data)
                else:
                    if key.data.pending:
                        key.data.feed(b"\n")
                    ready.unregister(key.fileobj)
            # The test supervisor cleans up descendants. An inherited pipe must not hold
            # the wrapper open after the actual phase has already returned.
            if proc.poll() is not None:
                drain_until = drain_until or time.monotonic() + 2
                if time.monotonic() >= drain_until:
                    break
        rc = proc.wait()
    try:
        chunks = [s.log.value() for s in streams] + [s.diagnostics.value() for s in streams]
        destination.write_text(json.dumps([base64.b64encode(c).decode("ascii") for c in chunks]), encoding="ascii")
    except OSError:
        pass  # Missing optional evidence does not change the command's result.
    return rc if rc >= 0 else 128 - rc


if __name__ == "__main__":
    sys.exit(capture(Path(sys.argv[1]), sys.argv[2:]))
