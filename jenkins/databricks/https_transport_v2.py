# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Databricks HTTPS command transport for trusted CI workloads (no SSH or CI-provider dependency)."""

from contextlib import contextmanager
import hashlib
import http.client
import json
import re
import sys
import time
from urllib.parse import urlencode


CHANNELS = ("stdout", "stderr", "diagnostics")
CLUSTERS = "/api/2.1/clusters"
COMMANDS = "/api/1.2"


class TransientRequest(RuntimeError):
    """A temporary API or network failure, not a workload failure."""


class MissingCluster(RuntimeError):
    """The workspace confirmed that this specific cluster no longer exists."""


def observe(call):
    for delay in (2, 5, None):
        try:
            return call()
        except TransientRequest:
            if delay is None:
                raise
            time.sleep(delay)


def workspace_api(host, token):
    origin = re.fullmatch(r"https://([a-z0-9.-]+\.(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com))/?", host)
    if not origin or not token or not token.isascii() or not token.isprintable():
        raise RuntimeError("Expected one HTTPS Databricks workspace and a token")

    def request(method, path, **fields):
        # Connect directly to the configured origin; never follow a redirect with the token.
        conn = http.client.HTTPSConnection(origin[1], timeout=15)
        try:
            query = "?" + urlencode(fields) if fields and method == "GET" else ""
            body = json.dumps(fields) if method == "POST" else None
            conn.request(method, path + query, body=body, headers={
                "Authorization": "Bearer " + token, "Content-Type": "application/json"})
            reply = conn.getresponse()
            if reply.status != 200:
                diagnostic = reply.read(16384)
                network_denial = any(word in diagnostic.lower() for word in (
                    b"ip access list", b"ip_access_list", b"network access", b"source ip",
                    b"ip acl", b"private link", b"privatelink", b"public access is not allowed"))
                reason = " (network policy denied)" if network_denial else ""
                try:
                    code = json.loads(diagnostic).get("error_code", "")
                except (ValueError, AttributeError):
                    code = ""
                if isinstance(code, str) and re.fullmatch(r"[A-Za-z_]{3,64}", code):
                    reason += " " + code.upper()
                error = TransientRequest if reply.status in (408, 429, 500, 502, 503, 504) else RuntimeError
                if ((method, path) == ("GET", CLUSTERS + "/get") and reply.status in (400, 404)
                        and code == "RESOURCE_DOES_NOT_EXIST"):
                    error = MissingCluster
                raise error(f"{method} {path}: HTTP {reply.status}{reason}")
            raw = reply.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError("Oversized API response")
            if not raw and method == "POST" and path in (
                    COMMANDS + "/contexts/destroy", CLUSTERS + "/delete", CLUSTERS + "/permanent-delete"):
                return {}
            response = json.loads(raw)
            if not isinstance(response, dict):
                raise ValueError
            return response
        except (OSError, http.client.HTTPException) as exc:
            raise TransientRequest(f"{method} {path}: {type(exc).__name__}") from None
        except ValueError as exc:
            raise RuntimeError(f"{method} {path}: {type(exc).__name__}") from None
        finally:
            conn.close()

    def call(method, path, **fields):
        # Never repeat cluster creation or a command submission after an ambiguous response.
        if method == "GET":
            return observe(lambda: request(method, path, **fields))
        return request(method, path, **fields)
    return call


def wait(label, read, key, target, pending, seconds, interval=8):
    deadline = time.monotonic() + seconds
    notice = 0
    while time.monotonic() < deadline:
        result = read()
        state = result.get(key)
        if state == target:
            return result
        if state not in pending:
            observed = state if isinstance(state, str) and re.fullmatch(r"[A-Za-z_]{1,32}", state) else "unrecognized"
            code = (result.get("termination_reason") or {}).get("code")
            reason = f", reason={code}" if isinstance(code, str) and re.fullmatch(r"[A-Z_]{2,80}", code) else ""
            raise RuntimeError(f"{label}: expected {target}, observed {observed}{reason}")
        if time.monotonic() >= notice:
            print(f"{label}: {state}; {int(time.monotonic() - (deadline - seconds))}s elapsed", flush=True)
            notice = time.monotonic() + 60
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    raise RuntimeError(f"{label}: did not reach {target} within {seconds}s")


def identifier(record, key):
    value = record.get(key)
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", value):
        raise RuntimeError(f"Missing or invalid {key} in API response")
    return value


@contextmanager
def command_context(call, cluster_id):
    """Open a Python context on the caller's already verified, disposable cluster.

    Only trusted controller code may call query(). Mutations are not replayed.
    The caller retains cluster termination, including on an ambiguous launch:
    destroying a context alone does not terminate a detached build process.
    """
    cluster = identifier({"cluster_id": cluster_id}, "cluster_id")
    context = identifier(call("POST", COMMANDS + "/contexts/create", clusterId=cluster, language="python"), "id")
    ids = {"clusterId": cluster, "contextId": context}
    try:
        ready = wait("Context startup", lambda: call("GET", COMMANDS + "/contexts/status", **ids),
                     "status", "Running", ("Pending",), 180)
        if ready.get("id") != context:
            raise RuntimeError("Unexpected execution context in status response")

        def query(code, *, timeout_seconds=100):
            command = identifier(call("POST", COMMANDS + "/commands/execute", **ids,
                                      language="python", command=code), "id")
            result = wait("Driver command", lambda: call("GET", COMMANDS + "/commands/status",
                          **ids, commandId=command), "status", "Finished",
                          ("Queued", "Running"), timeout_seconds, interval=2)
            output = result.get("results") or {}
            if (result.get("id") != command or output.get("resultType") != "text"
                    or not isinstance(output.get("data"), str) or output.get("truncated") is True):
                raise RuntimeError("Expected complete text for the submitted driver command")
            return json.loads(output["data"])

        yield query
    finally:
        failed = sys.exc_info()[0] is not None
        try:
            call("POST", COMMANDS + "/contexts/destroy", **ids)
        except RuntimeError:
            if not failed:
                raise
            print("Context cleanup unconfirmed; preserving original failure. Terminate the owned cluster.",
                  file=sys.stderr, flush=True)


def run_driver(query, payload, sha256, formatter, *, deadline_seconds=7200, label="build/test"):
    """Run reviewed Python once. Return its report and bounded post-run console pages.

    The caller supplies trusted payload/formatter text and its expected payload hash.
    The payload writes one JSON result to stdout and may write stdout.log, stderr.log
    and diagnostics.log in its private cwd. The formatter defines snapshot(directory,
    finished=True, ok=...). Output is data; rendering policy belongs to the caller.
    """
    if hashlib.sha256(payload.encode("utf-8")).hexdigest() != sha256:
        raise ValueError("Driver payload does not match its expected SHA-256")
    # Start once. Read the logs only after the driver-local process exits.
    setup = f"""
import hashlib, json, os, subprocess, sys, tempfile
from pathlib import Path
_ci_dir = Path(tempfile.mkdtemp(prefix="gpu-ci-console-"))
_ci_script = _ci_dir / "build_driver.py"
_ci_script.write_bytes({payload!r}.encode("utf-8"))
if hashlib.sha256(_ci_script.read_bytes()).hexdigest() != {sha256!r}:
    raise ValueError("Driver upload checksum mismatch")
_ci_view = {{}}
exec(compile({formatter!r}, "console_evidence.py", "exec"), _ci_view)
with (_ci_dir / "result.json").open("xb") as _ci_out:
    _ci_proc = subprocess.Popen([sys.executable, "-I", str(_ci_script)], cwd=_ci_dir,
        env={{"PATH":os.defpath, "LANG":"C.UTF-8"}}, stdin=subprocess.DEVNULL,
        stdout=_ci_out, stderr=subprocess.DEVNULL, start_new_session=True)
print(json.dumps({{"started": True}}))
"""
    started = query(setup)
    if not isinstance(started, dict) or started.get("started") is not True:
        raise RuntimeError("Driver did not confirm build process launch; not resubmitting")
    print(f"Started {label}; waiting for completion", flush=True)
    last_notice = 0
    start = time.monotonic()
    while time.monotonic() - start < deadline_seconds:
        rc = observe(lambda: query("print(json.dumps(_ci_proc.poll()))"))
        if rc is not None:
            if type(rc) is not int:
                raise ValueError("Invalid driver process exit code")
            break
        if time.monotonic() >= last_notice:
            print(f"Build/test process still running ({int(time.monotonic() - start)}s elapsed)", flush=True)
            last_notice = time.monotonic() + 60
        time.sleep(15)
    else:
        raise RuntimeError("Driver build/test process exceeded its deadline")

    if rc:
        result = {"ok": False, "exit": rc if rc >= 0 else 128 - rc, "lines": [f"Driver build process exit {rc}"]}
    else:
        result = query("""
import stat
try:
    with os.fdopen(os.open(_ci_dir / "result.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        meta = os.fstat(stream.fileno())
        if not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1 or meta.st_size > 32000:
            raise ValueError()
        raw = stream.read(32001)
        if len(raw) > 32000:
            raise ValueError()
        _ci_report = json.loads(raw)
except (OSError, ValueError):
    _ci_report = {"ok":False,"lines":["Driver exited without a complete build report"]}
print(json.dumps(_ci_report))
""")
    if not isinstance(result, dict) or type(result.get("ok")) is not bool:
        raise ValueError("Invalid driver result")
    passed = result["ok"]
    snapshot = {k: [] for k in CHANNELS}
    print("Final driver console: " + ("success tails" if passed else "failure tails and diagnostic evidence"), flush=True)
    # Fetch small pages. A truncated command response would discard the entire JSON tail.
    try:
        counts = query(f"""
_ci_pages = {{}}
for _key, _lines in _ci_view["snapshot"](_ci_dir, finished=True, ok={passed!r}).items():
    _parts, _part, _size = [], [], 2
    for _line in _lines:
        _n = len(json.dumps(_line)) + 2
        if _part and _size + _n > 8000:
            _parts.append(_part)
            _part, _size = [], 2
        _part.append(_line)
        _size += _n
    _ci_pages[_key] = _parts + ([_part] if _part else [])
print(json.dumps({{k:len(v) for k,v in _ci_pages.items()}}))
""")
        for channel in CHANNELS:
            n = counts[channel]
            if type(n) is not int or not 0 <= n <= 32:
                raise ValueError("Invalid console page count")
            for i in range(n):
                lines = query(f'print(json.dumps(_ci_pages[{channel!r}][{i}]))')
                if (not isinstance(lines, list) or any(not isinstance(line, str) for line in lines)
                        or len(json.dumps(lines)) > 8000):
                    raise ValueError("Invalid driver console page")
                snapshot[channel].extend(lines)
    except (OSError, RuntimeError, KeyError, TypeError, ValueError):
        print("Driver console retrieval incomplete; preserving workload result", flush=True)
    return result, snapshot
