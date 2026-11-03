# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""CI-only cluster lifecycle for cuDF-Spark Databricks fallback."""

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

import requests

from clusterutils import ClusterUtils

RETRYABLE_EXIT = 75
START_TIMEOUT = 30 * 60
SSH_ATTEMPTS = 6
CAPACITY_TERMINATIONS = {
    "AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
    "CLOUD_PROVIDER_RESOURCE_STOCKOUT",
}
RETRYABLE_TERMINATIONS = CAPACITY_TERMINATIONS | {
    "CLUSTER_OPERATION_TIMEOUT",
    "INSTANCE_UNREACHABLE",
    "SPOT_INSTANCE_TERMINATION",
}


class PreparationError(Exception):
    def __init__(self, reason, retryable=False):
        super().__init__(reason)
        self.retryable = retryable


def save_state(path, state):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, sort_keys=True) + "\n")
    temporary.replace(path)


def load_state(path):
    state = json.loads(path.read_text())
    if (state.get("schema") != 1 or state.get("workspace") != os.environ["DATABRICKS_HOST"]
            or state.get("jenkins_workspace") != os.environ["WORKSPACE"]
            or state.get("jenkins_node") != os.environ["NODE_NAME"]
            or state.get("build_tag") != os.environ["BUILD_TAG"]
            or state.get("runtime") != os.environ["DB_RUNTIME"]
            or state.get("profile") != os.environ["DB_TYPE"]):
        raise PreparationError("STATE_OWNERSHIP_MISMATCH")
    cluster_id = state.get("cluster_id")
    if cluster_id is not None and (not isinstance(cluster_id, str)
                                  or not re.fullmatch(r"[a-zA-Z0-9_-]+", cluster_id)):
        raise PreparationError("INVALID_CLUSTER_ID")
    return state


def request(method, endpoint, **kwargs):
    response = requests.request(
        method, os.environ["DATABRICKS_HOST"].rstrip("/") + "/api/2.0/clusters/" + endpoint,
        headers={"Authorization": "Bearer " + os.environ["DATABRICKS_TOKEN"]},
        timeout=(10, 30), **kwargs)
    response.raise_for_status()
    return response.json() if response.content else {}


def check_termination(info):
    if info.get("state") not in {"TERMINATING", "TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
        return
    reason = info.get("termination_reason") or {}
    code = reason.get("code", "UNKNOWN_TERMINATION")
    # These explicit capacity codes describe infrastructure shortages even when the API
    # categorizes the termination as CLIENT_ERROR. Do not extend that exception to other codes.
    retryable = code in CAPACITY_TERMINATIONS or (
        code in RETRYABLE_TERMINATIONS and reason.get("type") != "CLIENT_ERROR") or (
        code == "BOOTSTRAP_TIMEOUT" and reason.get("type") == "SERVICE_FAULT")
    print("Cluster termination: code=%s, type=%s, retryable=%s" %
          (code, reason.get("type", "UNKNOWN"), retryable), flush=True)
    raise PreparationError(code, retryable)


def wait_for_driver(cluster_id, timeout=START_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = request("GET", "get", params={"cluster_id": cluster_id})
        check_termination(info)
        print("Cluster %s: %s" % (cluster_id, info.get("state", "UNKNOWN")), flush=True)
        if info.get("state") == "RUNNING":
            address = ClusterUtils.get_master_addr_from_json(info) or ""
            if re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.-]*", address):
                return address
        time.sleep(20)
    # Retry this pre-build startup deadline on a fresh cluster only after confirmed cleanup.
    raise PreparationError("CLUSTER_READINESS_TIMEOUT", retryable=True)


def wait_for_ssh(address, cluster_id):
    command = [
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectionAttempts=1",
        "-o", "ConnectTimeout=15",
    ] + ClusterUtils.get_ssh_options(os.environ["DATABRICKS_PRIVKEY"]) + [
        "ubuntu@" + address, "true"]
    for attempt in range(SSH_ATTEMPTS):
        try:
            result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                    text=True, timeout=30)
        except subprocess.TimeoutExpired:
            reason = "SSH_READINESS_TIMEOUT"
        else:
            if result.returncode == 0:
                print("SSH readiness confirmed", flush=True)
                return
            # Auth, key/configuration errors and arbitrary remote exit codes are not retryable.
            transient = result.returncode == 255 and re.search(
                r"(?:ssh: connect to host \S+ port (?:22|2200): "
                r"|channel \d+: open failed: connect failed: )"
                r"(Connection timed out|Connection refused|No route to host)", result.stderr)
            if not transient:
                raise PreparationError("SSH_READINESS_FAILED")
            reason = "SSH_UNREACHABLE"
        print("SSH readiness check %d/%d: %s" % (attempt + 1, SSH_ATTEMPTS, reason), flush=True)
        info = request("GET", "get", params={"cluster_id": cluster_id})
        check_termination(info)
        if attempt + 1 < SSH_ATTEMPTS:
            time.sleep(10)
    raise PreparationError(reason, retryable=True)


def validate_readiness_modes(modes):
    # test.sh always starts with Databricks; only its two-shim path also runs upstream.
    if modes not in (["databricks"], ["databricks", "upstream"]):
        raise PreparationError("INVALID_READINESS_MODES")
    return modes


def load_configuration(path):
    configuration = json.loads(path.read_text())
    if not isinstance(configuration, dict):
        raise PreparationError("INVALID_CONFIGURATION")
    template = configuration.get("cluster_spec")
    if not isinstance(template, dict) or not template:
        raise PreparationError("INVALID_CLUSTER_SPEC")
    tags = template.setdefault("custom_tags", {})
    if not isinstance(tags, dict):
        raise PreparationError("INVALID_CLUSTER_TAGS")
    tags["CI_BUILD_TAG"] = os.environ["BUILD_TAG"]
    modes = validate_readiness_modes(configuration.get("readiness_modes"))
    return template, modes


def prepare(path, configuration):
    template, modes = load_configuration(configuration)
    # Never overwrite a cluster handle left by an interrupted/resumed attempt.
    with path.open("x") as output:
        state = {"schema": 1, "workspace": os.environ["DATABRICKS_HOST"],
                 "jenkins_workspace": os.environ["WORKSPACE"],
                 "jenkins_node": os.environ["NODE_NAME"],
                 "build_tag": os.environ["BUILD_TAG"], "runtime": os.environ["DB_RUNTIME"],
                 "profile": os.environ["DB_TYPE"], "readiness_modes": modes,
                 "cluster_id": None, "phase": "CREATE", "retryable": False}
        json.dump(state, output)
    try:
        print("Requesting cluster creation", flush=True)
        response = request("POST", "create", json=template)
        cluster_id = response["cluster_id"]
        if not isinstance(cluster_id, str) or not re.fullmatch(r"[a-zA-Z0-9_-]+", cluster_id):
            raise PreparationError("INVALID_CLUSTER_ID")
        state.update(cluster_id=cluster_id, phase="START")
        save_state(path, state)
        print("Cluster %s created; waiting for RUNNING (up to %ds)" %
              (cluster_id, START_TIMEOUT), flush=True)
        address = wait_for_driver(cluster_id)
        state.update(phase="SSH", driver=address)
        save_state(path, state)
        wait_for_ssh(address, cluster_id)
        state.update(phase="READY", reason="READY")
        save_state(path, state)
        print("Cluster preparation ready: " + cluster_id, flush=True)
        return 0
    except PreparationError as error:
        reason, retryable = str(error), error.retryable
    except requests.HTTPError as error:
        status = error.response.status_code
        reason = "API_HTTP_%d" % status
        retryable = bool(state["cluster_id"]) and status in {429, 502, 503, 504}
    except requests.exceptions.SSLError:
        reason, retryable = "API_TLS_FAILED", False
    except (requests.Timeout, requests.ConnectionError):
        reason = "API_CONNECTION_FAILED"
        # A lost create response can leave an unknown cluster; never repeat that allocation.
        retryable = bool(state["cluster_id"])
    except (KeyError, ValueError):
        reason, retryable = "INVALID_CONFIGURATION_OR_RESPONSE", False
    state.update(reason=reason, retryable=retryable)
    save_state(path, state)
    print("Cluster preparation failed: %s (retryable=%s)" % (reason, retryable),
          file=sys.stderr, flush=True)
    return RETRYABLE_EXIT if retryable else 1


def cleanup(path):
    state = load_state(path)
    cluster_id = state.get("cluster_id")
    if not cluster_id:
        print("No cluster ID recorded; no cluster cleanup request sent", flush=True)
        return
    if state.get("cleanup_complete"):
        print("Cluster cleanup already complete: " + cluster_id, flush=True)
        return
    if not state.get("termination_confirmed"):
        # Permanent deletion hides the record before cloud resources are released.
        print("Requesting cluster termination: " + cluster_id, flush=True)
        request("POST", "delete", json={"cluster_id": cluster_id})
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            info = request("GET", "get", params={"cluster_id": cluster_id})
            print("Cluster cleanup %s: %s" % (cluster_id, info.get("state", "UNKNOWN")),
                  flush=True)
            if info.get("state") == "TERMINATED":
                break
            time.sleep(10)
        else:
            raise PreparationError("CLUSTER_CLEANUP_TIMEOUT")
        state["termination_confirmed"] = True
        save_state(path, state)
    try:
        print("Termination confirmed; permanently deleting: " + cluster_id, flush=True)
        request("POST", "permanent-delete", json={"cluster_id": cluster_id})
    except requests.HTTPError as error:
        if not cluster_missing(error):
            raise
    state["cleanup_complete"] = True
    save_state(path, state)
    print("Cluster cleanup complete: " + cluster_id, flush=True)


def cluster_missing(error):
    if error.response.status_code == 404:
        return True
    try:
        return error.response.json().get("error_code") == "RESOURCE_DOES_NOT_EXIST"
    except ValueError:
        return False


def readiness_command(state, command, **kwargs):
    address = state.get("driver", "")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.-]*", address):
        raise PreparationError("INVALID_DRIVER_ADDRESS")
    return subprocess.run([
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectionAttempts=1",
        "-o", "ConnectTimeout=15",
    ] + ClusterUtils.get_ssh_options(os.environ["DATABRICKS_PRIVKEY"]) + [
        "ubuntu@" + address, command], text=True, timeout=60, check=True, **kwargs)


def spark_readiness(path, operation):
    state = load_state(path)
    if state.get("phase") != "READY" or not state.get("cluster_id"):
        raise PreparationError("INVALID_READINESS_STATE")
    directory = "/home/ubuntu/.databricks-fallback"
    modes = validate_readiness_modes(state.get("readiness_modes"))
    if operation == "install-readiness":
        print("Installing plugin-free Spark readiness hook; clearing prior results", flush=True)
        readiness_command(state, "mkdir -p %s && cat > %s/spark_readiness.sh && rm -f %s/spark-readiness*.json" %
                          (directory, directory, directory),
                          input=Path(__file__).with_name("spark_readiness.sh").read_text())
        return 0

    # This marker is written only by the plugin-free preflight. READY/missing markers must
    # never turn a later compilation, smoke-test or pytest failure into a provider retry.
    state.update(retryable=False, classification_available=False,
                 reason="SPARK_STARTUP_CLASSIFICATION_UNAVAILABLE")
    save_state(path, state)
    results = {}
    for mode in ("databricks", "upstream"):
        marker = shlex.quote(directory + "/spark-readiness-%s.json" % mode)
        result = readiness_command(state, "if test -e %s; then cat %s; fi" % (marker, marker),
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        if not result.stdout:
            print("Spark readiness %s: result missing" % mode, flush=True)
            continue
        readiness = json.loads(result.stdout)
        if (not isinstance(readiness, dict) or readiness.get("runtime") != mode
                or readiness.get("phase") not in {"STARTING", "READY", "FAILED"}):
            raise PreparationError("INVALID_SPARK_READINESS_RESULT")
        results[mode] = readiness
        state["spark_readiness"] = results
        save_state(path, state)
        print("Spark readiness %s: %s" % (mode, readiness["phase"]), flush=True)
    state["spark_readiness"] = results
    state.update(reason="NOT_A_CLASSIFIED_SPARK_STARTUP_FAILURE")
    save_state(path, state)
    if operation == "verify-readiness":
        print("Required Spark readiness checks: " + ", ".join(modes), flush=True)
        if (any(results.get(mode, {}).get("phase") != "READY" for mode in modes)
                or any(result["phase"] != "READY" for result in results.values())):
            raise PreparationError("SPARK_READINESS_NOT_COMPLETED")
        state.update(reason="SPARK_READINESS_CONFIRMED")
        save_state(path, state)
        print("All required Spark readiness checks passed", flush=True)
        return 0
    # Only the first incomplete check in the caller's execution order can explain startup
    # failure. Later markers contradict that attribution; missing markers fail closed.
    readiness = {}
    if all(mode in modes for mode in results):
        for index, mode in enumerate(modes):
            result = results.get(mode, {})
            if result.get("phase") != "READY":
                if not any(later in results for later in modes[index + 1:]):
                    readiness = result
                break
    if readiness.get("phase") == "FAILED":
        reason = readiness.get("reason")
        if reason in {"SPARK_STARTUP_DISK_FULL", "SPARK_STARTUP_DISK_IO"}:
            state.update(retryable=True, reason=reason)
        else:
            info = request("GET", "get", params={"cluster_id": state["cluster_id"]})
            try:
                check_termination(info)
            except PreparationError as error:
                state.update(retryable=error.retryable, reason=str(error))
    state["classification_available"] = True
    save_state(path, state)
    print("Spark readiness classification: %s (retryable=%s)" %
          (state["reason"], state["retryable"]), flush=True)
    return RETRYABLE_EXIT if state["retryable"] else 1


def gpu_diagnostics(path):
    state = load_state(path)
    # Both the remote command (10s) and the SSH transport (60s) are bounded.
    readiness_command(state, "timeout --kill-after=2s 10s nvidia-smi -q 2>&1 || "
                      "printf '\\nGPU diagnostics unavailable or incomplete.\\n'")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["prepare", "cleanup", "install-readiness",
                                             "classify-startup", "verify-readiness",
                                             "gpu-diagnostics"])
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--config", type=Path,
                        help="prepare only: JSON cluster_spec and ordered readiness_modes")
    args = parser.parse_args()
    if args.operation == "prepare" and args.config is None:
        parser.error("prepare requires --config")
    print("Databricks fallback operation: " + args.operation, flush=True)
    try:
        if args.operation == "prepare":
            return prepare(args.state, args.config)
        if args.operation == "gpu-diagnostics":
            return gpu_diagnostics(args.state)
        if args.operation in {"install-readiness", "classify-startup", "verify-readiness"}:
            return spark_readiness(args.state, args.operation)
        cleanup(args.state)
        return 0
    except Exception as error:
        # API response bodies and exception strings can contain sensitive workspace details.
        if isinstance(error, PreparationError):
            print("Databricks failure reason: " + str(error), file=sys.stderr, flush=True)
        print("Cluster %s failed (%s); inspect the saved state" %
              (args.operation, type(error).__name__), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
