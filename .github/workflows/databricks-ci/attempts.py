# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Run one Databricks matrix cell; only classified infrastructure failures may retry."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # Trusted checkout, including under python -I.
from runtimes import discover, runtime_label

DB = HERE.parents[2] / "jenkins/databricks"
PROVIDERS = ("aws2", "aws", "azure3")
REPO = "NVIDIA/cudf-spark"
WORKFLOW = ".github/workflows/databricks-ci.yml"
MANAGER = "cudf-spark-databricks-ci"


def managed_tags(profile, tag):
    return {"CI_MANAGED_BY": MANAGER, "CI_REPOSITORY": REPO,
            "CI_WORKFLOW": WORKFLOW, "CI_PROFILE": profile, "CI_BUILD_TAG": tag}


# Only these retryable outcomes come from the provider API, not a marker on the PR driver.
PROVIDER_TERMINATIONS = {"AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE", "CLOUD_PROVIDER_RESOURCE_STOCKOUT",
                         "CLUSTER_OPERATION_TIMEOUT", "INSTANCE_UNREACHABLE", "SPOT_INSTANCE_TERMINATION",
                         "BOOTSTRAP_TIMEOUT"}
IDENTITY = {"workspace": "DATABRICKS_HOST", "jenkins_workspace": "WORKSPACE",
            "jenkins_node": "NODE_NAME", "build_tag": "BUILD_TAG",
            "runtime": "DB_RUNTIME", "profile": "DB_TYPE"}


class Failure(Exception):
    pass


class Retryable(Failure):
    pass


def state_at(path, env, expected_id=None):
    try:
        state = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise Failure("Missing or unreadable cluster state; no replacement permitted") from exc
    if (not isinstance(state, dict) or type(state.get("schema")) is not int or state["schema"] != 1
            or any(not env.get(v) or state.get(k) != env[v] for k, v in IDENTITY.items())):
        raise Failure("Foreign cluster state; refusing to use or delete the cluster")
    cluster = state.get("cluster_id")
    if not isinstance(cluster, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", cluster):
        raise Failure(f"No confirmed cluster ID; reconcile CI_BUILD_TAG={env['BUILD_TAG']} before retry")
    if expected_id is not None and cluster != expected_id:
        raise Failure("Recorded cluster ID changed; refusing to act on the replacement")
    return state


def lifecycle(operation, path, env):
    args = [sys.executable, "-EsB", str(DB / "cluster_fallback.py"), operation, "--state", str(path)]
    if operation == "prepare":
        args.extend(["--config", str(path.with_name("config.json"))])
    # Existing launchers may log workspace identifiers and remote output; this is a public run.
    return subprocess.run(args, env=env, cwd=path.parent, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL, timeout=300 if operation == "cleanup" else None).returncode


def detail(operation, rc, state):
    reason = state.get("reason")
    if not isinstance(reason, str) or not re.fullmatch(r"[A-Z0-9_]{1,100}", reason):
        reason = "UNKNOWN"
    return f"{operation}: exit={rc}, reason={reason}"


def cleanup(path, env, cluster=None):
    receipt = path.with_name("cluster-id")
    if receipt.exists():
        original = receipt.read_text()
        if cluster is not None and cluster != original:
            raise Failure("Recorded cluster ID changed; refusing cleanup")
        cluster = original
    cluster = state_at(path, env, cluster)["cluster_id"]
    rc = lifecycle("cleanup", path, env)
    state = state_at(path, env, cluster)
    if rc or state.get("termination_confirmed") is not True or state.get("cleanup_complete") is not True:
        raise Failure(f"Cluster cleanup unconfirmed (exit={rc}); no replacement permitted")


def run_attempt(path, env, workload):
    cluster = None
    try:
        rc = lifecycle("prepare", path, env)
        state = state_at(path, env)
        cluster = state["cluster_id"]
        # A later GHA cleanup step must validate against the ID observed before the workload.
        with path.with_name("cluster-id").open("x") as receipt:
            receipt.write(cluster)
        if rc == 75 and state.get("retryable") is True and state.get("phase") in ("START", "SSH"):
            raise Retryable(detail("prepare", rc, state))
        if rc or state.get("phase") != "READY":
            raise Failure(detail("prepare", rc, state))
        driver = state.get("driver")
        if not isinstance(driver, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", driver):
            raise Failure("Prepared cluster has no valid driver address")
        env = dict(env, DATABRICKS_DRIVER_ADDRESS=driver, DATABRICKS_CLUSTER_ID=cluster)
        rc = workload("build", env, cluster)
        if rc:
            raise Failure(f"Build failed (exit={rc}); not retrying")
        rc = lifecycle("install-readiness", path, env)
        if rc:
            raise Failure(f"Readiness hook installation failed (exit={rc})")
        test_env = dict(env, DATABRICKS_CI_TEST_SUPERVISION="1")
        hook = "SPARK_STARTUP_CHECK=/home/ubuntu/.databricks-fallback/spark_readiness.sh"
        test_env["EXTRA_ENVS"] = ",".join(filter(None, (env.get("EXTRA_ENVS"), hook)))
        rc = workload("test", test_env, cluster)
        if rc:
            if rc < 0 or rc in (124, 130, 137, 143):
                raise Failure(f"Test timed out or was interrupted (exit={rc}); not retrying")
            classification = lifecycle("classify-startup", path, env)
            state = state_at(path, env, cluster)
            message = f"Test failed (exit={rc}); " + detail("classify-startup", classification, state)
            if (classification == 75 and state.get("classification_available") is True
                    and state.get("retryable") is True and state.get("reason") in PROVIDER_TERMINATIONS):
                raise Retryable(message)
            raise Failure(message)
        rc = lifecycle("verify-readiness", path, env)
        if rc:
            raise Failure(f"Readiness verification failed (exit={rc})")
    finally:
        # Keep the original failing phase visible if teardown also fails. Never retry ambiguous cleanup.
        primary = sys.exc_info()[1]
        if path.exists():
            try:
                cleanup(path, env, cluster)
            except Exception as exc:
                message = str(exc) if isinstance(exc, Failure) else type(exc).__name__
                if primary is not None:
                    cause = str(primary) if isinstance(primary, Failure) else type(primary).__name__
                    raise Failure(f"{cause}; cleanup: {message}") from exc
                raise Failure(f"Cleanup failed: {message}") from exc


def run_row(args, adapter):
    runtime_label(args.runtime)
    root = args.state_dir.absolute()
    if root.is_symlink() or root.parent.resolve() != Path(os.environ["RUNNER_TEMP"]).resolve():
        raise Failure("State directory must be a direct child of RUNNER_TEMP")
    root = root.resolve()
    run, attempt = os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"]
    if not all(re.fullmatch(r"[1-9][0-9]*", n) for n in (run, attempt)):
        raise Failure("Invalid GitHub run identity")
    owner = f"gha-{run}-{attempt}-{args.runtime}-{args.mode}"
    marker = root / "owner"
    private = root / "work"
    archive = private / "source.tgz"
    post = args.operation == "cleanup"
    if post:
        if not marker.is_file() or marker.read_text() != owner:
            raise Failure("Cannot confirm ownership of the attempt directory")
        if (root / "cleanup-complete").is_file():
            return
    else:
        if args.candidate is None or not re.fullmatch(r"[0-9a-f]{40}", args.merge_sha or ""):
            raise Failure("Run requires a candidate checkout and merge SHA")
        if root.is_relative_to(args.candidate.resolve()) or args.candidate.resolve().is_relative_to(root):
            raise Failure("Candidate checkout and state directory must be disjoint")
        spark = discover(args.candidate, args.merge_sha).get(args.runtime)
        if not spark:
            raise Failure(f"DBR {args.runtime} is not enabled at {args.merge_sha}")
        root.mkdir(mode=0o700)  # Never overwrite state left by an earlier invocation.
        marker.write_text(owner)
        private.mkdir(mode=0o700)

    try:
        if not post:
            adapter.make_archive(args.candidate.resolve(), args.merge_sha, archive)
        for profile in PROVIDERS:
            for number in (1, 2):
                label = f"{profile}-{number}"
                directory = root / label
                path = directory / "state.json"
                started = root / (label + ".started")
                tag = f"{owner}-{label}"
                identity = dict(DB_TYPE=profile, DB_RUNTIME=args.runtime, BUILD_TAG=tag,
                                WORKSPACE=str(root), NODE_NAME=f"gha-{run}-{attempt}")
                if post:
                    if started.exists() or path.exists() or path.with_name("cluster-id").exists():
                        cleanup(path, dict(adapter.cleanup_env(profile), **identity))
                    continue
                directory.mkdir(mode=0o700)
                with TemporaryDirectory(dir=private) as scratch:
                    scratch = Path(scratch)
                    env = adapter.attempt_env(profile, args.runtime, spark, args.mode, args.reduced, scratch)
                    env.update(identity, CLUSTER_NAME=tag, IDLE_TIMEOUT="390", NUM_WORKERS="0", DB_ZONE="auto")
                    print(f"DBR {args.runtime}, {args.mode}: {label}", flush=True)
                    result = subprocess.run([sys.executable, "-EsB", str(DB / "fallback-config.py")],
                                            cwd=scratch, env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                    if result.returncode:
                        raise Failure(f"Cluster configuration failed (exit={result.returncode})")
                    config = json.loads(result.stdout)
                    spec = config["cluster_spec"]
                    spec["cluster_name"] = tag
                    spec.setdefault("custom_tags", {}).update(managed_tags(profile, tag))
                    path.with_name("config.json").write_text(json.dumps(config))
                    started.touch(exist_ok=False)
                    try:
                        run_attempt(path, env, lambda phase, e, cid:
                                    adapter.run_workload(phase, e, cid, scratch, archive))
                    except Retryable as exc:
                        print(f"{label}: {exc}; termination and cleanup confirmed", flush=True)
                        if profile == PROVIDERS[-1] and number == 2:
                            raise Failure(f"Provider attempts exhausted: {exc}") from exc
                    else:
                        return
        if post:
            # Keep only a local, nonsecret receipt so repeated post-cleanup remains idempotent.
            (root / "cleanup-complete").touch(exist_ok=False)
            for directory in root.iterdir():
                if directory.is_dir():
                    shutil.rmtree(directory)
    finally:
        # Do not leave the archive or temporary SSH material on a self-hosted worker.
        if private.is_dir():
            shutil.rmtree(private)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("run", "cleanup"))
    parser.add_argument("--runtime", required=True, type=runtime_label)
    parser.add_argument("--mode", required=True, choices=("CI_PART1", "CI_PART2"))
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--merge-sha")
    parser.add_argument("--reduced", action="store_true")
    args = parser.parse_args()
    # The worker invokes this script with python -I from a trusted checkout.
    import workload

    def interrupt(signum, _frame):
        raise Failure(f"Cancelled by signal {signum}; no replacement permitted")

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupt)
    run_row(args, workload)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("Databricks CI: " + (str(error) if isinstance(error, Failure)
                                    else type(error).__name__), file=sys.stderr)
        sys.exit(1)
