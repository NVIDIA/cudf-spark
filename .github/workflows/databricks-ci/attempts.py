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
import sys
from tempfile import TemporaryDirectory


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # Trusted checkout, including under python -I.
from runtimes import discover, runtime_label

DB = HERE.parents[2] / "jenkins/databricks"
sys.path.insert(0, str(DB))
import https_transport_v2 as https

PROVIDERS = ("aws2", "aws", "azure3")
REPO = "NVIDIA/cudf-spark"
WORKFLOW = ".github/workflows/databricks-ci.yml"
MANAGER = "cudf-spark-databricks-ci"


def managed_tags(profile, tag, repository=REPO, workflow=WORKFLOW):
    return {"CI_MANAGED_BY": MANAGER if repository == REPO else MANAGER + "-fork",
            "CI_REPOSITORY": repository, "CI_WORKFLOW": workflow,
            "CI_PROFILE": profile, "CI_BUILD_TAG": tag}


# Only these retryable outcomes come from the provider API, not a marker on the PR driver.
PROVIDER_TERMINATIONS = {"AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE", "CLOUD_PROVIDER_RESOURCE_STOCKOUT",
                         "CLUSTER_OPERATION_TIMEOUT", "INSTANCE_UNREACHABLE", "SPOT_INSTANCE_TERMINATION",
                         "BOOTSTRAP_TIMEOUT"}
IDENTITY = {"workspace": "DATABRICKS_HOST", "jenkins_workspace": "WORKSPACE",
            "jenkins_node": "NODE_NAME", "build_tag": "BUILD_TAG",
            "runtime": "DB_RUNTIME", "profile": "DB_TYPE", "creator": "CI_CREATOR",
            "repository": "CI_REPO", "workflow": "CI_WORKFLOW"}


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


def cluster_spec(env):
    # Keep the Jenkins cloud/node/disk defaults, but use a dedicated command-capable driver.
    from clusterutils import ClusterUtils
    spec = ClusterUtils.generate_create_templ(
        None, env["BUILD_TAG"], env["DATABRICKS_RUNTIME"], int(env["IDLE_TIMEOUT"]),
        0, env["DATABRICKS_DRIVER"], env["DATABRICKS_WORKER"], env["DATABRICKS_CLOUD"],
        env.get("INIT_SCRIPT_PATHS", ""), env.get("DB_ZONE", "auto"))
    del spec["ssh_public_keys"]
    tags = managed_tags(env["DB_TYPE"], env["BUILD_TAG"], env["CI_REPO"], env["CI_WORKFLOW"])
    spec.update(cluster_name=env["BUILD_TAG"], data_security_mode="SINGLE_USER",
                single_user_name=env["CI_CREATOR"], custom_tags={**tags, "ResourceClass": "SingleNode"},
                spark_conf={"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*, 4]"})
    return spec


def save_state(path, state):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state))
    temporary.replace(path)


def owned(api, env, cid):
    info = api("GET", https.CLUSTERS + "/get", cluster_id=cid)
    tags = managed_tags(env["DB_TYPE"], env["BUILD_TAG"], env["CI_REPO"], env["CI_WORKFLOW"])
    observed = info.get("custom_tags")
    if (info.get("cluster_id") != cid or info.get("cluster_name") != env["BUILD_TAG"]
            or info.get("creator_user_name") != env["CI_CREATOR"] or not isinstance(observed, dict)
            or any(observed.get(k) != v for k, v in tags.items())):
        raise Failure("Workspace returned a foreign cluster identity; refusing to use or delete it")
    return info


def confirmed_infra(info):
    if info.get("state") not in {"TERMINATING", "TERMINATED", "INTERNAL_ERROR", "ERROR"}:
        return None
    reason = info.get("termination_reason")
    if not isinstance(reason, dict):
        return None
    code, kind = reason.get("code"), reason.get("type")
    if code in {"AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE", "CLOUD_PROVIDER_RESOURCE_STOCKOUT"}:
        return code
    if (code in PROVIDER_TERMINATIONS and kind in {"CLOUD_FAILURE", "SERVICE_FAULT"}
            and (code != "BOOTSTRAP_TIMEOUT" or kind == "SERVICE_FAULT")):
        return code
    return None


def cleanup(path, env, cluster=None):
    receipt = path.with_name("cluster-id")
    if receipt.exists():
        original = receipt.read_text()
        if cluster is not None and cluster != original:
            raise Failure("Recorded cluster ID changed; refusing cleanup")
        cluster = original
    state = state_at(path, env, cluster)
    if state.get("cleanup_complete") is True and state.get("termination_confirmed") is True:
        return
    cid = state["cluster_id"]
    api = https.workspace_api(env["DATABRICKS_HOST"], env["DATABRICKS_TOKEN"])
    try:
        def observe():
            info = owned(api, env, cid)
            if info.get("state") in {"PENDING", "RUNNING", "RESTARTING", "RESIZING", "ERROR", "INTERNAL_ERROR"}:
                if not state.get("termination_requested"):
                    state["termination_requested"] = True
                    save_state(path, state)
                    api("POST", https.CLUSTERS + "/delete", cluster_id=cid)
            return info

        https.wait("Cluster termination", observe, "state", "TERMINATED",
                   {"PENDING", "RUNNING", "RESTARTING", "RESIZING", "TERMINATING", "ERROR", "INTERNAL_ERROR"}, 180)
        if not state.get("purge_requested"):
            state.update(termination_confirmed=True, purge_requested=True)
            save_state(path, state)
            api("POST", https.CLUSTERS + "/permanent-delete", cluster_id=cid)
        # The request is not a receipt. While the object is visible it must stay terminated.
        def removed():
            try:
                return {"removed": False} if owned(api, env, cid)["state"] == "TERMINATED" else {}
            except https.MissingCluster:
                return {"removed": True}

        https.wait("Cluster removal", removed, "removed", True, {False}, 90)
    except https.MissingCluster:
        if state.get("termination_confirmed") is not True:
            raise Failure("Cluster missing before termination confirmation; withholding replacement") from None
    state["cleanup_complete"] = True
    save_state(path, state)


def run_attempt(path, env, workload):
    cluster = None
    api = https.workspace_api(env["DATABRICKS_HOST"], env["DATABRICKS_TOKEN"])
    if api("GET", "/api/2.0/preview/scim/v2/Me", attributes="userName").get("userName") != env["CI_CREATOR"]:
        raise Failure("Workspace principal does not match the configured cluster creator")
    spec = cluster_spec(env)
    state = dict(schema=1, cluster_id=None, **{k: env[v] for k, v in IDENTITY.items()})
    with path.open("x") as file:
        json.dump(state, file)
    (path.parent.parent / (path.parent.name + ".started")).touch(exist_ok=False)
    try:
        created = api("POST", https.CLUSTERS + "/create", **spec)
        cluster = https.identifier(created, "cluster_id")
        state.update(cluster_id=cluster)
        save_state(path, state)
        with path.with_name("cluster-id").open("x") as file:
            file.write(cluster)

        def starting():
            info = owned(api, env, cluster)
            reason = confirmed_infra(info)
            if reason:
                raise Retryable(f"Cluster startup: {reason}")
            return info

        ready = https.wait("Cluster startup", starting, "state", "RUNNING", {"PENDING", "RESTARTING", "RESIZING"}, 1800)
        if (type(ready.get("num_workers")) is not int or ready["num_workers"] != 0
                or ready.get("node_type_id") != spec["node_type_id"]
                or (ready.get("driver_node_type_id") or ready.get("node_type_id")) != spec["driver_node_type_id"]
                or ready.get("spark_version") != spec["spark_version"]
                or ready.get("data_security_mode") != spec["data_security_mode"]
                or ready.get("single_user_name") != spec["single_user_name"]):
            raise Failure("Allocated cluster does not match the dedicated single-node runtime or principal")
        env = dict(env, DATABRICKS_CLUSTER_ID=cluster)
        for phase in ("build", "test"):
            rc = workload(phase, env, cluster)
            if rc:
                # Neither a product exit code nor driver-controlled output authorizes fallback.
                if phase == "test" and rc > 0 and rc not in (124, 130, 137, 143):
                    try:
                        reason = confirmed_infra(owned(api, env, cluster))
                    except (Failure, RuntimeError) as exc:
                        raise Failure(f"Test failed (exit={rc}); provider classification unavailable: {exc}") from exc
                    if reason:
                        raise Retryable(f"Test exit={rc}; provider terminated cluster: {reason}")
                raise Failure(f"{phase.capitalize()} failed (exit={rc}); not retrying")
    finally:
        primary = sys.exc_info()[1]
        try:
            cleanup(path, env, cluster)
        except Exception as exc:
            if primary is not None:
                raise Failure(f"{primary}; cleanup: {exc}") from exc
            raise Failure(f"Cleanup failed: {exc}") from exc


def run_row(args, adapter, *, profiles=PROVIDERS, attempts_per_profile=2, repository=REPO, workflow=WORKFLOW):
    runtime_label(args.runtime)
    fork = repository != REPO
    if (repository != os.environ["GITHUB_REPOSITORY"]
            or (not fork and (workflow != WORKFLOW or tuple(profiles) != PROVIDERS or attempts_per_profile != 2))
            or (fork and (tuple(profiles) != ("aws2",) or attempts_per_profile != 1
                          or not os.environ["GITHUB_WORKFLOW_REF"].startswith(f"{repository}/{workflow}@refs/heads/")))):
        raise Failure("Workflow repository or validation scope mismatch")
    root = args.state_dir.absolute()
    if root.is_symlink() or root.parent.resolve() != Path(os.environ["RUNNER_TEMP"]).resolve():
        raise Failure("State directory must be a direct child of RUNNER_TEMP")
    root = root.resolve()
    run, attempt = os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"]
    if not all(re.fullmatch(r"[1-9][0-9]*", n) for n in (run, attempt)):
        raise Failure("Invalid GitHub run identity")
    owner = ("fork-" if fork else "") + f"gha-{run}-{attempt}-{args.runtime}-{args.mode}"
    marker = root / "owner"
    private = root / "work"
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
            source = adapter.source_manifest(args.candidate.resolve(), args.merge_sha)
        for profile in profiles:
            for number in range(1, attempts_per_profile + 1):
                label = f"{profile}-{number}"
                directory = root / label
                path = directory / "state.json"
                started = root / (label + ".started")
                tag = f"{owner}-{label}"
                identity = dict(DB_TYPE=profile, DB_RUNTIME=args.runtime, BUILD_TAG=tag,
                                WORKSPACE=str(root), NODE_NAME=f"gha-{run}-{attempt}",
                                CI_REPO=repository, CI_WORKFLOW=workflow)
                if post:
                    if started.exists() or path.exists() or path.with_name("cluster-id").exists():
                        identity["CI_CREATOR"] = adapter.creator_name(profile)
                        cleanup(path, dict(adapter.cleanup_env(profile), **identity))
                    continue
                directory.mkdir(mode=0o700)
                with TemporaryDirectory(dir=private) as scratch:
                    scratch = Path(scratch)
                    env = adapter.attempt_env(profile, args.runtime, spark, args.mode, args.reduced, scratch)
                    env.update(identity, CI_CREATOR=adapter.creator_name(profile),
                               IDLE_TIMEOUT="10" if fork else "390", DB_ZONE="auto")
                    print(f"DBR {args.runtime}, {args.mode}: {label}", flush=True)
                    try:
                        run_attempt(path, env, lambda phase, e, cid:
                                    adapter.run_workload(phase, e, cid, scratch, source))
                    except Retryable as exc:
                        print(f"{label}: {exc}; termination and cleanup confirmed", flush=True)
                        if profile == profiles[-1] and number == attempts_per_profile:
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
        # Do not leave driver workload scratch on the worker.
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
        print("Databricks CI: " + (str(error) if isinstance(error, (Failure, RuntimeError))
                                    else type(error).__name__), file=sys.stderr)
        sys.exit(1)
