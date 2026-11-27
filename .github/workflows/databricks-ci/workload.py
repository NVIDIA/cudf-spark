# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prepare an immutable source manifest. Run build and test only on a disposable Databricks driver."""

import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
from urllib.parse import urlsplit

from console_filter import public_lines
from runtimes import SPARK_VERSION, runtime_label


HERE = Path(__file__).resolve().parent
DB = HERE.parents[2] / "jenkins/databricks"
sys.path.insert(0, str(DB))  # Trusted checkout, also with python -I.
from https_transport_v2 import command_context, run_driver, workspace_api
from workload_v2 import SUBMODULES, gitlinks, validate_source

CLOUD = {"aws2": "aws", "aws": "aws", "azure3": "azure"}
HOSTNAME = r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?"
CLEAN_ENV = {"PATH": os.defpath, "LANG": "C", "GIT_CONFIG_NOSYSTEM": "1",
             "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0", "GIT_NO_LAZY_FETCH": "1"}


def source_manifest(candidate, merge_sha):
    if not re.fullmatch("[0-9a-f]{40}", merge_sha):
        raise ValueError("Expected an exact candidate revision")
    candidate = Path(candidate).resolve()

    def read(*args):
        return subprocess.check_output(["git", "--no-replace-objects", "-c", "protocol.allow=never", "-C",
                                        str(candidate), *args], env=CLEAN_ENV, stderr=subprocess.DEVNULL, timeout=60)

    if (read("rev-parse", "HEAD").strip().decode() != merge_sha or
            Path(os.fsdecode(read("rev-parse", "--show-toplevel").rstrip(b"\n"))) != candidate):
        raise ValueError("Candidate checkout does not match selected revision")
    source = {"revision": merge_sha, "submodules": gitlinks(read("ls-tree", "-rz", merge_sha))}
    validate_source(source)
    for path, origin in SUBMODULES.items():
        declared = read("config", "--no-includes", "--blob", merge_sha + ":.gitmodules", "--get",
                        f"submodule.{path}.url").decode().strip()
        if declared.removesuffix(".git") != origin.removesuffix(".git"):
            raise ValueError("Unexpected submodule origin")
    return source


def profile_config(profile):
    if profile not in CLOUD:
        raise ValueError("Unknown Databricks CI provider")
    config = json.loads(os.environ["DATABRICKS_PROFILES_JSON"])[profile]
    host = urlsplit(config["host"])
    if (host.scheme != "https" or not re.fullmatch(HOSTNAME, host.netloc)
            or host.path not in ("", "/") or host.query or host.fragment):
        raise ValueError("Invalid protected workspace URL")
    return config


def cleanup_env(profile):
    token = os.environ[f"DATABRICKS_{profile.upper()}_TOKEN"]
    if not token:
        raise ValueError("Missing provider credential")
    return {**CLEAN_ENV, "DATABRICKS_HOST": profile_config(profile)["host"].rstrip("/"),
            "DATABRICKS_TOKEN": token}


def creator_name(profile):
    creator = profile_config(profile).get("creator_user_name")
    if not isinstance(creator, str) or not creator or creator.strip() != creator:
        raise ValueError("Configure the exclusive CI service principal for the Databricks profile")
    return creator


def attempt_env(profile, runtime, spark, part, reduced, directory):
    creator_name(profile)
    runtime_label(runtime)
    if (not re.fullmatch(SPARK_VERSION, spark) or part not in ("CI_PART1", "CI_PART2")
            or type(reduced) is not bool):
        raise ValueError("Invalid Databricks test selection")
    cfg = profile_config(profile)
    for key in ("driver", "worker"):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", cfg[key]):
            raise ValueError("Invalid configured node type")
    digest = os.environ.get("DB_MAVEN_SEED_SHA256", "")
    return {**cleanup_env(profile), **({"DB_MAVEN_SEED_SHA256": digest} if re.fullmatch("[0-9a-f]{64}", digest) else {}),
            "DATABRICKS_CLOUD": CLOUD[profile], "DATABRICKS_DRIVER": cfg["driver"],
            "DATABRICKS_WORKER": cfg["worker"], "NUM_WORKERS": "0",
            "DATABRICKS_RUNTIME": runtime + ".x-gpu-ml-scala2.13", "BASE_SPARK_VERSION": spark,
            "EXTRA_ENVS": f"TEST_MODE={part},REDUCED_IT={str(reduced).lower()}"}


def report_console(snapshot, phase, rc):
    hidden = [v for k, v in os.environ.items() if k.startswith("DATABRICKS_") and k.endswith("_TOKEN") and v]
    for channel in ("stdout", "stderr", "diagnostics"):
        if not rc and channel == "diagnostics":
            continue
        # The remote formatter is not a security boundary. Filter again locally before publishing.
        lines = public_lines([s.encode() for s in snapshot[channel]], hidden)
        lines = lines[-(10 if rc and channel != "diagnostics" else 1000):]
        if not lines:
            continue
        marker = secrets.token_hex(24)
        if not rc:
            print(f"::group::Databricks {phase} {channel}", flush=True)
        print(f"::stop-commands::{marker}", flush=True)
        try:
            for line in lines:
                print(f"driver {channel} | {line}", flush=True)
        finally:
            print(f"::{marker}::", flush=True)
            if not rc:
                print("::endgroup::", flush=True)


def driver_payload(plan, files):
    return ((DB / "workload_v2.py").read_text() +
            f"\nprint(json.dumps(run_phase({plan!r}, {files!r}, Path.cwd())))\n")


def run_workload(phase, env, cluster_id, directory, source):
    """Use only protected launcher bytes. Never send the controller environment to the driver."""
    validate_source(source)
    if cluster_id != env["DATABRICKS_CLUSTER_ID"] or phase not in ("build", "test"):
        raise ValueError("Unexpected workload cluster or phase")
    options = re.fullmatch(r"TEST_MODE=(CI_PART[12]),REDUCED_IT=(true|false)", env["EXTRA_ENVS"])
    spark = env["BASE_SPARK_VERSION"]
    if not options or not re.fullmatch(SPARK_VERSION, spark):
        raise ValueError("Invalid remote workload selection")
    runtime = env["DATABRICKS_RUNTIME"].removesuffix(".x-gpu-ml-scala2.13")
    runtime_label(runtime)
    mode = options[1]
    plan = dict(source=source, runtime=runtime, spark=spark, mode=mode, phase=phase,
                reduced=options[2] == "true")
    files = {phase + ".sh": (DB / (phase + ".sh")).read_text(), "capture.py": (HERE / "capture_console.py").read_text()}
    if phase == "build" and re.fullmatch("[0-9a-f]{64}", env.get("DB_MAVEN_SEED_SHA256", "")):
        try:
            files["maven_seed_v2.py"] = (DB / "maven_seed_v2.py").read_text()
            plan["seed"] = env["DB_MAVEN_SEED_SHA256"]
        except OSError:
            print("Maven seed helper unavailable; continuing cold", flush=True)
    if phase == "test":
        files.update({name: (DB / name).read_text() for name in ("spark_readiness.sh", "run-test-command.sh")})
    payload = driver_payload(plan, files)
    call = workspace_api(env["DATABRICKS_HOST"], env["DATABRICKS_TOKEN"])
    outcome = None
    try:
        with command_context(call, cluster_id) as query:
            report, snapshot = run_driver(query, payload, hashlib.sha256(payload.encode()).hexdigest(),
                                          (HERE / "console_filter.py").read_text(),
                                          deadline_seconds=(95 if phase == "build" else 275) * 60, label=phase)
            outcome = finish_phase(plan, report, snapshot)
    except RuntimeError as exc:
        if outcome is not None:
            raise RuntimeError(f"Databricks {phase} exit={outcome}; context cleanup unconfirmed: {exc}") from exc
        raise
    return outcome


def finish_phase(plan, report, snapshot):
    phase, mode = plan["phase"], plan["mode"]
    rc = report.get("exit")
    if type(rc) is not int or not 0 <= rc <= 255 or report.get("ok") is not (rc == 0):
        raise RuntimeError("Missing or inconsistent driver phase exit")
    try:
        report_console(snapshot, phase, rc)
    except (OSError, UnicodeError, ValueError):
        print("Driver console unavailable; preserving phase result", flush=True)
    stage = report.get("stage")
    if phase == "build" and "seed" in plan:
        restored = report.get("seed")
        status = (f"restored {restored} released versions" if type(restored) is int and 0 < restored < 10**9
                  else "no confirmed restore; normal Maven resolution")
        print(f"Databricks Maven seed: {status}", flush=True)
    if rc:
        reason = stage if stage in ("runtime", "checkout", "build", "test-inputs", "test", "junit", "readiness") else "driver"
        detail = report.get("detail")
        safe = isinstance(detail, str) and re.fullmatch(r"timeout|subprocess|invalid evidence|errno [0-9]{1,3}", detail)
        suffix = f", {detail}" if safe else ""
        print(f"Databricks {phase}: {reason} failed (exit={rc}{suffix})", flush=True)
    if phase == "test":
        counts = report.get("junit")
        if not isinstance(counts, list) or len(counts) != 3 or any(type(x) is not int or not 0 <= x <= 10**8 for x in counts):
            return rc or 1
        total, skipped, failed = counts
        print(f"Databricks pytest JUnit: {total} cases, {skipped} skipped, {failed} failed", flush=True)
        expected = ["databricks", "upstream"] if mode == "CI_PART1" else ["databricks"]
        if total <= skipped or failed or report.get("ready") != expected:
            return rc or 1
    return rc if rc or stage == "complete" else 1
