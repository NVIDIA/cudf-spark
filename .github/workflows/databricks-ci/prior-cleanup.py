# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Block a DB cell rerun until its previous allocations have confirmed cleanup."""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))  # Trusted checkout, including under python -I.
from attempts import PROVIDERS, REPO, WORKFLOW

JANITOR = ".github/workflows/databricks-cleanup.yml"
BUILD = "Build and test the approved merge on Databricks"
CLEANUP = "Confirm cluster cleanup"
RECONCILE = "Reconcile only clusters owned by finished GHA attempts"
PROFILES = {f"reconcile ({profile})" for profile in PROVIDERS}
TERMINAL = {"success", "failure", "cancelled", "skipped", "timed_out", "action_required",
            "neutral", "stale", "startup_failure"}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def timestamp(value):
    require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z", value),
            "Expected a UTC timestamp from Actions")
    return datetime.fromisoformat(value[:-1] + "+00:00")


def jobs(get, run, attempt):
    require(type(run) is int and run > 0 and type(attempt) is int and attempt > 0, "Invalid Actions run/attempt ID")
    result = get(f"actions/runs/{run}/attempts/{attempt}/jobs?per_page=100")
    rows = result["jobs"]
    # These workflows have fewer than 100 jobs. Never infer absence from a truncated page.
    require(len(rows) == result["total_count"] <= 100 and
            all(j["run_id"] == run and j.get("run_attempt", attempt) == attempt and j["name"] for j in rows),
            f"Incomplete or mismatched jobs for run {run}, attempt {attempt}")
    return rows


def named(rows, name):
    hits = [r for r in rows if r["name"] == name]
    require(len(hits) <= 1, f"Ambiguous Actions record for {name}")
    return hits[0] if hits else {}


def completed(row, conclusion):
    return row.get("status") == "completed" and row.get("conclusion") == conclusion


def reconciled(get, after):
    # A later queued/completed time is insufficient: the sweep must *start* after the old attempt.
    result = get("actions/workflows/databricks-cleanup.yml/runs?branch=main&event=schedule&status=success&per_page=10")
    for run in result["workflow_runs"][:10]:
        if not (run["repository"]["full_name"] == run["head_repository"]["full_name"] == REPO and
                run["path"] == JANITOR and run["head_branch"] == "main" and run["event"] == "schedule" and
                completed(run, "success") and timestamp(run["run_started_at"]) > after):
            continue
        rows = jobs(get, run["id"], run["run_attempt"])
        if len(rows) != len(PROFILES) or {j["name"] for j in rows} != PROFILES:
            continue
        for job in rows:
            step = named(job.get("steps", []), RECONCILE)
            if not (completed(job, "success") and completed(step, "success") and
                    after < timestamp(step["started_at"]) <= timestamp(step["completed_at"])):
                break
        else:
            print(f"Confirmed all three managed cleanup profiles in run {run['id']}, attempt {run['run_attempt']}")
            return True
    return False


def verify(get, run, attempt, cell):
    require(run > 0 and 1 <= attempt <= 32, "Invalid run or more than 31 previous attempts to check")
    if attempt == 1:
        return
    current = get(f"actions/runs/{run}")
    require(current["id"] == run and current["run_attempt"] == attempt and current["status"] == "in_progress"
            and current["path"] == WORKFLOW and current["event"] == "workflow_run"
            and current["repository"]["full_name"] == REPO, "Not the active Databricks controller attempt")
    after = None
    for n in range(1, attempt):
        history = jobs(get, run, n)
        require(history, f"No job history for controller attempt {n}")
        row = named(history, cell)
        if not row:  # Failed-only reruns omit successful siblings.
            continue
        require(row["status"] == "completed" and row["conclusion"] in TERMINAL,
                f"Unknown prior result for {cell}, attempt {n}")
        if row["conclusion"] in ("success", "skipped"):
            continue
        if (row["conclusion"] == "cancelled" and "started_at" in row and row["started_at"] is None
                and row.get("steps") == [] and row.get("runner_id") == 0):
            continue  # Cancelled before a runner or any step started.
        steps = row.get("steps", [])
        if completed(named(steps, BUILD), "skipped") or completed(named(steps, CLEANUP), "success"):
            continue
        prior = get(f"actions/runs/{run}/attempts/{n}")
        require(prior["id"] == run and prior["run_attempt"] == n and prior["path"] == WORKFLOW and
                prior["status"] == "completed" and prior["conclusion"] in TERMINAL,
                f"Cannot establish when controller attempt {n} finished")
        end = timestamp(prior["updated_at"])
        require(timestamp(prior["run_started_at"]) <= timestamp(row["completed_at"]) <= end,
                f"Inconsistent completion time for {cell}, attempt {n}")
        after = max(after, end) if after else end
    if after and not reconciled(get, after):
        raise ValueError(f"{cell}: prior cluster cleanup is unconfirmed. Wait for the hourly managed cleanup "
                         f"on main to run all three profiles successfully after {after.isoformat()}, "
                         "then use Re-run failed jobs on this run. No new cluster was requested.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", required=True, choices=("17.3", "18.3"))
    parser.add_argument("--mode", required=True, choices=("CI_PART1", "CI_PART2"))
    args = parser.parse_args()
    require(os.environ["GITHUB_REPOSITORY"] == REPO and os.environ["GITHUB_EVENT_NAME"] == "workflow_run"
            and os.environ["GITHUB_REF"] == "refs/heads/main"
            and os.environ["GITHUB_WORKFLOW_REF"] == f"{REPO}/{WORKFLOW}@refs/heads/main",
            "Cleanup admission runs only in the trusted main Databricks controller")
    deadline = time.monotonic() + 90

    def get(path):
        remaining = deadline - time.monotonic()
        require(remaining > 0, "Timed out checking prior Databricks cleanup")
        return json.loads(subprocess.check_output(["gh", "api", "--method", "GET", f"repos/{REPO}/{path}"],
                                                  timeout=min(15, remaining)))

    verify(get, int(os.environ["GITHUB_RUN_ID"]), int(os.environ["GITHUB_RUN_ATTEMPT"]),
           f"DBR {args.runtime} / {args.mode}")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError) as exc:
        sys.exit(f"Databricks cleanup admission failed: {exc}")
