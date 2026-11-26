# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify the latest execution of each required matrix job on a selective rerun."""

import time


class NotVisible(ValueError):
    """The jobs API may not have exposed the terminal result yet."""


def passed(row, label):
    if row["status"] in ("queued", "in_progress", "waiting", "pending", "requested") and row["conclusion"] is None:
        raise NotVisible(f"{label}: still {row['status']}")
    if (row["status"], row["conclusion"]) != ("completed", "success"):
        raise ValueError(f"{label}: {row['status']} / {row['conclusion']}; a passing execution is required")


def require_jobs(run, current, steps, inactive, api, selection_attempt):
    if not steps or run < 1 or not 1 <= selection_attempt <= current:
        raise ValueError("Invalid run, selection attempt or empty matrix")
    deadline = time.monotonic() + 90
    for retry in range(3):
        needed = set(steps)
        try:
            # Newest execution wins. An explicit cancellation/failure must never borrow an older pass.
            for attempt in range(current, max(selection_attempt - 1, current - 32), -1):
                left = deadline - time.monotonic()
                if left <= 0:
                    raise ValueError("Timed out checking matrix job receipts")
                page = api(f"actions/runs/{run}/attempts/{attempt}/jobs?per_page=100", timeout=min(15, left))
                jobs = page["jobs"]
                if (not isinstance(jobs, list) or type(page["total_count"]) is not int
                        or len(jobs) != page["total_count"] or len(jobs) > 100
                        or any(not isinstance(j, dict) or not isinstance(j.get("name"), str)
                               or type(j.get("run_id")) is not int or j["run_id"] != run
                               or type(j.get("run_attempt", attempt)) is not int
                               or j.get("run_attempt", attempt) != attempt for j in jobs)):
                    raise ValueError(f"Incomplete or mismatched jobs for run {run} attempt {attempt}")
                if not jobs:
                    raise NotVisible(f"No visible jobs for run {run} attempt {attempt}")
                by_name = {}
                for job in jobs:
                    name = job["name"]
                    if name == "select" or name.startswith("Lane "):
                        if name in inactive and (job["status"], job["conclusion"]) != ("completed", "skipped"):
                            raise ValueError(f"Inactive route {name} did not skip in attempt {attempt}")
                        if name in by_name or (name != "select" and name not in steps and name not in inactive):
                            raise ValueError(f"Duplicate or unexpected job {name} in attempt {attempt}")
                        by_name[name] = job
                for name in needed.intersection(by_name):
                    job = by_name[name]
                    label = f"{name} in attempt {attempt}"
                    passed(job, label)
                    for step in steps[name]:
                        matches = [s for s in job["steps"] if s["name"] == step]
                        if len(matches) != 1:
                            raise ValueError(f"{label}: expected one '{step}' step, found {len(matches)}")
                        passed(matches[0], f"{label}: {step}")
                    needed.remove(name)
                if "select" in by_name:
                    passed(by_name["select"], f"select in attempt {attempt}")
                if not needed:
                    return
                if "select" in by_name:
                    break  # The previous selection may describe a different merge or workload.
            raise NotVisible("Missing passing jobs: " + ", ".join(sorted(needed)) + ". Re-run all jobs.")
        except NotVisible:
            if retry == 2 or time.monotonic() + 2 >= deadline:
                raise
            time.sleep(2)


def require_lanes(run, current, rows, *, api, selection_attempt=1):
    steps, inactive, groups = {}, set(), set()
    for row in rows:
        group, kind = row["group"], row["kind"]
        if not isinstance(group, str) or not group or group in groups:
            raise ValueError("Invalid or duplicate selected GPU group")
        groups.add(group)
        standalone = f"Lane {group} / Standalone"
        db = f"Lane {group} / Databricks"
        if kind in ("unit", "integration"):
            steps[standalone] = ("Run the shim group",)
            inactive.update((db, db + " / Cell"))
        elif kind == "databricks":
            steps[db + " / Cell"] = ("Build and test on Databricks", "Confirm cluster cleanup")
            inactive.add(standalone)
        else:
            raise ValueError(f"Unknown lane kind: {kind}")
    require_jobs(run, current, steps, inactive, api, selection_attempt)
