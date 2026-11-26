# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify the copy bot's PR revision; never load Python or Actions from the candidate."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

REPO = "NVIDIA/cudf-spark"
HERE = Path(__file__).resolve().parent
# Each lane has two routes; pending, select and result also occupy the 100-job receipt page.
MAX_LANES = (100 - 3) // 2


def api(path, *, timeout=45):
    return json.loads(subprocess.check_output(["gh", "api", f"repos/{REPO}/{path}"], timeout=timeout))


def check(ok, message):
    if not ok:
        raise ValueError(message)


def request():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    original = event["workflow_run"]
    run = api("actions/runs/" + str(int(original["id"])))
    branch = run["head_branch"]
    match = re.fullmatch(r"pull-request/([1-9][0-9]*)", branch)
    check(os.environ["GITHUB_REPOSITORY"] == REPO and os.environ["GITHUB_REF"] == "refs/heads/main"
          and match and run["event"] == "push" and run["path"] == ".github/workflows/gpu-ci.yml"
          and run["head_repository"]["full_name"] == REPO and run["actor"]["login"] == "copy-pr-bot[bot]"
          and run["workflow_id"] == api("actions/workflows/gpu-ci.yml")["id"]
          and run["id"] == original["id"] and run["run_attempt"] == original["run_attempt"]
          and run["head_sha"] == original["head_sha"],
          "Not the current copy-pr-bot GPU CI request")
    head = run["head_sha"]
    check(api(f"git/ref/heads/{branch}")["object"]["sha"] == head, "Copied branch has advanced")
    runs = api(f"actions/workflows/{run['workflow_id']}/runs?branch={branch}&event=push&head_sha={head}&per_page=1")
    check(runs["workflow_runs"] and runs["workflow_runs"][0]["id"] == run["id"],
          "A newer GPU CI registration owns this PR result")
    return run, match[1]


def verify(run, number):
    head = run["head_sha"]
    for delay in (0, 2, 5):
        time.sleep(delay)
        pr = api(f"pulls/{number}")
        check(pr["state"] == "open" and pr["head"]["sha"] == head and pr["base"]["repo"]["full_name"] == REPO,
              "PR is closed or no longer matches the copied head")
        if pr.get("mergeable") is not None:
            break
    base, merge = pr["base"]["sha"], pr["merge_commit_sha"]
    check(pr.get("mergeable") is True and all(re.fullmatch(r"[0-9a-f]{40}", x or "") for x in (base, head, merge)),
          "No current testable PR merge; retry once GitHub has computed it")
    policy = "".join("1" if re.search(regex, pr["title"], re.I) else "0" for regex in
                     (r"\[skip ci\]", r"\[databricks\]", r"\[reduced-it\]", r"\[serial[ -]ut\]"))
    for name, value in (("EXPECTED_BASE", base), ("EXPECTED_HEAD", head), ("EXPECTED_MERGE", merge),
                        ("EXPECTED_POLICY", policy)):
        check(not os.environ.get(name) or os.environ[name] == value, "PR revision changed after selection")
    # Also protects the final PR status against a stale synthetic merge.
    parents = api(f"git/commits/{merge}")["parents"]
    check([p["sha"] for p in parents] == [base, head], "Synthetic merge has unexpected parents")
    return pr, {"base": base, "head": head, "merge": merge, "policy": policy}


def db_rows(candidate, revisions, title):
    plan = json.loads(subprocess.check_output([
        sys.executable, "-I", str(HERE / "select-impact.py"), "--repo", str(candidate),
        "--base", revisions["base"], "--head", revisions["head"], "--merge", revisions["merge"],
    ], env={"PATH": os.environ["PATH"], "PR_TITLE": title}, timeout=90))
    check(type(plan["run"]) is bool and type(plan["reduced_it"]) is bool
          and plan["reduced_it"] == (revisions["policy"][2] == "1"), "Invalid Databricks selection")
    if not plan["run"]:
        return []
    check(os.environ.get("DB_ENABLED") == "true" and bool(os.environ.get("DB_RUNNER_GROUP")),
          "Databricks is required: configure the protected environment and runner group before enabling it")
    versions = json.loads(subprocess.check_output([
        sys.executable, "-I", str(HERE / "runtimes.py"), "--candidate", str(candidate), "--merge-sha", revisions["merge"],
    ], timeout=45))
    check(isinstance(versions, dict) and versions and all(re.fullmatch(r"[0-9]+\.[0-9]+", v) for v in versions),
          "No supported Databricks runtime found for the selected merge")
    return [dict(kind="databricks", group=f"db-{version}-part{n}", runtime=version, mode=f"CI_PART{n}")
            for version in sorted(versions) for n in (1, 2)]


def standalone_rows(candidate, revisions):
    matrix = json.loads(subprocess.check_output([
        "bash", str(HERE.parent / "gpu-ci" / "select-groups.sh"), str(candidate), revisions["base"], revisions["merge"],
    ], cwd=HERE.parents[2], timeout=90))
    rows = matrix["include"]
    check(isinstance(rows, list) and all(r["kind"] in ("unit", "integration") for r in rows),
          "Invalid standalone GPU selection")
    return rows


def require_lanes(rows):
    # python -I: load only trusted controller code, never the checked-out candidate.
    sys.path.insert(0, str(HERE.parent / "gpu-ci"))
    from job_receipts import require_lanes as verify_jobs

    verify_jobs(int(os.environ["GITHUB_RUN_ID"]), int(os.environ["GITHUB_RUN_ATTEMPT"]), rows,
                selection_attempt=int(os.environ["SELECTION_ATTEMPT"]), api=api)


def post_status(sha, state, description, run_id=None):
    subprocess.run(["gh", "api", "--silent", "--method", "POST", f"repos/{REPO}/statuses/{sha}",
                    "-f", "context=GPU CI gate", "-f", "state=" + state, "-f", "description=" + description,
                    "-f", "target_url=" + f"https://github.com/{REPO}/actions/runs/{run_id or os.environ['GITHUB_RUN_ID']}"],
                   check=True, timeout=45)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, help="Also select tests for the checked-out merge")
    parser.add_argument("--cell", action="store_true", help="Verify this Databricks runtime and part before allocation")
    parser.add_argument("--status", choices=("pending", "result"))
    args = parser.parse_args()
    if args.cell and (not args.candidate or args.status):
        parser.error("--cell requires --candidate without --status")
    run, number = request()
    pr, revisions = verify(run, number)
    if args.status == "pending":
        if run["status"] != "completed" or json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()).get("action") == "completed":
            post_status(revisions["merge"], "pending", "GPU CI is running")
        return
    if args.cell:
        check(all(os.environ.get("EXPECTED_" + k.upper()) == v for k, v in revisions.items()),
              "Missing or mismatched Databricks cell revision")
    if args.status == "result":
        if run["status"] != "completed" or run["conclusion"] != "success":
            post_status(revisions["merge"], "failure", "GPU CI registration did not pass; see this run", run_id=run["id"])
            raise SystemExit("GPU CI registration did not pass")
        try:
            check(os.environ["PLAN_RESULT"] == "success", "GPU CI selection did not pass")
            check(all(os.environ.get("EXPECTED_" + k.upper()) == v for k, v in revisions.items()),
                  "Missing or mismatched selection revision")
            rows = json.loads(os.environ["MATRIX"])["include"]
            check(isinstance(rows, list), "Missing selected GPU CI matrix")
            check(os.environ["LANE_RESULT"] == ("success" if rows else "skipped"),
                  "Selected GPU CI lanes did not all pass")
            if rows:
                require_lanes(rows)
        except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError):
            post_status(revisions["merge"], "failure", "GPU CI did not pass; inspect the lanes and re-run failed jobs here")
            raise
        post_status(revisions["merge"], "success",
                    "All selected GPU CI lanes passed" if rows else "GPU CI passed; no lanes were selected")
        return
    check(run["status"] == "completed" and run["conclusion"] == "success", "GPU CI registration did not pass")
    target = pr["base"]["ref"]
    release = re.fullmatch(r"release/([0-9]{2}\.[0-9]{2})", target)
    eligible = (target == "main" or bool(release and release[1] >= "27.02")) and revisions["policy"][0] == "0"
    outputs = {**revisions, "base_branch": target, "pull_number": number, "eligible": str(eligible).lower()}
    if args.candidate:
        candidate = args.candidate.resolve()
        db = db_rows(candidate, revisions, pr["title"]) if eligible else []
        if args.cell:
            check(any((r["runtime"], r["mode"]) == (os.environ["DB_RUNTIME"], os.environ["DB_MODE"]) for r in db),
                  "Databricks cell is not selected for this merge")
        else:
            rows = (standalone_rows(candidate, revisions) if eligible else []) + db
            check(len(rows) <= MAX_LANES, f"GPU CI selected {len(rows)} lanes; at most {MAX_LANES} fit on one job receipt page")
            check(len({r["group"] for r in rows}) == len(rows), "Duplicate GPU CI lane groups")
            outputs.update(matrix=json.dumps({"include": rows}, separators=(",", ":")),
                           selection_attempt=os.environ["GITHUB_RUN_ATTEMPT"])
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        for name, value in outputs.items():
            print(f"{name}={value}", file=out)


if __name__ == "__main__":
    main()
