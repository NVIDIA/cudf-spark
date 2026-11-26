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
          and run["run_attempt"] == original["run_attempt"] and run["head_sha"] == original["head_sha"],
          "Not the current copy-pr-bot GPU CI request")
    head = run["head_sha"]
    check(api(f"git/ref/heads/{branch}")["object"]["sha"] == head, "Approved copy has advanced")
    runs = api(f"actions/workflows/{run['workflow_id']}/runs?branch={branch}&event=push&head_sha={head}&per_page=1")
    check(runs["workflow_runs"] and runs["workflow_runs"][0]["id"] == run["id"],
          "A newer approval run owns this PR result")
    return run, match[1]


def verify(run, number):
    head = run["head_sha"]
    for delay in (0, 2, 5):
        time.sleep(delay)
        pr = api(f"pulls/{number}")
        check(pr["state"] == "open" and pr["head"]["sha"] == head and pr["base"]["repo"]["full_name"] == REPO,
              "PR is closed or no longer matches the approved copy")
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


def standalone(run, revisions):
    # The PR base may have changed since standalone tests ran, even for the same head SHA.
    jobs = api(f"actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100")
    expected = revisions["merge"] + " / " + revisions["policy"]
    matches = [j for j in jobs["jobs"] if j["name"] == "Standalone GPU CI result / " + expected]
    check(jobs["total_count"] == len(jobs["jobs"]) and len(matches) == 1 and
          matches[0]["status"] == "completed" and matches[0]["conclusion"] == "success",
          "No unambiguous successful standalone gate for this exact merge and run attempt")


def require_databricks_cells():
    # python -I: import only from the trusted workflow checkout, never the PR candidate.
    sys.path.insert(0, str(HERE.parent / "gpu-ci"))
    from job_receipts import require_jobs

    cells = {f"DBR {runtime} / {part}" for runtime in ("17.3", "18.3") for part in ("CI_PART1", "CI_PART2")}
    require_jobs(int(os.environ["GITHUB_RUN_ID"]), int(os.environ["GITHUB_RUN_ATTEMPT"]), cells, prefix="DBR ",
                 steps=("Build and test the approved merge on Databricks", "Confirm cluster cleanup"), api=api)


def post_status(sha, state, description, run_id=None):
    subprocess.run(["gh", "api", "--silent", "--method", "POST", f"repos/{REPO}/statuses/{sha}",
                    "-f", "context=GPU CI gate", "-f", "state=" + state, "-f", "description=" + description,
                    "-f", "target_url=" + f"https://github.com/{REPO}/actions/runs/{run_id or os.environ['GITHUB_RUN_ID']}"],
                   check=True, timeout=45)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, help="Also select tests for the checked-out merge")
    parser.add_argument("--status", choices=("pending", "result"))
    args = parser.parse_args()
    run, number = request()
    pr, revisions = verify(run, number)
    if args.status == "pending":
        if run["status"] != "completed" or json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()).get("action") == "completed":
            post_status(revisions["merge"], "pending", "GPU CI is evaluating the approved merge")
        return
    if args.status == "result" and (run["conclusion"] != "success" or os.environ["PLAN_RESULT"] != "success"
                                    or os.environ["DB_RESULT"] not in ("success", "skipped")):
        if run["conclusion"] != "success":
            post_status(revisions["merge"], "failure", "Standalone GPU CI did not pass; re-run failed jobs",
                        run_id=run["id"])
        else:
            post_status(revisions["merge"], "failure", "Databricks did not pass; re-run failed jobs here")
        raise SystemExit("GPU CI did not pass")
    target = pr["base"]["ref"]
    release = re.fullmatch(r"release/([0-9]{2}\.[0-9]{2})", target)
    supported = target == "main" or bool(release and release[1] >= "27.02")
    check(run["status"] == "completed" and run["conclusion"] == "success", "Standalone GPU CI did not pass")
    standalone(run, revisions)
    if args.status == "result":
        selected = os.environ["DB_SELECTED"]
        result = os.environ["DB_RESULT"]
        ok = (selected == "true" and result == "success") or (selected == "false" and result == "skipped")
        if ok and selected == "true":
            try:
                require_databricks_cells()
            except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError):
                post_status(revisions["merge"], "failure", "Could not confirm all four Databricks cells; see this run")
                raise
        post_status(revisions["merge"], "success" if ok else "failure", ("GPU CI passed, including all Databricks cells"
                    if selected == "true" else "GPU CI passed; Databricks was not selected")
                    if ok else "Missing result for the selected Databricks plan")
        check(ok, "Missing required result")
        return
    outputs = {**revisions, "supported": str(supported).lower()}
    if args.candidate and not supported:
        outputs.update(run="false", reduced_it="false")
    elif args.candidate:
        plan = json.loads(subprocess.check_output([
            sys.executable, "-I", str(HERE / "select-impact.py"), "--repo", str(args.candidate.resolve()),
            "--base", revisions["base"], "--head", revisions["head"], "--merge", revisions["merge"],
        ], env={"PATH": os.environ["PATH"], "PR_TITLE": pr["title"]}))
        for name in ("run", "reduced_it"):
            check(type(plan[name]) is bool, "Invalid Databricks selection")
            outputs[name] = str(plan[name]).lower()
        if plan["run"]:
            check(os.environ.get("DB_ENABLED") == "true" and bool(os.environ.get("DB_RUNNER_GROUP")),
                  "Databricks is required: configure the protected environment and runner group before enabling it")
        if "EXPECTED_REDUCED" in os.environ:
            check(outputs["run"] == "true" and outputs["reduced_it"] == os.environ["EXPECTED_REDUCED"],
                  "Databricks selection changed; re-run GPU CI")
        print("Databricks selected: " + outputs["run"] + "; reduced IT: " + outputs["reduced_it"])
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        for name, value in outputs.items():
            print(f"{name}={value}", file=out)


if __name__ == "__main__":
    main()
