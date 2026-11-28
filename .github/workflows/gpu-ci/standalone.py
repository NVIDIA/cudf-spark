# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bind the standalone coordinator and worker to the same registration and live PR."""

import json
import os
from pathlib import Path
import re
import sys
import time

# Both files come from the same trusted controller revision, not the candidate checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "databricks-ci"))
from source import REPO, api, check, request, verify


def check_revision(inputs, pr, rev):
    check(rev["policy"][0] == "0" and inputs["title-policy"] == rev["policy"]
          and inputs["base-branch"] == pr["base"]["ref"] and inputs["base-sha"] == rev["base"]
          and inputs["merge-sha"] == rev["merge"], "PR revision or test policy changed")


def dispatch():
    i = json.loads(os.environ["INPUTS"])
    row = json.loads(i["row"])
    copy, number = request()
    check(row["kind"] in ("unit", "integration") and copy["conclusion"] == "success"
          and i["approval-run-id"] == str(copy["id"]) and i["approval-run-attempt"] == str(copy["run_attempt"])
          and i["pull-number"] == str(number) and i["head-sha"] == copy["head_sha"], "Invalid GPU CI registration")
    pr, rev = verify(copy, number)
    check_revision(i, pr, rev)
    policy = rev["policy"]
    payload = {**row, **{key: i[key] for key in ("base-branch", "base-sha", "merge-sha", "title-policy")},
               "parallel-ut": policy[3] == "0", "reduced-it": policy[2] == "1",
               "approval-run-id": copy["id"], "approval-run-attempt": copy["run_attempt"]}
    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        print("payload=" + json.dumps(payload, separators=(",", ":")), file=out)


def worker():
    w = json.loads(os.environ["WORKLOAD"])
    ref, sha = os.environ["SOURCE_REF"], os.environ["SOURCE_SHA"]
    match = re.fullmatch(r"refs/heads/pull-request/([1-9]\d*)", ref)
    arch = w.get("arch", "amd64")
    check(match and ref == os.environ["GITHUB_REF"] and sha == os.environ["GITHUB_SHA"]
          and arch in ("amd64", "arm64") and os.environ["RUNNER_ARCH"] == {"amd64": "X64", "arm64": "ARM64"}[arch],
          "Copied source or runner architecture does not match the GPU workload")
    check(type(w["approval-run-id"]) is int and w["approval-run-id"] > 0
          and type(w["approval-run-attempt"]) is int and w["approval-run-attempt"] > 0, "Invalid registration ID")
    copy = api(f"actions/runs/{w['approval-run-id']}")
    check(copy["id"] == w["approval-run-id"] and copy["run_attempt"] == w["approval-run-attempt"]
          and copy["event"] == "push" and copy["path"] == ".github/workflows/gpu-ci.yml"
          and copy["actor"]["login"] == "copy-pr-bot[bot]" and copy["conclusion"] == "success"
          and copy["head_repository"]["full_name"] == REPO and copy["head_branch"] == ref.removeprefix("refs/heads/")
          and copy["head_sha"] == sha, "GPU CI registration changed")
    pr, rev = verify(copy, match[1])
    check_revision(w, pr, rev)
    check(w["parallel-ut"] is (rev["policy"][3] == "0") and w["reduced-it"] is (rev["policy"][2] == "1"),
          "GPU workload options do not match the PR title")
    with open(os.environ["GITHUB_ENV"], "a") as out:
        print(f"GPU_ARCH={arch}", file=out)
        print(f"GPU_DEADLINE={int(time.time()) + (int(os.environ['WORKER_MINUTES']) - 30) * 60}", file=out)


if __name__ == "__main__":
    {"dispatch": dispatch, "worker": worker}[sys.argv[1]]()
