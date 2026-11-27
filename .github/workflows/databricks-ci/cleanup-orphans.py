# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Sweep one configured CI workspace after its exact owning GitHub run attempt has finished."""

import argparse
import os
from pathlib import Path
import re
import sys
import time

# The workflow uses python -I and checks out this directory from trusted main.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from attempts import Failure, MANAGER, PROVIDERS, REPO, WORKFLOW, absent_from_inventory, managed_tags
from runtimes import RUNTIME
from workload import creator_name, profile_config


# A previously supported DBR can still own an orphan after its shim is retired.
OWNER = re.compile(r"gha-([1-9][0-9]{0,19})-([1-9][0-9]{0,3})-"
                   rf"({RUNTIME})-CI_PART[12]-(aws2|aws|azure3)-[12]")
TERMINAL = {"success", "failure", "cancelled", "timed_out", "skipped", "neutral", "action_required",
            "stale", "startup_failure"}


class SweepError(Exception):
    pass


class InvalidClusterLookup(SweepError):
    """A rejected GET is not itself proof that a cluster is gone."""


def ownership(info, profile, creator, *, listing=False):
    if not isinstance(info, dict):
        raise SweepError("Invalid cluster record")
    actual_creator = info.get("creator_user_name")
    if isinstance(actual_creator, str) and actual_creator and actual_creator != creator:
        return None
    tags = info.get("custom_tags")
    if tags is None:
        tags = {}
    if not isinstance(tags, dict):
        raise SweepError("Invalid cluster tags")
    if tags.get("CI_MANAGED_BY") != MANAGER:
        name = info.get("cluster_name")
        if isinstance(name, str) and OWNER.fullmatch(name):
            raise SweepError("CI-named cluster is missing its management tag")
        return None
    tag = tags.get("CI_BUILD_TAG")
    match = OWNER.fullmatch(tag) if isinstance(tag, str) else None
    if (not match or info.get("cluster_name") != tag
            or any(tags.get(k) != v for k, v in managed_tags(match[4], tag).items())):
        raise SweepError("Incomplete or inconsistent managed-cluster identity")
    if match[4] != profile:  # Two configured profiles can point at the same workspace.
        return None
    if actual_creator != creator and not listing:
        raise SweepError("Managed cluster has no verified creator")
    return tag, int(match[1]), int(match[2])


def terminal_attempt(github, workflow_id, run_id, attempt):
    run = github(f"actions/runs/{run_id}/attempts/{attempt}")
    if (not isinstance(run, dict) or any(not isinstance(run.get(k), dict) or run[k].get("full_name") != REPO
                                         for k in ("repository", "head_repository")) or run.get("event") != "workflow_run"
            or run.get("head_branch") != "main" or run.get("path") != WORKFLOW
            or any(type(run.get(k)) is not int or run[k] != v for k, v in
                   (("id", run_id), ("run_attempt", attempt), ("workflow_id", workflow_id)))):
        raise SweepError("GitHub run attempt identity does not match the cluster owner")
    if run.get("status") == "completed" and run.get("conclusion") in TERMINAL:
        return True
    if run.get("status") in {"queued", "in_progress", "requested", "waiting", "pending"} and (
            "conclusion" in run and run["conclusion"] is None):
        return False
    raise SweepError("GitHub run attempt has indeterminate status")


def candidates(db, profile, creator):
    # Finish pagination before deleting; mutation can change the contents of later pages.
    result, tokens = {}, set()
    params = {"page_size": 100}
    for _ in range(100):
        page = db("GET", "list", params)
        entries = page.get("clusters", [])
        if not isinstance(entries, list):
            raise SweepError("Invalid clusters/list response")
        for info in entries:
            owner = ownership(info, profile, creator, listing=True)
            if owner:
                cid = info.get("cluster_id")
                if not isinstance(cid, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", cid):
                    raise SweepError("Invalid managed cluster ID")
                if info.get("creator_user_name") != creator:
                    detail = db("GET", "get", {"cluster_id": cid})
                    if detail is None:
                        continue
                    if detail.get("cluster_id") != cid:
                        raise SweepError("Unexpected cluster ID from workspace")
                    actual = detail.get("creator_user_name")
                    if isinstance(actual, str) and actual and actual != creator:
                        continue
                    if ownership(detail, profile, creator) != owner:
                        raise SweepError("Cannot confirm inventory ownership")
                if result.setdefault(cid, owner) != owner:
                    raise SweepError("Inconsistent managed cluster in workspace listing")
        token = page.get("next_page_token")
        if token is None or token == "":
            return result
        if not isinstance(token, str) or token in tokens:
            raise SweepError("Invalid or repeated clusters/list page token")
        tokens.add(token)
        params = {"page_size": 100, "page_token": token}
    raise SweepError("Cluster listing exceeded the page budget")


def remove(db, profile, creator, cid, owner):
    data = {"cluster_id": cid}
    deadline = time.monotonic() + 180
    terminating = purging = False
    while time.monotonic() < deadline:
        try:
            info = db("GET", "get", data)
        except InvalidClusterLookup:
            if not purging:
                raise
            try:
                if absent_from_inventory(lambda params: db("GET", "list", params), cid, deadline):
                    return True
            except Failure as exc:
                raise SweepError(str(exc)) from exc
            time.sleep(max(0, min(10, deadline - time.monotonic())))
            continue
        if info is None:  # Already absent; e.g. inline cleanup completed since the list.
            return purging
        if info.get("cluster_id") != cid or ownership(info, profile, creator) != owner:
            raise SweepError("Workspace ownership changed; skipping deletion")
        state = info.get("state")
        if time.monotonic() >= deadline:
            break
        if state == "TERMINATED":
            if not purging:
                if db("POST", "permanent-delete", data) is None:
                    return False
                purging = True
        elif state not in {"PENDING", "RUNNING", "RESTARTING", "RESIZING", "TERMINATING", "ERROR"} or purging:
            raise SweepError("Unexpected cluster state; withholding further deletion")
        elif not terminating and state != "TERMINATING":
            if db("POST", "delete", data) is None:
                return False
            terminating = True
        time.sleep(max(0, min(10, deadline - time.monotonic())))
    raise SweepError("Removal was not confirmed within the deadline")


def sweep(db, github, profile, creator):
    matches = candidates(db, profile, creator)
    if not matches:
        return 0, 0
    wf = github("actions/workflows/databricks-ci.yml")
    if wf.get("path") != WORKFLOW or type(wf.get("id")) is not int or wf["id"] <= 0:
        raise SweepError("Cannot verify the canonical Databricks workflow")
    deleted = errors = 0
    for cid, owner in matches.items():
        _, run, attempt = owner
        try:
            if terminal_attempt(github, wf["id"], run, attempt):
                deleted += int(remove(db, profile, creator, cid, owner))
        except SweepError as exc:
            errors += 1
            print(f"::warning::{profile}: {exc}", file=sys.stderr)
    if errors:
        raise SweepError(f"{errors} managed cluster(s) could not be verified or cleaned; retry on next sweep")
    return len(matches), deleted


def transport(base, token):
    import requests

    def call(method, endpoint, data=None):
        try:
            response = requests.request(method, base + endpoint, headers={"Authorization": "Bearer " + token},
                                        **({"params": data} if method == "GET" else {"json": data}),
                                        timeout=(10, 30), allow_redirects=False)
            if endpoint in {"get", "delete", "permanent-delete"}:
                if response.status_code == 404:
                    return None
                if response.status_code == 400:
                    detail = response.json()
                    error = detail.get("error_code") if isinstance(detail, dict) else None
                    if error == "RESOURCE_DOES_NOT_EXIST":
                        return None
                    if (method, endpoint, error) == ("GET", "get", "INVALID_PARAMETER_VALUE"):
                        raise InvalidClusterLookup("Cleanup cluster GET: HTTP 400 INVALID_PARAMETER_VALUE")
            if not 200 <= response.status_code < 300:
                raise SweepError(f"Cleanup API {method}: HTTP {response.status_code}")
            result = response.json() if response.content or method == "GET" else {}
            if not isinstance(result, dict):
                raise SweepError(f"Cleanup API {method}: invalid response")
            return result
        except (requests.RequestException, ValueError) as exc:
            raise SweepError(f"Cleanup API {method}: {type(exc).__name__}") from exc
    return call


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=PROVIDERS, required=True)
    profile = parser.parse_args().profile
    creator = creator_name(profile)
    db = transport(profile_config(profile)["host"].rstrip("/") + "/api/2.1/clusters/", os.environ["DATABRICKS_TOKEN"])
    gh = transport(f"https://api.github.com/repos/{REPO}/", os.environ["GH_TOKEN"])
    matched, removed = sweep(db, lambda path: gh("GET", path), profile, creator)
    print(f"{profile}: {matched} managed cluster(s) listed, {removed} removed")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        sys.exit("Databricks cleanup: " + (str(error) if isinstance(error, SweepError) else type(error).__name__))
