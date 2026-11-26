# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the trusted status controller using independent GitHub API snapshots."""

from copy import deepcopy
import json
import os
from pathlib import Path
import runpy
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import source


BASE, HEAD, MERGE, OTHER = (c * 40 for c in "abcd")


class SourceTest(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        self.event, self.output = root / "event.json", root / "output"
        self.run = dict(id=88, run_attempt=2, workflow_id=7, path=".github/workflows/gpu-ci.yml",
                        event="push", head_branch="pull-request/12", head_sha=HEAD,
                        actor={"login": "copy-pr-bot[bot]"}, head_repository={"full_name": source.REPO},
                        status="completed", conclusion="success")
        self.event.write_text(json.dumps({"action": "completed", "workflow_run": deepcopy(self.run)}))
        self.pr = dict(number=12, state="open", title="Documentation", mergeable=True, merge_commit_sha=MERGE,
                       head={"sha": HEAD}, base={"sha": BASE, "ref": "main", "repo": {"full_name": source.REPO}})
        self.parents = [{"sha": BASE}, {"sha": HEAD}]
        self.jobs = {"total_count": 1, "jobs": [{"name": "Standalone GPU CI result / " + MERGE + " / 0000",
                                                "status": "completed", "conclusion": "success"}]}
        self.cells = [dict(run_id=99, run_attempt=1, name=f"DBR {runtime} / {part}", status="completed",
                           conclusion="success", steps=[dict(name=name, status="completed", conclusion="success")
                           for name in ("Build and test the approved merge on Databricks", "Confirm cluster cleanup")])
                      for runtime in ("17.3", "18.3") for part in ("CI_PART1", "CI_PART2")]
        self.db_history = {1: {"total_count": 4, "jobs": self.cells}}
        self.copy, self.latest = HEAD, 88
        self.run_id, self.pr_number = 88, 12
        self.patch(patch.dict(os.environ, {"GITHUB_REPOSITORY": source.REPO, "GITHUB_REF": "refs/heads/main",
                   "GITHUB_RUN_ID": "99", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_EVENT_PATH": str(self.event),
                   "GITHUB_OUTPUT": str(self.output),
                   "PATH": os.defpath, "PLAN_RESULT": "success", "DB_SELECTED": "false",
                   "DB_RESULT": "skipped", "EXPECTED_BASE": BASE, "EXPECTED_HEAD": HEAD,
                   "EXPECTED_MERGE": MERGE, "EXPECTED_POLICY": "0000"}, clear=True))
        self.patch(patch.object(source, "api", side_effect=self.api))
        self.patch(patch.object(source.time, "sleep"))
        self.posts = self.patch(patch.object(source.subprocess, "run"))

    def patch(self, p):
        self.addCleanup(p.stop)
        return p.start()

    def api(self, path, *, timeout=45):
        branch = f"pull-request/{self.pr_number}"
        views = {f"actions/runs/{self.run_id}": self.run, "actions/workflows/gpu-ci.yml": {"id": 7},
                 f"git/ref/heads/{branch}": {"object": {"sha": self.copy}},
                 f"actions/workflows/7/runs?branch={branch}&event=push&head_sha={HEAD}&per_page=1":
                     {"workflow_runs": [{"id": self.latest}]},
                 f"pulls/{self.pr_number}": self.pr,
                 f'git/commits/{self.pr["merge_commit_sha"]}': {"parents": self.parents},
                 f"actions/runs/{self.run_id}/attempts/2/jobs?per_page=100": self.jobs}
        views.update({f"actions/runs/99/attempts/{n}/jobs?per_page=100": page for n, page in self.db_history.items()})
        return deepcopy(views[path])

    def invoke(self, *args):
        with patch.object(sys, "argv", ["source.py", *args]):
            source.main()

    def assert_post(self, state, sha=MERGE):
        self.posts.assert_called_once()
        cmd = self.posts.call_args.args[0]
        self.assertIn("state=" + state, cmd)
        self.assertIn(f"repos/{source.REPO}/statuses/{sha}", cmd)
        self.assertNotIn(f"repos/{source.REPO}/statuses/{HEAD}", cmd)

    def test_no_db_plan_is_not_a_standalone_skip(self):
        with patch.object(source.subprocess, "check_output", return_value=b'{"run":false,"reduced_it":false}') as plan:
            self.invoke("--candidate", ".")
        cmd = plan.call_args.args[0]
        self.assertEqual(Path(cmd[2]).resolve(), source.HERE / "select-impact.py")
        self.assertEqual(cmd[-6:], ["--base", BASE, "--head", HEAD, "--merge", MERGE])
        self.assertIn("run=false", self.output.read_text().splitlines())
        self.posts.assert_not_called()

    def test_cluster_impact_is_independent_of_cpu_helper_tests(self):
        impact = runpy.run_path(str(source.HERE / "select-impact.py"))["cluster_impact"]
        cases = [
            ([], False),
            ([".github/workflows/gpu-ci/job_receipts.py", "README.md"], False),
            ([".github/actions/spark-archive-cache/archive.py"], False),
            ([".github/workflows/databricks-cleanup.yml"], True),
            (["scala2.13/delta-lake/pom.xml"], True),
            (["sql-plugin/src/main/spark400db173/Compat.scala"], True),
            (["sql-plugin/src/main/spark400/Compat.scala"], False),
            (["skills/databricks/setup.sh", "skills/docs.md"], False),
            (["skills/databricks/setup.sh", "README.md"], True),
        ]
        for paths, expected in cases:
            with self.subTest(paths=paths):
                self.assertEqual(impact([os.fsencode(path) for path in paths]), expected)

    def test_db_selection_preserves_reduced_policy_and_requires_configured_runner(self):
        self.pr["title"] = "[Databricks] [REDUCED-IT]"
        self.jobs["jobs"][0]["name"] = f"Standalone GPU CI result / {MERGE} / 0110"
        with patch.dict(os.environ, {"EXPECTED_POLICY": "0110", "EXPECTED_REDUCED": "true",
                                     "DB_ENABLED": "true"}), patch.object(
                source.subprocess, "check_output", return_value=b'{"run":true,"reduced_it":true}'):
            with self.assertRaisesRegex(ValueError, "runner group"):
                self.invoke("--candidate", ".")
            with patch.dict(os.environ, {"DB_RUNNER_GROUP": "test-only"}):
                with patch.dict(os.environ, {"DB_ENABLED": "false"}), self.assertRaisesRegex(ValueError, "required"):
                    self.invoke("--candidate", ".")
                self.invoke("--candidate", ".")
        outputs = dict(line.split("=", 1) for line in self.output.read_text().splitlines())
        self.assertEqual((outputs["run"], outputs["reduced_it"], outputs["policy"]), ("true", "true", "0110"))
        self.posts.assert_not_called()

    def test_skip_success_reports_on_the_exact_merge_only(self):
        self.db_history.clear()  # No DB inventory lookup for a verified no-DB plan.
        self.invoke("--status", "result")
        self.assert_post("success")

    def test_two_prs_sharing_one_head_receive_independent_results(self):
        self.invoke("--status", "result")
        self.assert_post("success")
        self.posts.reset_mock()
        self.run_id, self.latest, self.pr_number = 89, 89, 13
        self.run.update(id=89, head_branch="pull-request/13")
        self.event.write_text(json.dumps({"action": "completed", "workflow_run": self.run}))
        self.pr.update(number=13, merge_commit_sha=OTHER)
        self.pr["base"]["sha"] = "e" * 40
        self.parents = [{"sha": "e" * 40}, {"sha": HEAD}]
        self.jobs["jobs"][0]["name"] = f"Standalone GPU CI result / {OTHER} / 0000"
        with patch.dict(os.environ, {"EXPECTED_BASE": "e" * 40, "EXPECTED_MERGE": OTHER,
                                     "DB_SELECTED": "true", "DB_RESULT": "failure"}):
            with self.assertRaisesRegex(SystemExit, "did not pass"):
                self.invoke("--status", "result")
        self.assert_post("failure", OTHER)

    def test_late_pending_event_cannot_overwrite_a_finished_run(self):
        self.event.write_text(json.dumps({"action": "in_progress", "workflow_run": self.run}))
        self.invoke("--status", "pending")
        self.posts.assert_not_called()
        with patch.dict(self.run, {"status": "in_progress", "conclusion": None}):
            self.invoke("--status", "pending")
        self.assert_post("pending")

    def test_completed_notification_sets_pending_before_databricks_work(self):
        self.invoke("--status", "pending")
        self.assert_post("pending")

    def test_changed_title_cannot_reuse_previous_green(self):
        for title in ("[Databricks]", "[REDUCED-IT]", "[serial ut]", "[skip ci]"):
            with self.subTest(title=title), patch.dict(self.pr, {"title": title}):
                with self.assertRaises(ValueError):
                    self.invoke("--status", "result")
                self.posts.assert_not_called()

    def test_selected_requires_success_and_skip_requires_no_cells(self):
        for selected, result, ok in (("true", "success", True), ("true", "skipped", False),
                                     ("false", "success", False), ("", "skipped", False)):
            with self.subTest(selected=selected, result=result), patch.dict(os.environ,
                    {"DB_SELECTED": selected, "DB_RESULT": result}):
                self.posts.reset_mock()
                if ok:
                    self.invoke("--status", "result")
                else:
                    with self.assertRaises(ValueError):
                        self.invoke("--status", "result")
                self.assert_post("success" if ok else "failure")

    def test_failed_or_cancelled_work_never_reports_success(self):
        for field, result in (("parent", "failure"), ("parent", "cancelled"), ("plan", "failure"),
                              ("plan", "cancelled"), ("db", "failure"), ("db", "cancelled")):
            with self.subTest(field=field, result=result):
                self.posts.reset_mock()
                with patch.dict(self.run, {"conclusion": result} if field == "parent" else {}), patch.dict(
                        os.environ, {"PLAN_RESULT": result} if field == "plan" else
                        {"DB_RESULT": result} if field == "db" else {}):
                    with self.assertRaisesRegex(SystemExit, "did not pass"):
                        self.invoke("--status", "result")
                self.assert_post("failure")

    def test_failed_only_replay_keeps_verified_siblings_from_same_controller(self):
        with patch.dict(os.environ, GITHUB_RUN_ATTEMPT="2", DB_SELECTED="true", DB_RESULT="success"):
            rerun = dict(self.cells[-1], run_attempt=2)
            self.cells[-1]["conclusion"] = "failure"
            self.db_history[2] = {"total_count": 1, "jobs": [rerun]}
            self.invoke("--status", "result")
            self.assert_post("success")
            self.posts.reset_mock()
            new_plan = dict(name="select", run_id=99, run_attempt=2, status="completed", conclusion="success")
            self.db_history[2] = {"total_count": 2, "jobs": [rerun, new_plan]}
            with self.assertRaisesRegex(ValueError, "Missing passing jobs.*CI_PART"):
                self.invoke("--status", "result")
            self.assert_post("failure")

    def test_bad_current_cell_cannot_fall_back_to_its_older_success(self):
        cell = dict(self.cells[-1], run_attempt=2)
        cases = ([dict(cell, conclusion="cancelled")], [dict(cell, conclusion="skipped")],
                 [dict(cell, status="queued", conclusion=None)], [cell, cell],
                 [dict(cell, steps=[dict(step, conclusion="skipped") for step in cell["steps"]])],
                 [dict(cell, steps=cell["steps"][:1])], [dict(cell, steps=cell["steps"] * 2)],
                 [dict(cell, run_id=88)], [dict(cell, run_attempt=1)], [dict(cell, name="DBR 99 / CI_PART1")])
        for rows in cases:
            with self.subTest(rows=rows), patch.dict(os.environ, GITHUB_RUN_ATTEMPT="2", DB_SELECTED="true",
                                                     DB_RESULT="success"):
                self.posts.reset_mock()
                self.db_history[2] = {"total_count": len(rows), "jobs": rows}
                with self.assertRaises(ValueError):
                    self.invoke("--status", "result")
                self.assert_post("failure")

    def test_missing_unseen_truncated_job_history_or_api_failure_sets_failure(self):
        for page in ({"total_count": 4, "jobs": self.cells[:-1]},
                     {"total_count": 3, "jobs": self.cells[:-1]}, {"jobs": []}, None):
            with self.subTest(page=page), patch.dict(os.environ, DB_SELECTED="true", DB_RESULT="success"):
                self.posts.reset_mock()
                self.db_history = {1: page} if page is not None else {}
                with self.assertRaises((ValueError, KeyError)):
                    self.invoke("--status", "result")
                self.assert_post("failure")

    def test_result_only_rerun_requires_four_actual_previous_successes(self):
        self.db_history[2] = {"total_count": 1, "jobs": [{"name": "result", "run_id": 99, "run_attempt": 2}]}
        with patch.dict(os.environ, GITHUB_RUN_ATTEMPT="2", DB_SELECTED="true", DB_RESULT="success"):
            self.invoke("--status", "result")
            self.assert_post("success")
            for conclusion in ("failure", "cancelled", "skipped"):
                self.posts.reset_mock()
                with patch.dict(self.cells[-1], conclusion=conclusion), self.assertRaisesRegex(ValueError, "attempt 1"):
                    self.invoke("--status", "result")
                self.assert_post("failure")
            self.posts.reset_mock()
            with patch.object(source.time, "monotonic", side_effect=[0, 1, 91]), self.assertRaisesRegex(ValueError, "Timed out"):
                self.invoke("--status", "result")
            self.assert_post("failure")

    def test_wrong_merge_lineage_cannot_publish(self):
        for parents in ([HEAD, BASE], [BASE, OTHER], [OTHER, HEAD], [BASE, HEAD, OTHER]):
            with self.subTest(parents=parents):
                self.parents = [{"sha": sha} for sha in parents]
                with self.assertRaisesRegex(ValueError, "parents"):
                    self.invoke("--status", "result")
                self.posts.assert_not_called()

    def test_base_advanced_after_plan_with_same_head(self):
        self.pr["base"]["sha"] = OTHER
        with self.assertRaisesRegex(ValueError, "revision changed"):
            self.invoke("--status", "result")
        self.posts.assert_not_called()

    def test_wrong_actor_repository_or_attempt_cannot_publish(self):
        for change in ({"actor": {"login": "someone"}}, {"head_repository": {"full_name": "fork/repo"}},
                       {"run_attempt": 3}, {"workflow_id": 8}, {"event": "workflow_dispatch"}):
            with self.subTest(change=change), patch.dict(self.run, change):
                with self.assertRaisesRegex(ValueError, "copy-pr-bot"):
                    self.invoke("--status", "result")
                self.posts.assert_not_called()

    def test_approval_superseded_or_copy_moved(self):
        for self.latest, self.copy in ((90, HEAD), (88, OTHER)):
            with self.assertRaises(ValueError):
                self.invoke("--status", "result")
            self.posts.assert_not_called()

    def test_missing_duplicate_or_unrelated_standalone_receipt(self):
        good = deepcopy(self.jobs["jobs"][0])
        for jobs, n in (([], 0), ([good, good], 2), ([good], 2),
                        ([good, dict(good, conclusion="failure")], 2),
                        ([dict(good, status="in_progress")], 1),
                        ([dict(good, name="Standalone GPU CI result / " + OTHER)], 1),
                        ([dict(good, conclusion="skipped")], 1)):
            with self.subTest(reported=n, jobs=jobs):
                self.posts.reset_mock()
                self.jobs = {"jobs": jobs, "total_count": n}
                with self.assertRaisesRegex(ValueError, "standalone gate"):
                    self.invoke("--status", "result")
                self.posts.assert_not_called()
