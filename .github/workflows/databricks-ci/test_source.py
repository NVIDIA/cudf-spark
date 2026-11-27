# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the trusted status controller using independent GitHub API snapshots."""

from copy import deepcopy
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import source

with patch.object(sys, "path", [str(source.HERE.parent / "gpu-ci"), *sys.path]):
    from groups import gpu_matrix
    import select_impact


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
        self.db = [dict(kind="databricks", runtime=v, mode=f"CI_PART{i}", group=f"db-{v}-part{i}")
                   for v in ("17.3", "18.3") for i in (1, 2)]
        self.matrix = json.dumps({"include": self.db})
        self.cells = [dict(run_id=99, run_attempt=1, name=f"Lane {row['group']} / Databricks / Cell",
                           status="completed", conclusion="success",
                           steps=[dict(name=name, status="completed", conclusion="success") for name in
                                  ("Build and test on Databricks", "Confirm cluster cleanup")])
                      for row in self.db]
        self.db_history = {1: {"total_count": 4, "jobs": self.cells}}
        self.copy, self.latest = HEAD, 88
        self.run_id, self.pr_number = 88, 12
        self.patch(patch.dict(os.environ, {"GITHUB_REPOSITORY": source.REPO, "GITHUB_REF": "refs/heads/main",
                   "GITHUB_RUN_ID": "99", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_EVENT_PATH": str(self.event),
                   "GITHUB_OUTPUT": str(self.output),
                   "PATH": os.defpath, "PLAN_RESULT": "success", "MATRIX": '{"include":[]}',
                   "LANE_RESULT": "skipped", "SELECTION_ATTEMPT": "1", "EXPECTED_BASE": BASE, "EXPECTED_HEAD": HEAD,
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
                 f'git/commits/{self.pr["merge_commit_sha"]}': {"parents": self.parents}}
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
        oss = [{"kind": "unit", "group": "ut-1"}]
        with patch.object(source.subprocess, "check_output", side_effect=[
                b'{"run":false,"reduced_it":false}', json.dumps({"include": oss}).encode()]) as plan:
            self.invoke("--candidate", ".")
        cmd = plan.call_args_list[0].args[0]
        self.assertEqual(Path(cmd[2]).resolve(), source.HERE / "select-impact.py")
        self.assertEqual(cmd[-6:], ["--base", BASE, "--head", HEAD, "--merge", MERGE])
        self.assertEqual(plan.call_args_list[1].args[0], ["bash", str(source.HERE.parent / "gpu-ci/select-groups.sh"),
                                                        str(Path(".").resolve()), BASE, MERGE])
        outputs = dict(line.split("=", 1) for line in self.output.read_text().splitlines())
        self.assertEqual(json.loads(outputs["matrix"])["include"], oss)
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

    def test_combined_lane_selection_from_the_same_changes(self):
        root = source.HERE.parents[2]
        db_select = runpy.run_path(str(source.HERE / "select-impact.py"))["select"]
        catalog = json.loads((root / ".github/gpu-ci-suites.json").read_text())["suites"]
        scala = "tests/src/test/scala/com/nvidia/spark/rapids/ExampleSuite.scala"
        python = "integration_tests/src/main/python/example_test.py"
        db = "jenkins/databricks/test.sh"
        suite, case = ["com.nvidia.spark.rapids.ExampleSuite"], ["example_test.py::test_example"]
        ut_full = {r["group"]: None for r in catalog["unit"]["groups"]}
        it_full = {r["group"]: None for r in catalog["integration"]["groups"]}
        ut_target = dict.fromkeys(ut_full, suite)
        it_target = {r["group"]: case for r in catalog["integration"]["groups"] if r["shard"] == 1}
        unavailable = subprocess.CalledProcessError(128, ["git", "diff"])
        versions = json.dumps({"17.3": "4.0.0", "18.3": "4.1.0"})
        combinations = [
            ([scala], ut_target, {}, False),
            ([python], {}, it_target, False),
            ([scala, db], ut_target, {}, True),
            ([python, db], {}, it_target, True),
            ([scala, python], ut_target, it_target, False),
            ([scala, python, db], ut_target, it_target, True),
            ([".github/workflows/gpu-ci/check-reports.py", python, db], ut_full, it_target, True),
            ([scala, ".github/workflows/gpu-ci/it.sh"], ut_target, it_full, False),
            ([db], {}, {}, True),
            (["pom.xml", scala, python], ut_full, it_full, False),
            (None, ut_full, it_full, True),  # Neither selector can read the diff.
        ]

        def base_json(repo, ref, path):
            self.assertEqual(ref, BASE)
            return json.loads((repo / path).read_text())

        for paths, ut, it, needs_db in combinations:
            with self.subTest(paths=paths):
                diff = unavailable if paths is None else b"\0".join(os.fsencode(p) for p in paths)
                with patch.object(subprocess, "check_output", side_effect=[f"{MERGE} {BASE} {HEAD}".encode(), diff]):
                    db_plan = db_select(root, BASE, HEAD, MERGE, self.pr["title"])
                self.assertEqual(db_plan["run"], needs_db)

                modes = {p: "100755" if p.endswith(".sh") else "100644" for p in paths or []}
                raw = b"".join(f":{mode} {mode} aabbcc ddeeff M\0{p}\0".encode() for p, mode in modes.items())
                with (
                    patch.object(subprocess, "check_output", side_effect=[unavailable if paths is None else raw]),
                    patch.object(select_impact, "base_json", side_effect=base_json),
                    patch.object(select_impact, "affected_suites", return_value=suite),
                    patch.object(select_impact, "affected_cases", return_value={1: case}),
                ):
                    standalone = gpu_matrix(root, ["unit", "integration"], BASE, MERGE)["include"]
                self.output.unlink(missing_ok=True)
                with (
                    patch.dict(os.environ, DB_ENABLED="true", DB_RUNNER_GROUP="test-only"),
                    patch.object(source, "standalone_rows", return_value=standalone),
                    patch.object(subprocess, "check_output", side_effect=[json.dumps(db_plan), versions]),
                ):
                    self.invoke("--candidate", str(root))
                outputs = dict(line.split("=", 1) for line in self.output.read_text().splitlines())
                rows = json.loads(outputs["matrix"])["include"]
                self.assertEqual({r["group"]: r.get("targets") for r in rows if r["kind"] != "databricks"}, ut | it)
                self.assertEqual([r for r in rows if r["kind"] == "databricks"], self.db if needs_db else [])

    def test_missing_invalid_or_disagreeing_base_rules_keep_full_standalone(self):
        root = source.HERE.parents[2]
        kinds = ["unit", "integration"]
        full = gpu_matrix(root, kinds)
        db_path = "jenkins/databricks/test.sh"
        current_rules = json.loads((root / select_impact.RULES_PATH).read_text())
        for base_rules in (FileNotFoundError("no impact policy"), {"file_owners": {}}, current_rules):
            def base_json(repo, ref, path):
                self.assertEqual(ref, BASE)  # Never use the candidate's policy to drop its own lanes.
                if path == select_impact.DB_RULES_PATH:
                    return {"cluster": {"exclusive_files": []}}
                self.assertEqual(path, select_impact.RULES_PATH)
                if isinstance(base_rules, Exception):
                    raise base_rules
                return base_rules

            with (
                self.subTest(base_rules=base_rules),
                patch.object(select_impact, "modified_files", return_value={db_path: "100755"}),
                patch.object(select_impact, "base_json", side_effect=base_json),
            ):
                self.assertEqual(gpu_matrix(root, kinds, BASE, MERGE), full)

    def test_db_selection_preserves_reduced_policy_and_requires_configured_runner(self):
        self.pr["title"] = "[Databricks] [REDUCED-IT]"
        env = {"EXPECTED_POLICY": "0110", "DB_ENABLED": "true"}
        impact = b'{"run":true,"reduced_it":true}'
        versions = b'{"17.3":"4.0.0","18.3":"4.1.0"}'
        with patch.dict(os.environ, env), patch.object(source.subprocess, "check_output", return_value=impact) as cmd:
            with self.assertRaisesRegex(ValueError, "runner group"):
                self.invoke("--candidate", ".")
            with patch.dict(os.environ, DB_RUNNER_GROUP="test-only"):
                with patch.dict(os.environ, DB_ENABLED="false"), self.assertRaisesRegex(ValueError, "required"):
                    self.invoke("--candidate", ".")
                oss = [{"kind": "integration", "group": "it-1"}]
                cmd.side_effect = [impact, versions, json.dumps({"include": oss}).encode()]
                self.invoke("--candidate", ".")
                outputs = dict(line.split("=", 1) for line in self.output.read_text().splitlines())
                self.assertEqual(json.loads(outputs["matrix"])["include"], oss + self.db)
                self.assertEqual((outputs["policy"], outputs["selection_attempt"]), ("0110", "1"))
                for runtime, valid in (("17.3", True), ("99.1", False)):
                    cmd.side_effect = [impact, versions]
                    with patch.dict(os.environ, DB_RUNTIME=runtime, DB_MODE="CI_PART1"):
                        if valid:
                            self.invoke("--cell", "--candidate", ".")
                        else:
                            with self.assertRaisesRegex(ValueError, "not selected"):
                                self.invoke("--cell", "--candidate", ".")
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
        with patch.dict(os.environ, {"EXPECTED_BASE": "e" * 40, "EXPECTED_MERGE": OTHER,
                                     "MATRIX": self.matrix, "LANE_RESULT": "failure"}):
            with self.assertRaisesRegex(ValueError, "lanes did not all pass"):
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
        for selected, result, ok in ((self.matrix, "success", True), (self.matrix, "skipped", False),
                                     ('{"include":[]}', "success", False), ("", "skipped", False)):
            with self.subTest(selected=selected, result=result), patch.dict(os.environ,
                    {"MATRIX": selected, "LANE_RESULT": result}):
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
                        {"LANE_RESULT": result} if field == "db" else {}):
                    with self.assertRaises((ValueError, SystemExit)):
                        self.invoke("--status", "result")
                self.assert_post("failure")

    def test_failed_only_replay_keeps_verified_siblings_from_same_controller(self):
        with patch.dict(os.environ, GITHUB_RUN_ATTEMPT="2", MATRIX=self.matrix, LANE_RESULT="success"):
            rerun = dict(self.cells[-1], run_attempt=2)
            self.cells[-1]["conclusion"] = "failure"
            self.db_history[2] = {"total_count": 1, "jobs": [rerun]}
            self.invoke("--status", "result")
            self.assert_post("success")
            self.posts.reset_mock()
            new_plan = dict(name="select", run_id=99, run_attempt=2, status="completed", conclusion="success")
            self.db_history[2] = {"total_count": 2, "jobs": [rerun, new_plan]}
            with self.assertRaisesRegex(ValueError, "Missing passing jobs"):
                self.invoke("--status", "result")
            self.assert_post("failure")

    def test_bad_current_cell_cannot_fall_back_to_its_older_success(self):
        cell = dict(self.cells[-1], run_attempt=2)
        cases = ([dict(cell, conclusion="cancelled")], [dict(cell, conclusion="skipped")],
                 [dict(cell, status="queued", conclusion=None)], [cell, cell],
                 [dict(cell, steps=[dict(step, conclusion="skipped") for step in cell["steps"]])],
                 [dict(cell, steps=cell["steps"][:1])], [dict(cell, steps=cell["steps"] * 2)],
                 [dict(cell, run_id=88)], [dict(cell, run_attempt=1)], [dict(cell, name="Lane db-99-part1 / Databricks / Cell")])
        for rows in cases:
            with self.subTest(rows=rows), patch.dict(os.environ, GITHUB_RUN_ATTEMPT="2", MATRIX=self.matrix,
                                                     LANE_RESULT="success"):
                self.posts.reset_mock()
                self.db_history[2] = {"total_count": len(rows), "jobs": rows}
                with self.assertRaises(ValueError):
                    self.invoke("--status", "result")
                self.assert_post("failure")

    def test_missing_unseen_truncated_job_history_or_api_failure_sets_failure(self):
        for page in ({"total_count": 4, "jobs": self.cells[:-1]},
                     {"total_count": 3, "jobs": self.cells[:-1]}, {"jobs": []}, None):
            with self.subTest(page=page), patch.dict(os.environ, MATRIX=self.matrix, LANE_RESULT="success"):
                self.posts.reset_mock()
                self.db_history = {1: page} if page is not None else {}
                with self.assertRaises((ValueError, KeyError)):
                    self.invoke("--status", "result")
                self.assert_post("failure")

    def test_result_only_rerun_requires_four_actual_previous_successes(self):
        self.db_history[2] = {"total_count": 1, "jobs": [{"name": "result", "run_id": 99, "run_attempt": 2}]}
        with patch.dict(os.environ, GITHUB_RUN_ATTEMPT="2", MATRIX=self.matrix, LANE_RESULT="success"):
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

    def test_combined_gate_requires_each_standalone_and_databricks_lane(self):
        rows = [{"kind": "unit", "group": "ut"}, {"kind": "integration", "group": "it"}, self.db[0]]
        step = dict(name="Run the shim group", status="completed", conclusion="success")
        jobs = [dict(run_id=99, name=f"Lane {group} / Standalone", status="completed", conclusion="success",
                     steps=[step]) for group in ("ut", "it")] + [self.cells[0]]
        jobs.append(dict(run_id=99, name="Lane ut / Databricks", status="completed", conclusion="skipped"))
        self.db_history[1] = {"total_count": len(jobs), "jobs": jobs}
        with patch.dict(os.environ, MATRIX=json.dumps({"include": rows}), LANE_RESULT="success"):
            self.invoke("--status", "result")
            self.assert_post("success")
            for lane in jobs[:3]:
                self.posts.reset_mock()
                with patch.dict(lane, conclusion="cancelled"), self.assertRaises(ValueError):
                    self.invoke("--status", "result")
                self.assert_post("failure")
            self.posts.reset_mock()
            with patch.dict(jobs[-1], conclusion="success"), self.assertRaisesRegex(ValueError, "Inactive route"):
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

    def test_skip_or_unsupported_branch_does_not_call_selectors(self):
        for branch, title, policy in (("main", "[skip ci]", "1000"), ("release/26.12", "[databricks]", "0100")):
            with self.subTest(branch=branch), patch.dict(self.pr["base"], ref=branch), patch.dict(self.pr, title=title):
                with patch.dict(os.environ, EXPECTED_POLICY=policy), patch.object(source.subprocess, "check_output") as cmd:
                    self.invoke("--candidate", ".")
                    cmd.assert_not_called()
                    self.assertIn('matrix={"include":[]}', self.output.read_text().splitlines())

    def test_lane_limit_keeps_combined_job_receipts_on_one_page(self):
        standalone = [{"kind": "unit", "group": f"ut-{i}"} for i in range(44)]
        with patch.object(source, "db_rows", return_value=self.db), patch.object(source, "standalone_rows", return_value=standalone):
            self.invoke("--candidate", ".")
            outputs = dict(line.split("=", 1) for line in self.output.read_text().splitlines())
            self.assertEqual(len(json.loads(outputs["matrix"])["include"]), 48)
            self.output.unlink()
            standalone.append({"kind": "unit", "group": "another"})
            with self.assertRaisesRegex(ValueError, "selected 49 lanes; at most 48"):
                self.invoke("--candidate", ".")
            self.assertFalse(self.output.exists())
