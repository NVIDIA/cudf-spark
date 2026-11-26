# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the GHA orchestration boundary without a cluster, credentials or network."""

import argparse
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).resolve().parents[3] / ".github/workflows/databricks-ci/attempts.py"
spec = importlib.util.spec_from_file_location("gha_db_attempts", SCRIPT)
attempts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(attempts)


class GhaAttemptsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.args = argparse.Namespace(operation="run", runtime="17.3", mode="CI_PART2", reduced=False,
                                      state_dir=self.root / "row", candidate=self.root / "candidate",
                                      merge_sha="a" * 40)
        self.adapter = Mock()
        self.adapter.attempt_env.side_effect = lambda *args: {"DATABRICKS_HOST": "synthetic"}
        self.adapter.cleanup_env.return_value = {"DATABRICKS_HOST": "synthetic"}
        self.adapter.run_workload.return_value = 0
        self.start = []
        self.call = []
        self.cleanup_status = (0, True, True)
        self.classification = (1, False, False, "NOT_A_CLASSIFIED_SPARK_STARTUP_FAILURE")
        self.plan = []
        self.change_id = False
        self.enter(patch.dict(attempts.os.environ, {"GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "1",
                                                   "RUNNER_TEMP": str(self.root)}, clear=True))
        self.enter(patch.object(attempts, "lifecycle", side_effect=self.operation))
        self.configure = self.enter(patch.object(attempts.subprocess, "run", return_value=Mock(
            returncode=0, stdout=b'{"cluster_spec":{"custom_tags":{"Existing":"keep"}}}')))
        self.enter(contextlib.redirect_stdout(io.StringIO()))

    def enter(self, context):
        result = context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        return result

    def test_config_only_ssh_preserves_legacy_default(self):
        module = importlib.util.spec_from_file_location("clusterutils", attempts.DB / "clusterutils.py")
        utils = importlib.util.module_from_spec(module)
        with patch.dict(sys.modules, {"requests": Mock()}):
            module.loader.exec_module(utils)
        opts = utils.ClusterUtils.get_ssh_options
        self.assertEqual(opts("key")[-2:], ["-p", "2200"])
        cfg = str(self.root / "ssh_config")
        with patch.dict(attempts.os.environ, DATABRICKS_SSH_CONFIG=cfg):
            self.assertIn("StrictHostKeyChecking=no", opts("key"))
            with patch.dict(attempts.os.environ, DATABRICKS_SSH_CONFIG_ONLY="1"):
                with self.assertRaises(ValueError):
                    opts("key")
                Path(cfg).touch()
                self.assertEqual(opts("key"), ["-F", cfg, "-i", "key"])

    def operation(self, op, path, env):
        self.call.append(op)
        if op == "prepare":
            self.start.append(path.parent.name)
            rc, phase, cid = self.plan.pop(0) if self.plan else (0, "READY", "original")
            state = dict(schema=1, phase=phase, cluster_id=cid, driver="synthetic.invalid",
                         reason="TEST_STATE", retryable=rc == 75,
                         **{k: env[v] for k, v in attempts.IDENTITY.items()})
        else:
            state = json.loads(path.read_text())
            rc = 0
            if op == "classify-startup":
                rc, state["classification_available"], state["retryable"], state["reason"] = self.classification
                if self.change_id:
                    state["cluster_id"] = "another"
            if op == "cleanup":
                rc, state["termination_confirmed"], state["cleanup_complete"] = self.cleanup_status
        path.write_text(json.dumps(state))
        return rc

    def run_row(self):
        attempts.run_row(self.args, self.adapter)

    def test_success_then_post_cleanup(self):
        self.run_row()
        self.assertEqual(self.call, ["prepare", "install-readiness", "verify-readiness", "cleanup"])
        build, test = self.adapter.run_workload.call_args_list
        self.assertEqual(build.args[0], "build")
        self.assertEqual(test.args[0], "test")
        self.assertEqual(test.args[1]["DATABRICKS_CLUSTER_ID"], "original")
        self.assertNotIn("SPARK_STARTUP_CHECK", build.args[1].get("EXTRA_ENVS", ""))
        self.assertIn("SPARK_STARTUP_CHECK=", test.args[1]["EXTRA_ENVS"])
        self.assertFalse((self.args.state_dir / "work").exists())
        spec = json.loads((self.args.state_dir / "aws2-1/config.json").read_text())["cluster_spec"]
        self.assertEqual(spec["cluster_name"], "gha-42-1-17.3-CI_PART2-aws2-1")
        self.assertEqual(spec["custom_tags"], {"Existing": "keep", "CI_MANAGED_BY": "cudf-spark-databricks-ci",
                         "CI_REPOSITORY": "NVIDIA/cudf-spark", "CI_WORKFLOW": ".github/workflows/databricks-ci.yml",
                         "CI_PROFILE": "aws2", "CI_BUILD_TAG": spec["cluster_name"]})
        self.args.operation = "cleanup"
        self.run_row()
        self.run_row()
        self.assertEqual(self.start, ["aws2-1"])
        self.adapter.cleanup_env.assert_called_once_with("aws2")
        self.assertFalse((self.args.state_dir / "aws2-1").exists())
        self.assertTrue((self.args.state_dir / "cleanup-complete").is_file())

    def test_preparation_capacity_is_bounded_and_ordered(self):
        self.plan = [(75, "START", "allocated")] * 6
        with self.assertRaisesRegex(attempts.Failure, "attempts exhausted"):
            self.run_row()
        self.assertEqual(self.start, ["aws2-1", "aws2-2", "aws-1", "aws-2", "azure3-1", "azure3-2"])
        self.assertEqual(self.call, ["prepare", "cleanup"] * 6)
        self.adapter.run_workload.assert_not_called()

    def test_ambiguous_create_is_never_retried_or_deleted(self):
        self.plan = [(75, "CREATE", None)]
        with self.assertRaisesRegex(attempts.Failure, "No confirmed cluster ID"):
            self.run_row()
        self.assertEqual(self.call, ["prepare"])
        (self.args.state_dir / "aws2-1/state.json").unlink()
        self.args.operation = "cleanup"
        with self.assertRaisesRegex(attempts.Failure, "Missing or unreadable cluster state"):
            self.run_row()
        self.assertFalse((self.args.state_dir / "cleanup-complete").exists())

    def test_unconfirmed_cleanup_blocks_replacement(self):
        self.plan = [(75, "SSH", "allocated")]
        self.cleanup_status = (0, False, True)
        with self.assertRaisesRegex(attempts.Failure, "prepare: exit=75.*cleanup unconfirmed"):
            self.run_row()
        self.assertEqual(self.call, ["prepare", "cleanup"])

    def test_build_is_never_classified(self):
        self.adapter.run_workload.return_value = 75
        with self.assertRaisesRegex(attempts.Failure, "Build failed"):
            self.run_row()
        self.assertEqual(self.call, ["prepare", "cleanup"])

    def test_ordinary_test_failure_does_not_repeat(self):
        self.adapter.run_workload.side_effect = [0, 75]
        with self.assertRaisesRegex(attempts.Failure, "Test failed .*exit=75"):
            self.run_row()
        self.assertEqual(self.call, ["prepare", "install-readiness", "classify-startup", "cleanup"])

    def test_interrupted_test_has_no_classification_or_replacement(self):
        self.adapter.run_workload.side_effect = [0, 143]
        with self.assertRaisesRegex(attempts.Failure, "interrupted"):
            self.run_row()
        self.assertEqual(self.call, ["prepare", "install-readiness", "cleanup"])

    def test_foreign_classification_never_deletes_or_replaces(self):
        self.adapter.run_workload.side_effect = [0, 1]
        self.change_id = True
        with self.assertRaisesRegex(attempts.Failure, "cluster ID changed"):
            self.run_row()
        self.args.operation = "cleanup"
        with self.assertRaisesRegex(attempts.Failure, "cluster ID changed"):
            self.run_row()
        self.assertEqual(self.call, ["prepare", "install-readiness", "classify-startup"])

    def test_only_provider_corroborated_test_startup_can_retry(self):
        self.classification = (75, True, True, "CLOUD_PROVIDER_RESOURCE_STOCKOUT")
        self.adapter.run_workload.side_effect = [0, 1, 0, 0]
        self.run_row()
        self.assertEqual(self.start, ["aws2-1", "aws2-2"])
        self.assertEqual(self.call[2:5], ["classify-startup", "cleanup", "prepare"])

    def test_marker_on_candidate_driver_is_not_authority_for_retry(self):
        self.classification = (75, True, True, "SPARK_STARTUP_DISK_FULL")
        self.adapter.run_workload.side_effect = [0, 1]
        with self.assertRaisesRegex(attempts.Failure, "SPARK_STARTUP_DISK_FULL"):
            self.run_row()
        self.assertEqual(self.call, ["prepare", "install-readiness", "classify-startup", "cleanup"])

    def test_post_refuses_to_forget_a_known_id_if_state_is_lost(self):
        self.run_row()
        (self.args.state_dir / "aws2-1/state.json").unlink()
        self.args.operation = "cleanup"
        with self.assertRaisesRegex(attempts.Failure, "Missing or unreadable cluster state"):
            self.run_row()
        self.assertTrue(self.args.state_dir.exists())

    def test_post_rejects_other_run_before_using_profile(self):
        self.run_row()
        self.args.operation = "cleanup"
        with patch.dict(attempts.os.environ, {"GITHUB_RUN_ATTEMPT": "2"}):
            with self.assertRaisesRegex(attempts.Failure, "ownership"):
                self.run_row()
        self.adapter.cleanup_env.assert_not_called()

    def test_post_cannot_confirm_cleanup_if_runner_lost_attempt_directory(self):
        self.run_row()
        attempts.shutil.rmtree(self.args.state_dir)
        self.args.operation = "cleanup"
        with self.assertRaisesRegex(attempts.Failure, "ownership"):
            self.run_row()
        self.adapter.cleanup_env.assert_not_called()


class LifecycleContractTest(unittest.TestCase):
    def test_real_cli_state_classifier_and_cleanup(self):
        # Exercise the checked-in CLI and argument parser. Only its network/SSH boundaries are fake.
        spec = importlib.util.spec_from_file_location("db_lifecycle", attempts.DB / "cluster_fallback.py")
        cli = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"requests": Mock(), "clusterutils": Mock()}):
            spec.loader.exec_module(cli)
        env = {v: "offline-" + k for k, v in attempts.IDENTITY.items()}
        marker = json.dumps({"runtime": "databricks", "phase": "FAILED", "reason": "UNSPECIFIED"})
        calls = []

        def api(method, endpoint, **kwargs):
            calls.append((method, endpoint))
            if endpoint == "create":
                self.assertEqual(kwargs["json"]["custom_tags"]["CI_BUILD_TAG"], env["BUILD_TAG"])
                return {"cluster_id": "fixture-id"}
            self.assertEqual((kwargs.get("params") or kwargs["json"])["cluster_id"], "fixture-id")
            return {"state": "TERMINATED", "termination_reason": {"code": "CLOUD_PROVIDER_RESOURCE_STOCKOUT"}}

        def fake_transport(argv, **kwargs):
            self.assertEqual(Path(argv[2]), attempts.DB / "cluster_fallback.py")
            with patch.object(sys, "argv", argv[2:]), patch.dict(attempts.os.environ, kwargs["env"], clear=True):
                return SimpleNamespace(returncode=cli.main())

        with tempfile.TemporaryDirectory() as directory, contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(cli, "request", side_effect=api))
            stack.enter_context(patch.object(cli, "wait_for_driver", return_value="driver.invalid"))
            stack.enter_context(patch.object(cli, "wait_for_ssh"))
            stack.enter_context(patch.object(cli, "readiness_command", side_effect=[
                None, SimpleNamespace(stdout=marker), SimpleNamespace(stdout="")]))
            stack.enter_context(patch.object(attempts.subprocess, "run", side_effect=fake_transport))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            path = Path(directory) / "state.json"
            path.with_name("config.json").write_text(json.dumps(
                {"cluster_spec": {"num_workers": 0}, "readiness_modes": ["databricks"]}))
            with self.assertRaisesRegex(attempts.Retryable, "CLOUD_PROVIDER_RESOURCE_STOCKOUT"):
                attempts.run_attempt(path, env, lambda phase, _env, _cid: int(phase == "test"))
            self.assertEqual(calls, [("POST", "create"), ("GET", "get"), ("POST", "delete"),
                                     ("GET", "get"), ("POST", "permanent-delete")])
            state = json.loads(path.read_text())
            self.assertIs(state["cleanup_complete"], True)
            self.assertIs(state["termination_confirmed"], True)
