# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise the GHA orchestration boundary without a cluster, credentials or network."""

import argparse
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).resolve().parents[3] / ".github/workflows/databricks-ci/attempts.py"
spec = importlib.util.spec_from_file_location("gha_db_attempts", SCRIPT)
attempts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(attempts)


class Workspace:
    """Independent API-side records: the controller cannot change these via its local state."""

    def __init__(self):
        self.calls = []
        self.records = {}
        self.specs = []
        self.startups = []
        self.creator = "fixture-principal"

    def api(self, method, path, **data):
        op = path.rsplit("/", 1)[1]
        self.calls.append((method, op))
        if op == "Me":
            return {"userName": self.creator}
        if op == "create":
            self.specs.append(data)
            cid = f"fixture-{len(self.specs)}"
            self.records[cid] = {**data, "cluster_id": cid, "creator_user_name": self.creator,
                                 **(self.startups.pop(0) if self.startups else {"state": "RUNNING"})}
            return {"cluster_id": cid}
        cid = data["cluster_id"]
        if op == "get":
            if cid not in self.records:
                raise attempts.https.MissingCluster("GET: fixture no longer exists")
            return self.records[cid].copy()
        if op == "delete":
            self.records[cid]["state"] = "TERMINATED"
        elif op == "permanent-delete":
            if self.records[cid]["state"] != "TERMINATED":
                raise AssertionError("permanent-delete on a nonterminal cluster")
            del self.records[cid]
        else:
            raise AssertionError(op)
        return {}

    def writes(self):
        return [op for method, op in self.calls if method == "POST"]


class GhaAttemptsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.args = argparse.Namespace(operation="run", runtime="17.3", mode="CI_PART2", reduced=False,
                                      state_dir=self.root / "row", candidate=self.root / "candidate",
                                      merge_sha="a" * 40)
        self.adapter = Mock()
        self.discover = self.enter(patch.object(attempts, "discover", return_value={"17.3": "4.0.0"}))
        self.api = Workspace()
        self.adapter.creator_name.return_value = self.api.creator
        self.env = dict(DATABRICKS_HOST="synthetic", DATABRICKS_TOKEN="offline", DATABRICKS_CLOUD="aws",
                        DATABRICKS_DRIVER="driver", DATABRICKS_WORKER="worker", DATABRICKS_RUNTIME="runtime")
        self.adapter.attempt_env.side_effect = lambda *args: {**self.env}
        self.adapter.cleanup_env.return_value = {**self.env}
        self.adapter.run_workload.return_value = 0
        self.adapter.source_manifest.return_value = {"sha": self.args.merge_sha, "pins": {}}
        self.enter(patch.dict(attempts.os.environ, {"GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "1",
                       "GITHUB_REPOSITORY": attempts.REPO, "RUNNER_TEMP": str(self.root)}, clear=True))
        self.enter(patch.dict(sys.modules, {"requests": Mock()}))  # Legacy template imports requests.
        self.client = self.enter(patch.object(attempts.https, "workspace_api", return_value=self.api.api))
        self.enter(patch.object(attempts.https.time, "sleep"))
        self.enter(contextlib.redirect_stdout(io.StringIO()))

    def enter(self, context):
        result = context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        return result

    def run_row(self, **scope):
        attempts.run_row(self.args, self.adapter, **scope)

    def post(self, **scope):
        self.args.operation = "cleanup"
        self.run_row(**scope)

    def test_manual_ssh_default_and_optional_config_unchanged(self):
        module = importlib.util.spec_from_file_location("clusterutils", attempts.DB / "clusterutils.py")
        utils = importlib.util.module_from_spec(module)
        with patch.dict(sys.modules, {"requests": Mock()}):
            module.loader.exec_module(utils)
        opts = utils.ClusterUtils.get_ssh_options
        self.assertEqual(opts("key")[-2:], ["-p", "2200"])
        cfg = str(self.root / "ssh_config")
        with patch.dict(attempts.os.environ, DATABRICKS_SSH_CONFIG=cfg):
            self.assertIn("StrictHostKeyChecking=no", opts("key"))
            self.assertEqual(opts("key")[-2:], ["-F", cfg])

    def test_success_then_post_cleanup(self):
        self.run_row()
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])
        self.assertEqual(self.api.calls[-1], ("GET", "get"))
        self.adapter.source_manifest.assert_called_once_with(self.args.candidate.resolve(), self.args.merge_sha)
        build, test = self.adapter.run_workload.call_args_list
        self.assertEqual([build.args[0], test.args[0]], ["build", "test"])
        for call in (build, test):
            self.assertEqual(call.args[1]["DATABRICKS_CLUSTER_ID"], "fixture-1")
            self.assertNotIn("DATABRICKS_DRIVER_ADDRESS", call.args[1])
            self.assertIs(call.args[-1], self.adapter.source_manifest.return_value)
        config, = self.api.specs
        name = "gha-42-1-17.3-CI_PART2-aws2-1"
        self.assertEqual(config["cluster_name"], name)
        self.assertEqual(config["custom_tags"], {**attempts.managed_tags("aws2", name), "ResourceClass": "SingleNode"})
        self.assertNotIn("ssh_public_keys", config)
        self.assertEqual(config["autotermination_minutes"], 390)
        self.assertEqual((config["data_security_mode"], config["single_user_name"]), ("SINGLE_USER", self.api.creator))
        self.assertEqual((config["driver_node_type_id"], config["node_type_id"]), ("driver", "worker"))
        self.assertEqual(config["aws_attributes"]["availability"], "SPOT_WITH_FALLBACK")
        self.assertFalse((self.args.state_dir / "work").exists())
        self.post()
        self.post()
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])
        self.assertTrue((self.args.state_dir / "cleanup-complete").is_file())

    def test_validate_inputs_and_identity_before_create(self):
        self.args.runtime = "19.10"
        with self.assertRaisesRegex(attempts.Failure, "not enabled"):
            self.run_row()
        self.adapter.source_manifest.assert_not_called()
        self.args.runtime = "17.3"
        self.adapter.creator_name.return_value = "wrong-principal"
        with self.assertRaisesRegex(attempts.Failure, "principal"):
            self.run_row()
        self.assertEqual(self.api.writes(), [])

    def test_capacity_bounded_and_ordered(self):
        self.api.startups = [{"state": "TERMINATED", "termination_reason": {
            "code": "CLOUD_PROVIDER_RESOURCE_STOCKOUT", "type": "CLIENT_ERROR"}}] * 6
        with self.assertRaisesRegex(attempts.Failure, "exhausted.*STOCKOUT"):
            self.run_row()
        self.assertEqual([s["cluster_name"].split("PART2-")[1] for s in self.api.specs],
                         ["aws2-1", "aws2-2", "aws-1", "aws-2", "azure3-1", "azure3-2"])
        self.assertEqual(self.api.writes(), ["create", "permanent-delete"] * 6)
        self.adapter.run_workload.assert_not_called()

    def test_unexpected_compute_is_cleaned_without_running_approved_source(self):
        self.api.startups = [{"state": "RUNNING", "data_security_mode": "NONE"}]
        with self.assertRaisesRegex(attempts.Failure, "dedicated single-node"):
            self.run_row()
        self.adapter.run_workload.assert_not_called()
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])

    def test_lost_workload_response_does_not_reexecute(self):
        self.adapter.run_workload.side_effect = [0, attempts.https.TransientRequest("GET command status timed out")]
        with self.assertRaisesRegex(attempts.https.TransientRequest, "command status timed out"):
            self.run_row()
        self.assertEqual(len(self.adapter.run_workload.call_args_list), 2)
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])

    def test_lost_create_response_no_second_allocation(self):
        def ambiguous(method, path, **data):
            result = self.api.api(method, path, **data)
            if path.endswith("/create"):
                raise attempts.https.TransientRequest("POST create timed out")
            return result
        self.client.return_value = ambiguous
        with self.assertRaisesRegex(attempts.Failure, "create timed out.*No confirmed cluster ID"):
            self.run_row()
        with self.assertRaisesRegex(attempts.Failure, "No confirmed cluster ID"):
            self.post()
        self.assertEqual(self.api.writes(), ["create"])

    def test_foreign_cluster_record_blocks_cleanup_and_retry(self):
        def foreign(phase, env, cid, *args):
            if phase == "test":
                self.api.records[cid]["custom_tags"]["CI_PROFILE"] = "different"
                return 75
            return 0
        self.adapter.run_workload.side_effect = foreign
        with self.assertRaisesRegex(attempts.Failure, "foreign cluster identity"):
            self.run_row()
        with self.assertRaisesRegex(attempts.Failure, "foreign cluster identity"):
            self.post()
        self.assertEqual(self.api.writes(), ["create"])

    def test_post_reconciles_ambiguous_purge_without_replay(self):
        def interrupted(method, path, **data):
            result = self.api.api(method, path, **data)
            if path.endswith("/permanent-delete"):
                raise attempts.https.TransientRequest("response lost")
            return result
        self.client.return_value = interrupted
        self.adapter.run_workload.return_value = 12
        with self.assertRaisesRegex(attempts.Failure, "Build failed.*12.*cleanup: response lost"):
            self.run_row()
        self.post()
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])

    def test_api_denial_is_not_missing_and_cannot_authorize_replacement(self):
        def denied(method, path, **data):
            if path.endswith("/get") and not self.api.records:
                raise RuntimeError("GET cluster: HTTP 404 (unclassified)")
            return self.api.api(method, path, **data)
        self.client.return_value = denied
        self.api.startups = [{"state": "TERMINATED", "termination_reason": {
            "code": "CLOUD_PROVIDER_RESOURCE_STOCKOUT"}}]
        with self.assertRaisesRegex(attempts.Failure, "STOCKOUT.*HTTP 404"):
            self.run_row()
        self.assertEqual(self.api.writes(), ["create", "permanent-delete"])

    def test_accepted_purge_requires_complete_inventory_on_invalid_get(self):
        target = {"cluster_id": "fixture-1", "state": "TERMINATED"}
        other = {"cluster_id": "other"}
        listing = Mock(side_effect=[
            {"clusters": [other], "next_page_token": "next"}, {"clusters": [target]},
            {"clusters": [other], "next_page_token": "next"}, {},
        ])

        def workspace(method, path, **fields):
            if path.endswith("/get") and not self.api.records:
                raise attempts.https.InvalidClusterParameter("GET: HTTP 400 INVALID_PARAMETER_VALUE")
            if path.endswith("/list"):
                self.assertEqual(method, "GET")
                return listing(fields)
            return self.api.api(method, path, **fields)
        self.client.return_value = workspace
        self.run_row()
        self.assertEqual([c.args[0].get("page_token") for c in listing.call_args_list],
                         [None, "next", None, "next"])
        state = json.loads((self.args.state_dir / "aws2-1/state.json").read_text())
        self.assertTrue(state["purge_accepted"] and state["cleanup_complete"])
        self.post()
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])

    def test_unconfirmed_inventory_can_be_retried_without_repeating_purge(self):
        listing = Mock(side_effect=[RuntimeError("list denied"), {"clusters": []}])

        def workspace(method, path, **fields):
            if path.endswith("/get") and not self.api.records:
                raise attempts.https.InvalidClusterParameter("GET: HTTP 400 INVALID_PARAMETER_VALUE")
            if path.endswith("/list"):
                return listing()
            return self.api.api(method, path, **fields)
        self.client.return_value = workspace
        with self.assertRaisesRegex(attempts.Failure, "list denied"):
            self.run_row()
        self.post()
        self.assertEqual(listing.call_count, 2)
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])

    def test_invalid_get_after_lost_purge_response_cannot_use_list(self):
        def ambiguous(method, path, **fields):
            if path.endswith("/get") and not self.api.records:
                raise attempts.https.InvalidClusterParameter("GET: HTTP 400 INVALID_PARAMETER_VALUE")
            result = self.api.api(method, path, **fields)
            if path.endswith("/permanent-delete"):
                raise attempts.https.TransientRequest("purge response lost")
            return result
        self.client.return_value = ambiguous
        with self.assertRaisesRegex(attempts.Failure, "purge response lost"):
            self.run_row()
        with self.assertRaisesRegex(attempts.https.InvalidClusterParameter, "INVALID_PARAMETER_VALUE"):
            self.post()
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])
        self.assertFalse((self.args.state_dir / "cleanup-complete").exists())

    def test_invalid_or_incomplete_list_never_proves_absence(self):
        for page in ({"clusters": None}, {"clusters": [{}]}, {"message": "bad response"},
                     {"clusters": [{"cluster_id": " "}]}, {"clusters": [], "has_more": True},
                     {"clusters": [{"cluster_id": "fixture-1", "state": "RUNNING"}]},
                     {"next_page_token": "repeated"}, {"next_page_token": None}):
            with self.subTest(page=page), self.assertRaises(attempts.Failure):
                attempts.absent_from_inventory(lambda _: page, "fixture-1", float("inf"))
        listing = Mock(return_value={})
        with patch.object(attempts.time, "monotonic", side_effect=[1, 3]), self.assertRaisesRegex(
                attempts.Failure, "time or page limit"):
            attempts.absent_from_inventory(listing, "fixture-1", 2)
        listing.assert_called_once()

    def test_invalid_get_before_purge_does_not_trigger_listing_or_deletion(self):
        def workspace(method, path, **fields):
            if path.endswith("/get"):
                raise attempts.https.InvalidClusterParameter("GET: HTTP 400 INVALID_PARAMETER_VALUE")
            return self.api.api(method, path, **fields)
        self.client.return_value = workspace
        with self.assertRaisesRegex(attempts.Failure, "cleanup: GET: HTTP 400 INVALID_PARAMETER_VALUE"):
            self.run_row()
        self.assertEqual(self.api.writes(), ["create"])

    def test_malformed_purge_acknowledgement_does_not_authorize_inventory(self):
        def workspace(method, path, **fields):
            if path.endswith("/get") and not self.api.records:
                raise attempts.https.InvalidClusterParameter("GET: HTTP 400 INVALID_PARAMETER_VALUE")
            result = self.api.api(method, path, **fields)
            return {"error_code": "INVALID_PARAMETER_VALUE"} if path.endswith("/permanent-delete") else result
        self.client.return_value = workspace
        with self.assertRaisesRegex(attempts.Failure, "purge acceptance not confirmed"):
            self.run_row()
        with self.assertRaises(attempts.https.InvalidClusterParameter):
            self.post()
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])

    def test_ordinary_test_failure_and_cancellation_do_not_reallocate(self):
        self.adapter.run_workload.side_effect = [0, 76]
        with self.assertRaisesRegex(attempts.Failure, "Test failed .*76"):
            self.run_row()
        self.assertEqual(self.api.writes(), ["create", "delete", "permanent-delete"])
        self.post()
        self.args.operation = "run"
        self.args.state_dir = self.root / "next"
        def cancel(phase, env, cid, *args):
            if phase == "test":
                self.api.records[cid].update(state="TERMINATED", termination_reason={
                    "code": "CLOUD_PROVIDER_RESOURCE_STOCKOUT"})
                return 143
            return 0
        self.adapter.run_workload.side_effect = cancel
        with self.assertRaisesRegex(attempts.Failure, "Test failed .*143"):
            self.run_row()
        self.assertEqual(len(self.api.specs), 2)

    def test_only_provider_corroborated_failure_can_retry(self):
        def fail(phase, env, cid, *args):
            if phase == "test" and cid == "fixture-1":
                self.api.records[cid].update(state="TERMINATED", termination_reason={
                    "code": "CLOUD_PROVIDER_RESOURCE_STOCKOUT", "type": "CLIENT_ERROR"})
                return 1
            return 0
        self.adapter.run_workload.side_effect = fail
        self.run_row()
        self.assertEqual(self.api.writes(), ["create", "permanent-delete", "create", "delete", "permanent-delete"])
        self.assertIsNone(attempts.confirmed_infra({"state": "RUNNING", "termination_reason": {
            "code": "CLOUD_PROVIDER_RESOURCE_STOCKOUT"}}))
        self.assertIsNone(attempts.confirmed_infra({"state": "TERMINATED", "termination_reason": {
            "code": "BOOTSTRAP_TIMEOUT", "type": "CLIENT_ERROR"}}))

    def test_post_rejects_lost_or_changed_identity(self):
        self.run_row()
        with patch.dict(attempts.os.environ, GITHUB_RUN_ATTEMPT="2"):
            with self.assertRaisesRegex(attempts.Failure, "ownership"):
                self.post()
        path = self.args.state_dir / "aws2-1/state.json"
        state = json.loads(path.read_text())
        path.write_text(json.dumps({**state, "cluster_id": "other"}))
        with self.assertRaisesRegex(attempts.Failure, "ID changed"):
            self.post()
        path.unlink()
        with self.assertRaisesRegex(attempts.Failure, "Missing or unreadable cluster state"):
            self.post()
        self.assertFalse((self.args.state_dir / "cleanup-complete").exists())

    def test_fork_single_allocation_cannot_use_production_tags(self):
        scope = dict(profiles=("aws2",), attempts_per_profile=1, repository="example/cudf-spark",
                     workflow=".github/workflows/fork.yml")
        with self.assertRaisesRegex(attempts.Failure, "scope mismatch"):
            self.run_row(**scope)
        self.api.startups = [{"state": "TERMINATED", "termination_reason": {"code": "CLOUD_PROVIDER_RESOURCE_STOCKOUT"}}]
        with patch.dict(attempts.os.environ, GITHUB_REPOSITORY=scope["repository"],
                        GITHUB_WORKFLOW_REF=scope["repository"] + "/" + scope["workflow"] + "@refs/heads/pilot"):
            with self.assertRaisesRegex(attempts.Failure, "exhausted"):
                self.run_row(**scope)
            self.post(**scope)
        self.assertEqual(len(self.api.specs), 1)
        self.assertEqual(self.api.specs[0]["autotermination_minutes"], 10)
        self.assertTrue(self.api.specs[0]["cluster_name"].startswith("fork-gha-"))
        self.assertEqual(self.api.specs[0]["custom_tags"]["CI_MANAGED_BY"], attempts.MANAGER + "-fork")
