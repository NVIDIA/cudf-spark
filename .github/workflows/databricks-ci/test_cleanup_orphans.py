# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, call, patch


spec = importlib.util.spec_from_file_location("sweeper", Path(__file__).with_name("cleanup-orphans.py"))
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)


def cluster(profile="aws2", runtime="17.3", **overrides):
    name = f"gha-42-1-{runtime}-CI_PART2-{profile}-1"
    return dict({"cluster_id": "offline-1", "cluster_name": name, "creator_user_name": "ci",
                 "custom_tags": s.managed_tags(profile, name), "state": "RUNNING"}, **overrides)


def run(**overrides):
    repo = {"full_name": "NVIDIA/cudf-spark"}
    return dict({"id": 42, "run_attempt": 1, "workflow_id": 87, "path": s.WORKFLOW, "head_branch": "main",
                 "repository": repo, "head_repository": repo, "event": "workflow_run",
                 "status": "completed", "conclusion": "cancelled"}, **overrides)


class CleanupOrphansTest(unittest.TestCase):
    def setUp(self):
        self.wf = {"id": 87, "path": s.WORKFLOW}
        self.gh = Mock(side_effect=[self.wf, run()])
        self.sleep = patch.object(s.time, "sleep").start()
        self.addCleanup(patch.stopall)
        stderr = contextlib.redirect_stderr(io.StringIO())
        stderr.__enter__()
        self.addCleanup(stderr.__exit__, None, None, None)

    def test_two_pages_dedup_terminate_wait_reverify_and_confirm_absent(self):
        info = cluster()
        db = Mock(side_effect=[{"clusters": [info, cluster(profile="aws")], "next_page_token": "next"},
                               {"clusters": [info, {}, cluster(creator_user_name="unrelated")]},
                               info, {}, cluster(state="TERMINATING"), cluster(state="TERMINATED"), {},
                               cluster(state="TERMINATED"), None])
        self.assertEqual(s.sweep(db, self.gh, "aws2", "ci"), (1, 1))
        self.assertEqual(self.gh.call_args_list, [call("actions/workflows/databricks-ci.yml"),
                                                  call("actions/runs/42/attempts/1")])
        resource = {"cluster_id": info["cluster_id"]}
        self.assertEqual(db.call_args_list[:2], [call("GET", "list", {"page_size": 100}),
                                                 call("GET", "list", {"page_size": 100, "page_token": "next"})])
        self.assertEqual([(c.args[0], c.args[1]) for c in db.call_args_list[2:]],
                         [("GET", "get"), ("POST", "delete"), ("GET", "get"), ("GET", "get"),
                          ("POST", "permanent-delete"), ("GET", "get"), ("GET", "get")])
        self.assertTrue(all(c.args[2] == resource for c in db.call_args_list[2:]))

    def test_never_touch_active_or_foreign_clusters(self):
        db = Mock(return_value={"clusters": [cluster(), cluster(profile="azure3"), {},
                                             cluster(creator_user_name="someone-else")]})
        self.gh.side_effect = [self.wf, run(status="in_progress", conclusion=None)]
        self.assertEqual(s.sweep(db, self.gh, "aws2", "ci"), (1, 0))
        db.assert_called_once_with("GET", "list", {"page_size": 100})

    def test_ambiguous_get_needs_accepted_purge_and_complete_inventory(self):
        rejected = s.InvalidClusterLookup("HTTP 400 INVALID_PARAMETER_VALUE")
        info = cluster(state="TERMINATED")
        owner = s.ownership(info, "aws2", "ci")
        def remove(api):
            return s.remove(api, "aws2", "ci", info["cluster_id"], owner)

        early = Mock(side_effect=[rejected])
        with self.assertRaises(s.InvalidClusterLookup):
            remove(early)
        self.assertEqual(early.call_count, 1)
        db = Mock(side_effect=[info, {}, rejected, {"next_page_token": "next"},
                               {"clusters": [{"cluster_id": "other"}]}])
        self.assertTrue(remove(db))
        self.assertEqual([c.args[1] for c in db.call_args_list], ["get", "permanent-delete", "get", "list", "list"])
        self.assertEqual(db.call_args.args[2]["page_token"], "next")
        for page in ({"clusters": [cluster()]}, {"clusters": [{}]}):
            with self.subTest(page=page), self.assertRaises(s.SweepError):
                remove(Mock(side_effect=[info, {}, rejected, page]))

    def test_omitted_creator_requires_independent_workspace_get_even_for_active_attempt(self):
        listed = cluster(creator_user_name=None)
        for detail, fails in [(cluster(creator_user_name=None), True), (cluster(cluster_id="different"), True),
                              (cluster(creator_user_name="another"), False), (None, False)]:
            with self.subTest(detail=detail):
                db = Mock(side_effect=[{"clusters": [listed]}, detail])
                if fails:
                    with self.assertRaises(s.SweepError):
                        s.sweep(db, self.gh, "aws2", "ci")
                else:
                    self.assertEqual(s.sweep(db, self.gh, "aws2", "ci"), (0, 0))
                self.gh.assert_not_called()

    def test_unavailable_github_workflow_or_attempt_never_authorizes_deletion(self):
        for responses in ([{"path": "elsewhere", "id": 87}], [self.wf, s.SweepError("GitHub unavailable")]):
            db = Mock(return_value={"clusters": [cluster()]})
            with self.assertRaises(s.SweepError):
                s.sweep(db, Mock(side_effect=responses), "aws2", "ci")
            self.assertEqual(db.call_count, 1)

    def test_exact_historical_attempt_required(self):
        for change in ({"id": 43}, {"run_attempt": 2}, {"run_attempt": True}, {"workflow_id": 99},
                       {"event": "workflow_dispatch"}, {"head_branch": "elsewhere"}, {"path": "elsewhere"},
                       {"repository": {"full_name": "other/repo"}}, {"head_repository": None},
                       {"status": "in_progress"}, {"conclusion": None}):
            with self.subTest(change=change):
                db = Mock(return_value={"clusters": [cluster()]})
                with self.assertRaises(s.SweepError):
                    s.sweep(db, Mock(side_effect=[self.wf, run(**change)]), "aws2", "ci")
                self.assertEqual(db.call_count, 1)

    def test_incomplete_inventory_is_not_a_successful_sweep(self):
        for pages in ([{"clusters": [cluster()], "next_page_token": "x"}, {"next_page_token": "x"}],
                      [{"clusters": {}}], [{"clusters": [cluster(cluster_name="wrong-name")]}],
                      [{"clusters": [cluster(custom_tags={})]}],
                      [{"clusters": [cluster(custom_tags={"CI_MANAGED_BY": s.MANAGER})]}],
                      [{"clusters": [cluster(cluster_id="")]}],
                      [{"next_page_token": "x"}, s.SweepError("list unavailable")]):
            with self.subTest(pages=pages):
                db = Mock(side_effect=pages)
                with self.assertRaises(s.SweepError):
                    s.sweep(db, self.gh, "aws2", "ci")
                self.gh.assert_not_called()
                self.assertTrue(all(c.args[:2] == ("GET", "list") for c in db.call_args_list))

    def test_revalidate_creator_tags_name_and_id_before_each_mutation(self):
        for changed in (cluster(creator_user_name="another"), cluster(creator_user_name=None),
                        cluster(cluster_name="replacement"), cluster(custom_tags={}), cluster(cluster_id="replacement")):
            for after_terminate in (False, True):
                with self.subTest(changed=changed, after_terminate=after_terminate):
                    # A missing creator in the list is never an authorization to delete.
                    listed = cluster()
                    del listed["creator_user_name"]
                    db = Mock(side_effect=[{"clusters": [listed]}, cluster()] + ([cluster(), {}] if after_terminate else [])
                              + [dict(changed, state="TERMINATED")])
                    with self.assertRaises(s.SweepError):
                        s.sweep(db, Mock(side_effect=[self.wf, run()]), "aws2", "ci")
                    self.assertEqual([c.args[1] for c in db.call_args_list if c.args[0] == "POST"],
                                     ["delete"] if after_terminate else [])

    def test_retired_runtime_still_requires_exact_owner_and_terminal_run(self):
        info = cluster(runtime="14.3", state="TERMINATED")
        db = Mock(side_effect=[{"clusters": [info]}, info, {}, None])
        self.assertEqual(s.sweep(db, self.gh, "aws2", "ci"), (1, 1))
        for label in ("14.03", "14.3.x", "latest", "14.3/other"):
            with self.subTest(label=label), self.assertRaises(s.SweepError):
                s.ownership(cluster(runtime=label), "aws2", "ci")

    def test_404_idempotent_but_timeout_and_ambiguous_post_not_success(self):
        for remaining in ([None], [cluster(), None], [cluster(state="TERMINATED"), None]):
            db = Mock(side_effect=[{"clusters": [cluster()]}] + remaining)
            self.assertEqual(s.sweep(db, Mock(side_effect=[self.wf, run()]), "aws2", "ci"), (1, 0))
        for detail in (cluster(), cluster(state="TERMINATED")):
            db = Mock(side_effect=[{"clusters": [cluster()]}, detail, s.SweepError("POST timed out")])
            with self.assertRaises(s.SweepError):
                s.sweep(db, Mock(side_effect=[self.wf, run()]), "aws2", "ci")
            self.assertEqual(db.call_count, 3)
        with patch.object(s.time, "monotonic", side_effect=[0, 0, 1, 1, 181]):
            db = Mock(side_effect=[{"clusters": [cluster()]}, cluster(state="TERMINATING")])
            with self.assertRaises(s.SweepError):
                s.sweep(db, self.gh, "aws2", "ci")
            self.assertTrue(all(c.args[0] == "GET" for c in db.call_args_list))

    def test_transport_scopes_404_to_databricks_ids_and_refuses_redirects(self):
        requests = Mock(RequestException=RuntimeError)
        with patch.dict(sys.modules, requests=requests):
            api = s.transport("https://test.invalid/", "fixture")
            requests.request.return_value = Mock(status_code=400, json=lambda: {"error_code": "INVALID_PARAMETER_VALUE"})
            with self.assertRaises(s.InvalidClusterLookup):
                api("GET", "get", {"cluster_id": "offline-1"})
            for status, body, endpoint, missing in [(404, {}, "get", True),
                    (400, {"error_code": "RESOURCE_DOES_NOT_EXIST"}, "permanent-delete", True),
                    (400, {"error_code": "other"}, "delete", False), (403, {}, "delete", False),
                    (404, {}, "actions/runs/42/attempts/1", False), (404, {}, "list", False),
                    (302, {}, "get", False), (503, {}, "get", False)]:
                with self.subTest(status=status, endpoint=endpoint):
                    requests.request.return_value = Mock(status_code=status, content=b"{}", json=lambda: body)
                    if missing:
                        self.assertIsNone(api("GET", endpoint))
                    else:
                        with self.assertRaises(s.SweepError):
                            api("GET", endpoint)
                    self.assertFalse(requests.request.call_args.kwargs["allow_redirects"])

    def test_selected_profile_token_only_and_principal_required_before_network(self):
        env = {"DATABRICKS_TOKEN": "fixture", "GH_TOKEN": "fixture",
               "DATABRICKS_PROFILES_JSON": json.dumps({"aws2": {"host": "https://test.invalid/"}})}
        with (patch.object(sys, "argv", ["cleanup-orphans.py", "--profile", "aws2"]),
              patch.object(s, "transport", return_value=Mock(return_value={})) as transport):
            with patch.dict(s.os.environ, env, clear=True), self.assertRaisesRegex(ValueError, "principal"):
                s.main()
            transport.assert_not_called()
            env["DATABRICKS_PROFILES_JSON"] = json.dumps({"aws2": {"host": "https://test.invalid/",
                                                                    "creator_user_name": "ci"}})
            with patch.dict(s.os.environ, env, clear=True):
                s.main()
            transport.assert_any_call("https://test.invalid/api/2.1/clusters/", "fixture")
