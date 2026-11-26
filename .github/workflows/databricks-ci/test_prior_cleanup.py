# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("prior_cleanup", Path(__file__).with_name("prior-cleanup.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)

CELL = "Lane db-17.3-part1 / Databricks / Cell"
START = "2025-01-01T00:00:00Z"
END = "2025-01-01T01:00:00Z"
LATER = "2025-01-01T02:00:00Z"
WORKFLOW_RUNS = "actions/workflows/databricks-cleanup.yml/runs?branch=main&event=schedule&status=success&per_page=10"


def job(run=1, attempt=1, name=CELL, outcome="failure", steps=()):
    return {"id": 123, "name": name, "run_id": run, "run_attempt": attempt, "status": "completed",
            "conclusion": outcome, "completed_at": END, "steps": list(steps)}


def step(name, outcome):
    return {"name": name, "status": "completed", "conclusion": outcome, "started_at": LATER, "completed_at": LATER}


def listing(*rows):
    return {"total_count": len(rows), "jobs": list(rows)}


class PriorCleanupTest(unittest.TestCase):
    def setUp(self):
        ref = {"full_name": guard.REPO}
        self.current = {"id": 1, "run_attempt": 2, "status": "in_progress", "repository": ref,
                        "path": guard.WORKFLOW, "event": "workflow_run"}
        prior = dict(self.current, run_attempt=1, status="completed", conclusion="failure",
                     run_started_at=START, updated_at=END)
        self.sweep = {"id": 2, "run_attempt": 1, "event": "schedule", "head_branch": "main",
                      "path": guard.JANITOR, "repository": ref, "head_repository": ref,
                      "status": "completed", "conclusion": "success", "run_started_at": LATER}
        self.swept = [job(2, name=n, outcome="success", steps=[step(guard.RECONCILE, "success")])
                      for n in sorted(guard.PROFILES)]
        self.controller_jobs = "actions/runs/1/attempts/1/jobs?per_page=100"
        self.sweep_jobs = "actions/runs/2/attempts/1/jobs?per_page=100"
        self.data = {"actions/runs/1": self.current, self.controller_jobs: listing(job()),
                     "actions/runs/1/attempts/1": prior, WORKFLOW_RUNS: {"workflow_runs": [self.sweep]},
                     self.sweep_jobs: listing(*self.swept)}

    def verify(self, attempt=2):
        guard.verify(self.data.__getitem__, 1, attempt, "17.3", "CI_PART1")

    def test_first_or_no_allocation_or_confirmed_cleanup_needs_no_janitor(self):
        guard.verify(lambda _: self.fail("first attempt made an API call"), 1, 1, "17.3", "CI_PART1")
        del self.data[WORKFLOW_RUNS], self.data["actions/runs/1/attempts/1"]
        for row in (job(name="select"), job(outcome="success"), job(outcome="skipped"),
                    dict(job(outcome="cancelled"), started_at=None, runner_id=0),
                    job(steps=[step(guard.BUILD, "skipped")]),
                    job(steps=[step(guard.BUILD, "failure"), step(guard.CLEANUP, "success")])):
            with self.subTest(row=row):
                self.data[self.controller_jobs] = listing(row)
                self.verify()

    def test_missing_cell_after_unknown_or_successful_selection_blocks_rerun(self):
        sibling = job(name="Lane db-17.3-part2 / Databricks / Cell")
        for rows in (listing(job(name="result")), listing(job(name="select", outcome="success")),
                     listing(job(name="select", outcome="neutral")), listing(job(name="select", outcome=None)),
                     listing(job(name="select", outcome="success"), sibling)):
            with self.subTest(rows=rows), self.assertRaisesRegex(ValueError, "Missing Actions record"):
                self.data[self.controller_jobs] = rows
                self.verify()
        self.current["run_attempt"] = 3
        self.data[self.controller_jobs] = listing(job(outcome="success"))
        self.data["actions/runs/1/attempts/2/jobs?per_page=100"] = listing(
            job(attempt=2, name="select", outcome="success"))
        with self.assertRaisesRegex(ValueError, "attempt 2"):
            self.verify(3)

    def test_failed_only_retry_cannot_hide_an_earlier_unconfirmed_allocation(self):
        self.current["run_attempt"] = 3
        self.data["actions/runs/1/attempts/2/jobs?per_page=100"] = listing(job(attempt=2, name="result"))
        self.verify(3)
        self.sweep["run_started_at"] = END
        with self.assertRaisesRegex(ValueError, "Re-run failed jobs"):
            self.verify(3)

    def test_janitor_must_cover_the_latest_unconfirmed_attempt(self):
        self.current["run_attempt"] = 3
        self.data["actions/runs/1/attempts/2/jobs?per_page=100"] = listing(job(attempt=2))
        self.data["actions/runs/1/attempts/2"] = dict(self.data["actions/runs/1/attempts/1"],
                                                       run_attempt=2, updated_at=LATER)
        with self.assertRaisesRegex(ValueError, "Re-run failed jobs"):
            self.verify(3)

    def test_reject_skipped_wrong_or_incomplete_janitor(self):
        for rows in ([job(2, name="reconcile", outcome="skipped")], self.swept[:2],
                     self.swept[:2] + [self.swept[1]],
                     [*self.swept[:2], dict(self.swept[2], steps=[step(guard.RECONCILE, "skipped")])],
                     [*self.swept[:2], dict(self.swept[2], steps=[dict(step(guard.RECONCILE, "success"),
                                                                       started_at=START)])]):
            with self.subTest(rows=rows):
                self.data[self.sweep_jobs] = listing(*rows)
                with self.assertRaisesRegex(ValueError, "No new cluster"):
                    self.verify()
        self.data[self.sweep_jobs] = listing(*self.swept)
        for patch in ({"path": guard.WORKFLOW}, {"head_branch": "release/x"}, {"event": "workflow_dispatch"},
                      {"conclusion": "cancelled"}, {"repository": {"full_name": "other/repo"}}):
            with self.subTest(patch=patch):
                self.data[WORKFLOW_RUNS]["workflow_runs"] = [dict(self.sweep, **patch)]
                with self.assertRaisesRegex(ValueError, "No new cluster"):
                    self.verify()

    def test_reject_ambiguous_history_and_invalid_dates(self):
        for rows in (listing(job(), job()), dict(listing(job()), total_count=101), listing(job(attempt=2)),
                     listing(dict(job(), status="in_progress")), listing(dict(job(), completed_at="not a date"))):
            with self.subTest(rows=rows):
                self.data[self.controller_jobs] = rows
                with self.assertRaises(ValueError):
                    self.verify()
        self.data[self.controller_jobs] = listing(job())
        self.data["actions/runs/1/attempts/1"]["updated_at"] = START
        with self.assertRaisesRegex(ValueError, "Inconsistent completion"):
            self.verify()
