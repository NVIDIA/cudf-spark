# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


CI_IMAGES = Path(".github/workflows/ci-images")
SOURCE = Path(__file__).resolve().parents[1]
INPUT = "integration_tests/requirements.txt"
SPARK = "jenkins/Dockerfile-blossom.ubuntu"
EXAMPLE = "docs/additional-functionality/shuffle-docker-examples/Dockerfile.ubuntu_rdma"


class ImageSelectorTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.env = dict(os.environ, GIT_ALLOW_PROTOCOL="file", GIT_CONFIG_NOSYSTEM="1",
                        GIT_CONFIG_GLOBAL=os.devnull, GIT_AUTHOR_NAME="CI",
                        GIT_AUTHOR_EMAIL="ci@example.invalid", GIT_COMMITTER_NAME="CI",
                        GIT_COMMITTER_EMAIL="ci@example.invalid")
        self.git("init", "-q", "-b", "main")
        catalog = (SOURCE / "suites.json").read_text()
        self.write(CI_IMAGES / "suites.json", catalog)
        for entries in json.loads(catalog).values():
            for entry in entries.values():
                self.write(entry["dockerfile"], "FROM scratch\n")
                self.write(CI_IMAGES / "smoke" / (entry["smoke"] + ".sh"), "# smoke\n")
                for path in entry.get("recipe-inputs", []):
                    self.write(path, "# input\n")
        self.commit("Initial recipes")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, env=self.env,
                                       text=True).strip()

    def write(self, name, content):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-qm", message)

    def invoke(self, *args, event="push", ref="main", payload=None, checkout=None):
        event_path = self.root / "event.json"
        event_path.write_text(json.dumps(payload or {}))
        output = self.root / "output"
        output.write_text("")
        env = dict(self.env, GITHUB_EVENT_NAME=event, GITHUB_EVENT_PATH=str(event_path),
                   GITHUB_REF_NAME=ref, GITHUB_REF="refs/heads/" + ref, GITHUB_OUTPUT=str(output))
        subprocess.run([sys.executable, str(SOURCE / "select.py"), *args], check=True,
                       cwd=checkout or self.repo, env=env, stdout=subprocess.PIPE)
        return {key: json.loads(value) for key, value in
                (line.split("=", 1) for line in output.read_text().splitlines())}

    def test_pr_selects_full_merge_diff_not_last_commit_or_base_only_changes(self):
        self.git("switch", "-qc", "topic")
        self.write(SPARK, "FROM scratch\n# first PR commit\n")
        self.commit("Change the Spark builder")
        self.write(EXAMPLE, "FROM scratch\n# second PR commit\n")
        self.commit("Change one documented example")
        self.git("switch", "-q", "main")
        self.write("skills/ci/Dockerfile.pre-merge", "FROM scratch\n# base only\n")
        self.commit("Advance the base independently")
        self.git("merge", "--no-ff", "-qm", "Synthetic PR merge", "topic")
        jobs = self.invoke(event="pull_request", ref="17/merge")
        self.assertCountEqual([c["image"] for c in jobs["validate"]],
                              ["spark-build", "shuffle-ubuntu-rdma"])
        self.assertEqual(jobs["publish"], [])

    def test_push_selects_only_recipes_using_changed_input(self):
        before = self.git("rev-parse", "HEAD")
        self.write(INPUT, "# new requirement\n")
        self.commit("Change the input, not a Dockerfile")
        self.write("notes.md", "unrelated last commit\n")
        self.commit("Finish the same push with an unrelated change")
        jobs = self.invoke(payload={"created": False, "before": before})
        self.assertEqual(jobs["validate"], [])
        self.assertCountEqual([c["image"] for c in jobs["publish"]],
                              ["spark-build", "integration-ubuntu", "integration-rocky"])

    def test_unrelated_push_selects_nothing(self):
        before = self.git("rev-parse", "HEAD")
        self.write("notes.md", "unrelated\n")
        self.commit("Docs only")
        self.assertEqual(self.invoke(payload={"created": False, "before": before}),
                         {"validate": [], "publish": []})

    def test_new_release_bootstraps_toolchains_only(self):
        self.git("switch", "-qc", "release/26.12")
        jobs = self.invoke(ref="release/26.12", payload={"created": True, "before": "0" * 40})
        self.assertEqual(jobs["validate"], [])
        self.assertCountEqual([(c["image"], c["tag"]) for c in jobs["publish"]],
                              [(name, "26.12") for name in
                               ("spark-build", "skills-build", "integration-ubuntu",
                                "integration-rocky")])

    def test_superseded_checkout_checks_current_branch_inputs_and_catalog(self):
        worker = self.root / "worker"
        self.git("clone", "-q", "--depth=1", self.repo.as_uri(), str(worker))
        self.write("notes.md", "unrelated\n")
        self.commit("The branch advanced but the recipe did not")
        self.assertIs(self.invoke("--current", "spark-build", checkout=worker)["current"], True)

        self.write(INPUT, "# new requirement\n")
        self.commit("Supersede recipes consuming this input")
        self.assertIs(self.invoke("--current", "spark-build", checkout=worker)["current"], False)
        self.assertIs(self.invoke("--current", "skills-build", checkout=worker)["current"], True)

        path = CI_IMAGES / "suites.json"
        catalog = json.loads((self.repo / path).read_text())
        catalog["ci-toolchains"]["skills-build"]["build-args"] = {"VERSION": "2"}
        self.write(path, json.dumps(catalog))
        self.commit("Supersede the skills recipe through its catalog entry alone")
        self.assertIs(self.invoke("--current", "skills-build", checkout=worker)["current"], False)
