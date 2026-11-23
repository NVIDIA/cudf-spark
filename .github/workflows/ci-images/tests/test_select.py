# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check which Docker images each event selects and which tags and caches each build uses."""

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
# Each row specifies the published tag suffix and the Dockerfile arguments it promises.
VARIANTS = [
    ("spark-build", "cuda12.0.1-ubuntu22.04", "12.0.1", "UBUNTU_VER", "22.04"),
    ("integration-ubuntu", "cuda12.8.0-ubuntu22.04", "12.8.0", "UBUNTU_VER", "22.04"),
    ("integration-ubuntu", "cuda12.8.0-ubuntu24.04", "12.8.0", "UBUNTU_VER", "24.04"),
    ("integration-rocky", "cuda12.8.0-rockylinux8", "12.8.0", "ROCKY_VER", "8"),
    ("integration-rocky", "cuda12.8.0-rockylinux9", "12.8.0", "ROCKY_VER", "9"),
]


def cache_scope(cache_spec):
    return dict(option.split("=", 1) for option in cache_spec.split(","))["scope"]


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
        self.write_pom("26.12.0-SNAPSHOT")
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

    def write_pom(self, version, description="CI"):
        self.write("pom.xml", '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                   '<modelVersion>4.0.0</modelVersion><parent><groupId>org.example</groupId>'
                   '<artifactId>parent</artifactId><version>99.01.0</version></parent>'
                   f'<artifactId>example</artifactId><version>{version}</version>'
                   f'<description>{description}</description></project>\n')

    def invoke(self, *args, event="push", ref="main", base_ref="main", payload=None, checkout=None):
        event_path = self.root / "event.json"
        event_path.write_text(json.dumps(payload or {}))
        output = self.root / "output"
        output.write_text("")
        env = dict(self.env, GITHUB_EVENT_NAME=event, GITHUB_EVENT_PATH=str(event_path),
                   GITHUB_REF_NAME=ref, GITHUB_BASE_REF=base_ref, GITHUB_REF="refs/heads/" + ref,
                   GITHUB_OUTPUT=str(output))
        command = [sys.executable, str(SOURCE / "select.py"), *args]
        subprocess.run(command, cwd=checkout or self.repo, env=env, check=True,
                       capture_output=True, text=True)
        return {key: json.loads(value) for key, value in
                (line.split("=", 1) for line in output.read_text().splitlines())}

    def assert_versioned_images(self, configs, prefix, alias_prefix=None):
        self.assertCountEqual([(c["image"], c["tag"]) for c in configs],
                              [(name, f"{prefix}-{suffix}") for name, suffix, *_ in VARIANTS])
        by_tag = {c["tag"]: c for c in configs}
        ids = set()
        scopes = set()
        for name, suffix, cuda, distro_arg, distro_version in VARIANTS:
            config = by_tag[f"{prefix}-{suffix}"]
            self.assertEqual(config["release_tag"], f"{alias_prefix}-{suffix}" if alias_prefix else "")
            self.assertNotIn(config["id"], ids)
            ids.add(config["id"])
            arches = ["amd64", "arm64"] if name == "spark-build" else ["amd64"]
            self.assertCountEqual([w["arch"] for w in config["workers"]], arches)
            for worker in config["workers"]:
                self.assertEqual(worker["build-args"]["CUDA_VER"], cuda)
                self.assertEqual(worker["build-args"][distro_arg], distro_version)
                self.assertEqual(worker["cache-from"], [])
                export, = worker["cache-to"]
                scope = cache_scope(export)
                self.assertNotIn(scope, scopes)
                scopes.add(scope)

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
        self.assert_versioned_images(jobs["publish"], "main", alias_prefix="26.12")

    def test_pr_cache_imports_match_publication_for_each_variant_and_arch(self):
        self.git("switch", "-qc", "topic")
        self.write(CI_IMAGES / "select.py", "# select all images\n")
        self.commit("Validate every image")
        self.git("switch", "-q", "main")
        self.git("merge", "--no-ff", "-qm", "Synthetic PR merge", "topic")
        pr = self.invoke(event="pull_request", ref="17/merge")
        self.assertEqual(pr["publish"], [])
        published = self.invoke(event="workflow_dispatch")["publish"]
        expected = {(c["id"], w["arch"]): cache_scope(w["cache-to"][0])
                    for c in published for w in c["workers"]}
        self.assertEqual(len(expected), 7)  # Four integration, two Spark and one Skills worker.
        restored = {}
        pr_only = []
        for config in pr["validate"]:
            self.assertEqual((config["tag"], config["release_tag"]), ("validate", ""))
            for worker in config["workers"]:
                cache_import, = worker["cache-from"]
                key = (config["id"], worker["arch"])
                if key in expected:
                    self.assertEqual(worker["cache-to"], [])
                    restored[key] = cache_scope(cache_import)
                else:
                    self.assertEqual(worker["cache-to"], [cache_import + ",mode=min"])
                    pr_only.append(config["id"])
        self.assertEqual(restored, expected)
        self.assertCountEqual(pr_only, ["shuffle-ubuntu-no-rdma", "shuffle-ubuntu-rdma",
                                       "shuffle-rocky-no-rdma", "shuffle-rocky-rdma",
                                       "udf-cuda-template"])

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
        skills = [c for c in jobs["publish"] if c["image"] == "skills-build"]
        self.assertEqual([(c["tag"], c["release_tag"]) for c in skills], [("26.12", "")])
        self.assert_versioned_images([c for c in jobs["publish"] if c["image"] != "skills-build"],
                                     "26.12")

    def test_new_release_branch_must_match_project_version(self):
        self.git("switch", "-qc", "release/27.02")
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            self.invoke(ref="release/27.02", payload={"created": True, "before": "0" * 40})
        self.assertIn("release/27.02", caught.exception.stderr)
        self.assertIn("26.12", caught.exception.stderr)

    def test_release_pr_validates_the_target_line_before_publication(self):
        self.git("switch", "-qc", "topic")
        self.write_pom("27.02.0-SNAPSHOT")
        self.commit("Move to the next release line")
        self.git("switch", "-q", "main")
        self.git("merge", "--no-ff", "-qm", "Synthetic PR merge", "topic")
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            self.invoke(event="pull_request", ref="17/merge", base_ref="release/26.12")
        self.assertIn("Branch release/26.12 does not match root pom.xml release line 27.02",
                      caught.exception.stderr)
        for target in ["main", "release/27.02"]:
            with self.subTest(target=target):
                jobs = self.invoke(event="pull_request", ref="17/merge", base_ref=target)
                self.assertEqual(len(jobs["validate"]), len(VARIANTS))
                self.assertEqual(jobs["publish"], [])

    def test_main_pom_rollover_selects_only_versioned_images_and_invalidates_old_publisher(self):
        worker = self.root / "worker"
        self.git("clone", "-q", "--depth=1", self.repo.as_uri(), str(worker))
        for version, description in [("26.12.0-SNAPSHOT", "new description"), ("26.12.1", "CI")]:
            with self.subTest(version=version):
                before = self.git("rev-parse", "HEAD")
                self.write_pom(version, description)
                self.commit("Edit the POM without changing the release line")
                self.assertEqual(self.invoke(payload={"created": False, "before": before}),
                                 {"validate": [], "publish": []})
                self.assertIs(self.invoke("--current", "integration-ubuntu", checkout=worker)["current"],
                              True)

        before = self.git("rev-parse", "HEAD")
        self.write_pom("27.02.0-SNAPSHOT")
        self.commit("Advance the release line without changing any image recipes")
        jobs = self.invoke(payload={"created": False, "before": before})
        self.assertEqual(jobs["validate"], [])
        self.assert_versioned_images(jobs["publish"], "main", alias_prefix="27.02")
        self.assertIs(self.invoke("--current", "integration-ubuntu", checkout=worker)["current"], False)
        self.assertIs(self.invoke("--current", "skills-build", checkout=worker)["current"], True)

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
