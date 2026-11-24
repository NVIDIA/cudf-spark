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

from release_fixture import NEXT_RELEASE_BRANCH, NEXT_RELEASE_LINE, RELEASE_BRANCH, RELEASE_LINE


CI_IMAGES = Path(".github/workflows/ci-images")
SOURCE = Path(__file__).resolve().parents[1]
INPUT = "integration_tests/requirements.txt"
SPARK = "jenkins/Dockerfile-blossom.ubuntu"
EXAMPLE = "docs/additional-functionality/shuffle-docker-examples/Dockerfile.ubuntu_rdma"
# Each row specifies the published tag suffix and the Dockerfile arguments it promises.
VARIANTS = [
    ("spark-build", "ubuntu22.04", "12.0.1", "UBUNTU_VER", "22.04"),
    ("integration-ubuntu", "ubuntu22.04", "12.8.0", "UBUNTU_VER", "22.04"),
    ("integration-ubuntu", "ubuntu24.04", "12.8.0", "UBUNTU_VER", "24.04"),
    ("integration-rocky", "rockylinux8", "12.8.0", "ROCKY_VER", "8"),
    ("integration-rocky", "rockylinux9", "12.8.0", "ROCKY_VER", "9"),
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
        self.write_pom(f"{RELEASE_LINE}.0-SNAPSHOT")
        catalog = (SOURCE / "suites.json").read_text()
        self.write(CI_IMAGES / "suites.json", catalog)
        self.write(CI_IMAGES / "paths.json", (SOURCE / "paths.json").read_text())
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

    def resolve(self, *args):
        helper = self.root / "metadata.py"
        helper.write_text(
            "import json\n"
            "def fingerprint(config, root):\n"
            "    assert (root / config['dockerfile']).is_file()\n"
            "    return json.dumps(config, sort_keys=True)\n"
        )
        # The nightly resolver has Python but may not have Git on PATH.
        return json.loads(subprocess.check_output(
            [sys.executable, str(SOURCE / "select.py"), "--resolve", *args],
            cwd=self.repo, env=dict(self.env, PATH="", CI_IMAGE_METADATA_HELPER=str(helper)),
            text=True, stderr=subprocess.PIPE))

    def test_consumer_uses_pom_tags_and_published_worker_recipe(self):
        ignore = SPARK + ".dockerignore"
        self.write(ignore, "*.log\n")
        self.commit("Add a Spark-specific Docker ignore file")
        self.git("checkout", "--detach", "-q")
        selections = ["spark-build/ubuntu22.04/amd64", "integration-ubuntu/ubuntu22.04/arm64",
                      "skills-build/amd64"]
        strict = self.resolve(*selections)
        fallback = self.resolve(*selections, "--allow-main-fallback")
        self.assertEqual(strict.keys(), set(selections))
        self.assertEqual(fallback.keys(), set(selections))
        publications = self.invoke(event="workflow_dispatch")["publish"]
        workers = {(c["image"], c["tag"], w["platform"]): (c, w)
                   for c in publications for w in c["workers"]}
        for key in selections:
            package, *variant, arch = key.split("/")
            suffix = "-" + variant[0] if variant else ""
            version = strict[key]["version"]
            platform = f"linux/{arch}"
            expected = {"image": f"ghcr.io/nvidia/cudf-spark/{package}", "platform": platform,
                        "version": version, "tags": [RELEASE_LINE + suffix]}
            self.assertEqual(strict[key], expected)
            self.assertEqual(fallback[key], expected | {"tags": [RELEASE_LINE + suffix, "main" + suffix]})
            recipe = json.loads(version)  # The fixture returns the fingerprint inputs verbatim.
            config, worker = workers[package, "main" + suffix, platform]
            for field in ("context", "dockerfile", "recipe-inputs"):
                self.assertEqual(recipe[field], config[field])
            self.assertEqual(ignore in recipe["recipe-inputs"], package == "spark-build")
            self.assertEqual(recipe["platform"], platform)
            self.assertEqual(recipe["build-args"], worker["build-args"])

    def test_consumer_rejects_unsupported_or_unpublished_selections(self):
        for key in ("skills-build/arm64", "integration-ubuntu/ubuntu20.04/amd64",
                    "udf-cuda-template/amd64"):
            with self.subTest(key=key):
                with self.assertRaises(subprocess.CalledProcessError) as error:
                    self.resolve("skills-build/amd64", key)
                self.assertIn(key, error.exception.stderr)
                self.assertFalse(error.exception.output.strip())

    def assert_versioned_images(self, configs, prefix, alias_prefix=None):
        self.assertCountEqual([(c["image"], c["tag"]) for c in configs],
                              [(name, f"{prefix}-{suffix}") for name, suffix, *_ in VARIANTS])
        by_ref = {(c["image"], c["tag"]): c for c in configs}
        ids = set()
        scopes = set()
        for name, suffix, cuda, distro_arg, distro_version in VARIANTS:
            config = by_ref[name, f"{prefix}-{suffix}"]
            self.assertEqual(config["release_tag"], f"{alias_prefix}-{suffix}" if alias_prefix else "")
            self.assertNotIn(config["id"], ids)
            ids.add(config["id"])
            self.assertTrue(config["multi"])
            self.assertCountEqual([(w["arch"], w["platform"]) for w in config["workers"]],
                                  [("amd64", "linux/amd64"), ("arm64", "linux/arm64")])
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
        self.assert_versioned_images(jobs["publish"], "main", alias_prefix=RELEASE_LINE)

    def test_pr_cache_imports_match_publication_for_each_variant_and_arch(self):
        self.git("switch", "-qc", "topic")
        self.write(CI_IMAGES / "select.py", "# select all images\n")
        self.commit("Validate every image")
        self.git("switch", "-q", "main")
        self.git("merge", "--no-ff", "-qm", "Synthetic PR merge", "topic")
        pr = self.invoke(event="pull_request", ref="17/merge")
        self.assertEqual(pr["publish"], [])
        self.assertTrue(pr["run-tests"])
        published = self.invoke(event="workflow_dispatch")["publish"]
        expected = {(c["id"], w["arch"]): cache_scope(w["cache-to"][0])
                    for c in published for w in c["workers"]}
        self.assertEqual(len(expected), 2 * len(VARIANTS) + 1)  # Versioned pairs plus Skills AMD64.
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

    def test_cuda_bump_rebuilds_existing_tags_for_both_architectures(self):
        before = self.git("rev-parse", "HEAD")
        path = CI_IMAGES / "suites.json"
        catalog = json.loads((self.repo / path).read_text())
        catalog["ci-toolchains"]["integration-ubuntu"]["build-args"]["CUDA_VER"] = "99.0.0"
        self.write(path, json.dumps(catalog))
        self.commit("Update the Ubuntu integration base image")
        jobs = self.invoke(payload={"created": False, "before": before})
        self.assertCountEqual([(c["image"], c["tag"], c["release_tag"]) for c in jobs["publish"]],
                              [("integration-ubuntu", f"main-{distro}", f"{RELEASE_LINE}-{distro}")
                               for distro in ("ubuntu22.04", "ubuntu24.04")])
        for config in jobs["publish"]:
            self.assertTrue(config["multi"])
            self.assertCountEqual([(w["arch"], w["build-args"]["CUDA_VER"]) for w in config["workers"]],
                                  [("amd64", "99.0.0"), ("arm64", "99.0.0")])

    def test_unrelated_pr_and_push_emit_empty_matrices(self):
        before = self.git("rev-parse", "HEAD")
        self.git("switch", "-qc", "topic")
        self.write("notes.md", "unrelated\n")
        self.write(CI_IMAGES / "docs/notes.md", "image documentation only\n")
        self.commit("Docs only")
        self.git("switch", "-q", "main")
        self.git("merge", "--no-ff", "-qm", "Synthetic PR merge", "topic")

        for event in [
            {"event": "pull_request", "ref": "17/merge"},
            {"event": "pull_request", "ref": "17/merge", "base_ref": RELEASE_BRANCH},
            {"event": "push", "payload": {"created": False, "before": before}},
        ]:
            with self.subTest(event=event):
                expected = {"validate": [], "publish": []}
                if event["event"] == "pull_request":
                    expected["run-tests"] = False
                self.assertEqual(self.invoke(**event), expected)
                # The workflow uses the literal [] output to allow a skipped image job.
                self.assertCountEqual((self.root / "output").read_text().splitlines(),
                                      [f"{key}={json.dumps(value)}" for key, value in expected.items()])

    def test_test_only_path_from_configuration(self):
        path = CI_IMAGES / "paths.json"
        rules = json.loads((self.repo / path).read_text())
        rules["unit-tests"]["include"].append("ci/probes/")
        self.write(path, json.dumps(rules))
        self.commit("Configure an additional test input")
        self.git("switch", "-qc", "topic")
        self.write("ci/probes/example.py", "# changed\n")
        self.commit("Change the configured test input")
        self.git("switch", "-q", "main")
        self.git("merge", "--no-ff", "-qm", "Synthetic PR merge", "topic")
        self.assertEqual(self.invoke(event="pull_request", ref="17/merge"),
                         {"validate": [], "publish": [], "run-tests": True})

    def test_publisher_path_uses_the_same_config_for_push_and_freshness(self):
        path = CI_IMAGES / "paths.json"
        rules = json.loads((self.repo / path).read_text())
        rules["multi-platform-publication"] = ["ci/promote-example.sh"]
        self.write(path, json.dumps(rules))
        self.commit("Configure the multi-platform promotion input")
        before = self.git("rev-parse", "HEAD")
        worker = self.root / "worker"
        self.git("clone", "-q", "--depth=1", self.repo.as_uri(), str(worker))
        self.write("ci/promote-example.sh", "# changed\n")
        self.commit("Change the configured promotion input")
        jobs = self.invoke(payload={"created": False, "before": before})
        self.assert_versioned_images(jobs["publish"], "main", alias_prefix=RELEASE_LINE)
        for image in {job["image"] for job in jobs["publish"]}:
            self.assertFalse(self.invoke("--current", image, checkout=worker)["current"])
        self.assertTrue(self.invoke("--current", "skills-build", checkout=worker)["current"])

    def test_moving_the_configured_version_file_supersedes_old_checkouts(self):
        before = self.git("rev-parse", "HEAD")
        worker = self.root / "worker"
        self.git("clone", "-q", "--depth=1", self.repo.as_uri(), str(worker))
        path = CI_IMAGES / "paths.json"
        rules = json.loads((self.repo / path).read_text())
        rules["release-version-file"] = "ci/version.xml"
        self.write(path, json.dumps(rules))
        self.write("ci/version.xml", (self.repo / "pom.xml").read_text())
        self.git("rm", "pom.xml")
        self.commit("Move the version source and update the policy together")
        jobs = self.invoke(payload={"created": False, "before": before})
        self.assert_versioned_images([row for row in jobs["publish"] if row["release_tag"]],
                                     "main", alias_prefix=RELEASE_LINE)
        self.assertFalse(self.invoke("--current", "spark-build", checkout=worker)["current"])

    def test_new_release_bootstraps_toolchains_only(self):
        self.git("switch", "-qc", RELEASE_BRANCH)
        jobs = self.invoke(ref=RELEASE_BRANCH, payload={"created": True, "before": "0" * 40})
        self.assertEqual(jobs["validate"], [])
        skills = [c for c in jobs["publish"] if c["image"] == "skills-build"]
        self.assertEqual([(c["tag"], c["release_tag"]) for c in skills], [(RELEASE_LINE, "")])
        self.assert_versioned_images([c for c in jobs["publish"] if c["image"] != "skills-build"],
                                     RELEASE_LINE)

    def test_new_release_branch_must_match_project_version(self):
        self.git("switch", "-qc", NEXT_RELEASE_BRANCH)
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            self.invoke(ref=NEXT_RELEASE_BRANCH, payload={"created": True, "before": "0" * 40})
        self.assertIn(NEXT_RELEASE_BRANCH, caught.exception.stderr)
        self.assertIn(RELEASE_LINE, caught.exception.stderr)

    def test_release_pr_validates_the_target_line_before_publication(self):
        self.git("switch", "-qc", "topic")
        self.write_pom(f"{NEXT_RELEASE_LINE}.0-SNAPSHOT")
        self.commit("Move to the next release line")
        self.git("switch", "-q", "main")
        self.git("merge", "--no-ff", "-qm", "Synthetic PR merge", "topic")
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            self.invoke(event="pull_request", ref="17/merge", base_ref=RELEASE_BRANCH)
        self.assertIn(
            f"Branch {RELEASE_BRANCH} does not match root pom.xml release line {NEXT_RELEASE_LINE}",
            caught.exception.stderr,
        )
        for target in ["main", NEXT_RELEASE_BRANCH]:
            with self.subTest(target=target):
                jobs = self.invoke(event="pull_request", ref="17/merge", base_ref=target)
                self.assertEqual(len(jobs["validate"]), len(VARIANTS))
                self.assertEqual(jobs["publish"], [])

    def test_main_pom_rollover_selects_only_versioned_images_and_invalidates_old_publisher(self):
        worker = self.root / "worker"
        self.git("clone", "-q", "--depth=1", self.repo.as_uri(), str(worker))
        for version, description in [
            (f"{RELEASE_LINE}.0-SNAPSHOT", "new description"),
            (f"{RELEASE_LINE}.1", "CI"),
        ]:
            with self.subTest(version=version):
                before = self.git("rev-parse", "HEAD")
                self.write_pom(version, description)
                self.commit("Edit the POM without changing the release line")
                self.assertEqual(self.invoke(payload={"created": False, "before": before}),
                                 {"validate": [], "publish": []})
                self.assertIs(self.invoke("--current", "integration-ubuntu", checkout=worker)["current"],
                              True)

        before = self.git("rev-parse", "HEAD")
        self.write_pom(f"{NEXT_RELEASE_LINE}.0-SNAPSHOT")
        self.commit("Advance the release line without changing any image recipes")
        jobs = self.invoke(payload={"created": False, "before": before})
        self.assertEqual(jobs["validate"], [])
        self.assert_versioned_images(jobs["publish"], "main", alias_prefix=NEXT_RELEASE_LINE)
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
