# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify branch tag freshness, release ownership and digest preservation."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from release_fixture import RELEASE_BRANCH, RELEASE_LINE


CI_IMAGES = Path(".github/workflows/ci-images")
SOURCE = Path(__file__).resolve().parents[1]
DIGEST = "sha256:" + "f" * 64
PACKAGE = "integration-ubuntu"
SUFFIX = "-test-toolchain"


class ReleaseTagTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.origin = self.root / "origin"
        self.worker = self.root / "worker"
        self.calls = self.root / "docker-args"
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
                        GIT_ALLOW_PROTOCOL="file", GIT_AUTHOR_NAME="CI", GIT_COMMITTER_NAME="CI",
                        GIT_AUTHOR_EMAIL="ci@example.invalid", GIT_COMMITTER_EMAIL="ci@example.invalid",
                        GITHUB_REF="refs/heads/main", GITHUB_REPOSITORY="NVIDIA/cudf-spark",
                        GITHUB_STEP_SUMMARY=str(self.root / "summary"), GITHUB_OUTPUT=str(self.root / "output"),
                        DOCKER_ARGS=str(self.calls),
                        PATH=str(self.root) + os.pathsep + os.environ["PATH"])
        (self.origin / CI_IMAGES).mkdir(parents=True)
        for name in ("select.py", "suites.json", "paths.json"):
            shutil.copy(SOURCE / name, self.origin / CI_IMAGES / name)
        self.git("init", "-q", "-b", "main")
        self.git("add", ".")
        self.git("commit", "-qm", "recipe")
        self.git("clone", "-q", "--depth=1", self.origin.as_uri(), str(self.worker))
        # Capture every attempted write. Return the descriptor Buildx wrote for that operation.
        self.command("docker", """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
Path(os.environ["DOCKER_ARGS"]).write_text(json.dumps(args))
metadata = Path(args[args.index("--metadata-file") + 1])
digest = os.environ.get("RETURN_DIGEST", args[-1].split("@")[1])
metadata.write_text(json.dumps({"containerimage.descriptor": {"digest": digest}}))
""")

    def git(self, *args):
        subprocess.run(["git", *args], cwd=self.origin, env=self.env, check=True, capture_output=True)

    def command(self, name, script):
        program = self.root / name
        program.write_text(script)
        program.chmod(0o755)

    def publish(self, package, branch_tag, tag):
        return subprocess.run(["bash", str(SOURCE / "publish-tag.sh"), package, branch_tag, tag, DIGEST],
                              cwd=self.worker, env=self.env, text=True, capture_output=True)

    def seed(self, release_tag=RELEASE_LINE + SUFFIX):
        return self.publish(PACKAGE, "main" + SUFFIX, release_tag)

    def prepare_retry(self):
        self.baselines = self.root / "baselines"
        self.baselines.mkdir()
        self.env.update(GITHUB_RUN_ID="15", GITHUB_RUN_ATTEMPT="2",
                        GITHUB_SHA=subprocess.check_output(["git", "rev-parse", "HEAD"],
                                                          cwd=self.worker, text=True).strip(),
                        PLATFORM_RECORDS=str(self.baselines))
        # Invoke the real CLI with an in-memory registry instead of contacting GHCR.
        (self.worker / CI_IMAGES / "tag-baseline.py").write_text(
            "import importlib.util, json, os, sys\n"
            "sys.dont_write_bytecode = True\n"
            f"spec = importlib.util.spec_from_file_location('guard', {str(SOURCE / 'tag-baseline.py')!r})\n"
            "guard = importlib.util.module_from_spec(spec)\n"
            "spec.loader.exec_module(guard)\n"
            "guard.manifest_digest = lambda image, tag: json.loads(os.environ['REGISTRY_TAGS'])[image + ':' + tag]\n"
            "guard.main()\n")

    def baseline(self, arch, previous, **changes):
        record = dict(image=f"ghcr.io/nvidia/cudf-spark/{PACKAGE}", source=self.env["GITHUB_SHA"],
                      run=self.env["GITHUB_RUN_ID"], attempt="1", platform=f"linux/{arch}", previous=previous)
        (self.baselines / f"baseline-{arch}.json").write_text(json.dumps({**record, **changes}))

    def registry(self, tags):
        image = f"ghcr.io/nvidia/cudf-spark/{PACKAGE}"
        self.env["REGISTRY_TAGS"] = json.dumps({f"{image}:{tag}": digest for tag, digest in tags.items()})

    def assert_no_write(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.calls.exists())
        self.assertFalse(Path(self.env["GITHUB_OUTPUT"]).exists())

    def test_retry_allows_unchanged_missing_or_already_published_tag(self):
        self.prepare_retry()
        tag, old = "main" + SUFFIX, "sha256:" + "1" * 64
        for before, now in ((old, old), (None, None), (old, DIGEST)):
            with self.subTest(before=before, now=now):
                # A failed first-attempt sibling need not have saved a baseline.
                self.baseline("arm64", {tag: before})
                self.registry({tag: now})
                result = self.publish(PACKAGE, tag, tag)
                self.assertEqual(result.returncode, 0, result.stderr)
                args = json.loads(self.calls.read_text())
                self.assertEqual(args[-2:], [f"ghcr.io/nvidia/cudf-spark/{PACKAGE}:{tag}",
                                            f"ghcr.io/nvidia/cudf-spark/{PACKAGE}@{DIGEST}"])
                self.calls.unlink()
                Path(self.env["GITHUB_OUTPUT"]).unlink()

    def test_retry_rejects_changed_deleted_and_conflicting_previous_tags(self):
        self.prepare_retry()
        tag = "main" + SUFFIX
        old, newer = ("sha256:" + c * 64 for c in "12")
        for amd, arm, now in ((old, old, newer), (old, old, None), (None, None, newer), (old, newer, old)):
            with self.subTest(amd=amd, arm=arm, now=now):
                self.baseline("amd64", {tag: amd})
                self.baseline("arm64", {tag: arm})
                self.registry({tag: now})
                self.assert_no_write(self.publish(PACKAGE, tag, tag))

    def test_retry_rejects_missing_or_foreign_original_baseline(self):
        self.prepare_retry()
        tag = "main" + SUFFIX
        self.registry({tag: DIGEST})
        self.assert_no_write(self.publish(PACKAGE, tag, tag))
        self.baseline("arm64", {tag: None})
        for changes in ({"run": "16"}, {"source": "9" * 40}, {"image": "ghcr.io/other/image"},
                        {"attempt": "2"}, {"platform": "linux/arm64"}):
            with self.subTest(changes=changes):
                self.baseline("amd64", {tag: None}, **changes)
                self.assert_no_write(self.publish(PACKAGE, tag, tag))

    def test_retry_checks_release_alias_separately_after_main_succeeds(self):
        self.prepare_retry()
        main, alias = "main" + SUFFIX, RELEASE_LINE + SUFFIX
        old, newer = ("sha256:" + c * 64 for c in "12")
        self.registry({main: old, alias: newer})
        self.baseline("amd64", {main: old, alias: None})
        result = self.publish(PACKAGE, main, main)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.calls.unlink()
        Path(self.env["GITHUB_OUTPUT"]).unlink()
        self.assert_no_write(self.seed())

        # A missing alias key is not a recorded absence, even when the registry has no tag.
        self.baseline("amd64", {main: old})
        self.registry({main: DIGEST, alias: None})
        self.assert_no_write(self.seed())
        self.baseline("amd64", {main: old, alias: None})
        result = self.seed()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"ghcr.io/nvidia/cudf-spark/{PACKAGE}:{alias}", json.loads(self.calls.read_text()))

    def test_stable_skills_tag_checks_recipe_again_after_build(self):
        package = "skills-build"
        dockerfile = self.origin / "skills/ci/Dockerfile.pre-merge"
        dockerfile.parent.mkdir(parents=True)
        dockerfile.write_text("FROM scratch\n")
        self.git("add", ".")
        self.git("commit", "-qm", "new Skills recipe")
        subprocess.run(["git", "pull", "--ff-only"], cwd=self.worker, env=self.env, check=True, capture_output=True)
        subprocess.run(["python3", str(CI_IMAGES / "select.py"), "--current", package],
                       cwd=self.worker, env=self.env, check=True, capture_output=True)
        output = Path(self.env["GITHUB_OUTPUT"])
        self.assertEqual(output.read_text().strip(), "current=true")
        output.unlink()
        self.git("revert", "--no-edit", "HEAD")
        result = self.publish(package, "main", "main")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Skipping main", result.stdout)
        self.assertFalse(self.calls.exists())
        self.assertFalse(output.exists())

    def test_native_stable_tag_keeps_digest_on_main_and_release(self):
        self.git("branch", RELEASE_BRANCH)
        for ref, tag in (("main", "main"), (RELEASE_BRANCH, RELEASE_LINE)):
            with self.subTest(ref=ref):
                self.env["GITHUB_REF"] = "refs/heads/" + ref
                result = self.publish("skills-build", tag, tag)
                self.assertEqual(result.returncode, 0, result.stderr)
                args = json.loads(self.calls.read_text())
                image = "ghcr.io/nvidia/cudf-spark/skills-build"
                self.assertIn("--prefer-index=false", args)
                self.assertEqual(args[-3:], ["--tag", image + ":" + tag, image + "@" + DIGEST])
                output = Path(self.env["GITHUB_OUTPUT"])
                self.assertEqual(output.read_text().strip(), "digest=" + DIGEST)
                output.unlink()
                self.calls.unlink()
        result = self.publish("skills-build", "main", "main")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.calls.exists())

    def test_same_digest_pre_cut_and_release_ownership_after_cut(self):
        self.git("branch", f"{RELEASE_BRANCH}-rc")
        result = self.seed()
        self.assertEqual(result.returncode, 0, result.stderr)
        args = json.loads(self.calls.read_text())
        self.assertEqual(args[:4], ["buildx", "imagetools", "create", "--prefer-index=false"])
        image = "ghcr.io/nvidia/cudf-spark/integration-ubuntu"
        self.assertEqual(args[-3:], ["--tag", f"{image}:{RELEASE_LINE}{SUFFIX}", image + "@" + DIGEST])

        self.calls.unlink()
        self.git("branch", RELEASE_BRANCH)
        result = self.seed()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.calls.exists())

    def test_stale_checkout_cannot_seed(self):
        dockerfile = self.origin / "jenkins/Dockerfile-blossom.integration.ubuntu"
        dockerfile.parent.mkdir()
        dockerfile.write_text("FROM scratch\n")
        self.git("add", ".")
        self.git("commit", "-qm", "new recipe")
        result = self.seed()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.calls.exists())

    def test_failed_ref_query_is_not_a_missing_branch(self):
        git = shutil.which("git")
        self.command("git", f'#!/bin/sh\nif [ "$1" = ls-remote ]; then exit 128; fi\nexec "{git}" "$@"\n')
        result = self.seed()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("git exit 128", result.stderr)
        self.assertFalse(self.calls.exists())

    def test_mismatched_toolchain_and_wrong_returned_digest(self):
        result = self.seed(f"{RELEASE_LINE}-other-toolchain")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.calls.exists())
        self.env["RETURN_DIGEST"] = "sha256:" + "0" * 64
        result = self.seed()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("did not confirm", result.stderr)
