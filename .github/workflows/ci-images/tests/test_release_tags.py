# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


CI_IMAGES = Path(".github/workflows/ci-images")
SOURCE = Path(__file__).resolve().parents[1]
DIGEST = "sha256:" + "f" * 64
PACKAGE = "integration-ubuntu"
SUFFIX = "-cuda12.8.0-ubuntu22.04"


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
                        GITHUB_STEP_SUMMARY=str(self.root / "summary"), DOCKER_ARGS=str(self.calls),
                        PATH=str(self.root) + os.pathsep + os.environ["PATH"])
        (self.origin / CI_IMAGES).mkdir(parents=True)
        for name in ("select.py", "suites.json"):
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

    def seed(self, release_tag="26.12" + SUFFIX):
        return subprocess.run(["bash", str(SOURCE / "seed-release-tag.sh"), PACKAGE,
                               "main" + SUFFIX, release_tag, DIGEST],
                              cwd=self.worker, env=self.env, text=True, capture_output=True)

    def test_same_digest_pre_cut_and_release_ownership_after_cut(self):
        self.git("branch", "release/26.12-rc")
        result = self.seed()
        self.assertEqual(result.returncode, 0, result.stderr)
        args = json.loads(self.calls.read_text())
        self.assertEqual(args[:4], ["buildx", "imagetools", "create", "--prefer-index=false"])
        image = "ghcr.io/nvidia/cudf-spark/integration-ubuntu"
        self.assertEqual(args[-3:], ["--tag", image + ":26.12" + SUFFIX, image + "@" + DIGEST])

        self.calls.unlink()
        self.git("branch", "release/26.12")
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
        result = self.seed("26.12-cuda12.8.0-ubuntu24.04")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.calls.exists())
        self.env["RETURN_DIGEST"] = "sha256:" + "0" * 64
        result = self.seed()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("did not confirm", result.stderr)
