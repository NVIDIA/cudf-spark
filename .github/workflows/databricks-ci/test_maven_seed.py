# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import publish_maven_seed as publisher
import maven_seed_v2 as seed


class MavenSeedTests(unittest.TestCase):
    def test_release_only_and_fresh_driver(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            repo, tar, driver = root/"repo", root/"seed.tar", root/"driver"
            for path, sources in [
                ("org/example/foo/1", {"foo-1.jar": "central", "foo-1.pom": "google-maven-central",
                                       "foo-1-local.jar": "", "foo-1-private.jar": "private-repo"}),
                ("org/example/foo/2-SNAPSHOT", {"foo-2-SNAPSHOT.jar": "central"}),
                ("org/example/foo/2-20261127.013759-1", {"foo-2-20261127.013759-1.jar": "central"}),
                ("com/nvidia/private/1", {"private-1.jar": "central"}),
                ("com/databricks/driver/1", {"driver-1.jar": "central"}),
            ]:
                folder = repo/path
                folder.mkdir(parents=True)
                (folder/"_remote.repositories").write_text("".join(f"{f}>{remote}=\n" for f,remote in sources.items()))
                for name in sources:
                    (folder/name).write_bytes(b"public test fixture")
            (repo/"org/example/foo/1/extra.jar").symlink_to(repo/"org/example/foo/1/foo-1.jar")
            with (repo/"org/example/foo/1/_remote.repositories").open("a") as markers:
                markers.write("foo-1-local.jar>central=\nfoo-1-private.jar>google-maven-central=\n")
            self.assertEqual(seed.pack(repo, tar), 2)
            with tarfile.open(tar) as archive:
                self.assertEqual(sorted(archive.getnames()), [
                    "org/example/foo/1/_remote.repositories", "org/example/foo/1/foo-1.jar", "org/example/foo/1/foo-1.pom"])
            with self.assertRaisesRegex(ValueError, "digest|trusted producer"):
                seed.restore(driver, tar, "0"*64)
            self.assertFalse((driver/"org").exists())
            sha = hashlib.sha256(tar.read_bytes()).hexdigest()
            shared = root/"shared"
            shared.mkdir()
            (root/"redirect").symlink_to(shared, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "driver-local"):
                seed.restore(root/"redirect"/"repository", tar, sha)
            self.assertEqual(list(shared.iterdir()), [])
            self.assertEqual(seed.restore(driver, tar, sha), 1)
            jar = driver/"org/example/foo/1/foo-1.jar"
            self.assertEqual(jar.read_bytes(), b"public test fixture")
            jar.write_bytes(b"previous download")
            self.assertEqual(seed.restore(driver, tar, sha), 0)
            self.assertEqual(jar.read_bytes(), b"previous download")
            duplicate = root/"again.tar"
            seed.pack(repo, duplicate)
            self.assertEqual(tar.read_bytes(), duplicate.read_bytes())

    def test_upload_and_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp)/"seed.tar"
            file.write_bytes(b"seed")
            api = Mock(return_value={"is_dir": False, "file_size": 4})
            with patch.object(publisher, "workspace_api", return_value=api), patch.object(publisher.subprocess, "run") as curl:
                self.assertIn("unchanged size", publisher.publish("https://workspace.invalid", "fixture", file, True))
                curl.assert_not_called()
                curl.return_value = Mock(returncode=0, stdout="200")
                self.assertEqual(publisher.publish("https://workspace.invalid", "fixture", file, False), "uploaded")
                self.assertNotIn("fixture", repr(curl.call_args.args))
                self.assertNotIn("--location", curl.call_args.args[0])
                api.return_value = {"is_dir": False, "file_size": 0}
                with self.assertRaisesRegex(RuntimeError, "size not confirmed"):
                    publisher.publish("https://workspace.invalid", "fixture", file, False)
