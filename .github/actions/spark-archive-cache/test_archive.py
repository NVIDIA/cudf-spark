# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock

import archive


class SparkArchiveTest(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.root = Path(root.name).resolve()
        self.path = self.root / "cache" / "fixture.tgz"
        self.url = "https://example.invalid/fixture.tgz"
        self.content = b"test archive bytes"
        self.digest = hashlib.sha512(self.content).hexdigest()

    def test_verified_download_then_hit_without_network(self):
        fetch = Mock(side_effect=lambda _, file: file.write_bytes(self.content))
        archive.ensure(self.path, self.url, self.digest, fetch)
        archive.ensure(self.path, self.url, self.digest, fetch)
        self.assertEqual(self.path.read_bytes(), self.content)
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(fetch.call_args.args[0], self.url)

    def test_corrupt_restore_and_invalid_download_are_not_reused(self):
        self.path.parent.mkdir()
        self.path.write_bytes(b"bad restored archive")
        with self.assertRaisesRegex(ValueError, "expected SHA-512"):
            archive.ensure(self.path, self.url, self.digest, lambda _, file: file.write_bytes(b"bad download"))
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])
        self.assertEqual(self.path.read_bytes(), b"bad restored archive")

        def refetch(url, file):
            self.assertEqual(self.path.read_bytes(), b"bad restored archive")
            file.write_bytes(self.content)

        archive.ensure(self.path, self.url, self.digest, refetch)
        self.assertEqual(self.path.read_bytes(), self.content)
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_ancestor_symlink_is_rejected_before_restore_and_use(self):
        home = self.root / "home"
        cache = home / ".cache" / "cudf-spark" / "spark"
        cache.parent.mkdir(parents=True)
        outside = self.root / "outside"
        outside.mkdir()
        name, _, _ = archive.archive("2.12")
        external_file = outside / name
        external_file.write_bytes(b"must not change")

        def invoke(operation):
            return subprocess.run(
                [sys.executable, "-B", archive.__file__, operation, "2.12"],
                env={**os.environ, "HOME": str(home), "PATH": ""},
                capture_output=True, text=True,
            )

        metadata = invoke("metadata")
        self.assertEqual(metadata.returncode, 0, metadata.stderr)
        self.assertIn(f"path={cache / name}\n", metadata.stdout)
        cache.symlink_to(outside, target_is_directory=True)
        for operation in ("metadata", "ensure"):
            with self.subTest(operation=operation):
                result = invoke(operation)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("Refusing symlink", result.stderr)
                self.assertEqual(external_file.read_bytes(), b"must not change")

    def test_leaf_symlink_is_never_used_even_if_digest_matches(self):
        self.path.parent.mkdir()
        outside = self.root / "verified-bytes"
        outside.write_bytes(self.content)
        self.path.symlink_to(outside)
        fetch = Mock()
        with self.assertRaisesRegex(ValueError, "Refusing symlink"):
            archive.ensure(self.path, self.url, self.digest, fetch)
        fetch.assert_not_called()
        self.assertTrue(self.path.is_symlink())
        self.assertEqual(outside.read_bytes(), self.content)

    def test_failed_download_leaves_no_partial_archive(self):
        def fail(url, file):
            file.write_bytes(b"partial")
            raise subprocess.TimeoutExpired("curl", 600)

        with self.assertRaises(subprocess.TimeoutExpired):
            archive.ensure(self.path, self.url, self.digest, fail)
        self.assertEqual(list(self.path.parent.iterdir()), [])

    def test_lane_metadata_addresses_distinct_public_archives(self):
        entries = [archive.archive(scala) for scala in ("2.12", "2.13")]
        self.assertNotEqual(entries[0][0], entries[1][0])
        for name, url, digest in entries:
            self.assertTrue(url.startswith("https://archive.apache.org/dist/spark/spark-"))
            self.assertTrue(url.endswith("/" + name))
            self.assertRegex(digest, r"^[0-9a-f]{128}$")
