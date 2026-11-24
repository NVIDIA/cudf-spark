# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock

import archive


class SparkArchiveTest(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.path = Path(root.name) / "cache" / "fixture.tgz"
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
        self.assertEqual(list(self.path.parent.iterdir()), [])

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
