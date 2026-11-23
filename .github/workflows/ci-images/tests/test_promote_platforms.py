# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Check that only the verified pair of native images can update the multi-platform tag."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from release_fixture import RELEASE_LINE


PROMOTE = Path(__file__).resolve().parents[1] / "promote-platforms.sh"
IMAGE = "ghcr.io/nvidia/cudf-spark/spark-build"
TAG = f"{RELEASE_LINE}-test-toolchain"
INDEX_DIGEST = "sha256:" + "f" * 64


class PromotePlatformsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.env = dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ["PATH"],
                        GITHUB_REPOSITORY="NVIDIA/cudf-spark", GITHUB_SHA="1" * 40,
                        GITHUB_RUN_ID="15", GITHUB_RUN_ATTEMPT="1", INDEX_DIGEST=INDEX_DIGEST,
                        GITHUB_OUTPUT=str(self.root / "output"), GITHUB_STEP_SUMMARY=str(self.root / "summary"))
        self.sources = []
        for arch, digit in (("amd64", "a"), ("arm64", "b")):
            digest = "sha256:" + digit * 64
            record = dict(image=IMAGE, tag=TAG, platform="linux/" + arch,
                          source=self.env["GITHUB_SHA"], digest=digest)
            (self.root / (arch + ".json")).write_text(json.dumps(record))
            self.sources.append(IMAGE + "@" + digest)
        self.descriptors = [
            {"digest": "sha256:" + digit * 64, "platform": {"os": system, "architecture": arch}}
            for system, arch, digit in (("linux", "amd64", "c"), ("linux", "arm64", "d"),
                                        ("unknown", "unknown", "e"))
        ]
        for name in ("planned", "published"):
            (self.root / name).write_text(json.dumps({"manifests": self.descriptors}))
        (self.root / "calls").touch()
        # The real promoter runs jq and Bash. Only Docker's registry operations are substituted.
        docker = self.root / "docker"
        docker.write_text("""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with open("calls", "a") as calls:
    print(json.dumps(args), file=calls)
if "--dry-run" in args:
    print(Path("planned").read_text())
elif "--metadata-file" in args:
    Path(args[args.index("--metadata-file") + 1]).write_text(
        json.dumps({"containerimage.descriptor": {"digest": os.environ["INDEX_DIGEST"]}}))
elif "--raw" in args:
    print(Path("published").read_text())
elif "--prefer-index=false" not in args:
    sys.exit("unexpected docker invocation")
""")
        docker.chmod(0o755)

    def promote(self):
        result = subprocess.run(["bash", str(PROMOTE), "spark-build", TAG, str(self.root)],
                                cwd=self.root, env=self.env, text=True, capture_output=True)
        calls = [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]
        return result, calls

    def test_promotes_verified_index_from_both_native_digests(self):
        result, calls = self.promote()
        self.assertEqual(result.returncode, 0, result.stderr)
        planned, candidate, inspected, promoted = calls
        self.assertEqual(planned[-3:], ["--dry-run", *self.sources])
        self.assertEqual(candidate[-4:], ["--tag", IMAGE + ":build-" + TAG + "-15-1", *self.sources])
        reference = IMAGE + "@" + INDEX_DIGEST
        self.assertEqual(inspected[-3:], ["inspect", "--raw", reference])
        self.assertIn("--config", inspected)
        self.assertEqual(promoted[-4:], ["--prefer-index=false", "--tag", IMAGE + ":" + TAG, reference])
        self.assertEqual((self.root / "output").read_text(), "digest=" + INDEX_DIGEST + "\n")

    def test_missing_published_attestation_leaves_release_tag_untouched(self):
        (self.root / "published").write_text(json.dumps({"manifests": self.descriptors[:2]}))
        result, calls = self.promote()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("descriptors differ", result.stderr)
        self.assertFalse(any(IMAGE + ":" + TAG in args for args in calls))
        self.assertFalse((self.root / "output").exists())

    def test_duplicate_native_digest_stops_before_registry_access(self):
        arm_record = self.root / "arm64.json"
        record = json.loads(arm_record.read_text())
        record["digest"] = json.loads((self.root / "amd64.json").read_text())["digest"]
        arm_record.write_text(json.dumps(record))
        result, calls = self.promote()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("two different native image digests", result.stderr)
        self.assertEqual(calls, [])
