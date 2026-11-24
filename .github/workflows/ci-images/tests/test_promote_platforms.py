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
                        GITHUB_REF="refs/heads/release/" + RELEASE_LINE,
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
        # The publisher's ownership checks have separate tests.
        selector = self.root / ".github/workflows/ci-images/select.py"
        selector.parent.mkdir(parents=True)
        selector.write_text('import os\nfrom pathlib import Path\n'
                            'Path(os.environ["GITHUB_OUTPUT"]).write_text("current=true\\n")\n')
        # Keep the fixture independent of wall time and GNU timeout availability.
        for name, body in {
            "timeout": '[ "$1" = "--kill-after=2s" ] && [ "$2" = "15s" ] || exit 125\n'
                       'shift 2\nexec "$@"',
            "sleep": 'echo "$1" >> sleeps',
        }.items():
            executable = self.root / name
            executable.write_text("#!/bin/sh\n" + body + "\n")
            executable.chmod(0o755)
        self.failures = self.root / "inspect-failures"
        self.failures.write_text("[]")
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
    failures = json.loads(Path("inspect-failures").read_text())
    if failures:
        code, message = failures.pop(0)
        Path("inspect-failures").write_text(json.dumps(failures))
        print(message, file=sys.stderr)
        sys.exit(code)
    print(Path("published").read_text())
else:
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
        self.assertIn("--prefer-index=false", promoted)
        self.assertEqual(promoted[-3:], ["--tag", IMAGE + ":" + TAG, reference])
        self.assertEqual((self.root / "output").read_text(), "digest=" + INDEX_DIGEST + "\n")

    def test_pending_manifest_then_timed_out_read_recovers_without_repeating_writes(self):
        self.failures.write_text(json.dumps([(1, "manifest unknown"), (124, "")]))
        result, calls = self.promote()
        self.assertEqual(result.returncode, 0, result.stderr)
        planned, candidate, first, second, third, promoted = calls
        self.assertIn("--dry-run", planned)
        self.assertIn(IMAGE + ":build-" + TAG + "-15-1", candidate)
        self.assertEqual(first[-3:], ["inspect", "--raw", IMAGE + "@" + INDEX_DIGEST])
        self.assertEqual([second, third], [first, first])
        self.assertIn(IMAGE + ":" + TAG, promoted)
        self.assertEqual(len((self.root / "sleeps").read_text().splitlines()), 2)

    def test_missing_manifest_exhausts_reads_without_updating_tag(self):
        self.failures.write_text(json.dumps([(1, IMAGE + "@" + INDEX_DIGEST + ": not found")] * 3))
        result, calls = self.promote()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("after 3 attempts", result.stderr)
        self.assertIn(INDEX_DIGEST, result.stderr)
        self.assertEqual(sum("--raw" in args for args in calls), 3)
        self.assertEqual(sum("--metadata-file" in args for args in calls), 1)
        self.assertFalse((self.root / "output").exists())
        self.assertEqual(len((self.root / "sleeps").read_text().splitlines()), 2)

    def test_unauthorized_read_fails_immediately(self):
        self.failures.write_text(json.dumps([(1, "401 Unauthorized")]))
        result, calls = self.promote()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("401 Unauthorized", result.stderr)
        self.assertEqual(sum("--raw" in args for args in calls), 1)
        self.assertFalse(any(IMAGE + ":" + TAG in args for args in calls))
        self.assertFalse((self.root / "sleeps").exists())

    def test_missing_published_attestation_leaves_release_tag_untouched(self):
        (self.root / "published").write_text(json.dumps({"manifests": self.descriptors[:2]}))
        result, calls = self.promote()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("descriptors differ", result.stderr)
        self.assertEqual(sum("--raw" in args for args in calls), 1)
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
