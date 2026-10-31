# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "binary-dedupe.sh"
VERSIONED_CLASS = "META-INF/versions/9/example/Feature.class"
SERVICE = "META-INF/services/example.Provider"
UNRELATED_VERSIONED_CLASS = (
    "META-INF/versions/11/com/nvidia/shaded/spark/org/roaringbitmap/ArraysShim.class")


def write_entry(root, entry, contents):
    path = root / entry
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)


class BinaryDedupeTest(unittest.TestCase):
    def run_dedupe(self, directory, fast, differing=False, shims=("spark355", "spark356")):
        root = directory / "parallel-world"
        write_entry(root, VERSIONED_CLASS, b"same class")
        for index, shim in enumerate(shims):
            contents = b"different class" if differing and index else b"same class"
            write_entry(root / shim, VERSIONED_CLASS, contents)
            write_entry(root / shim, "example/Common.class", b"common class")
            write_entry(root / shim, SERVICE, b"example.Provider\n")
            write_entry(root / shim, UNRELATED_VERSIONED_CLASS, b"unrelated versioned class")
        allowlist = directory / "promote.txt"
        allowlist.write_text("META-INF/versions/*/example/*.class\n")
        environment = os.environ.copy()
        for variable in ("SKIP_BINARY_DEDUPE", "UNSHIM_DEDUPE_CACHE_DIR",
                         "UNSHIM_PROMOTE_DEFAULT_SPARK_SHARED_CLASSES",
                         "KEEP_IN_SPARK_SHARED_TXT", "KEEP_IN_SPARK_SHIM_DIRS_TXT"):
            environment.pop(variable, None)
        environment.update({
            "UNSHIM_FAST": "1" if fast else "0",
            "UNSHIMMED_COMMON_FROM_SINGLE_SHIM_TXT": str(allowlist),
            "UNSHIM_ANALYZER_SCRIPT": str(directory / "missing-analyzer.py"),
        })
        return subprocess.run([str(SCRIPT)], cwd=directory, env=environment,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    def test_identical_versioned_classes_keep_one_root_copy(self):
        for fast in (False, True):
            for shims in (("spark355",), ("spark355", "spark356")):
                with self.subTest(fast=fast, shims=shims):
                    with tempfile.TemporaryDirectory() as temporary:
                        directory = Path(temporary)
                        result = self.run_dedupe(directory, fast, shims=shims)
                        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                        root = directory / "parallel-world"
                        self.assertEqual(b"same class", (root / VERSIONED_CLASS).read_bytes())
                        self.assertFalse((root / "spark-shared" / VERSIONED_CLASS).exists())
                        self.assertFalse((root / UNRELATED_VERSIONED_CLASS).exists())
                        self.assertFalse((root / "spark-shared" / UNRELATED_VERSIONED_CLASS).exists())
                        for shim in shims:
                            self.assertEqual(b"unrelated versioned class",
                                             (root / shim / UNRELATED_VERSIONED_CLASS).read_bytes())
                            self.assertFalse((root / shim / VERSIONED_CLASS).exists())
                            self.assertEqual(b"example.Provider\n",
                                             (root / shim / SERVICE).read_bytes())

    def test_different_versioned_classes_cannot_be_promoted(self):
        for fast in (False, True):
            with self.subTest(fast=fast):
                with tempfile.TemporaryDirectory() as temporary:
                    result = self.run_dedupe(Path(temporary), fast, differing=True)
                    self.assertNotEqual(0, result.returncode)
                    self.assertIn(VERSIONED_CLASS, result.stderr)


if __name__ == "__main__":
    unittest.main()
