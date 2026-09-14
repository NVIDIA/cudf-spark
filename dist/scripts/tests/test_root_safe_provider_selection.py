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

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile


DIST_DIR = Path(__file__).resolve().parents[2]
STANDARD_ASSEMBLER = DIST_DIR / "build" / "package-parallel-worlds.py"
FAST_ASSEMBLER = DIST_DIR / "scripts" / "build-unshim-parallel-world.py"
BINARY_DEDUPE = DIST_DIR / "scripts" / "binary-dedupe.sh"

SHARED = "org/apache/iceberg/Shared.class"
NEWER_ONLY = "org/apache/iceberg/NewerOnly.class"
OLDER_ONLY = "org/apache/iceberg/OlderOnly.class"
NEWER_IMPL = "org/apache/iceberg/NewerImpl.class"
OLDER_IMPL = "org/apache/iceberg/OlderImpl.class"


def write_jar(path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as jar:
        for name, contents in entries.items():
            jar.writestr(name, contents)


def artifact_path(base_dir, artifact, buildver):
    artifact_id = "rapids-4-spark-%s_2.13" % artifact
    return (base_dir / artifact / "target" / ("spark%s" % buildver) /
            ("%s-1.0-spark%s.jar" % (artifact_id, buildver)))


def create_artifacts(base_dir):
    for buildver in ("353", "413"):
        write_jar(artifact_path(base_dir, "sql-plugin-api", buildver), {})

    write_jar(artifact_path(base_dir, "iceberg-common", "413"), {
        SHARED: b"module-shared-413",
        NEWER_ONLY: b"module-newer-only",
    })
    write_jar(artifact_path(base_dir, "aggregator", "413"), {
        SHARED: b"aggregator-shared-413",
        NEWER_ONLY: b"aggregator-newer-only",
        NEWER_IMPL: b"aggregator-newer-impl",
    })
    write_jar(artifact_path(base_dir, "iceberg-common", "353"), {
        SHARED: b"module-shared-353",
        OLDER_ONLY: b"module-older-only",
    })
    write_jar(artifact_path(base_dir, "aggregator", "353"), {
        SHARED: b"aggregator-shared-353",
        OLDER_ONLY: b"aggregator-older-only",
        OLDER_IMPL: b"aggregator-older-impl",
    })


def read_bytes(root, entry):
    return (root / entry).read_bytes()


class FakeAttributes:
    def get(self, name):
        if name == "artifact_csv":
            return "sql-plugin-api,aggregator"
        raise KeyError(name)


class FakeProject:
    def __init__(self, source_dir, project_dir, target_dir, repository_dir):
        self.properties = {
            "included_buildvers": "353,413",
            "spark.rapids.source.basedir": str(source_dir),
            "spark.rapids.project.basedir": str(project_dir),
            "project.version": "1.0",
            "scala.binary.version": "2.13",
            "project.build.directory": str(target_dir),
            "env.ART_URL": "",
            "maven.local.repository": str(repository_dir),
            "should.build.conventional.jar": False,
        }

    def getProperty(self, name):
        return self.properties.get(name)


def execfile_compat(path, globals_dict):
    with open(path, "rb") as source:
        code = compile(source.read(), str(path), "exec")
    exec(code, globals_dict)


def load_fast_assembler():
    spec = importlib.util.spec_from_file_location("build_unshim_parallel_world", FAST_ASSEMBLER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RootSafeProviderSelectionTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.project_dir = self.root / "project"
        self.source_dir = self.root / "source"
        self.config_dir = self.source_dir / "dist"
        self.config_dir.mkdir(parents=True)
        (self.config_dir / "build").mkdir()
        (self.config_dir / "unshimmed-common-from-single-shim.txt").write_text("")
        (self.config_dir / "unshimmed-from-each-spark3xx.txt").write_text("")
        (self.config_dir / "root-safe-module-classes.txt").write_text("iceberg-common\n")
        (self.config_dir / "keep-in-spark-shared.txt").write_text("")
        (self.config_dir / "keep-in-spark-shim-dirs.txt").write_text(
            "org/apache/iceberg/*.class\n")
        (self.config_dir / "build" / "iceberg_runtime.py").write_text(
            "def coordinates(zip_handle, buildver, scala_version, get_property):\n"
            "    return []\n")
        create_artifacts(self.project_dir)

    def tearDown(self):
        self.temp_dir.cleanup()

    def assemble_standard(self):
        target_dir = self.root / "standard-target"
        (target_dir / "deps").mkdir(parents=True)
        globals_dict = {
            "attributes": FakeAttributes(),
            "project": FakeProject(
                self.source_dir, self.project_dir, target_dir, self.root / "repository"),
            "execfile": execfile_compat,
            "self": self,
        }
        execfile_compat(STANDARD_ASSEMBLER, globals_dict)
        return target_dir

    def assemble_fast(self):
        target_dir = self.root / "fast-target"
        fast = load_fast_assembler()
        fast.copy_and_extract_jars(
            self.project_dir, target_dir, "2.13", "1.0", ["353", "413"],
            [], [], ["iceberg-common"])
        return target_dir

    def assert_provider_selection(self, target_dir):
        parallel_world = target_dir / "parallel-world"
        self.assertEqual(b"aggregator-shared-413", read_bytes(parallel_world, SHARED))
        self.assertEqual(b"aggregator-newer-only", read_bytes(parallel_world, NEWER_ONLY))
        self.assertEqual(b"aggregator-older-only", read_bytes(parallel_world, OLDER_ONLY))
        self.assertFalse((parallel_world / NEWER_IMPL).exists())
        self.assertFalse((parallel_world / OLDER_IMPL).exists())
        self.assertTrue((parallel_world / "spark413" / NEWER_IMPL).is_file())
        self.assertTrue((parallel_world / "spark353" / OLDER_IMPL).is_file())

    def run_dedupe(self, target_dir):
        parallel_world = target_dir / "parallel-world"
        # Root-safe classes must remain binary-compatible across the worlds where they occur.
        # Normalize the synthetic shared class after verifying which provider supplied the root.
        (parallel_world / "spark353" / SHARED).write_bytes(
            read_bytes(parallel_world / "spark413", SHARED))
        env = os.environ.copy()
        env.update({
            "UNSHIM_FAST": "1",
            "UNSHIMMED_COMMON_FROM_SINGLE_SHIM_TXT": str(
                self.config_dir / "unshimmed-common-from-single-shim.txt"),
            "KEEP_IN_SPARK_SHARED_TXT": str(
                self.config_dir / "keep-in-spark-shared.txt"),
            "KEEP_IN_SPARK_SHIM_DIRS_TXT": str(
                self.config_dir / "keep-in-spark-shim-dirs.txt"),
            "UNSHIM_ANALYZER_SCRIPT": str(self.root / "missing-analyzer.py"),
        })
        result = subprocess.run([str(BINARY_DEDUPE)], cwd=target_dir, env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def assert_final_layout(self, target_dir):
        parallel_world = target_dir / "parallel-world"
        for helper in (SHARED, NEWER_ONLY, OLDER_ONLY):
            self.assertTrue((parallel_world / helper).is_file())
            self.assertFalse((parallel_world / "spark413" / helper).exists())
            self.assertFalse((parallel_world / "spark353" / helper).exists())
        self.assertTrue((parallel_world / "spark413" / NEWER_IMPL).is_file())
        self.assertTrue((parallel_world / "spark353" / OLDER_IMPL).is_file())
        self.assertFalse((parallel_world / "spark353" / NEWER_IMPL).exists())
        self.assertFalse((parallel_world / "spark413" / OLDER_IMPL).exists())
        shared_iceberg = parallel_world / "spark-shared" / "org/apache/iceberg"
        self.assertEqual([], list(shared_iceberg.rglob("*.class")))

    def test_provider_selection_and_dedupe_for_both_assemblers(self):
        for name, assemble in (
                ("standard", self.assemble_standard),
                ("fast", self.assemble_fast)):
            with self.subTest(assembler=name):
                target_dir = assemble()
                self.assert_provider_selection(target_dir)
                self.run_dedupe(target_dir)
                self.assert_final_layout(target_dir)


if __name__ == "__main__":
    unittest.main()
