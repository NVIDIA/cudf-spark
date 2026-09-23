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
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile


MODULE_PATH = Path(__file__).parents[2] / "jenkins" / "jni_dependency.py"
SPEC = importlib.util.spec_from_file_location("jni_dependency", MODULE_PATH)
jni_dependency = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(jni_dependency)


JNI_SHA = "a" * 40
CUDF_SHA = "b" * 40
VERSION = "26.10.0-SNAPSHOT"
RESOLVED = "26.10.0-20260923.010203-17"
FINGERPRINT = "c" * 64


def metadata(entries):
    items = []
    for extension, classifier in entries:
        classifier_xml = f"<classifier>{classifier}</classifier>" if classifier else ""
        items.append(
            "<snapshotVersion>"
            f"<extension>{extension}</extension>{classifier_xml}"
            f"<value>{RESOLVED}</value>"
            "</snapshotVersion>"
        )
    return (
        "<metadata><versioning><snapshotVersions>"
        + "".join(items)
        + "</snapshotVersions></versioning></metadata>"
    ).encode()


def provenance(jni_sha=JNI_SHA):
    return {
        "schemaVersion": 1,
        "logicalVersion": VERSION,
        "source": {"jniSha": jni_sha, "cudfSha": CUDF_SHA},
        "build": {"fingerprint": FINGERPRINT, "inputs": {}},
        "artifacts": [],
    }


class JniDependencyTest(unittest.TestCase):
    def test_reads_gitlink_and_simple_snapshot_version(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", root], check=True)
            subprocess.run(
                ["git", "-C", root, "update-index", "--add", "--cacheinfo", "160000", JNI_SHA,
                 "thirdparty/cudf-spark-jni"],
                check=True,
            )
            pom = root / "pom.xml"
            pom.write_text(
                "<project><properties><cudf-spark-jni.version>"
                f"{VERSION}</cudf-spark-jni.version></properties></project>"
            )
            state = jni_dependency.dependency_state(
                root, "thirdparty/cudf-spark-jni", [pom]
            )
            self.assertEqual(JNI_SHA, state["jniSha"])
            self.assertEqual(VERSION, state["logicalVersion"])

    def test_rejects_commit_qualified_pom_version(self):
        with tempfile.TemporaryDirectory() as directory:
            pom = Path(directory) / "pom.xml"
            pom.write_text(
                "<project><properties><cudf-spark-jni.version>"
                "26.10.0-jni.a33da84b12ef.r1-SNAPSHOT"
                "</cudf-spark-jni.version></properties></project>"
            )
            with self.assertRaisesRegex(ValueError, "moving SNAPSHOT"):
                jni_dependency.dependency_state(
                    Path(directory), "thirdparty/cudf-spark-jni", [pom]
                )

    def test_resolves_and_validates_repository_provenance(self):
        expected = {"jniSha": JNI_SHA, "logicalVersion": VERSION}

        def fetch(url):
            if url.endswith("maven-metadata.xml"):
                return metadata([("json", "provenance")])
            self.assertTrue(url.endswith(f"-{RESOLVED}-provenance.json"))
            return json.dumps(provenance()).encode()

        actual = jni_dependency.repository_provenance(
            "https://repo.example/snapshots", expected, fetch, 1, 0
        )
        self.assertEqual(JNI_SHA, actual["source"]["jniSha"])

    def test_rejects_repository_for_different_submodule(self):
        expected = {"jniSha": JNI_SHA, "logicalVersion": VERSION}

        def fetch(url):
            if url.endswith("maven-metadata.xml"):
                return metadata([("json", "provenance")])
            return json.dumps(provenance("d" * 40)).encode()

        with self.assertRaisesRegex(ValueError, "revision mismatch"):
            jni_dependency.repository_provenance(
                "https://repo.example/snapshots", expected, fetch, 1, 0
            )

    def test_reads_embedded_jar_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            jar = Path(directory) / "jni.jar"
            with zipfile.ZipFile(jar, "w") as archive:
                archive.writestr(
                    jni_dependency.PROPERTIES_PATH,
                    f"version={VERSION}\nrevision={JNI_SHA}\n",
                )
            self.assertEqual(JNI_SHA, jni_dependency.jar_revision(jar))


if __name__ == "__main__":
    unittest.main()
