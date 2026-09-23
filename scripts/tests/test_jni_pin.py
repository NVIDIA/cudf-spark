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
import tempfile
import unittest


MODULE_PATH = Path(__file__).parents[2] / "jenkins" / "jni_pin.py"
SPEC = importlib.util.spec_from_file_location("jni_pin", MODULE_PATH)
jni_pin = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(jni_pin)


JNI_SHA = "a33da84b12ef0000000000000000000000000000"
CUDF_SHA = "c101000000000000000000000000000000000000"
CANDIDATE = "26.12.0-jni.a33da84b12ef.r1-SNAPSHOT"
RESOLVED = "26.12.0-jni.a33da84b12ef.r1-20260922.010203-1"


def write_pom(path, version):
    path.write_text(
        "<project xmlns=\"http://maven.apache.org/POM/4.0.0\">"
        f"<properties><cudf-spark-jni.version>{version}</cudf-spark-jni.version>"
        "</properties></project>"
    )


def manifest(checksum="a" * 64):
    coordinates = [
        ("jar", None),
        ("jar", "cuda12"),
        ("jar", "sources"),
        ("jar", "javadoc"),
        ("pom", None),
    ]
    artifacts = []
    for extension, classifier in coordinates:
        suffix = f"-{classifier}" if classifier else ""
        artifacts.append(
            {
                "publishedName": f"cudf-spark-jni-{CANDIDATE}{suffix}.{extension}",
                "extension": extension,
                "classifier": classifier,
                "sha256": checksum,
            }
        )
    repository_artifacts = [
        {
            "publishedName": artifact["publishedName"],
            "resolvedName": artifact["publishedName"].replace(CANDIDATE, RESOLVED),
            "sha256": artifact["sha256"],
        }
        for artifact in artifacts
    ]
    repository = {"state": "COMPLETE", "artifacts": repository_artifacts}
    return {
        "schemaVersion": 1,
        "candidateId": CANDIDATE,
        "artifactRevision": 1,
        "source": {"jniCommit": JNI_SHA, "cudfCommit": CUDF_SHA},
        "requiredClassifiers": ["cuda12"],
        "defaultClassifier": "cuda12",
        "artifacts": artifacts,
        "repositories": {"sonatype": repository, "urm": repository},
        "integrity": {"state": "VERIFIED"},
    }


class JniPinTest(unittest.TestCase):
    def test_repository_readback_matches_metadata_and_bytes(self):
        value = manifest(jni_pin.sha256(b"artifact"))
        metadata_entries = []
        for artifact in value["artifacts"]:
            classifier = artifact["classifier"]
            classifier_xml = (
                f"<classifier>{classifier}</classifier>" if classifier else ""
            )
            metadata_entries.append(
                "<snapshotVersion>"
                f"<extension>{artifact['extension']}</extension>{classifier_xml}"
                f"<value>{RESOLVED}</value>"
                "</snapshotVersion>"
            )
        metadata = (
            "<metadata><versioning><snapshotVersions>"
            + "".join(metadata_entries)
            + "</snapshotVersions></versioning></metadata>"
        ).encode()

        def fetch(url):
            return metadata if url.endswith("maven-metadata.xml") else b"artifact"

        self.assertEqual(
            5,
            jni_pin.verify_repository(
                value, "sonatype", "https://repo.example/snapshots", fetch, 1, 0
            ),
        )

    def test_repository_readback_rejects_moved_metadata(self):
        value = manifest(jni_pin.sha256(b"artifact"))
        metadata = (
            "<metadata><versioning><snapshotVersions><snapshotVersion>"
            "<extension>jar</extension><value>different</value>"
            "</snapshotVersion></snapshotVersions></versioning></metadata>"
        ).encode()
        with self.assertRaisesRegex(ValueError, "metadata moved"):
            jni_pin.verify_repository(
                value,
                "urm",
                "https://repo.example/snapshots",
                lambda url: metadata if url.endswith("maven-metadata.xml") else b"artifact",
                1,
                0,
            )

    def test_current_moving_snapshot_needs_no_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            pom = Path(directory) / "pom.xml"
            write_pom(pom, "26.10.0-SNAPSHOT")
            self.assertEqual(
                "26.10.0-SNAPSHOT",
                jni_pin.check([pom], Path(directory) / "missing.json"),
            )

    def test_update_writes_both_poms_and_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            poms = [root / "pom.xml", root / "scala2.13-pom.xml"]
            for pom in poms:
                write_pom(pom, "26.10.0-SNAPSHOT")
            source = root / "promotion.json"
            source.write_text(json.dumps(manifest()))
            provenance = root / "jni-candidate-manifest.json"

            self.assertEqual(CANDIDATE, jni_pin.update(poms, source, provenance))
            self.assertTrue(provenance.is_file())
            self.assertEqual({CANDIDATE}, {jni_pin.pom_pin(pom) for pom in poms})

    def test_rejects_repository_mismatch(self):
        value = manifest()
        value["repositories"]["urm"] = {
            "state": "COMPLETE",
            "artifacts": [dict(artifact) for artifact in value["repositories"]["urm"]["artifacts"]],
        }
        value["repositories"]["urm"]["artifacts"][0]["sha256"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "checksums do not match"):
            jni_pin.validate_manifest(value)

    def test_candidate_pin_requires_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            pom = Path(directory) / "pom.xml"
            write_pom(pom, CANDIDATE)
            with self.assertRaisesRegex(ValueError, "requires a promotion manifest"):
                jni_pin.check([pom], Path(directory) / "missing.json")


if __name__ == "__main__":
    unittest.main()
