# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
import runpy
import subprocess
import unittest
from unittest.mock import Mock, patch
from xml.etree import ElementTree as ET


helper = runpy.run_path(str(Path(__file__).with_name("dependency-key.py")))


def model(jni="26.12.0-SNAPSHOT", private="26.10.0", scala="2.13"):
    return ET.fromstring(f"""<project xmlns="http://maven.apache.org/POM/4.0.0"><properties>
        <cudf-spark-jni.artifactId>jni</cudf-spark-jni.artifactId>
        <cudf-spark-jni.version>{jni}</cudf-spark-jni.version>
        <cudf-spark-private.artifactId>private_{scala}</cudf-spark-private.artifactId>
        <cudf-spark-private.version>{private}</cudf-spark-private.version>
        </properties><repositories><repository><id>snapshots-repo</id>
        <url>https://example.org/snapshots</url></repository></repositories></project>""")


class DependencyKeyTest(unittest.TestCase):
    def test_restore_target_and_completeness(self):
        script = Path(__file__).with_name("cache-target.sh")
        for full in (False, True):
            for match in ("", "old-premerge-current", "v-verify", "v-premerge-old", "v-premerge-current"):
                with self.subTest(premerge=full, match=match):
                    env = dict(os.environ, STEM="v", FULL="v-premerge-current", MATCH=match,
                               PREMERGE=str(full).lower())
                    result = subprocess.check_output(["bash", str(script)], env=env, text=True)
                    outputs = dict(line.split("=", 1) for line in result.splitlines())
                    expected = "v-premerge-current" if full else (
                        match if match.startswith("v-premerge-") else "v-verify")
                    self.assertEqual(outputs.get("ready") == "true", match == expected)
                    self.assertEqual(outputs["key"], expected)

    def test_pins_do_not_require_snapshot_service(self):
        key = helper["dependency_key"]
        offline = Mock(side_effect=AssertionError("unexpected network request"))
        pin = key(model(jni="26.12.0"), offline)
        self.assertNotEqual(pin, key(model(jni="26.12.1"), offline))
        self.assertNotEqual(pin, key(model(jni="26.12.0", private="26.10.1"), offline))
        self.assertNotEqual(pin, key(model(jni="26.12.0", scala="2.12"), offline))

    def test_each_dependency_tracks_its_own_publication(self):
        key = helper["dependency_key"]
        both = model(private="26.12.0-SNAPSHOT")
        candidates = {"jni": "build-1", "private_2.13": "build-1"}
        resolve = lambda repo, artifact, version: candidates[artifact]
        before = key(both, resolve)
        candidates["private_2.13"] = "build-2"
        after_private = key(both, resolve)
        self.assertNotEqual(before, after_private)
        candidates["jni"] = "build-2"
        self.assertNotEqual(after_private, key(both, resolve))

    def test_optional_history_metrics_snapshot(self):
        pom = model(jni="26.12.0")
        prop = ET.SubElement(pom.find("{*}properties"), "history-metrics.version")
        prop.text = "26.12.0-SNAPSHOT"
        resolver = Mock(side_effect=["history-1", "history-2"])
        key = helper["dependency_key"]
        self.assertNotEqual(key(pom, resolver), key(pom, resolver))
        resolver.assert_called_with("https://example.org/snapshots", "cudf-spark-history-metrics-local",
                                    "26.12.0-SNAPSHOT")

    def test_classifier_publication_not_xml_order_changes_the_key(self):
        def metadata(entries):
            versions = "".join(f"<snapshotVersion><classifier>{classifier}</classifier>"
                               f"<extension>jar</extension><value>{value}</value></snapshotVersion>"
                               for classifier, value in entries)
            return (f"<metadata><groupId>com.nvidia</groupId><artifactId>jni</artifactId>"
                    f"<version>26.12.0-SNAPSHOT</version><versioning><snapshotVersions>{versions}"
                    "</snapshotVersions></versioning></metadata>").encode()

        entries = [("cuda12", "build-1"), ("cuda13", "build-1")]
        with patch("subprocess.check_output") as fetch:
            fetch.return_value = metadata(entries)
            before = helper["dependency_key"](model())
            fetch.return_value = metadata(reversed(entries))
            self.assertEqual(before, helper["dependency_key"](model()))
            fetch.return_value = metadata([("cuda12", "build-2"), entries[1]])
            self.assertNotEqual(before, helper["dependency_key"](model()))
            fetch.return_value = metadata([])
            with self.assertRaisesRegex(ValueError, "No usable snapshot JAR"):
                helper["dependency_key"](model())
