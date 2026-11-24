# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path
import runpy
import subprocess
import tempfile
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


def metadata(entries):
    versions = "".join(f"<snapshotVersion><classifier>{classifier}</classifier>"
                       f"<extension>jar</extension><value>{value}</value></snapshotVersion>"
                       for classifier, value in entries)
    return (f"<metadata><groupId>com.nvidia</groupId><artifactId>jni</artifactId>"
            f"<version>26.12.0-SNAPSHOT</version><versioning><snapshotVersions>{versions}"
            "</snapshotVersions></versioning></metadata>").encode()


class DependencyKeyTest(unittest.TestCase):
    def test_release_branch_cache_keys(self):
        script = Path(__file__).with_name("cache-keys.sh")
        for branch, scope in (("main", "main"), ("release/26.12", "26.12"),
                              ("release/26.12-rc", "release/26.12-rc")):
            with self.subTest(branch=branch):
                env = dict(os.environ, BRANCH=branch, RUNNER_OS="Linux", RUNNER_ARCH="X64",
                           SCALA_VER="2.12", INPUT_HASH="inputs", PREMERGE_HASH="fixture")
                output = subprocess.check_output(["bash", str(script), "upstream"],
                                                 cwd=script.parents[3], env=env, text=True)
                keys = dict(row.split("=", 1) for row in output.splitlines())
                self.assertEqual(keys["prefix"], f"spark-maven-Linux-X64-2.12-{scope}-")
                self.assertEqual(keys["snapshot-prefix"], keys["prefix"] + "upstream-")
                self.assertEqual(keys["stem"], keys["snapshot-prefix"] + "inputs")
                self.assertTrue(keys["full"].startswith(keys["stem"] + "-premerge-"))

    def test_cleanup_from_fingerprinted_model_only_removes_tracked_snapshots(self):
        pom = model(private="26.10.0")
        ET.SubElement(pom.find("{*}properties"), "history-metrics.version").text = "1-SNAPSHOT"
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            group = home / ".m2/repository/com/nvidia"
            stale = ["jni/26.12.0-SNAPSHOT", "cudf-spark-history-metrics-local/1-SNAPSHOT"]
            keep = ["jni/26.10.0-SNAPSHOT", "private_2.13/26.10.0", "unrelated/1-SNAPSHOT"]
            for item in stale + keep:
                directory = group / item
                directory.mkdir(parents=True)
                (directory / "artifact.jar").touch()
            effective = home / "effective.xml"
            ET.ElementTree(pom).write(effective)
            script = Path(__file__).with_name("dependency-key.py")
            command = ["python3", "-B", script, "--discard-restored", effective]
            refused = subprocess.run(command, env=dict(os.environ, HOME=tmp, RUNNER_ENVIRONMENT="self-hosted"),
                                     text=True, capture_output=True)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn("non-disposable", refused.stderr)
            self.assertTrue(all((group / item / "artifact.jar").is_file() for item in stale))
            subprocess.run(command, env=dict(os.environ, HOME=tmp, RUNNER_ENVIRONMENT="github-hosted"), check=True)
            self.assertFalse(any((group / item).exists() for item in stale))
            self.assertTrue(all((group / item / "artifact.jar").is_file() for item in keep))

    def test_cleanup_refuses_path_components_and_symlinked_cache_paths(self):
        cleanup = helper["discard_restored_snapshots"]
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            for name, value in (("cudf-spark-jni.artifactId", "../outside"),
                                ("cudf-spark-jni.version", "/outside/1-SNAPSHOT")):
                pom = model()
                pom.find(f"{{*}}properties/{{*}}{name}").text = value
                with self.subTest(value=value), self.assertRaisesRegex(ValueError, "Invalid upstream"):
                    cleanup(pom, home)
            outside = home / "outside"
            version = outside / "jni/26.12.0-SNAPSHOT"
            version.mkdir(parents=True)
            marker = version / "do-not-delete"
            marker.touch()
            group = home / ".m2/repository/com/nvidia"
            group.parent.mkdir(parents=True)
            group.symlink_to(outside, target_is_directory=True)
            current = helper["dependency_key"](model(), lambda *_: "unchanged")
            matching_key = "scope-" + current + "-" + "a" * 64 + "-verify"
            with self.assertRaisesRegex(ValueError, "symlinked Maven path"):
                cleanup(model(), home, current, "scope-", matching_key)
            self.assertTrue(marker.exists())

    def test_restore_keeps_only_individually_verified_snapshots(self):
        pom = model(private="26.12.0-SNAPSHOT")
        ET.SubElement(pom.find("{*}properties"), "history-metrics.version").text = "26.12.0"
        publications = {"jni": "jni-1", "private_2.13": "private-1"}
        def key():
            return helper["dependency_key"](pom, lambda repo, artifact, version: publications[artifact])

        before = key()
        publications["private_2.13"] = "private-2"
        after = key()
        prefix = "spark-maven-Linux-X64-2.13-26.12-"
        stem = prefix + before + "-" + "a" * 64
        full = stem + "-premerge-4.1.2-" + "b" * 64
        self.assertLessEqual(len(full), 512)
        for restored, keep_jni in [(full, True), (stem + "-verify", True), ("", False),
                                   (prefix + "c" * 64 + "-" + "a" * 64 + "-verify", False),
                                   (stem + "-malformed", False), ("wrong-" + full, False)]:
            with self.subTest(key=restored), tempfile.TemporaryDirectory() as tmp:
                home = Path(tmp)
                group = home / ".m2/repository/com/nvidia"
                jni = group / "jni/26.12.0-SNAPSHOT/cuda12.jar"
                private = group / "private_2.13/26.12.0-SNAPSHOT/spark405.jar"
                pinned = group / "cudf-spark-history-metrics-local/26.12.0/fixed.jar"
                for jar in (jni, private, pinned):
                    jar.parent.mkdir(parents=True)
                    jar.write_bytes(b"original JAR")
                with patch.dict(os.environ, RUNNER_ENVIRONMENT="github-hosted"):
                    helper["discard_restored_snapshots"](pom, home, after, prefix, restored)
                if keep_jni:
                    self.assertEqual(jni.read_bytes(), b"original JAR")
                else:
                    self.assertFalse(jni.exists())
                self.assertFalse(private.parent.exists())
                self.assertEqual(pinned.read_bytes(), b"original JAR")

    def test_restore_target_and_completeness(self):
        script = Path(__file__).with_name("cache-target.sh")
        verify = "v-verify"
        current = "v-premerge-current"
        previous_fixture = "v-premerge-old"
        for full, usable in ((False, {verify, previous_fixture, current}), (True, {current})):
            for match in ("", "old-verify", "old-premerge-current", "v-verify-extra", "v-premerge-",
                          verify, previous_fixture, current):
                with self.subTest(premerge=full, match=match):
                    env = dict(os.environ, STEM="v", FULL=current, MATCH=match,
                               PREMERGE=str(full).lower())
                    result = subprocess.check_output(["bash", str(script)], env=env, text=True)
                    outputs = dict(line.split("=", 1) for line in result.splitlines())
                    self.assertEqual(outputs.get("ready") == "true", match in usable)
                    self.assertEqual(outputs["key"], match if match in usable else (current if full else verify))

    def test_only_an_exact_post_save_lookup_exposes_a_key(self):
        script = Path(__file__).with_name("cache-target.sh")
        for premerge in ("false", "true"):
            for match in ("", "v-verify-older", "v-verify"):
                with self.subTest(premerge=premerge, match=match):
                    env = dict(os.environ, PREMERGE=premerge, TARGET="v-verify", MATCH=match)
                    result = subprocess.run(["bash", script, "--after-save"], env=env,
                                            text=True, capture_output=True)
                    if match == env["TARGET"]:
                        self.assertEqual(result.returncode, 0)
                        self.assertEqual(result.stdout.strip(), "key=v-verify")
                    else:
                        self.assertEqual(result.stdout, "")
                        self.assertEqual(result.returncode, 1 if premerge == "true" else 0)
                        self.assertIn("::error::" if premerge == "true" else "::warning::", result.stderr)

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

    def test_confirm_before_save_rechecks_live_classifier_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            pom = model()
            pom.find(".//{*}repository/{*}url").text = repo.as_uri()
            effective = repo / "effective.xml"
            ET.ElementTree(pom).write(effective)
            published = repo / "com/nvidia/jni/26.12.0-SNAPSHOT/maven-metadata.xml"
            published.parent.mkdir(parents=True)
            published.write_bytes(metadata([("cuda12", "build-1"), ("cuda13", "build-1")]))
            original = helper["dependency_key"](pom)
            cmd = ["python3", "-B", Path(__file__).with_name("dependency-key.py"),
                   "--confirm-snapshots", effective, original]
            subprocess.run(cmd, check=True)
            published.write_bytes(metadata([("cuda12", "build-1"), ("cuda13", "build-2")]))
            changed = subprocess.run(cmd, text=True, capture_output=True)
            self.assertNotEqual(changed.returncode, 0)
            self.assertIn("changed during warm-up", changed.stderr)
            published.unlink()
            self.assertNotEqual(subprocess.run(cmd, capture_output=True).returncode, 0)
