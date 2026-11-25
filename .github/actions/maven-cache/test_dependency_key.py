# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
from xml.etree import ElementTree as ET


RELEASE_LINE = "00.12"
SNAPSHOT_VERSION = "2.0.0-SNAPSHOT"

helper = runpy.run_path(str(Path(__file__).with_name("dependency-key.py")))


def packaged_history_modules(pom_path="dist/pom.xml"):
    pom = ET.parse(Path(__file__).resolve().parents[3] / pom_path)
    query = ".//execution[id='unpack-history-metrics-at-root']/configuration/includeArtifactIds"
    artifacts = pom.findtext(query, namespaces={"": "http://maven.apache.org/POM/4.0.0"})
    if not artifacts:
        raise ValueError(f"Missing includeArtifactIds for unpack-history-metrics-at-root in {pom_path}")
    names = (name.strip() for name in artifacts.split(","))
    return [name for name in names if name.startswith("cudf-spark-history-metrics-")]


def model(jni=SNAPSHOT_VERSION, private="1.0.0", scala="2.13"):
    return ET.fromstring(f"""<project xmlns="http://maven.apache.org/POM/4.0.0"><properties>
        <cudf-spark-jni.artifactId>jni</cudf-spark-jni.artifactId>
        <cudf-spark-jni.version>{jni}</cudf-spark-jni.version>
        <cudf-spark-private.artifactId>private_{scala}</cudf-spark-private.artifactId>
        <cudf-spark-private.version>{private}</cudf-spark-private.version>
        </properties><repositories><repository><id>snapshots-repo</id>
        <url>https://example.org/snapshots</url></repository></repositories></project>""")


def add_history(pom, version, modules):
    ET.SubElement(pom.find("{*}properties"), "history-metrics.version").text = version
    pom.append(ET.fromstring(f"""<build xmlns="http://maven.apache.org/POM/4.0.0"><plugins><plugin>
        <groupId>org.apache.maven.plugins</groupId><artifactId>maven-dependency-plugin</artifactId>
        <executions><execution><id>unpack-history-metrics-at-root</id>
        <configuration><includeArtifactIds>{",".join(modules)}</includeArtifactIds></configuration>
        </execution></executions></plugin></plugins></build>"""))


def metadata(entries):
    versions = "".join(f"<snapshotVersion><classifier>{classifier}</classifier>"
                       f"<extension>jar</extension><value>{value}</value></snapshotVersion>"
                       for classifier, value in entries)
    return (f"<metadata><groupId>com.nvidia</groupId><artifactId>jni</artifactId>"
            f"<version>{SNAPSHOT_VERSION}</version><versioning><snapshotVersions>{versions}"
            "</snapshotVersions></versioning></metadata>").encode()


class DependencyKeyTest(unittest.TestCase):
    def test_release_branch_cache_keys(self):
        script = Path(__file__).with_name("cache-keys.sh")
        release_branch = f"release/{RELEASE_LINE}"
        rc_branch = f"{release_branch}-rc"
        for branch, scope in (("main", "main"), (release_branch, RELEASE_LINE), (rc_branch, rc_branch)):
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
        pom = model()
        add_history(pom, "1-SNAPSHOT", packaged_history_modules())
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            group = home / ".m2/repository/com/nvidia"
            stale = [f"jni/{SNAPSHOT_VERSION}", "cudf-spark-history-metrics-local/1-SNAPSHOT"]
            keep = ["jni/1.0.0-SNAPSHOT", "private_2.13/1.0.0", "unrelated/1-SNAPSHOT"]
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
            version = outside / "jni" / SNAPSHOT_VERSION
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
        pom = model(private=SNAPSHOT_VERSION)
        add_history(pom, "2.0.0", packaged_history_modules())
        publications = {"jni": "jni-1", "private_2.13": "private-1"}
        def key():
            return helper["dependency_key"](pom, lambda repo, artifact, version: publications[artifact])

        before = key()
        publications["private_2.13"] = "private-2"
        after = key()
        prefix = f"spark-maven-Linux-X64-2.13-{RELEASE_LINE}-"
        stem = prefix + before + "-" + "a" * 64
        full = stem + "-premerge-4.1.2-" + "b" * 64
        self.assertLessEqual(len(full), 512)
        for restored, keep_jni in [(full, True), (stem + "-verify", True), ("", False),
                                   (prefix + "c" * 64 + "-" + "a" * 64 + "-verify", False),
                                   (stem + "-malformed", False), ("wrong-" + full, False)]:
            with self.subTest(key=restored), tempfile.TemporaryDirectory() as tmp:
                home = Path(tmp)
                group = home / ".m2/repository/com/nvidia"
                jni = group / "jni" / SNAPSHOT_VERSION / "cuda12.jar"
                private = group / "private_2.13" / SNAPSHOT_VERSION / "spark405.jar"
                pinned = group / "cudf-spark-history-metrics-local/2.0.0/fixed.jar"
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
        pin = key(model(jni="2.0.0"), offline)
        self.assertNotEqual(pin, key(model(jni="2.0.1"), offline))
        self.assertNotEqual(pin, key(model(jni="2.0.0", private="1.0.1"), offline))
        self.assertNotEqual(pin, key(model(jni="2.0.0", scala="2.12"), offline))

    def test_each_dependency_tracks_its_own_publication(self):
        key = helper["dependency_key"]
        both = model(private=SNAPSHOT_VERSION)
        candidates = {"jni": "build-1", "private_2.13": "build-1"}
        resolve = lambda repo, artifact, version: candidates[artifact]
        before = key(both, resolve)
        candidates["private_2.13"] = "build-2"
        after_private = key(both, resolve)
        self.assertNotEqual(before, after_private)
        candidates["jni"] = "build-2"
        self.assertNotEqual(after_private, key(both, resolve))

    def test_each_history_module_changes_only_the_history_slot(self):
        history_modules = packaged_history_modules()
        self.assertCountEqual(history_modules, packaged_history_modules("scala2.13/dist/pom.xml"))
        pom = model(private=SNAPSHOT_VERSION)
        key = helper["dependency_key"]
        no_history = key(pom, lambda *_: "build-1")
        packaged = model(private=SNAPSHOT_VERSION)
        add_history(packaged, SNAPSHOT_VERSION, history_modules)
        original = key(packaged, lambda *_: "build-1")
        # A new module in the packaging input must be tracked without editing the cache helper.
        history_modules.append("cudf-spark-history-metrics-new-module")
        add_history(pom, SNAPSHOT_VERSION, [*history_modules, "sqlite-jdbc"])
        publications = dict.fromkeys(("jni", "private_2.13", *history_modules), "build-1")
        resolve = Mock(side_effect=lambda repo, artifact, version: publications[artifact])
        baseline = key(pom, resolve).split(".")
        self.assertEqual(baseline[:-1], no_history.split("."))
        self.assertRegex(baseline[-1], r"^[0-9a-f]{64}$")
        self.assertNotEqual(baseline[-1], original.split(".")[-1])
        initial_requests = [call.args for call in resolve.call_args_list]
        for module in history_modules:
            with self.subTest(module=module), patch.dict(publications, {module: "build-2"}):
                changed = key(pom, resolve).split(".")
                self.assertEqual(changed[:-1], baseline[:-1])
                self.assertNotEqual(changed[-1], baseline[-1])
        self.assertCountEqual(initial_requests, [("https://example.org/snapshots", name, SNAPSHOT_VERSION)
                                                for name in publications])

    def test_history_version_requires_packaged_history_artifacts(self):
        for missing_execution, artifacts in [(True, []), (False, []), (False, ["sqlite-jdbc"])]:
            with self.subTest(missing_execution=missing_execution, artifacts=artifacts):
                pom = model(jni="2.0.0")
                add_history(pom, SNAPSHOT_VERSION, artifacts)
                if missing_execution:
                    pom.remove(pom.find("{*}build"))
                with self.assertRaises(ValueError):
                    helper["dependency_key"](pom, Mock(side_effect=AssertionError("unexpected fetch")))

    def test_restore_refreshes_the_history_group_and_keeps_other_snapshots(self):
        history_modules = packaged_history_modules()
        pom = model(private=SNAPSHOT_VERSION)
        add_history(pom, SNAPSHOT_VERSION, history_modules)
        key = helper["dependency_key"]
        before = key(pom, lambda *_: "build-1")
        updated = {"cudf-spark-history-metrics-provider-runtime": "build-2"}
        after = key(pom, lambda repo, artifact, version: updated.get(artifact, "build-1"))
        # Previous s1 keys stored one coordinate per slot, with only local identifying history.
        legacy_slots = ["s1"]
        for name in ("jni", "private_2.13", "cudf-spark-history-metrics-local"):
            publication = [name, SNAPSHOT_VERSION, "https://example.org/snapshots", "build-1"]
            legacy_slots.append(hashlib.sha256(json.dumps(publication).encode()).hexdigest())
        legacy = ".".join(legacy_slots)
        prefix = "spark-maven-Linux-X64-2.13-main-"
        cases = [(before, before, True), (before, after, False), (legacy, before, False)]
        for restored, current, keep_history in cases:
            with self.subTest(restored=restored, current=current), tempfile.TemporaryDirectory() as tmp:
                home = Path(tmp)
                group = home / ".m2/repository/com/nvidia"
                jars = {}
                for name in ("jni", "private_2.13", *history_modules):
                    jar = group / name / SNAPSHOT_VERSION / "artifact.jar"
                    jar.parent.mkdir(parents=True)
                    jar.write_bytes(b"cached JAR")
                    jars[name] = jar
                restored_key = f"{prefix}{restored}-{'a' * 64}-verify"
                with patch.dict(os.environ, RUNNER_ENVIRONMENT="github-hosted"):
                    helper["discard_restored_snapshots"](pom, home, current, prefix, restored_key)
                for name, jar in jars.items():
                    if name in history_modules and not keep_history:
                        self.assertFalse(jar.parent.exists(), name)
                    else:
                        self.assertEqual(jar.read_bytes(), b"cached JAR", name)

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
            published = repo / "com/nvidia/jni" / SNAPSHOT_VERSION / "maven-metadata.xml"
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
