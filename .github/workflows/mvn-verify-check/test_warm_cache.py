# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the Maven Verify cache warmer with the repository's POMs and an offline Maven stub."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


WARMER = Path(__file__).resolve().with_name("populate-daily-cache.sh")
ROOT = WARMER.parents[3]


class MavenCacheWarmupTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.bin = Path(temporary.name)
        self.log = self.bin / "calls"
        self.workspace = self.bin / "workspace"
        helper = Path("integration_tests/run_unity_catalog_server.sh")
        (self.workspace / helper.parent).mkdir(parents=True)
        shutil.copy2(ROOT / helper, self.workspace / helper)
        for path in ["pom.xml", "scala2.13", "build", ".github"]:
            (self.workspace / path).symlink_to(ROOT / path)
        self.executable("mvn", """
printf '%s\\n' "$*" >> "$MAVEN_LOG"
for arg do
    if [ "$arg" = "$FAIL_ARG" ]; then exit 29; fi
    case "$arg" in
        -PpremergeUT1|-PpremergeScala213) printf '%s,\\n    %s\\n' "$FIRST_SELECTED" "$LAST_SELECTED" ;;
        -PpremergeUT2|-PpremergeUTF8) printf '%s\\n' "$LAST_SELECTED" ;;
        -Dexpression=delta42x.version) echo 4.2.0 ;;
        -Dexpression=delta43x.version) echo 4.3.0 ;;
        -Dmdep.outputFile=*) echo fixture.jar > "${arg#*=}" ;;
    esac
done
""")
        self.executable("sleep", "exit 0")
        self.executable("java", 'touch "$MAVEN_LOG.java"; exit 1')

    def executable(self, name, command):
        path = self.bin / name
        path.write_text("#!/bin/sh\n" + command + "\n")
        path.chmod(0o755)

    def build_versions(self, pom):
        output = subprocess.check_output(
            ["python3", "build/get_buildvers.py", "no_snapshots", pom], cwd=ROOT, text=True)
        return output.strip().split(", ")

    def warm(self, scala, versions, fail_arg="", premerge=True):
        self.log.write_text("")
        env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                   MAVEN_LOG=str(self.log), SCALA_VER=scala, FAIL_ARG=fail_arg,
                   TMPDIR=str(self.bin / "server-runtime"),
                   FIRST_SELECTED=versions[1], LAST_SELECTED=versions[-1])
        if premerge:
            env["WARM_PREMERGE"] = "true"
        else:
            env.pop("WARM_PREMERGE", None)
        result = subprocess.run([str(WARMER)], cwd=self.workspace, env=env, capture_output=True, text=True)
        return result, [line.split() for line in self.log.read_text().splitlines()]

    def test_warms_each_scala_pom_and_its_compiler_bridge(self):
        for scala, pom, profiles in [
            ("2.12", "pom.xml", ["premergeUT1", "premergeUT2", "premergeUTF8"]),
            ("2.13", "scala2.13/pom.xml", ["premergeScala213"]),
        ]:
            with self.subTest(scala=scala):
                versions = self.build_versions(pom)
                result, calls = self.warm(scala, versions)
                self.assertEqual(result.returncode, 0, result.stderr)
                reactor = [args for args in calls if "--file" in args]
                self.assertTrue(all(args[args.index("--file") + 1] == pom for args in reactor))
                lookups = [args for args in calls if "-Dexpression=included_buildvers" in args]
                self.assertEqual(len(lookups), len(profiles))
                for args, profile in zip(lookups, profiles):
                    self.assertIn("-P" + profile, args)
                    self.assertIn("-Dexpression=included_buildvers", args)
                resolver = "de.qaware.maven:go-offline-maven-plugin:resolve-dependencies"
                resolves = [args for args in calls if resolver in args]
                copies = [args for args in calls if "dependency:copy@copy" in args]
                self.assertEqual(len(resolves), len(versions))
                for version, args in zip(versions, resolves):
                    self.assertIn("-Dbuildver=" + version, args)
                self.assertEqual(len(copies), 3)
                for version, args in zip([versions[0], versions[1], versions[-1]], copies):
                    self.assertIn("-Dbuildver=" + version, args)
                    self.assertEqual(args[args.index("-pl") + 1], "integration_tests")
                    self.assertIn("dependency:copy@copy-spark-avro", args)
                    self.assertIn("dependency:copy@copy-spark-protobuf", args)
                self.assertIn("process-test-resources", calls[-1])
                classpaths = [args for args in calls if "dependency:build-classpath" in args]
                # The two Delta variants share the same Unity Catalog server dependency graph.
                self.assertEqual(len(classpaths), 3 if scala == "2.13" else 0)
                self.assertFalse((self.bin / "server-runtime").exists())
                self.assertFalse((self.bin / "calls.java").exists())

    def test_default_verify_warms_only_the_existing_shims_and_bridge(self):
        versions = self.build_versions("scala2.13/pom.xml")
        result, calls = self.warm("2.13", versions, premerge=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), len(versions) + 1)
        self.assertTrue(all("de.qaware.maven:go-offline-maven-plugin:resolve-dependencies" in args
                            for args in calls[:-1]))
        self.assertIn("process-test-resources", calls[-1])

    def test_failed_lookup_resolve_or_copy_does_not_report_a_complete_cache(self):
        versions = self.build_versions("pom.xml")
        for fail_arg in ["-PpremergeUT1", "-Dbuildver=" + versions[0], "dependency:copy@copy"]:
            with self.subTest(fail_arg=fail_arg):
                result, calls = self.warm("2.12", versions, fail_arg)
                self.assertEqual(result.returncode, 29)
                self.assertEqual(sum(fail_arg in args for args in calls), 4)
                self.assertFalse(any("process-test-resources" in args for args in calls))

    def test_failed_unity_catalog_resolution_does_not_complete_the_warmup(self):
        versions = self.build_versions("scala2.13/pom.xml")
        result, calls = self.warm("2.13", versions, "dependency:build-classpath")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sum("dependency:build-classpath" in args for args in calls), 4)
        self.assertFalse(any("process-test-resources" in args for args in calls))
        self.assertFalse((self.bin / "server-runtime").exists())
