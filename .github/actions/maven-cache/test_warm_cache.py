# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the Maven Verify cache warmer with the repository's POMs and an offline Maven stub."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


WARMER = Path(__file__).resolve().with_name("populate-cache.sh")
ROOT = WARMER.parents[3]


class MavenCacheWarmupTest(unittest.TestCase):
    def test_incomplete_restore_discards_only_hosted_dependencies(self):
        script = WARMER.with_name("recover-restore.sh")
        with tempfile.TemporaryDirectory() as home:
            m2 = Path(home) / ".m2"
            repo = m2 / "repository"
            repo.mkdir(parents=True)
            (repo / "partial.jar").touch()
            settings = m2 / "settings.xml"
            settings.write_text("keep this file")
            github_env = Path(home) / "github-env"
            subprocess.run(["bash", script, "verify"], env=dict(os.environ, HOME=home,
                           GITHUB_ENV=str(github_env), RUNNER_TEMP=home, RUNNER_ENVIRONMENT="self-hosted",
                           MAVEN_OPTS="-Xmx1g"), check=True)
            opts, path = github_env.read_text().splitlines()
            fresh_repo = Path(path.removeprefix("MAVEN_REPOSITORY="))
            self.assertEqual(opts, f"MAVEN_OPTS=-Xmx1g -Dmaven.repo.local={fresh_repo}")
            self.assertTrue(fresh_repo.is_dir())
            self.assertFalse(any(fresh_repo.iterdir()))
            self.assertTrue((repo / "partial.jar").exists())
            self.executable("rm", "exit 1")
            result = subprocess.run(["bash", script, "verify"], env=dict(os.environ, HOME=home,
                                    PATH=f"{self.bin}:{os.defpath}", GITHUB_ENV=str(github_env),
                                    RUNNER_TEMP=home, RUNNER_ENVIRONMENT="github-hosted"),
                                    check=True, capture_output=True, text=True)
            self.assertIn("Could not reclaim space", result.stdout)
            self.assertTrue((repo / "partial.jar").exists())
            for runner, retained in [("self-hosted", True), ("github-hosted", False)]:
                subprocess.run(["bash", script, "warm"], env=dict(os.environ, HOME=home,
                               RUNNER_ENVIRONMENT=runner), check=True)
                self.assertEqual(repo.exists(), retained)
                self.assertEqual(settings.read_text(), "keep this file")

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.bin = Path(temporary.name)
        self.log = self.bin / "calls"
        self.scratch = self.bin / "tmp"
        self.scratch.mkdir()
        self.workspace = self.bin / "workspace"
        helper = Path("integration_tests/run_unity_catalog_server.sh")
        (self.workspace / helper.parent).mkdir(parents=True)
        shutil.copy2(ROOT / helper, self.workspace / helper)
        for path in ["pom.xml", "scala2.13", "build", ".github"]:
            (self.workspace / path).symlink_to(ROOT / path)
        self.executable("mvn", """
printf '%s\\n' "$*" >> "$MAVEN_LOG"
for arg do
    if [ "$arg" = wrapper:wrapper ]; then
        test -f pom.xml || exit 10
        pwd >> "$MAVEN_LOG.wrapper"
        touch mvnw
    fi
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
                   TMPDIR=str(self.scratch),
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
                self.assertEqual(list(self.scratch.iterdir()), [])
                self.assertFalse((self.bin / "calls.java").exists())

    def test_verify_and_premerge_prime_their_cli_plugins(self):
        for scala, pom in [("2.12", "pom.xml"), ("2.13", "scala2.13/pom.xml")]:
            versions = self.build_versions(pom)
            for premerge in [False, True]:
                with self.subTest(scala=scala, premerge=premerge):
                    result, calls = self.warm(scala, versions, premerge=premerge)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    plugins = calls[0]
                    self.assertIn("-N", plugins)
                    self.assertIn("help:evaluate", plugins)
                    self.assert_pinned_plugin(plugins, "maven-dependency-plugin",
                                              "dist/build/package-parallel-worlds.py", "get")
                    self.assertEqual("wrapper:wrapper" in plugins, scala == "2.12")
                    self.assertEqual("source:help" in plugins, scala == "2.13")
                    if scala == "2.13":
                        self.assert_pinned_plugin(plugins, "maven-help-plugin",
                                                  "scripts/get_iceberg_versions.py", "effective-pom")
                    if scala == "2.12":
                        generated_in = Path(self.log.with_suffix(".wrapper").read_text().splitlines()[-1])
                        self.assertEqual(generated_in.parent.resolve(), self.scratch.resolve())
                        self.assertFalse(generated_in.exists())
                    if not premerge:
                        self.assertEqual(len(calls), len(versions) + 2)
                        self.assertTrue(all("de.qaware.maven:go-offline-maven-plugin:resolve-dependencies" in args
                                            for args in calls[1:-1]))
                        self.assertIn("process-test-resources", calls[-1])

    def assert_pinned_plugin(self, goals, artifact, consumer, consumer_goal):
        prefix = f"org.apache.maven.plugins:{artifact}:"
        matches = [goal for goal in goals if goal.startswith(prefix) and goal.endswith(":help")]
        self.assertEqual(len(matches), 1, matches)
        coordinate = matches[0].rsplit(":", 1)[0]
        self.assertIn(f"{coordinate}:{consumer_goal}", (ROOT / consumer).read_text())

    def test_failed_lookup_resolve_or_copy_does_not_report_a_complete_cache(self):
        versions = self.build_versions("pom.xml")
        for fail_arg in ["wrapper:wrapper", "-PpremergeUT1", "-Dbuildver=" + versions[0],
                         "dependency:copy@copy"]:
            with self.subTest(fail_arg=fail_arg):
                result, calls = self.warm("2.12", versions, fail_arg)
                self.assertEqual(result.returncode, 29)
                self.assertEqual(sum(fail_arg in args for args in calls), 4)
                self.assertFalse(any("process-test-resources" in args for args in calls))
                self.assertEqual(list(self.scratch.iterdir()), [])

    def test_failed_unity_catalog_resolution_does_not_complete_the_warmup(self):
        versions = self.build_versions("scala2.13/pom.xml")
        result, calls = self.warm("2.13", versions, "dependency:build-classpath")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sum("dependency:build-classpath" in args for args in calls), 4)
        self.assertFalse(any("process-test-resources" in args for args in calls))
        self.assertEqual(list(self.scratch.iterdir()), [])
