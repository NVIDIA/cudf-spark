# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import runtimes
import workload


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], env={
        **workload.CLEAN_ENV, "GIT_AUTHOR_NAME": "Fixture", "GIT_AUTHOR_EMAIL": "test@example.invalid",
        "GIT_COMMITTER_NAME": "Fixture", "GIT_COMMITTER_EMAIL": "test@example.invalid",
    }, stderr=subprocess.DEVNULL, text=True).strip()


class WorkloadTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name).resolve()

    def test_only_pinned_blobs_are_archived(self):
        repo = self.tmp / "candidate"
        repo.mkdir()

        git(repo, "init", "-q")
        for name in workload.SUBMODULES:
            sub = repo / name
            sub.mkdir(parents=True)
            git(sub, "init", "-q")
            (sub / "tracked").write_text("pinned")
            git(sub, "add", "tracked")
            git(sub, "commit", "-qm", "fixture")
            git(repo, "update-index", "--add", "--cacheinfo", f"160000,{git(sub, 'rev-parse', 'HEAD')},{name}")
            (sub / "tracked").write_text("dirty")
            (sub / "extra").write_text("do not upload")
        (repo / "source").write_text("approved")
        (repo / ".gitattributes").write_text("source export-ignore\n")
        git(repo, "add", "source", ".gitattributes")
        git(repo, "commit", "-qm", "fixture")
        sha = git(repo, "rev-parse", "HEAD")
        (repo / "source").write_text("dirty")
        (repo / "fake-secret").write_text("do not upload")
        archive = self.tmp / "source.tgz"
        workload.make_archive(repo, sha, archive)
        with tarfile.open(archive) as tar:
            self.assertEqual(set(tar.getnames()),
                             {"source", ".gitattributes"} | {name + "/tracked" for name in workload.SUBMODULES})
            self.assertEqual(tar.extractfile("source").read(), b"approved")
            for name in workload.SUBMODULES:
                self.assertEqual(tar.extractfile(name + "/tracked").read(), b"pinned")
        archive.unlink()
        with self.assertRaises(ValueError):
            workload.make_archive(repo, "0" * 40, archive)
        archive.unlink()
        (repo / "linked").symlink_to("../outside")
        git(repo, "add", "linked")
        git(repo, "commit", "-qm", "unsafe fixture")
        with self.assertRaisesRegex(ValueError, "link"):
            workload.make_archive(repo, git(repo, "rev-parse", "HEAD"), archive)
        archive.unlink()
        git(repo, "rm", "linked")
        git(repo, "commit", "-qm", "separate fixture")
        git(sub, "commit", "-qam", "wrong submodule revision")
        with self.assertRaisesRegex(ValueError, "pinned revision"):
            workload.make_archive(repo, git(repo, "rev-parse", "HEAD"), archive)

    def test_runtime_metadata_comes_from_enabled_profiles_and_pinned_provider_blobs(self):
        git(self.tmp, "init", "-q")
        pom = self.tmp / "scala2.13/pom.xml"
        pom.parent.mkdir()
        pom.write_text("""<project><properties>
          <spark421db1910.version>4.2.1-databricks-1910</spark421db1910.version>
          <spark410db183.version>4.1.0-databricks-183</spark410db183.version>
          <spark500db201.version>5.0.0-databricks-201-SNAPSHOT</spark500db201.version>
          <dyn.shim.excluded.releases>410db183</dyn.shim.excluded.releases>
          </properties><profiles><profile><id>release421db1910</id>
          <properties><spark.version>${spark421db1910.version}</spark.version></properties></profile>
          <profile><id>release410db183</id></profile><profile><id>release500db201</id></profile>
          </profiles></project>""")
        provider = self.tmp / ("sql-plugin/src/main/spark421db1910/scala/com/nvidia/spark/rapids/"
                               "shims/spark421db1910/SparkShimServiceProvider.scala")
        provider.parent.mkdir(parents=True)
        provider.write_text('  val VERSION = DatabricksShimVersion(4, 2, 1, "19.10")\n')
        git(self.tmp, "add", ".")
        git(self.tmp, "commit", "-qm", "supported")
        pinned = git(self.tmp, "rev-parse", "HEAD")
        for text, error in [
                ('val VERSION = DatabricksShimVersion(4, 2, 0, "19.10")', "inconsistent"),
                ('val VERSION = DatabricksShimVersion(4, 2, 1, "19.1")', "inconsistent"),
                ('val VERSION = computeVersion()', "one explicit"),
                (provider.read_text() * 2, "one explicit")]:
            provider.write_text(text)
            git(self.tmp, "commit", "-qam", "different publication")
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, error):
                runtimes.discover(self.tmp, git(self.tmp, "rev-parse", "HEAD"))
        # The workspace and newer commits cannot override the original request.
        self.assertEqual(runtimes.discover(self.tmp, pinned), {"19.10": "4.2.1"})

    def test_provider_and_transport(self):
        profiles = {profile: {"host": "https://workspace.example.invalid", "driver": "driver", "creator_user_name": "ci",
                              "worker": "worker", "ssh_port": 2200} for profile in workload.CLOUD}
        profiles["aws2"].update(ssh_port=22, jump_host="jump.example.invalid", jump_user="ci")
        fake = {f"DATABRICKS_{p.upper()}_TOKEN": "fixture-api-" + p for p in profiles}
        fake.update(DATABRICKS_PROFILES_JSON=json.dumps(profiles), DATABRICKS_JUMP_PRIVATE_KEY="fixture-jump-key",
                    DATABRICKS_JUMP_KNOWN_HOSTS="fixture-known-host", UNRELATED_SECRET="unrelated")
        with patch.dict(os.environ, fake, clear=True):
            for i, (profile, runtime, spark, part) in enumerate((
                    ("aws2", "17.3", "4.0.0", "CI_PART1"), ("aws", "18.3", "4.1.0", "CI_PART2"),
                    ("azure3", "19.10", "4.2.1", "CI_PART2"))):
                directory = self.tmp / str(i)
                directory.mkdir(mode=0o700)
                env = workload.attempt_env(profile, runtime, spark, part, False, directory)
                self.assertEqual(env["BASE_SPARK_VERSION"], spark)
                self.assertEqual(env["DATABRICKS_RUNTIME"], runtime + ".x-gpu-ml-scala2.13")
                self.assertEqual(env["DATABRICKS_TOKEN"], "fixture-api-" + profile)
                self.assertNotIn("UNRELATED_SECRET", env)
                self.assertEqual(Path(env["DATABRICKS_PRIVKEY"]).stat().st_mode & 0o777, 0o600)
                cfg = Path(env["DATABRICKS_SSH_CONFIG"]).read_text()
                self.assertIn("ForwardAgent no", cfg)
                self.assertEqual("ProxyJump db-ci-jump" in cfg, profile == "aws2")
                self.assertNotIn("fixture-jump-key", cfg)
                env.update(DATABRICKS_DRIVER_ADDRESS="driver.example.invalid", DATABRICKS_CLUSTER_ID="owned")
                archive = self.tmp / "source.tgz"
                archive.write_bytes(b"fixture archive")
                with patch("subprocess.run") as proc, patch.object(workload, "check_pytest", return_value=0):
                    proc.return_value.returncode = 0
                    self.assertEqual(workload.run_workload("build", env, "owned", directory, archive), 0)
                    self.assertIn("DBFS_CACHE_DIR=/home/ubuntu/.cache/spark-ci", proc.call_args.args[0][-1])
                    self.assertEqual(workload.run_workload("test", env, "owned", directory, archive), 0)
                    test_command = proc.call_args.args[0][-1]
                    self.assertIn("bash /home/ubuntu/test.sh", test_command)
                    self.assertNotIn("DBFS_CACHE_DIR=", test_command)
                    self.assertNotIn("SPARK_CACHE_FILE=", test_command)
                    self.assertNotIn("archive.apache.org", repr(proc.call_args_list))
                    for call in proc.call_args_list:
                        self.assertEqual(call.kwargs["env"], workload.CLEAN_ENV)
                        self.assertEqual(call.kwargs["stdout"], subprocess.DEVNULL)
                        self.assertEqual(call.kwargs["stderr"], subprocess.DEVNULL)
                        self.assertNotIn("fixture-api", repr(call))
                        self.assertNotIn("fixture-jump-key", repr(call))
                    proc.return_value.returncode = 23
                    self.assertEqual(workload.run_workload("build", env, "owned", directory, archive), 23)
                    with self.assertRaises(ValueError):
                        workload.run_workload("test", env, "foreign", directory, archive)
                    env["EXTRA_ENVS"] += ",SOME_SECRET=bad"
                    with self.assertRaises(ValueError):
                        workload.run_workload("test", env, "owned", directory, archive)
                self.assertNotIn("DATABRICKS_PRIVKEY", workload.cleanup_env(profile))

    def test_failed_tests_report_counts_without_overriding_the_exit(self):
        env = dict(DATABRICKS_DRIVER_ADDRESS="driver.invalid", DATABRICKS_CLUSTER_ID="owned",
                   EXTRA_ENVS="TEST_MODE=CI_PART2,REDUCED_IT=false", BASE_SPARK_VERSION="4.1.0",
                   DATABRICKS_SSH_CONFIG="fixture")
        for exits, summary, expected, reads in [
                ([0, 0, 1], 0, 1, 1), ([0, 0, 7], OSError(), 7, 1),
                ([0, 0, 0], OSError(), 1, 1), ([23], 0, 23, 0),
                ([0, 0, 255], 0, 255, 0), ([0, 0, 124], 0, 124, 0)]:
            with self.subTest(exits=exits), patch("subprocess.run", side_effect=[
                    subprocess.CompletedProcess([], code) for code in exits]), patch.object(
                    workload, "check_pytest", side_effect=[summary]) as report:
                self.assertEqual(workload.run_workload("test", env, "owned", self.tmp, None), expected)
                self.assertEqual(report.call_count, reads)

    def test_driver_output_is_bounded_and_never_treated_as_commands(self):
        for text, expected in (("3 1 0\n", 0), ("0 0 0\n", 1), ("1 1 0\n", 1), ("2 0 1\n", 1),
                               ("::notice::bad\n", 1), ("1 " * 64, 1)):
            # This local stand-in ignores the remote command and returns driver-controlled stdout.
            ssh = ["/bin/sh", "-c", 'printf "%s" "$1"', "fixture", text]
            self.assertEqual(workload.check_pytest(ssh, self.tmp), expected)


if __name__ == "__main__":
    unittest.main()
