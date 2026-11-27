# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import capture_console
import driver_archive
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

    def test_verified_driver_archive(self):
        body = b"pinned distribution bytes"
        name = "fixture.tgz"
        entry = {"size": len(body), "sha512": hashlib.sha512(body).hexdigest(), "url": "https://example.invalid/a"}
        dbfs = self.tmp / "dbfs"
        dbfs.mkdir()
        shared = dbfs / "cached_jars" / "cudf-spark-ci" / entry["sha512"] / name

        def ensure(driver):
            return driver_archive.ensure(name, entry, self.tmp / driver, dbfs)

        def fetch(args, **_):
            self.assertEqual(args[-1], entry["url"])
            Path(args[args.index("--output") + 1]).write_bytes(body)

        with patch("subprocess.run", side_effect=fetch):
            self.assertEqual(ensure("a").read_bytes(), body)
        self.assertEqual(list(dbfs.iterdir()), [])  # A cold driver never publishes shared state.
        shared.parent.mkdir(parents=True)
        shared.write_bytes(body)  # Simulate preparation by the trusted warmer.
        with patch("subprocess.run", side_effect=AssertionError("unexpected download")):
            self.assertEqual(ensure("c").read_bytes(), body)  # new driver, same DBFS object
            legacy = dbfs / "cached_jars" / name
            legacy.write_bytes(body)
            shared.unlink()
            before = legacy.stat().st_mtime_ns
            self.assertEqual(ensure("legacy-hit").read_bytes(), body)
            self.assertFalse(shared.exists())
            self.assertEqual(legacy.stat().st_mtime_ns, before)
        legacy.write_bytes(b"leave legacy data untouched")
        shared.write_bytes(b"x" * len(body))
        with patch("subprocess.run", side_effect=fetch):
            self.assertEqual(ensure("d").read_bytes(), body)
        self.assertEqual(shared.read_bytes(), b"x" * len(body))
        self.assertEqual(legacy.read_bytes(), b"leave legacy data untouched")
        shared.write_bytes(b"partial")
        with patch("subprocess.run"):  # bad/missing bytes from the mirror cannot be installed
            with self.assertRaisesRegex(ValueError, "SHA-512"):
                ensure("e")
        self.assertFalse((self.tmp / "e" / name).exists())
        self.assertEqual(shared.read_bytes(), b"partial")
        with patch("subprocess.run", side_effect=fetch):
            for root in (self.tmp / "missing", self.tmp / "unwritable"):
                if root.name == "unwritable":
                    root.mkdir()
                    (root / "cached_jars").touch()  # cannot create a directory here, even as root
                dest = driver_archive.ensure(name, entry, self.tmp / ("driver-" + root.name), root)
                self.assertEqual(dest.read_bytes(), body)
            self.assertFalse((self.tmp / "missing").exists())
        with self.assertRaisesRegex(OSError, "driver-local"):
            driver_archive.ensure(name, entry, dbfs / "new-local-cache", dbfs)
        self.assertFalse((dbfs / "new-local-cache").exists())

    def test_staging_fallback_and_receipts(self):
        name = "apache-maven-3.8.1-bin.tar.gz"
        entry = json.loads(Path(workload.__file__).with_name("archives.json").read_text())[name].copy()
        self.assertTrue(entry["fallback_url"].startswith("https://archive.apache.org/"))
        body = b"fixture distribution"
        entry.update(size=len(body), sha512=hashlib.sha512(body).hexdigest())
        for scenario, status, label, attempts in [
                ("outage", 0, "fallback", 2), ("timeout", 0, "fallback", 2),
                ("both offline", 22, "download-error", 2), ("bad size", 63, "verification-error", 1),
                ("bad checksum", 1, "verification-error", 1), ("bad fallback", 1, "verification-error", 2)]:
            with self.subTest(scenario=scenario):
                directory = self.tmp / scenario

                def fetch(argv, **kwargs):
                    if argv[-1] == entry["url"]:
                        if scenario == "timeout":
                            raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
                        if scenario != "bad checksum":
                            raise subprocess.CalledProcessError(63 if scenario == "bad size" else 22, argv)
                    else:
                        self.assertEqual(argv[-1], entry["fallback_url"])
                        if scenario == "both offline":
                            raise subprocess.CalledProcessError(22, argv)
                    Path(argv[argv.index("--output") + 1]).write_bytes(
                        b"x" * len(body) if scenario.startswith("bad") else body)

                output = io.StringIO()
                with patch("subprocess.run", side_effect=fetch) as proc, redirect_stdout(output):
                    self.assertEqual(driver_archive.receipt(name, entry, directory, self.tmp / "no-dbfs"), status)
                self.assertEqual(output.getvalue(), label + "\n")
                self.assertEqual(proc.call_count, attempts)
                self.assertEqual((directory / name).exists(), status == 0)

        linked = self.tmp / "linked"
        linked.symlink_to(self.tmp / "outage", target_is_directory=True)
        blocked = self.tmp / "file"
        blocked.touch()
        for directory in (linked / "subdir", blocked / "subdir"):
            with patch("subprocess.run", side_effect=AssertionError("unexpected download")):
                self.assertNotEqual(driver_archive.receipt(name, entry, directory, self.tmp / "no-dbfs"), 0)
            self.assertFalse(directory.exists())

    def test_stage_reports_only_fixed_labels_and_preserves_failure(self):
        for data, status, result, text in [
                (b"dbfs\n", 0, 0, "from DBFS cache"),
                (b"fallback\n", 0, 0, "from fallback download"),
                (b"verification-error\n", 1, 1, "failed verification"),
                (b"download-error\n", 22, 22, "download failed"),
                (b"", 124, 124, "exit 124"), (b"dbfs\n", 255, 255, "exit 255"),
                (b"dbfs\n::warning::SECRET\n", 0, 1, "invalid verified staging receipt"),
                (b"x" * 1024, 0, 1, "invalid verified staging receipt")]:
            def remote(command, helper, *, timeout, stdout):
                stdout.write(data)
                return status
            output = io.StringIO()
            with self.subTest(data=data[:20], status=status), redirect_stdout(output):
                self.assertEqual(workload.stage_archive(remote, "build", "fixture.tgz", {}, "/tmp"), result)
            self.assertIn(text, output.getvalue())
            self.assertNotIn("SECRET", output.getvalue())

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
                with patch("subprocess.run") as proc, patch.object(workload, "check_pytest", return_value=0), \
                        patch.object(workload, "report_console") as console:
                    proc.return_value.returncode = 0
                    def remote(command, **kwargs):
                        if command[-1].endswith("--receipt"):
                            kwargs["stdout"].write(b"dbfs\n")
                        return proc.return_value
                    proc.side_effect = remote
                    self.assertEqual(workload.run_workload("build", env, "owned", directory, archive), 0)
                    self.assertIn("apache-maven-3.8.1-bin.tar.gz", proc.call_args_list[0].args[0][-1])
                    self.assertIn("DBFS_CACHE_DIR=/home/ubuntu/.cache/spark-ci", proc.call_args.args[0][-1])
                    proc.reset_mock()
                    self.assertEqual(workload.run_workload("test", env, "owned", directory, archive), 0)
                    test_command = proc.call_args.args[0][-1]
                    self.assertIn('exec bash /home/ubuntu/run-test-command.sh "$@"', test_command)
                    self.assertIn("set -- /usr/bin/python3 -I ", test_command)
                    self.assertIn("bash /home/ubuntu/test.sh", test_command)
                    self.assertEqual([c.args[-2:] for c in console.call_args_list], [("build", 0), ("test", 0)])
                    self.assertEqual("DBFS_CACHE_DIR=" in test_command, part == "CI_PART1")
                    self.assertEqual("spark-3.5.0-bin-hadoop3-scala2.13.tgz" in proc.call_args_list[0].args[0][-1],
                                     part == "CI_PART1")
                    self.assertNotIn("SPARK_CACHE_FILE=", test_command)
                    for call in proc.call_args_list:
                        self.assertEqual(call.kwargs["env"], workload.CLEAN_ENV)
                        self.assertEqual(call.kwargs["stdout"] != subprocess.DEVNULL,
                                         call.args[0][-1].endswith("--receipt"))
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
                    workload, "check_pytest", side_effect=[summary]) as report, patch.object(
                    workload, "report_console", side_effect=OSError()) as console:
                self.assertEqual(workload.run_workload("test", env, "owned", self.tmp, None), expected)
                self.assertEqual(report.call_count, reads)
                self.assertEqual(console.call_count, reads)
                if reads:
                    self.assertEqual(console.call_args.args[-1], expected)

    def test_postrun_streams_and_failure_evidence(self):
        receipt = self.tmp / "receipt"
        program = """import sys
print('=== Databricks GPU diagnostics ===', file=sys.stderr)
print('Memory ECC errors: three', file=sys.stderr)
print('=== End Databricks GPU diagnostics ===', file=sys.stderr)
for i in range(1102):
    print('[INFO] Compiling module', i)
    print('job counter', i, file=sys.stderr)
print('::warning::forged')
print('Authorization: Bearer should-not-appear')
print('[INFO] received private-fixture-value')
print('-----BEGIN PRIVATE KEY-----')
print('pem-body-not-public')
print('-----END PRIVATE KEY-----')
sys.exit(int(sys.argv[1]))
"""
        with patch.dict(os.environ, {"DATABRICKS_AWS2_TOKEN": "private-fixture-value"}):
            for rc in (0, 42):
                with self.subTest(rc=rc), redirect_stdout(io.StringIO()) as out:
                    self.assertEqual(capture_console.capture(receipt, [sys.executable, "-c", program, str(rc)]), rc)
                    self.assertEqual(out.getvalue(), "")
                    workload.report_console(["/bin/sh", "-c"], self.tmp, str(receipt), "test", rc)
                    lines = out.getvalue().splitlines()
                    self.assertEqual(sum(line.startswith("driver stdout |") for line in lines), 10 if rc else 1000)
                    self.assertEqual(sum(line.startswith("driver stderr |") for line in lines), 10 if rc else 1000)
                    self.assertIn("driver stdout | [INFO] Compiling module 1101", lines)
                    self.assertIn("driver stderr | job counter 1101", lines)
                    self.assertEqual("driver diagnostics | Memory ECC errors: three" in lines, bool(rc))
                    self.assertEqual(out.getvalue().count("::group::"), 0 if rc else 2)
                    stops = [s.split("::")[-1] for s in lines if s.startswith("::stop-commands::")]
                    self.assertEqual(len(set(stops)), 3 if rc else 2)
                    self.assertTrue(all(f"::{s}::" in lines for s in stops))
                    for private in ("::warning::", "should-not-appear", "private-fixture-value", "pem-body-not-public"):
                        self.assertNotIn(private, out.getvalue())

    def test_actual_build_launcher_with_and_without_capture(self):
        # Replay the submitted shell locally, relocating only the driver's fixed paths.
        env = dict(DATABRICKS_DRIVER_ADDRESS="driver.invalid", DATABRICKS_CLUSTER_ID="owned",
                   EXTRA_ENVS="TEST_MODE=CI_PART2,REDUCED_IT=false", BASE_SPARK_VERSION="4.1.0",
                   DATABRICKS_SSH_CONFIG="fixture")
        archive = self.tmp / "source.tgz"
        archive.touch()
        run = subprocess.run
        for capture_ready in (True, False):
            home = self.tmp / str(capture_ready)
            home.mkdir()
            (home / "build.sh").write_text("umask >> calls; echo compiled; echo warning >&2; exit 17")
            private = home / "db-ci-console-fixture"
            if not capture_ready:
                private.mkdir()  # mkdir must fail without rerunning or skipping the build.

            def execute(args, **opts):
                command = args[-1]
                if command.endswith("--receipt"):
                    opts["stdout"].write(b"driver\n")
                if "set --" not in command:
                    return subprocess.CompletedProcess(args, 0)
                command = command.replace("/home/ubuntu", str(home)).replace("/tmp/db-ci-console-fixture", str(private))
                return run(["/bin/sh", "-c", "umask 022; " + command], **opts)

            with self.subTest(capture_ready=capture_ready), patch("subprocess.run", side_effect=execute), \
                    patch("secrets.token_hex", return_value="fixture"), patch.object(workload, "report_console"):
                self.assertEqual(workload.run_workload("build", env, "owned", home, archive), 17)
            self.assertEqual((home / "calls").read_text().strip().lstrip("0"), "77")
            self.assertEqual((private / "console.json").is_file(), capture_ready)
            if capture_ready:
                self.assertEqual(private.stat().st_mode & 0o777, 0o700)

    def test_console_bounds_and_unavailable_receipt(self):
        receipt = self.tmp / "receipt"
        for payload in (b"invalid", b'["not base64", "", "", ""]', b"A" * (512 * 1024)):
            receipt.write_bytes(payload)
            with self.assertRaises(ValueError), redirect_stdout(io.StringIO()) as out:
                workload.report_console(["/bin/sh", "-c"], self.tmp, str(receipt), "build", 0)
            self.assertEqual(out.getvalue(), "")
        data = capture_console.Stream()
        data.feed(b"=== Databricks GPU diagnostics ===\n")
        for i in range(8000):
            data.feed(f"unrecognized detail {i}\n".encode())
        data.feed(b"=== End Databricks GPU diagnostics ===\n")
        self.assertLess(len(data.log.value()), 129 * 1024)
        self.assertLess(len(data.diagnostics.value()), 33 * 1024)
        self.assertIn(b"bytes omitted", data.diagnostics.value())
        self.assertIn(b"unrecognized detail 0\n", data.diagnostics.value())
        self.assertIn(b"unrecognized detail 7999\n", data.diagnostics.value())

    def test_driver_output_is_bounded_and_never_treated_as_commands(self):
        for text, expected in (("3 1 0\n", 0), ("0 0 0\n", 1), ("1 1 0\n", 1), ("2 0 1\n", 1),
                               ("::notice::bad\n", 1), ("1 " * 64, 1)):
            # This local stand-in ignores the remote command and returns driver-controlled stdout.
            ssh = ["/bin/sh", "-c", 'printf "%s" "$1"', "fixture", text]
            self.assertEqual(workload.check_pytest(ssh, self.tmp), expected)


if __name__ == "__main__":
    unittest.main()
