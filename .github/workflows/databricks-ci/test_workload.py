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
import tempfile
import unittest
from unittest.mock import patch

import capture_console
import runtimes
import workload
import workload_v2 as driver
import https_transport_v2 as transport
from console_filter import snapshot


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

    def test_pinned_source_and_remote_checkout(self):
        repo = self.tmp / "candidate"
        repo.mkdir()
        git(repo, "init", "-q")
        paths = {}
        for name, origin in workload.SUBMODULES.items():
            sub = self.tmp / name.split("/")[-1]
            sub.mkdir()
            git(sub, "init", "-q")
            (sub / "tracked").write_text("pinned")
            git(sub, "add", "tracked")
            git(sub, "commit", "-qm", "fixture")
            git(repo, "config", "-f", ".gitmodules", f"submodule.{name}.url", origin)
            git(repo, "update-index", "--add", "--cacheinfo", f"160000,{git(sub, 'rev-parse', 'HEAD')},{name}")
            paths[origin] = str(sub)
        (repo / "source").write_text("pinned-source")
        git(repo, "add", ".gitmodules", "source")
        git(repo, "commit", "-qm", "approved")
        sha = git(repo, "rev-parse", "HEAD")
        (repo / "source").write_text("worktree is not the source")
        source = workload.source_manifest(repo, sha)
        with self.assertRaises(ValueError):
            workload.source_manifest(repo, "0" * 40)
        paths[driver.PUBLIC] = str(repo)
        real = subprocess.check_output

        def local_git(args, **kwargs):
            self.assertEqual(kwargs["env"]["GIT_ALLOW_PROTOCOL"], "https")
            args = [paths.get(a, a) for a in args]
            return real(args, **{**kwargs, "env": dict(kwargs["env"], GIT_ALLOW_PROTOCOL="file")})

        with patch("subprocess.check_output", side_effect=local_git) as commands:
            dest = self.tmp / "driver"
            driver.checkout(source, dest)
            self.assertEqual((dest / "source").read_text(), "pinned-source")
            for name in workload.SUBMODULES:
                self.assertEqual((dest / name / "tracked").read_text(), "pinned")
            with self.assertRaisesRegex(ValueError, "submodules mismatch"):
                driver.checkout(dict(source, submodules=dict.fromkeys(workload.SUBMODULES, sha)), self.tmp / "wrong")
            commands.reset_mock()
            missing = "f" * 40
            with self.assertRaises(subprocess.CalledProcessError):
                driver.checkout(dict(source, revision=missing), self.tmp / "unavailable")
            fetches = [c.args[0][-2:] for c in commands.call_args_list if "fetch" in c.args[0]]
            self.assertEqual(fetches, [[driver.PUBLIC, missing]])
            self.assertFalse((self.tmp / "unavailable/source").exists())
        driver.verify_checkout(source, dest)
        with self.assertRaisesRegex(ValueError, "Wrong build tree"):
            driver.verify_checkout(dict(source, submodules=dict.fromkeys(workload.SUBMODULES, sha)), dest)
        git(repo, "config", "-f", ".gitmodules", "submodule.thirdparty/parquet-testing.url", "https://bad.invalid/")
        git(repo, "add", ".gitmodules")
        git(repo, "commit", "-qm", "wrong origin")
        with self.assertRaisesRegex(ValueError, "origin"):
            workload.source_manifest(repo, git(repo, "rev-parse", "HEAD"))
        (repo / "symlink").symlink_to("../outside")
        git(repo, "add", "symlink")
        git(repo, "commit", "-qm", "unsupported link")
        with self.assertRaisesRegex(ValueError, "link"):
            workload.source_manifest(repo, git(repo, "rev-parse", "HEAD"))

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
        source = {"revision": "a" * 40, "submodules": dict.fromkeys(workload.SUBMODULES, "b" * 40)}
        profiles = {p: dict(host="https://sample.cloud.databricks.com", driver="gpu", worker="gpu",
                            creator_user_name="owned") for p in workload.CLOUD}
        secrets = {"DATABRICKS_PROFILES_JSON": json.dumps(profiles),
                   **{f"DATABRICKS_{p.upper()}_TOKEN": "fixture-control-only" for p in profiles}}
        empty = {"stdout": [], "stderr": [], "diagnostics": []}
        with patch.dict(os.environ, secrets, clear=True):
            for p in profiles:
                env = workload.attempt_env(p, "17.3", "4.0.0", "CI_PART1", False, self.tmp)
                self.assertFalse(any("SSH" in k or "PRIVKEY" in k for k in env))
                self.assertEqual(list(self.tmp.iterdir()), [])
                env["DATABRICKS_CLUSTER_ID"] = "owned"
                with patch.object(workload, "workspace_api") as api, patch.object(workload, "command_context"), \
                        patch.object(workload, "run_driver") as remote:
                    for phase, report, expected in [
                            ("build", {"ok": True, "exit": 0, "stage": "complete"}, 0),
                            ("test", {"ok": True, "exit": 0, "stage": "complete", "junit": [3, 1, 0],
                                      "ready": ["databricks", "upstream"]}, 0),
                            ("test", {"ok": True, "exit": 0, "stage": "complete", "junit": [3, 3, 0],
                                      "ready": ["databricks", "upstream"]}, 1),
                            ("test", {"ok": True, "exit": 0, "stage": "complete", "junit": [3, 0, 0],
                                      "ready": ["databricks"]}, 1),
                            ("test", {"ok": False, "exit": 76, "stage": "test", "junit": None}, 76)]:
                        remote.return_value = report, empty
                        self.assertEqual(workload.run_workload(phase, env, "owned", self.tmp, source), expected)
                        payload = remote.call_args.args[1]
                        self.assertNotIn("fixture-control-only", payload)
                        self.assertNotIn("stage_archive", payload)
                        self.assertEqual(hashlib.sha256(payload.encode()).hexdigest(), remote.call_args.args[2])
                    api.assert_called_with(profiles[p]["host"], "fixture-control-only")
                    previous = remote.call_count
                    with self.assertRaises(ValueError):
                        workload.run_workload("test", env, "other", self.tmp, source)
                    with self.assertRaises(ValueError):
                        workload.run_workload("unregistered", env, "owned", self.tmp, source)
                    self.assertEqual(remote.call_count, previous)
                    remote.side_effect = RuntimeError("ambiguous HTTPS launch")
                    with self.assertRaisesRegex(RuntimeError, "ambiguous"):
                        workload.run_workload("test", env, "owned", self.tmp, source)

    def test_postrun_streams_and_failure_evidence(self):
        receipt = self.tmp / "console.json"
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
                    workload.report_console(snapshot(self.tmp, True, rc == 0), "test", rc)
                    lines = out.getvalue().splitlines()
                    # One in-window line is suppressed by a secret known only to the controller.
                    self.assertEqual(sum(line.startswith("driver stdout |") for line in lines), 9 if rc else 999)
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

    def test_driver_phase_uses_real_capture_and_requires_fresh_evidence(self):
        source = {"revision": "a" * 40, "submodules": dict.fromkeys(workload.SUBMODULES, "b" * 40)}
        cap = Path(capture_console.__file__).read_text()
        xml = '<testsuite><testcase name="first"/><testcase name="another"/></testsuite>'
        for part, rc, evidence, expected in [
                ("CI_PART1", 0, "good", 0), ("CI_PART1", 0, "no-upstream", 1),
                ("CI_PART2", 0, "good", 0), ("CI_PART2", 0, "missing-junit", 1),
                ("CI_PART1", 0, "failure-in-junit", 1), ("CI_PART1", 0, "all-skipped", 1),
                ("CI_PART1", 0, "stale", 1),
                ("CI_PART2", 76, "missing-junit", 76)]:
            with self.subTest(part=part, rc=rc, evidence=evidence), tempfile.TemporaryDirectory(dir=self.tmp) as tmp:
                home = Path(tmp).resolve()
                tree, work, markers, version = (home / n for n in ("spark-rapids", "work", ".databricks-fallback", "version"))
                work.mkdir()
                version.write_text("17.3")
                reports = tree / "scala2.13/integration_tests/target/run_dir_fixture"
                reports.mkdir(parents=True)
                report = reports / "TEST-pytest-fixture.xml"
                plan = dict(phase="test", source=source, runtime="17.3", spark="4.0.0", reduced=False, mode=part)
                script = 'test -z "${DATABRICKS_TOKEN:-}${DBFS_CACHE_DIR:-}" || exit 99\n'
                script += 'bash "$SPARK_STARTUP_CHECK" databricks || exit $?\n'
                if part == "CI_PART1" and evidence != "no-upstream":
                    script += 'bash "$SPARK_STARTUP_CHECK" upstream || exit $?\n'
                if evidence != "missing-junit":
                    text = {"failure-in-junit": "<testsuite><testcase><failure/></testcase></testsuite>",
                            "all-skipped": "<testsuite><testcase><skipped/></testcase></testsuite>"}.get(evidence, xml)
                    script += f"echo '{text}' > '{report}'\n"
                if evidence == "stale":
                    report.write_text(xml)
                hook = f"""printf '{{"phase":"READY","runtime":"%s"}}' "$1" > '{markers}'/spark-readiness-"$1".json"""
                files = {"capture.py": cap, "test.sh": script + f"echo actual-output; exit {rc}",
                         "spark_readiness.sh": hook, "run-test-command.sh": 'exec "$@"'}
                with patch.multiple(driver, HOME=home, SOURCE=tree, MARKERS=markers, DBR_VERSION=version), \
                        patch.object(driver, "verify_checkout"), \
                        patch.dict(os.environ, DATABRICKS_TOKEN="host-only"):
                    result = driver.run_phase(plan, files, work)
                self.assertEqual(result["exit"], expected)
                self.assertEqual(result["ok"], expected == 0)
                if evidence == "good":
                    self.assertEqual(result["junit"], [2, 0, 0])
                if evidence == "stale":
                    self.assertFalse((work / "console.json").exists())
                else:
                    self.assertIn("actual-output", snapshot(work, True, result["ok"])["stdout"])

    def test_optional_seed_never_changes_product_exit(self):
        source = {"revision": "a" * 40, "submodules": dict.fromkeys(workload.SUBMODULES, "b" * 40)}
        digest = "d" * 64
        for configured, seed_rc, product_rc, receipt, status in [
                (digest, 0, 0, "Maven seed: restored 2 released artifact versions (512 bytes)", 2),
                (digest, 0, 0, "Maven seed unavailable", "cold"),
                (digest, 17, 42, "", "cold"), ("invalid", 0, 3, "", "disabled")]:
            with self.subTest(status=status), tempfile.TemporaryDirectory(dir=self.tmp) as tmp:
                home = Path(tmp).resolve()
                version = home / "version"
                version.write_text("17.3")
                work = home / "work"
                work.mkdir()
                tree = home / "source"
                marker = work / "seed-env"
                plan = dict(phase="build", mode="CI_PART1", reduced=False, runtime="17.3", spark="4.0.0",
                            source=source, seed=configured)
                files = {"capture.py": Path(capture_console.__file__).read_text(),
                         "maven_seed_v2.py": ("import os,sys\nfrom pathlib import Path\n"
                                              f"assert sys.argv[1:] == ['restore','--sha256','{digest}']\n"
                                              f"Path({str(marker)!r}).write_text(os.environ['HOME'])\n"
                                              f"print({receipt!r})\n"
                                              f"sys.exit({seed_rc})"),
                         "build.sh": ('test -z "${DATABRICKS_TOKEN:-}${DBFS_CACHE_DIR:-}" || exit 99\n'
                                      f'echo "building"; exit {product_rc}')}
                with patch.multiple(driver, HOME=home, SOURCE=tree, DBR_VERSION=version), \
                        patch.object(driver, "checkout", side_effect=lambda _, dest: dest.mkdir()), \
                        patch.dict(os.environ, DATABRICKS_TOKEN="host-only"):
                    result = driver.run_phase(plan, files, work)
                self.assertEqual((result["exit"], result["seed"]), (product_rc, status))
                self.assertIn("building", snapshot(work, True, result["ok"])["stdout"])
                if status == "disabled":
                    self.assertFalse(marker.exists())
                else:
                    self.assertEqual(marker.read_text(), driver.pwd.getpwuid(os.geteuid()).pw_dir)
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("fixture", 180)):
            self.assertEqual(driver.restore_seed(self.tmp, digest, {}), "cold")

    def test_console_bounds_and_unavailable_receipt(self):
        receipt = self.tmp / "console.json"
        for payload in (b"invalid", b'["not base64", "", "", ""]', b"A" * (512 * 1024)):
            receipt.write_bytes(payload)
            with self.assertRaises(ValueError), redirect_stdout(io.StringIO()) as out:
                snapshot(self.tmp, True, True)
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

    def test_workspace_missing_requires_exact_provider_error(self):
        fake = unittest.mock.MagicMock()
        response = fake.return_value.getresponse.return_value
        call = transport.workspace_api("https://sample.cloud.databricks.com", "fixture")
        with patch("http.client.HTTPSConnection", fake):
            for status, body, is_missing in [
                    (404, b'{"error_code":"RESOURCE_DOES_NOT_EXIST"}', True),
                    (400, b'{"error_code":"RESOURCE_DOES_NOT_EXIST"}', True),
                    (404, b'{"error_code":"resource_does_not_exist"}', False), (404, b'not found', False),
                    (403, b'{"error_code":"RESOURCE_DOES_NOT_EXIST"}', False)]:
                response.status, response.read.return_value = status, body
                with self.assertRaises(RuntimeError) as caught:
                    call("GET", transport.CLUSTERS + "/get", cluster_id="owned")
                self.assertIsInstance(caught.exception, transport.MissingCluster if is_missing else RuntimeError)
                self.assertEqual(isinstance(caught.exception, transport.MissingCluster), is_missing)
            response.status, response.read.return_value = 200, b""
            for path in ("/delete", "/permanent-delete"):
                self.assertEqual(call("POST", transport.CLUSTERS + path, cluster_id="owned"), {})

if __name__ == "__main__":
    unittest.main()
