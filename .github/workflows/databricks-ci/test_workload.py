# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import workload


class WorkloadTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name).resolve()

    def test_only_pinned_blobs_are_archived(self):
        repo = self.tmp / "candidate"
        repo.mkdir()

        def git(path, *args):
            return subprocess.check_output(["git", "-C", str(path), *args], env={
                **workload.CLEAN_ENV, "GIT_AUTHOR_NAME": "Fixture", "GIT_AUTHOR_EMAIL": "test@example.invalid",
                "GIT_COMMITTER_NAME": "Fixture", "GIT_COMMITTER_EMAIL": "test@example.invalid",
            }, stderr=subprocess.DEVNULL, text=True).strip()

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

    def test_provider_and_transport(self):
        profiles = {profile: {"host": "https://workspace.example.invalid", "driver": "driver", "creator_user_name": "ci",
                              "worker": "worker", "ssh_port": 2200} for profile in workload.CLOUD}
        profiles["aws2"].update(ssh_port=22, jump_host="jump.example.invalid", jump_user="ci")
        fake = {f"DATABRICKS_{p.upper()}_TOKEN": "fixture-api-" + p for p in profiles}
        fake.update(DATABRICKS_PROFILES_JSON=json.dumps(profiles), DATABRICKS_JUMP_PRIVATE_KEY="fixture-jump-key",
                    DATABRICKS_JUMP_KNOWN_HOSTS="fixture-known-host", UNRELATED_SECRET="unrelated")
        with patch.dict(os.environ, fake, clear=True):
            for i, (profile, runtime, part) in enumerate((
                    ("aws2", "17.3", "CI_PART1"), ("aws", "18.3", "CI_PART2"), ("azure3", "17.3", "CI_PART2"))):
                directory = self.tmp / str(i)
                directory.mkdir(mode=0o700)
                env = workload.attempt_env(profile, runtime, part, False, directory)
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
                    self.assertNotIn("SPARK_CACHE_FILE=", repr(proc.call_args_list))
                    self.assertEqual(workload.run_workload("test", env, "owned", directory, archive), 0)
                    self.assertEqual("SPARK_CACHE_FILE=" in repr(proc.call_args_list), part == "CI_PART1")
                    self.assertEqual("archive.apache.org" in repr(proc.call_args_list), part == "CI_PART1")
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

    def test_smoke_archive_transfer_and_verified_cold_recovery(self):
        helper = workload.runpy.run_path(str(workload.DB.parents[1] / ".github/actions/spark-archive-cache/archive.py"))
        name, url, _ = helper["archive"]("2.13", "databricks-smoke")
        content = b"public release fixture"
        helper["archive"] = lambda *args: (name, url, hashlib.sha512(content).hexdigest())
        cache = self.tmp / ".cache/cudf-spark/spark" / name
        cache.parent.mkdir(parents=True)
        cache.write_bytes(content)
        release = self.tmp / "release"
        release.write_bytes(content)
        driver = self.tmp / "driver"
        calls = []

        def remote(command, source, timeout):
            calls.append(source is not None)
            # Exercise the generated shell including its digest check. The URL has no network access.
            fixture = 'curl() { cp "$FIXTURE" "$tmp"; }\n'
            return subprocess.run(["sh", "-c", fixture + command], stdin=source or subprocess.DEVNULL,
                                  env={"PATH": os.defpath + ":/sbin", "FIXTURE": str(release)},
                                  capture_output=True, timeout=timeout).returncode

        with patch.object(workload.runpy, "run_path", return_value=helper), \
                patch.dict(os.environ, {"HOME": str(self.tmp), "SPARK_CACHE_FILE": str(cache)}):
            rc, target = workload.stage_smoke_archive(remote, str(driver))
            self.assertEqual((rc, Path(target).read_bytes(), calls), (0, content, [True]))
            Path(target).unlink()
            calls.clear()

            def broken_transfer(command, source, timeout):
                if source is not None:
                    calls.append(True)
                    return 255
                return remote(command, source, timeout)

            rc, _ = workload.stage_smoke_archive(broken_transfer, str(driver))
            self.assertEqual((rc, Path(target).read_bytes(), calls), (0, content, [True, False]))
            Path(target).unlink()
            calls.clear()
            cache.write_bytes(b"bad restore")
            release.write_bytes(b"corrupt download")
            rc, _ = workload.stage_smoke_archive(remote, str(driver))
            self.assertNotEqual(rc, 0)
            self.assertEqual((calls, list(driver.iterdir())), ([False], []))
            calls.clear()
            cache.unlink()
            cache.symlink_to(release)
            release.write_bytes(content)
            rc, _ = workload.stage_smoke_archive(remote, str(driver))
            self.assertEqual((rc, Path(target).read_bytes(), calls), (0, content, [False]))

    def test_driver_output_is_bounded_and_never_treated_as_commands(self):
        for text, expected in (("3 1 0\n", 0), ("0 0 0\n", 1), ("1 1 0\n", 1), ("2 0 1\n", 1),
                               ("::notice::bad\n", 1), ("1 " * 64, 1)):
            # This local stand-in ignores the remote command and returns driver-controlled stdout.
            ssh = ["/bin/sh", "-c", 'printf "%s" "$1"', "fixture", text]
            self.assertEqual(workload.check_pytest(ssh, self.tmp), expected)


if __name__ == "__main__":
    unittest.main()
