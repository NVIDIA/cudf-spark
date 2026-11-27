# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

"""Driver-side HTTPS workload. Its caller supplies the trusted launchers and immutable source pins."""

import json
import os
from pathlib import Path
import pwd
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET


HOME = Path("/home/ubuntu")
SOURCE = HOME / "spark-rapids"
MARKERS = HOME / ".databricks-fallback"
DBR_VERSION = Path("/databricks/DBR_VERSION")
PUBLIC = "https://github.com/NVIDIA/cudf-spark.git"
SUBMODULES = {"thirdparty/parquet-testing": "https://github.com/apache/parquet-testing.git",
              "thirdparty/spark-rapids-common": "https://github.com/NVIDIA/spark-rapids-common.git"}
PATH = "/databricks/python/bin:/usr/local/nvidia/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


def gitlinks(tree):
    links = {}
    for record in tree.rstrip(b"\0").split(b"\0"):
        meta, name = record.split(b"\t", 1)
        mode, kind, oid = meta.split()
        path = os.fsdecode(name)
        if (not path.isprintable() or "\\" in path
                or any(p.lower() in ("", ".", "..", ".git") for p in path.split("/"))):
            raise ValueError("Unsafe source path")
        if mode == b"160000" and kind == b"commit":
            links[path] = oid.decode()
        elif kind != b"blob" or mode not in (b"100644", b"100755"):
            raise ValueError("Unsupported source link or special file")
    return links


def validate_source(source):
    if (not isinstance(source, dict) or set(source) != {"revision", "submodules"}
            or not isinstance(source["submodules"], dict) or source["submodules"].keys() != SUBMODULES.keys()
            or any(not isinstance(s, str) or not re.fullmatch("[0-9a-f]{40}", s)
                   for s in (source["revision"], *source["submodules"].values()))):
        raise ValueError("Invalid source manifest")


def checkout(source, destination):
    validate_source(source)
    destination = destination.parent.resolve() / destination.name
    destination.mkdir(mode=0o700)  # A fresh, caller-owned cluster; never reuse an earlier build tree.
    with tempfile.TemporaryDirectory() as git_home:
        env = {"PATH": os.defpath, "HOME": git_home, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
               "GIT_TERMINAL_PROMPT": "0", "GIT_ALLOW_PROTOCOL": "https", "GIT_LFS_SKIP_SMUDGE": "1"}

        def git(repo, *args):
            return subprocess.check_output(["git", "--no-replace-objects", "-c", "core.hooksPath=/dev/null",
                                            "-C", str(repo), *args], env=env, stderr=subprocess.DEVNULL, timeout=240)

        def fetch(repo, url, pin, expected_links):
            git(repo, "init", "--quiet", "--template=")
            git(repo, "fetch", "--quiet", "--depth=1", "--no-tags", url, pin)
            if git(repo, "rev-parse", "FETCH_HEAD").strip().decode() != pin:
                raise ValueError("Remote revision mismatch")
            if gitlinks(git(repo, "ls-tree", "-rz", pin)) != expected_links:
                raise ValueError("Remote submodules mismatch")
            git(repo, "checkout", "--quiet", "--detach", pin)

        fetch(destination, PUBLIC, source["revision"], source["submodules"])
        for path, url in SUBMODULES.items():
            pin = source["submodules"][path]
            sub = destination / path
            if sub.resolve() != sub or not sub.is_dir() or any(sub.iterdir()):
                raise ValueError("Remote submodule identity mismatch")
            fetch(sub, url, pin, {})


def verify_checkout(source, destination):
    destination = destination.parent.resolve() / destination.name
    for path, pin in [("", source["revision"]), *source["submodules"].items()]:
        repo = destination / path
        identity = subprocess.check_output(["git", "--no-replace-objects", "-C", str(repo), "rev-parse",
                                            "--show-toplevel", "HEAD"], stderr=subprocess.DEVNULL, timeout=30,
                                           env={"PATH": os.defpath, "GIT_CONFIG_GLOBAL": os.devnull,
                                                "GIT_CONFIG_NOSYSTEM": "1", "GIT_ALLOW_PROTOCOL": ""})
        if repo.resolve() != repo or identity.splitlines() != [os.fsencode(repo), pin.encode("ascii")]:
            raise ValueError("Wrong build tree at test time")


def junit(root):
    total = skipped = failed = 0
    for path in root.glob("run_dir*/TEST-pytest-*.xml"):
        if not path.is_file() or path.is_symlink() or path.stat().st_size > 64 * 1024 * 1024:
            raise ValueError("Invalid pytest report")
        for case in ET.parse(path).getroot().iter("testcase"):
            total += 1
            skipped += case.find("skipped") is not None
            failed += case.find("failure") is not None or case.find("error") is not None
    return [total, skipped, failed]


def restore_seed(work, digest, env):
    """The product needs /home/ubuntu; Maven's default user.home follows the Linux account."""
    if not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest):
        return "disabled"
    try:
        with tempfile.TemporaryFile() as output:
            result = subprocess.run(["/usr/bin/python3", "-I", str(work / "maven_seed_v2.py"), "restore",
                                     "--sha256", digest], env=dict(env, HOME=pwd.getpwuid(os.geteuid()).pw_dir),
                                    stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL, timeout=180)
            output.seek(0)
            match = re.fullmatch(rb"Maven seed: restored ([1-9]\d{0,8}) released artifact versions "
                                 rb"\([1-9]\d{0,11} bytes\)\n", output.read(256))
        return int(match[1]) if not result.returncode and match else "cold"
    except (OSError, KeyError, subprocess.SubprocessError):
        return "cold"


def run_phase(plan, files, work):
    phase, mode, runtime = plan["phase"], plan["mode"], plan["runtime"]
    if (phase not in ("build", "test") or mode not in ("CI_PART1", "CI_PART2")
            or type(plan["reduced"]) is not bool or not re.fullmatch(r"\d+\.\d+", runtime)
            or not re.fullmatch(r"\d+\.\d+\.\d+", plan["spark"])):
        raise ValueError("Invalid phase inputs")
    validate_source(plan["source"])
    rc, counts, ready, stage, seed, detail = 0, None, [], "runtime", "disabled", None
    try:
        if DBR_VERSION.read_text().strip() != runtime:
            raise ValueError("Unexpected Databricks runtime")
        for name, content in files.items():
            with (work / name).open("x", encoding="utf-8") as out:
                out.write(content)
        env = {"PATH": PATH, "HOME": str(HOME), "LANG": "C.UTF-8", "DB_HOME": "/databricks",
               "BASE_SPARK_VERSION": plan["spark"], "TEST_MODE": mode, "REDUCED_IT": str(plan["reduced"]).lower(),
               "DEBIAN_FRONTEND": "noninteractive"}
        command = ["bash", str(work / (phase + ".sh"))]
        logs = work / "console.json"
        capture = ["/usr/bin/python3", "-I", str(work / "capture.py"), str(logs)]
        reports = SOURCE / "scala2.13/integration_tests/target"
        if phase == "build":
            stage = "checkout"
            checkout(plan["source"], SOURCE)
            seed = restore_seed(work, plan.get("seed"), env)
            env["MVN_OPT"] = ""
        else:
            stage = "test-inputs"
            verify_checkout(plan["source"], SOURCE)
            if any(reports.glob("run_dir*/TEST-pytest-*.xml")):
                raise ValueError("Refusing stale pytest evidence")
            MARKERS.mkdir(mode=0o700)
            env.update(SPARK_STARTUP_CHECK=str(work / "spark_readiness.sh"), TEST_TYPE="pre-commit",
                       LOCAL_JAR_PATH="", SPARK_CONF="")
            capture = ["bash", str(work / "run-test-command.sh"), *capture]
        stage = phase
        rc = subprocess.run(capture + command, cwd=SOURCE if phase == "build" else HOME,
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            env=env, timeout=(75 if phase == "build" else 270) * 60).returncode
        rc = rc if rc >= 0 else 128 - rc
        if phase == "test":
            # A missing receipt is secondary evidence when the command itself failed.
            failed_command = bool(rc)
            stage = phase if failed_command else "junit"
            counts = junit(reports)
            if counts[0] <= counts[1] or counts[2]:
                raise ValueError("Missing or failed product tests")
            stage = phase if failed_command else "readiness"
            for name in (["databricks", "upstream"] if mode == "CI_PART1" else ["databricks"]):
                marker = MARKERS / f"spark-readiness-{name}.json"
                if (not marker.is_file() or marker.is_symlink() or marker.stat().st_size > 4096 or
                        json.loads(marker.read_bytes()) != {"phase": "READY", "runtime": name}):
                    raise ValueError("Readiness not confirmed")
                ready.append(name)
        stage = phase if rc else "complete"
    except subprocess.TimeoutExpired:
        rc = rc or 124
        detail = "timeout"
    except subprocess.CalledProcessError as exc:
        rc = rc or max(1, min(255, exc.returncode))
        detail = "subprocess"
    except (OSError, ValueError, ET.ParseError) as exc:
        rc = rc or 1
        detail = f"errno {exc.errno}" if isinstance(exc, OSError) and exc.errno is not None else "invalid evidence"
    return {"ok": rc == 0, "exit": rc, "stage": stage, "junit": counts, "ready": ready, "seed": seed, "detail": detail}
