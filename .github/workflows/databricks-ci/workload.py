# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prepare immutable build inputs locally; run the product only on the allocated DB driver."""

import base64
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import tarfile
from urllib.parse import urlsplit

from console_filter import public_lines
from runtimes import SPARK_VERSION, runtime_label


DB = Path(__file__).resolve().parents[3] / "jenkins/databricks"
SUBMODULES = {"thirdparty/parquet-testing", "thirdparty/spark-rapids-common"}
CLOUD = {"aws2": "aws", "aws": "aws", "azure3": "azure"}
HOSTNAME = r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?"
CLEAN_ENV = {"PATH": os.defpath, "LANG": "C", "GIT_CONFIG_NOSYSTEM": "1",
             "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0", "GIT_NO_LAZY_FETCH": "1"}


def make_archive(candidate, merge_sha, output):
    """Stream only tracked regular blobs. Never run filters, tar hooks or candidate scripts."""
    candidate = Path(candidate).resolve()
    if not re.fullmatch(r"[0-9a-f]{40}", merge_sha) or Path(output).resolve().is_relative_to(candidate):
        raise ValueError("Invalid candidate archive inputs")

    def pack(tar, repo, revision, prefix):
        git = ["git", "--no-replace-objects", "-c", "protocol.allow=never", "-C", str(repo)]

        def read(*args):
            return subprocess.check_output(git + list(args), env=CLEAN_ENV, stderr=subprocess.DEVNULL)

        if (repo.resolve() != repo or Path(os.fsdecode(read("rev-parse", "--show-toplevel").rstrip(b"\n"))) != repo
                or read("rev-parse", "HEAD").strip().decode() != revision):
            raise ValueError("Candidate or submodule checkout does not match its pinned revision")
        links = {}
        tree = read("ls-tree", "-r", "-z", "--full-tree", revision)
        with subprocess.Popen(git + ["cat-file", "--batch"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.DEVNULL, env=CLEAN_ENV) as blobs:
            try:
                for record in tree.rstrip(b"\0").split(b"\0"):
                    meta, name = record.split(b"\t", 1)
                    mode, kind, oid = meta.split()
                    path = os.fsdecode(name)
                    if (any(part.lower() in ("", ".", "..", ".git") for part in path.split("/"))
                            or "\\" in path or not path.isprintable()):
                        raise ValueError("Unsafe path in candidate Git tree")
                    if mode == b"160000" and kind == b"commit" and not prefix and path in SUBMODULES:
                        links[path] = oid.decode()
                        continue
                    if kind != b"blob" or mode not in (b"100644", b"100755"):
                        raise ValueError("Candidate contains an unsupported link or special entry")
                    blobs.stdin.write(oid + b"\n")
                    blobs.stdin.flush()
                    actual, objtype, size = blobs.stdout.readline().split()
                    if (actual, objtype) != (oid, b"blob"):
                        raise ValueError("Missing pinned Git blob")
                    entry = tarfile.TarInfo(prefix + path)
                    entry.mode, entry.size = int(mode, 8) & 0o777, int(size)
                    tar.addfile(entry, blobs.stdout)
                    if blobs.stdout.read(1) != b"\n":
                        raise ValueError("Truncated pinned Git blob")
            finally:
                blobs.stdin.close()
                blobs.stdout.close()
            if blobs.wait():
                raise ValueError("Cannot read pinned Git objects")
        if not prefix:
            if links.keys() != SUBMODULES:
                raise ValueError("Missing required pinned submodules")
            for path, oid in sorted(links.items()):
                pack(tar, repo / path, oid, path + "/")

    with Path(output).open("xb") as stream, tarfile.open(fileobj=stream, mode="w:gz", compresslevel=1) as tar:
        pack(tar, candidate, merge_sha, "")


def profile_config(profile):
    if profile not in CLOUD:
        raise ValueError("Unknown Databricks CI provider")
    config = json.loads(os.environ["DATABRICKS_PROFILES_JSON"])[profile]
    host = urlsplit(config["host"])
    if (host.scheme != "https" or not re.fullmatch(HOSTNAME, host.netloc)
            or host.path not in ("", "/") or host.query or host.fragment):
        raise ValueError("Invalid protected workspace URL")
    return config


def cleanup_env(profile):
    token = os.environ[f"DATABRICKS_{profile.upper()}_TOKEN"]
    if not token:
        raise ValueError("Missing provider credential")
    return {**CLEAN_ENV, "DATABRICKS_HOST": profile_config(profile)["host"].rstrip("/"),
            "DATABRICKS_TOKEN": token}


def creator_name(profile):
    creator = profile_config(profile).get("creator_user_name")
    if not isinstance(creator, str) or not creator or creator.strip() != creator:
        raise ValueError("Configure the exclusive CI service principal for the Databricks profile")
    return creator


def private_file(path, text):
    if not text.strip():
        raise ValueError("Missing SSH configuration material")
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
        f.write(text.rstrip("\n") + "\n")


def attempt_env(profile, runtime, spark, part, reduced, directory):
    creator_name(profile)  # Refuse new allocations when their owner cannot be verified by the sweeper.
    runtime_label(runtime)
    if not re.fullmatch(SPARK_VERSION, spark) or part not in ("CI_PART1", "CI_PART2") or type(reduced) is not bool:
        raise ValueError("Invalid Databricks test selection")
    cfg = profile_config(profile)
    directory = Path(directory).resolve()
    # OpenSSH expands tokens and backslash escapes in config paths. Use only the runner's private temp path.
    if directory.stat().st_mode & 0o077 or not re.fullmatch(r"[/A-Za-z0-9_.-]+", str(directory)):
        raise ValueError("Expected a private SSH configuration directory")
    for key in ("driver", "worker"):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", cfg[key]):
            raise ValueError("Invalid configured node type")
    port = cfg["ssh_port"]
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("Invalid configured SSH port")
    key = directory / "driver-key"
    if key.exists():
        raise ValueError("SSH material already exists")
    subprocess.run(["ssh-keygen", "-q", "-t", "rsa", "-b", "3072", "-C", "db-ci", "-N", "", "-f", str(key)],
                   check=True, env=CLEAN_ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cfgfile = directory / "ssh_config"
    sections = []
    if profile == "aws2":
        host, user = cfg["jump_host"], cfg["jump_user"]
        if not re.fullmatch(HOSTNAME, host) or not re.fullmatch(r"[A-Za-z0-9_-]+", user):
            raise ValueError("Invalid SSH bastion configuration")
        jump_key, known = directory / "jump-key", directory / "jump-known-hosts"
        private_file(jump_key, os.environ["DATABRICKS_JUMP_PRIVATE_KEY"])
        private_file(known, os.environ["DATABRICKS_JUMP_KNOWN_HOSTS"])
        sections += [f"Host db-ci-jump\n HostName {host}\n User {user}\n Port 22\n IdentityFile {jump_key}"
                     f"\n StrictHostKeyChecking yes\n UserKnownHostsFile {known}",
                     "Host * !db-ci-jump\n ProxyJump db-ci-jump"]
    sections += [f"Host *\n IdentitiesOnly yes\n ForwardAgent no\n BatchMode yes\n LogLevel ERROR"
                 "\n ConnectTimeout 15\n ServerAliveInterval 30\n ServerAliveCountMax 3",
                 f"Host * !db-ci-jump\n Port {port}\n IdentityFile {key}\n StrictHostKeyChecking accept-new"
                 f"\n UserKnownHostsFile {directory / 'driver-known-hosts'}"]
    private_file(cfgfile, "\n".join(sections))
    return {**cleanup_env(profile), "DATABRICKS_CLOUD": CLOUD[profile], "DATABRICKS_DRIVER": cfg["driver"],
            "DATABRICKS_WORKER": cfg["worker"], "DATABRICKS_PRIVKEY": str(key), "DATABRICKS_SSH_CONFIG": str(cfgfile),
            "DATABRICKS_SSH_CONFIG_ONLY": "1",
            "DATABRICKS_PUBKEY": key.with_suffix(".pub").read_text().strip(), "NUM_WORKERS": "0",
            "DATABRICKS_RUNTIME": runtime + ".x-gpu-ml-scala2.13", "BASE_SPARK_VERSION": spark,
            "EXTRA_ENVS": f"TEST_MODE={part},REDUCED_IT={str(reduced).lower()}"}


def check_pytest(ssh, directory):
    # Parse on the driver, not in the credentialed workspace. Even a compromised driver can
    # send only a bounded numeric summary back; it cannot choose a local path or print to the log.
    summary = """from pathlib import Path
import xml.etree.ElementTree as ET
path = Path('/home/ubuntu/spark-rapids/scala2.13/integration_tests/target')
total = skipped = failed = 0
for report in path.glob('run_dir*/TEST-pytest-*.xml'):
    if not report.is_file() or report.is_symlink() or report.stat().st_size > 64 * 1024 * 1024:
        raise ValueError('Invalid pytest report')
    root = ET.parse(report).getroot()
    for case in root.iter('testcase'):
        total += 1
        skipped += case.find('skipped') is not None
        failed += case.find('failure') is not None or case.find('error') is not None
print(total, skipped, failed)
"""
    with subprocess.Popen(ssh + [shlex.join(["/usr/bin/python3", "-I", "-c", summary])],
                          stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          cwd=directory, env=CLEAN_ENV) as proc:
        try:
            bounded = subprocess.run(["head", "-c", "64"], stdin=proc.stdout, capture_output=True,
                                     env=CLEAN_ENV, timeout=120, check=True).stdout
            if len(bounded) == 64 or proc.wait(timeout=10):
                return 1
        except subprocess.TimeoutExpired:
            return 124
        finally:
            proc.kill()
    if not re.fullmatch(rb"[0-9]{1,9} [0-9]{1,9} [0-9]{1,9}\n", bounded):
        return 1
    total, skipped, failed = map(int, bounded.split())
    print(f"Databricks pytest JUnit: {total} cases, {skipped} skipped, {failed} failed", flush=True)
    return 0 if total > skipped and failed == 0 else 1


def report_console(ssh, directory, receipt, phase, rc):
    # The driver runs PR code. Limit bytes and time on THIS side of SSH, and never trust
    # the driver to sanitize output, supply a local path or change the workload status.
    cap = 512 * 1024
    with subprocess.Popen(ssh + ["cat " + shlex.quote(receipt)], stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, cwd=directory, env=CLEAN_ENV) as proc:
        try:
            data = subprocess.run(["head", "-c", str(cap)], stdin=proc.stdout, capture_output=True,
                                  env=CLEAN_ENV, timeout=30, check=True).stdout
            if len(data) >= cap or proc.wait(timeout=5):
                raise ValueError("Incomplete driver console")
        finally:
            proc.kill()
    chunks = json.loads(data)
    if not isinstance(chunks, list) or len(chunks) != 4 or not all(isinstance(x, str) for x in chunks):
        raise ValueError("Invalid driver console")
    hidden = [s for k, v in os.environ.items() if k.startswith("DATABRICKS_") and
              k.endswith(("_TOKEN", "_PRIVATE_KEY", "_KNOWN_HOSTS")) for s in v.splitlines() if s]
    sections = []
    for chunk, max_bytes in zip(chunks, (129, 129, 33, 33)):
        raw = base64.b64decode(chunk, validate=True)
        if len(raw) > max_bytes * 1024:
            raise ValueError("Oversized driver console section")
        records = re.sub(rb"\x1b\[[0-9;:]*m", b"", raw).split(b"\n")
        sections.append(public_lines(records, hidden))
    limit = 10 if rc else 1000
    channels = [("stdout", sections[0][-limit:]), ("stderr", sections[1][-limit:])]
    if rc:
        diagnostics = sections[2] + sections[3] or (sections[1] or sections[0])[-80:]
        channels.append(("diagnostics", diagnostics))
    for channel, lines in channels:
        if not lines:
            continue
        marker = secrets.token_hex(24)
        if not rc:
            print(f"::group::Databricks {phase} {channel}", flush=True)
        print(f"::stop-commands::{marker}", flush=True)
        try:
            for line in lines:
                print(f"driver {channel} | {line}", flush=True)
        finally:
            print(f"::{marker}::", flush=True)
            if not rc:
                print("::endgroup::", flush=True)


def stage_archive(remote, phase, name, entry, local_cache):
    stage = shlex.join(["/usr/bin/python3", "-I", "-", name, json.dumps(entry), local_cache, "--receipt"])
    # An honest helper writes one label. A local pipe bounds buffering even if the driver is compromised.
    read_fd, write_fd = os.pipe()
    with os.fdopen(read_fd, "rb", buffering=0) as reader:
        helper_path = Path(__file__).with_name("driver_archive.py")
        with os.fdopen(write_fd, "wb", buffering=0) as writer, helper_path.open("rb") as helper:
            rc = remote(stage, helper, timeout=(10 if phase == "build" else 70) * 60, stdout=writer)
        os.set_blocking(reader.fileno(), False)
        label = reader.read(64)
    if rc:
        reasons = {b"download-error\n": "upstream download failed", b"download-timeout\n": "download timed out",
                   b"verification-error\n": "archive or metadata failed verification",
                   b"local-error\n": "driver cache path, filesystem or tool error"}
        reason = reasons.get(label, "no confirmed driver result")
        print(f"Databricks {phase}: failed to stage {name}: {reason} (exit {rc})", flush=True)
        return rc
    sources = {b"driver\n": "driver-local cache", b"dbfs\n": "DBFS cache", b"legacy-dbfs\n": "legacy DBFS cache",
               b"primary\n": "primary download", b"fallback\n": "fallback download"}
    if label not in sources:
        print(f"Databricks {phase}: missing or invalid verified staging receipt for {name}", flush=True)
        return 1
    print(f"Databricks {phase}: staged {name} from {sources[label]} (size and SHA-512 verified)", flush=True)
    return 0


def run_workload(phase, env, cluster_id, directory, archive):
    """Send trusted launchers and source to the remote driver. No credentials enter the workload environment."""
    address = env["DATABRICKS_DRIVER_ADDRESS"]
    if (not re.fullmatch(HOSTNAME, address) or address.lower() == "db-ci-jump"
            or cluster_id != env["DATABRICKS_CLUSTER_ID"]):
        raise ValueError("Driver does not match the prepared cluster")
    # Only the expected nonsecret test switches can cross this boundary.
    options = env["EXTRA_ENVS"]
    hook = ",SPARK_STARTUP_CHECK=/home/ubuntu/.databricks-fallback/spark_readiness.sh"
    if not re.fullmatch(r"TEST_MODE=CI_PART[12],REDUCED_IT=(?:true|false)(?:" + re.escape(hook) + r")?", options):
        raise ValueError("Invalid remote test environment")
    version = env["BASE_SPARK_VERSION"]
    if not re.fullmatch(SPARK_VERSION, version):
        raise ValueError("Unexpected Spark build version")
    ssh = ["ssh", "-F", env["DATABRICKS_SSH_CONFIG"], "-o", "ClearAllForwardings=yes",
           "-o", "ForwardAgent=no", "ubuntu@" + address]

    def remote(command, source=None, timeout=30 * 60, stdout=subprocess.DEVNULL):
        try:
            return subprocess.run(ssh + [command], stdin=source or subprocess.DEVNULL, env=CLEAN_ENV,
                                  cwd=directory, stdout=stdout, stderr=subprocess.DEVNULL,
                                  timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            return 124

    def upload(local, dest):
        with local.open("rb") as source:
            return remote("umask 077; cat > " + dest, source)

    # Each cell builds its own artifact.
    home = "/home/ubuntu/"
    remote_env = ["env", "BASE_SPARK_VERSION=" + version, "EXTRA_ENVS=" + options]
    name = None
    if phase == "build":
        name = "apache-maven-3.8.1-bin.tar.gz"
    elif phase == "test" and options.startswith("TEST_MODE=CI_PART1,"):
        # Matches the upstream two-shim smoke in jenkins/databricks/test.sh.
        name = "spark-3.5.0-bin-hadoop3-scala2.13.tgz" if version.startswith("4.") else "spark-3.3.0-bin-hadoop3.tgz"
    if name:
        catalog = json.loads(Path(__file__).with_name("archives.json").read_text())
        if name not in catalog:
            raise ValueError(f"No trusted archive metadata for {name}")
        local_cache = home + ".cache/spark-ci"
        rc = stage_archive(remote, phase, name, catalog[name], local_cache)
        if rc:
            return rc
        remote_env.append("DBFS_CACHE_DIR=" + local_cache)

    if phase == "build":
        rc = upload(archive, home + "spark-rapids-ci.tgz") or upload(DB / "build.sh", home + "build.sh")
        command = remote_env + ["SPARKSRCTGZ=" + home + "spark-rapids-ci.tgz", "MVN_OPT=",
                                "BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=" + version, "bash", home + "build.sh"]
    elif phase == "test":
        rc = (upload(DB / "test.sh", home + "test.sh")
              or upload(DB / "run-test-command.sh", home + "run-test-command.sh"))
        command = remote_env + ["TEST_TYPE=pre-commit", "LOCAL_JAR_PATH=", "SPARK_CONF=", "bash", home + "test.sh"]
    else:
        raise ValueError("Unknown Databricks workload phase")
    # DBR 18.3 part 2 has successful test commands longer than three hours.
    limit = (270 if phase == "test" else 75) * 60
    if rc:
        return rc
    logdir = "/tmp/db-ci-console-" + secrets.token_hex(16)
    receipt = logdir + "/console.json"
    wrapper = shlex.join(["/usr/bin/python3", "-I", logdir + "/capture.py", receipt])
    supervisor = "bash /home/ubuntu/run-test-command.sh " if phase == "test" else ""
    with Path(__file__).with_name("capture_console.py").open("rb") as capture:
        # Keep the original umask and supervisor even when optional capture setup fails.
        rc = remote(f"umask 077; cd /home/ubuntu || exit 1; set -- {shlex.join(command)}; "
                    f"if mkdir {logdir} && cat > {logdir}/capture.py; then "
                    f'set -- {wrapper} "$@"; fi; exec {supervisor}"$@" </dev/null', source=capture, timeout=limit)
    if rc < 0 or rc in (124, 255):
        return rc
    if phase == "test":
        # Collect counts even when pytest failed, without masking the original exit.
        try:
            summary_rc = check_pytest(ssh, directory)
        except (OSError, subprocess.SubprocessError):
            summary_rc = 1
        if summary_rc and not rc:
            print(f"No complete passing pytest JUnit summary (check exit={summary_rc})", flush=True)
        rc = rc or summary_rc
    try:
        report_console(ssh, directory, receipt, phase, rc)
    except (OSError, ValueError, RecursionError, subprocess.SubprocessError):
        print(f"Databricks {phase} console unavailable (phase exit={rc})", flush=True)
    return rc
