# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prepare immutable build inputs locally; run the product only on the allocated DB driver."""

import json
import os
from pathlib import Path
import re
import runpy
import shlex
import subprocess
import tarfile
from urllib.parse import urlsplit


DB = Path(__file__).resolve().parents[3] / "jenkins/databricks"
SUBMODULES = {"thirdparty/parquet-testing", "thirdparty/spark-rapids-common"}
SPARK = {"17.3": "4.0.0", "18.3": "4.1.0"}
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


def attempt_env(profile, runtime, part, reduced, directory):
    creator_name(profile)  # Refuse new allocations when their owner cannot be verified by the sweeper.
    if runtime not in SPARK or part not in ("CI_PART1", "CI_PART2") or type(reduced) is not bool:
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
            "DATABRICKS_RUNTIME": runtime + ".x-gpu-ml-scala2.13", "BASE_SPARK_VERSION": SPARK[runtime],
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


def stage_smoke_archive(remote, cache_dir):
    # Use the same trusted catalog and filename as the warmer, not the DB runtime's Spark version.
    reader = runpy.run_path(str(DB.parents[1] / ".github/actions/spark-archive-cache/archive.py"))
    name, url, digest = reader["archive"]("2.13", "databricks-smoke")
    target = f"{cache_dir}/{name}"
    path = Path.home().resolve() / ".cache/cudf-spark/spark" / name

    def stage(producer, source, timeout):
        # A broken SSH stream or failed download must never leave a reusable partial tarball.
        cmd = f"""set -eu
umask 077
mkdir -p {shlex.quote(cache_dir)}
cd {shlex.quote(cache_dir)}
if [ -f {name} ] && printf '%s  %s\\n' {digest} {name} | sha512sum -c -; then exit 0; fi
[ ! -d {name} ]
tmp=$(mktemp .spark.XXXXXXXX)
trap 'rm -f -- "$tmp"' EXIT
{producer}
printf '%s  %s\\n' {digest} "$tmp" | sha512sum -c -
mv -f "$tmp" {name}"""
        return remote(cmd, source, timeout=timeout)

    if os.environ.get("SPARK_CACHE_FILE") == str(path):
        try:
            reader["check_path"](path)
            if path.is_file():
                with path.open("rb") as source:
                    if reader["stream_sha512"](source) == digest:
                        source.seek(0)
                        rc = stage('cat > "$tmp"', source, 600)
                        if rc == 0:
                            print(f"Verified prepared {name} on the driver.", flush=True)
                            return 0, target
                        print(f"Prepared Spark transfer failed (exit={rc}); trying Apache on the driver.", flush=True)
        except (OSError, ValueError):
            pass  # Cache availability is optional; a verified distribution is not.
    print(f"Preparing {name} on the driver from the Apache release.", flush=True)
    rc = stage(f'curl --fail --silent --show-error --location --proto =https --proto-redir =https '
               f'--connect-timeout 15 --max-time 3600 -o "$tmp" {shlex.quote(url)}', None, 3660)
    if rc:
        print(f"Could not prepare the checksum-verified Spark smoke archive on the driver (exit={rc}).", flush=True)
    return rc, target


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
    if version not in SPARK.values():
        raise ValueError("Unexpected Spark build version")
    ssh = ["ssh", "-F", env["DATABRICKS_SSH_CONFIG"], "-o", "ClearAllForwardings=yes",
           "-o", "ForwardAgent=no", "ubuntu@" + address]

    def remote(command, source=None, timeout=30 * 60):
        try:
            return subprocess.run(ssh + [command], stdin=source or subprocess.DEVNULL, env=CLEAN_ENV,
                                  cwd=directory, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                  timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            return 124

    def upload(local, dest):
        with local.open("rb") as source:
            return remote("umask 077; cat > " + dest, source)

    # Each cell builds its own artifact.
    home = "/home/ubuntu/"
    remote_env = ["env", "BASE_SPARK_VERSION=" + version, "EXTRA_ENVS=" + options,
                  "DBFS_CACHE_DIR=" + home + ".cache/spark-ci"]
    if phase == "build":
        rc = upload(archive, home + "spark-rapids-ci.tgz") or upload(DB / "build.sh", home + "build.sh")
        command = remote_env + ["SPARKSRCTGZ=" + home + "spark-rapids-ci.tgz", "MVN_OPT=",
                                "BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=" + version, "bash", home + "build.sh"]
    elif phase == "test":
        rc = (upload(DB / "test.sh", home + "test.sh")
              or upload(DB / "run-test-command.sh", home + "run-test-command.sh"))
        if not rc and options.startswith("TEST_MODE=CI_PART1,"):
            rc, smoke_path = stage_smoke_archive(remote, home + ".cache/spark-ci")
            remote_env.append("SPARK_CACHE_FILE=" + smoke_path)
        command = ["bash", home + "run-test-command.sh", *remote_env,
                   "TEST_TYPE=pre-commit", "LOCAL_JAR_PATH=", "SPARK_CONF=", "bash", home + "test.sh"]
    else:
        raise ValueError("Unknown Databricks workload phase")
    redirection = ">buildout 2>&1" if phase == "build" else ">/dev/null 2>&1"
    # DBR 18.3 part 2 has successful test commands longer than three hours.
    limit = (270 if phase == "test" else 75) * 60
    rc = rc or remote("umask 077; cd /home/ubuntu && " + shlex.join(command) + " " + redirection, timeout=limit)
    if phase == "test" and not rc:
        rc = check_pytest(ssh, directory)
        if rc:
            print(f"No complete passing pytest JUnit summary (check exit={rc})", flush=True)
    return rc
