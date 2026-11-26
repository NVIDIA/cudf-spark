# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prepare the Apache binary used by prepare_spark; never extract or execute it."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def archive(scala, distribution="standalone"):
    release = json.loads(Path(__file__).with_name("releases.json").read_text())[scala]
    suffix = ""
    if distribution == "databricks-smoke" and distribution in release:
        release = release[distribution]
        suffix = f"-scala{scala}"
    elif distribution != "standalone":
        raise ValueError(f"Unsupported Spark distribution {distribution} for Scala {scala}")
    version, digest = release["version"], release["sha512"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version) or not re.fullmatch(r"[0-9a-f]{128}", digest):
        raise ValueError(f"Invalid Spark release or SHA-512 for Scala {scala}")
    name = f"spark-{version}-bin-hadoop3{suffix}.tgz"
    return name, f"https://archive.apache.org/dist/spark/spark-{version}/{name}", digest


def stream_sha512(stream):
    digest = hashlib.sha512()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def sha512(path):
    with path.open("rb") as stream:
        return stream_sha512(stream)


def download(url, path):
    attempt_timeout = 60 * 60  # Only a cold or invalid archive needs a network transfer.
    retry_window = attempt_timeout + 5 * 60
    # curl can start a final attempt just before its retry window closes.
    process_timeout = retry_window + attempt_timeout + 60
    subprocess.run([
        "curl", "--fail", "--silent", "--show-error", "--location",
        "--proto", "=https", "--proto-redir", "=https", "--connect-timeout", "15",
        "--max-time", str(attempt_timeout), "--retry", "2", "--retry-max-time", str(retry_window),
        "--output", str(path), url,
    ], check=True, timeout=process_timeout)


def check_path(path):
    if path.resolve() != path:
        raise ValueError(f"Refusing symlink at Spark archive location: {path}")
    if path.exists() and not path.is_file():
        raise ValueError(f"Spark archive location is not a regular file: {path}")


def ensure(path, url, expected, fetch=download):
    check_path(path)
    if path.is_file():
        observed = sha512(path)
        if observed == expected:
            print(f"Verified local Spark archive: {path}")
            return
        print(f"Cached {path.name}: expected SHA-512 {expected}, found {observed}; "
              "downloading a replacement.", file=sys.stderr)
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url}", flush=True)
    with tempfile.TemporaryDirectory(dir=path.parent) as tmp:
        candidate = Path(tmp) / path.name
        fetch(url, candidate)
        observed = sha512(candidate)
        if observed != expected:
            raise ValueError(f"{path.name}: expected SHA-512 {expected}, downloaded {observed}")
        check_path(path)
        candidate.replace(path)
    print(f"Verified downloaded Spark archive: {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("metadata", "ensure"))
    parser.add_argument("scala", choices=("2.12", "2.13"))
    parser.add_argument("--distribution", choices=("standalone", "databricks-smoke"), default="standalone")
    args = parser.parse_args()
    name, url, digest = archive(args.scala, args.distribution)
    path = Path.home().resolve() / ".cache/cudf-spark/spark" / name
    if args.operation == "metadata":
        check_path(path)
        print(f"name={name}\npath={path}\nkey=spark-archive-v1-{name}-{digest}")
    else:
        ensure(path, url, digest)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as e:
        sys.exit(f"Preparing Spark archive failed: {e}")
