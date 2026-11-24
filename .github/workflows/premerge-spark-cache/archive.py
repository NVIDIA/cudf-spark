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


def archive(scala):
    release = json.loads(Path(__file__).with_name("releases.json").read_text())[scala]
    version, digest = release["version"], release["sha512"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version) or not re.fullmatch(r"[0-9a-f]{128}", digest):
        raise ValueError(f"Invalid Spark release or SHA-512 for Scala {scala}")
    name = f"spark-{version}-bin-hadoop3.tgz"
    return name, f"https://archive.apache.org/dist/spark/spark-{version}/{name}", digest


def sha512(path):
    digest = hashlib.sha512()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url, path):
    subprocess.run([
        "curl", "--fail", "--silent", "--show-error", "--location",
        "--proto", "=https", "--proto-redir", "=https", "--connect-timeout", "15",
        "--max-time", "300", "--retry", "2", "--retry-max-time", "480",
        "--output", str(path), url,
    ], check=True, timeout=600)


def ensure(path, url, expected, fetch=download):
    if path.is_symlink():
        raise ValueError(f"Refusing symlink at Spark archive location: {path}")
    if path.is_file():
        observed = sha512(path)
        if observed == expected:
            print(f"Verified local Spark archive: {path}")
            return
        print(f"Cached {path.name}: expected SHA-512 {expected}, found {observed}; "
              "discarding invalid file and downloading a fresh copy.", file=sys.stderr)
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url}", flush=True)
    with tempfile.TemporaryDirectory(dir=path.parent) as tmp:
        candidate = Path(tmp) / path.name
        fetch(url, candidate)
        observed = sha512(candidate)
        if observed != expected:
            raise ValueError(f"{path.name}: expected SHA-512 {expected}, downloaded {observed}")
        candidate.replace(path)
    print(f"Verified downloaded Spark archive: {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("metadata", "ensure"))
    parser.add_argument("scala", choices=("2.12", "2.13"))
    args = parser.parse_args()
    name, url, digest = archive(args.scala)
    path = Path.home() / ".cache/cudf-spark/spark" / name
    if args.operation == "metadata":
        print(f"name={name}\npath={path}\nkey=premerge-spark-v1-{name}-{digest}")
    else:
        ensure(path, url, digest)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as e:
        sys.exit(f"Preparing Spark archive failed: {e}")
