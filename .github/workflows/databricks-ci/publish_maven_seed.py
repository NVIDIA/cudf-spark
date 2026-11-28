# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Upload a trusted Maven dependency seed without allocating any Databricks compute."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "jenkins/databricks"))
from https_transport_v2 import workspace_api
from maven_seed_v2 import DEFAULT_ARCHIVE, pack

PATH = DEFAULT_ARCHIVE.removeprefix("/dbfs")
PROFILES = ("aws2", "aws", "azure3")


def publish(host, token, file, same, path=PATH):
    api = workspace_api(host, token)  # Enforce HTTPS workspace allowlist before passing any token.
    if same:
        try:
            status = api("GET", "/api/2.0/dbfs/get-status", path=path)
            if status.get("is_dir") is False and status.get("file_size") == file.stat().st_size:
                return "unchanged size; driver verifies the digest"
        except RuntimeError:
            pass
    api("POST", "/api/2.0/dbfs/mkdirs", path=path.rsplit("/", 1)[0])
    # Stream multipart bytes from disk; the secret is only on stdin. Never follow redirects.
    result = subprocess.run(["curl", "-q", "--silent", "--show-error", "--proto", "=https",
                             "--connect-timeout", "15", "--max-time", "1200", "--max-filesize", "65536",
                             "--header", "@-", "--form-string", "path=" + path,
                             "--form-string", "overwrite=true", "--form", "contents=@" + str(file),
                             "--output", os.devnull, "--write-out", "%{http_code}",
                             host.rstrip("/") + "/api/2.0/dbfs/put"],
                            input="Authorization: Bearer " + token + "\n", capture_output=True, text=True)
    if result.returncode or result.stdout != "200":
        raise RuntimeError(f"DBFS upload failed (curl={result.returncode}, HTTP={result.stdout[:3]})")
    status = api("GET", "/api/2.0/dbfs/get-status", path=path)
    if status.get("is_dir") is not False or status.get("file_size") != file.stat().st_size:
        raise RuntimeError("DBFS seed size not confirmed")
    return "uploaded"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.home() / ".m2/repository")
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--single", action="store_true", help="Use DATABRICKS_HOST/TOKEN for one workspace")
    parser.add_argument("--force", action="store_true", help="Replace an existing seed even when its size matches")
    parser.add_argument("--directory", default=PATH.rsplit("/", 1)[0], help="Dedicated DBFS seed directory")
    args = parser.parse_args()
    if not re.fullmatch(r"(/[A-Za-z0-9_-]+)+", args.directory):
        parser.error("Expected an absolute DBFS seed directory")
    profiles = {"workspace": {"host": os.environ["DATABRICKS_HOST"]}} if args.single else {
        k: json.loads(os.environ["DATABRICKS_PROFILES_JSON"])[k] for k in PROFILES}
    old = args.receipt.read_text().strip() if args.receipt.is_file() else ""
    with tempfile.TemporaryDirectory() as temp:
        archive = Path(temp) / "seed.tar"
        count = pack(args.repository, archive)
        digest = hashlib.sha256()
        with archive.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        sha = digest.hexdigest()
        failed = []
        for profile, settings in profiles.items():
            try:
                token = os.environ["DATABRICKS_TOKEN" if args.single else f"DATABRICKS_{profile.upper()}_TOKEN"]
                result = publish(settings["host"], token, archive, not args.force and old == sha,
                                 args.directory + "/" + Path(PATH).name)
                print(f"{profile}: {result}; {count} released files, {archive.stat().st_size} bytes")
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                print(f"{profile}: seed not confirmed ({exc})", file=sys.stderr)
                failed.append(profile)
        if failed:
            sys.exit("Maven seed incomplete: " + ", ".join(failed))
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(sha + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as out:
            print(f"digest={sha}", file=out)


if __name__ == "__main__":
    main()
