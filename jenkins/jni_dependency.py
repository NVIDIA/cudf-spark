#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Validate the JNI submodule pointer and its published SNAPSHOT provenance."""

import argparse
import base64
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Callable, Dict, Iterable, Optional, Tuple
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile


GROUP_ID = "com.nvidia"
ARTIFACT_ID = "cudf-spark-jni"
DEFAULT_SUBMODULE = "thirdparty/cudf-spark-jni"
PROVENANCE_CLASSIFIER = "provenance"
PROVENANCE_EXTENSION = "json"
PROPERTIES_PATH = "cudf-spark-jni-version-info.properties"
SHA_RE = re.compile(r"[0-9a-f]{40}")
MOVING_SNAPSHOT_RE = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT")


def run_git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def submodule_sha(repo_root: Path, submodule: str) -> str:
    entry = run_git(repo_root, "ls-files", "--stage", "--", submodule)
    fields = entry.split()
    if len(fields) < 4 or fields[0] != "160000" or fields[3] != submodule:
        raise ValueError(f"{submodule} is not a tracked Git submodule")
    sha = fields[1].lower()
    if not SHA_RE.fullmatch(sha):
        raise ValueError(f"invalid submodule commit: {sha}")
    return sha


def pom_jni_version(pom: Path) -> str:
    root = ET.parse(pom).getroot()
    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.partition("}")[0] + "}"
    value = root.findtext(f"{namespace}properties/{namespace}cudf-spark-jni.version")
    if not value:
        raise ValueError(f"{pom} does not define cudf-spark-jni.version")
    return value.strip()


def dependency_state(repo_root: Path, submodule: str, poms: Iterable[Path]) -> dict:
    versions = {pom_jni_version(pom) for pom in poms}
    if len(versions) != 1:
        raise ValueError(f"JNI versions differ across POMs: {sorted(versions)}")
    version = versions.pop()
    if not MOVING_SNAPSHOT_RE.fullmatch(version):
        raise ValueError(
            f"JNI version must remain a release-train moving SNAPSHOT, got {version}"
        )
    return {
        "schemaVersion": 1,
        "jniSha": submodule_sha(repo_root, submodule),
        "logicalVersion": version,
        "submodulePath": submodule,
    }


def snapshot_versions(metadata: bytes) -> Dict[Tuple[str, Optional[str]], str]:
    root = ET.fromstring(metadata)
    versions = {}
    for item in root.findall("./versioning/snapshotVersions/snapshotVersion"):
        extension = item.findtext("extension")
        classifier = item.findtext("classifier")
        value = item.findtext("value")
        if extension and value:
            versions[(extension, classifier)] = value
    if not versions:
        raise ValueError("Maven metadata contains no snapshotVersions")
    return versions


def validate_provenance(provenance: dict, expected: dict) -> dict:
    if provenance.get("schemaVersion") != 1:
        raise ValueError("unsupported JNI provenance schema")
    if provenance.get("logicalVersion") != expected["logicalVersion"]:
        raise ValueError(
            "JNI provenance version mismatch: "
            f"expected {expected['logicalVersion']}, got {provenance.get('logicalVersion')}"
        )
    actual_sha = provenance.get("source", {}).get("jniSha", "").lower()
    if actual_sha != expected["jniSha"]:
        raise ValueError(
            "JNI provenance revision mismatch: "
            f"submodule points to {expected['jniSha']}, published artifact reports {actual_sha}"
        )
    fingerprint = provenance.get("build", {}).get("fingerprint")
    if not fingerprint or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise ValueError("JNI provenance does not contain a valid build fingerprint")
    return provenance


def basic_auth_header(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {token}"


def url_fetcher(authorization: Optional[str]) -> Callable[[str], bytes]:
    def fetch(url: str) -> bytes:
        request = urllib.request.Request(url)
        if authorization:
            request.add_header("Authorization", authorization)
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()

    return fetch


def retry_fetch(fetch: Callable[[str], bytes], url: str, attempts: int, wait: int) -> bytes:
    error = None
    for attempt in range(attempts):
        try:
            return fetch(url)
        except (OSError, urllib.error.URLError) as current:
            error = current
            if attempt + 1 < attempts:
                time.sleep(wait)
    raise RuntimeError(f"failed to fetch {url} after {attempts} attempts: {error}")


def repository_provenance(
    base_url: str,
    expected: dict,
    fetch: Callable[[str], bytes],
    attempts: int,
    wait: int,
) -> dict:
    version = expected["logicalVersion"]
    artifact_base = "/".join(
        [base_url.rstrip("/"), GROUP_ID.replace(".", "/"), ARTIFACT_ID, version]
    )
    metadata = retry_fetch(fetch, f"{artifact_base}/maven-metadata.xml", attempts, wait)
    versions = snapshot_versions(metadata)
    resolved = versions.get((PROVENANCE_EXTENSION, PROVENANCE_CLASSIFIER))
    if not resolved:
        raise ValueError("Maven metadata has no JNI provenance snapshot")
    name = f"{ARTIFACT_ID}-{resolved}-{PROVENANCE_CLASSIFIER}.{PROVENANCE_EXTENSION}"
    content = retry_fetch(fetch, f"{artifact_base}/{name}", attempts, wait)
    provenance = json.loads(content)
    validate_provenance(provenance, expected)
    provenance["resolvedProvenance"] = name
    return provenance


def jar_revision(jar: Path) -> str:
    with zipfile.ZipFile(jar) as archive:
        content = archive.read(PROPERTIES_PATH).decode()
    properties = {}
    for line in content.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            properties[key.strip()] = value.strip()
    revision = properties.get("revision", "").lower()
    if not SHA_RE.fullmatch(revision):
        raise ValueError(f"{jar} has no valid JNI revision")
    return revision


def resolve_paths(repo_root: Path, values: Iterable[Path]) -> list:
    return [value if value.is_absolute() else repo_root / value for value in values]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--submodule", default=DEFAULT_SUBMODULE)
    parser.add_argument("--pom", action="append", type=Path, default=[])
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check")
    check.add_argument("--output", type=Path)

    verify_repository = subparsers.add_parser("verify-repository")
    verify_repository.add_argument("--base-url", required=True)
    verify_repository.add_argument("--username-env")
    verify_repository.add_argument("--password-env")
    verify_repository.add_argument("--attempts", type=int, default=1)
    verify_repository.add_argument("--wait-seconds", type=int, default=30)

    verify_jar = subparsers.add_parser("verify-jar")
    verify_jar.add_argument("--jar", action="append", type=Path, required=True)
    return parser.parse_args()


def expected_state(args: argparse.Namespace) -> dict:
    repo_root = args.repo_root.resolve()
    poms = args.pom or [Path("pom.xml"), Path("scala2.13/pom.xml")]
    return dependency_state(repo_root, args.submodule, resolve_paths(repo_root, poms))


def main() -> int:
    args = parse_args()
    try:
        expected = expected_state(args)
        if args.command == "check":
            rendered = json.dumps(expected, indent=2, sort_keys=True) + "\n"
            if args.output:
                args.output.write_text(rendered)
            else:
                print(rendered, end="")
        elif args.command == "verify-repository":
            authorization = None
            if bool(args.username_env) != bool(args.password_env):
                raise ValueError("username-env and password-env must be provided together")
            if args.username_env:
                import os

                username = os.environ.get(args.username_env)
                password = os.environ.get(args.password_env)
                if not username or not password:
                    raise ValueError("repository credentials are not available")
                authorization = basic_auth_header(username, password)
            provenance = repository_provenance(
                args.base_url,
                expected,
                url_fetcher(authorization),
                args.attempts,
                args.wait_seconds,
            )
            print(json.dumps(provenance, indent=2, sort_keys=True))
        elif args.command == "verify-jar":
            for jar in args.jar:
                actual = jar_revision(jar)
                if actual != expected["jniSha"]:
                    raise ValueError(
                        f"{jar} contains JNI revision {actual}; "
                        f"submodule points to {expected['jniSha']}"
                    )
                print(f"VERIFIED {jar} {actual}")
    except (
        KeyError,
        OSError,
        RuntimeError,
        subprocess.CalledProcessError,
        ValueError,
        ET.ParseError,
        zipfile.BadZipFile,
        json.JSONDecodeError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
