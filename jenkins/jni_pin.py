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

"""Update and verify the cudf-spark last-known-good JNI candidate pin."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
from typing import Callable, Optional
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET


PROPERTY = "cudf-spark-jni.version"
MAVEN_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}
CANDIDATE_RE = re.compile(
    r"^(?P<base>\d+\.\d+\.\d+)-jni\.(?P<sha>[0-9a-f]{12})\.r(?P<revision>[1-9]\d*)-SNAPSHOT$"
)
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
CHECKSUM_RE = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_REPOSITORIES = {"sonatype", "urm"}
GROUP_PATH = "com/nvidia"
ARTIFACT_ID = "cudf-spark-jni"


def pom_pin(pom: Path) -> str:
    root = ET.parse(pom).getroot()
    value = root.find(f"m:properties/m:{PROPERTY}", MAVEN_NAMESPACE)
    if value is None or not value.text:
        raise ValueError(f"{PROPERTY} is missing from {pom}")
    return value.text.strip()


def replace_pin(pom: Path, candidate: str) -> None:
    current = pom_pin(pom)
    text = pom.read_text()
    marker = f"<{PROPERTY}>{current}</{PROPERTY}>"
    if text.count(marker) != 1:
        raise ValueError(f"expected exactly one {PROPERTY} marker in {pom}")
    pom.write_text(text.replace(marker, f"<{PROPERTY}>{candidate}</{PROPERTY}>", 1))


def checksum_map(repository: dict) -> dict:
    artifacts = repository.get("artifacts", [])
    checksums = {
        artifact["publishedName"]: artifact["sha256"] for artifact in artifacts
    }
    if len(checksums) != len(artifacts):
        raise ValueError("artifact list contains duplicate published names")
    if any(not CHECKSUM_RE.fullmatch(value) for value in checksums.values()):
        raise ValueError("artifact list contains an invalid SHA-256 checksum")
    return checksums


def snapshot_versions(metadata: bytes) -> dict:
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


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


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


def url_fetcher(authorization: Optional[str]) -> Callable[[str], bytes]:
    def fetch(url: str) -> bytes:
        request = urllib.request.Request(url)
        if authorization:
            request.add_header("Authorization", authorization)
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()

    return fetch


def validate_manifest(manifest: dict) -> str:
    if manifest.get("schemaVersion") != 1:
        raise ValueError("promotion manifest schemaVersion must be 1")
    candidate = manifest.get("candidateId", "")
    match = CANDIDATE_RE.fullmatch(candidate)
    if not match:
        raise ValueError("promotion manifest has an invalid candidateId")
    source = manifest.get("source", {})
    jni_sha = source.get("jniCommit", "")
    cudf_sha = source.get("cudfCommit", "")
    if not SHA_RE.fullmatch(jni_sha) or not SHA_RE.fullmatch(cudf_sha):
        raise ValueError("promotion manifest must contain full JNI and cuDF commit SHAs")
    if not jni_sha.startswith(match.group("sha")):
        raise ValueError("candidateId JNI SHA does not match source.jniCommit")
    if manifest.get("artifactRevision") != int(match.group("revision")):
        raise ValueError("candidateId revision does not match artifactRevision")
    required_classifiers = manifest.get("requiredClassifiers", [])
    if not required_classifiers or len(set(required_classifiers)) != len(required_classifiers):
        raise ValueError("requiredClassifiers must be a non-empty unique list")
    if manifest.get("defaultClassifier") not in required_classifiers:
        raise ValueError("defaultClassifier must be one of requiredClassifiers")
    if manifest.get("integrity", {}).get("state") != "VERIFIED":
        raise ValueError("promotion manifest integrity state is not VERIFIED")

    repositories = manifest.get("repositories", {})
    if set(repositories) != REQUIRED_REPOSITORIES:
        raise ValueError("promotion manifest must contain exactly Sonatype and URM results")
    for name, repository in repositories.items():
        if repository.get("state") != "COMPLETE":
            raise ValueError(f"{name} publication is not COMPLETE")
    sonatype = checksum_map(repositories["sonatype"])
    urm = checksum_map(repositories["urm"])
    if not sonatype or sonatype != urm:
        raise ValueError("Sonatype and URM artifact checksums do not match")

    build_entries = manifest.get("artifacts", [])
    build_artifacts = checksum_map({"artifacts": build_entries})
    coordinates = {
        (artifact.get("extension"), artifact.get("classifier"))
        for artifact in build_entries
    }
    expected_coordinates = {
        ("jar", None),
        ("jar", "sources"),
        ("jar", "javadoc"),
        ("pom", None),
        *(("jar", classifier) for classifier in required_classifiers),
    }
    if coordinates != expected_coordinates or len(coordinates) != len(build_entries):
        raise ValueError("build manifest does not contain the exact required artifact set")
    if build_artifacts != sonatype:
        raise ValueError("published artifact checksums do not match the build manifest")
    return candidate


def verify_repository(
    manifest: dict,
    repository_name: str,
    base_url: str,
    fetch: Callable[[str], bytes],
    attempts: int,
    wait: int,
) -> int:
    candidate = validate_manifest(manifest)
    repository = manifest["repositories"][repository_name]
    published = {artifact["publishedName"]: artifact for artifact in repository["artifacts"]}
    artifact_base = "/".join(
        [base_url.rstrip("/"), GROUP_PATH, ARTIFACT_ID, candidate]
    )
    metadata = retry_fetch(fetch, f"{artifact_base}/maven-metadata.xml", attempts, wait)
    current_versions = snapshot_versions(metadata)
    for artifact in manifest["artifacts"]:
        extension = artifact["extension"]
        classifier = artifact.get("classifier")
        resolved_version = current_versions.get((extension, classifier))
        if not resolved_version:
            raise ValueError(
                f"{repository_name} metadata no longer contains {extension}/{classifier}"
            )
        classifier_suffix = f"-{classifier}" if classifier else ""
        current_name = f"{ARTIFACT_ID}-{resolved_version}{classifier_suffix}.{extension}"
        recorded = published[artifact["publishedName"]]
        if recorded.get("resolvedName") != current_name:
            raise ValueError(
                f"{repository_name} metadata moved for {artifact['publishedName']}: "
                f"expected {recorded.get('resolvedName')}, got {current_name}"
            )
        content = retry_fetch(fetch, f"{artifact_base}/{current_name}", attempts, wait)
        actual_sha = sha256(content)
        if actual_sha != artifact["sha256"]:
            raise ValueError(
                f"{repository_name} checksum changed for {artifact['publishedName']}"
            )
    return len(manifest["artifacts"])


def check(poms: list[Path], provenance: Path) -> str:
    pins = {pom_pin(pom) for pom in poms}
    if len(pins) != 1:
        raise ValueError("Scala 2.12 and 2.13 POMs use different JNI pins")
    pin = pins.pop()
    if not CANDIDATE_RE.fullmatch(pin):
        if provenance.exists():
            raise ValueError("candidate provenance exists but the POM pin is not a candidate")
        return pin
    if not provenance.is_file():
        raise ValueError("candidate JNI pin requires a promotion manifest")
    manifest = json.loads(provenance.read_text())
    candidate = validate_manifest(manifest)
    if candidate != pin:
        raise ValueError(f"POM pin {pin} does not match manifest candidate {candidate}")
    return pin


def update(poms: list[Path], manifest_path: Path, provenance: Path) -> str:
    manifest = json.loads(manifest_path.read_text())
    candidate = validate_manifest(manifest)
    current_pins = {pom_pin(pom) for pom in poms}
    if len(current_pins) != 1:
        raise ValueError("refusing to update POMs that currently use different JNI pins")
    for pom in poms:
        replace_pin(pom, candidate)
    provenance.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(manifest_path, provenance)
    check(poms, provenance)
    return candidate


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pom", action="append", type=Path, required=True)
    parser.add_argument(
        "--provenance",
        type=Path,
        default=Path("jenkins/jni-candidate-manifest.json"),
    )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    check_parser = commands.add_parser("check")
    add_common_arguments(check_parser)
    update_parser = commands.add_parser("update")
    add_common_arguments(update_parser)
    update_parser.add_argument("--manifest", type=Path, required=True)
    repository_parser = commands.add_parser("verify-repository")
    add_common_arguments(repository_parser)
    repository_parser.add_argument(
        "--repository", choices=sorted(REQUIRED_REPOSITORIES), required=True
    )
    repository_parser.add_argument("--base-url", required=True)
    repository_parser.add_argument(
        "--basic-auth-env",
        help="Comma-separated environment variable names for username and password",
    )
    repository_parser.add_argument("--attempts", type=int, default=5)
    repository_parser.add_argument("--wait-seconds", type=int, default=5)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        if args.command == "check":
            pin = check(args.pom, args.provenance)
            print(f"verified JNI pin: {pin}")
        elif args.command == "update":
            pin = update(args.pom, args.manifest, args.provenance)
            print(f"verified JNI pin: {pin}")
        else:
            pin = check(args.pom, args.provenance)
            if not CANDIDATE_RE.fullmatch(pin):
                print(f"repository readback skipped for non-candidate JNI pin: {pin}")
                return 0
            authorization = None
            if args.basic_auth_env:
                variables = args.basic_auth_env.split(",")
                if len(variables) != 2 or not all(variables):
                    raise ValueError("basic auth requires USERNAME_ENV,PASSWORD_ENV")
                username = os.environ.get(variables[0])
                password = os.environ.get(variables[1])
                if not username or not password:
                    raise ValueError("repository credentials are not available")
                token = base64.b64encode(f"{username}:{password}".encode()).decode()
                authorization = f"Basic {token}"
            manifest = json.loads(args.provenance.read_text())
            count = verify_repository(
                manifest,
                args.repository,
                args.base_url,
                url_fetcher(authorization),
                args.attempts,
                args.wait_seconds,
            )
            print(f"verified {count} {args.repository} artifacts for JNI pin: {pin}")
    except (
        OSError,
        RuntimeError,
        ValueError,
        ET.ParseError,
        json.JSONDecodeError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
