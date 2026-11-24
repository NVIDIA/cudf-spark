# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fingerprint the separately published plugin dependencies used by premerge."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from xml.etree import ElementTree as ET


def snapshot_candidates(url, artifact, version):
    metadata_url = f"{url.rstrip('/')}/com/nvidia/{artifact}/{version}/maven-metadata.xml"
    data = subprocess.check_output(["curl", "--fail", "--silent", "--show-error", "--location",
                                    "--max-time", "15", "--retry", "2", metadata_url])
    root = ET.fromstring(data)
    if [root.findtext(k) for k in ("groupId", "artifactId", "version")] != ["com.nvidia", artifact, version]:
        raise ValueError(f"Unexpected snapshot coordinates at {metadata_url}")
    # Classifier uploads need not arrive together. A single unclassified JAR
    # (or just the last entry in the XML) does not identify the whole build.
    candidates = sorted((entry.findtext("extension"), entry.findtext("classifier", ""),
                         entry.findtext("value", ""))
                        for entry in root.findall("versioning/snapshotVersions/snapshotVersion")
                        if entry.findtext("extension") in ("jar", "pom"))
    if not any(ext == "jar" for ext, _, _ in candidates) or not all(value for _, _, value in candidates):
        raise ValueError(f"No usable snapshot JAR metadata at {metadata_url}")
    return candidates


def tracked_coordinates(pom):
    props = pom.find("{*}properties")
    coordinates = [(props.findtext(f"{{*}}{name}.artifactId"), props.findtext(f"{{*}}{name}.version"))
                   for name in ("cudf-spark-jni", "cudf-spark-private")]
    history_version = props.findtext("{*}history-metrics.version")
    if history_version:
        coordinates.append(("cudf-spark-history-metrics-local", history_version))
    for artifact, version in coordinates:
        if not all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+-]*", part or "") for part in (artifact, version)):
            raise ValueError(f"Invalid upstream coordinates in effective POM: {artifact}:{version}")
    return coordinates


def discard_restored_snapshots(pom, home, current="", prefix="", restored=""):
    # Only keys in our verified format carry provenance. The digest covers the
    # coordinate as well as the publication, so it cannot match a different GAV.
    fingerprint = r"s1(?:\.[0-9a-f]{64}){2,3}"
    previous = re.fullmatch(re.escape(prefix) + f"({fingerprint})-" +
                            r"[0-9a-f]{64}-(?:verify|premerge-[A-Za-z0-9._+-]+-[0-9a-f]{64})", restored)
    proven = set(previous[1].split(".")[1:]) if prefix and previous else set()
    coordinates = tracked_coordinates(pom)
    current_ids = current.split(".")[1:] if re.fullmatch(fingerprint, current) else []
    group = home.resolve() / ".m2/repository/com/nvidia"
    for index, (artifact, version) in enumerate(coordinates):
        if version.endswith("-SNAPSHOT"):
            directory = group / artifact / version
            if directory.resolve() != directory:
                raise ValueError(f"Refusing to clean a symlinked Maven path: {directory}")
            if len(current_ids) == len(coordinates) and current_ids[index] in proven:
                continue
            if directory.exists():
                if os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
                    raise ValueError("Refusing to clean restored SNAPSHOTs on a non-disposable runner")
                shutil.rmtree(directory)


def dependency_key(pom, resolve=snapshot_candidates):
    snapshots = next((repo.findtext("{*}url") for repo in pom.findall("{*}repositories/{*}repository")
                      if repo.findtext("{*}id") == "snapshots-repo"), None)
    # The slots are JNI, private, and optional history-metrics, in tracked_coordinates order.
    identities = ["s1"]
    for artifact, version in tracked_coordinates(pom):
        candidates = []
        if version.endswith("-SNAPSHOT"):
            if not snapshots:
                raise ValueError(f"No snapshots-repo configured for {artifact}:{version}")
            candidates = resolve(snapshots, artifact, version)
        publication = [artifact, version, snapshots if candidates else None, candidates]
        identities.append(hashlib.sha256(json.dumps(publication).encode()).hexdigest())
    return ".".join(identities)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("scala", nargs="?", choices=("2.12", "2.13"))
    mode.add_argument("--discard-restored", type=Path, metavar="EFFECTIVE_POM",
                      help="Remove tracked SNAPSHOT versions from an older restored cache")
    mode.add_argument("--confirm-snapshots", nargs=2, metavar=("EFFECTIVE_POM", "EXPECTED_HASH"),
                      help="Check the current upstream fingerprint before saving a warmed cache")
    parser.add_argument("--keep-model", type=Path, metavar="FILE",
                        help="Keep the effective POM used for fingerprinting for use after restore")
    args = parser.parse_args()
    if args.discard_restored:
        discard_restored_snapshots(ET.parse(args.discard_restored).getroot(), Path.home(),
                                   os.environ.get("EXPECTED", ""), os.environ.get("CACHE_PREFIX", ""),
                                   os.environ.get("RESTORED_KEY", ""))
        sys.exit(0)
    if args.confirm_snapshots:
        model, expected = args.confirm_snapshots
        actual = dependency_key(ET.parse(model).getroot())
        if actual != expected:
            sys.exit(f"Upstream SNAPSHOTs changed during warm-up ({expected} -> {actual}); not saving Maven cache")
        sys.exit(0)
    pom_file = "pom.xml" if args.scala == "2.12" else "scala2.13/pom.xml"
    with tempfile.TemporaryDirectory() as temp:
        effective = args.keep_model or Path(temp) / "effective.xml"
        subprocess.run(["mvn", "-B", "-q", "-N", "-f", pom_file,
                        "org.apache.maven.plugins:maven-help-plugin:3.5.1:effective-pom",
                        f"-Doutput={effective}"], check=True, stdout=sys.stderr)
        print(dependency_key(ET.parse(effective).getroot()))
