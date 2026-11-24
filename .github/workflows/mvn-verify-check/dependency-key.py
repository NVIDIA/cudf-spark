# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fingerprint the separately published plugin dependencies used by premerge."""

import argparse
import hashlib
import json
from pathlib import Path
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


def dependency_key(pom, resolve=snapshot_candidates):
    props = pom.find("{*}properties")
    snapshots = next((repo.findtext("{*}url") for repo in pom.findall("{*}repositories/{*}repository")
                      if repo.findtext("{*}id") == "snapshots-repo"), None)
    coordinates = [(props.findtext(f"{{*}}{name}.artifactId"), props.findtext(f"{{*}}{name}.version"))
                   for name in ("cudf-spark-jni", "cudf-spark-private")]
    history_version = props.findtext("{*}history-metrics.version")
    if history_version:
        coordinates.append(("cudf-spark-history-metrics-local", history_version))
    identities = []
    for artifact, version in coordinates:
        if not artifact or not version or "${" in artifact + version:
            raise ValueError(f"Unresolved upstream coordinates in effective POM: {artifact}:{version}")
        candidates = []
        if version.endswith("-SNAPSHOT"):
            if not snapshots:
                raise ValueError(f"No snapshots-repo configured for {artifact}:{version}")
            candidates = resolve(snapshots, artifact, version)
        identities.append([artifact, version, candidates])
    return hashlib.sha256(json.dumps(identities).encode()).hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scala", choices=("2.12", "2.13"))
    scala = parser.parse_args().scala
    pom_file = "pom.xml" if scala == "2.12" else "scala2.13/pom.xml"
    with tempfile.TemporaryDirectory() as temp:
        effective = Path(temp) / "effective.xml"
        subprocess.run(["mvn", "-B", "-q", "-N", "-f", pom_file,
                        "org.apache.maven.plugins:maven-help-plugin:3.5.1:effective-pom",
                        f"-Doutput={effective}"], check=True, stdout=sys.stderr)
        print(dependency_key(ET.parse(effective).getroot()))
