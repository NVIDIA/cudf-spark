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

"""Read and validate the Iceberg integration-test compatibility matrix."""

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = Path(__file__).with_name("iceberg-test-matrix.yaml")
DEFAULT_POM = REPO_ROOT / "pom.xml"
VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SPARK_PROPERTY_PATTERN = re.compile(r"^spark[0-9]+\.version$")


class MatrixError(ValueError):
    pass


def _version_tuple(version):
    if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
        raise MatrixError(f"invalid version: {version!r}")
    return tuple(int(part) for part in version.split("."))


def _spark_family(version):
    return ".".join(version.split(".")[:2])


def read_spark_shims(pom_path):
    root = ET.parse(pom_path).getroot()
    namespace = {"pom": "http://maven.apache.org/POM/4.0.0"}
    properties = root.find("pom:properties", namespace)
    if properties is None:
        raise MatrixError(f"properties not found in {pom_path}")

    versions = set()
    for child in properties:
        name = child.tag.rsplit("}", 1)[-1]
        value = (child.text or "").strip()
        if SPARK_PROPERTY_PATTERN.fullmatch(name) and VERSION_PATTERN.fullmatch(value):
            versions.add(value)
    return versions


def load_matrix(matrix_path=DEFAULT_MATRIX, pom_path=DEFAULT_POM):
    with open(matrix_path, encoding="utf-8") as stream:
        document = yaml.safe_load(stream)
    if not isinstance(document, dict) or set(document) != {"iceberg_versions"}:
        raise MatrixError("matrix must contain only an iceberg_versions list")
    entries = document["iceberg_versions"]
    if not isinstance(entries, list) or not entries:
        raise MatrixError("iceberg_versions must be a non-empty list")

    spark_shims = read_spark_shims(pom_path)
    seen_iceberg_versions = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {
                "version", "upstream_minimums", "spark_versions"}:
            raise MatrixError(
                "each Iceberg entry requires version, upstream_minimums, and spark_versions")
        iceberg_version = entry["version"]
        _version_tuple(iceberg_version)
        if iceberg_version in seen_iceberg_versions:
            raise MatrixError(f"duplicate Iceberg version: {iceberg_version}")
        seen_iceberg_versions.add(iceberg_version)

        minimums = entry["upstream_minimums"]
        spark_versions = entry["spark_versions"]
        if not isinstance(minimums, dict) or not minimums:
            raise MatrixError(f"upstream_minimums for Iceberg {iceberg_version} must be a mapping")
        if not isinstance(spark_versions, list) or not spark_versions:
            raise MatrixError(f"spark_versions for Iceberg {iceberg_version} must be a list")

        expected_versions = set()
        for family, minimum in minimums.items():
            minimum_tuple = _version_tuple(minimum)
            if not isinstance(family, str) or _spark_family(minimum) != family:
                raise MatrixError(
                    f"minimum {minimum!r} does not belong to Spark family {family!r}")
            expected_versions.update(
                version for version in spark_shims
                if _spark_family(version) == family and _version_tuple(version) >= minimum_tuple)

        actual_versions = set()
        for spark_entry in spark_versions:
            if not isinstance(spark_entry, dict):
                raise MatrixError(f"invalid Spark entry for Iceberg {iceberg_version}")
            required_keys = {"version", "supported"}
            if not required_keys.issubset(spark_entry) or not set(spark_entry).issubset(
                    required_keys | {"reason"}):
                raise MatrixError(
                    f"Spark entries for Iceberg {iceberg_version} require version and supported")
            spark_version = spark_entry["version"]
            _version_tuple(spark_version)
            if spark_version in actual_versions:
                raise MatrixError(
                    f"duplicate Spark version {spark_version} for Iceberg {iceberg_version}")
            actual_versions.add(spark_version)
            if type(spark_entry["supported"]) is not bool:
                raise MatrixError(
                    f"supported must be boolean for Iceberg {iceberg_version}, "
                    f"Spark {spark_version}")
            reason = spark_entry.get("reason")
            if not spark_entry["supported"] and (not isinstance(reason, str) or not reason.strip()):
                raise MatrixError(
                    f"unsupported Iceberg {iceberg_version}, Spark {spark_version} needs a reason")
            if spark_entry["supported"] and reason is not None:
                raise MatrixError(
                    f"supported Iceberg {iceberg_version}, Spark {spark_version} "
                    "cannot have a reason")

        missing = expected_versions - actual_versions
        extra = actual_versions - expected_versions
        if missing or extra:
            details = []
            if missing:
                details.append(f"missing {', '.join(sorted(missing, key=_version_tuple))}")
            if extra:
                details.append(f"unexpected {', '.join(sorted(extra, key=_version_tuple))}")
            raise MatrixError(f"Iceberg {iceberg_version} Spark entries: {'; '.join(details)}")
    return entries


def supported_iceberg_versions(entries, spark_version):
    _version_tuple(spark_version)
    return [
        entry["version"]
        for entry in entries
        for spark_entry in entry["spark_versions"]
        if spark_entry["version"] == spark_version and spark_entry["supported"]
    ]


def validate_requested_versions(entries, spark_version, requested_versions):
    _version_tuple(spark_version)
    by_iceberg = {entry["version"]: entry for entry in entries}
    for iceberg_version in requested_versions:
        if iceberg_version not in by_iceberg:
            raise MatrixError(
                f"Iceberg version {iceberg_version} is not present in the test matrix")
        spark_entry = next((item for item in by_iceberg[iceberg_version]["spark_versions"]
                            if item["version"] == spark_version), None)
        if spark_entry is None:
            raise MatrixError(
                f"Iceberg {iceberg_version} is not upstream-compatible with Spark {spark_version}")
        if not spark_entry["supported"]:
            raise MatrixError(
                f"Iceberg {iceberg_version} is not supported with Spark {spark_version}: "
                f"{spark_entry['reason']}")
    return requested_versions


def parse_args(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spark-version", help="Spark version whose Iceberg versions should be listed")
    parser.add_argument(
        "--requested-versions",
        help="comma- or whitespace-separated Iceberg versions to validate and echo")
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--pom", type=Path, default=DEFAULT_POM)
    parser.add_argument(
        "--validate", action="store_true", help="validate the matrix without querying it")
    args = parser.parse_args(arguments)
    if not args.validate and not args.spark_version:
        parser.error("--spark-version is required unless --validate is used")
    if args.requested_versions and not args.spark_version:
        parser.error("--requested-versions requires --spark-version")
    return args


def main(arguments=None):
    args = parse_args(arguments)
    try:
        entries = load_matrix(args.matrix, args.pom)
        if args.validate:
            return 0
        if args.requested_versions:
            requested = [
                version for version in re.split(r"[\s,]+", args.requested_versions) if version]
            versions = validate_requested_versions(entries, args.spark_version, requested)
        else:
            versions = supported_iceberg_versions(entries, args.spark_version)
        print(" ".join(versions))
        return 0
    except (MatrixError, ET.ParseError, OSError, yaml.YAMLError) as error:
        print(f"Iceberg test matrix error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
