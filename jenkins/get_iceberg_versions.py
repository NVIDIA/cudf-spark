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
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = REPO_ROOT / "iceberg" / "iceberg-versions.yml"
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


@dataclass(frozen=True)
class SparkSupport:
    version: str
    supported: bool
    reason: Optional[str] = None

    @classmethod
    def from_dict(cls, entry, iceberg_version):
        if not isinstance(entry, dict):
            raise MatrixError(f"invalid Spark entry for Iceberg {iceberg_version}")
        required_keys = {"version", "supported"}
        if not required_keys.issubset(entry) or not set(entry).issubset(
                required_keys | {"reason"}):
            raise MatrixError(
                f"Spark entries for Iceberg {iceberg_version} require version and supported")

        version = entry["version"]
        _version_tuple(version)
        supported = entry["supported"]
        if type(supported) is not bool:
            raise MatrixError(
                f"supported must be boolean for Iceberg {iceberg_version}, Spark {version}")
        reason = entry.get("reason")
        if not supported and (not isinstance(reason, str) or not reason.strip()):
            raise MatrixError(
                f"unsupported Iceberg {iceberg_version}, Spark {version} needs a reason")
        if supported and reason is not None:
            raise MatrixError(
                f"supported Iceberg {iceberg_version}, Spark {version} cannot have a reason")
        return cls(version, supported, reason)


@dataclass(frozen=True)
class IcebergSupport:
    version: str
    spark_minor_to_patch: Dict[str, str]
    spark_supports: List[SparkSupport]

    @classmethod
    def from_dict(cls, entry, spark_shims):
        if not isinstance(entry, dict) or set(entry) != {
                "version", "upstream_minimums", "spark_versions"}:
            raise MatrixError(
                "each Iceberg entry requires version, upstream_minimums, and spark_versions")

        version = entry["version"]
        _version_tuple(version)
        spark_minor_to_patch = entry["upstream_minimums"]
        spark_entries = entry["spark_versions"]
        if not isinstance(spark_minor_to_patch, dict) or not spark_minor_to_patch:
            raise MatrixError(f"upstream_minimums for Iceberg {version} must be a mapping")
        if not isinstance(spark_entries, list) or not spark_entries:
            raise MatrixError(f"spark_versions for Iceberg {version} must be a list")

        expected_versions = set()
        for family, minimum in spark_minor_to_patch.items():
            minimum_tuple = _version_tuple(minimum)
            if not isinstance(family, str) or _spark_family(minimum) != family:
                raise MatrixError(
                    f"minimum {minimum!r} does not belong to Spark family {family!r}")
            expected_versions.update(
                spark_version for spark_version in spark_shims
                if _spark_family(spark_version) == family and
                _version_tuple(spark_version) >= minimum_tuple)

        spark_supports = [SparkSupport.from_dict(item, version) for item in spark_entries]
        actual_versions = set()
        for spark_support in spark_supports:
            if spark_support.version in actual_versions:
                raise MatrixError(
                    f"duplicate Spark version {spark_support.version} for Iceberg {version}")
            actual_versions.add(spark_support.version)

        missing = expected_versions - actual_versions
        extra = actual_versions - expected_versions
        if missing or extra:
            details = []
            if missing:
                details.append(f"missing {', '.join(sorted(missing, key=_version_tuple))}")
            if extra:
                details.append(f"unexpected {', '.join(sorted(extra, key=_version_tuple))}")
            raise MatrixError(f"Iceberg {version} Spark entries: {'; '.join(details)}")
        return cls(version, spark_minor_to_patch, spark_supports)

    def support_for(self, spark_version):
        return next(
            (support for support in self.spark_supports if support.version == spark_version), None)

    def supports(self, spark_version):
        spark_support = self.support_for(spark_version)
        return spark_support is not None and spark_support.supported


@dataclass(frozen=True)
class IcebergVersionMatrix:
    iceberg_supports: List[IcebergSupport]

    @classmethod
    def load(cls, matrix_path=DEFAULT_MATRIX, pom_path=DEFAULT_POM):
        with open(matrix_path, encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
        if not isinstance(document, dict) or set(document) != {"iceberg_versions"}:
            raise MatrixError("matrix must contain only an iceberg_versions list")
        entries = document["iceberg_versions"]
        if not isinstance(entries, list) or not entries:
            raise MatrixError("iceberg_versions must be a non-empty list")

        spark_shims = read_spark_shims(pom_path)
        iceberg_supports = [IcebergSupport.from_dict(entry, spark_shims) for entry in entries]
        versions = [support.version for support in iceberg_supports]
        if len(set(versions)) != len(versions):
            duplicate = next(version for index, version in enumerate(versions)
                             if version in versions[:index])
            raise MatrixError(f"duplicate Iceberg version: {duplicate}")
        return cls(iceberg_supports)

    def supported_iceberg_versions(self, spark_version):
        _version_tuple(spark_version)
        return [
            iceberg_support.version
            for iceberg_support in self.iceberg_supports
            if iceberg_support.supports(spark_version)
        ]

    def validate_requested_versions(self, spark_version, requested_versions):
        _version_tuple(spark_version)
        by_version = {support.version: support for support in self.iceberg_supports}
        for iceberg_version in requested_versions:
            if iceberg_version not in by_version:
                raise MatrixError(
                    f"Iceberg version {iceberg_version} is not present in the test matrix")
            spark_support = by_version[iceberg_version].support_for(spark_version)
            if spark_support is None:
                raise MatrixError(
                    f"Iceberg {iceberg_version} is not upstream-compatible with "
                    f"Spark {spark_version}")
            if not spark_support.supported:
                raise MatrixError(
                    f"Iceberg {iceberg_version} is not supported with Spark {spark_version}: "
                    f"{spark_support.reason}")
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
        matrix = IcebergVersionMatrix.load(args.matrix, args.pom)
        if args.validate:
            return 0
        if args.requested_versions:
            requested = [
                version for version in re.split(r"[\s,]+", args.requested_versions) if version]
            versions = matrix.validate_requested_versions(args.spark_version, requested)
        else:
            versions = matrix.supported_iceberg_versions(args.spark_version)
        print(" ".join(versions))
        return 0
    except (MatrixError, ET.ParseError, OSError, yaml.YAMLError) as error:
        print(f"Iceberg test matrix error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
