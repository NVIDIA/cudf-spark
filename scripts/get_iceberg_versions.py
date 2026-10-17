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

"""Read and validate the Iceberg integration-test matrix defined by issue #15875.

Iceberg's Spark dependency pins are minimum test baselines by cudf-spark policy,
not upstream compatibility guarantees. Packaged combinations below those baselines
are intentionally excluded: https://github.com/NVIDIA/cudf-spark/issues/15875.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, TypeVar, Union

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = REPO_ROOT / "scripts" / "iceberg-versions.json"
DEFAULT_POM = REPO_ROOT / "scala2.13" / "pom.xml"
VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SPARK_PROPERTY_PATTERN = re.compile(r"^spark[0-9]+\.version$")
POM_NAMESPACE = {"pom": "http://maven.apache.org/POM/4.0.0"}


class MatrixError(ValueError):
    """Invalid matrix metadata or a query that violates the test-selection policy.

    The message identifies the offending version, field, or missing mapping.
    File access, JSON decoding, and XML parsing errors retain their native types;
    the CLI reports all of these failures on stderr and exits with status 1.
    """


def _version_parts(version: str) -> Tuple[int, int, int]:
    if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
        raise MatrixError(f"invalid version: {version!r}")
    major, minor, patch = version.split(".")
    return int(major), int(minor), int(patch)


@dataclass(frozen=True, order=True)
class SparkVersion:
    """A release Spark version, ordered numerically by major, minor, and patch.

    Attributes:
        major: Nonnegative Spark major release number.
        minor: Nonnegative minor release number within the major release.
        patch: Nonnegative patch release number within the minor release.
    """

    major: int
    minor: int
    patch: int

    def __post_init__(self) -> None:
        """Reject noninteger or negative release components."""
        _validate_version_parts(self.major, self.minor, self.patch)

    @classmethod
    def parse(cls, version: str) -> SparkVersion:
        """Parse an exact major.minor.patch release; raise MatrixError otherwise."""
        return cls(*_version_parts(version))

    @property
    def family(self) -> str:
        """Return the major.minor family used to look up minimum test baselines."""
        return f"{self.major}.{self.minor}"

    def __str__(self) -> str:
        """Return the release string used by JSON and the command line."""
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True, order=True)
class IcebergVersion:
    """An Iceberg runtime release, distinct from a Spark version.

    Attributes:
        major: Nonnegative Iceberg major release number.
        minor: Nonnegative minor release number within the major release.
        patch: Nonnegative patch release number within the minor release.
    """

    major: int
    minor: int
    patch: int

    def __post_init__(self) -> None:
        """Reject noninteger or negative release components."""
        _validate_version_parts(self.major, self.minor, self.patch)

    @classmethod
    def parse(cls, version: str) -> IcebergVersion:
        """Parse an exact major.minor.patch runtime; raise MatrixError otherwise."""
        return cls(*_version_parts(version))

    def __str__(self) -> str:
        """Return the runtime string used by JSON and the command line."""
        return f"{self.major}.{self.minor}.{self.patch}"


def _validate_version_parts(major: int, minor: int, patch: int) -> None:
    if any(type(part) is not int or part < 0 for part in (major, minor, patch)):
        raise MatrixError(f"invalid release components: {(major, minor, patch)!r}")


def _pom_properties(element: ET.Element) -> Dict[str, str]:
    properties = element.find("pom:properties", POM_NAMESPACE)
    if properties is None:
        return {}
    return {child.tag.rsplit("}", 1)[-1]: (child.text or "").strip()
            for child in properties}


def _spark_shims(properties: Dict[str, str]) -> Set[SparkVersion]:
    return {SparkVersion.parse(value) for name, value in properties.items()
            if SPARK_PROPERTY_PATTERN.fullmatch(name) and VERSION_PATTERN.fullmatch(value)}


def _effective_pom(pom_path: Path, profile: Optional[str] = None,
                   modules: Sequence[str] = ()) -> List[ET.Element]:
    """Ask Maven to resolve a parent or a release profile's Iceberg projects.

    Args:
        pom_path: Parent POM passed to Maven as an absolute path.
        profile: Profile to activate; None evaluates only the parent project.
        modules: Iceberg reactor paths selected alongside the parent for a profile.

    Returns:
        Effective project elements, with the parent first. Maven handles property
        interpolation, inheritance, profile activation, and dependency management.
        Only Maven's generated XML is parsed; source POMs are not interpreted here.

    Raises:
        MatrixError: Maven fails or returns no effective projects.
        OSError, ET.ParseError: Maven cannot run, or its output cannot be read.

    The existing MVN environment variable supplies the Maven command and settings.
    Captured Maven logs cannot contaminate the shell-readable CLI output.
    """
    command = shlex.split(os.environ.get("MVN", "mvn"))
    command.extend(["-B", "-q", "-f", str(pom_path.resolve())])
    if profile is None:
        command.append("-N")
    else:
        command.extend(["-P", profile, "-pl", ",".join([".", *modules])])
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory) / "effective-pom.xml"
        command.extend(["help:effective-pom", f"-Doutput={output}"])
        try:
            subprocess.run(command, check=True, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, text=True)
        except subprocess.CalledProcessError as error:
            raise MatrixError(
                f"Maven effective-POM failed for {profile or 'parent'}: {error.stdout.strip()}") \
                from error
        root = ET.parse(output).getroot()
    projects = [root] if root.tag.rsplit("}", 1)[-1] == "project" else \
        root.findall("pom:project", POM_NAMESPACE)
    if not projects:
        raise MatrixError(f"Maven returned no effective projects for {profile or 'parent'}")
    return projects


@dataclass(frozen=True)
class MavenPackaging:
    """Spark × Iceberg runtime combinations inferred from Maven release profiles.

    Attributes:
        spark_versions: Release Spark shims declared by the POM's properties,
            including versions that have no packaged Iceberg integration.
        iceberg_versions_by_spark: Exact Iceberg runtime versions packaged for
            each release Spark profile, read from its included module POMs.
            Stub modules contribute no runtime version.
    """

    spark_versions: Set[SparkVersion]
    iceberg_versions_by_spark: Dict[SparkVersion, Set[IcebergVersion]]

    @classmethod
    def load(cls, pom_path: Path) -> MavenPackaging:
        """Evaluate release profiles and their Iceberg runtime dependencies with Maven.

        Args:
            pom_path: Parent POM, normally ``scala2.13/pom.xml``. Included
                ``iceberg/`` module POMs are resolved relative to this file.

        Returns:
            Packaging records read from Maven's effective POMs. Non-release Spark
            profiles, including snapshots and Databricks versions, are ignored.

        Raises:
            MatrixError: Maven fails, parent properties are absent, a release
                profile has no declared Spark shim, or a runtime is malformed.
            OSError, ET.ParseError: Maven cannot run or its output is invalid.
        """
        pom_path = Path(pom_path)
        root = _effective_pom(pom_path)[0]
        if root.find("pom:properties", POM_NAMESPACE) is None:
            raise MatrixError(f"properties not found in {pom_path}")
        properties = _pom_properties(root)
        spark_versions = _spark_shims(properties)
        packaged: Dict[SparkVersion, Set[IcebergVersion]] = {}
        for profile in root.findall("pom:profiles/pom:profile", POM_NAMESPACE):
            profile_values = _pom_properties(profile)
            version = profile_values.get("spark.version")
            if version is None or not VERSION_PATTERN.fullmatch(version):
                continue
            profile_id = profile.findtext("pom:id", "", POM_NAMESPACE)
            modules = [(module.text or "").strip() for module in
                       profile.findall("pom:modules/pom:module", POM_NAMESPACE)
                       if (module.text or "").strip().startswith("iceberg/")]
            projects = _effective_pom(pom_path, profile_id, modules)
            spark_version = SparkVersion.parse(_pom_properties(projects[0]).get("spark.version", ""))
            if spark_version not in spark_versions:
                raise MatrixError(f"profile Spark version {spark_version} has no declared shim property")
            versions = packaged.setdefault(spark_version, set())
            for project in projects[1:]:
                for dependency in project.findall(
                        "pom:dependencies/pom:dependency", POM_NAMESPACE):
                    group = dependency.findtext("pom:groupId", "", POM_NAMESPACE).strip()
                    artifact = dependency.findtext("pom:artifactId", "", POM_NAMESPACE).strip()
                    if group == "org.apache.iceberg" and artifact.startswith("iceberg-spark-runtime-"):
                        runtime = dependency.findtext("pom:version", "", POM_NAMESPACE).strip()
                        versions.add(IcebergVersion.parse(runtime))
        return cls(spark_versions, packaged)

    def supports(self, spark_version: SparkVersion, iceberg_version: IcebergVersion) -> bool:
        """Return whether a release profile packages the exact runtime combination.

        Args:
            spark_version: SparkVersion for an exact release Spark version.
            iceberg_version: IcebergVersion for an exact runtime version.

        Returns:
            A boolean; missing profiles and stub-only profiles return false.
            Minimum test baselines are applied separately by the matrix validator.
        """
        return iceberg_version in self.iceberg_versions_by_spark.get(spark_version, set())


Record = TypeVar("Record", "SparkSupport", "IcebergSupport")
Version = Union[SparkVersion, IcebergVersion]


def _index_records(records: List[Record]) -> Dict[Version, Record]:
    """Index decoded JSON records, rejecting duplicate or malformed versions."""
    if not isinstance(records, list):
        raise MatrixError("version records must be a list")
    indexed: Dict[Version, Record] = {}
    for record in records:
        if not isinstance(record, (SparkSupport, IcebergSupport)):
            raise MatrixError("each version entry requires its declared record fields")
        if record.version in indexed:
            kind = "Spark" if isinstance(record, SparkSupport) else "Iceberg"
            raise MatrixError(f"duplicate {kind} version: {record.version}")
        indexed[record.version] = record
    return indexed


def _decode_minimums(minimums: Dict[str, str]) -> Dict[str, SparkVersion]:
    """Decode JSON minimum test baselines as Spark versions."""
    if not isinstance(minimums, dict):
        raise MatrixError("upstream_minimums must be a mapping")
    return {family: SparkVersion.parse(minimum) for family, minimum in minimums.items()}


@dataclass(frozen=True)
class SparkSupport:
    """One Spark release's test eligibility for a particular Iceberg release.

    Construction validates the record, including through the JSON decoder hook.
    This records cudf-spark test policy, not an upstream compatibility guarantee.

    Attributes:
        version: SparkVersion describing an exact Spark release.
        supported: Whether the matrix enables this combination. For entries at
            or above the test baseline, this reflects the packaged integration.
        reason: Non-blank explanation required when ``supported`` is false;
            otherwise None. It is included in rejected-request diagnostics.
    """

    version: SparkVersion = field(metadata={"decoder": SparkVersion.parse})
    supported: bool
    reason: Optional[str] = None

    def __post_init__(self) -> None:
        """Reject invalid versions, support flags, and exclusion reasons."""
        if not isinstance(self.version, SparkVersion):
            raise MatrixError("Spark support requires a SparkVersion")
        if type(self.supported) is not bool:
            raise MatrixError(f"supported must be boolean for Spark {self.version}")
        if not self.supported and (not isinstance(self.reason, str) or not self.reason.strip()):
            raise MatrixError(f"unsupported Spark {self.version} needs a reason")
        if self.supported and self.reason is not None:
            raise MatrixError(f"supported Spark {self.version} cannot have a reason")


@dataclass(frozen=True)
class IcebergSupport:
    """Test baselines and Spark eligibility records for one Iceberg runtime.

    Attributes:
        version: IcebergVersion describing an exact Iceberg runtime release.
        spark_minor_to_patch: Mapping from a Spark family to its full minimum
            test SparkVersion, serialized for example as ``{"3.5": "3.5.6"}``
            under ``upstream_minimums``. These are Iceberg dependency pins interpreted
            as minimum test baselines by cudf-spark policy.
        spark_supports: Records keyed by SparkVersion, in JSON order, including unsupported
            combinations and their reasons. ``validate`` checks that this mapping
            contains every POM shim at or above a declared family's baseline
            exactly once, and that each support flag agrees with Maven packaging.
    """

    version: IcebergVersion = field(metadata={"decoder": IcebergVersion.parse})
    spark_minor_to_patch: Dict[str, SparkVersion] = field(
        metadata={"json_name": "upstream_minimums", "decoder": _decode_minimums})
    spark_supports: Dict[SparkVersion, SparkSupport] = field(
        metadata={"json_name": "spark_versions", "decoder": _index_records})

    def __post_init__(self) -> None:
        """Validate the version, baselines, and decoded Spark records at construction."""
        if not isinstance(self.version, IcebergVersion):
            raise MatrixError("Iceberg support requires an IcebergVersion")
        if not isinstance(self.spark_minor_to_patch, dict) or not self.spark_minor_to_patch:
            raise MatrixError(f"upstream_minimums for Iceberg {self.version} must be a mapping")
        if not isinstance(self.spark_supports, dict) or not self.spark_supports:
            raise MatrixError(f"spark_versions for Iceberg {self.version} must decode to a mapping")
        for family, minimum in self.spark_minor_to_patch.items():
            if not isinstance(minimum, SparkVersion) or minimum.family != family:
                raise MatrixError(
                    f"minimum {minimum!r} does not belong to Spark family {family!r}")
        for version, support in self.spark_supports.items():
            if not isinstance(version, SparkVersion) or not isinstance(support, SparkSupport):
                raise MatrixError(
                    f"Spark entries for Iceberg {self.version} require version and supported")
            if support.version != version:
                raise MatrixError(f"Spark key {version} disagrees with record {support.version}")

    def validate(self, packaging: MavenPackaging) -> None:
        """Compare eligible Spark records and support flags with inferred Maven combinations.

        Args:
            packaging: ``MavenPackaging`` loaded from the parent and module POMs.

        Returns:
            None after the expected Spark entries and every support flag agree
            with the baseline policy and Maven packaging.

        Raises:
            MatrixError: The Spark entry list differs from the eligible POM shims,
                or a support flag disagrees with the included modules' runtime pins.

        Packaged combinations below the minimum test baselines remain excluded
        under issue #15875; all combinations at or above a baseline are checked.
        """
        # Issue #15875 uses upstream build/test pins as minimum test baselines.
        # Maven packaging alone does not qualify a below-baseline Spark patch.
        expected_versions: Set[SparkVersion] = set()
        for family, minimum in self.spark_minor_to_patch.items():
            expected_versions.update(
                spark_version for spark_version in packaging.spark_versions
                if spark_version.family == family and spark_version >= minimum)

        actual_versions = set(self.spark_supports)

        missing = expected_versions - actual_versions
        extra = actual_versions - expected_versions
        if missing or extra:
            details = []
            if missing:
                details.append(f"missing {', '.join(map(str, sorted(missing)))}")
            if extra:
                details.append(f"unexpected {', '.join(map(str, sorted(extra)))}")
            raise MatrixError(f"Iceberg {self.version} Spark entries: {'; '.join(details)}")
        for support in self.spark_supports.values():
            inferred = packaging.supports(support.version, self.version)
            if support.supported != inferred:
                raise MatrixError(
                    f"Iceberg {self.version}, Spark {support.version}: supported={support.supported} "
                    f"disagrees with Maven packaging (expected {inferred})")

    def support_for(self, spark_version: SparkVersion) -> Optional[SparkSupport]:
        """Return the record for a SparkVersion, or None if absent.

        Unsupported records are returned too, so callers can inspect their reason.
        The input is a parsed SparkVersion; no additional validation is performed.
        """
        return self.spark_supports.get(spark_version)

    def supports(self, spark_version: SparkVersion) -> bool:
        """Return whether an exact Spark version has an enabled matrix entry.

        Both a missing record and a record marked unsupported return false.
        As with ``support_for``, this lookup accepts a parsed SparkVersion.
        """
        spark_support = self.support_for(spark_version)
        return spark_support is not None and spark_support.supported


@dataclass(frozen=True)
class IcebergVersionMatrix:
    """Collection of Iceberg releases used to select CI test combinations.

    Use ``load`` to decode the JSON records and validate their Maven combinations.

    Attributes:
        iceberg_supports: Records keyed by IcebergVersion in JSON order. ``load`` requires a
            nonempty mapping of unique Iceberg versions; automatic selection
            preserves this order.
    """

    iceberg_supports: Dict[IcebergVersion, IcebergSupport] = field(
        metadata={"json_name": "iceberg_versions", "decoder": _index_records})

    def __post_init__(self) -> None:
        """Require decoded Iceberg records with unique runtime versions."""
        if not isinstance(self.iceberg_supports, dict) or not self.iceberg_supports:
            raise MatrixError("iceberg_versions must decode to a non-empty mapping")
        if any(not isinstance(version, IcebergVersion) or not isinstance(support, IcebergSupport)
               for version, support in self.iceberg_supports.items()):
            raise MatrixError(
                "each Iceberg entry requires version, upstream_minimums, and spark_versions")
        if any(version != support.version for version, support in self.iceberg_supports.items()):
            raise MatrixError("Iceberg version keys disagree with their records")

    @classmethod
    def load(cls, matrix_path: Path = DEFAULT_MATRIX,
             pom_path: Path = DEFAULT_POM) -> IcebergVersionMatrix:
        """Decode JSON dataclasses, validate the inferred Maven combinations, and return them.

        Args:
            matrix_path: JSON file with a single ``iceberg_versions`` list.
                Defaults to the repository's ``scripts/iceberg-versions.json``.
            pom_path: Maven POM supplying the Spark shim properties and profiles.
                Defaults to the repository's ``scala2.13/pom.xml``. Its included Iceberg
                module POMs supply the packaged runtime versions.

        Raises:
            MatrixError: The schema, an entry, or its inferred Maven support is
                invalid, or a packaged Iceberg version is missing from the matrix.
            OSError: The JSON cannot be read, Maven cannot run, or its output is unreadable.
            json.JSONDecodeError: The matrix is not valid JSON.
            ET.ParseError: Maven's effective POM output is not well-formed XML.

        The standard-library JSON ``object_hook`` constructs the dataclasses using
        their field metadata and constructor validation. Maven comparison covers
        the entire matrix before any individual version query.
        """
        with open(matrix_path, encoding="utf-8") as stream:
            matrix = json.load(stream, object_hook=_decode_matrix_record)
        if not isinstance(matrix, cls):
            raise MatrixError("matrix must contain only an iceberg_versions list")
        packaging = MavenPackaging.load(pom_path)
        packaged_versions: Set[IcebergVersion] = set().union(
            *packaging.iceberg_versions_by_spark.values())
        missing = packaged_versions - matrix.iceberg_supports.keys()
        if missing:
            raise MatrixError(
                "packaged Iceberg versions missing from the matrix: "
                f"{', '.join(map(str, sorted(missing)))}")
        for support in matrix.iceberg_supports.values():
            support.validate(packaging)
        return matrix

    def supported_iceberg_versions(self, spark_version: str) -> List[str]:
        """Return enabled Iceberg versions for Spark, preserving matrix order.

        Args:
            spark_version: Exact ``major.minor.patch`` Spark release, or a
                well-formed ``major.minor.patch-SNAPSHOT`` in a family absent
                from every Iceberg baseline mapping.

        Returns:
            A list of Iceberg version strings. No enabled match yields an empty
            list, including the accepted snapshot case, so CI can skip the run.

        Raises:
            MatrixError: The Spark version is malformed or is a snapshot in a
                family covered by the matrix. Snapshots never select release entries.
        """
        # Spark master can use a snapshot in a family with no Iceberg integration.
        # Preserve the callers' skip for that family without treating snapshots
        # in covered families as their corresponding releases or silently skipping them.
        if isinstance(spark_version, str) and spark_version.endswith("-SNAPSHOT"):
            snapshot = SparkVersion.parse(spark_version[:-len("-SNAPSHOT")])
            if not any(snapshot.family in support.spark_minor_to_patch
                       for support in self.iceberg_supports.values()):
                return []
        release = SparkVersion.parse(spark_version)
        return [
            str(iceberg_support.version)
            for iceberg_support in self.iceberg_supports.values()
            if iceberg_support.supports(release)
        ]

    def validate_requested_versions(self, spark_version: str,
                                    requested_versions: List[str]) -> List[str]:
        """Validate caller-selected Iceberg versions and return the original list.

        Args:
            spark_version: Exact Spark release in ``major.minor.patch`` form.
                Explicit requests do not accept Spark snapshots.
            requested_versions: List of exact Iceberg version strings. Each must
                have an enabled entry for Spark; caller order and duplicates are
                preserved, and an empty list remains empty.

        Raises:
            MatrixError: Spark is malformed, an Iceberg version is not in the
                matrix, or a combination is absent or marked unsupported. The
                latter error includes the entry's exclusion reason.
        """
        release = SparkVersion.parse(spark_version)
        for iceberg_version in requested_versions:
            runtime = IcebergVersion.parse(iceberg_version)
            if runtime not in self.iceberg_supports:
                raise MatrixError(
                    f"Iceberg version {iceberg_version} is not present in the test matrix")
            spark_support = self.iceberg_supports[runtime].support_for(release)
            if spark_support is None:
                raise MatrixError(
                    f"Iceberg {iceberg_version} with Spark {spark_version} is excluded "
                    "by the minimum-baseline test policy (issue #15875)")
            if not spark_support.supported:
                raise MatrixError(
                    f"Iceberg {iceberg_version} is not supported with Spark {spark_version}: "
                    f"{spark_support.reason}")
        return requested_versions


def _decode_matrix_record(record: Dict[str, Any]) -> Union[
        Dict[str, Any], SparkSupport, IcebergSupport, IcebergVersionMatrix]:
    """JSON object hook constructing records from dataclass fields and JSON aliases.

    The decoder visits nested objects first. Required fields and optional defaults
    come from ``dataclasses.fields``; field metadata also specifies decoders for
    typed versions and version-indexed collections. Other dictionaries remain
    dictionaries until their enclosing record decodes them.
    """
    for record_type in (SparkSupport, IcebergSupport, IcebergVersionMatrix):
        record_fields = fields(record_type)
        aliases = {item.metadata.get("json_name", item.name): item.name for item in record_fields}
        required = {item.metadata.get("json_name", item.name) for item in record_fields
                    if item.default is MISSING and item.default_factory is MISSING}
        if required <= record.keys() <= aliases.keys():
            decoders: Dict[str, Callable[[Any], Any]] = {
                item.name: item.metadata["decoder"] for item in record_fields
                if "decoder" in item.metadata}
            arguments = {aliases[name]: decoders[aliases[name]](value)
                         if aliases[name] in decoders else value
                         for name, value in record.items()}
            return record_type(**arguments)
    return record


def _print_versions(spark_version: str, versions: List[str]) -> None:
    """Print shell-readable versions and explain an empty selection on stderr."""
    if not versions:
        # A valid matrix can intentionally exclude even a packaged combination.
        # Missing entries for shims at or above a baseline fail during load().
        print(
            f"No Iceberg versions selected for Spark {spark_version}: "
            "the minimum-baseline and packaging test policy excludes all entries "
            "(https://github.com/NVIDIA/cudf-spark/issues/15875).",
            file=sys.stderr)
    print(" ".join(versions))


def command_validate(args: argparse.Namespace) -> None:
    """Check the entire matrix against the POM without selecting test versions.

    Args:
        args: Parsed ``validate`` arguments. ``matrix`` is the JSON input path
            and ``pom`` is the Maven POM declaring Spark release profiles.
            Maven evaluates included Iceberg module POMs relative to the parent.

    Returns:
        None after both inputs have been read and all matrix entries validated.
        Success produces no output; ``main`` translates it to exit status 0.

    Raises:
        MatrixError: The schema, version entries, or inferred Maven combinations
            are invalid. Validation includes duplicates and unsupported-entry reasons.
        OSError: Either input cannot be read.
        json.JSONDecodeError: The matrix is not valid JSON.
        ET.ParseError: Maven's effective POM output is not well-formed XML.

    These failures reach ``main``, which reports them and returns exit status 1.
    """
    IcebergVersionMatrix.load(args.matrix, args.pom)


def command_list(args: argparse.Namespace) -> None:
    """Validate the inputs and print matrix-selected Iceberg versions for Spark.

    Args:
        args: Parsed ``list`` arguments containing the ``matrix`` JSON path,
            the ``pom`` path, and the exact ``spark_version`` to query.

    Returns:
        None after printing space-separated versions in matrix order and a
        newline. An empty selection prints a newline and a policy diagnostic
        on stderr; it is successful and lets CI skip that Spark combination.

    Raises:
        MatrixError: Matrix validation fails or the Spark query is malformed,
            including a snapshot in a family covered by the matrix.
        OSError, json.JSONDecodeError, ET.ParseError: An input cannot be read
            or parsed. ``main`` reports these failures and returns status 1.
    """
    matrix = IcebergVersionMatrix.load(args.matrix, args.pom)
    versions = matrix.supported_iceberg_versions(args.spark_version)
    _print_versions(args.spark_version, versions)


def command_validate_requested(args: argparse.Namespace) -> None:
    """Validate caller-selected versions for Spark and echo them in caller order.

    Args:
        args: Parsed ``validate-requested`` arguments containing ``matrix`` and
            ``pom`` paths, an exact Spark release in ``spark_version``, and a
            comma- or whitespace-separated ``requested_versions`` string.

    Returns:
        None after printing the requested versions separated by spaces and a
        newline. Input order and duplicates are retained. An empty version list
        prints a newline and the empty-selection diagnostic on stderr.

    Raises:
        MatrixError: The matrix or Spark release is invalid, or a requested
            Iceberg version is unknown, below the test baseline, or unsupported.
        OSError, json.JSONDecodeError, ET.ParseError: An input cannot be read
            or parsed. ``main`` reports these failures and returns status 1.

    The entire matrix is validated before the requested combinations are checked.
    """
    matrix = IcebergVersionMatrix.load(args.matrix, args.pom)
    requested = [
        version for version in re.split(r"[\s,]+", args.requested_versions) if version]
    versions = matrix.validate_requested_versions(args.spark_version, requested)
    _print_versions(args.spark_version, versions)


def parse_args(arguments: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-specific CLI options into an ``argparse.Namespace``.

    ``arguments`` is a sequence of argument strings; None reads ``sys.argv[1:]``.
    Each required subcommand owns its input-file options and accepts only the
    query arguments relevant to that operation. The returned namespace includes
    the command's ``handler`` function. Invalid usage raises SystemExit with
    status 2; requesting help raises SystemExit with status 0.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inputs = argparse.ArgumentParser(add_help=False)
    inputs.add_argument(
        "--matrix", type=Path, default=DEFAULT_MATRIX,
        help="matrix JSON file (default: scripts/iceberg-versions.json)")
    inputs.add_argument(
        "--pom", type=Path, default=DEFAULT_POM,
        help="Spark shim POM (default: scala2.13/pom.xml)")

    validate_parser = subparsers.add_parser(
        "validate", parents=[inputs], help="validate the entire matrix against the POM")
    validate_parser.set_defaults(handler=command_validate)

    list_parser = subparsers.add_parser(
        "list", parents=[inputs], help="list matrix-selected Iceberg versions for Spark")
    list_parser.add_argument(
        "--spark-version", required=True, help="exact Spark version to query")
    list_parser.set_defaults(handler=command_list)

    requested_parser = subparsers.add_parser(
        "validate-requested", parents=[inputs],
        help="validate and echo caller-selected Iceberg versions for Spark")
    requested_parser.add_argument(
        "--spark-version", required=True, help="exact Spark release to query")
    requested_parser.add_argument(
        "--requested-versions", required=True,
        help="comma- or whitespace-separated Iceberg versions to validate and echo")
    requested_parser.set_defaults(handler=command_validate_requested)
    return parser.parse_args(arguments)


def main(arguments: Optional[Sequence[str]] = None) -> int:
    r"""Validate or query the integration-test source of truth, scripts/iceberg-versions.json.

    Run from the repository root, with Maven available::

        python3 scripts/get_iceberg_versions.py validate
        python3 scripts/get_iceberg_versions.py list --spark-version 3.5.6
        python3 scripts/get_iceberg_versions.py validate-requested \
            --spark-version 3.5.6 --requested-versions '1.10.1, 1.9.2'

    Each command accepts --matrix and --pom to override the JSON and parent POM
    inputs; defaults are scripts/iceberg-versions.json and scala2.13/pom.xml.
    Use --help on a command to see its arguments. The MVN environment variable
    can supply the existing Maven command and settings options. Relative settings
    paths resolve against the current working directory. Maven's effective POMs
    validate the entire matrix before selection; Maven logs stay off stdout.

    validate produces no output on success. list prints enabled versions in JSON
    order. validate-requested accepts comma- or whitespace-separated versions and
    echoes valid requests in caller order, retaining duplicates. An empty selection
    prints a newline and a policy diagnostic on stderr, letting CI skip that run.

    Issue #15875 treats Iceberg's Spark dependency pins in upstream_minimums as
    minimum test baselines, not upstream compatibility guarantees. Combinations
    below a baseline are intentionally excluded even when Maven packages them:
    Spark 3.5.0 and 3.5.4 select no versions; Spark 3.5.5 selects 1.9.2 but excludes
    1.10.1. Spark 4.2.0 uses the stub and selects none. Well-formed snapshots in
    families absent from the matrix, including 5.0.0-SNAPSHOT, also select none;
    snapshots in covered families and malformed versions fail. Explicit requests
    must satisfy both the baseline and packaging rules.

    Args:
        arguments: Command-line strings; None reads sys.argv[1:].

    Returns:
        0 on success; 1 after reporting file, Maven, parse, or validation failures
        on stderr. Missing matrix entries and rejected requests are failures.

    Raises:
        SystemExit: argparse uses status 2 for invalid usage and 0 for --help.
    """
    args = parse_args(arguments)
    try:
        handler: Callable[[argparse.Namespace], None] = args.handler
        handler(args)
        return 0
    except (MatrixError, ET.ParseError, OSError, json.JSONDecodeError) as error:
        print(f"Iceberg test matrix error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
