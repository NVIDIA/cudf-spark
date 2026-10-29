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
not upstream compatibility guarantees. Enabled combinations must exactly match
Maven packaging. Four documented legacy combinations retain below-baseline support
for compatibility; new Iceberg versions must respect the minimum test baselines.
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
from typing import (Any, Callable, Dict, List, Optional, Sequence, Set, Type, TypeVar, Union,
                    get_args, get_origin, get_type_hints)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = REPO_ROOT / "scripts" / "iceberg-versions.json"
# Scala 2.13 is the canonical packaging source: it includes Spark 4 profiles
# absent from the Scala 2.12 reactor. Runtime queries only read DEFAULT_MATRIX.
DEFAULT_POM = REPO_ROOT / "scala2.13" / "pom.xml"
VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SPARK_PROPERTY_PATTERN = re.compile(r"^spark[0-9]+\.version$")
POM_NAMESPACE = {"pom": "http://maven.apache.org/POM/4.0.0"}
# Preserve only combinations already supported before the matrix was introduced.
# Do not extend this exception set for new Iceberg versions.
LEGACY_BASELINE_EXCEPTIONS = frozenset({
    ("1.6.1", "3.5.0"),
    ("1.9.2", "3.5.4"),
    ("1.10.1", "3.5.4"),
    ("1.10.1", "3.5.5"),
})


class MatrixError(ValueError):
    """Invalid matrix metadata or a query that violates the test-selection policy.

    The message identifies the offending version, field, or missing mapping.
    File access, JSON decoding, and XML parsing errors retain their native types;
    the CLI reports all of these failures on stderr and exits with status 1.
    """


Release = TypeVar("Release", bound="_ReleaseVersion")


@dataclass(frozen=True, order=True)
class _ReleaseVersion:
    """Shared numeric release implementation; subclasses retain distinct domains."""

    major: int
    minor: int
    patch: int

    def __post_init__(self) -> None:
        parts = (self.major, self.minor, self.patch)
        if any(type(part) is not int or part < 0 for part in parts):
            raise MatrixError(f"invalid release components: {parts!r}")

    @classmethod
    def parse(cls: Type[Release], version: str) -> Release:
        """Parse an exact major.minor.patch release; raise MatrixError otherwise."""
        if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
            raise MatrixError(f"invalid version: {version!r}")
        major, minor, patch = version.split(".")
        return cls(int(major), int(minor), int(patch))

    def __str__(self) -> str:
        """Return the release string used by JSON and the command line."""
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True, order=True)
class SparkVersion(_ReleaseVersion):
    """A Spark release with nonnegative integer major, minor, and patch fields.

    Releases are ordered numerically within the Spark domain. Parsing and string
    formatting use exact major.minor.patch strings, without snapshot suffixes.
    """

    @property
    def family(self) -> str:
        """Return the major.minor family used to look up minimum test baselines."""
        return f"{self.major}.{self.minor}"


@dataclass(frozen=True, order=True)
class IcebergVersion(_ReleaseVersion):
    """An Iceberg release with nonnegative integer major, minor, and patch fields.

    Numeric ordering, parsing, and formatting share the release implementation,
    while equality and ordering remain distinct from the Spark version domain.
    """


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
    for index, argument in enumerate(command[1:], start=1):
        defines_buildver = argument in ("-Dbuildver", "--define=buildver") or \
            argument.startswith(("-Dbuildver=", "--define=buildver="))
        if argument in ("-D", "--define") and index + 1 < len(command):
            defines_buildver = command[index + 1].split("=", 1)[0] == "buildver"
        if argument.startswith(("-P", "--activate-profiles")) or defines_buildver:
            raise MatrixError(
                "MVN must not select profiles (-P or -Dbuildver); "
                "validate evaluates each release profile itself")
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
    """Index typed JSON records without losing duplicate-version diagnostics."""
    indexed: Dict[Version, Record] = {}
    for record in records:
        if record.version in indexed:
            kind = "Spark" if isinstance(record, SparkSupport) else "Iceberg"
            raise MatrixError(f"duplicate {kind} version: {record.version}")
        indexed[record.version] = record
    return indexed


@dataclass(frozen=True)
class SparkSupport:
    """One Spark release's test eligibility for a particular Iceberg release.

    Field annotations drive JSON decoding and type checks; construction checks
    the relationship between the support flag and exclusion reason.
    This records cudf-spark test policy, not an upstream compatibility guarantee.

    Attributes:
        version: SparkVersion describing an exact Spark release.
        supported: Whether the matrix enables this combination. Must agree with
            Maven packaging, including existing below-baseline combinations.
        reason: Non-blank explanation required when ``supported`` is false;
            otherwise None. It is included in rejected-request diagnostics.
        comment: Optional documentation; required for a legacy baseline exception.
    """

    version: SparkVersion
    supported: bool
    reason: Optional[str] = None
    comment: Optional[str] = None

    def __post_init__(self) -> None:
        """Require a non-blank reason exactly when a combination is unsupported."""
        if not self.supported and (self.reason is None or not self.reason.strip()):
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
            contains every packaged combination, references only declared Spark shims,
            and has support flags matching Maven packaging. Unsupported records are optional.
    """

    version: IcebergVersion
    spark_minor_to_patch: Dict[str, SparkVersion] = field(
        metadata={"json_name": "upstream_minimums"})
    spark_supports: Dict[SparkVersion, SparkSupport] = field(
        metadata={"json_name": "spark_versions"})

    def __post_init__(self) -> None:
        """Check baseline families and permit only documented legacy exceptions."""
        for family, minimum in self.spark_minor_to_patch.items():
            if minimum.family != family:
                raise MatrixError(
                    f"minimum {minimum!r} does not belong to Spark family {family!r}")
        for support in self.spark_supports.values():
            if not support.supported:
                continue
            baseline = self.spark_minor_to_patch.get(support.version.family)
            if baseline is None:
                raise MatrixError(
                    f"Iceberg {self.version}: missing minimum for Spark family "
                    f"{support.version.family}")
            if support.version < baseline:
                if (str(self.version), str(support.version)) not in LEGACY_BASELINE_EXCEPTIONS:
                    raise MatrixError(
                        f"Iceberg {self.version}, Spark {support.version}: below minimum test "
                        f"baseline {baseline}; only existing compatibility exceptions are allowed")
                if support.comment is None or not support.comment.strip():
                    raise MatrixError(
                        f"Iceberg {self.version}, Spark {support.version}: "
                        "legacy baseline exception needs a compatibility comment")

    def validate(self, packaging: MavenPackaging) -> None:
        """Require declared Spark shims and an exact match of enabled combinations.

        Args:
            packaging: ``MavenPackaging`` loaded from the parent and module POMs.

        Returns:
            None when every packaged combination is represented and each listed
            Spark shim and support flag agrees with Maven packaging.

        Raises:
            MatrixError: A packaged combination is missing, a Spark shim is not
                declared in the POM, or a support flag disagrees with packaging.

        Unsupported records are optional. Baseline and legacy-exception policy
        is checked independently during construction.
        """
        unknown_versions = set(self.spark_supports) - packaging.spark_versions
        if unknown_versions:
            raise MatrixError(
                f"Iceberg {self.version} has unknown Spark shims: "
                f"{', '.join(map(str, sorted(unknown_versions)))}")
        expected_versions = {
            spark_version for spark_version, runtimes in packaging.iceberg_versions_by_spark.items()
            if self.version in runtimes
        }
        supported_versions = {
            spark_version for spark_version, support in self.spark_supports.items()
            if support.supported
        }

        missing = expected_versions - supported_versions
        extra = supported_versions - expected_versions
        if missing or extra:
            details = []
            if missing:
                details.append(f"missing {', '.join(map(str, sorted(missing)))}")
            if extra:
                details.append(f"unexpected {', '.join(map(str, sorted(extra)))}")
            raise MatrixError(
                f"Iceberg {self.version} Spark support disagrees with Maven packaging: "
                f"{'; '.join(details)}")

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

    Use ``load`` for JSON-only queries and ``validate`` for the premerge Maven check.

    Attributes:
        iceberg_supports: Records keyed by IcebergVersion in JSON order. ``load`` requires a
            nonempty mapping of unique Iceberg versions; automatic selection
            preserves this order.
    """

    iceberg_supports: Dict[IcebergVersion, IcebergSupport] = field(
        metadata={"json_name": "iceberg_versions"})

    @classmethod
    def load(cls, matrix_path: Path = DEFAULT_MATRIX) -> IcebergVersionMatrix:
        """Decode and check the JSON schema without invoking Maven.

        Args:
            matrix_path: Reviewable integration-test JSON source of truth.

        Returns:
            Typed, version-indexed records in JSON order. The standard-library
            object hook constructs the dataclasses and checks their fields.

        Raises:
            MatrixError: The schema, versions, duplicates, or reasons are invalid.
            OSError, json.JSONDecodeError: The JSON cannot be read or parsed.

        Packaging drift is checked separately by ``validate`` in premerge.
        """
        with open(matrix_path, encoding="utf-8") as stream:
            matrix = json.load(stream, object_hook=_decode_matrix_record)
        if not isinstance(matrix, cls):
            raise MatrixError("matrix must contain only an iceberg_versions list")
        return matrix

    def validate(self, pom_path: Path = DEFAULT_POM) -> None:
        """Check the complete matrix against Maven packaging and test baselines.

        Args:
            pom_path: Canonical Scala 2.13 parent POM, which includes Spark 4
                profiles absent from the Scala 2.12 reactor. Overrides support
                fixture or alternate reactors; a Scala 2.12-only POM cannot
                validate the complete cross-Spark matrix.

        Raises:
            MatrixError: Maven fails, a packaged runtime is missing, or a mapping
                disagrees with the minimum-baseline and packaging policy.
            OSError, ET.ParseError: Maven or its generated XML is unavailable.

        Only the explicit premerge validation command calls Maven. Runtime
        selection through ``load`` and the query methods needs only JSON.
        """
        packaging = MavenPackaging.load(pom_path)
        packaged_versions: Set[IcebergVersion] = set().union(
            *packaging.iceberg_versions_by_spark.values())
        actual_versions = set(self.iceberg_supports)
        missing = packaged_versions - actual_versions
        if missing:
            raise MatrixError(
                "packaged Iceberg versions missing from the matrix: "
                f"{', '.join(map(str, sorted(missing)))}")
        extra = actual_versions - packaged_versions
        if extra:
            raise MatrixError(
                "matrix Iceberg versions not packaged by Maven: "
                f"{', '.join(map(str, sorted(extra)))}")
        for support in self.iceberg_supports.values():
            support.validate(packaging)

    def supported_iceberg_versions(self, spark_version: str) -> List[str]:
        """Return enabled Iceberg versions for Spark, preserving matrix order.

        Args:
            spark_version: Exact ``major.minor.patch`` Spark release.

        Returns:
            A list of Iceberg version strings. No enabled match yields an empty
            list, so CI can skip the run.

        Raises:
            MatrixError: The Spark version is not an exact release.
        """
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
                    f"Iceberg {iceberg_version} has no matrix entry for Spark {spark_version}")
            if not spark_support.supported:
                raise MatrixError(
                    f"Iceberg {iceberg_version} is not supported with Spark {spark_version}: "
                    f"{spark_support.reason}")
        return requested_versions


def _decode_json_value(value: Any, value_type: Any) -> Any:
    """Decode the matrix's annotated field types, without coercing scalar values.

    All matrix mappings are nonempty. Version-keyed mappings are serialized as
    lists of records; the object hook has already decoded their nested records.
    """
    if get_origin(value_type) is Union:
        # The only union field in the schema is Optional[str].
        if value is None:
            return None
        value_type = next(kind for kind in get_args(value_type) if kind is not type(None))
    if get_origin(value_type) is dict:
        key_type, item_type = get_args(value_type)
        if key_type in (SparkVersion, IcebergVersion):
            if not isinstance(value, list) or not value:
                raise MatrixError("version records must be a non-empty list")
            return _index_records([_decode_json_value(item, item_type) for item in value])
        if not isinstance(value, dict) or not value:
            raise MatrixError("minimum test baselines must be a non-empty mapping")
        return {_decode_json_value(key, key_type): _decode_json_value(item, item_type)
                for key, item in value.items()}
    if issubclass(value_type, _ReleaseVersion):
        return value_type.parse(value)
    if type(value) is not value_type:
        raise MatrixError(f"expected {value_type.__name__}, got {value!r}")
    return value


def _decode_matrix_record(record: Dict[str, Any]) -> Union[
        Dict[str, Any], SparkSupport, IcebergSupport, IcebergVersionMatrix]:
    """JSON object hook constructing records from dataclass fields and JSON aliases.

    Field annotations specify types, dataclass defaults identify optional fields,
    and metadata only supplies JSON aliases. Post-init hooks check cross-field
    rules; scalar and collection type checks happen here once during decoding.
    Unrecognized dictionaries are checked by their enclosing record or ``load``.
    """
    for record_type in (SparkSupport, IcebergSupport, IcebergVersionMatrix):
        record_fields = fields(record_type)
        aliases = {item.metadata.get("json_name", item.name): item.name for item in record_fields}
        required = {item.metadata.get("json_name", item.name) for item in record_fields
                    if item.default is MISSING and item.default_factory is MISSING}
        if required <= record.keys() <= aliases.keys():
            annotations = get_type_hints(record_type)
            arguments = {aliases[name]: _decode_json_value(value, annotations[aliases[name]])
                         for name, value in record.items()}
            return record_type(**arguments)
    return record


def _print_versions(spark_version: str, versions: List[str]) -> None:
    """Print shell-readable versions and explain an empty selection on stderr."""
    if not versions:
        print(
            f"No Iceberg versions selected for Spark {spark_version}: "
            "the matrix contains no enabled combination; "
            "validate checks all packaged combinations against Maven.",
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
    IcebergVersionMatrix.load(args.matrix).validate(args.pom)


def command_list(args: argparse.Namespace) -> None:
    """Read JSON and print matrix-selected Iceberg versions without Maven.

    Args:
        args: Parsed ``list`` arguments containing the ``matrix`` JSON path,
            and the exact ``spark_version`` to query.

    Returns:
        None after printing space-separated versions in matrix order and a
        newline. An empty selection prints a newline and a policy diagnostic
        on stderr; it is successful and lets CI skip that Spark combination.

    Raises:
        MatrixError: JSON schema validation fails or the Spark query is not an
            exact release.
        OSError, json.JSONDecodeError: The JSON cannot be read
            or parsed. ``main`` reports these failures and returns status 1.
    """
    matrix = IcebergVersionMatrix.load(args.matrix)
    versions = matrix.supported_iceberg_versions(args.spark_version)
    _print_versions(args.spark_version, versions)


def command_validate_requested(args: argparse.Namespace) -> None:
    """Validate caller-selected versions for Spark and echo them in caller order.

    Args:
        args: Parsed ``validate-requested`` arguments containing the ``matrix``
            JSON path, an exact Spark release in ``spark_version``, and a
            comma- or whitespace-separated ``requested_versions`` string.

    Returns:
        None after printing the requested versions separated by spaces and a
        newline. Input order and duplicates are retained. An empty version list
        prints a newline and the empty-selection diagnostic on stderr.

    Raises:
        MatrixError: The matrix or Spark release is invalid, or a requested
            Iceberg version is unknown, absent for this Spark version, or unsupported.
        OSError, json.JSONDecodeError: The JSON cannot be read or parsed.
            ``main`` reports these failures and returns status 1.

    Only JSON schema and requested-combination checks run here; Maven packaging
    validation belongs to the explicit premerge ``validate`` command.
    """
    matrix = IcebergVersionMatrix.load(args.matrix)
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
    validate_parser = subparsers.add_parser(
        "validate", parents=[inputs], help="validate the entire matrix against the POM")
    validate_parser.add_argument(
        "--pom", type=Path, default=DEFAULT_POM,
        help="canonical packaging POM (default: scala2.13/pom.xml, including Spark 4)")
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

    Run from the repository root::

        python3 scripts/get_iceberg_versions.py validate
        python3 scripts/get_iceberg_versions.py list --spark-version 3.5.6
        python3 scripts/get_iceberg_versions.py validate-requested \
            --spark-version 3.5.6 --requested-versions '1.10.1, 1.9.2'

    Each command accepts --matrix (default: scripts/iceberg-versions.json).
    list and validate-requested read only JSON and never invoke Maven. Premerge
    runs validate explicitly to compare the complete matrix with Maven packaging.
    Only validate accepts --pom (default: scala2.13/pom.xml). Scala 2.13 is the
    intentional canonical source because it includes Spark 4 profiles missing
    from the Scala 2.12 POM; this is a cross-Spark test matrix, not a per-Scala
    packaging matrix.

    For validate, MVN can supply the Maven executable and settings; relative
    settings resolve against the current working directory. MVN must not include
    -P or -Dbuildver, since validation selects each release profile itself.
    Maven logs stay off stdout. Use --help on a command to see its arguments.

    validate produces no output on success. list prints enabled versions in JSON
    order. validate-requested accepts comma- or whitespace-separated versions and
    echoes valid requests in caller order, retaining duplicates. An empty selection
    prints a newline and a policy diagnostic on stderr, letting CI skip that run.

    Issue #15875 treats Iceberg's Spark dependency pins in upstream_minimums as
    minimum test baselines, not upstream compatibility guarantees. Enabled JSON
    combinations must exactly match Maven packaging. Existing combinations
    3.5.0/1.6.1, 3.5.4/1.9.2, and 3.5.4-3.5.5/1.10.1 retain below-baseline support
    for compatibility, with comments on their JSON entries. New Iceberg versions
    cannot use these exceptions. Spark 4.2.0 uses the stub and selects none.
    Queries accept exact major.minor.patch releases only.

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
