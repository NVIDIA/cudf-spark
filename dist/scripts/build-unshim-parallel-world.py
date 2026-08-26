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

"""Build dist/target/parallel-world directly for repeated unshim analysis.

This mirrors the analyzer-relevant part of dist/maven-antrun/build-parallel-worlds.xml
without starting a final Maven dist generate-resources invocation. It assumes buildall
has already built the per-shim sql-plugin-api, aggregator, and root-safe module jars under target/sparkXYZ.
"""

import argparse
import fnmatch
import hashlib
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile


ARTIFACTS = ("sql-plugin-api", "aggregator")
BUILDVER_RE = re.compile(r"^[0-9][0-9a-z]*$")

def read_patterns(path):
    with path.open() as fh:
        return [
            line.strip()
            for line in fh
            if line.strip() and not line.lstrip().startswith("#")
        ]


def has_fnmatch_magic(pattern):
    return any(ch in pattern for ch in "*?[")


def matching_members(namelist, patterns):
    names_by_entry = {}
    for name in namelist:
        names_by_entry.setdefault(name, []).append(name)

    matches = []
    for pattern in patterns:
        if has_fnmatch_magic(pattern):
            matches.extend(fnmatch.filter(namelist, pattern))
        else:
            matches.extend(names_by_entry.get(pattern, []))
    return matches


def safe_extract(zip_handle, destination, members=None):
    destination = destination.resolve()
    for member in members if members is not None else zip_handle.namelist():
        target = (destination / member).resolve()
        if not str(target).startswith(str(destination) + os.sep):
            raise RuntimeError("refusing to extract outside destination: %s" % member)
        zip_handle.extract(member, destination)


def clean_output(target_dir):
    for dirname in ("parallel-world", "deps", "extra-resources"):
        path = target_dir / dirname
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    for jar_path in target_dir.glob("*.jar"):
        jar_path.unlink()


def artifact_jar(base_dir, artifact, scala_binary_version, project_version, buildver):
    artifact_id = "rapids-4-spark-%s_%s" % (artifact, scala_binary_version)
    classifier = "spark%s" % buildver
    jar_name = "%s-%s-%s.jar" % (artifact_id, project_version, classifier)
    jar_path = base_dir / artifact / "target" / classifier / jar_name
    if not jar_path.is_file():
        raise FileNotFoundError(
            "expected built %s jar missing: %s" % (artifact, jar_path))
    return jar_path


def file_sha1(path):
    checksum = hashlib.sha1()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def jar_signature(jar_path):
    stat = jar_path.stat()
    return "\n".join((
        "path=%s" % jar_path,
        "size=%s" % stat.st_size,
        "sha1=%s" % file_sha1(jar_path),
        "",
    ))


def dedupe_cache_key(base_dir, scala_binary_version, project_version, buildvers):
    parts = []
    for buildver in sorted(buildvers, reverse=True):
        for artifact in ARTIFACTS:
            jar_path = artifact_jar(
                base_dir, artifact, scala_binary_version, project_version, buildver)
            parts.extend((
                "buildver=%s" % buildver,
                "artifact=%s" % artifact,
                jar_signature(jar_path),
            ))
    return hashlib.sha1("\n".join(parts).encode("utf-8")).hexdigest()


def ensure_extracted_cache(jar_path, cache_dir):
    contents_dir = cache_dir / "contents"
    marker = cache_dir / ".source"
    signature = jar_signature(jar_path)

    if marker.is_file() and marker.read_text() == signature:
        return contents_dir

    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    contents_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(jar_path) as zip_handle:
        safe_extract(zip_handle, contents_dir)
    marker.write_text(signature)
    return contents_dir


def link_or_copy(src, dst):
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def link_tree_contents(src_dir, dst_dir):
    for root, _, files in os.walk(src_dir):
        root_path = Path(root)
        rel_root = root_path.relative_to(src_dir)
        target_root = dst_dir / rel_root
        target_root.mkdir(parents=True, exist_ok=True)
        for name in files:
            link_or_copy(root_path / name, target_root / name)


def link_members(contents_dir, destination, members):
    for member in members:
        if member.endswith("/"):
            continue
        src = contents_dir / member
        if src.is_file():
            link_or_copy(src, destination / member)


def root_safe_module_class_members(
        base_dir,
        scala_binary_version,
        project_version,
        buildver,
        root_safe_modules):
    members = set()
    for module in root_safe_modules:
        jar_path = artifact_jar(
            base_dir, module, scala_binary_version, project_version, buildver)
        with zipfile.ZipFile(jar_path) as zip_handle:
            members.update([
                name for name in zip_handle.namelist()
                if name.endswith(".class")
            ])
    return members


def copy_and_extract_jars(
        base_dir,
        target_dir,
        scala_binary_version,
        project_version,
        buildvers,
        from_single_shim,
        from_each,
        root_safe_modules):
    parallel_world = target_dir / "parallel-world"
    cache_root = target_dir / "unshim-parallel-world-cache"
    sorted_buildvers = sorted(buildvers, reverse=True)
    root_buildver = sorted_buildvers[0]

    for buildver in sorted_buildvers:
        classifier = "spark%s" % buildver
        for artifact in ARTIFACTS:
            jar_path = artifact_jar(
                base_dir, artifact, scala_binary_version, project_version, buildver)
            contents_dir = ensure_extracted_cache(
                jar_path, cache_root / classifier / artifact)
            with zipfile.ZipFile(jar_path) as zip_handle:
                namelist = zip_handle.namelist()

            link_tree_contents(contents_dir, parallel_world / classifier)
            if buildver == root_buildver and artifact == "sql-plugin-api":
                link_tree_contents(contents_dir, parallel_world)
            if buildver == root_buildver and artifact == "aggregator":
                root_safe_members = root_safe_module_class_members(
                    base_dir,
                    scala_binary_version,
                    project_version,
                    buildver,
                    root_safe_modules)
                missing_members = sorted(root_safe_members - set(namelist))
                if missing_members:
                    raise RuntimeError(
                        "root-safe module classes missing from aggregator: %s" %
                        ", ".join(missing_members))
                link_members(contents_dir, parallel_world, sorted(root_safe_members))

            patterns = from_each
            if buildver == root_buildver:
                patterns = from_single_shim + from_each
            members = matching_members(namelist, patterns)
            link_members(contents_dir, parallel_world, members)


def run_checked(command, cwd, env=None):
    subprocess.run(command, cwd=str(cwd), env=env, check=True)


def remove_allowlisted_from_spark_shared(parallel_world, from_single_shim):
    shared_dir = parallel_world / "spark-shared"
    if not shared_dir.is_dir():
        return
    resolved_shared_dir = shared_dir.resolve()

    def validate_pattern(pattern):
        pattern_path = Path(pattern)
        if pattern_path.is_absolute():
            raise RuntimeError("refusing absolute spark-shared allowlist path: %s" % pattern)
        if ".." in pattern_path.parts:
            raise RuntimeError("refusing parent traversal in spark-shared allowlist path: %s" %
                               pattern)

    def resolve_candidate(path, pattern):
        if Path(pattern).is_absolute():
            raise RuntimeError("refusing absolute spark-shared allowlist path: %s" % pattern)
        resolved = path.resolve()
        if resolved_shared_dir not in resolved.parents:
            raise RuntimeError("refusing spark-shared allowlist path outside spark-shared: %s" %
                               pattern)
        return resolved

    for pattern in from_single_shim:
        validate_pattern(pattern)
        if has_fnmatch_magic(pattern):
            for path in shared_dir.rglob("*"):
                if fnmatch.fnmatch(path.relative_to(shared_dir).as_posix(), pattern):
                    resolved = resolve_candidate(path, pattern)
                    if resolved.is_file():
                        resolved.unlink()
        else:
            path = shared_dir / pattern
            resolved = resolve_candidate(path, pattern)
            if resolved.is_file():
                resolved.unlink()


def validate_buildvers(buildvers):
    invalid = [buildver for buildver in buildvers if not BUILDVER_RE.match(buildver)]
    if invalid:
        raise RuntimeError("invalid build version token(s): %s" % ", ".join(invalid))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mvn-base-dir", required=True,
        help="Maven build root containing module target directories")
    parser.add_argument("--source-dir", required=True,
        help="Top-level spark-rapids source directory")
    parser.add_argument("--project-version", required=True)
    parser.add_argument("--scala-binary-version", required=True)
    parser.add_argument("--buildvers", required=True,
        help="Comma-separated Spark build versions, for example 350,411")
    parser.add_argument("--ignore-shim-revisions-check", action="store_true",
        help="Continue when per-shim build metadata revisions differ")
    args = parser.parse_args()

    base_dir = Path(args.mvn_base_dir).resolve()
    source_dir = Path(args.source_dir).resolve()
    dist_dir = source_dir / "dist"
    target_dir = base_dir / "dist" / "target"
    parallel_world = target_dir / "parallel-world"
    buildvers = [item.strip() for item in args.buildvers.split(",") if item.strip()]

    if len(buildvers) == 0:
        raise RuntimeError("no build versions were supplied")
    validate_buildvers(buildvers)

    from_single_shim = read_patterns(dist_dir / "unshimmed-common-from-single-shim.txt")
    from_each = read_patterns(dist_dir / "unshimmed-from-each-spark3xx.txt")
    root_safe_modules = read_patterns(dist_dir / "root-safe-module-classes.txt")

    print("Direct unshim parallel-world assembly for Spark versions: %s" %
          ", ".join(buildvers),
          flush=True)
    clean_output(target_dir)
    copy_and_extract_jars(
        base_dir,
        target_dir,
        args.scala_binary_version,
        args.project_version,
        buildvers,
        from_single_shim,
        from_each,
        root_safe_modules)

    revision_check = subprocess.run(
        [str(dist_dir / "scripts" / "check-shims-revisions.sh"), ",".join(buildvers)],
        cwd=str(target_dir),
        check=False)
    if revision_check.returncode != 0:
        if args.ignore_shim_revisions_check:
            print("Ignoring shim revision check failure for direct unshim parallel-world assembly",
                  flush=True)
        else:
            revision_check.check_returncode()

    dedupe_env = os.environ.copy()
    dedupe_env["UNSHIM_FAST"] = "1"
    dedupe_env["UNSHIM_DEDUPE_CACHE_DIR"] = str(
        target_dir / "unshim-dedupe-cache" / dedupe_cache_key(
            base_dir,
            args.scala_binary_version,
            args.project_version,
            buildvers))
    dedupe_env["UNSHIMMED_COMMON_FROM_SINGLE_SHIM_TXT"] = str(
        dist_dir / "unshimmed-common-from-single-shim.txt")
    dedupe_env["KEEP_IN_SPARK_SHARED_TXT"] = str(dist_dir / "keep-in-spark-shared.txt")
    dedupe_env["KEEP_IN_SPARK_SHIM_DIRS_TXT"] = str(
        dist_dir / "keep-in-spark-shim-dirs.txt")
    dedupe_env["UNSHIM_ANALYZER_SCRIPT"] = str(
        dist_dir / "scripts" / "analyze-parallel-world-deps.py")
    run_checked([str(dist_dir / "scripts" / "binary-dedupe.sh")],
                cwd=target_dir,
                env=dedupe_env)
    remove_allowlisted_from_spark_shared(parallel_world, from_single_shim)

    print("Direct unshim parallel-world output: %s" % parallel_world, flush=True)


if __name__ == "__main__":
    sys.exit(main())
