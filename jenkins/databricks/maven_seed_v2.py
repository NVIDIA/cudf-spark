# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

"""Seed a driver's private Maven repository with immutable public releases, never SNAPSHOTs."""

import argparse
from collections import defaultdict
import hashlib
import io
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
import tempfile


DEFAULT_ARCHIVE = "/dbfs/cached_jars/cudf-spark-ci/maven-releases-v1.tar"
BUDGET = 512 * 1024 * 1024
MAX_ARCHIVE = BUDGET + 48 * 1024 * 1024
CENTRAL = {"central": "https://repo.maven.apache.org/maven2",
           "google-maven-central": "https://maven-central.storage-download.googleapis.com/maven2"}
SOURCES = {repo: repo for repo in CENTRAL}
# Newer Resolver records the URL as well as the ID. Accept only the known mirror URLs,
# and write the bare IDs understood by the older Maven on the Databricks driver.
for repo, url in CENTRAL.items():
    for exact in (url, url + "/"):
        SOURCES[repo + "-" + hashlib.sha1(exact.encode(), usedforsecurity=False).hexdigest()] = repo
EXCLUDED = ("com/nvidia/", "com/databricks/", "ai/rapids/")


def release(name):
    p = PurePosixPath(name)
    if (len(p.parts) < 4 or str(p) != name or p.is_absolute() or name.startswith(EXCLUDED)
            or any(not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._+-]*", x) for x in p.parts)
            or "SNAPSHOT" in name.upper()):
        return False
    artifact, version, filename = p.parts[-3:]
    return (not re.search(r"\d{8}\.\d{6}-\d+", version)
            and p.suffix in {".jar", ".pom"} and filename.startswith(artifact + "-" + version)
            and filename[len(artifact + "-" + version):][:1] in {".", "-"}
            and not filename.endswith(("-sources.jar", "-javadoc.jar")))


def regular_under(root, path):
    return path.is_file() and path.resolve().is_relative_to(root) and path.resolve() == path


def pack(repository, archive):
    root = repository.resolve()
    candidates = {}
    for marker in root.rglob("_remote.repositories"):
        if not regular_under(root, marker) or marker.stat().st_size > 128 * 1024:
            continue
        origins = defaultdict(set)
        for line in marker.read_text(errors="replace").splitlines():
            if ">" in line and not line.startswith(("#", "!")):
                filename, origin = line.split(">", 1)
                origins[filename].add(origin.removesuffix("=") if origin.endswith("=") else None)
        for filename, repos in origins.items():
            # Resolver can add a local-install marker alongside the old remote marker
            # when different bytes overwrite the same GAV. Never export ambiguous bytes.
            if not repos.issubset(SOURCES) or not re.fullmatch(r"[\w.+-]+", filename):
                continue
            item = marker.parent / filename
            name = item.relative_to(root).as_posix()
            if release(name) and regular_under(root, item):
                candidates[name] = {SOURCES[key] for key in repos}
    # Prefer POMs and small dependencies; one large classifier must not crowd out the toolchain.
    ranked = sorted(candidates, key=lambda n: (not n.endswith(".pom"), (root / n).stat().st_size, n))
    selected, size = [], 0
    for name in ranked:
        actual = (root / name).stat().st_size
        if actual and size + actual <= BUDGET and len(selected) < 12000:
            size += actual
            selected.append(name)
    if not any(n.endswith(".jar") for n in selected):
        raise ValueError("No released Maven dependency JARs from Central")
    metadata = defaultdict(list)
    with tarfile.open(archive, "x", format=tarfile.GNU_FORMAT) as tar:
        for name in sorted(selected):
            source = root / name
            info = tarfile.TarInfo(name)
            info.size = source.stat().st_size
            with source.open("rb") as stream:
                tar.addfile(info, stream)
            parent, filename = name.rsplit("/", 1)
            metadata[parent].extend(f"{filename}>{repo}=\n" for repo in sorted(candidates[name]))
        for directory, items in sorted(metadata.items()):
            content = "".join(items).encode()
            info = tarfile.TarInfo(directory + "/_remote.repositories")
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    if archive.stat().st_size > MAX_ARCHIVE:
        raise ValueError("Maven seed exceeds transfer budget")
    return len(selected)


def restore(repository, archive, expected):
    if not re.fullmatch("[0-9a-f]{64}", expected):
        raise ValueError("Missing trusted Maven seed digest")
    if not archive.is_file() or not 0 < archive.stat().st_size <= MAX_ARCHIVE:
        raise ValueError("Missing or oversized Maven seed")
    root = repository.resolve()
    if root != repository.absolute() or root.parts[1:2] in (("dbfs",), ("Volumes",)):
        raise ValueError("Expected a driver-local Maven repository without symlinks")
    repository.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=root.parent, prefix=".maven-seed-") as tmp:
        stage = Path(tmp)
        local = stage / "seed.tar"
        digest = hashlib.sha256()
        with archive.open("rb") as source, local.open("xb") as target:
            while block := source.read(1024 * 1024):
                if target.tell() + len(block) > MAX_ARCHIVE:
                    raise ValueError("Maven seed exceeded transfer budget")
                digest.update(block)
                target.write(block)
        if digest.hexdigest() != expected:
            raise ValueError("Maven seed does not match the trusted producer")
        tree = stage / "repository"
        versions = set()
        with tarfile.open(local, "r:") as tar:
            for member in tar:
                name = member.name
                # Marker paths are subject to the same GAV/path checks as their artifacts.
                prefix, _, leaf = name.rpartition("/")
                parts = prefix.split("/")
                marker = (leaf == "_remote.repositories" and len(parts) >= 3
                          and release(f"{prefix}/{parts[-2]}-{parts[-1]}.pom"))
                if not member.isreg() or not (release(name) or marker):
                    raise ValueError("Invalid Maven seed member")
                path = tree / name
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("xb") as output, tar.extractfile(member) as source:
                    shutil.copyfileobj(source, output)
                versions.add(prefix)
        restored = 0
        for version in sorted(versions):
            dest = root / version
            if dest.exists() or dest.parent.resolve() != dest.parent:
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            (tree / version).rename(dest)
            restored += 1
    print(f"Maven seed: restored {restored} released artifact versions ({archive.stat().st_size} bytes)")
    return restored


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["pack", "restore"])
    parser.add_argument("--repository", type=Path, default=Path.home() / ".m2/repository")
    parser.add_argument("--archive", type=Path, default=Path(DEFAULT_ARCHIVE))
    parser.add_argument("--sha256", default="")
    args = parser.parse_args()
    if args.operation == "pack":
        print(f"Maven seed: packed {pack(args.repository, args.archive)} released dependency files")
    else:
        try:
            restore(args.repository, args.archive, args.sha256)
        except (OSError, ValueError, tarfile.TarError) as exc:
            print(f"Maven seed unavailable ({exc}); continuing with normal Maven resolution")


if __name__ == "__main__":
    main()
