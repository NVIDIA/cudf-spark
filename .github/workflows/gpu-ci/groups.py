# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validate selected shim assignments before reserving any GPU runners."""

import json
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET


def gpu_matrix(root, suites):
    catalog = json.loads((root / ".github/gpu-ci-suites.json").read_text())
    archives = json.loads((root / ".github/actions/spark-archive-cache/releases.json").read_text())
    groups = []
    seen_groups = set()
    seen_shims = set()
    it_shards = {}
    profiles = {}
    for scala, pom in (("2.12", "pom.xml"), ("2.13", "scala2.13/pom.xml")):
        profiles[scala] = {node.text for node in ET.parse(root / pom).findall(
            "./{*}profiles/{*}profile/{*}id")}

    for kind in suites:
        if kind not in ("unit", "integration") or not catalog["suites"][kind]["groups"]:
            raise ValueError(f"Unknown or empty GPU suite: {kind}")
        groups.extend(dict(row, kind=kind) for row in catalog["suites"][kind]["groups"])
    for row in groups:
        name, scala = row["group"], row["scala"]
        if not re.fullmatch(r"[a-z0-9-]+", name) or name in seen_groups:
            raise ValueError(f"Invalid or duplicate GPU group: {name}")
        if scala not in profiles:
            raise ValueError(f"{name}: unsupported Scala version {scala}")
        seen_groups.add(name)
        if row["kind"] == "integration":
            shim, shard = row["shim"], row["shard"]
            if shim != archives[scala]["version"].replace(".", "") or f"release{shim}" not in profiles[scala]:
                raise ValueError(f"{name}: Spark archive does not match the configured shim {shim}")
            shards = it_shards.setdefault((scala, shim), [])
            if type(shard) is not int or shard not in (1, 2) or shard in shards:
                raise ValueError(f"{name}: invalid or duplicate integration shard")
            shards.append(shard)
            continue
        if not row["shims"]:
            raise ValueError(f"{name}: empty unit shim list")
        for entry in row["shims"]:
            shim, mode = entry["shim"], entry["unit-group"]
            if (not re.fullmatch(r"[0-9]+", shim) or f"release{shim}" not in profiles[scala]
                    or (scala, shim) in seen_shims):
                raise ValueError(f"{name}: unknown or duplicate Scala {scala} shim {shim}")
            modes = ("ut1", "ut2") if scala == "2.12" else ("scala213",)
            if mode not in modes:
                raise ValueError(f"{name}: invalid unit mode {mode} for Scala {scala}")
            seen_shims.add((scala, shim))
    if any(set(shards) != {1, 2} for shards in it_shards.values()):
        raise ValueError("Each integration-test shim requires both case partitions")
    return {"include": groups}


if __name__ == "__main__":
    print(json.dumps(gpu_matrix(Path.cwd(), json.loads(sys.argv[1])), separators=(",", ":")))
