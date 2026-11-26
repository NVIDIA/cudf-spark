# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read supported DBR -> base Spark from the tested commit, without executing it."""

import argparse
import json
import os
import re
import subprocess
from xml.etree import ElementTree as ET


COMPONENT = r"(?:0|[1-9][0-9]{0,2})"
RUNTIME = rf"[1-9][0-9]{{0,2}}\.{COMPONENT}"
SPARK_VERSION = rf"{COMPONENT}\.{COMPONENT}\.{COMPONENT}"
PROVIDER = re.compile(
    rf'^\s*val VERSION\s*=\s*DatabricksShimVersion\(\s*({COMPONENT}),\s*({COMPONENT}),'
    rf'\s*({COMPONENT}),\s*"({RUNTIME})"\s*\)\s*$', re.MULTILINE)


def runtime_label(value):
    if not isinstance(value, str) or not re.fullmatch(RUNTIME, value):
        raise ValueError("Expected a numeric major.minor DBR version")
    return value


def discover(candidate, merge_sha):
    if not re.fullmatch(r"[0-9a-f]{40}", merge_sha):
        raise ValueError("Expected a full merge SHA for DBR discovery")
    git = ["git", "--no-replace-objects", "-c", "protocol.allow=never", "-C", str(candidate), "cat-file"]
    env = {"PATH": os.defpath, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_NO_LAZY_FETCH": "1"}

    def blob(path):
        spec = f"{merge_sha}:{path}"
        if int(subprocess.check_output(git + ["-s", spec], env=env, timeout=10)) > 1024 * 1024:
            raise ValueError(f"DBR metadata is too large: {path}")
        return subprocess.check_output(git + ["blob", spec], env=env, timeout=10)

    pom = ET.fromstring(blob("scala2.13/pom.xml"))
    props = pom.find("{*}properties")
    if props is None:
        raise ValueError("Missing Scala 2.13 POM properties")
    excluded = {s.strip() for s in props.findtext("{*}dyn.shim.excluded.releases", "").split(",")}
    versions = {}
    for profile in pom.findall("{*}profiles/{*}profile"):
        release = profile.findtext("{*}id", "")
        if not release.startswith("release") or "db" not in release:
            continue
        shim = release.removeprefix("release")
        if shim in excluded:
            continue
        if not re.fullmatch(r"[0-9]+db[0-9]*", shim):
            raise ValueError("Unsupported Databricks release profile ID")
        pin = props.findtext(f"{{*}}spark{shim}.version", "")
        if pin.endswith("SNAPSHOT"):
            continue
        if profile.findtext("{*}properties/{*}spark.version") != "${spark" + shim + ".version}":
            raise ValueError(f"Unexpected Spark version source for {release}")
        provider = (f"sql-plugin/src/main/spark{shim}/scala/com/nvidia/spark/rapids/"
                    f"shims/spark{shim}/SparkShimServiceProvider.scala")
        matches = PROVIDER.findall(blob(provider).decode())
        if len(matches) != 1:
            raise ValueError(f"Expected one explicit DatabricksShimVersion in {provider}")
        major, minor, patch, runtime = matches[0]
        spark = f"{major}.{minor}.{patch}"
        # Digits alone are ambiguous; the provider supplies the actual dotted versions.
        base, suffix = shim.split("db")
        if (base != spark.replace(".", "") or (suffix and suffix != runtime.replace(".", ""))
                or pin != spark + "-databricks" + ("-" + suffix if suffix else "") or runtime in versions):
            raise ValueError(f"Duplicate runtime or inconsistent Scala POM/provider versions for {release}")
        versions[runtime] = spark
    if not versions:
        raise ValueError("No supported non-SNAPSHOT Databricks runtimes in the Scala 2.13 POM")
    return versions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--merge-sha", required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(discover(args.candidate, args.merge_sha), separators=(",", ":")))
    except (OSError, ValueError, ET.ParseError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Could not determine Databricks runtimes: {error}\n")
