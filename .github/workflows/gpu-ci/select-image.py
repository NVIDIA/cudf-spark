# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Use a compatible published AMD64 builder, or build the approved merge recipe locally."""

import json
import os
from pathlib import Path
import runpy
import subprocess
import sys


def local_build(selector, version):
    _, entry = selector["at_revision"]("HEAD")["spark-build"]
    _, _, ignores = selector["inputs_for"](entry)
    tracked = {path for path in ignores if Path(path).is_file()}
    for config in selector["configurations"]("spark-build", entry, tracked, "validate", "", False, True):
        if config["id"] == "spark-build-ubuntu22.04":
            for worker in config["workers"]:
                if worker["platform"] == "linux/amd64":
                    return {k: config[k] for k in ("image", "dockerfile", "context", "smoke-test", "smoke-args")} | {
                        "platform": worker["platform"], "build-args": worker["build-args"], "version": version,
                        "tag": f"gpu-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}",
                    }
    sys.exit("No native build configuration for spark-build/ubuntu22.04/amd64")


def main():
    tools = Path(".github/workflows/ci-images")
    selection = "spark-build/ubuntu22.04/amd64"
    selector = runpy.run_path(str(tools / "select.py"))
    spec = selector["resolve_images"]([selection], os.environ["BASE_BRANCH"] == "main")[selection]
    image = spec["image"]
    if image != "ghcr.io/nvidia/cudf-spark/spark-build" or spec["platform"] != "linux/amd64":
        sys.exit(f"Unexpected Spark builder: {image} ({spec['platform']})")
    digest_for = runpy.run_path(str(tools / "tag-baseline.py"))["manifest_digest"]
    for tag in spec["tags"]:
        digest = digest_for(image, tag)
        if digest is None:
            print(f"No published builder for {image}:{tag}", flush=True)
            continue
        ref = f"{image}@{digest}"
        subprocess.run(["docker", "pull", "--platform", "linux/amd64", ref], check=True)
        info, = json.loads(subprocess.check_output(["docker", "image", "inspect", ref]))
        label = (info["Config"].get("Labels") or {}).get("org.opencontainers.image.version")
        if (info["Os"], info["Architecture"], label) == ("linux", "amd64", spec["version"]):
            print(f"Using {image}:{tag} at {digest} ({label})")
            with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as out:
                print(f"image={ref}", file=out)
            return
        print(f"Builder at {ref} does not match {spec['platform']} recipe {spec['version']}")
    # Transport/auth/pull exceptions propagate; only confirmed missing or incompatible images reach here.
    print(f"No compatible published {image} for {spec['tags']}; building locally.")
    config = local_build(selector, spec["version"])
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as out:
        print("build=" + json.dumps(config, separators=(",", ":")), file=out)


if __name__ == "__main__":
    main()
