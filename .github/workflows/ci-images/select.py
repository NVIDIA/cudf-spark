# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select Docker recipes from the complete event diff; run from the repository root."""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(".github/workflows/ci-images")
CATALOG = HERE / "suites.json"
SHARED = {str(HERE / "select.py"), ".github/workflows/docker-images.yml",
          ".github/workflows/build-docker-image.yml"}


def git(*args):
    return subprocess.check_output(["git", *args])


def paths(*args):
    return {os.fsdecode(p) for p in git(*args).split(b"\0") if p}


def recipes(catalog):
    result = {}
    for suite, entries in catalog.items():
        if suite not in ("ci-toolchains", "shuffle-examples", "udf-template"):
            raise ValueError(f"Unknown Docker suite: {suite}")
        for name, entry in entries.items():
            if name in result:
                raise ValueError(f"Duplicate image name: {name}")
            result[name] = (suite, entry)
    return result


def at_revision(rev):
    if not git("ls-tree", "--name-only", rev, "--", str(CATALOG)):
        return {}
    return recipes(json.loads(git("show", f"{rev}:{CATALOG}")))


def inputs_for(entry):
    dockerfile = Path(entry["dockerfile"])
    context = Path(entry.get("context", dockerfile.parent))
    smoke = HERE / "smoke" / (entry["smoke"] + ".sh")
    ignores = [str(context / ".dockerignore"), str(dockerfile) + ".dockerignore"]
    return str(context), str(smoke), ignores


def affected(entry, changed):
    _, smoke, ignores = inputs_for(entry)
    watched = SHARED | {entry["dockerfile"], smoke, *ignores, *entry.get("recipe-inputs", [])}
    if len(entry.get("platform-args", {})) > 1:
        watched.add(str(HERE / "promote-platforms.sh"))
    return bool(changed & watched)


def still_current(name):
    # Called under the package lock: an old rerun must not roll a newer tag back.
    git("fetch", "--no-tags", "--depth=1", "origin", os.environ["GITHUB_REF"])
    recipe = at_revision("HEAD")[name]
    return (at_revision("FETCH_HEAD").get(name) == recipe and
            not affected(recipe[1], paths("diff", "--no-renames", "--name-only", "-z",
                                           "HEAD", "FETCH_HEAD")))


def select():
    event = os.environ["GITHUB_EVENT_NAME"]
    payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    ref = os.environ["GITHUB_REF_NAME"]
    tag = "validate"
    base = None
    if event == "pull_request":
        # Checkout is the synthetic merge; its first parent is the base it was tested against.
        base = "HEAD^1"
    else:
        if ref != "main" and not re.fullmatch(r"release/\d{2}\.\d{2}", ref):
            raise ValueError(f"Not an image publication branch: {ref}")
        tag = ref.removeprefix("release/")
        if event == "push" and not payload["created"]:
            base = payload["before"]
            if not re.fullmatch(r"[0-9a-f]{40}", base):
                raise ValueError(f"Invalid push base: {base}")
            if subprocess.run(["git", "cat-file", "-e", base + "^{commit}"],
                              stderr=subprocess.DEVNULL).returncode:
                git("fetch", "--no-tags", "--depth=1", "origin", base)
    tracked = paths("ls-files", "-z")
    images = recipes(json.loads(CATALOG.read_text()))
    expected = {p for p in tracked if re.fullmatch(r"(?:dockerfile(?:[._-].+)?|.+\.dockerfile)",
                                                   Path(p).name, re.IGNORECASE)
                and not p.endswith(".dockerignore")}
    configured = [entry["dockerfile"] for _, entry in images.values()]
    if expected != set(configured) or len(set(configured)) != len(configured):
        raise ValueError(f"Each Dockerfile needs one suite entry: missing={sorted(expected-set(configured))}, "
                         f"obsolete={sorted(set(configured)-expected)}; check for duplicate entries")
    changed = paths("diff", "--no-renames", "--name-only", "-z", base, "HEAD") if base else set()
    previous = images
    if str(CATALOG) in changed:
        previous = at_revision(base)

    jobs = {"validate": [], "publish": []}
    for name, (suite, entry) in images.items():
        ci = suite == "ci-toolchains"
        context, smoke, ignores = inputs_for(entry)
        inputs = entry.get("recipe-inputs", [])
        if not all(p in tracked for p in [smoke, *inputs]):
            raise ValueError(f"{name}: missing tracked smoke script or recipe input")
        if not ci and event != "pull_request":
            # Examples and templates are checked before merge; they are never published.
            continue
        if base:
            if not (affected(entry, changed) or previous.get(name) != (suite, entry)):
                continue
        publish = ci and event != "pull_request"
        workers = []
        for arch, overrides in entry.get("platform-args", {"amd64": {}}).items():
            if arch not in ("amd64", "arm64"):
                raise ValueError(f"Unsupported runner architecture: {arch}")
            cache = f"type=gha,version=2,scope={name}-{arch},timeout=2m"
            workers.append({"platform": f"linux/{arch}", "arch": arch,
                            "build-args": entry.get("build-args", {}) | overrides,
                            "cache-from": [] if publish else [cache],
                            "cache-to": [cache + ",mode=max,ignore-error=true"]})
        config = {"image": name, "context": context, "dockerfile": entry["dockerfile"],
                  "tag": tag, "smoke-test": smoke, "smoke-args": entry.get("smoke-args", []),
                  "recipe-inputs": inputs + [p for p in ignores if p in tracked],
                  "workers": workers, "multi": len(workers) > 1}
        jobs["publish" if publish else "validate"].append(config)
    return jobs


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--current":
        current = still_current(sys.argv[2])
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            print("current=" + str(current).lower(), file=output)
        if not current:
            print(f"Skipping superseded recipe for {sys.argv[2]}; its branch inputs changed.")
        sys.exit(0)
    jobs = select()
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        for operation, matrix in jobs.items():
            print(f"{operation}=" + json.dumps(matrix, separators=(",", ":")), file=output)
            print(f"{operation}: " + (", ".join(c["image"] for c in matrix) or "none"))
