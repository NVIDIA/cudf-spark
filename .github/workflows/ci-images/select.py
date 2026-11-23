# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Select affected Docker images and compute their validation or publication settings."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree

HERE = Path(".github/workflows/ci-images")
CATALOG = HERE / "suites.json"
PATH_RULES = HERE / "paths.json"


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


def affected(entry, changed, rules, *, publish):
    _, smoke, ignores = inputs_for(entry)
    watched = {entry["dockerfile"], smoke, *ignores, *entry.get("recipe-inputs", []),
               *rules["shared"], *rules["publication" if publish else "validation"]}
    if publish and len(entry.get("platform-args", {})) > 1:
        watched.update(rules["multi-platform-publication"])
    return bool(changed & watched)


def release_line(revision, version_file):
    pom = ElementTree.fromstring(git("show", f"{revision}:{version_file}"))
    version = pom.findtext("{*}version", default="").strip()
    match = re.fullmatch(r"(\d{2}\.\d{2})\.\d+(?:-[A-Za-z0-9.-]+)?", version)
    if not match:
        raise ValueError(f"Cannot determine release line from {version_file} version {version!r} at {revision}")
    return match.group(1)


def still_current(name):
    # Called under the package lock: an old rerun must not roll a newer tag back.
    git("fetch", "--no-tags", "--depth=1", "origin", os.environ["GITHUB_REF"])
    recipe = at_revision("HEAD")[name]
    if at_revision("FETCH_HEAD").get(name) != recipe:
        return False
    changed = paths("diff", "--no-renames", "--name-only", "-z", "HEAD", "FETCH_HEAD")
    rules = json.loads(PATH_RULES.read_text())
    if affected(recipe[1], changed, rules, publish=True):
        return False
    version_file = rules["release-version-file"]
    if "variants" in recipe[1] and version_file in changed:
        return release_line("HEAD", version_file) == release_line("FETCH_HEAD", version_file)
    return True


def configurations(name, entry, tracked, branch_tag, line, publish, has_publisher):
    context, smoke, ignores = inputs_for(entry)
    inputs = entry.get("recipe-inputs", []) + [p for p in ignores if p in tracked]
    variants = entry.get("variants", [{}])
    if not variants:
        raise ValueError(f"{name}: missing image variants")

    seen = set()
    for variant in variants:
        args = entry.get("build-args", {}) | variant
        suffix = ""
        if "variants" in entry:
            if set(variant) == {"UBUNTU_VER"}:
                distro = "ubuntu" + variant["UBUNTU_VER"]
            elif set(variant) == {"ROCKY_VER"}:
                distro = "rockylinux" + variant["ROCKY_VER"]
            else:
                raise ValueError(f"{name}: expected one Ubuntu or Rocky version, got {variant}")
            suffix = f"-cuda{args['CUDA_VER']}-{distro}"
        if suffix in seen:
            raise ValueError(f"{name}: duplicate variant {suffix}")
        seen.add(suffix)

        identifier = name + suffix
        workers = []
        for arch, overrides in entry.get("platform-args", {"amd64": {}}).items():
            if arch not in ("amd64", "arm64"):
                raise ValueError(f"Unsupported runner architecture: {arch}")
            cache = f"type=gha,version=2,scope={identifier}-{arch},timeout=2m"
            exports = []
            if publish:
                exports = [cache + ",mode=max"]
            elif not has_publisher:
                # These recipes have no branch publisher to populate a shared cache.
                exports = [cache + ",mode=min"]
            workers.append({
                "platform": f"linux/{arch}", "arch": arch, "build-args": args | overrides,
                "cache-from": [] if publish else [cache],
                "cache-to": exports,
            })
        yield {
            "id": identifier, "image": name, "context": context, "dockerfile": entry["dockerfile"],
            "tag": branch_tag + suffix if publish else "validate",
            "release_tag": line + suffix if publish and suffix and branch_tag == "main" else "",
            "smoke-test": smoke, "smoke-args": entry.get("smoke-args", []), "recipe-inputs": inputs,
            "workers": workers, "multi": len(workers) > 1,
        }


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

    rules = json.loads(PATH_RULES.read_text())
    version_file = rules["release-version-file"]
    line = release_line("HEAD", version_file)
    target = os.environ["GITHUB_BASE_REF"] if event == "pull_request" else ref
    if target.startswith("release/") and target != f"release/{line}":
        raise ValueError(f"Branch {target} does not match root {version_file} release line {line}")
    # A shared change already selects all recipes. Its new version file may not exist at base.
    compare_version = version_file in changed and changed.isdisjoint(rules["shared"])
    line_changed = compare_version and release_line(base, version_file) != line

    jobs = {"validate": [], "publish": []}
    for name, (suite, entry) in images.items():
        ci = suite == "ci-toolchains"
        _, smoke, _ = inputs_for(entry)
        inputs = entry.get("recipe-inputs", [])
        if not all(p in tracked for p in [smoke, *inputs]):
            raise ValueError(f"{name}: missing tracked smoke script or recipe input")
        if not ci and event != "pull_request":
            # Examples and templates are checked before merge; they are never published.
            continue
        publish = ci and event != "pull_request"
        rollover = "variants" in entry and line_changed
        if base and not (rollover or affected(entry, changed, rules, publish=publish)
                         or previous.get(name) != (suite, entry)):
            continue
        jobs["publish" if publish else "validate"].extend(
            configurations(name, entry, tracked, tag, line, publish, has_publisher=ci)
        )
    run_tests = None
    if event == "pull_request":
        tests = rules["unit-tests"]
        run_tests = bool(jobs["validate"])
        if not run_tests and tests["include"]:
            filters = [*tests["include"], *(":(exclude)" + pattern for pattern in tests["exclude"])]
            run_tests = bool(paths("diff", "--no-renames", "--name-only", "-z", base, "HEAD", "--", *filters))
    return jobs, run_tests


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--current":
        current = still_current(sys.argv[2])
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            print("current=" + str(current).lower(), file=output)
        if not current:
            print(f"Skipping superseded recipe for {sys.argv[2]}; its branch inputs changed.")
        sys.exit(0)
    jobs, run_tests = select()
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        for operation, matrix in jobs.items():
            print(f"{operation}=" + json.dumps(matrix, separators=(",", ":")), file=output)
            print(f"{operation}: " + (", ".join(c["id"] for c in matrix) or "none"))
        if run_tests is not None:
            print(f"run-tests={json.dumps(run_tests)}", file=output)
