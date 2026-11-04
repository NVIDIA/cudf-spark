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

"""Read-only shim preflight. Static coverage is deliberately narrower than compilation."""

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import html
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

from get_buildvers import _get_buildvers
from shimpler_source_graph import load_source_graph, source_lineage
from shimpler_source_resolver import resolve_sources_with_ties, scan_module_sources
from shimpler_source_targets import EXTERNAL_EXACT_TARGETS


MAX_PEER_CHECKS = 5
VERSIONED = re.compile(r"^(.*)/src/main/spark([^/]+)/(scala|java)/(.+)$")
PACKAGE = re.compile(r"(?m)^\s*package\s+([\w.]+)")
DECLARATION = re.compile(
    r"(?m)^(?:(?:(?:private|protected)(?:\[[\w.]+\])?"
    r"|public|sealed|abstract|final|case)\s+)*"
    r"(?:class|object|trait|interface|enum)\s+(\w+)")
IMPORT = re.compile(r"(?m)^\s*import\s+([^\n;{]+(?:\{[^}]*\}[^\n;]*)?)")
LEGACY_EXACT = re.compile(
    rb'(?m)^([ \t]*)\{[ \t]*"spark"[ \t]*:[ \t]*"([^"]+)"[ \t]*\}([ \t]*)$')
LEGACY_DESCENDANTS = re.compile(rb'"includeDescendantShims"\s*:\s*(true|false)')


def normalize_legacy_exact(source):
    source = LEGACY_EXACT.sub(
        lambda match: match[1] + b'{"spark":["' + match[2] +
        b'"],"carryForward":"none"}' + match[3], source)
    return LEGACY_DESCENDANTS.sub(
        lambda match: b'"carryForward":"all"' if match[1] == b'true'
        else b'"carryForward":"none"', source)


class PreflightError(Exception):
    pass


class MergeConflict(PreflightError):
    pass


def run(command, root, timeout=90):
    result = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        # Do not echo gh stderr: it may contain authentication diagnostics.
        raise PreflightError("{} failed (exit {})".format(command[0], result.returncode))
    return result.stdout


def git(root, *args):
    return run(["git", *args], root).strip()


def git_paths(root, *args):
    return set(filter(None, run(["git", *args, "-z"], root).split("\0")))


def commit_sha(root, ref):
    try:
        return git(root, "rev-parse", "--verify", ref + "^{commit}")
    except PreflightError as error:
        raise PreflightError("Cannot resolve commit {}; fetch its history and retry".format(ref)) from error


def without_comments(source):
    """Keep strings opaque and preserve lines while removing Scala/Java comments."""
    token = re.compile(r'"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|/\*[\s\S]*?\*/|//[^\n]*')
    return token.sub(lambda m: "\n" * m[0].count("\n") + " "
                     if m[0].startswith(("/", '"')) else m[0], source)


def named_imports(source):
    """Return explicit import paths; wildcard imports are reported as unmodelled."""
    names, wildcards = set(), []
    for match in IMPORT.finditer(without_comments(source)):
        value = match[1].strip().removeprefix("static ")
        if "{" in value:
            prefix, selectors = value.split("{", 1)
            for selector in selectors.split("}", 1)[0].split(","):
                original = re.split(r"\s*(?:=>|\bas\b)\s*", selector.strip())[0]
                if original in ("_", "*"):
                    wildcards.append(prefix + original)
                elif original:
                    names.add(prefix + original)
        elif value.endswith(("._", ".*")):
            wildcards.append(value)
        else:
            names.add(re.split(r"\s+as\s+", value)[0])
    return names, wildcards


def supported_versions(root):
    result = set()
    for relative in ("pom.xml", "scala2.13/pom.xml"):
        pom = root / relative
        if pom.is_file():
            try:
                result.update(v.strip() for v in _get_buildvers("all.buildvers", str(pom)).split(",")
                              if v.strip())
            except Exception as error:
                raise PreflightError("Cannot enumerate release profiles in " + relative) from error
    if not result:
        raise PreflightError("No supported release profiles found")
    return result


def coverage(root, modules, requested):
    versions = supported_versions(root)
    if requested:
        unknown = set(requested) - versions
        if unknown:
            raise PreflightError("Unknown or excluded build profiles: " + ", ".join(sorted(unknown)))
        versions &= set(requested)
    try:
        graphs = [load_source_graph(str(root / 'pom.xml'))]
        scala_213 = root / 'scala2.13/pom.xml'
        if scala_213.is_file():
            graphs.append(load_source_graph(str(scala_213)))
        for target in set.intersection(*(set(graph) for graph in graphs)):
            if any(source_lineage(graph, target) != source_lineage(graphs[0], target)
                   for graph in graphs[1:]):
                raise PreflightError('Different source parents for %s across Scala trees' % target)
    except ValueError as error:
        raise PreflightError('Invalid shim source graph: %s' % error) from error
    graph_by_target = {target: graph for graph in graphs for target in graph}
    findings, skipped, checked = [], set(), set()
    for module in modules:
        main = root / module / "src/main"
        if not main.is_dir():
            raise PreflightError("Missing module source directory: " + str(main))
        try:
            variants, unannotated = scan_module_sources(
                str(root / module), set(graph_by_target), EXTERNAL_EXACT_TARGETS)
        except ValueError as error:
            raise PreflightError('Invalid shim selectors in %s: %s' % (module, error)) from error
        unannotated_main = [path for path in unannotated if '/src/main/' in path]
        if unannotated_main:
            raise PreflightError('%s: missing shim annotation' % unannotated_main[0])
        main_variants = [variant for variant in variants
                         if variant.logical_path.startswith('src/main/')]
        selected_by_path = defaultdict(set)
        for version in sorted(versions):
            graph = graph_by_target.get(version)
            if graph is None:
                raise PreflightError('No source graph for build profile %s' % version)
            selected, ties = resolve_sources_with_ties(graph, main_variants, version)
            for logical, paths in sorted(ties.items()):
                findings.append(dict(kind='duplicate_source', module=module, buildver=version,
                                     source=logical[len('src/main/'):],
                                     providers=sorted(Path(path).relative_to(root).as_posix()
                                                      for path in paths)))
            for source in selected.values():
                selected_by_path[Path(source.physical_path).relative_to(root).as_posix()].add(version)
        variant_by_path = {Path(variant.physical_path).relative_to(root).as_posix(): variant
                           for variant in main_variants}
        providers, callers = defaultdict(list), []
        versioned_symbols = set()
        # Source roots only: never inspect target/, generated links or dependencies.
        for source_root in sorted(main.iterdir()):
            if source_root.name not in ("scala", "java") and not re.fullmatch(
                    r"spark[0-9][\w]*", source_root.name):
                continue
            for path in sorted(source_root.rglob("*")):
                if path.is_symlink() or path.suffix not in (".scala", ".java") or not path.is_file():
                    continue
                relative = path.relative_to(root).as_posix()
                source = path.read_text(encoding="utf-8")
                versioned = VERSIONED.fullmatch(relative)
                active = selected_by_path[relative] if versioned else versions
                callers.append((relative, source, active))
                code = without_comments(source)
                package = PACKAGE.search(code)
                if package:
                    for symbol in set(DECLARATION.findall(code)):
                        qualified = package[1] + "." + symbol
                        providers[qualified].append((relative, active))
                        if versioned:
                            versioned_symbols.add(qualified)
                if versioned:
                    variant = variant_by_path[relative]
                    owner = min(target for selector in variant.selectors
                                for target in selector.targets)
                    if versioned[2] != owner:
                        raise PreflightError("{}: owner directory must be spark{}".format(
                            relative, owner))
        for path, source, active in callers:
            if not active:
                continue
            imports, wildcards = named_imports(source)
            skipped.update((path, wildcard) for wildcard in wildcards if "shim" in wildcard.lower())
            for imported in sorted(imports):
                # Imports of members (Foo.method) also require Foo's implementation.
                symbols = [s for s in versioned_symbols
                           if imported == s or imported.startswith(s + ".")]
                for symbol in symbols:
                    available = set().union(*(enabled for _, enabled in providers[symbol]))
                    checked.update((module, symbol, v) for v in active)
                    for version in sorted(active - available):
                        findings.append(dict(kind="missing_implementation", module=module,
                                             buildver=version, symbol=symbol, caller=path,
                                             providers=[p for p, _ in providers[symbol]]))
    return dict(status="BLOCKED" if findings else "PASS", scope="explicit imports of module-local "
                "versioned declarations and generated source collisions; not a compile result",
                modules=modules, buildvers=sorted(versions), checked_requirements=len(checked),
                findings=findings, unmodelled_wildcard_imports=[dict(caller=p, imported=i)
                                                            for p, i in sorted(skipped)])


def graphql(root, query, **variables):
    command = ["gh", "api", "graphql", "-f", "query=" + query]
    for name, value in variables.items():
        if value is not None:
            command.extend(["-F", "{}={}".format(name, value)])
    payload = json.loads(run(command, root))
    if payload.get("errors") or not payload.get("data"):
        raise PreflightError("GitHub returned incomplete GraphQL data")
    return payload["data"]["repository"]


PR_FIELDS = ("number title url state baseRefName headRefOid isDraft mergeCommit { oid } "
             "baseRepository { nameWithOwner }")


def next_cursor(page, previous):
    cursor = page["pageInfo"].get("endCursor")
    if not cursor or cursor == previous:
        raise PreflightError("Pagination did not advance; scan incomplete")
    return cursor


def remote_base(root, owner, name, branch):
    query = """query($owner:String!, $name:String!, $ref:String!) {
      repository(owner:$owner, name:$name) { ref(qualifiedName:$ref) { target { oid } } }
    }"""
    ref = graphql(root, query, owner=owner, name=name, ref="refs/heads/" + branch)["ref"]
    if not ref:
        raise PreflightError("Target branch does not exist")
    return ref["target"]["oid"]


def open_prs(root, owner, name, branch):
    query = """query($owner:String!, $name:String!, $branch:String!, $cursor:String) {
      repository(owner:$owner, name:$name) {
        pullRequests(states:OPEN, baseRefName:$branch, first:100, after:$cursor) {
          nodes { %s changedFiles files(first:100) {
            nodes { path } pageInfo { hasNextPage endCursor }
          } } pageInfo { hasNextPage endCursor }
        }
      }
    }""" % PR_FIELDS
    result, cursor = [], None
    while True:
        page = graphql(root, query, owner=owner, name=name, branch=branch,
                       cursor=cursor)["pullRequests"]
        result.extend(page["nodes"])
        if not page["pageInfo"]["hasNextPage"]:
            return result
        cursor = next_cursor(page, cursor)


def merged_prs(root, owner, name, commits, branch):
    result = {}
    for start in range(0, len(commits), 40):
        fields = []
        for index, sha in enumerate(commits[start:start + 40]):
            fields.append('c%d: object(oid:"%s") { ... on Commit { associatedPullRequests'
                          '(first:100) { nodes { %s } pageInfo { hasNextPage } } } }'
                          % (index, sha, PR_FIELDS))
        query = ('query($owner:String!, $name:String!) { repository(owner:$owner,name:$name) {'
                 + " ".join(fields) + "} }")
        data = graphql(root, query, owner=owner, name=name)
        for commit in data.values():
            if commit is None:
                raise PreflightError("GitHub could not read a base commit")
            prs = commit["associatedPullRequests"]
            if prs["pageInfo"]["hasNextPage"]:
                raise PreflightError("Commit has more than 100 associated PRs; scan incomplete")
            for pr in prs["nodes"]:
                merge = pr.get("mergeCommit")
                if (pr["state"] == "MERGED" and pr["baseRefName"] == branch
                        and pr["baseRepository"]["nameWithOwner"].lower() == (owner + "/" + name).lower()
                        and merge and merge["oid"] in commits):
                    result[pr["number"]] = pr
    return list(result.values())


def pr_files(root, owner, name, number):
    query = """query($owner:String!, $name:String!, $number:Int!, $cursor:String) {
      repository(owner:$owner, name:$name) { pullRequest(number:$number) {
        headRefOid changedFiles files(first:100, after:$cursor) {
          nodes { path } pageInfo { hasNextPage endCursor }
        }
      } }
    }"""
    paths, cursor, head = [], None, None
    while True:
        pr = graphql(root, query, owner=owner, name=name, number=number,
                     cursor=cursor)["pullRequest"]
        if head is not None and head != pr["headRefOid"]:
            raise PreflightError("PR head changed during file pagination")
        head = pr["headRefOid"]
        page = pr["files"]
        paths.extend(node["path"] for node in page["nodes"])
        if not page["pageInfo"]["hasNextPage"]:
            if len(set(paths)) != pr["changedFiles"]:
                raise PreflightError("PR file list is truncated or changed during scan")
            return head, set(paths)
        cursor = next_cursor(page, cursor)


def candidate_files(root, owner, name, pr):
    page = pr.get("files")
    if page is not None and not page["pageInfo"]["hasNextPage"]:
        paths = {node["path"] for node in page["nodes"]}
        if len(paths) != pr["changedFiles"]:
            raise PreflightError("PR file list is truncated")
        return pr["headRefOid"], paths
    return pr_files(root, owner, name, pr["number"])


def path_signals(paths):
    families = {m[3] + "/" + m[4] for p in paths
                if p.endswith((".scala", ".java")) and (m := VERSIONED.fullmatch(p))}
    matrix = any(p in ("pom.xml", "scala2.13/pom.xml", "build/get_buildvers.py",
                       "build/shim_source_task.py", "build/dyn_shim_detection.py", "build/buildall",
                       "jenkins/version-def.sh", "jenkins/spark-premerge-build.sh",
                       ".github/workflows/mvn-verify-check.yml")
                 or p.startswith("shim-deps/") or p.startswith("scala2.13/shim-deps/")
                 or (p.startswith("jenkins/") and "databricks" in p)
                 for p in paths)
    return families, matrix


def imported_families(source):
    names, _ = named_imports(source)
    result = set()
    for name in names:
        parts = name.split(".")
        # A member import also requires its containing declaration. The peer's
        # actual file path determines which of these prefixes is a family.
        for end in range(1, len(parts) + 1):
            for language, extension in (("scala", ".scala"), ("java", ".java")):
                result.add(language + "/" + "/".join(parts[:end]) + extension)
    return result


def changed_imports(root, paths, head, dirty):
    result = set()
    for path in sorted(paths):
        if not re.search(r"/src/main/(?:scala|java|spark[^/]+/(?:scala|java))/", path):
            continue
        if not path.endswith((".scala", ".java")):
            continue
        if dirty:
            source = root / path
            if source.is_file() and not source.is_symlink():
                result.update(imported_families(source.read_text(encoding="utf-8")))
        elif git(root, "ls-tree", "--name-only", head, "--", path):
            result.update(imported_families(git(root, "show", head + ":" + path)))
    return result


def overlap_reasons(current, other, references=()):
    families, matrix = path_signals(current)
    families.update(references)
    peer_families, peer_matrix = path_signals(other)
    reasons = []
    # Documentation/tooling overlaps do not establish a shim dependency.
    overlap = {p for p in current & other if (VERSIONED.fullmatch(p) and p.endswith((".scala", ".java")))
               or re.search(r"/src/main/(?:scala|java)/.*\.(?:scala|java)$", p)
               or path_signals({p})[1]}
    if overlap:
        reasons.append("same files: " + ", ".join(sorted(overlap)))
    if families & peer_families:
        reasons.append("same shim family: " + ", ".join(sorted(families & peer_families)))
    if (families and peer_matrix) or (peer_families and matrix) or (matrix and peer_matrix):
        reasons.append("shim implementation and/or build matrix changes; inspect actual diffs")
    # Discovery uses paths only; the advisory can inspect selected combined trees.
    # Shared callers may depend on our shim/profile without changing a versioned path.
    peer_shared = any(re.search(r"/src/main/(?:scala|java)/", path)
                      and path.endswith((".scala", ".java")) for path in other)
    if (families or matrix) and peer_shared:
        reasons.append("shared-source peer may call changed shims or use changed profiles; "
                       "inspect actual diffs")
    return reasons


def related_prs(root, repo, base_ref, head_ref, validated_base, branch, own_pr,
                include_worktree=True, require_pr_head=False):
    owner, name = repo.split("/")
    base, head = commit_sha(root, base_ref), commit_sha(root, head_ref)
    if remote_base(root, owner, name, branch) != base:
        raise PreflightError("Fetched base differs from GitHub; fetch the target branch and retry")
    start = commit_sha(root, validated_base) if validated_base else git(
        root, "merge-base", base, head)
    git(root, "merge-base", "--is-ancestor", start, base)
    common = git(root, "merge-base", base, head)
    current = git_paths(root, "diff", "--name-only", "--no-renames", common, head)
    # Include work in progress when preparing a new PR; record that it is not an exact-head scan.
    is_worktree_head = head == git(root, "rev-parse", "HEAD")
    dirty = include_worktree and is_worktree_head and bool(
        git(root, "status", "--porcelain", "--untracked-files=normal"))
    if dirty:
        current.update(git_paths(root, "diff", "--name-only", "--no-renames", "HEAD"))
        current.update(git_paths(root, "ls-files", "--others", "--exclude-standard"))
    references = changed_imports(root, current, head, dirty)
    commits = git(root, "rev-list", start + ".." + base).splitlines()
    candidates = list(open_prs(root, owner, name, branch))
    if require_pr_head and own_pr is not None:
        own = next((pr for pr in candidates if pr["number"] == own_pr), None)
        if own is None or own["headRefOid"] != head:
            raise PreflightError("Requested PR is not open at the analyzed head/base; "
                                 "refresh the PR and rerun")
    open_heads = {(pr["number"], pr["headRefOid"]) for pr in candidates}
    candidates += merged_prs(root, owner, name, commits, branch)
    related = []
    candidates = sorted((p for p in candidates if p["number"] != own_pr), key=lambda p: p["number"])
    # Each read is independent; keep API concurrency bounded.
    with ThreadPoolExecutor(max_workers=4) as pool:
        files = pool.map(lambda p: candidate_files(root, owner, name, p), candidates)
        for pr, (live_head, paths) in zip(candidates, files):
            if live_head != pr["headRefOid"]:
                raise PreflightError("PR head changed during scan; retry")
            reasons = overlap_reasons(current, paths, references)
            if reasons:
                related.append(dict(number=pr["number"], title=pr["title"], url=pr["url"],
                                    state=pr["state"], head=live_head, draft=pr["isDraft"],
                                    reasons=reasons, files=sorted(paths),
                                    joint_candidate=bool(path_signals(current)[0]
                                                         or path_signals(current)[1]
                                                         or references)))
    if git(root, "rev-parse", base_ref) != base or git(root, "rev-parse", head_ref) != head:
        raise PreflightError("Local base/head refs changed during scan; retry")
    live_open = open_prs(root, owner, name, branch)
    if (remote_base(root, owner, name, branch) != base
            or {(pr["number"], pr["headRefOid"]) for pr in live_open} != open_heads):
        raise PreflightError("GitHub base or open PR heads changed during scan; retry")
    return dict(status="NEEDS_REVIEW" if related else "NO_RELATED_PRS", base=base, head=head,
                history_start=start, history_commits=len(commits), dirty=dirty,
                changed_files=sorted(current), candidates_scanned=len(candidates), related=related,
                scope="local fetched base and observed PR heads; association is not a confirmed defect")


def source_snapshot(root, tree, destination):
    """Export source data, normalizing pre-migration exact tags for comparison."""
    paths = ("pom.xml", "scala2.13/pom.xml", "sql-plugin/src/main")
    entries = []
    for record in run(["git", "ls-tree", "-rz", tree, "--", *paths], root).split("\0"):
        if record:
            metadata, path = record.split("\t", 1)
            mode, kind, oid = metadata.split()
            if kind == "blob" and mode in ("100644", "100755"):
                entries.append((oid, path))
    if not entries:
        raise PreflightError("No source-selection inputs in " + tree)
    # Reading blobs bypasses export-ignore attributes and avoids executing either tree's code.
    blobs = subprocess.run(["git", "cat-file", "--batch"], cwd=root,
                           input="".join(oid + "\n" for oid, _ in entries).encode(),
                           capture_output=True, timeout=90)
    if blobs.returncode:
        raise PreflightError("Cannot export source-selection inputs")
    stream = io.BytesIO(blobs.stdout)
    for oid, relative in entries:
        actual, kind, size = stream.readline().decode().split()
        data = stream.read(int(size))
        if actual != oid or kind != "blob" or len(data) != int(size) or stream.read(1) != b"\n":
            raise PreflightError("Truncated source snapshot")
        path = destination / relative
        if not path.resolve().is_relative_to(destination.resolve()):
            raise PreflightError("Invalid source path")
        path.parent.mkdir(parents=True, exist_ok=True)
        if relative.endswith((".scala", ".java")) and b'spark-rapids-shim-json-lines' in data:
            # The strict selector parser still needs to compare older base/peer trees.
            data = normalize_legacy_exact(data)
        path.write_bytes(data)


def finding_key(finding):
    key = (finding["kind"], finding["module"], finding["buildver"],
           finding.get("symbol"), finding.get("caller"), finding.get("source"))
    # A third selected provider worsens an existing two-provider collision.
    if finding["kind"] == "duplicate_source":
        key += (tuple(sorted(finding["providers"])),)
    return key


def review_scope(paths):
    families, matrix = path_signals(paths)
    return bool(families or matrix or any(
        re.search(r"/src/main/(?:scala|java)/.*\.(?:scala|java)$", path)
        or path in ("build/check_shim_compatibility.py", ".github/workflows/shim-compatibility.yml")
        or path.startswith(".agents/skills/check-shim-compatibility/") for path in paths))


def verify_review_refs(root, repo, base_ref, head_ref, base, head, branch, own_pr):
    owner, name = repo.split("/")
    if (commit_sha(root, base_ref), commit_sha(root, head_ref)) != (base, head):
        raise PreflightError("Local base/head refs changed during review; retry")
    if remote_base(root, owner, name, branch) != base:
        raise PreflightError("Fetched base differs from GitHub; fetch the target branch and retry")
    if own_pr is not None:
        query = """query($owner:String!, $name:String!, $number:Int!) {
          repository(owner:$owner, name:$name) { pullRequest(number:$number) {
            state headRefOid baseRefName baseRepository { nameWithOwner }
          } }
        }"""
        pr = graphql(root, query, owner=owner, name=name, number=own_pr)["pullRequest"]
        if (not pr or pr["state"] != "OPEN" or pr["headRefOid"] != head
                or pr["baseRefName"] != branch
                or pr["baseRepository"]["nameWithOwner"].lower() != repo.lower()):
            raise PreflightError("Requested PR is not open at the analyzed head/base; "
                                 "refresh the PR and rerun")


def review(root, repo, base_ref, head_ref, branch, own_pr):
    """Check the requested PR and current target without discovering unrelated PRs."""
    report = dict(status="INCOMPLETE", scope="sql-plugin source selection on the combined tree; "
                  "not compilation, runtime validation, or merge authorization")
    try:
        base, head = commit_sha(root, base_ref), commit_sha(root, head_ref)
        report.update(base=base, head=head, pr=own_pr)
        common = git(root, "merge-base", base, head)
        changed = git_paths(root, "diff", "--name-only", "--no-renames", common, head)
        report["changed_files"] = sorted(changed)
        if not review_scope(changed):
            report.update(status="NOT_APPLICABLE", reason="No shim, shared JVM source, "
                          "profile/matrix, or preflight implementation changes")
            return report
        verify_review_refs(root, repo, base_ref, head_ref, base, head, branch, own_pr)
        merged = subprocess.run(["git", "merge-tree", "--write-tree", base, head], cwd=root,
                                text=True, capture_output=True, timeout=90)
        if merged.returncode == 1:
            report.update(status="BLOCKED", error="Head and current base have merge conflicts")
            return report
        if merged.returncode:
            raise PreflightError("Cannot construct the combined source tree")
        tree = merged.stdout.splitlines()[0]
        report["combined_tree"] = tree
        with tempfile.TemporaryDirectory(prefix="shim-review-") as temporary:
            snapshots = Path(temporary)
            for label, revision in (("base", base), ("combined", tree)):
                destination = snapshots / label
                source_snapshot(root, revision, destination)
                report[label + "_coverage"] = coverage(destination, ["sql-plugin"], None)
        previous = {finding_key(f) for f in report["base_coverage"]["findings"]}
        current = report["combined_coverage"]["findings"]
        report["new_findings"] = [f for f in current if finding_key(f) not in previous]
        report["existing_findings"] = [f for f in current if finding_key(f) in previous]
        verify_review_refs(root, repo, base_ref, head_ref, base, head, branch, own_pr)
        report["status"] = "BLOCKED" if report["new_findings"] else "PASS"
    except (PreflightError, OSError, ValueError, KeyError, TypeError,
            subprocess.TimeoutExpired) as error:
        report["error"] = str(error)
    return report


def advisory(root, repo, base_ref, head_ref, validated_base, branch, own_pr):
    """Collect peer evidence independently, retaining a report when discovery fails."""
    report = dict(status="INCOMPLETE", scope="cross-PR associations; independent of the core "
                  "source-selection check, not confirmed incompatibilities")
    try:
        base, head = commit_sha(root, base_ref), commit_sha(root, head_ref)
        report.update(base=base, head=head, pr=own_pr, base_branch=branch)
        common = git(root, "merge-base", base, head)
        changed = git_paths(root, "diff", "--name-only", "--no-renames", common, head)
        if not review_scope(changed):
            report.update(status="NOT_APPLICABLE", reason="No shim, shared JVM source, "
                          "profile/matrix, or preflight implementation changes")
            return report
        report.update(related_prs(root, repo, base_ref, head_ref, validated_base, branch,
                                  own_pr, include_worktree=False, require_pr_head=True))
        report["scope"] = "cross-PR associations and bounded joint source-selection checks; not compilation"
        if check_peer_combinations(root, repo, report):
            verify_review_refs(root, repo, base_ref, head_ref, base, head, branch, own_pr)
    except (PreflightError, OSError, ValueError, KeyError, TypeError,
            subprocess.TimeoutExpired) as error:
        report["status"] = "INCOMPLETE"
        report["error"] = str(error)
    return report


def merge_snapshot(root, left, right):
    result = subprocess.run(["git", "merge-tree", "--write-tree", left, right], cwd=root,
                            text=True, capture_output=True, timeout=30)
    if result.returncode == 1:
        raise MergeConflict("Source trees have merge conflicts")
    if result.returncode:
        raise PreflightError("Cannot construct a combined source tree")
    return result.stdout.splitlines()[0]


def fetch_peer(root, repo, head):
    if not re.fullmatch(r"[0-9a-f]{40}", head) or not re.fullmatch(r"[\w.-]+/[\w.-]+", repo):
        raise PreflightError("Invalid peer repository or commit identity")
    present = subprocess.run(["git", "cat-file", "-e", head + "^{commit}"], cwd=root,
                             capture_output=True, timeout=30)
    if present.returncode:
        # Fetch an immutable commit, without changing refs, FETCH_HEAD, or the worktree.
        run(["git", "fetch", "--quiet", "--no-tags", "--no-write-fetch-head",
             "--no-recurse-submodules", "https://github.com/" + repo + ".git", head], root, timeout=30)
    return commit_sha(root, head)


def tree_coverage(root, tree, temporary, cache):
    if tree not in cache:
        snapshot = temporary / tree
        source_snapshot(root, tree, snapshot)
        cache[tree] = coverage(snapshot, ["sql-plugin"], None)
    return cache[tree]


def peer_combination(root, repo, base, current_commit, current_coverage, peer, temporary, cache):
    result = dict(status="INCOMPLETE", base=base, peer_head=peer["head"], stage="fetch")
    try:
        peer_head = fetch_peer(root, repo, peer["head"])
        result["stage"] = "peer_target"
        peer_tree = merge_snapshot(root, base, peer_head)
        result["peer_tree"] = peer_tree
        peer_coverage = tree_coverage(root, peer_tree, temporary, cache)
        result["stage"] = "joint"
        joint_tree = merge_snapshot(root, current_commit, peer_head)
        result["joint_tree"] = joint_tree
        joint_coverage = tree_coverage(root, joint_tree, temporary, cache)
        # Existing current/peer defects are not new cross-PR interaction findings.
        existing = {finding_key(f) for r in (current_coverage, peer_coverage) for f in r["findings"]}
        result.update(current_findings=current_coverage["findings"],
                      peer_findings=peer_coverage["findings"],
                      buildvers=joint_coverage["buildvers"],
                      new_findings=[f for f in joint_coverage["findings"]
                                    if finding_key(f) not in existing])
        result["status"] = ("NEW_INTERACTION_FINDINGS" if result["new_findings"]
                            else "NO_NEW_INTERACTION_FINDINGS")
    except MergeConflict as error:
        result.update(status="COMBINATION_CONFLICT" if result["stage"] == "joint" else "INCOMPLETE",
                      error=str(error))
    except (PreflightError, OSError, ValueError, KeyError, TypeError,
            subprocess.TimeoutExpired) as error:
        result["error"] = str(error)
    return result


def check_peer_combinations(root, repo, report):
    report["joint_scope"] = ("sql-plugin explicit imports and generated source collisions; "
                             "compare joint findings against each PR combined with target; "
                             "not compilation, signatures, runtime validation, or merge readiness")
    report["joint_check_limit"] = MAX_PEER_CHECKS
    candidates = []
    for peer in report["related"]:
        if peer["state"] == "MERGED":
            peer["joint_check"] = dict(status="ALREADY_IN_TARGET",
                                      reason="Already in the target snapshot; inspect the core report")
        elif not peer["joint_candidate"]:
            peer["joint_check"] = dict(status="NOT_CHECKED",
                                      reason="Shared-source overlap without a shim/profile signal")
        else:
            candidates.append(peer)
    # Prefer explicit family associations over broad path/matrix matches.
    candidates.sort(key=lambda p: (not any(r.startswith("same shim family:") for r in p["reasons"]),
                                   not any(path_signals(p["files"])), -p["number"]))
    for peer in candidates[MAX_PEER_CHECKS:]:
        peer["joint_check"] = dict(status="NOT_CHECKED", reason="Per-run joint-check limit reached")
    selected = candidates[:MAX_PEER_CHECKS]
    if not selected:
        return False
    deadline = time.monotonic() + 150
    base, head = report["base"], report["head"]
    current_tree = merge_snapshot(root, base, head)
    report["current_tree"] = current_tree
    # Synthetic commit objects provide ancestry for merge-tree; no refs or hooks change.
    current_commit = git(root, "-c", "commit.gpgsign=false", "-c", "user.name=Shim preflight", "-c",
                         "user.email=shim-preflight@example.invalid", "commit-tree", current_tree,
                         "-p", base, "-p", head, "-m", "Temporary shim source-selection combination")
    with tempfile.TemporaryDirectory(prefix="shim-peers-") as directory:
        temporary, cache = Path(directory), {}
        current_coverage = tree_coverage(root, current_tree, temporary, cache)
        for peer in selected:
            if time.monotonic() >= deadline:
                peer["joint_check"] = dict(status="NOT_CHECKED", reason="Joint-check time budget reached")
                continue
            peer["joint_check"] = peer_combination(root, repo, base, current_commit,
                                                  current_coverage, peer, temporary, cache)
    owner, name = repo.split("/")
    query = ('query($owner:String!, $name:String!) { repository(owner:$owner,name:$name) {'
             + " ".join('p%d: pullRequest(number:%d) { state headRefOid baseRefName }'
                        % (p["number"], p["number"]) for p in selected) + "} }")
    live = graphql(root, query, owner=owner, name=name)
    for peer in selected:
        pr = live.get("p" + str(peer["number"]))
        if (not pr or pr["state"] != "OPEN" or pr["headRefOid"] != peer["head"]
                or pr["baseRefName"] != report["base_branch"]):
            peer["joint_check"].update(status="INCOMPLETE", error="Peer changed during joint verification; "
                                      "observed snapshot evidence is stale, refresh before attributing findings")
    return True


def finding_message(finding):
    if finding["kind"] == "missing_implementation":
        return "{}: {} imports {}, but no implementation is selected".format(
            finding["buildver"], finding["caller"], finding["symbol"])
    return "{}: duplicate destination {} selected from {}".format(
        finding["buildver"], finding["source"], ", ".join(finding["providers"]))


def annotation_location(root, head, finding, changed=()):
    # Locate against the actual PR head, rather than assuming combined-tree line numbers match it.
    paths = [finding["caller"]] if "caller" in finding else finding["providers"]
    for path in sorted(paths, key=lambda path: (path not in changed, path)):
        if not git(root, "ls-tree", "--name-only", head, "--", path):
            continue
        original = git(root, "show", head + ":" + path)
        source = without_comments(original)
        token = finding.get("symbol", "").split(".")[-1]
        if token:
            line = next((i for i, text in enumerate(source.splitlines(), 1)
                         if re.search(r"\b" + re.escape(token) + r"\b", text)), 1)
        else:
            line = next((i for i, text in enumerate(original.splitlines(), 1)
                         if re.search(r'"spark"\s*:\s*\[[^\]]*"' +
                                      re.escape(finding["buildver"]) + '"',
                                      text)), 1)
        return path, line
    return None


def actions_escape(value, parameter=False):
    value = str(value).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return value.replace(":", "%3A").replace(",", "%2C") if parameter else value


def write_review(root, report, output, github_actions=False):
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n",
                                      encoding="utf-8")
    lines = ["# Shim compatibility preflight: " + report["status"], "", report["scope"], ""]
    for key in ("head", "base", "combined_tree"):
        if key in report:
            lines.append("- {}: `{}`".format(key, report[key]))
    if "error" in report:
        lines += ["", "Check incomplete or blocked: " + html.escape(report["error"])]
    if "reason" in report:
        lines += ["", report["reason"]]
    for key, title in (("new_findings", "New static findings"),
                       ("existing_findings", "Existing base findings (not attributed to this PR)")):
        if key in report:
            lines += ["", "## {} ({})".format(title, len(report[key]))]
            lines += ["- " + html.escape(finding_message(f)) for f in report[key]] or ["None."]
    if "combined_coverage" in report:
        result = report["combined_coverage"]
        lines += ["", "Profiles: " + ", ".join(result["buildvers"]),
                  "", "Unmodelled wildcard imports: {}. See report.json for full inputs and limits.".
                  format(len(result["unmodelled_wildcard_imports"]))]
    lines += ["", "Cross-PR evidence is published separately by the shim-related-prs advisory. "
              "Static PASS does not establish compilation or merge readiness.", ""]
    (output / "report.md").write_text("\n".join(lines), encoding="utf-8")
    if github_actions:
        # GitHub limits annotations per step; the complete report remains in the artifact.
        for finding in report.get("new_findings", [])[:10]:
            location = annotation_location(root, report["head"], finding,
                                           report.get("changed_files", []))
            parameters = "title=Shim compatibility"
            if location:
                parameters += ",file={},line={}".format(actions_escape(location[0], True), location[1])
            print("::error {}::{}".format(parameters, actions_escape(finding_message(finding))))
        if report["status"] in ("INCOMPLETE", "BLOCKED") and "error" in report:
            print("::error title=Shim compatibility::" + actions_escape(report["error"]))


def write_advisory(report, output, github_actions=False):
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n",
                                      encoding="utf-8")
    lines = ["# Related PR advisory: " + report["status"], "",
             "This advisory does not change the core shim-compatibility result.", ""]
    for key in ("head", "base", "history_start", "candidates_scanned"):
        if key in report:
            lines.append("- {}: `{}`".format(key, report[key]))
    if "error" in report:
        lines += ["", "Discovery incomplete: " + html.escape(report["error"]),
                  "No conclusion about related PRs; refresh this advisory when needed."]
    if "reason" in report:
        lines += ["", report["reason"]]
    if "joint_scope" in report:
        lines += ["", html.escape(report["joint_scope"]),
                  "Joint-check limit: {} open peers; unverified peers remain explicit.".
                  format(report["joint_check_limit"])]
    for pr in report.get("related", []):
        lines += ["", "- [#{}]({}): {} ({})".format(
            pr["number"], pr["url"], html.escape(pr["title"]), pr["state"]),
            "  Observed head: `{}`. {}".format(pr["head"],
                                               html.escape("; ".join(pr["reasons"])))]
        joint = pr.get("joint_check")
        if joint:
            lines += ["  Joint check: `{}`. {}".format(
                joint["status"], html.escape(joint.get("error", joint.get("reason", ""))))]
            lines += ["  - " + html.escape(finding_message(f)) for f in joint.get("new_findings", [])]
    if report["status"] == "NO_RELATED_PRS":
        lines += ["", "No related PRs found in this scan."]
    lines += ["", "Associations require diff inspection. Joint findings are static source-selection "
              "evidence; matching compilation and semantic validation remain necessary. "
              "No new static interaction findings does not prove compatibility. "
              "See report.json for full inputs and file lists.", ""]
    (output / "report.md").write_text("\n".join(lines), encoding="utf-8")
    if github_actions:
        warnings = []
        if report["status"] == "INCOMPLETE":
            warnings.append("Advisory incomplete: " + report["error"] + "; core result is unchanged")
        for pr in report.get("related", []):
            joint = pr.get("joint_check", {})
            if joint.get("status") in ("NO_NEW_INTERACTION_FINDINGS", "ALREADY_IN_TARGET"):
                continue
            if report["status"] == "INCOMPLETE":
                continue
            details = ("; ".join(finding_message(f) for f in joint.get("new_findings", [])[:3])
                       if joint.get("status") == "NEW_INTERACTION_FINDINGS" else "")
            warnings.append("Inspect related PR #{}: {}. {} {}".format(
                pr["number"], pr["url"], joint.get("status", "Association only; not a confirmed defect"),
                details or joint.get("error", joint.get("reason", ""))))
        for warning in warnings[:10]:
            print("::warning title=Related PR advisory::" + actions_escape(warning))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    subparsers = parser.add_subparsers(dest="mode", required=True)
    static = subparsers.add_parser("coverage", help="offline source-selection checks")
    static.add_argument("--module", action="append", help="default: sql-plugin")
    static.add_argument("--buildver", action="append", help="default: all supported release profiles")
    for mode, description in (("related-prs", "read-only GitHub PR association scan"),
                              ("review", "combined-tree source-selection check"),
                              ("advisory", "independent cross-PR report for relevant changes")):
        online = subparsers.add_parser(mode, help=description)
        online.add_argument("--repo", default="NVIDIA/cudf-spark")
        online.add_argument("--base-ref", default="origin/main")
        online.add_argument("--head-ref", default="HEAD")
        online.add_argument("--base-branch", default="main")
        online.add_argument("--pr", type=int, help=("PR to validate" if mode != "related-prs"
                                                  else "exclude this PR from candidates"))
        if mode != "related-prs":
            online.add_argument("--output-dir", type=Path, required=True)
            online.add_argument("--github-actions", action="store_true")
        if mode != "review":
            online.add_argument("--validated-base", help="base SHA used in the last relevant validation")
    args = parser.parse_args(argv)
    try:
        root = args.root.resolve()
        if args.mode == "coverage":
            result = coverage(root, args.module or ["sql-plugin"], args.buildver)
        elif args.mode == "review":
            result = review(root, args.repo, args.base_ref, args.head_ref,
                            args.base_branch, args.pr)
            write_review(root, result, args.output_dir, args.github_actions)
        elif args.mode == "advisory":
            result = advisory(root, args.repo, args.base_ref, args.head_ref,
                              args.validated_base, args.base_branch, args.pr)
            write_advisory(result, args.output_dir, args.github_actions)
        else:
            result = related_prs(root, args.repo, args.base_ref, args.head_ref,
                                 args.validated_base, args.base_branch, args.pr)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 2 if result["status"] == "INCOMPLETE" else 1 if result["status"] == "BLOCKED" else 0
    except (PreflightError, OSError, ValueError, KeyError, TypeError,
            subprocess.TimeoutExpired) as error:
        print(json.dumps(dict(status="INCOMPLETE", error=str(error)), indent=2))
        return 2


if __name__ == "__main__":
    sys.exit(main())
