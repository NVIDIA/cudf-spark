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

"""Analyze dependencies between conventional, spark-shared, and shim classes.

The dist jar contains classes in the conventional root layout, in spark-shared,
and in one or more Spark-version-specific directories. This script inspects the
class files and reports which root or spark-shared classes still have a static
dependency path to version-specific bytecode.
"""

import argparse
import collections
import json
import os
import re
import struct
import sys
import zipfile


SHIM_DIR_RE = re.compile(r"^spark[0-9][0-9a-z]*$")
CLASSIFIER_PACKAGE_RE = re.compile(r"(^|\.)spark[0-9][0-9a-z]*($|\.)")
DESCRIPTOR_CLASS_RE = re.compile(r"L([^;<>\[\]\(\)]+);")

DEFAULT_EXCLUDES = (
    "ai.rapids.cudf.",
    "com.nvidia.shaded.",
    "org.openucx.",
)


# Bound advertised uncompressed class payloads before reading zip entries. These
# limits are well above expected generated Spark/Scala class sizes while avoiding
# unbounded decompression from malformed inputs.
MAX_ZIP_CLASS_ENTRY_BYTES = 64 * 1024 * 1024
MAX_ZIP_TOTAL_CLASS_BYTES = 1024 * 1024 * 1024


ClassInfo = collections.namedtuple("ClassInfo", ("name", "location", "entry", "deps"))


def _read_u1(data, offset):
    return data[offset], offset + 1


def _read_u2(data, offset):
    return struct.unpack_from(">H", data, offset)[0], offset + 2


def _read_u4(data, offset):
    return struct.unpack_from(">I", data, offset)[0], offset + 4


def _class_names_from_descriptor(value):
    for match in DESCRIPTOR_CLASS_RE.finditer(value):
        yield match.group(1)


def _normalize_internal_name(value):
    if not value:
        return []
    if value.startswith("["):
        return list(_class_names_from_descriptor(value))
    if "/" in value and not value.startswith("("):
        return [value]
    return list(_class_names_from_descriptor(value))


def parse_class_file(data):
    magic, offset = _read_u4(data, 0)
    if magic != 0xCAFEBABE:
        raise ValueError("not a class file")

    # minor_version, major_version
    _, offset = _read_u2(data, offset)
    _, offset = _read_u2(data, offset)

    cp_count, offset = _read_u2(data, offset)
    constant_pool = [None] * cp_count
    class_name_indexes = []
    utf8_values = []

    index = 1
    while index < cp_count:
        tag, offset = _read_u1(data, offset)
        if tag == 1:  # CONSTANT_Utf8
            length, offset = _read_u2(data, offset)
            raw = data[offset:offset + length]
            offset += length
            value = raw.decode("utf-8", errors="replace")
            constant_pool[index] = value
            utf8_values.append(value)
        elif tag in (3, 4):  # Integer, Float
            offset += 4
        elif tag in (5, 6):  # Long, Double
            offset += 8
            index += 1
        elif tag == 7:  # Class
            name_index, offset = _read_u2(data, offset)
            constant_pool[index] = name_index
            class_name_indexes.append(name_index)
        elif tag == 8:  # String
            offset += 2
        elif tag in (9, 10, 11, 12, 17, 18):  # refs, NameAndType, Dynamic, InvokeDynamic
            offset += 4
        elif tag == 15:  # MethodHandle
            offset += 3
        elif tag in (16, 19, 20):  # MethodType, Module, Package
            offset += 2
        else:
            raise ValueError("unknown constant pool tag %s" % tag)
        index += 1

    # access_flags
    _, offset = _read_u2(data, offset)
    this_class_index, offset = _read_u2(data, offset)
    this_name_index = constant_pool[this_class_index]
    this_name = constant_pool[this_name_index]

    deps = set()
    for name_index in class_name_indexes:
        for dep in _normalize_internal_name(constant_pool[name_index]):
            deps.add(dep.replace("/", "."))
    for value in utf8_values:
        for dep in _class_names_from_descriptor(value):
            deps.add(dep.replace("/", "."))

    class_name = this_name.replace("/", ".")
    deps.discard(class_name)
    return class_name, deps


def location_from_entry(entry):
    first = entry.split("/", 1)[0]
    if first == "spark-shared":
        return "spark-shared"
    if SHIM_DIR_RE.match(first):
        return first
    return "root"


def is_classifier_class(class_name):
    return bool(CLASSIFIER_PACKAGE_RE.search(class_name))


def is_version_location(location):
    return bool(SHIM_DIR_RE.match(location))


def is_version_node(node):
    class_name, location = node
    return is_version_location(location) or is_classifier_class(class_name)


def iter_class_entries(path):
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf:
            total_class_bytes = 0
            for info in zf.infolist():
                name = info.filename
                if name.endswith(".class") and not name.endswith("/module-info.class"):
                    if info.file_size > MAX_ZIP_CLASS_ENTRY_BYTES:
                        raise RuntimeError(
                            "refusing to read oversized class entry %s (%s bytes)" %
                            (name, info.file_size))
                    total_class_bytes += info.file_size
                    if total_class_bytes > MAX_ZIP_TOTAL_CLASS_BYTES:
                        raise RuntimeError(
                            "refusing to read zip with oversized class payload "
                            "(more than %s bytes)" % MAX_ZIP_TOTAL_CLASS_BYTES)
                    yield name, zf.read(info)
        return

    for root, _, files in os.walk(path):
        for file_name in files:
            if not file_name.endswith(".class") or file_name == "module-info.class":
                continue
            full_path = os.path.join(root, file_name)
            rel_path = os.path.relpath(full_path, path).replace(os.sep, "/")
            with open(full_path, "rb") as fh:
                yield rel_path, fh.read()


def should_exclude(class_name, prefixes):
    return any(class_name.startswith(prefix) for prefix in prefixes)


def load_classes(path, exclude_prefixes):
    classes = {}
    name_locations = collections.defaultdict(set)
    errors = []
    for entry, data in iter_class_entries(path):
        try:
            class_name, deps = parse_class_file(data)
        except ValueError as exc:
            errors.append("%s: %s" % (entry, exc))
            continue
        if should_exclude(class_name, exclude_prefixes):
            continue
        location = location_from_entry(entry)
        info = ClassInfo(class_name, location, entry, deps)
        node = (class_name, location)
        classes[node] = info
        name_locations[class_name].add(location)
    return classes, name_locations, errors


def resolve_dependency_targets(source_location, dep_name, name_locations):
    locations = name_locations.get(dep_name)
    if not locations:
        return []

    # Parent/root class loading wins in the current layout. When no conventional
    # class exists, prefer the source archive, then spark-shared, then the
    # remaining version-specific locations.
    if "root" in locations:
        return [(dep_name, "root")]

    ordered = []
    for preferred in (source_location, "spark-shared"):
        if preferred in locations and preferred not in ordered:
            ordered.append(preferred)
    ordered.extend(sorted(loc for loc in locations if loc not in ordered))
    return [(dep_name, loc) for loc in ordered]


def build_graph(classes, name_locations):
    graph = {node: set() for node in classes}
    for node, info in classes.items():
        for dep_name in info.deps:
            for target in resolve_dependency_targets(info.location, dep_name, name_locations):
                if target in classes:
                    graph[node].add(target)
    return graph


def reverse_graph(graph):
    rev = {node: set() for node in graph}
    for source, targets in graph.items():
        for target in targets:
            rev[target].add(source)
    return rev


def reachable_to_version_specific(graph):
    rev = reverse_graph(graph)
    version_nodes = {node for node in graph if is_version_node(node)}
    marked = set(version_nodes)
    queue = collections.deque(version_nodes)
    while queue:
        node = queue.popleft()
        for parent in rev[node]:
            if parent not in marked:
                marked.add(parent)
                queue.append(parent)
    return marked, version_nodes


def shortest_path_to_version(graph, start):
    queue = collections.deque([(start, [start])])
    seen = {start}
    while queue:
        node, path = queue.popleft()
        if node != start and is_version_node(node):
            return path
        for next_node in sorted(graph[node]):
            if next_node not in seen:
                seen.add(next_node)
                queue.append((next_node, path + [next_node]))
    return None


def tarjan_scc(graph):
    index = 0
    stack = []
    on_stack = set()
    indexes = {}
    lowlinks = {}
    components = []

    def start_node(node):
        nonlocal index
        indexes[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        return [node, iter(graph[node])]

    def maybe_emit_component(node):
        if lowlinks[node] != indexes[node]:
            return

        component = []
        while True:
            item = stack.pop()
            on_stack.remove(item)
            component.append(item)
            if item == node:
                break
        components.append(component)

    for root in graph:
        if root in indexes:
            continue

        call_stack = [start_node(root)]
        while call_stack:
            node, child_iter = call_stack[-1]
            try:
                next_node = next(child_iter)
            except StopIteration:
                call_stack.pop()
                maybe_emit_component(node)
                if call_stack:
                    parent = call_stack[-1][0]
                    lowlinks[parent] = min(lowlinks[parent], lowlinks[node])
                continue

            if next_node not in indexes:
                call_stack.append(start_node(next_node))
            elif next_node in on_stack:
                lowlinks[node] = min(lowlinks[node], indexes[next_node])
    return components


def dependency_first_component_order(graph, components):
    comp_by_node = {}
    for comp_id, component in enumerate(components):
        for node in component:
            comp_by_node[node] = comp_id

    # Source -> target means "source depends on target". Reverse component
    # edges so Kahn's algorithm emits dependencies before their users.
    prereq_edges = collections.defaultdict(set)
    indegree = collections.Counter()
    for source, targets in graph.items():
        source_comp = comp_by_node[source]
        indegree.setdefault(source_comp, 0)
        for target in targets:
            target_comp = comp_by_node[target]
            if source_comp == target_comp:
                continue
            if source_comp not in prereq_edges[target_comp]:
                prereq_edges[target_comp].add(source_comp)
                indegree[source_comp] += 1
                indegree.setdefault(target_comp, indegree[target_comp])

    ready = collections.deque(sorted(
        comp_id for comp_id in range(len(components)) if indegree[comp_id] == 0))
    ordered = []
    while ready:
        comp_id = ready.popleft()
        ordered.append(comp_id)
        for dependent in sorted(prereq_edges[comp_id]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
    return ordered


def format_node(node):
    class_name, location = node
    return "%s (%s)" % (class_name, location)


def print_path(path):
    return " -> ".join(format_node(node) for node in path)


def json_node(node):
    class_name, location = node
    return {
        "className": class_name,
        "location": location,
    }


def location_relative_entry(info):
    parts = info.entry.split("/", 1)
    if info.location == "root":
        return info.entry
    if len(parts) == 2:
        return parts[1]
    return info.entry


def direct_classifier_edges(graph):
    edges = []
    for source, targets in graph.items():
        if is_classifier_class(source[0]):
            continue
        for target in targets:
            if is_classifier_class(target[0]):
                edges.append((source, target))
    return sorted(edges)


def version_blocker_counts(graph, version_nodes, root_or_shared):
    """Count root/shared classes that can reach each version-specific node."""
    rev = reverse_graph(graph)
    counts = []
    for version_node in sorted(version_nodes):
        seen = {version_node}
        queue = collections.deque([version_node])
        impacted = set()
        while queue:
            node = queue.popleft()
            for parent in rev[node]:
                if parent in seen:
                    continue
                seen.add(parent)
                queue.append(parent)
                if parent in root_or_shared:
                    impacted.add(parent)
        if impacted:
            counts.append((len(impacted), version_node))
    return sorted(counts, key=lambda item: (-item[0], item[1]))


def nearest_version_target_counts(graph, blocked):
    """Count terminal version nodes from each blocked node's shortest path."""
    rev = reverse_graph(graph)
    distance = {}
    queue = collections.deque()
    for node in sorted(node for node in graph if is_version_node(node)):
        distance[node] = 0
        queue.append(node)

    while queue:
        node = queue.popleft()
        for parent in sorted(rev[node]):
            if parent in distance:
                continue
            distance[parent] = distance[node] + 1
            queue.append(parent)

    def rebuild_path(start):
        path = [start]
        node = start
        while not is_version_node(node):
            next_node = None
            for candidate in sorted(graph[node]):
                if distance.get(candidate) == distance[node] - 1:
                    next_node = candidate
                    break
            if next_node is None:
                return None
            path.append(next_node)
            node = next_node
        return path

    counts = collections.Counter()
    examples = {}
    paths = []
    for node in blocked:
        if node not in distance:
            continue
        path = rebuild_path(node)
        if not path:
            continue
        paths.append((node, path))
        target = path[-1]
        counts[target] += 1
        examples.setdefault(target, path)
    ranked = sorted(
        ((count, target, examples[target]) for target, count in counts.items()),
        key=lambda item: (-item[0], item[1]))
    return ranked, paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", help="dist/target/parallel-world directory or a dist jar")
    parser.add_argument("--limit", type=int, default=20,
        help="maximum number of examples to print per section")
    parser.add_argument("--exclude-prefix", action="append", default=[],
        help="class name prefix to exclude; may be passed more than once")
    parser.add_argument("--show-safe", action="store_true",
        help="print examples of spark-shared classes with no path to version-specific code")
    parser.add_argument("--show-topo", action="store_true",
        help="print root-safe spark-shared SCCs in dependency-first order")
    parser.add_argument("--show-reachability", action="store_true",
        help="print overlapping reachability counts for version-specific nodes")
    parser.add_argument("--format", choices=("text", "json"), default="text",
        help="output format")
    parser.add_argument("--write-safe-paths",
        help="write root-safe spark-shared class paths, one per line")
    args = parser.parse_args()

    exclude_prefixes = tuple(DEFAULT_EXCLUDES) + tuple(args.exclude_prefix)
    classes, name_locations, errors = load_classes(args.path, exclude_prefixes)
    graph = build_graph(classes, name_locations)
    contaminated, version_nodes = reachable_to_version_specific(graph)
    components = tarjan_scc(graph)
    component_order = dependency_first_component_order(graph, components)

    by_location = collections.Counter(info.location for info in classes.values())
    root_or_shared = {
        node for node, info in classes.items()
        if info.location in ("root", "spark-shared") and not is_classifier_class(info.name)
    }
    blocked = sorted(root_or_shared & contaminated)
    safe_shared = sorted(
        node for node in root_or_shared - contaminated
        if classes[node].location == "spark-shared")
    classifier_edges = direct_classifier_edges(graph)
    version_components = [comp for comp in components if any(is_version_node(node) for node in comp)]
    safe_sccs = []
    for comp_id in component_order:
        component = components[comp_id]
        safe_members = sorted(node for node in component if node in safe_shared)
        if safe_members:
            safe_sccs.append((comp_id, safe_members))
    version_blockers = (
        version_blocker_counts(graph, version_nodes, root_or_shared)
        if args.show_reachability or args.format == "json" else [])
    nearest_targets, blocked_paths = nearest_version_target_counts(graph, blocked)
    safe_shared_paths = sorted(location_relative_entry(classes[node]) for node in safe_shared)

    if args.write_safe_paths:
        with open(args.write_safe_paths, "w", encoding="utf-8") as out:
            for path in safe_shared_paths:
                out.write(path)
                out.write("\n")

    if args.format == "json":
        output = {
            "path": args.path,
            "classCount": len(classes),
            "locationCounts": dict(sorted(by_location.items())),
            "versionSpecificNodeCount": len(version_nodes),
            "rootOrSharedBlockedCount": len(blocked),
            "rootSafeSparkSharedCount": len(safe_shared),
            "sccCount": len(components),
            "versionSpecificSccCount": len(version_components),
            "directClassifierDependencyCount": len(classifier_edges),
            "rootSafeSparkSharedPaths": safe_shared_paths,
            "directClassifierDependencyExamples": [
                {
                    "source": json_node(source),
                    "target": json_node(target),
                }
                for source, target in classifier_edges[:args.limit]
            ],
            "topVersionBlockersByReachability": [
                {
                    "blockedRootOrSharedCount": count,
                    "target": json_node(target),
                }
                for count, target in version_blockers[:args.limit]
            ],
            "nearestVersionTargetCounts": [
                {
                    "blockedShortestPathCount": count,
                    "target": json_node(target),
                    "examplePath": [json_node(node) for node in path],
                }
                for count, target, path in nearest_targets[:args.limit]
            ],
            "rootSafeSparkSharedSccCount": len(safe_sccs),
            "rootSafeSparkSharedSccExamples": [
                {
                    "componentId": comp_id,
                    "classCount": len(members),
                    "classExamples": [json_node(node) for node in members[:args.limit]],
                }
                for comp_id, members in safe_sccs[:args.limit]
            ],
            "blockedExamples": [
                [json_node(node) for node in path]
                for _, path in blocked_paths[:args.limit]
            ],
        }
        json.dump(output, sys.stdout, indent=2, sort_keys=True)
        print()
        return

    print("Loaded %d classes from %s" % (len(classes), args.path))
    if errors:
        print("Skipped %d malformed class files" % len(errors))
    print("Class locations:")
    for location, count in sorted(by_location.items()):
        print("  %s: %d" % (location, count))
    print("Version-specific/classifier nodes: %d" % len(version_nodes))
    print("Root or spark-shared nodes with a path to version-specific code: %d" % len(blocked))
    print("Root-safe spark-shared nodes: %d" % len(safe_shared))
    print("SCCs: %d total, %d containing version-specific code" %
        (len(components), len(version_components)))

    print("\nDirect classifier-package dependencies: %d" % len(classifier_edges))
    for source, target in classifier_edges[:args.limit]:
        print("  %s -> %s" % (format_node(source), format_node(target)))
    if len(classifier_edges) > args.limit:
        print("  ... %d more" % (len(classifier_edges) - args.limit))

    if args.show_reachability:
        print("\nTop version-specific blockers by upstream root/shared reachability:")
        for count, target in version_blockers[:args.limit]:
            print("  %d <- %s" % (count, format_node(target)))
        if len(version_blockers) > args.limit:
            print("  ... %d more" % (len(version_blockers) - args.limit))

    print("\nNearest version targets from shortest blocked paths:")
    for count, target, path in nearest_targets[:args.limit]:
        print("  %d -> %s" % (count, format_node(target)))
        print("    e.g. %s" % print_path(path))
    if len(nearest_targets) > args.limit:
        print("  ... %d more" % (len(nearest_targets) - args.limit))

    print("\nNearest paths from root/spark-shared code to version-specific code:")
    for _, path in blocked_paths[:args.limit]:
        print("  %s" % print_path(path))
    if len(blocked) > args.limit:
        print("  ... %d more blocked classes" % (len(blocked) - args.limit))

    if args.show_safe:
        print("\nSpark-shared classes with no path to version-specific code:")
        for node in safe_shared[:args.limit]:
            print("  %s" % format_node(node))
        if len(safe_shared) > args.limit:
            print("  ... %d more" % (len(safe_shared) - args.limit))

    if args.show_topo:
        print("\nRoot-safe spark-shared SCCs in dependency-first order:")
        for printed, (comp_id, safe_members) in enumerate(safe_sccs):
            print("  component %d, %d class(es)" % (comp_id, len(safe_members)))
            for node in safe_members[:3]:
                print("    %s" % format_node(node))
            if len(safe_members) > 3:
                print("    ... %d more in component" % (len(safe_members) - 3))
            if printed + 1 >= args.limit:
                break


if __name__ == "__main__":
    sys.exit(main())
