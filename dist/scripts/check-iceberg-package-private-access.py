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

"""Require Iceberg package-private callers to be stored at the dist jar root."""

from __future__ import print_function

import argparse
import collections
import os
import re
import sys

from java.io import DataInputStream, FileInputStream, IOException
from java.util.jar import JarFile
from javassist.bytecode import AccessFlag, ClassFile, ConstPool


ICEBERG_PREFIX = "org/apache/iceberg/"
ICEBERG_SHADED_PREFIX = "org/apache/iceberg/shaded/"
SHIM_DIR_RE = re.compile(r"^spark[0-9][0-9a-z]*$")
DESCRIPTOR_CLASS_RE = re.compile(r"L([^;<>()\[\]]+);")
BUILD_VERSION_RE = re.compile(r"^[0-9][0-9a-z]*$")
MEMBER_VISIBILITY = AccessFlag.PUBLIC | AccessFlag.PRIVATE | AccessFlag.PROTECTED

Member = collections.namedtuple("Member", "access name descriptor")
MemberRef = collections.namedtuple("MemberRef", "kind owner name descriptor")
ClassInfo = collections.namedtuple(
    "ClassInfo", "name access super_name interfaces fields methods class_refs member_refs")
Finding = collections.namedtuple("Finding", "entry caller target reason runtime")
RuntimeSelection = collections.namedtuple("RuntimeSelection", "build_version path")


def _internal_name(name):
    return name.replace(".", "/") if name else None


def _descriptor_classes(descriptor):
    return set(DESCRIPTOR_CLASS_RE.findall(descriptor or ""))


def _normalized_class_names(name):
    internal_name = _internal_name(name)
    if internal_name.startswith("["):
        return _descriptor_classes(internal_name)
    return {internal_name}


def _parse_class(stream):
    class_file = ClassFile(DataInputStream(stream))
    pool = class_file.getConstPool()
    class_refs = set()
    for name in pool.getClassNames():
        class_refs.update(_normalized_class_names(name))
    member_refs = []

    for index in range(1, pool.getSize()):
        tag = pool.getTag(index)
        if tag == ConstPool.CONST_Fieldref:
            ref = MemberRef("field", pool.getFieldrefClassName(index),
                            pool.getFieldrefName(index), pool.getFieldrefType(index))
        elif tag == ConstPool.CONST_Methodref:
            ref = MemberRef("method", pool.getMethodrefClassName(index),
                            pool.getMethodrefName(index), pool.getMethodrefType(index))
        elif tag == ConstPool.CONST_InterfaceMethodref:
            ref = MemberRef("method", pool.getInterfaceMethodrefClassName(index),
                            pool.getInterfaceMethodrefName(index),
                            pool.getInterfaceMethodrefType(index))
        else:
            if tag == ConstPool.CONST_MethodType:
                class_refs.update(_descriptor_classes(
                    pool.getUtf8Info(pool.getMethodTypeInfo(index))))
            elif tag == ConstPool.CONST_Dynamic:
                class_refs.update(_descriptor_classes(pool.getDynamicType(index)))
            elif tag == ConstPool.CONST_InvokeDynamic:
                class_refs.update(_descriptor_classes(pool.getInvokeDynamicType(index)))
            continue
        ref = MemberRef(ref.kind, _internal_name(ref.owner), ref.name, ref.descriptor)
        member_refs.append(ref)
        class_refs.add(ref.owner)
        class_refs.update(_descriptor_classes(ref.descriptor))

    fields = tuple(Member(field.getAccessFlags(), field.getName(), field.getDescriptor())
                   for field in class_file.getFields())
    methods = tuple(Member(method.getAccessFlags(), method.getName(), method.getDescriptor())
                    for method in class_file.getMethods())
    for member in fields + methods:
        class_refs.update(_descriptor_classes(member.descriptor))

    name = _internal_name(class_file.getName())
    class_refs.discard(name)
    inner_access = class_file.getInnerAccessFlags()
    access = inner_access if inner_access >= 0 else class_file.getAccessFlags()
    return ClassInfo(name, access,
                     _internal_name(class_file.getSuperclass()),
                     tuple(_internal_name(name) for name in class_file.getInterfaces()),
                     fields, methods, frozenset(class_refs), tuple(member_refs))


def _is_class_entry(entry):
    return (entry.endswith(".class") and not entry.endswith("/module-info.class") and
            not entry.startswith("META-INF/versions/"))


def has_class_entries(path):
    if not os.path.exists(path):
        return False
    repository = LazyClassRepository(path, lambda entry: True)
    try:
        return bool(repository)
    finally:
        repository.close()


def load_classes(path, entry_predicate):
    repository = LazyClassRepository(path, entry_predicate)
    try:
        return list(repository.items())
    finally:
        repository.close()


class LazyClassRepository(object):
    """Index a class layout and parse classes only when resolution needs them."""

    def __init__(self, path, entry_predicate):
        if not os.path.exists(path):
            raise IOError("class layout does not exist: %s" % path)
        self.path = path
        self.archive = JarFile(path) if os.path.isfile(path) else None
        self.entries = {}
        self.cache = {}
        if self.archive:
            entries = self.archive.entries()
            while entries.hasMoreElements():
                item = entries.nextElement()
                entry = item.getName()
                if not item.isDirectory() and _is_class_entry(entry) and entry_predicate(entry):
                    self.entries[entry[:-6]] = item
        else:
            for root, _, files in os.walk(path):
                for file_name in files:
                    full_path = os.path.join(root, file_name)
                    entry = os.path.relpath(full_path, path).replace(os.sep, "/")
                    if _is_class_entry(entry) and entry_predicate(entry):
                        self.entries[entry[:-6]] = full_path

    def __len__(self):
        return len(self.entries)

    def get(self, name, default=()):
        if name not in self.entries:
            return default
        return (self._load(name),)

    def _load(self, name):
        if name not in self.cache:
            source = self.entries[name]
            stream = (self.archive.getInputStream(source) if self.archive else
                      FileInputStream(source))
            try:
                self.cache[name] = _parse_class(stream)
            finally:
                stream.close()
        return self.cache[name]

    def items(self):
        for name in sorted(self.entries):
            yield name + ".class", self._load(name)

    def close(self):
        if self.archive:
            self.archive.close()


def _is_runtime_entry(entry):
    return entry.startswith(ICEBERG_PREFIX) and not entry.startswith(ICEBERG_SHADED_PREFIX)


def _is_plugin_entry(entry):
    return ICEBERG_PREFIX in entry and ICEBERG_SHADED_PREFIX not in entry


def _classes_by_name(entries):
    result = collections.defaultdict(list)
    for _, info in entries:
        result[info.name].append(info)
    return result


def _entry_world(entry):
    first = entry.split("/", 1)[0]
    if first == "spark-shared":
        return "shared"
    if SHIM_DIR_RE.match(first):
        return first[len("spark"):]
    return "root"


def _plugin_world(layout_entries, build_version):
    if build_version is None:
        return layout_entries
    visible = []
    class_names = set()
    for world in ("root", "shared", build_version):
        for entry, info in layout_entries:
            if _entry_world(entry) == world and info.name not in class_names:
                visible.append((entry, info))
                class_names.add(info.name)
    return visible


def _find_members(runtime_classes, plugin_classes, owner, reference):
    pending = [owner]
    visited = set()
    while pending:
        next_pending = []
        found = set()
        for current in pending:
            if current in visited:
                continue
            visited.add(current)
            infos = (list(runtime_classes.get(current, ())) +
                     list(plugin_classes.get(current, ())))
            for info in infos:
                members = info.fields if reference.kind == "field" else info.methods
                for member in members:
                    if member.name == reference.name and member.descriptor == reference.descriptor:
                        found.add((info.name, member))
                if info.super_name:
                    next_pending.append(info.super_name)
                next_pending.extend(info.interfaces)
        if found:
            return found
        pending = next_pending
    return set()


def _package_name(class_name):
    return class_name.rsplit("/", 1)[0]


def _is_root_entry(entry):
    first = entry.split("/", 1)[0]
    return first != "spark-shared" and not SHIM_DIR_RE.match(first)


def read_runtime_manifest(path):
    if not os.path.isfile(path):
        raise RuntimeError("Iceberg runtime manifest is missing: %s" % path)
    selections = []
    paths_by_build_version = collections.defaultdict(set)
    with open(path, "r") as manifest:
        for line_number, line in enumerate(manifest, 1):
            line = line.rstrip("\r\n")
            if not line:
                continue
            fields = line.split("\t")
            if len(fields) != 2 or not BUILD_VERSION_RE.match(fields[0]) or not fields[1]:
                raise RuntimeError("invalid Iceberg runtime manifest line %d: %s" %
                                   (line_number, line))
            build_version, runtime_path = fields
            runtime_path = None if runtime_path == "-" else runtime_path
            seen_paths = paths_by_build_version[build_version]
            if runtime_path in seen_paths or (None in seen_paths and runtime_path is not None) or \
                    (runtime_path is None and seen_paths):
                raise RuntimeError("conflicting or duplicate Iceberg runtime manifest line %d: %s" %
                                   (line_number, line))
            seen_paths.add(runtime_path)
            selections.append(RuntimeSelection(build_version, runtime_path))
    if not selections:
        raise RuntimeError("Iceberg runtime manifest is empty: %s" % path)
    return selections


def find_package_private_access(layout_entries, runtime_classes, runtime_label):
    plugin_classes = _classes_by_name(layout_entries)
    findings = set()
    callers = set()
    for entry, caller in layout_entries:
        caller_findings = set()
        for target_name in caller.class_refs:
            for target in runtime_classes.get(target_name, ()):
                if not target.access & AccessFlag.PUBLIC:
                    caller_findings.add((target_name, "package-private class"))
        for reference in caller.member_refs:
            if not reference.owner.startswith(ICEBERG_PREFIX):
                continue
            for declaring_class, member in _find_members(
                    runtime_classes, plugin_classes, reference.owner, reference):
                if (not member.access & MEMBER_VISIBILITY and
                        _package_name(caller.name) == _package_name(declaring_class)):
                    separator = ":" if reference.kind == "field" else ""
                    target = "%s.%s%s%s" % (declaring_class, member.name,
                                             separator, member.descriptor)
                    caller_findings.add((target, "package-private %s" % reference.kind))
        if caller_findings:
            callers.add((entry, caller.name))
            for target, reason in caller_findings:
                findings.add(Finding(entry, caller.name, target, reason, runtime_label))
    return callers, findings


def _runtime_paths(args):
    if args.runtime_manifest:
        if args.iceberg_runtime or args.runtime_directory:
            raise RuntimeError("runtime manifest cannot be combined with runtime paths")
        return read_runtime_manifest(args.runtime_manifest)
    paths = list(args.iceberg_runtime)
    for directory in args.runtime_directory:
        paths.extend(os.path.join(directory, name) for name in os.listdir(directory)
                     if name.endswith(".jar"))
    if not paths:
        raise RuntimeError("no Iceberg audit runtime was provided")
    return [RuntimeSelection(None, path) for path in sorted(paths)]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("layout", help="assembled dist jar or parallel-world directory")
    parser.add_argument("iceberg_runtime", nargs="*", help="Iceberg runtime jar(s)")
    parser.add_argument("--runtime-directory", action="append", default=[])
    parser.add_argument("--runtime-manifest")
    parser.add_argument("--expected-build-versions")
    args = parser.parse_args(argv)

    try:
        if not has_class_entries(args.layout):
            raise RuntimeError("assembled layout is missing or contains no class files: %s" %
                               args.layout)
        layout_entries = load_classes(args.layout, _is_plugin_entry)
        runtime_selections = _runtime_paths(args)
        runtime_paths = [selection for selection in runtime_selections if selection.path]
        if args.expected_build_versions:
            if not args.runtime_manifest:
                raise RuntimeError("expected build versions require a runtime manifest")
            expected_build_versions = set(filter(None, re.split(
                r"[,\s]+", args.expected_build_versions)))
            if not expected_build_versions or any(not BUILD_VERSION_RE.match(build_version)
                                                  for build_version in expected_build_versions):
                raise RuntimeError("invalid expected build versions: %s" %
                                   args.expected_build_versions)
            manifest_build_versions = set(selection.build_version
                                          for selection in runtime_selections)
            if manifest_build_versions != expected_build_versions:
                raise RuntimeError("Iceberg runtime manifest build versions %s do not match "
                                   "expected build versions %s" %
                                   (sorted(manifest_build_versions),
                                    sorted(expected_build_versions)))
        if runtime_paths and not layout_entries:
            raise RuntimeError("assembled layout contains no Iceberg plugin classes")
        callers = set()
        findings = set()
        for selection in runtime_selections:
            runtime_path = selection.path
            if runtime_path is None:
                continue
            runtime_classes = LazyClassRepository(runtime_path, _is_runtime_entry)
            try:
                if not runtime_classes:
                    raise RuntimeError("runtime contains no Iceberg classes: %s" % runtime_path)
                world_entries = _plugin_world(
                    layout_entries, selection.build_version)
                runtime_label = os.path.basename(runtime_path)
                if selection.build_version:
                    runtime_label = "spark%s; %s" % (
                        selection.build_version, runtime_label)
                runtime_callers, runtime_findings = find_package_private_access(
                    world_entries, runtime_classes, runtime_label)
                callers.update(runtime_callers)
                findings.update(runtime_findings)
            finally:
                runtime_classes.close()
    except (IOError, OSError, IOException, RuntimeError) as error:
        print("Iceberg package-private access audit failed: %s" % error, file=sys.stderr)
        return 2

    violations = sorted(finding for finding in findings if not _is_root_entry(finding.entry))
    if violations:
        print("Iceberg package-private access must be root-safe; found non-root callers:",
              file=sys.stderr)
        for entry in sorted(set(finding.entry for finding in violations)):
            entry_findings = [finding for finding in violations if finding.entry == entry]
            print("  %s (%s)" % (entry, entry_findings[0].caller.replace("/", ".")),
                  file=sys.stderr)
            for finding in entry_findings:
                print("    -> %s [%s; %s]" % (finding.target.replace("/", "."),
                                               finding.reason, finding.runtime),
                      file=sys.stderr)
        print("Move each caller to a module listed in dist/root-safe-module-classes.txt "
              "or promote it with dist/unshimmed-common-from-single-shim.txt.", file=sys.stderr)
        return 1

    print("Iceberg package-private access audit passed: %d caller classes are at the jar root "
          "across %d runtime world(s)" % (len(callers), len(runtime_paths)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
