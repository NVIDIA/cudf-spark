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

from __future__ import print_function

import contextlib
import imp
import os
import shutil
import sys
import tempfile
import unittest
import zipfile

from java.io import DataOutputStream, FileOutputStream
from javassist.bytecode import AccessFlag, ClassFile, FieldInfo, MethodInfo


SCRIPT = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                      "check-iceberg-package-private-access.py")
LINT = imp.load_source("check_iceberg_package_private_access", SCRIPT)
RUNTIME_DISCOVERY = imp.load_source(
    "iceberg_runtime",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                 "build", "iceberg_runtime.py"))


POM_TEMPLATE = """\
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <artifactId>%s</artifactId>
  <dependencies>%s</dependencies>
</project>
"""
RUNTIME_DEPENDENCY = """\
<dependency>
  <groupId>org.apache.iceberg</groupId>
  <artifactId>iceberg-spark-runtime-${iceberg.artifact.suffix}_${scala.binary.version}</artifactId>
  <version>${iceberg.111x.version}</version>
</dependency>
"""


@contextlib.contextmanager
def temporary_directory():
    path = tempfile.mkdtemp()
    try:
        yield path
    finally:
        shutil.rmtree(path)


class OutputSink(object):
    def __init__(self):
        self.parts = []

    def write(self, value):
        self.parts.append(value)

    def flush(self):
        pass

    def getvalue(self):
        return "".join(self.parts)


@contextlib.contextmanager
def captured_stream(name):
    output = OutputSink()
    original = getattr(sys, name)
    setattr(sys, name, output)
    try:
        yield output
    finally:
        setattr(sys, name, original)


def write_class(root, entry, class_name, super_name="java.lang.Object",
                methods=(), fields=(), references=(), class_references=(),
                access=AccessFlag.PUBLIC):
    class_file = ClassFile(False, class_name, super_name)
    class_file.setAccessFlags(access)
    pool = class_file.getConstPool()
    for target in class_references:
        pool.addClassInfo(target)
    for access, name, descriptor in methods:
        method = MethodInfo(pool, name, descriptor)
        method.setAccessFlags(access)
        class_file.addMethod(method)
    for access, name, descriptor in fields:
        field = FieldInfo(pool, name, descriptor)
        field.setAccessFlags(access)
        class_file.addField(field)
    for kind, owner, name, descriptor in references:
        owner_index = pool.addClassInfo(owner)
        if kind == "field":
            pool.addFieldrefInfo(owner_index, name, descriptor)
        else:
            pool.addMethodrefInfo(owner_index, name, descriptor)

    path = os.path.join(root, *entry.split("/"))
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        os.makedirs(parent)
    output = DataOutputStream(FileOutputStream(path))
    try:
        class_file.write(output)
    finally:
        output.close()


def write_runtime(runtime, method_access=0):
    write_class(runtime, "org/apache/iceberg/p/Base.class", "org.apache.iceberg.p.Base",
                methods=((method_access, "hidden", "()V"),))


def write_inherited_caller(layout, prefix):
    write_class(layout, "org/apache/iceberg/p/GpuChild.class",
                "org.apache.iceberg.p.GpuChild", "org.apache.iceberg.p.Base")
    write_class(layout, prefix + "org/apache/iceberg/p/Caller.class",
                "org.apache.iceberg.p.Caller",
                references=(("method", "org.apache.iceberg.p.GpuChild", "hidden", "()V"),))


def write_aggregator(path, modules):
    archive = zipfile.ZipFile(path, "w")
    try:
        for artifact_id, dependencies in modules:
            archive.writestr(
                "META-INF/maven/com.nvidia/%s/pom.xml" % artifact_id,
                POM_TEMPLATE % (artifact_id, dependencies))
    finally:
        archive.close()


class IcebergPackagePrivateAccessTest(unittest.TestCase):
    def test_aggregator_runtime_discovery_is_fail_closed(self):
        with temporary_directory() as root:
            aggregator = os.path.join(root, "aggregator.jar")
            real_module = "rapids-4-spark-iceberg-1-11-x_2.13"
            write_aggregator(aggregator, [(real_module, RUNTIME_DEPENDENCY)])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                self.assertEqual([
                    ("org.apache.iceberg", "iceberg-spark-runtime-4.1_2.13", "1.11.0")
                ], RUNTIME_DISCOVERY.coordinates(
                    archive, "413", "2.13",
                    lambda name: {"iceberg.111x.version": "1.11.0"}.get(name)))
            finally:
                archive.close()

            write_aggregator(aggregator, [(real_module, "")])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                with self.assertRaises(RuntimeError):
                    RUNTIME_DISCOVERY.coordinates(archive, "413", "2.13", lambda name: None)
            finally:
                archive.close()

            write_aggregator(aggregator, [
                ("rapids-4-spark-iceberg-common_2.13", "")])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                with self.assertRaises(RuntimeError):
                    RUNTIME_DISCOVERY.coordinates(archive, "413", "2.13", lambda name: None)
            finally:
                archive.close()

    def test_aggregator_stub_is_explicit(self):
        with temporary_directory() as root:
            aggregator = os.path.join(root, "aggregator.jar")
            write_aggregator(aggregator, [
                ("rapids-4-spark-iceberg-stub_2.12", "")])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                self.assertEqual([], RUNTIME_DISCOVERY.coordinates(
                    archive, "330", "2.12", lambda name: None))
            finally:
                archive.close()

    def test_root_inherited_package_private_caller_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            write_inherited_caller(layout, "")
            with captured_stream("stdout") as stdout:
                result = LINT.main([layout, runtime])
            self.assertEqual(0, result)
            self.assertIn("1 caller classes", stdout.getvalue())

    def test_non_root_inherited_package_private_caller_fails(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            write_inherited_caller(layout, "spark-shared/")
            with captured_stream("stderr") as stderr:
                result = LINT.main([layout, runtime])
            self.assertEqual(1, result)
            self.assertIn("Caller.class", stderr.getvalue())
            self.assertIn("package-private method", stderr.getvalue())

    def test_runtime_versions_are_audited_independently(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            public_runtime = os.path.join(root, "runtime-public")
            private_runtime = os.path.join(root, "runtime-private")
            write_runtime(public_runtime, AccessFlag.PUBLIC)
            write_runtime(private_runtime)
            write_inherited_caller(layout, "spark-shared/")
            with captured_stream("stderr") as stderr:
                result = LINT.main([layout, public_runtime, private_runtime])
            self.assertEqual(1, result)
            self.assertIn("runtime-private", stderr.getvalue())

    def test_array_reference_to_package_private_class_fails(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_class(runtime, "org/apache/iceberg/p/Hidden.class",
                        "org.apache.iceberg.p.Hidden", access=0)
            write_class(layout, "spark-shared/org/apache/iceberg/p/Caller.class",
                        "org.apache.iceberg.p.Caller",
                        class_references=("[Lorg.apache.iceberg.p.Hidden;",))
            with captured_stream("stderr") as stderr:
                result = LINT.main([layout, runtime])
            self.assertEqual(1, result)
            self.assertIn("package-private class", stderr.getvalue())

    def test_package_private_field_fails(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_class(runtime, "org/apache/iceberg/p/Base.class",
                        "org.apache.iceberg.p.Base", fields=((0, "hidden", "I"),))
            write_class(layout, "spark-shared/org/apache/iceberg/p/Caller.class",
                        "org.apache.iceberg.p.Caller",
                        references=(("field", "org.apache.iceberg.p.Base", "hidden", "I"),))
            with captured_stream("stderr") as stderr:
                result = LINT.main([layout, runtime])
            self.assertEqual(1, result)
            self.assertIn("package-private field", stderr.getvalue())

    def test_scala_synthetic_caller_fails(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            write_class(layout, "spark-shared/org/apache/iceberg/p/Caller$anon$1.class",
                        "org.apache.iceberg.p.Caller$anon$1",
                        references=(("method", "org.apache.iceberg.p.Base",
                                     "hidden", "()V"),))
            with captured_stream("stderr") as stderr:
                result = LINT.main([layout, runtime])
            self.assertEqual(1, result)
            self.assertIn("Caller$anon$1.class", stderr.getvalue())

    def test_public_override_stops_inherited_member_resolution(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            write_class(layout, "org/apache/iceberg/p/GpuChild.class",
                        "org.apache.iceberg.p.GpuChild", "org.apache.iceberg.p.Base",
                        methods=((AccessFlag.PUBLIC, "hidden", "()V"),))
            write_class(layout, "spark-shared/org/apache/iceberg/p/Caller.class",
                        "org.apache.iceberg.p.Caller",
                        references=(("method", "org.apache.iceberg.p.GpuChild",
                                     "hidden", "()V"),))
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))

    def test_missing_and_empty_layouts_fail(self):
        with temporary_directory() as root:
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            missing = os.path.join(root, "missing")
            empty = os.path.join(root, "empty")
            os.makedirs(empty)
            with captured_stream("stderr"):
                self.assertEqual(2, LINT.main([missing, runtime]))
                self.assertEqual(2, LINT.main([empty, runtime]))

    def test_runtime_manifest_preserves_build_runtime_pairs(self):
        with temporary_directory() as root:
            manifest = os.path.join(root, "runtimes.txt")
            with open(manifest, "w") as output:
                output.write("359\t/tmp/iceberg-1.9.jar\n")
                output.write("359\t/tmp/iceberg-1.10.jar\n")
                output.write("413\t/tmp/iceberg-1.11.jar\n")
            self.assertEqual([
                LINT.RuntimeSelection("359", "/tmp/iceberg-1.9.jar"),
                LINT.RuntimeSelection("359", "/tmp/iceberg-1.10.jar"),
                LINT.RuntimeSelection("413", "/tmp/iceberg-1.11.jar"),
            ], LINT.read_runtime_manifest(manifest))

    def test_stub_runtime_manifest_is_explicit(self):
        with temporary_directory() as root:
            manifest = os.path.join(root, "runtimes.txt")
            with open(manifest, "w") as output:
                output.write("330\t-\n")
            self.assertEqual([LINT.RuntimeSelection("330", None)],
                             LINT.read_runtime_manifest(manifest))

    def test_invalid_runtime_manifest_fails(self):
        with temporary_directory() as root:
            manifest = os.path.join(root, "runtimes.txt")
            with open(manifest, "w") as output:
                output.write("not a valid manifest line\n")
            with self.assertRaises(RuntimeError):
                LINT.read_runtime_manifest(manifest)
            with self.assertRaises(RuntimeError):
                LINT.read_runtime_manifest(os.path.join(root, "missing"))
            with open(manifest, "w") as output:
                pass
            with self.assertRaises(RuntimeError):
                LINT.read_runtime_manifest(manifest)
            with open(manifest, "w") as output:
                output.write("330\t-\n330\t/tmp/runtime.jar\n")
            with self.assertRaises(RuntimeError):
                LINT.read_runtime_manifest(manifest)

    def test_runtime_manifest_must_cover_parallel_worlds(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            manifest = os.path.join(root, "runtimes.txt")
            write_runtime(runtime, AccessFlag.PUBLIC)
            for build_version in ("350", "413"):
                write_class(layout, "spark%s/example/Marker.class" % build_version,
                            "example.Marker%s" % build_version)
            with open(manifest, "w") as output:
                output.write("350\t%s\n" % runtime)
            with captured_stream("stderr"):
                self.assertEqual(2, LINT.main([
                    "--runtime-manifest", manifest,
                    "--expected-build-versions", "350,413", layout]))

    def test_stub_manifest_passes_for_no_iceberg_layout(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            manifest = os.path.join(root, "runtimes.txt")
            write_class(layout, "example/Marker.class", "example.Marker")
            with open(manifest, "w") as output:
                output.write("330\t-\n")
            with captured_stream("stdout") as stdout:
                self.assertEqual(0, LINT.main([
                    "--runtime-manifest", manifest,
                    "--expected-build-versions", "330", layout]))
            self.assertIn("0 runtime world(s)", stdout.getvalue())

    def test_callers_are_paired_only_with_their_supported_runtime(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime350 = os.path.join(
                root, "iceberg-spark-runtime-3.5_2.13-1.6.new.jar")
            runtime413 = os.path.join(
                root, "iceberg-spark-runtime-4.1_2.13-1.11.new.jar")
            manifest = os.path.join(root, "runtimes.txt")
            write_runtime(runtime350)
            write_runtime(runtime413, AccessFlag.PUBLIC)
            with open(manifest, "w") as output:
                output.write("350\t%s\n" % runtime350)
                output.write("413\t%s\n" % runtime413)
            for build_version in ("350", "413"):
                prefix = "spark%s/org/apache/iceberg/p/" % build_version
                methods = ((AccessFlag.PUBLIC, "hidden", "()V"),) \
                    if build_version == "350" else ()
                write_class(layout, prefix + "GpuChild.class",
                            "org.apache.iceberg.p.GpuChild", "org.apache.iceberg.p.Base",
                            methods=methods)
                write_class(layout, prefix + "Caller.class", "org.apache.iceberg.p.Caller",
                            references=(("method", "org.apache.iceberg.p.GpuChild",
                                         "hidden", "()V"),))
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([
                    "--runtime-manifest", manifest, layout]))

    def test_plugin_hierarchies_are_isolated_by_spark_world(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            for build_version, access in (("350", AccessFlag.PUBLIC), ("413", None)):
                prefix = "spark%s/org/apache/iceberg/p/" % build_version
                methods = () if access is None else ((access, "hidden", "()V"),)
                write_class(layout, prefix + "GpuChild.class",
                            "org.apache.iceberg.p.GpuChild", "org.apache.iceberg.p.Base",
                            methods=methods)
                write_class(layout, prefix + "Caller.class", "org.apache.iceberg.p.Caller",
                            references=(("method", "org.apache.iceberg.p.GpuChild",
                                         "hidden", "()V"),))
            entries = LINT.load_classes(layout, LINT._is_plugin_entry)
            repository = LINT.LazyClassRepository(runtime, LINT._is_runtime_entry)
            try:
                _, spark350_findings = LINT.find_package_private_access(
                    LINT._plugin_world(entries, "350"), repository, "spark350")
                _, spark413_findings = LINT.find_package_private_access(
                    LINT._plugin_world(entries, "413"), repository, "spark413")
                self.assertFalse(spark350_findings)
                self.assertTrue(spark413_findings)
                self.assertTrue(all(finding.entry.startswith("spark413/")
                                    for finding in spark413_findings))
            finally:
                repository.close()

    def test_runtime_classes_are_loaded_lazily(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            write_class(runtime, "org/apache/iceberg/p/Unused.class",
                        "org.apache.iceberg.p.Unused")
            write_inherited_caller(layout, "")
            repository = LINT.LazyClassRepository(runtime, LINT._is_runtime_entry)
            try:
                entries = LINT.load_classes(layout, LINT._is_plugin_entry)
                LINT.find_package_private_access(entries, repository, "runtime")
                self.assertEqual(set(("org/apache/iceberg/p/Base",)),
                                 set(repository.cache))
            finally:
                repository.close()

    def test_no_runtime_fails(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            write_class(layout, "org/apache/iceberg/p/Caller.class",
                        "org.apache.iceberg.p.Caller")
            with captured_stream("stderr"):
                self.assertEqual(2, LINT.main([layout]))

    def test_malformed_class_fails_closed(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            path = os.path.join(layout, "spark-shared/org/apache/iceberg/p/Broken.class")
            os.makedirs(os.path.dirname(path))
            output = FileOutputStream(path)
            try:
                output.write(bytearray([0, 1, 2, 3]))
            finally:
                output.close()
            with captured_stream("stderr"):
                self.assertEqual(2, LINT.main([layout, runtime]))


if __name__ == "__main__":
    unittest.main()
