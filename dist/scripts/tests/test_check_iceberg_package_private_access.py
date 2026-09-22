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

from java.io import DataOutputStream, File, FileOutputStream
from java.lang import Class, Double, IllegalAccessError, Long, VerifyError
from java.net import URLClassLoader
from javassist.bytecode import (
    AccessFlag, Bytecode, ClassFile, ConstPool, FieldInfo, InnerClassesAttribute,
    MethodInfo, Opcode)


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
ICEBERG_411_PROPERTIES = {
    "iceberg.111x.version": "1.11.0",
    "spark41x.iceberg.artifact.suffix": "4.1",
}


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


def write_class_file(root, entry, class_file):
    path = os.path.join(root, *entry.split("/"))
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        os.makedirs(parent)
    output = DataOutputStream(FileOutputStream(path))
    try:
        class_file.write(output)
    finally:
        output.close()


@contextlib.contextmanager
def split_loader(runtime, layout):
    parent_loader = URLClassLoader.newInstance([File(runtime).toURI().toURL()])
    child_loader = URLClassLoader.newInstance([
        File(os.path.join(layout, "spark-shared")).toURI().toURL()], parent_loader)
    try:
        yield child_loader
    finally:
        child_loader.close()
        parent_loader.close()


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

    write_class_file(root, entry, class_file)


def write_protected_field_subclass(root, entry, reference_owner, receiver_local):
    class_name = "org.apache.iceberg.p.GpuChild"
    class_file = ClassFile(False, class_name, "org.apache.iceberg.p.Base")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "org.apache.iceberg.p.Base")
    pool = class_file.getConstPool()
    descriptor = "()I" if receiver_local == 0 else "(Lorg/apache/iceberg/p/Base;)I"
    method = MethodInfo(pool, "read", descriptor)
    method.setAccessFlags(AccessFlag.PUBLIC)
    code = Bytecode(pool, 1, receiver_local + 1)
    code.addAload(receiver_local)
    code.addGetfield(reference_owner, "hidden", "I")
    code.addOpcode(Opcode.IRETURN)
    method.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(method)
    write_class_file(root, entry, class_file)


def write_protected_method_subclass(root, entry, reference_owner, receiver_local):
    class_file = ClassFile(False, "org.apache.iceberg.p.GpuChild",
                           "org.apache.iceberg.p.Base")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "org.apache.iceberg.p.Base")
    pool = class_file.getConstPool()
    descriptor = "()V" if receiver_local == 0 else "(Lorg/apache/iceberg/p/Base;)V"
    method = MethodInfo(pool, "call", descriptor)
    method.setAccessFlags(AccessFlag.PUBLIC)
    code = Bytecode(pool, 1, receiver_local + 1)
    code.addAload(receiver_local)
    code.addInvokevirtual(reference_owner, "hidden", "()V")
    code.addOpcode(Opcode.RETURN)
    method.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(method)
    write_class_file(root, entry, class_file)


def write_protected_wide_access(root, entry, field_access):
    class_file = ClassFile(False, "org.apache.iceberg.p.GpuChild",
                           "org.apache.iceberg.p.Base")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "org.apache.iceberg.p.Base")
    pool = class_file.getConstPool()
    descriptor = "(J)V" if field_access else "(JD)V"
    method = MethodInfo(pool, "access", descriptor)
    method.setAccessFlags(AccessFlag.PUBLIC)
    code = Bytecode(pool, 5, 5)
    code.addAload(0)
    code.addLload(1)
    if field_access:
        code.addPutfield("org.apache.iceberg.p.Base", "hidden", "J")
    else:
        code.addDload(3)
        code.addInvokevirtual("org.apache.iceberg.p.Base", "hidden", "(JD)V")
    code.addOpcode(Opcode.RETURN)
    method.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(method)
    write_class_file(root, entry, class_file)


def add_constructor(class_file, super_name, access=AccessFlag.PUBLIC):
    pool = class_file.getConstPool()
    constructor = MethodInfo(pool, MethodInfo.nameInit, "()V")
    constructor.setAccessFlags(access)
    code = Bytecode(pool, 1, 1)
    code.addAload(0)
    code.addInvokespecial(super_name, MethodInfo.nameInit, "()V")
    code.addOpcode(Opcode.RETURN)
    constructor.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(constructor)


def write_protected_super_caller(root, entry):
    class_file = ClassFile(False, "org.apache.iceberg.p.GpuChild",
                           "org.apache.iceberg.p.Base")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "org.apache.iceberg.p.Base")
    pool = class_file.getConstPool()
    method = MethodInfo(pool, "callSuper", "()V")
    method.setAccessFlags(AccessFlag.PUBLIC)
    code = Bytecode(pool, 1, 1)
    code.addAload(0)
    code.addInvokespecial("org.apache.iceberg.p.Base", "hidden", "()V")
    code.addOpcode(Opcode.RETURN)
    method.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(method)
    write_class_file(root, entry, class_file)


def write_protected_static_caller(root, entry):
    class_file = ClassFile(False, "org.apache.iceberg.p.GpuChild",
                           "org.apache.iceberg.p.Base")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "org.apache.iceberg.p.Base")
    pool = class_file.getConstPool()
    method = MethodInfo(pool, "callStatic", "()V")
    method.setAccessFlags(AccessFlag.PUBLIC | AccessFlag.STATIC)
    code = Bytecode(pool, 0, 1)
    code.addInvokestatic("org.apache.iceberg.p.Base", "hidden", "()V")
    code.addOpcode(Opcode.RETURN)
    method.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(method)
    write_class_file(root, entry, class_file)


def write_protected_constructor_base(root):
    class_file = ClassFile(False, "org.apache.iceberg.p.Base", "java.lang.Object")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    pool = class_file.getConstPool()
    constructor = MethodInfo(pool, MethodInfo.nameInit, "()V")
    constructor.setAccessFlags(AccessFlag.PROTECTED)
    code = Bytecode(pool, 1, 1)
    code.addAload(0)
    code.addInvokespecial("java.lang.Object", MethodInfo.nameInit, "()V")
    code.addOpcode(Opcode.RETURN)
    constructor.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(constructor)
    write_class_file(root, "org/apache/iceberg/p/Base.class", class_file)


def write_protected_constructor_subclass(root, entry, create_base):
    class_file = ClassFile(False, "org.apache.iceberg.p.GpuChild",
                           "org.apache.iceberg.p.Base")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    pool = class_file.getConstPool()
    constructor = MethodInfo(pool, MethodInfo.nameInit, "()V")
    constructor.setAccessFlags(AccessFlag.PUBLIC)
    code = Bytecode(pool, 1, 1)
    code.addAload(0)
    code.addInvokespecial("org.apache.iceberg.p.Base", MethodInfo.nameInit, "()V")
    code.addOpcode(Opcode.RETURN)
    constructor.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(constructor)
    if create_base:
        method = MethodInfo(pool, "createBase", "()V")
        method.setAccessFlags(AccessFlag.PUBLIC | AccessFlag.STATIC)
        code = Bytecode(pool, 2, 0)
        code.addNew("org.apache.iceberg.p.Base")
        code.addOpcode(Opcode.DUP)
        code.addInvokespecial("org.apache.iceberg.p.Base", MethodInfo.nameInit, "()V")
        code.addOpcode(Opcode.POP)
        code.addOpcode(Opcode.RETURN)
        method.setCodeAttribute(code.toCodeAttribute())
        class_file.addMethod(method)
    write_class_file(root, entry, class_file)


def write_method_handle_caller(root, entry, handle_kind, reference_owner,
                               member_name, descriptor,
                               constructor_access=AccessFlag.PUBLIC):
    class_file = ClassFile(False, "org.apache.iceberg.p.GpuChild",
                           "org.apache.iceberg.p.Base")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    class_file.setMajorVersion(ClassFile.JAVA_7)
    add_constructor(class_file, "org.apache.iceberg.p.Base", constructor_access)
    pool = class_file.getConstPool()
    owner = pool.addClassInfo(reference_owner)
    if handle_kind in (ConstPool.REF_getField, ConstPool.REF_getStatic,
                       ConstPool.REF_putField, ConstPool.REF_putStatic):
        target = pool.addFieldrefInfo(owner, member_name, descriptor)
    else:
        target = pool.addMethodrefInfo(owner, member_name, descriptor)
    handle = pool.addMethodHandleInfo(handle_kind, target)
    method = MethodInfo(pool, "loadHandle", "()V")
    method.setAccessFlags(AccessFlag.PUBLIC | AccessFlag.STATIC)
    code = Bytecode(pool, 1, 0)
    code.addLdc(handle)
    code.addOpcode(Opcode.POP)
    code.addOpcode(Opcode.RETURN)
    method.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(method)
    write_class_file(root, entry, class_file)


def write_invalid_member_operand(root, entry):
    class_file = ClassFile(False, "org.apache.iceberg.p.Broken", "java.lang.Object")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    pool = class_file.getConstPool()
    method = MethodInfo(pool, "read", "()I")
    method.setAccessFlags(AccessFlag.PUBLIC)
    code = Bytecode(pool, 1, 1)
    code.addAload(0)
    code.addOpcode(Opcode.GETFIELD)
    code.addIndex(pool.addClassInfo("org.apache.iceberg.p.Base"))
    code.addOpcode(Opcode.IRETURN)
    method.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(method)
    write_class_file(root, entry, class_file)


def write_invalid_method_handle(root, entry):
    class_file = ClassFile(False, "org.apache.iceberg.p.Broken", "java.lang.Object")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    class_file.setMajorVersion(ClassFile.JAVA_7)
    pool = class_file.getConstPool()
    pool.addMethodHandleInfo(
        ConstPool.REF_invokeVirtual,
        pool.addClassInfo("org.apache.iceberg.p.Base"))
    write_class_file(root, entry, class_file)


def write_invalid_legacy_interface_method_handle(root, entry):
    class_file = ClassFile(False, "org.apache.iceberg.p.Broken", "java.lang.Object")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    class_file.setMajorVersion(ClassFile.JAVA_7)
    pool = class_file.getConstPool()
    owner = pool.addClassInfo("org.apache.iceberg.p.Base")
    target = pool.addInterfaceMethodrefInfo(owner, "hidden", "()V")
    pool.addMethodHandleInfo(ConstPool.REF_invokeStatic, target)
    write_class_file(root, entry, class_file)


def write_runtime(runtime, method_access=0):
    class_file = ClassFile(False, "org.apache.iceberg.p.Base", "java.lang.Object")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "java.lang.Object")
    pool = class_file.getConstPool()
    method = MethodInfo(pool, "hidden", "()V")
    method.setAccessFlags(method_access)
    code = Bytecode(pool, 0, 1)
    code.addOpcode(Opcode.RETURN)
    method.setCodeAttribute(code.toCodeAttribute())
    class_file.addMethod(method)
    write_class_file(runtime, "org/apache/iceberg/p/Base.class", class_file)


def write_protected_source_public_nested_class(runtime):
    class_name = "org.apache.iceberg.p.BaseTaskWriter$RollingFileWriter"
    class_file = ClassFile(False, class_name, "java.lang.Object")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "java.lang.Object")
    inner_classes = InnerClassesAttribute(class_file.getConstPool())
    inner_classes.append(
        class_name, "org.apache.iceberg.p.BaseTaskWriter", "RollingFileWriter",
        AccessFlag.PROTECTED)
    class_file.addAttribute(inner_classes)
    write_class_file(
        runtime, "org/apache/iceberg/p/BaseTaskWriter$RollingFileWriter.class", class_file)


def write_protected_wide_runtime(runtime, field_access):
    class_file = ClassFile(False, "org.apache.iceberg.p.Base", "java.lang.Object")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "java.lang.Object")
    pool = class_file.getConstPool()
    if field_access:
        field = FieldInfo(pool, "hidden", "J")
        field.setAccessFlags(AccessFlag.PROTECTED)
        class_file.addField(field)
    else:
        method = MethodInfo(pool, "hidden", "(JD)V")
        method.setAccessFlags(AccessFlag.PROTECTED)
        code = Bytecode(pool, 0, 5)
        code.addOpcode(Opcode.RETURN)
        method.setCodeAttribute(code.toCodeAttribute())
        class_file.addMethod(method)
    write_class_file(runtime, "org/apache/iceberg/p/Base.class", class_file)


def write_protected_field_runtime(runtime):
    class_file = ClassFile(False, "org.apache.iceberg.p.Base", "java.lang.Object")
    class_file.setAccessFlags(AccessFlag.PUBLIC)
    add_constructor(class_file, "java.lang.Object")
    field = FieldInfo(class_file.getConstPool(), "hidden", "I")
    field.setAccessFlags(AccessFlag.PROTECTED)
    class_file.addField(field)
    write_class_file(runtime, "org/apache/iceberg/p/Base.class", class_file)


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
                    lambda name: ICEBERG_411_PROPERTIES.get(name)))
            finally:
                archive.close()

            write_aggregator(aggregator, [(real_module, "")])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                with self.assertRaises(RuntimeError) as raised:
                    RUNTIME_DISCOVERY.coordinates(
                        archive, "413", "2.13",
                        lambda name: ICEBERG_411_PROPERTIES.get(name))
                self.assertIn("0 runtime dependencies", str(raised.exception))
            finally:
                archive.close()

            write_aggregator(aggregator, [
                ("rapids-4-spark-iceberg-common_2.13", "")])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                with self.assertRaises(RuntimeError) as raised:
                    RUNTIME_DISCOVERY.coordinates(archive, "413", "2.13", lambda name: None)
                self.assertIn("must contain real Iceberg module(s) or one stub module",
                              str(raised.exception))
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

    def test_aggregator_system_runtime_is_explicit(self):
        with temporary_directory() as root:
            aggregator = os.path.join(root, "aggregator.jar")
            real_module = "rapids-4-spark-iceberg-1-10-x_2.13"
            write_aggregator(aggregator, [(real_module, "")])
            properties = {
                RUNTIME_DISCOVERY.SYSTEM_RUNTIME_PROPERTY:
                    "/usr/share/aws/iceberg/lib/iceberg-spark-runtime.jar",
                "spark40x.iceberg.artifact.suffix": "4.0",
            }
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                self.assertEqual([], RUNTIME_DISCOVERY.coordinates(
                    archive, "402", "2.13", lambda name: properties.get(name)))
            finally:
                archive.close()

    def test_empty_system_runtime_is_rejected(self):
        with self.assertRaises(RuntimeError) as raised:
            RUNTIME_DISCOVERY.system_runtime_path(lambda name: "")
        self.assertIn(RUNTIME_DISCOVERY.SYSTEM_RUNTIME_PROPERTY, str(raised.exception))

    def test_aggregator_rejects_system_and_maven_runtimes(self):
        with temporary_directory() as root:
            aggregator = os.path.join(root, "aggregator.jar")
            real_module = "rapids-4-spark-iceberg-1-11-x_2.13"
            write_aggregator(aggregator, [(real_module, RUNTIME_DEPENDENCY)])
            properties = dict(ICEBERG_411_PROPERTIES)
            properties[RUNTIME_DISCOVERY.SYSTEM_RUNTIME_PROPERTY] = \
                "/usr/share/aws/iceberg/lib/iceberg-spark-runtime.jar"
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                with self.assertRaises(RuntimeError) as raised:
                    RUNTIME_DISCOVERY.coordinates(
                        archive, "413", "2.13", lambda name: properties.get(name))
                self.assertIn("1 runtime dependencies", str(raised.exception))
            finally:
                archive.close()

    def test_real_module_requires_declared_spark_line_artifact_suffix(self):
        with temporary_directory() as root:
            aggregator = os.path.join(root, "aggregator.jar")
            write_aggregator(aggregator, [(
                "rapids-4-spark-iceberg-1-11-x_2.13", RUNTIME_DEPENDENCY)])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                with self.assertRaises(RuntimeError) as raised:
                    RUNTIME_DISCOVERY.coordinates(
                        archive, "420", "2.13",
                        lambda name: {"iceberg.111x.version": "1.11.0"}.get(name))
                self.assertIn("spark42x.iceberg.artifact.suffix", str(raised.exception))
            finally:
                archive.close()

    def test_aggregator_rejects_multiple_runtime_dependencies(self):
        with temporary_directory() as root:
            aggregator = os.path.join(root, "aggregator.jar")
            real_module = "rapids-4-spark-iceberg-1-11-x_2.13"
            write_aggregator(aggregator, [(
                real_module, RUNTIME_DEPENDENCY + RUNTIME_DEPENDENCY)])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                with self.assertRaises(RuntimeError) as raised:
                    RUNTIME_DISCOVERY.coordinates(
                        archive, "413", "2.13",
                        lambda name: ICEBERG_411_PROPERTIES.get(name))
                self.assertIn("2 runtime dependencies", str(raised.exception))
            finally:
                archive.close()

    def test_aggregator_rejects_real_and_stub_modules(self):
        with temporary_directory() as root:
            aggregator = os.path.join(root, "aggregator.jar")
            write_aggregator(aggregator, [
                ("rapids-4-spark-iceberg-1-11-x_2.13", RUNTIME_DEPENDENCY),
                ("rapids-4-spark-iceberg-stub_2.13", ""),
            ])
            archive = zipfile.ZipFile(aggregator, "r")
            try:
                with self.assertRaises(RuntimeError) as raised:
                    RUNTIME_DISCOVERY.coordinates(
                        archive, "413", "2.13",
                        lambda name: ICEBERG_411_PROPERTIES.get(name))
                self.assertIn("both real and stub Iceberg modules", str(raised.exception))
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

    def test_protected_source_public_nested_class_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            nested_class = "org.apache.iceberg.p.BaseTaskWriter$RollingFileWriter"
            write_protected_source_public_nested_class(runtime)
            write_class(
                layout, "spark-shared/org/apache/iceberg/p/Caller.class",
                "org.apache.iceberg.p.Caller", class_references=(nested_class,))
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                Class.forName(nested_class, True, loader).newInstance()

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

    def test_same_package_non_subclass_protected_caller_fails(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime, AccessFlag.PROTECTED)
            write_class(layout, "spark-shared/org/apache/iceberg/p/Caller.class",
                        "org.apache.iceberg.p.Caller",
                        references=(("method", "org.apache.iceberg.p.Base",
                                     "hidden", "()V"),))
            with captured_stream("stderr") as stderr:
                result = LINT.main([layout, runtime])
            self.assertEqual(1, result)
            self.assertIn("protected method requiring same runtime package", stderr.getvalue())

    def test_subclass_protected_this_caller_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_protected_field_runtime(runtime)
            write_protected_field_subclass(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class",
                "org.apache.iceberg.p.Base", 0)
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                child.getMethod("read", []).invoke(child.newInstance(), [])

    def test_subclass_protected_long_putfield_caller_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_protected_wide_runtime(runtime, True)
            write_protected_wide_access(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class", True)
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                child.getMethod("access", [Long.TYPE]).invoke(
                    child.newInstance(), [Long(1)])

    def test_subclass_protected_wide_method_caller_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_protected_wide_runtime(runtime, False)
            write_protected_wide_access(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class", False)
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                child.getMethod("access", [Long.TYPE, Double.TYPE]).invoke(
                    child.newInstance(), [Long(1), Double(2.0)])

    def test_subclass_protected_super_caller_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime, AccessFlag.PROTECTED)
            write_protected_super_caller(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class")
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                child.getMethod("callSuper", []).invoke(child.newInstance(), [])

    def test_subclass_protected_static_caller_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime, AccessFlag.PROTECTED | AccessFlag.STATIC)
            write_protected_static_caller(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class")
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                child.getMethod("callStatic", []).invoke(None, [])

    def test_subclass_protected_super_constructor_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_protected_constructor_base(runtime)
            write_protected_constructor_subclass(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class", False)
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                Class.forName("org.apache.iceberg.p.GpuChild", True, loader)

    def test_subclass_protected_new_base_constructor_fails(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_protected_constructor_base(runtime)
            write_protected_constructor_subclass(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class", True)
            with captured_stream("stderr") as stderr:
                self.assertEqual(1, LINT.main([layout, runtime]))
            self.assertIn("protected method requiring same runtime package", stderr.getvalue())
            with split_loader(runtime, layout) as loader:
                with self.assertRaises(VerifyError):
                    Class.forName("org.apache.iceberg.p.GpuChild", True, loader)

    def test_subclass_protected_base_receiver_fails_verification(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_protected_field_runtime(runtime)
            write_protected_field_subclass(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class",
                "org.apache.iceberg.p.Base", 1)
            with captured_stream("stderr") as stderr:
                self.assertEqual(1, LINT.main([layout, runtime]))
            self.assertIn("protected field requiring same runtime package", stderr.getvalue())

            with split_loader(runtime, layout) as loader:
                with self.assertRaises(VerifyError):
                    Class.forName("org.apache.iceberg.p.GpuChild", True, loader)

    def test_subclass_protected_base_method_receiver_fails_verification(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime, AccessFlag.PROTECTED)
            write_protected_method_subclass(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class",
                "org.apache.iceberg.p.Base", 1)
            with captured_stream("stderr") as stderr:
                self.assertEqual(1, LINT.main([layout, runtime]))
            self.assertIn("protected method requiring same runtime package", stderr.getvalue())
            with split_loader(runtime, layout) as loader:
                with self.assertRaises(VerifyError):
                    Class.forName("org.apache.iceberg.p.GpuChild", True, loader)

    def test_subclass_protected_static_method_handle_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime, AccessFlag.PROTECTED | AccessFlag.STATIC)
            write_method_handle_caller(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class",
                ConstPool.REF_invokeStatic, "org.apache.iceberg.p.Base", "hidden", "()V")
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                child.getMethod("loadHandle", []).invoke(None, [])

    def test_subclass_protected_base_virtual_method_handle_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime, AccessFlag.PROTECTED)
            write_method_handle_caller(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class",
                ConstPool.REF_invokeVirtual, "org.apache.iceberg.p.Base", "hidden", "()V")
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                child.getMethod("loadHandle", []).invoke(None, [])

    def test_subclass_protected_constructor_method_handle_fails(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_protected_constructor_base(runtime)
            write_method_handle_caller(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class",
                ConstPool.REF_newInvokeSpecial, "org.apache.iceberg.p.Base",
                MethodInfo.nameInit, "()V")
            with captured_stream("stderr") as stderr:
                self.assertEqual(1, LINT.main([layout, runtime]))
            self.assertIn("protected method requiring same runtime package", stderr.getvalue())
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                with self.assertRaises(IllegalAccessError):
                    child.getMethod("loadHandle", []).invoke(None, [])

    def test_same_class_protected_constructor_method_handle_passes(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime, AccessFlag.PUBLIC)
            write_method_handle_caller(
                layout, "spark-shared/org/apache/iceberg/p/GpuChild.class",
                ConstPool.REF_newInvokeSpecial, "org.apache.iceberg.p.GpuChild",
                MethodInfo.nameInit, "()V", AccessFlag.PROTECTED)
            with captured_stream("stdout"):
                self.assertEqual(0, LINT.main([layout, runtime]))
            with split_loader(runtime, layout) as loader:
                child = Class.forName("org.apache.iceberg.p.GpuChild", True, loader)
                child.getMethod("loadHandle", []).invoke(None, [])

    def test_invalid_member_operand_fails_closed(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            write_invalid_member_operand(
                layout, "spark-shared/org/apache/iceberg/p/Broken.class")
            with captured_stream("stderr") as stderr:
                self.assertEqual(2, LINT.main([layout, runtime]))
            self.assertIn("incompatible constant-pool operand", stderr.getvalue())

    def test_invalid_method_handle_fails_closed(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            write_invalid_method_handle(
                layout, "spark-shared/org/apache/iceberg/p/Broken.class")
            with captured_stream("stderr") as stderr:
                self.assertEqual(2, LINT.main([layout, runtime]))
            self.assertIn("method handle", stderr.getvalue())

    def test_legacy_interface_method_handle_fails_closed(self):
        with temporary_directory() as root:
            layout = os.path.join(root, "layout")
            runtime = os.path.join(root, "runtime")
            write_runtime(runtime)
            write_invalid_legacy_interface_method_handle(
                layout, "spark-shared/org/apache/iceberg/p/Broken.class")
            with captured_stream("stderr") as stderr:
                self.assertEqual(2, LINT.main([layout, runtime]))
            self.assertIn("method handle", stderr.getvalue())

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
