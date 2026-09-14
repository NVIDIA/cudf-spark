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

"""Compile deprecation-policy fixtures using Jython 2.7-compatible code."""

from __future__ import print_function

import argparse
import io
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ElementTree


MAVEN_NAMESPACE = "{http://maven.apache.org/POM/4.0.0}"
try:
    TEXT_TYPE = unicode
except NameError:  # Python 3
    TEXT_TYPE = str


class CommandResult(object):
    def __init__(self, returncode, stdout):
        self.returncode = returncode
        self.stdout = stdout


def maven_tag(name):
    return MAVEN_NAMESPACE + name


def find_executable(name):
    extensions = [""]
    if os.name == "nt":
        extensions.extend(os.environ.get("PATHEXT", ".EXE").split(os.pathsep))
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        for extension in extensions:
            candidate = os.path.join(directory, name + extension)
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
    return None


def compiler_configuration(pom_path):
    root = ElementTree.parse(pom_path).getroot()
    scala_version = root.findtext(
        "{0}/{1}".format(maven_tag("properties"), maven_tag("scala.version")))
    if not scala_version:
        raise RuntimeError("Could not find scala.version in {0}".format(pom_path))
    plugin_paths = (
        "{0}/{1}/{2}".format(
            maven_tag("build"), maven_tag("plugins"), maven_tag("plugin")),
        "{0}/{1}/{2}/{3}".format(
            maven_tag("build"), maven_tag("pluginManagement"),
            maven_tag("plugins"), maven_tag("plugin")),
    )
    plugins = (
        plugin
        for plugin_path in plugin_paths
        for plugin in root.findall(plugin_path)
    )
    for plugin in plugins:
        artifact_id = plugin.findtext(maven_tag("artifactId"))
        if artifact_id == "scala-maven-plugin":
            args = [
                argument.text
                for argument in plugin.findall("{0}/{1}/{2}".format(
                    maven_tag("configuration"), maven_tag("args"), maven_tag("arg")))
                if argument.text
            ]
            if not args:
                raise RuntimeError(
                    "scala-maven-plugin has no compiler arguments in {0}".format(pom_path))
            return scala_version, args
    raise RuntimeError("Could not find scala-maven-plugin in {0}".format(pom_path))


def scala_compiler_classpath(maven_repo, scala_version):
    scala_root = os.path.join(maven_repo, "org", "scala-lang")
    jars = [
        os.path.join(
            scala_root, artifact, scala_version,
            "{0}-{1}.jar".format(artifact, scala_version))
        for artifact in ("scala-compiler", "scala-library", "scala-reflect")
    ]
    missing = [jar for jar in jars if not os.path.isfile(jar)]
    if missing:
        raise RuntimeError("Missing Scala compiler dependencies: " + ", ".join(missing))
    return jars


def run_command(command):
    process = subprocess.Popen(
        command, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, _ = process.communicate()
    return CommandResult(process.returncode, stdout)


def write_fixtures(root):
    sources = {
        "ai/rapids/cudf/fixture/NvidiaApi.java": """
package ai.rapids.cudf.fixture;
public final class NvidiaApi {
  private NvidiaApi() {}
  @Deprecated public static void oldApi() {}
}
""",
        "org/example/fixture/ThirdPartyApi.java": """
package org.example.fixture;
public final class ThirdPartyApi {
  private ThirdPartyApi() {}
  @Deprecated public static void oldApi() {}
}
""",
        "com/nvidia/spark/rapids/jni/fixture/JniApi.java": """
package com.nvidia.spark.rapids.jni.fixture;
public final class JniApi {
  private JniApi() {}
  @Deprecated public static void oldApi() {}
}
""",
        "com/nvidia/spark/rapids/optimizer/fixture/PrivateApi.java": """
package com.nvidia.spark.rapids.optimizer.fixture;
public final class PrivateApi {
  private PrivateApi() {}
  @Deprecated public static void oldApi() {}
}
""",
        "org/apache/spark/sql/rapids/internal/fixture/PrivateApi.java": """
package org.apache.spark.sql.rapids.internal.fixture;
public final class PrivateApi {
  private PrivateApi() {}
  @Deprecated public static void oldApi() {}
}
""",
        "org/apache/spark/sql/execution/aggregate/PartialAggUtils.java": """
package org.apache.spark.sql.execution.aggregate;
public final class PartialAggUtils {
  private PartialAggUtils() {}
  @Deprecated public static void oldApi() {}
}
""",
        "org/apache/spark/sql/execution/aggregate/PartialAggUtilsNeighbor.java": """
package org.apache.spark.sql.execution.aggregate;
public final class PartialAggUtilsNeighbor {
  private PartialAggUtilsNeighbor() {}
  @Deprecated public static void oldApi() {}
}
""",
        "org/apache/spark/sql/execution/aggregate/SparkApi.java": """
package org.apache.spark.sql.execution.aggregate;
public final class SparkApi {
  private SparkApi() {}
  @Deprecated public static void oldApi() {}
}
""",
        "NvidiaCall.scala": """
object NvidiaCall {
  def call(): Unit = ai.rapids.cudf.fixture.NvidiaApi.oldApi()
}
""",
        "JniCall.scala": """
object JniCall {
  def call(): Unit = com.nvidia.spark.rapids.jni.fixture.JniApi.oldApi()
}
""",
        "PrivateComNvidiaCall.scala": """
object PrivateComNvidiaCall {
  def call(): Unit = com.nvidia.spark.rapids.optimizer.fixture.PrivateApi.oldApi()
}
""",
        "PrivateRapidsCall.scala": """
object PrivateRapidsCall {
  def call(): Unit = org.apache.spark.sql.rapids.internal.fixture.PrivateApi.oldApi()
}
""",
        "PrivateSparkBridgeCall.scala": """
object PrivateSparkBridgeCall {
  def call(): Unit = org.apache.spark.sql.execution.aggregate.PartialAggUtils.oldApi()
}
""",
        "PartialAggUtilsNeighborCall.scala": """
object PartialAggUtilsNeighborCall {
  def call(): Unit = org.apache.spark.sql.execution.aggregate.PartialAggUtilsNeighbor.oldApi()
}
""",
        "SparkCall.scala": """
object SparkCall {
  def call(): Unit = org.apache.spark.sql.execution.aggregate.SparkApi.oldApi()
}
""",
        "ThirdPartyCall.scala": """
object ThirdPartyCall {
  def call(): Unit = org.example.fixture.ThirdPartyApi.oldApi()
}
""",
    }
    for relative_path, source in sources.items():
        path = os.path.join(root, relative_path)
        parent = os.path.dirname(path)
        if not os.path.isdir(parent):
            os.makedirs(parent)
        with io.open(path, "w", encoding="utf-8") as source_file:
            source_file.write(TEXT_TYPE(source.lstrip()))


def check_policy(pom_path, maven_repo):
    scala_version, compiler_args = compiler_configuration(pom_path)
    compiler_jar, library_jar, reflect_jar = scala_compiler_classpath(
        maven_repo, scala_version)
    javac = find_executable("javac")
    java = find_executable("java")
    if not javac or not java:
        raise RuntimeError("Both java and javac are required for the deprecation policy check")

    temp_dir = tempfile.mkdtemp(prefix="cudf-spark-deprecation-policy-")
    try:
        fixture_root = temp_dir
        classes = os.path.join(fixture_root, "classes")
        os.mkdir(classes)
        write_fixtures(fixture_root)
        java_compile = run_command([
            javac, "-d", classes,
            os.path.join(fixture_root, "ai/rapids/cudf/fixture/NvidiaApi.java"),
            os.path.join(fixture_root, "com/nvidia/spark/rapids/jni/fixture/JniApi.java"),
            os.path.join(
                fixture_root, "com/nvidia/spark/rapids/optimizer/fixture/PrivateApi.java"),
            os.path.join(
                fixture_root, "org/apache/spark/sql/rapids/internal/fixture/PrivateApi.java"),
            os.path.join(
                fixture_root,
                "org/apache/spark/sql/execution/aggregate/PartialAggUtils.java"),
            os.path.join(
                fixture_root,
                "org/apache/spark/sql/execution/aggregate/PartialAggUtilsNeighbor.java"),
            os.path.join(
                fixture_root, "org/apache/spark/sql/execution/aggregate/SparkApi.java"),
            os.path.join(fixture_root, "org/example/fixture/ThirdPartyApi.java"),
        ])
        if java_compile.returncode:
            raise RuntimeError("Could not compile Java fixtures:\n" + java_compile.stdout)

        compiler_classpath = os.pathsep.join((compiler_jar, library_jar, reflect_jar))
        source_classpath = os.pathsep.join((classes, library_jar))

        def compile_scala(source):
            command = [
                java, "-cp", compiler_classpath, "scala.tools.nsc.Main",
                "-classpath", source_classpath, "-d", classes,
            ]
            command.extend(compiler_args)
            command.append(os.path.join(fixture_root, source))
            return run_command(command)

        nvidia_sources = (
            ("cuDF Java", "NvidiaCall.scala"),
            ("cudf-spark-jni", "JniCall.scala"),
            ("cudf-spark-private com.nvidia namespace", "PrivateComNvidiaCall.scala"),
            ("cudf-spark-private RAPIDS namespace", "PrivateRapidsCall.scala"),
            ("cudf-spark-private Spark-package bridge", "PrivateSparkBridgeCall.scala"),
        )
        for api_name, source in nvidia_sources:
            nvidia_compile = compile_scala(source)
            if nvidia_compile.returncode or "deprecated" not in nvidia_compile.stdout.lower():
                raise RuntimeError(
                    "{0} deprecation must be visible and nonfatal, ".format(api_name) +
                    "but compilation produced:\n" + nvidia_compile.stdout)

        fatal_sources = (
            ("Third-party", "ThirdPartyCall.scala"),
            ("Apache Spark sibling", "SparkCall.scala"),
            ("PartialAggUtils prefix neighbor", "PartialAggUtilsNeighborCall.scala"),
        )
        for api_name, source in fatal_sources:
            fatal_compile = compile_scala(source)
            if fatal_compile.returncode == 0 or "deprecated" not in fatal_compile.stdout.lower():
                raise RuntimeError(
                    "{0} deprecation must be visible and fatal, ".format(api_name) +
                    "but compilation produced:\n" + fatal_compile.stdout)
    finally:
        shutil.rmtree(temp_dir)

    print("Deprecation policy check passed for Scala {0}".format(scala_version))


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pom", required=True)
    parser.add_argument("--maven-repo", required=True)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        check_policy(args.pom, args.maven_repo)
    except RuntimeError as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
