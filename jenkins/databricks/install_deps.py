# Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import argparse
import glob
import sys


class Artifact:

    def __init__(self, group_id, artifact_id, filename):
        self.group_id = group_id
        self.artifact_id = artifact_id
        self.filename = filename

    def __repr__(self):
        return f'{self.group_id} {self.artifact_id} {self.filename}'


def define_deps(spark_version, scala_version):
    hadoop_version = "3.2"
    hive_version = "2.3"
    spark_prefix = '----workspace'
    mvn_prefix = '--maven-trees'

    if spark_version.startswith('3.2'):
        spark_prefix = '----workspace_spark_3_2'
    elif spark_version.startswith('3.3'):
        spark_prefix = '----ws_3_3'
        mvn_prefix = '--mvn'
    elif spark_version.startswith('3.4'):
        spark_prefix = '----ws_3_4'
        mvn_prefix = '--mvn'
    elif spark_version.startswith('3.5'):
        spark_prefix = '----ws_3_5'
        mvn_prefix = '--mvn'
    elif spark_version.startswith('4.0'):
        spark_prefix = '----ws_4_0'
        mvn_prefix = '--mvn'
    else:
        raise Exception(f"Unsupported Databricks version {spark.version}")

    spark_suffix = f'hive-{hive_version}__hadoop-{hadoop_version}_{scala_version}'

    if spark_version.startswith('3.2'):
        prefix_ws_sp_mvn_hadoop = f'{spark_prefix}{mvn_prefix}--hive-{hive_version}__hadoop-{hadoop_version}'
    else:
        prefix_ws_sp_mvn_hadoop = f'{spark_prefix}{mvn_prefix}--hadoop3'

    deps = [
        # Spark
        Artifact('org.apache.spark', f'spark-network-common_{scala_version}',
            f'{spark_prefix}--common--network-common--network-common-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-network-shuffle_{scala_version}',
            f'{spark_prefix}--common--network-shuffle--network-shuffle-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-unsafe_{scala_version}',
            f'{spark_prefix}--common--unsafe--unsafe-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-sketch_{scala_version}',
            f'{spark_prefix}--common--sketch--sketch-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-launcher_{scala_version}',
                 f'{spark_prefix}--launcher--launcher-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-sql_{scala_version}',
                 f'{spark_prefix}--sql--core--core-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-catalyst_{scala_version}',
                 f'{spark_prefix}--sql--catalyst--catalyst-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-annotation_{scala_version}',
                 f'{spark_prefix}--common--tags--tags-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-core_{scala_version}',
                 f'{spark_prefix}--core--core-{spark_suffix}_deploy.jar'),
        Artifact('org.apache.spark', f'spark-versions_{scala_version}',
                 f'spark--versions--*--shim*_{scala_version}_deploy.jar'),
        Artifact('org.apache.spark', f'databricks-versions_{scala_version}',
                 f'common--build-info--build-info-spark_*_{scala_version}_deploy.jar'),
        # Spark Hive Patches
        Artifact('org.apache.spark', f'spark-hive_{scala_version}',
                         f'{spark_prefix}--sql--hive--hive-{spark_suffix}_*.jar'),
        Artifact('org.apache.hive', 'hive-exec',
                         f'{spark_prefix}--patched-hive-with-glue--hive-exec*.jar'),
    ]
    # hive-metastore-client-patched has different naming pattern in Spark 4.0
    if spark_version.startswith('4.0'):
        # Spark 4.0: the patch jar uses scala 2.13
        deps += [Artifact('org.apache.hive', 'hive-metastore-client-patched',
                 f'{spark_prefix}--patched-hive-with-glue--hive-*-patch-hive-2.3__hadoop-3.2_2.*.jar')]
    else:
        # Spark 3.x versions
        deps += [Artifact('org.apache.hive', 'hive-metastore-client-patched',
                 f'{spark_prefix}--patched-hive-with-glue--hive-*-patch-{spark_suffix}_deploy.jar')]
    deps += [
        # Hive
        Artifact('org.apache.hive', 'hive-serde',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.hive--hive-serde--org.apache.hive__hive-serde__*.jar'),
        Artifact('org.apache.hive', 'hive-storage-api',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.hive--hive-storage-api--org.apache.hive__hive-storage-api__*.jar'),

        # Orc
        Artifact('org.apache.orc', 'orc-core',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.orc--orc-core--org.apache.orc__orc-core__*.jar'),
        Artifact('org.apache.orc', 'orc-shims',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.orc--orc-shims--org.apache.orc__orc-shims__*.jar'),
        Artifact('org.apache.orc', 'orc-mapreduce',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.orc--orc-mapreduce--org.apache.orc__orc-mapreduce__*.jar'),

        # Arrow
        Artifact('org.apache.arrow', 'arrow-format',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.arrow--arrow-format--org.apache.arrow__arrow-format__*.jar'),
        Artifact('org.apache.arrow', 'arrow-memory',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.arrow--arrow-memory-core--org.apache.arrow__arrow-memory-core__*.jar'),
        Artifact('org.apache.arrow', 'arrow-vector',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.arrow--arrow-vector--org.apache.arrow__arrow-vector__*.jar'),

        Artifact('com.google.protobuf', 'protobuf-java',
                 f'{prefix_ws_sp_mvn_hadoop}--com.google.protobuf--protobuf-java--com.google.protobuf__protobuf-java__*.jar'),
        Artifact('com.esotericsoftware.kryo', f'kryo-shaded-db',
                 f'{prefix_ws_sp_mvn_hadoop}--com.esotericsoftware--kryo-shaded--com.esotericsoftware__kryo-shaded__*.jar'),
        Artifact('org.apache.commons', 'commons-io',
                 f'{prefix_ws_sp_mvn_hadoop}--commons-io--commons-io--commons-io__commons-io__*.jar'),
        Artifact('org.apache.commons', 'commons-lang3',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.commons--commons-lang3--org.apache.commons__commons-lang3__*.jar'),
        Artifact('org.json4s', f'json4s-ast_{scala_version}',
                 f'{prefix_ws_sp_mvn_hadoop}--org.json4s--json4s-ast_{scala_version}--org.json4s__json4s-ast_{scala_version}__*.jar'),
        Artifact('org.json4s', f'json4s-core_{scala_version}',
                 f'{prefix_ws_sp_mvn_hadoop}--org.json4s--json4s-core_{scala_version}--org.json4s__json4s-core_{scala_version}__*.jar'),
        Artifact('org.json4s', f'json4s-jackson_{scala_version}',
                 f'{prefix_ws_sp_mvn_hadoop}--org.json4s--json4s-jackson_{scala_version}--org.json4s__json4s-jackson_{scala_version}__*.jar'),
        Artifact('org.javaassist', 'javaassist',
                 f'{prefix_ws_sp_mvn_hadoop}--org.javassist--javassist--org.javassist__javassist__*.jar'),
        Artifact('com.fasterxml.jackson.core', 'jackson-databind',
                 f'{prefix_ws_sp_mvn_hadoop}--com.fasterxml.jackson.core--jackson-databind--com.fasterxml.jackson.core__jackson-databind__*.jar'),
        Artifact('com.fasterxml.jackson.core', 'jackson-core',
                 f'{prefix_ws_sp_mvn_hadoop}--com.fasterxml.jackson.core--jackson-core--com.fasterxml.jackson.core__jackson-core__*.jar'),
        Artifact('com.fasterxml.jackson.core', 'jackson-annotations',
                 f'{prefix_ws_sp_mvn_hadoop}--com.fasterxml.jackson.core--jackson-annotations--com.fasterxml.jackson.core__jackson-annotations__*.jar'),
        Artifact('org.apache.avro', 'avro-mapred',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.avro--avro-mapred--org.apache.avro__avro-mapred__*.jar'),
        Artifact('org.apache.avro', 'avro',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.avro--avro--org.apache.avro__avro__*.jar'),
        Artifact('com.github.luben', 'zstd-jni',
                 f'{prefix_ws_sp_mvn_hadoop}--com.github.luben--zstd-jni--com.github.luben__zstd-jni__*.jar'),

        # Netty dependencies for MultithreadedShuffleBufferCatalog
        Artifact('io.netty', 'netty-common',
                 f'{prefix_ws_sp_mvn_hadoop}--io.netty--netty-common--io.netty__netty-common__*.jar'),
        Artifact('io.netty', 'netty-transport',
                 f'{prefix_ws_sp_mvn_hadoop}--io.netty--netty-transport--io.netty__netty-transport__*.jar'),
        Artifact('io.netty', 'netty-handler',
                 f'{prefix_ws_sp_mvn_hadoop}--io.netty--netty-handler--io.netty__netty-handler__*.jar'),
    ]

    # spark-avro has different locations depending on Spark version
    if spark_version.startswith('4.0'):
        deps += [Artifact('org.apache.spark', f'spark-avro_{scala_version}',
                 f'{spark_prefix}--connector--avro--avro-{spark_suffix}_*.jar')]
    elif spark_version.startswith('3.5'):
        deps += [Artifact('org.apache.spark', f'spark-avro_{scala_version}',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.avro--avro--org.apache.avro*.jar')]
    else:
        deps += [Artifact('org.apache.spark', f'spark-avro_{scala_version}',
                 f'{spark_prefix}--vendor--avro--avro-*.jar')]

    # Parquet
    if spark_version.startswith('3.4') or spark_version.startswith('3.5') or spark_version.startswith('4.0'):
        deps += [
        Artifact('org.apache.parquet', 'parquet-hadoop',
             f'{spark_prefix}--third_party--parquet-mr--parquet-hadoop--parquet-hadoop-shaded--*--libparquet-hadoop-internal.jar'),
        Artifact('org.apache.parquet', 'parquet-common',
             f'{spark_prefix}--third_party--parquet-mr--parquet-common--parquet-common-shaded--*--libparquet-common-internal.jar'),
        Artifact('org.apache.parquet', 'parquet-column',
             f'{spark_prefix}--third_party--parquet-mr--parquet-column--parquet-column-shaded--*--libparquet-column-internal.jar'),
        Artifact('org.apache.parquet', 'parquet-format',
             f'{spark_prefix}--third_party--parquet-mr--parquet-format-structures--parquet-format-structures-shaded--*--libparquet-format-structures-internal.jar'),
        Artifact('shaded.parquet.org.apache.thrift', f'shaded-parquet-thrift_{scala_version}',
            f'{spark_prefix}--third_party--parquet-mr--parquet-format-structures--parquet-format-structures-shaded--*--org.apache.thrift__libthrift__*.jar'),
        Artifact('org.apache.parquet', f'parquet-format-internal_{scala_version}',
            f'{spark_prefix}--third_party--parquet-mr--parquet-format-structures--parquet-format-structures-shaded--*--libparquet-thrift.jar')
        ]
    else:
        deps += [
        Artifact('org.apache.parquet', 'parquet-hadoop',
             f'{prefix_ws_sp_mvn_hadoop}--org.apache.parquet--parquet-hadoop--org.apache.parquet__parquet-hadoop__*-databricks*.jar'),
        Artifact('org.apache.parquet', 'parquet-common',
             f'{prefix_ws_sp_mvn_hadoop}--org.apache.parquet--parquet-common--org.apache.parquet__parquet-common__*-databricks*.jar'),
        Artifact('org.apache.parquet', 'parquet-column',
             f'{prefix_ws_sp_mvn_hadoop}--org.apache.parquet--parquet-column--org.apache.parquet__parquet-column__*-databricks*.jar'),
        Artifact('org.apache.parquet', 'parquet-format',
             f'{prefix_ws_sp_mvn_hadoop}--org.apache.parquet--parquet-format-structures--org.apache.parquet__parquet-format-structures__*-databricks*.jar')
        ]


    # log4j-core
    if spark_version.startswith('3.3') or spark_version.startswith('3.4') or spark_version.startswith('3.5') or spark_version.startswith('4.0'):
        deps += Artifact('org.apache.logging.log4j', 'log4j-core',
                         f'{prefix_ws_sp_mvn_hadoop}--org.apache.logging.log4j--log4j-core--org.apache.logging.log4j__log4j-core__*.jar'),

    # Scala parser
    deps += [
        Artifact('org.scala-lang.modules', f'scala-parser-combinators_{scala_version}',
                 f'{prefix_ws_sp_mvn_hadoop}--org.scala-lang.modules--scala-parser-combinators_{scala_version}-*.jar')
    ]

    if spark_version.startswith('3.4') or spark_version.startswith('3.5') or spark_version.startswith('4.0'):
        deps += [
        # Spark Internal Logging
        Artifact('org.apache.spark', f'spark-common-utils_{scala_version}', f'{spark_prefix}--common--utils--common-utils-hive-2.3__hadoop-3.2_{scala_version}_deploy.jar'),
        # Spark SQL API
        Artifact('org.apache.spark', f'spark-sql-api_{scala_version}', f'{spark_prefix}--sql--api--sql-api-hive-2.3__hadoop-3.2_{scala_version}_deploy.jar')
        ]

    if spark_version.startswith('3.5') or spark_version.startswith('4.0'):
        deps += [
        Artifact('org.scala-lang.modules', f'scala-collection-compat_{scala_version}',
             f'{prefix_ws_sp_mvn_hadoop}--org.scala-lang.modules--scala-collection-compat_{scala_version}--org.scala-lang.modules__scala-collection-compat_{scala_version}__2.11.0.jar'), 
        Artifact('org.apache.avro', f'avro-connector', f'{spark_prefix}--connector--avro--avro-hive-2.3__hadoop-3.2_{scala_version}_shaded--*--avro-unshaded-hive-2.3__hadoop-3.2_{scala_version}_deploy.jar')
        ]

    # Databricks 17.3 (Spark 4.0) requires additional common-utils JARs
    if spark_version.startswith('4.0'):
        deps += [
        # Logging class is in common-utils-other
        Artifact('org.apache.spark', f'spark-common-utils-other_{scala_version}', f'{spark_prefix}--common--utils--common-utils-other-hive-2.3__hadoop-3.2_{scala_version}_deploy.jar'),
        # ConfigEntry and related config classes are in common-config
        Artifact('org.apache.spark', f'spark-common-config_{scala_version}', f'{spark_prefix}--common--utils--common-config-hive-2.3__hadoop-3.2_{scala_version}_deploy.jar'),
        Artifact('org.apache.orc', 'orc-format',
                 f'{prefix_ws_sp_mvn_hadoop}--org.apache.orc--orc-format--org.apache.orc__orc-format__*-shaded-protobuf.jar')
        ]

    return deps

def install_deps(deps, spark_version_to_install_databricks_jars, m2_dir, jar_dir, file):
    pom_xml_header = """<?xml version="1.0" encoding="UTF-8"?>
        <project xmlns="http://maven.apache.org/POM/4.0.0"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
            <modelVersion>4.0.0</modelVersion>
            <groupId>com.nvidia</groupId>
            <artifactId>rapids-4-spark-databricks-deps-installer</artifactId>
            <description>bulk databricks deps installer</description>
            <version>${SPARK_PLUGIN_JAR_VERSION}</version>
            <packaging>pom</packaging>
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-install-plugin</artifactId>
                        <version>2.4</version>
                        <executions>
        """
    print(pom_xml_header, file=file)

    i = 0
    for artifact in deps:
        print(f'Generating an execution for {artifact}', file=sys.stderr)

        group_id = artifact.group_id
        artifact_id = artifact.artifact_id
        filename = artifact.filename

        files = glob.glob(f'{jar_dir}/{filename}')
        if len(files) == 0:
            raise Exception("No jar found that matches pattern {}".format(filename))
        elif len(files) > 1:
            raise Exception("Ambiguous filename pattern {} matches multiple files: {}".format(filename, files))

        jar = files[0]

        key = str(i)
        i = i + 1

        pom_xml_dep = f"""
            <execution>
                <id>install-db-jar-{key}</id>
                <phase>initialize</phase>
                <goals><goal>install-file</goal></goals>
                <configuration>
                    <localRepositoryPath>{m2_dir}</localRepositoryPath>
                    <file>{jar}</file>
                    <groupId>{group_id}</groupId>
                    <artifactId>{artifact_id}</artifactId>
                    <version>{spark_version_to_install_databricks_jars}</version>
                    <packaging>jar</packaging>
                </configuration>
            </execution>"""
        print(pom_xml_dep, file=file)

    pom_xml_footer = """
                        </executions>
                    </plugin>
                </plugins>
            </build>
        </project>
        """
    print(pom_xml_footer, file=file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('spark_version')
    parser.add_argument('spark_version_to_install_databricks_jars')
    parser.add_argument('scala_version')
    parser.add_argument('m2_dir')
    parser.add_argument('jar_dir')
    parser.add_argument('pom_filename')
    args = parser.parse_args()
    deps = define_deps(args.spark_version, args.scala_version)
    with open(args.pom_filename, "w") as f:
        install_deps(deps, args.spark_version_to_install_databricks_jars, args.m2_dir, args.jar_dir, f)
