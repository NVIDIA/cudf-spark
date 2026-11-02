---
layout: page
title: Inspect a cuDF Plugin Distribution JAR
parent: Additional Functionality
nav_order: 9
---

# Inspect a cuDF Plugin Distribution JAR

The cuDF plugin distribution JAR is executable without starting Spark. Run it with no
arguments to report build information for the plugin and its internal cuDF dependencies,
the packaged Spark versions, and the bundled native libraries:

```shell
java -jar <dist.jar>
```

The report includes the source revisions and build dates when they are available, along
with the Scala library version and Java bytecode target used to build the plugin.

To compare the binary payloads of two distribution JARs, compute their canonical payload
checksums:

```shell
java -jar <dist.jar> checksum
```

The checksum ignores ZIP ordering, compression, timestamps, signatures, manifests, Maven
metadata, and build-information properties. It therefore remains stable when only artifact
coordinates, product names, or other publication metadata change.

To extract a bundled native library, put the distribution JAR and `zstd-jni` on the
classpath, then specify the library name and destination path. Spark supplies
`zstd-jni` when the plugin runs inside Spark; standalone extraction needs the
dependency JAR explicitly. Running
`java -jar <dist.jar> extract <library-name> <destination>` prints the command
to use.

```shell
java -cp "<dist.jar>:<zstd-jni.jar>" com.nvidia.spark.rapids.SparkRapidsJarTool \
  extract <library-name> <destination>
```

On Windows, use `;` instead of `:` as the classpath separator.

Running the JAR without arguments lists the native library names and architectures packaged
for the current operating system. Extraction uses the host architecture when available, or
the sole packaged architecture otherwise. If multiple non-host architectures contain the
requested library, extraction fails rather than choosing one arbitrarily. Run
`java -jar <dist.jar> help` to show all commands.
