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

To extract a bundled native library, specify its name and destination path:

```shell
java -jar <dist.jar> extract <library-name> <destination>
```

Running the JAR without arguments lists the native library names available for the current
operating system and architecture. Run `java -jar <dist.jar> help` to show all commands.
