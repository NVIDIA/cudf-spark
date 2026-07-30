---
layout: page
title: NVIDIA cuDF plugin for Apache Spark Distribution Packaging
nav_order: 1
parent: Developer Overview
---
# NVIDIA cuDF plugin for Apache Spark Distribution Packaging

The distribution module creates a jar with support for the Spark versions you need combined into a single jar.

See the [CONTRIBUTING.md](../CONTRIBUTING.md) doc for details on building and profiles available to build an uber jar.

Note that when you use the profiles to build an uber jar there are currently some hardcoded service provider files that get put into place. One file for each of the
above profiles. Please note that you will need to update these if adding or removing support for a Spark version.

Files are: `com.nvidia.spark.rapids.SparkShimServiceProvider.sparkNonSnapshot`, `com.nvidia.spark.rapids.SparkShimServiceProvider.sparkSnapshot`, `com.nvidia.spark.rapids.SparkShimServiceProvider.sparkNonSnapshotDB`, and `com.nvidia.spark.rapids.SparkShimServiceProvider.sparkSnapshotDB`.

The new uber jar is structured like:

1. Base common classes are user visible classes. For these we use Spark 3.3.0 versions because they are assumed to be
bitwise-identical to the other shims, this assumption is subject to the future automatic validation.
2. META-INF/services. This is a file that has to list all the shim versions supported by this jar. 
The files talked about above for each profile are put into place here for uber jars. Although we currently do not use 
[ServiceLoader API](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) we use the same service 
provider discovery mechanism
3. META-INF base files are from 3.3.0  - maven, LICENSE, NOTICE, etc
4. Spark specific directory (aka Parallel World in the jargon of 
[ParallelWorldClassloader](https://github.com/openjdk/jdk/blob/jdk8-b120/jaxws/src/share/jaxws_classes/com/sun/istack/internal/tools/ParallelWorldClassLoader.java)) 
for each version of Spark supported in the jar, i.e., spark330/, spark341/, etc.

If you have to change the contents of the uber jar the following files control what goes into the base jar as classes that are not shaded.

1. `unshimmed-common-from-single-shim.txt` - This has classes and files that should go into the base jar with their normal
package name (not shaded). This includes user visible classes (i.e., com/nvidia/spark/SQLPlugin), python files,
and other files that aren't version specific. Uses Spark 3.2.0 built jar for these base classes as explained above.
2. `unshimmed-from-each-spark3xx.txt` - This is applied to all the individual Spark specific version jars to pull
any files that need to go into the base of the jar and not into the Spark specific directory.
