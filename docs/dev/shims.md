---
layout: page
title: Shim Development
nav_order: 4
parent: Developer Overview
---

# Shim Development

RAPIDS Accelerator For Apache Spark supports multiple feature version lines of
Apache Spark such as 3.3.x, 3.4.x, 3.5.x, 4.x and a number of vendor releases that contain
a mix of patches from different upstream releases. These artifacts are generally
incompatible between each other, at both source code level and even more often
at the binary level. The role of the Shim layer is to hide these issues from the
common code, maximize reuse, and minimize logic duplication.

This is achieved by using a ServiceProvider pattern. All Shims implement the same API,
the suitable Shim implementation is loaded after detecting the current Spark build version
attempting to instantiate our plugin. We use the
[ShimLoader](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin-api/src/main/scala/com/nvidia/spark/rapids/ShimLoader.scala)
class as a tight entry point for interacting with the host Spark runtime.

In the following we provide recipes for typical scenarios addressed by the Shim layer.

## Method signature discrepancies

It's among the easiest issues to resolve. We define a method in SparkShims
trait covering a superset of parameters from all versions and call it

```Scala
SparkShimImpl.methodWithDiscrepancies(p_1, ..., p_n)
```

instead of referencing it directly. Shim implementations (SparkShimImpl) are in charge of dispatching it further
to correct version-dependent methods. Moreover, unlike in the below sections
conflicts between versions are easily avoided by using different package or class names
for conflicting Shim implementations.

## Base Classes/Traits Changes

### Compile-time issues

#### Methods added in new versions

If the base class or trait in the new version just adds new methods on top of previous versions and can be implemented
with default behavior, they can be added directly to the unshimmed class. For methods introduced
in newer versions that do not exist in older versions, removing the override keyword ensures that
these methods are treated as new additions rather than overrides. This allows the same class to work
across different Spark versions.

#### Different parent class signatures

Upstream base classes we derive from might be incompatible in the sense that one version
requires us to implement/override the method `M` whereas the other prohibits it by marking
the base implementation `final`, E.g. `org.apache.spark.sql.catalyst.trees.TreeNode` changes
between Spark 3.3.x and Spark 3.5.x. So instead of deriving from such classes directly we
inject an intermediate trait e.g. `com.nvidia.spark.rapids.shims.ShimExpression` that
has a varying source code depending on the Spark version we compile against to overcome this
issue as you can see e.g., comparing shim implementations across versions:

1. [Shim implementation for 3.3.0](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin/src/main/spark330/scala/com/nvidia/spark/rapids/shims/Spark330PlusShims.scala)
2. [Shim service provider for 3.5.1](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin/src/main/spark351/scala/com/nvidia/spark/rapids/shims/spark351/SparkShimServiceProvider.scala)

The `ShimExpression` and related traits themselves live in a single shared file,
[TreeNode.scala](../../sql-plugin/src/main/scala/com/nvidia/spark/rapids/shims/TreeNode.scala),
which is built as regular unshimmed source.

This resolves compile-time problems, however, now we face the problem at run time.

### Run-time Issues

Plugin has to be able to deterministically load the right class files
for identically named classes depending on the detected
Spark runtime version. This is typically solved by using ASM-based relocation in the bytecode,
however it does not work easily with
[Scala packages](https://contributors.scala-lang.org/t/scala-signature-layout/3327/4)

So instead we resort to the idea of JDK's ParallelWorldClassloader in combination with the fact that
Spark runtime uses mutable classloaders we can alter after detecting the runtime version.
Using JarURLConnection URLs we create a Parallel World of the current version within the jar, e.g.:

Spark 3.3.0's URLs:

```text
jar:file:/home/spark/rapids-4-spark_2.12-26.08.0.jar!/
jar:file:/home/spark/rapids-4-spark_2.12-26.08.0.jar!/spark-shared/
jar:file:/home/spark/rapids-4-spark_2.12-26.08.0.jar!/spark330/
```

Spark 3.5.1's URLs:

```text
jar:file:/home/spark/rapids-4-spark_2.12-26.08.0.jar!/
jar:file:/home/spark/rapids-4-spark_2.12-26.08.0.jar!/spark-shared/
jar:file:/home/spark/rapids-4-spark_2.12-26.08.0.jar!/spark351/
```

### Late Inheritance in Public Classes

Most classes needed by the plugin can be disambiguated using Parallel World locations without
reservations except for documented classes that are exposed to the user that may be loaded before
the Plugin is even instantiated by the Spark runtime. The most important example of such a class
is a configurable ShuffleManager. `ShuffleManager` has also changed in a backwards incompatible
manner over the span of supported Spark versions.

The first issue with such a class, since it's loaded by Spark directly outside our control we
cannot have a single class name for our implementation that would work across versions. This is resolved,
by having the documented facade classes with a shim specifier in their package names.

The second issue that every parent class/trait in the inheritance graph is loaded using the classloader outside
Plugin's control. Therefore, all this bytecode must reside in the conventional jar location, and it must
be bitwise-identical across *all* shims. The only way to keep the source code for shared functionality unduplicated,
(i.e., in `sql-plugin/src/main/scala` as opposed to being duplicated in versioned shim source roots such as `sql-plugin/src/main/spark3*/scala` and `sql-plugin/src/main/spark4*/scala`)
is to delay inheriting `ShuffleManager` until as late as possible, as close as possible to the facade class where we
have to split the source code anyway. Use traits as much as possible for flexibility.

### Late Initialization of Public Classes' Ancestors

The third issue may arise from the fact that the shared logic may transitively reference a class that
for one another reason resides in a Parallel World. Untangling this is tedious and may be unnecessary.
The following approach robustly prevents from running into issues related to that.

We know that at the time such a class is loaded by Spark it's not strictly needed if the Plugin
has not been loaded yet. More accurately, it may not be strictly needed until later when the first
query can be run when the Spark SQL session and its extensions are initialized. It improves the
user experience if the first query is not penalized beyond necessary though. By design, Plugin guarantees
that the classloader is
[set up at load time](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin-api/src/main/scala/com/nvidia/spark/SQLPlugin.scala#L29)
before the DriverPlugin and ExecutorPlugin instances are called the `init` method on.

By making a visible class merely a wrapper of the real implementation where the real implementation
is a `lazy val` we prevent classes from Parallel Worlds to be loaded before they can be, and are
actually required.

For examples see:

1. `class ProxyRapidsShuffleInternalManagerBase`
2. `class ExclusiveModeGpuDiscoveryPlugin`

Note that we currently have to manually code up the delegation methods to the tune of:

```Scala
  def method(x: SomeThing) = realImpl.method(x)
```

This could be automatically generated with a simple tool processing the `scalap` output or Scala macros at
build/compile time. Pull requests are welcome.

## How to externalize an internal class as a compile-time dependency

At some point you may find it necessary to expose an existing
`class/trait/object A` currently residing in a "hidden" parallel world
as a dependency for Maven modules/projects dependencies depending on the `dist`
module artifact `rapids-4-spark_2.12`.

This has two pre-requisites:

1. The .class file with the bytecode is bitwise-identical among the currently
supported Spark versions. To verify this you can inspect the dist jar and check
if the class file is under `spark-shared` jar entry. If this is not the case then
code should be refactored until all discrepancies are shimmed away.
1. The transitive closure of the classes compile-time-referenced by `A` should
have the property above.

JDK ships the `jdeps` tool that can help analyze static dependencies of
a class. Unfortunately, it does not compute the transitive closure (recursive)
at the class granularity. Thus you need additional tools such as
the `[Graphviz tool](https://graphviz.org/)` used here.

To figure out the transitive closure of a class we first need to build
the `dist` module. While iterating on the PR, it should be sufficient
to build against two representative versions of the supported Spark version
range (e.g. 330 and 351):

```bash
./build/buildall --parallel=4  --profile=330,351 --module=dist
```

However, before submitting the PR execute the full build `--profile=noSnapshots`.

Then switch to the parallel-world build dir.

```bash
cd dist/target/parallel-world/
```

Move the current externalized classes (outside the spark3* parallel worlds) to
a dedicated directory, say `public`.

```bash
mv org com ai public/
```

`jdeps` can now treat public classes as a separate archive
and you will see the dependencies of `public` classes. By design `public` classes
should have only edges only to other `public` classes in the dist jar.

Execute `jdeps` against `public`, `spark-shared` and an *exactly one* parallel
world such as `spark330`

```bash
${JAVA_HOME}/bin/jdeps -v \
  -dotoutput /tmp/jdeps330 \
  -regex '(com|org)\..*\.rapids\..*' \
  public spark-shared spark330
```

This will produce three DOT files for each "archive" with directed edges for
a class in the archive to a class either in this or another archive.

Looking at an output file, e.g. `/tmp/jdeps330/spark-shared.dot`,
unfortunately you see that jdeps does not label the source class node but labels
the target class node of an edge. Thus the graph is incorrect as it breaks paths
if a node has both incoming and outgoing edges.

```bash
$ grep 'com.nvidia.spark.rapids.GpuFilterExec\$' spark-shared.dot
   "com.nvidia.spark.rapids.GpuFilterExec$"           -> "com.nvidia.spark.rapids.GpuFilterExec (spark330)";
   "com.nvidia.spark.rapids.GpuOverrides$$anon$204"   -> "com.nvidia.spark.rapids.GpuFilterExec$ (spark-shared)";
```

So first create and `cd` to some other directory `/tmp/jdep330.processed` to massage
the original jdeps output for further analysis.

Decorate source nodes from `<archive>.dot` with the `(<archive>)` label given
that the source nodes are guaranteed to be from the `<archive>`.

```bash
sed 's/"\([^(]*\)"\(\s*->.*;\)/"\1 (public)"\2/' \
  /tmp/jdeps330/public.dot > public.dot
sed 's/"\([^(]*\)"\(\s*->.*;\)/"\1 (spark-shared)"\2/' \
  /tmp/jdeps330/spark-shared.dot > spark-shared.dot
sed 's/"\([^(]*\)"\(\s*->.*;\)/"\1 (spark330)"\2/' \
  /tmp/jdeps330/spark330.dot > spark330.dot
```

Next you need to union edges of all three graphs into a single graph to be able
to analyze cross-archive paths.

```bash
cat public.dot spark-shared.dot spark330.dot | \
  tr '\n' '\r' | \
  sed 's/}\rdigraph "[^"]*" {\r//g' | \
  tr '\r' '\n' > merged.dot
```

Now you can finally examine the classes reachable from the class you are trying
to externalize. Using the `dijkstra` tool you can annotate all nodes with the
shortest path distance from the input node, the class you are trying to externalize

If you see a reachable node out of the parallel world `spark330` then you need
to refactor until the dependencies paths to it are eliminated.

Focus on the nodes with lowest distance to eliminate dependency on the shim.

### Examples

GpuTypeColumnVector needs refactoring prior externalization as of the time
of this writing:

```bash
$ dijkstra -d -p "com.nvidia.spark.rapids.GpuColumnVector (spark-shared)" merged.dot | \
  grep '\[dist=' | grep '(spark330)'
        "org.apache.spark.sql.rapids.GpuFileSourceScanExec (spark330)"  [dist=5.000,
        "com.nvidia.spark.rapids.GpuExec (spark330)"    [dist=3.000,
...
```

RegexReplace could be externalized safely:

```bash
$ dijkstra -d -p "org.apache.spark.sql.rapids.RegexReplace (spark-shared)"  merged.dot | grep '\[dist='
        "org.apache.spark.sql.rapids.RegexReplace (spark-shared)"    [dist=0.000];
        "org.apache.spark.sql.rapids.RegexReplace$ (spark-shared)"   [dist=1.000,
```

because it is self-contained.

### Estimating the scope of the task

Dealing with a single class at a time may quickly turn into a tedious task.
You can look at the bigger picture by generating clusters of the strongly
connected components using `sccmap`

```bash
$ sccmap -d -s merged.dot
2440 nodes, 11897 edges, 637 strong components  # example output; actual numbers vary
```

Review the clusters in the output of `sccmap -d merged.dot`. Find the cluster containing
your class and how it is connected to the rest of the clusters in the definition of the digraph
`scc_map`.

This mechanism can also be used as a guidance for refactoring the code in a more self-contained
packages.
