---
layout: page
title: Shim Source Code Layout Simplification with Shimplify
nav_order: 8
parent: Developer Overview
---

# Shim Source Code Layout Simplification with Shimplify

This document describes the next iteration of shim source code maintenance. It addresses the
drawback introduced with the [shim layer rework][1] resulting in the guaranteed ABI-compatible
bytecode management for each of the 14 currently supported Spark builds but at the expense of
maintaining 50+ directories. Many shims are spread over an overlapping set of directories making it
hard to determine where to make additions while keeping code duplication in check.

[shimplify.py][2] is the new goal in the Maven build binding to the `generate-sources` phase.

* It defines a new simpler shim directory structure where there is only a single directory per shim,
and a special comment is injected to define metadata defining all the shim builds it participates in.
* It can convert all or a subset of existing shims to the new build. The build can support partially
converted shims if a longer transition is desired.

## Simplified Shim Source Directory Structure

In our build each supported Apache Spark build and its corresponding shim is identified by its
[`buildver`][3] property. Every Maven submodule requiring shimming (`sql-plugin`, `tests` as of the
time of this writing) have a new set of special sibling directories
`src/(main|test)/spark${buildver}`.

Previous `src/(main|test)/${buildver}` and
version-range-with-exceptions directories such as `src/main/320until340-non330db` are deprecated and
are being removed as a result of the conversion to the new structure.

`shimplify` changes the way the source code is shared among shims by using an explicit
lexicographically sorted list of `buildver` property values
in a source-code level comment instead of the shared directories.

```scala
/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
spark-rapids-shim-json-lines ***/
```

The content inside the tags `spark-rapids-shim-json-lines` is in the [JSON Lines][4] format where
each line is an extensible object with the shim metadata currently consisting just of the Spark
build dependency version. The top object in the comment, the minimum version in the comment
intuitively represents the first version of Spark requiring shimming in the plugin, albeit it might
not be the original one as support for older Spark releases is eventually dropped. This `buildver`
is called the *owner shim*.

On the default read-only invocation path of the Maven build shimplify does not make any changes to
shim source code files and their locations.

* It analyzes the pre-shimplify directory structure and identifies the shims that through the code
evolution ended up using more dedicated directories than necessary contributing avoidable
complexity on top of an inherently complex directory structure already.

* For the shimplify directory structure all files under `src/(main|test)/spark*` directories
are read to parse the `spark-rapids-shim-json-lines` comments. It performs the following
validations:

  * It makes sure that the comment is present and can be parsed
  * The list of shims is non-empty (e.g., has not orphaned through dropping shims) and sorted.
  * The file is stored under the *owner shim* directory.

* All files participating listing the `buildver` of the current Maven build session are symlinked to
`target/${buildver}/generated/src/(main|test)/(scala|java)`
except for template classes requiring spark.version.classifier in the package name.

* If the package name of a class such as RapidsShuffleManager contains `$_spark.version.classifier_`
(because it is source-identical across shims up to the package name) it will be materialized in the
`target/${buildver}/generated/src/(main|test)/(scala|java)` with `spark.version.classifier`
interpolated into the package name.

Thus, instead of hardcoding distinct
lists of directories for `build-helper` Maven plugin to add (one for each shim) after the full
transition to shimplify, the pom will have only 4 add source statements that is independent of the
number of supported shims.

With the shimplify format in place it is easy to review all the files for a single shim without
relying on Maven:

```bash
git grep '{"spark": "323"}' '*.scala' '*.java'
```

## Conversion to the Shimplify-based Directory Structure

Shimplify can automatically convert the prior version-range-with-exceptions directory structure to
the simplified version. This allows to make it an atomic transition without having to resolve
almost unavoidable merge conflicts due to the sheer size of this sweeping change while the shim
development is ongoing. The conversion of the shims source code and the regular build should not
be done simultaneously for faster isolation and correction of potential bugs in the conversion code.

Prior to invoking the conversion standalone, you first run

```bash
mvn clean install -DskipTests
```

on the current state of the `spark-rapids` repo.

After that you can execute conversion in one or more iterations depending on specified -D parameters

```bash
mvn generate-sources -Dshimplify=true [-D...]
```

With `-Dshimplify=true`, shimplify is put on the write call path to generate and inject
spark-rapids-shim-json-lines comments to all shim source files. The files are not yet moved to their
owner shim directory, and so it is easy to verify with `git diff` the comments being injected. If
you see any issue you can fix it and re-execute the command by adding
`-Dshimplify.overwrite=true`. However, it is usually easier to just have git restore the
previous state:

```bash
git restore sql-plugin tests
```

Once the shim comments looks good (as expected, it was tested), you can repeat it and now actually
move the files to designated locations by invoking

```bash
mvn generate-sources -Dshimplify=true -Dshimplify.move=true
```

Now you can run a package build with the simplified directory structure and run a few integration
tests preferably in the test standalone mode with the RAPIDS Shuffle Manager on for increased
coverage:

```bash
mvn clean package -DskipTests -Dbuildver=331
SPARK_HOME=~/dist/spark-3.3.1-bin-hadoop3 \
    NUM_LOCAL_EXECS=2 \
    PYSP_TEST_spark_rapids_shuffle_mode=MULTITHREADED \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_writer_threads=2 \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_reader_threads=2 \
    PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.spark331.RapidsShuffleManager \
    PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0 \
    PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=0.1 \
    PYSP_TEST_spark_rapids_memory_gpu_allocFraction=0.1 \
    ./integration_tests/run_pyspark_from_build.sh -k test_hash_grpby_sum
```

If smoke testing does not reveal any issues proceed to committing the change. If there are issues
you can undo with

```bash
git restore --staged sql-plugin tests
git restore sql-plugin tests
```

and by reviewing and removing the new directories with

```bash
git clean -f -d --dry-run
```

### Partial Conversion

It is not expected to be really necessary but it is possible to convert a subset of the shims

* Either by adding -Dshimplify.shims=buildver1,buildver2,... to the commands above
* Or by specifying a list of directories you would like to delete to have a simpler directory
-Dshimplify.dirs=320until340-non330db,320until330-noncdh

The latter is just a minor twist on the former. Instead of having an explicit list of shims, it
first computes the list of all `buildver` values using provided directories. After this *all* the
files for the shims, not just under specified directories are converted.

In both cases, the conversion does not leave the rest of the shims totally unaffected when
there are common files with a specified shim. However, it guarantees to leave the previous dedicated
files under `src/(main|test)/${buildver}` in place for shims outside the list. This is useful when
developers of a certain shim would like to continue working on it without adapting the new method.
However, for the simplicity of future refactoring the full transition is preferred.

### Evolving shims without automatic conversion

Suppose a bulk-conversion of existing shims is not an option whereas the next shimming issue
requires difficult refactoring of version ranges with adding more directories with exceptions.
Now it can be resolved easily by placing just the affected files to owner shim directories and
adding shim JSON lines comments by hand.

## Adding a new shim

Shimplify can clone an existing shim based as a basis of the new shim. For example when adding
support for a new [maintenance][5] version of Spark, say 3.2.4, it's expected to be similar to 3.2.3.

If just 3.2.3 or all shims after the full transition have already been converted you can execute

```bash
mvn generate-sources -Dshimplify=true \
    -Dshimplify.move=true -Dshimplify.overwrite=true \
    -Dshimplify.add.shim=324 -Dshimplify.add.base=323
```

to clone 323 as 324. This will add `{"spark": "324"}` to every shared file constituting the 323
shim. Moreover, it will create

* a copy of dedicated 323 files with spark323 under spark324 shim
directory
* substitute spark324 for spark323 in the package name and path,
* and modify the comment from `{"spark": "323"}` to `{"spark": "324"}`

Review the new repo state, e.g., using `git grep '{"spark": "324"}'`.
Besides having to add the `release324` profile to various pom.xml as before, this alone
is likely to be insufficient to complete the work on 324. It is expected you will need to
work on resolving potential compilation failures manually.

## Deleting a Shim

Every Spark build is de-supported eventually. To drop a build say 330 you can run

```bash
mvn generate-sources -Dshimplify=true -Dshimplify.move=true \
    -Dshimplify.remove.shim=330
```

This command will remove the comment line `{"spark": "330"}` from all source files contributing to
the 330 shim. If a file belongs exclusively to 330 it will be removed.

After adding or deleting shims you should sanity-check the diff in the local git repo and
run the integration tests above.

## Symlinks & IDE

IDEs may or may not reveal whether a file is accessed via a symlink. IntelliJ IDEA treats the
original file path and a path via a symlink to the same file as two independent files by default.

In the context of shimplify, only the generated symlink path is part of the project
because the owner shim path is not `add-source`d during build and therefore during IDEA Project
Import. The user can install the [Resolve Symlinks][6] plugin to prevent IDEA from opening multiple
windows for the same physical source file. As of the time of this writing, it works seamlessly with
the exception when the file is open via a Debugger either on a breakpoint hit or subsequent clicking
on the affected stack frame in which case you will see an extra editor tab being added.

No matter whether or not you use the [Resolve Symlinks][6] plugin, IDEA is able to add a breakpoint
set directly via the original physical file or a symlink path.

## Reducing Code Duplication

You can help reducing code complexity by consolidating copy-and-pasted shim code accumulated because
it had been hard to fit it into a less flexible shim inheritance hierarchy based on versions with
exceptions.

You can use the CPD tool that is integrated into our Maven build to find duplicate code in the shim
and in the regular code base. It is not ready for automation and has to invoked manually, separately
for Java and Scala, e.g.:

```bash
mvn antrun:run@duplicate-code-detector \
    -Dcpd.argLine='--minimum-tokens 50 --language scala --skip-blocks-pattern /*|*/' \
    -Dcpd.sourceType='main' \
    > target/cpd.scala.txt
```

Delete duplicate methods and move a single copy into an object such as `SomethingShim` and annotate
its file with the list of buildvers.

See [CPD user doc][7] for more details about the options you can pass inside `cpd.argLine`.

[1]: https://github.com/NVIDIA/cudf-spark/issues/3223
[2]: https://github.com/NVIDIA/cudf-spark/blob/b7b1a5d544b6a3ac35ed064b5c32ee0d63c78845/build/shimplify.py#L15-L79
[3]: https://github.com/NVIDIA/cudf-spark/blob/74ce729ca1306db01359e68f7f0b7cc31cd3d850/pom.xml#L494-L500
[4]: https://jsonlines.org/
[5]: https://spark.apache.org/versioning-policy.html
[6]: https://plugins.jetbrains.com/plugin/16429-idea-resolve-symlinks
[7]: https://docs.pmd-code.org/latest/pmd_userdocs_cpd.html
