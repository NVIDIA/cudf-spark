# Contributing to RAPIDS Accelerator for Apache Spark

Contributions to RAPIDS Accelerator for Apache Spark fall into the following three categories.

1. To report a bug, request a new feature, or report a problem with
    documentation, please file an [issue](https://github.com/NVIDIA/spark-rapids/issues/new/choose)
    describing in detail the problem or new feature. The project team evaluates
    and triages issues, and schedules them for a release. If you believe the
    issue needs priority attention, please comment on the issue to notify the
    team.
2. To propose and implement a new Feature, please file a new feature request
    [issue](https://github.com/NVIDIA/spark-rapids/issues/new/choose). Describe the
    intended feature and discuss the design and implementation with the team and
    community. Once the team agrees that the plan looks good, go ahead and
    implement it using the [code contributions](#code-contributions) guide below.
3. To implement a feature or bug-fix for an existing outstanding issue, please
    follow the [code contributions](#code-contributions) guide below. If you
    need more context on a particular issue, please ask in a comment.

## Branching Convention

There are two types of branches in this repository:

* `branch-[version]`: are development branches which can change often. Note that we merge into
  the branch with the greatest version number, as that is our default branch.

* `main`: is the branch with the latest released code, and the version tag (i.e. `v0.1.0`)
  is held here. `main` will change with new releases, but otherwise it should not change with
  every pull request merged, making it a more stable branch.

## Git Submodules

This repository uses git submodules. The submodules may need to be updated after the repository
is cloned or after moving to a new commit via `git submodule update --init`. See the
[Git documentation on submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules) for more
information.

## Building From Source

We use [Maven](https://maven.apache.org) for most aspects of the build. We test the build with latest
patch versions for Maven 3.6.x, 3.8.x and 3.9.x. Maven version 3.6.0 or more recent is enforced.

Some important parts
of the build execute in the `verify` phase of the Maven build lifecycle.  We recommend when
building at least running to the `verify` phase, e.g.:

```shell script
mvn verify
```

After a successful build, the RAPIDS Accelerator jar will be in the `dist/target/` directory.
This will build the plugin for a single version of Spark.  By default, this is Apache Spark
3.3.0. To build against other versions of Spark you use the `-Dbuildver=XXX` command line option
to Maven. For instance to build Spark 3.3.0 you would use:

```shell script
mvn -Dbuildver=330 verify
```
You can find all available build versions in the top level pom.xml file. If you are building
for Databricks then you should use the `jenkins/databricks/build.sh` script and modify it for
the version you want.

Note that we build against both Scala 2.12 and 2.13. Any contribution you make to the
codebase should compile with both Scala 2.12 and 2.13 for Apache Spark versions 3.3.0 and
higher.

Also, if you make changes in the parent `pom.xml` or any other of the module `pom.xml`
files, you must run the following command to sync the changes between the Scala 2.12 and
2.13 pom files:

```shell script
./build/make-scala-version-build-files.sh 2.13
```

That way any new dependencies or other changes will also be picked up in the Scala 2.13 build.

See the [scala2.13](scala2.13) directory for more information on how to build against
Scala 2.13.

To get an uber jar with more than 1 version you have to `mvn package` each version
and then use one of the defined profiles in the dist module, or a comma-separated list of
build versions. See the next section for more details.

You might see a warning during scala-maven-plugin compile goal invocation.
```
[INFO] Compiling 94 Scala sources and 1 Java source to /home/user/gits/NVIDIA/spark-rapids/tests/target/spark3XY/test-classes ...
OpenJDK 64-Bit Server VM warning: CodeCache is full. Compiler has been disabled.
OpenJDK 64-Bit Server VM warning: Try increasing the code cache size using -XX:ReservedCodeCacheSize=
CodeCache: size=245760Kb used=236139Kb max_used=243799Kb free=9620Kb
 bounds [0x00007f9681000000, 0x00007f9690000000, 0x00007f9690000000]
 total_blobs=60202 nmethods=59597 adapters=504
 compilation: disabled (not enough contiguous free space left)
```

It can be mitigated by increasing [ReservedCodeCacheSize](https://spark.apache.org/docs/latest/building-spark.html#setting-up-mavens-memory-usage)
passed in the `MAVEN_OPTS` environment variable.

### Building a Distribution for Multiple Versions of Spark

By default, the distribution jar only includes code for a single version of Spark, albeit the jar file
layout will be such that it can be accessed only using the Shim loading logic for
[multiple Spark versions](./docs/dev/shims.md#run-time-issues). See
[below](#building-a-distribution-for-a-single-spark-release) for dist jar creation without
the need for a special shim class loader.

If you want to create a jar with multiple versions we have the following options.

1. Build for all Apache Spark versions and CDH with no SNAPSHOT versions of Spark, only released. Use `-PnoSnapshots`.
2. Build for all Apache Spark versions and CDH including SNAPSHOT versions of Spark we have supported for. Use `-Psnapshots`.
3. Build for all Apache Spark versions, CDH and Databricks with no SNAPSHOT versions of Spark, only released. Use `-PnoSnaphsotsWithDatabricks`.
4. Build for all Apache Spark versions, CDH and Databricks including SNAPSHOT versions of Spark we have supported for. Use `-PsnapshotsWithDatabricks`
5. Build for an arbitrary combination of comma-separated build versions using `-Dincluded_buildvers=<CSV list of build versions>`.
   E.g., `-Dincluded_buildvers=330,331`

You must first build each of the versions of Spark and then build one final time using the profile for the option you want.

You can also install some manually and build a combined jar. For instance to build non-snapshot versions:

```shell script
mvn clean
mvn -Dbuildver=330 install -Drat.skip=true -DskipTests
mvn -Dbuildver=331 install -Drat.skip=true -DskipTests
mvn -Dbuildver=332 install -Drat.skip=true -DskipTests
mvn -pl dist -PnoSnapshots package -DskipTests
```

Verify that shim-specific classes are hidden from a conventional classloader.

```bash
$ javap -cp dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar com.nvidia.spark.rapids.shims.SparkShimImpl
Error: class not found: com.nvidia.spark.rapids.shims.SparkShimImpl
```

However, its bytecode can be loaded if prefixed with `spark3XY` not contained in the package name

```bash
$ javap -cp dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar spark330.com.nvidia.spark.rapids.shims.SparkShimImpl | head -2
Warning: File dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar(/spark330/com/nvidia/spark/rapids/shims/SparkShimImpl.class) does not contain class spark330.com.nvidia.spark.rapids.shims.SparkShimImpl
Compiled from "SparkShims.scala"
public final class com.nvidia.spark.rapids.shims.SparkShimImpl {
```

#### Building with buildall script

There is a build script `build/buildall` that automates the local build process. Use
`./build/buildall --help` for up-to-date use information.

By default, it builds everything that is needed to create a distribution jar for all released (noSnapshots) Spark versions except for Databricks. Other profiles that you can pass using `--profile=<distribution profile>` include
- `snapshots` that includes all released (noSnapshots) and snapshots Spark versions except for Databricks
- `minimumFeatureVersionMix` that currently includes 330 is recommended for catching incompatibilities already in the local development cycle

For initial quick iterations we can use `--profile=<buildver>` to build a single-shim version. e.g., `--profile=330` for Spark 3.3.0.

The option `--module=<module>` allows to limit the number of build steps. When iterating, we often don't have the need for the entire build. We may be interested in building everything necessary just to run integration tests (`--module=integration_tests`), or we may want to just rebuild the distribution jar (`--module=dist`)

By default, `buildall` builds up to 4 shims in parallel using `xargs -P <n>`. This can be adjusted by
specifying the environment variable `BUILD_PARALLEL=<n>`.

### Building against different CUDA Toolkit versions

You can build against different versions of the CUDA Toolkit by modifying the variable `cuda.version`:
* `-Dcuda.version=cuda12` (CUDA 12.x, default)

### Building a Distribution for a Single Spark Release

In many situations the user knows that the Plugin jar will be deployed for a single specific Spark
release. It is most commonly the case when a container image for a cloud or local deployment includes
Spark binaries as well. In such a case it is advantageous to create a jar with
a conventional class directory structure avoiding complications such as
[#3704](https://github.com/NVIDIA/spark-rapids/issues/3704). To this end add
`-DallowConventionalDistJar=true` when invoking Maven.

```bash
mvn package -pl dist -am -Dbuildver=340 -DallowConventionalDistJar=true
```

Verify `com.nvidia.spark.rapids.shims.SparkShimImpl` is conventionally loadable:

```bash
$ javap -cp dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar com.nvidia.spark.rapids.shims.SparkShimImpl | head -2
Compiled from "SparkShims.scala"
public final class com.nvidia.spark.rapids.shims.SparkShimImpl {
```

### Building and Testing

We support JDK17 as our main JDK version, and test JDK8, JDK11 and JDK17. It is possible to build and run
with more modern JDK versions, however these are untested. The first step is to set `JAVA_HOME` in
the environment to your JDK root directory.
If you need to build with a JDK version that we do not test internally add
`-Denforcer.skipRules=requireJavaVersion` to the Maven invocation.

### Building and Testing with ARM

To build our project on ARM platform, please add `-Parm64` to your Maven commands.
NOTE: Build process does not require an ARM machine, so if you want to build the artifacts only
on X86 machine, please also add `-DskipTests` in commands.

```bash
mvn clean verify -Dbuildver=330 -Parm64
```

### Iterative development during local testing

When iterating on changes impacting the `dist` module artifact directly or via
dependencies you might find the jar creation step unacceptably slow. Due to the
current size of the artifact `rapids-4-spark_2.12` Maven Jar Plugin spends the
bulk of the time compressing the artifact content.
Since the JAR file specification focuses on the file entry layout in a ZIP
archive without requiring file entries to be compressed it is possible to skip
compression, and increase the speed of creating `rapids-4-spark_2.12` jar ~3x
for a single Spark version Shim alone.

To this end in a pre-production build you can set the Boolean property
`dist.jar.compress` to `false`, its default value is `true`.

Furthermore, after the first build execution on the clean repository the spark-rapids-jni
SNAPSHOT dependency typically does not change until the next nightly CI build, or the next install
to the local Maven repo if you are working on a change to the native code. So you can save
significant time spent on repeated unpacking these dependencies by adding `-Drapids.jni.unpack.skip`
to the `dist` build command.

The time saved is more significant if you are merely changing
the `aggregator` module, or the `dist` module, or just incorporating changes from
[spark-rapids-jni](https://github.com/NVIDIA/spark-rapids-jni/blob/branch-23.04/CONTRIBUTING.md#local-testing-of-cross-repo-contributions-cudf-spark-rapids-jni-and-spark-rapids)

For example, to quickly repackage `rapids-4-spark` after the
initial `./build/buildall` you can iterate by invoking
```Bash
mvn package -pl dist -PnoSnapshots -Ddist.jar.compress=false -Drapids.jni.unpack.skip
```

or similarly
```Bash
 ./build/buildall --rebuild-dist-only --option="-Ddist.jar.compress=false -Drapids.jni.unpack.skip"
```

## Code contributions

### Source code layout

Conventional code locations in Maven modules are found under `src/main/<language>`. In addition to
that and in order to support multiple versions of Apache Spark with the minimum amount of source
code we maintain Spark-version-specific locations within non-shim modules if necessary. This allows
us to switch between incompatible parent classes inside without copying the shared code to
dedicated shim modules.

Thus, the conventional source code root directories `src/main/<language>` contain the files that
are source-compatible with all supported Spark releases, both upstream and vendor-specific.

The following acronyms may appear in directory names:

|Acronym|Definition  |Example|Example Explanation                           |
|-------|------------|-------|----------------------------------------------|
|db     |Databricks  |350db143|Databricks Spark based on Spark 3.5.0        |
|cdh    |Cloudera CDH|(removed)|Cloudera CDH shims have been removed         |

The version-specific directory names have one of the following forms / use cases:

* `src/main/spark${buildver}`, example: `src/main/spark350db143`
* `src/test/spark${buildver}`, example: `src/test/spark340`

with a special shim descriptor as a Scala/Java comment. See [shimplify.md][1]

[1]: ./docs/dev/shimplify.md

### Setting up an Integrated Development Environment

Our project currently uses `build-helper-maven-plugin` for shimming against conflicting definitions of superclasses
in upstream versions that cannot be resolved without significant code duplication otherwise. To this end different
source directories with differently implemented same-named classes are
[added](https://www.mojohaus.org/build-helper-maven-plugin/add-source-mojo.html)
for compilation depending on the targeted Spark version.

This may require some modifications to IDEs' standard Maven import functionality.

#### IntelliJ IDEA

Last tested with IntelliJ IDEA 2023.1.2 (Community Edition)

##### Manual Maven Install for a target Spark build

Before proceeding with importing spark-rapids into IDEA or switching to a different Spark release
profile, execute the install phase with the corresponding `buildver`, e.g. for Spark 3.4.0:

```bash
 mvn clean install -Dbuildver=340 -Dmaven.scaladoc.skip -DskipTests
```

##### Importing the project

Our build relies on [symlink generation](./docs/dev/shimplify.md#symlinks--ide) for Spark
release-specific sources. It is recommended to install
the IDEA Resolve Symlinks plugin via `Marketplace` tab.

To start working with the project in IDEA is as easy as
[opening](https://blog.jetbrains.com/idea/2008/03/opening-maven-projects-is-easy-as-pie/) the top level (parent)
[pom.xml](pom.xml).

In IDEA 2022.3.1 [unselect](https://www.jetbrains.com/help/idea/2022.3/maven-importing.html)
"Import using the new IntelliJ Workspace Model API (experimental)". After 2023.1.2 the default
"Enable fast import" can be used.

In order to make sure that IDEA handles profile-specific source code roots within a single Maven module correctly,
[unselect](https://www.jetbrains.com/help/idea/2022.3/maven-importing.html) "Keep source and test folders on reimport".

If you develop a feature that has to interact with the Shim layer or simply need to test the Plugin with a different
Spark version, open [Maven tool window](https://www.jetbrains.com/help/idea/2022.3/maven-projects-tool-window.html) and
select one of the `release3xx` profiles (e.g, `release330`) for Apache Spark 3.3.0.
Make sure [Manual Maven Install](#manual-maven-install-for-a-target-spark-build) for that profile
has been executed.

Go to `File | Settings | Build, Execution, Deployment | Build Tools | Maven | Importing` and make sure
that `Generated sources folders` is set to `Detect automatically` and `Phase to be used for folders update`
is changed to `process-test-resources`.

In the Maven tool window hit

1. `Reload all projects`
1. `Generate Sources and Update Folders For all Projects`.

Known Issues:

* With the old "slow" Maven importer it might be necessary to bump up maximum Java Heap size `-Xmx`
via
`File | Settings | Build, Execution, Deployment | Build Tools | Maven | Importing | VM options for importer`

* When IDEA is upgraded, it might be necessary to remove `.idea` from the local git repository root.

* There is a known issue that the test sources added via the `build-helper-maven-plugin` are not handled
[properly](https://youtrack.jetbrains.com/issue/IDEA-100532). The workaround is to `mark` the affected
folders such as `tests/src/test/spark3*` manually as `Test Sources Root`

* There is a known issue where, even after selecting a different Maven profile in the Maven submenu,
the source folders from a previously selected profile may remain active. As a workaround,
when switching to a different profile, go to
`File | Project Structure ... | Modules`, select the `rapids-4-spark-sql_2.12` module,
click `Sources`, and delete all the shim source roots from the `Source Folders` list. Make sure
the right test source folders are in `rapids-4-spark-sql_2.12` and `rapids-4-spark-tests_2.12`.
Re-execute the steps above: reload, and `Generate Sources ...`.

If you see Scala symbols unresolved (highlighted red) in IDEA please try the following steps to resolve it:

* Make sure there are no relevant poms in
`File | Settings | Build, Execution, Deployment | Build Tools | Maven | Ignored Files`

* Restart IDEA and click `Reload All Maven Projects` again

#### Bloop Build Server

[Bloop](https://scalacenter.github.io/bloop/) is a build server and a set of tools around Build
Server Protocol (BSP) for Scala providing an integration path with IDEs that support it. In fact,
you can generate a Bloop project from Maven just for the Maven modules and profiles you are
interested in. For example, to generate the Bloop projects for the Spark 3.3.0 dependency
just for the production code run:

```shell script
mvn -B clean install \
    -DbloopInstall \
    -DdownloadSources=true \
    -Dbuildver=330
```

With `--generate-bloop` we integrated Bloop project generation into `buildall`. It makes it easier
to generate projects for multiple Spark dependencies using the same profiles as our regular build.
It makes sure that the project files belonging to different Spark dependencies are
not clobbered by repeated `bloopInstall` Maven plugin invocations, and it uses
[jq](https://stedolan.github.io/jq/) to post-process JSON-formatted project files such that they
compile project classes into non-overlapping set of output directories.

To activate the Spark dependency version 3XY you currently are working with update
the symlink `.bloop` to point to the corresponding directory `.bloop-spark3XY`

Example usage:
```Bash
./build/buildall --generate-bloop --profile=330
rm -vf .bloop
ln -s .bloop-spark330 .bloop
```

You can now open the spark-rapids as a
[BSP project in IDEA](https://www.jetbrains.com/help/idea/bsp-support.html)

Read on for VS Code Scala Metals instructions.

#### Bloop, Scala Metals, and Visual Studio Code

_Last tested with 1.63.0-insider (Universal) Commit: bedf867b5b02c1c800fbaf4d6ce09cefba_

Another, and arguably more popular, use of Bloop arises in connection with
[Scala Metals](https://scalameta.org/metals/) and [VS @Code](https://code.visualstudio.com/).
Scala Metals implements the
[Language Server Protocol (LSP)](https://microsoft.github.io/language-server-protocol/) for Scala,
and enables features such as context-aware autocomplete, and code browsing between Scala symbol
definitions, references and vice versa. LSP is supported by many editors including Vim and Emacs.

Here we document the integration with VS code. It makes development on a remote node almost
as easy as local development, which comes very handy when working in Cloud environments.

Run `./build/buildall --generate-bloop --profile=<profile>` to generate Bloop projects
for required Spark dependencies, e.g. `--profile=330` for Spark 3.3.0. When developing
remotely this is done on the remote node.

Install [Scala Metals extension](https://scalameta.org/metals/docs/editors/vscode) in VS Code,
either locally or into a Remote-SSH extension destination depending on your target environment.
When your project folder is open in VS Code, it may prompt you to import Maven project.
IMPORTANT: always decline with "Don't ask again", otherwise it will overwrite the Bloop projects
generated with the default `330` profile. If you need to use a different profile, always rerun the
command above manually. When regenerating projects it's recommended to proceed to Metals
"Build commands" View, and click:
1. "Restart build server"
1. "Clean compile workspace"
to avoid stale class files.

Now you should be able to see Scala class members in the Explorer's Outline view and in the
Breadcrumbs view at the top of the Editor with a Scala file open.

Check Metals logs, "Run Doctor" etc. if something is not working as expected. You can also verify
that the Bloop build server and the Metals language server are running by executing `jps` in the
Terminal window:
```shell script
jps -l
72960 sun.tools.jps.Jps
72356 bloop.Server
72349 scala.meta.metals.Main
```

##### Known Issues

###### java.lang.RuntimeException: boom

Metals background compilation process status appears to be resetting to 0% after reaching 99%
and you see a peculiar error message [`java.lang.RuntimeException: boom`][1]. This is a known issue
when running Metals/Bloop on Java 8. To work around it, ensure Metals and Bloop are both running on
Java 17 (minimum Java version for our project):

1. The `-DbloopInstall` profile will enforce Java 11+ compliance.

1. Add [`metals.javaHome`][2] to VSCode preferences to point to Java 11+.

[1]: https://github.com/sourcegraph/scip-java/blob/b7d268233f1a303f66b6d9804a68f64b1e5d7032/semanticdb-javac/src/main/java/com/sourcegraph/semanticdb_javac/SemanticdbTaskListener.java#L76

[2]: https://github.com/scalameta/metals-vscode/pull/644/files#diff-04bba6a35cad1c794cbbe677678a51de13441b7a6ee8592b7b50be1f05c6f626R132
#### Other IDEs
We welcome pull requests with tips on how to setup your favorite IDE!

### Your first issue

1. Read the [Developer Overview](docs/dev/README.md) to understand how the RAPIDS Accelerator
    plugin works.
2. Find an issue to work on. The best way is to look for the
    [good first issue](https://github.com/NVIDIA/spark-rapids/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
    or [help wanted](https://github.com/NVIDIA/spark-rapids/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
    labels.
3. Comment on the issue stating that you are going to work on it.
4. Code! Make sure to update unit tests and integration tests if needed! [refer to test section](#testing-your-code)
5. When done, [create your pull request](https://github.com/NVIDIA/spark-rapids/compare).
6. Verify that CI passes all [status checks](https://help.github.com/articles/about-status-checks/).
    Fix if needed.
7. Wait for other developers to review your code and update code as needed.
8. Once reviewed and approved, a project committer will merge your pull request.

Remember, if you are unsure about anything, don't hesitate to comment on issues
and ask for clarifications!

### Code Formatting
RAPIDS Accelerator for Apache Spark follows the same coding style guidelines as the Apache Spark
project.  For IntelliJ IDEA users, an
[example code style settings file](docs/dev/idea-code-style-settings.xml) is available in the
`docs/dev/` directory.

#### Scala

This project follows the official
[Scala style guide](https://docs.scala-lang.org/style/) and the
[Databricks Scala guide](https://github.com/databricks/scala-style-guide), preferring the latter.

#### Java

This project follows the
[Oracle Java code conventions](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html)
and the Scala conventions detailed above, preferring the latter.

### Sign your work

We require that all contributors sign-off on their commits. This certifies that the contribution is your original work, or you have the rights to submit it under the same license, or a compatible license.

Any contribution which contains commits that are not signed off will not be accepted.

To sign off on a commit use the `--signoff` (or `-s`) option when committing your changes:

```shell
git commit -s -m "Add cool feature."
```

This will append the following to your commit message:

```
Signed-off-by: Your Name <your@email.com>
```

The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch. Use your real name, no pseudonyms or anonymous contributions.  If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`.


The sign-off means you certify the below (from [developercertificate.org](https://developercertificate.org)):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

### Testing Your Code
Please visit the [testing doc](tests/README.md) for details about how to run tests


### Pre-commit hooks
We provide a basic config `.pre-commit-config.yaml` for [pre-commit](https://pre-commit.com/) to
automate some aspects of the development process. As a convenience you can enable automatic
copyright year updates by following the installation instructions on the
[pre-commit homepage](https://pre-commit.com/).

To this end, first install `pre-commit` itself using the method most suitable for your development
environment. Then you will need to run `pre-commit install` to enable it in your local git
repository. Using `--allow-missing-config` will make it easy to work with older branches
that do not have `.pre-commit-config.yaml`.

```bash
pre-commit install --allow-missing-config
```

and setting the environment variable:

```bash
export SPARK_RAPIDS_AUTO_COPYRIGHTER=ON
```
The default value of `SPARK_RAPIDS_AUTO_COPYRIGHTER` is `OFF`.

When automatic copyright updater is enabled and you modify a file with a prior
year in the copyright header it will be updated on `git commit` to the current year automatically.
However, this will abort the [commit process](https://github.com/pre-commit/pre-commit/issues/532)
with the following error message:
```
Update copyright year....................................................Failed
- hook id: auto-copyrighter
- duration: 0.01s
- files were modified by this hook
```
You can confirm that the update has actually happened by either inspecting its effect with
`git diff` first or simply re-executing `git commit` right away. The second time no file
modification should be triggered by the copyright year update hook and the commit should succeed.

There is a known issue for macOS users if they use the default version of `sed`. The copyright update
script may fail and generate an unexpected file named `source-file-E`. As a workaround, please
install GNU sed

```bash
brew install gnu-sed
# and add to PATH to make it as default sed for your shell
export PATH="/usr/local/opt/gnu-sed/libexec/gnubin:$PATH"
```

### Creating a Pull Request

Here are some guidelines to follow when creating a pull request:

1. If your pull request is not ready for review but you want to make use of the
   continuous integration testing facilities, please make it as a draft and label it with `[WIP]`.

2. If your pull request is ready to be reviewed without requiring additional
   work on top of it, then convert it to a regular pull request and remove the `[WIP]` label
   (if applicable).

3. Once the review has taken place, please do not add features or make changes
   out of the scope of those even if the reviewer requests them. Instead, please
   consider filing new issues for those changes.

4. Please avoid rebasing your branch during the review process, as this causes the context
   of any comments made by reviewers to be lost. If conflicts occur during
   review, then they should be resolved by merging into the branch used for
   making the pull request.


### Pull request status checks
A pull request should pass all status checks before being merged.
#### sign-off check
Please follow the steps in the [Sign your work](#sign-your-work) section,
and make sure at least one commit in your pull request is signed-off.
#### blossom-ci
The check runs on NVIDIA self-hosted runner, a [project committer](.github/workflows/blossom-ci.yml#L36) can
manually trigger it by commenting `build`. It includes the following steps,
1. Mergeable check
2. Blackduck vulnerability scan
3. Fetch merged code (merge the pull request HEAD into BASE branch, e.g. fea-001 into branch-x)
4. Run `mvn verify` and unit tests for multiple Spark versions in parallel.
Ref: [spark-premerge-build.sh](jenkins/spark-premerge-build.sh)

If it fails, you can click the `Details` link of this check, and go to `Upload log -> Jenkins log for pull request xxx (click here)` to
find the uploaded log.

Options:
1. Skip tests run by adding `[skip ci]` to title, this should only be used for doc-only change
2. Run build and tests in databricks runtimes by adding `[databricks]` to title, this would add around 30-40 minutes


### Code Review Guidelines

We have [code review guidelines](CODE_REVIEW_GUIDELINES.md) that outline how we collaborate during code review. Please read them before opening or reviewing a pull request.


## Attribution
Portions adopted from https://github.com/rapidsai/cudf/blob/main/CONTRIBUTING.md, https://github.com/NVIDIA/nvidia-docker/blob/master/CONTRIBUTING.md, and https://github.com/NVIDIA/DALI/blob/main/CONTRIBUTING.md
