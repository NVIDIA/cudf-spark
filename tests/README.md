# RAPIDS Accelerator for Apache Spark Testing

We have a stand-alone example that you can run in the [integration tests](../integration_tests).
The example is based off of the mortgage dataset you can download
[here](https://capitalmarkets.fanniemae.com/credit-risk-transfer/single-family-credit-risk-transfer/fannie-mae-single-family-loan-performance-data)
and the code is in the `com.nvidia.spark.rapids.tests.mortgage` package.

## Unit Tests

Unit tests implemented using the ScalaTest framework reside in the [tests]() directory. This is
unconventional and is done so we can run the tests on the close-to-final shaded single-shim version
of the plugin. It also helps with how we collect code coverage.

The `tests` module depends on the `aggregator` module which shades external dependencies and
aggregates them along with internal submodules into an artifact supporting a single Spark version.

The minimum required Maven phase to run unit tests is `package`. Alternatively, you may run
`mvn install` and use `mvn test` for subsequent testing. However, to avoid dealing with stale jars
in the local Maven repo cache, we recommend to invoke `mvn package -pl tests -am ...` from the
`spark-rapids` root directory. Add `-f scala2.13` if you want to run unit tests against
Apache Spark dependencies based on Scala 2.13.

To run targeted Scala tests use

`-DwildcardSuites=<comma separated list of packages or fully-qualified test suites>`

Or easier, use a combination of

`-Dsuffixes=<comma separated list of suffix regexes>` to restrict the test suites being run,
which corresponds to `-q` option in the
[ScalaTest runner](https://www.scalatest.org/user_guide/using_the_runner).

and

`-Dtests=<comma separated list of keywords or test names>`, to restrict tests run within test suites,
which corresponds to `-z` or `-t` options in the
[ScalaTest runner](https://www.scalatest.org/user_guide/using_the_runner).

For more information about using scalatest with Maven please refer to the
[scalatest documentation](https://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin)
and the the
[source code](https://github.com/scalatest/scalatest-maven-plugin/blob/383f396162b7654930758b76a0696d3aa2ce5686/src/main/java/org/scalatest/tools/maven/AbstractScalaTestMojo.java#L34).


#### Running Unit Tests Against Specific Apache Spark Versions
You can run the unit tests against different versions of Spark using the different profiles. The
default version runs against Spark 3.3.0, to run against a specific version use a buildver property:

- `-Dbuildver=330` (Spark 3.3.0)
- `-Dbuildver=350` (Spark 3.5.0)

etc

Please refer to the [tests project POM](pom.xml) to see the list of test profiles supported.
Apache Spark specific configurations can be passed in by setting the `SPARK_CONF` environment
variable.

Examples:

To run all tests against Apache Spark 3.3.0,

```bash
mvn package -pl tests -am -Dbuildver=330
```

To pass Apache Spark configs `--conf spark.dynamicAllocation.enabled=false --conf spark.task.cpus=1`
do something like.

```bash
SPARK_CONF="spark.dynamicAllocation.enabled=false,spark.task.cpus=1" mvn ...
```

To run all tests in `ParquetWriterSuite` in package com.nvidia.spark.rapids, issue

```bash
mvn package -pl tests -am -DwildcardSuites="com.nvidia.spark.rapids.ParquetWriterSuite"
```

To run all AnsiCastOpSuite and CastOpSuite tests dealing with decimals using
Apache Spark 3.3.0 on Scala 2.13 artifacts, issue:

```bash
mvn package -f scala2.13 -pl tests -am -Dbuildver=330 -Dsuffixes='.*CastOpSuite' -Dtests=decimal
```

### Parallel Unit Tests

Premerge runs the Scala unit tests in parallel, using
`ParallelUnitTestRunner` to run suites in separate worker JVMs. Add `[serial ut]` or `[serial-ut]`
to the PR title to run them serially when debugging a concurrency-only failure, reading a linear
ScalaTest log, or verifying a fix. Local runs are serial unless parallel execution is enabled
explicitly:

```bash
mvn package -pl tests -am -Drapids.parallelUnitTests=true -DparallelForkCount=4
```

- `-Dsuffixes` and `-Dtests` are not supported; the runner fails fast. Use
  `-DwildcardSuites`, which matches fully qualified suite-name prefixes.
- The GPU is shared. Each worker gets
  `rapids.test.gpu.allocFraction * 0.8 / parallelForkCount`; with the defaults, four workers each
  get 20% of the GPU memory pool instead of the 100% available to serial execution.
- Each suite has a watchdog controlled by `-DparallelSuiteTimeout`, which defaults to 1800 seconds.
  On timeout, the runner captures a `jstack`, kills the worker, and fails the run.
- The `RapidsDynamicPartitionPruningV1SuiteAEOff` and
  `RapidsDynamicPartitionPruningV1SuiteAEOn` suites are assigned to the same worker so they execute
  serially with each other because concurrent execution previously caused GPU broadcast
  contention. The list is `DPP_SUITES` in `ParallelUnitTestRunner.scala`; the root cause remains
  tracked in [#15401](https://github.com/NVIDIA/cudf-spark/issues/15401).
- To debug a parallel failure, follow the `[wave-<run>-worker-<id>]` log prefix for the failing
  suite. Re-run that suite alone with `-DwildcardSuites=<fully.qualified.Suite>`, both with and
  without `-Drapids.parallelUnitTests=true`, to determine whether concurrency caused the failure.

## Integration Tests

Please refer to the integration-tests [README](../integration_tests/README.md)
