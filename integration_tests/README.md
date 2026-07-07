# RAPIDS Plugin For Apache Spark Integration Tests

This is a set of integration tests for the RAPIDS Plugin for Apache Spark. These tests
are intended to be able to be run against any Spark-compatible cluster/release to help
verify that the plugin is doing the right thing in as many cases as possible.

There are two sets of tests. The PySpark tests are described on this page. The scala tests are
described [here](../tests/README.md).

## Setting Up the Environment

The tests are based off of `pyspark` and `pytest` running on Python 3. There really are
only a small number of Python dependencies that you need to install for the tests. The
dependencies also only need to be on the driver.  You can install them on all nodes
in the cluster but it is not required.

### Prerequisites

The build requires `OpenJDK 17`, `maven`, and `python`.
Skip to the next section if you have already installed them.

#### Java Environment

It is recommended to use `alternatives` to manage multiple java versions.
Then you can simply set `JAVA_HOME` to JDK directory:

  ```shell script
  JAVA_HOME=$(readlink -nf $(which java) | xargs dirname | xargs dirname | xargs dirname)
  ```

#### Installing python using pyenv

It is recommended that you use `pyenv` to manage Python installations.

- First, make sure to install all the required dependencies listed
  [here](https://github.com/pyenv/pyenv/wiki#suggested-build-environment).
- Follow instructions to use the right method of installation described
  [here](https://github.com/pyenv/pyenv#installation)
- Verify that `pyenv` is set correctly

  ```shell script
  which pyenv
  ```

- Using `pyenv` to set Python installation
  - To check versions to be installed (will return a long list)

    ```shell script
    ls ~/.pyenv/versions/
    ```

  - To install a specific version from the available list

    ```shell script
    pyenv install 3.X.Y
    ```

  - To check available versions locally

    ```shell script
    ls ~/.pyenv/versions/
    ```

  - To set python environment to one of the installed versions

    ```shell script
    pyenv global 3.X.Y
    ```

For full details on `pyenv` and instructions, see [pyenv github page](https://github.com/pyenv/pyenv).

#### Installing specific version of Maven

All package managers like `brew` and `apt` offer maven. However, it may lag behind some
versions. In that case, you can install the latest binary from the [Maven download page](https://maven.apache.org/download.cgi).
For manual installation, you need to setup your environment:

  ```shell script
  export M2_HOME=PATH_TO_MAVEN_ROOT_DIRECTOTY
  export M2=${M2_HOME}/bin
  export PATH=$M2:$PATH
  ```

### Dependencies

- pytest
  : A framework that makes it easy to write small, readable tests, and can scale to support complex
  functional testing for applications and libraries (requires  Python 3.6+).
- sre_yield
  : Provides a set of APIs to generate string data from a regular expression.
- pandas
  : A fast, powerful, flexible and easy to use open source data analysis and manipulation
  tool and is only needed when testing integration with pandas.
- pyarrow
  : Provides a Python API for functionality provided by the Arrow C++ libraries, along with
  tools for Arrow integration and interoperability with pandas, NumPy, and other software in
  the Python ecosystem. This is used to test improved transfer performance to pandas based user
  defined functions.
- pytest-xdist
  : A plugin that extends pytest with new test execution modes, the most used being distributing
  tests across multiple CPUs to speed up test execution
- findspark
  : Adds pyspark to sys.path at runtime
- [fastparquet](https://fastparquet.readthedocs.io)
  : A Python library (independent of Apache Spark) for reading/writing Parquet. Used in the
  integration tests for checking Parquet read/write compatibility with the RAPIDS plugin.

You can install all the dependencies using `pip` by running the following command:

  ```shell script
  pip install -r requirements.txt
  ```

### Installing Spark

You need to install spark-3.x and set `$SPARK_HOME/bin` to your `$PATH`, where
`SPARK_HOME` points to the directory of a runnable Spark distribution.
This can be done in the following three steps:

1. Choose the appropriate way to create Spark distribution:

   - To run the plugin against a non-snapshot version of spark, download a distribution from Apache-Spark [download page](https://spark.apache.org/downloads.html);
   - To run the plugin against a snapshot version of Spark, you will need to buid
     the distribution from source:

      ```shell script
      ## clone locally
      git clone https://github.com/apache/spark.git spark-src-latest
      cd spark-src-latest
      ## build a distribution with hive support
      ## generate a single tgz file $MY_SPARK_BUILD.tgz
      ./dev/make-distribution.sh --name $MY_SPARK_BUILD --tgz -Pkubernetes -Phive
      ```

      For more details about the configurations, and the arguments, visit [Apache Spark Docs::Building Spark](https://spark.apache.org/docs/latest/building-spark.html#building-a-runnable-distribution).

2. Extract the `.tgz` file to a suitable work directory `$SPARK_INSTALLS_DIR/$MY_SPARK_BUILD`.

3. Set the  variables to appropriate values:

    ```shell script
    export SPARK_HOME=$SPARK_INSTALLS_DIR/$MY_SPARK_BUILD
    export PATH=${SPARK_HOME}/bin:$PATH
    ```

### Building The Plugin

Next, visit [CONTRIBUTING::Building From Source](../CONTRIBUTING.md#building-from-source) to learn
about building the plugin for different versions of Spark.
Make sure that you compile the plugin against the same version of Spark that it is going to run with.

## Running

Tests will run as a part of the maven build if you have the environment variable `SPARK_HOME` set.

The suggested way to run these tests is to use the shell-script file located in the
 integration_tests folder called [run_pyspark_from_build.sh](run_pyspark_from_build.sh). This script takes
care of some of the flags that are required to run the tests which will have to be set for the
plugin to work. It will also automatically detect the Scala version used by the Spark located
at `$SPARK_HOME`.  It will be very useful to read the contents of the
[run_pyspark_from_build.sh](run_pyspark_from_build.sh) to get a better insight
into what is needed as we constantly keep working on to improve and expand the plugin-support.

The python tests run with pytest and the script honors pytest parameters:

- The explicit test specification of specific modules, methods, and their parametrization
  is supported by using the `TESTS` environment variable instead of positional arguments
  in pytest CLI
- `-k` <keyword_expression>. This will run all the tests satisfying the keyword
  expression.
- `-s` Doesn't capture the output and instead prints to the screen.
- `-v` Increase the verbosity of the tests
- `-r fExXs` Show extra test summary info as specified by chars: (f)ailed, (E)rror, (x)failed, (X)passed, (s)kipped
- For other options and more details please visit [pytest-usage](https://docs.pytest.org/en/stable/usage.html) or type `pytest --help`

Examples:

  ```shell script
  ## running all integration tests for Map
  ./integration_tests/run_pyspark_from_build.sh -k map_test.py
  ## Running a single integration test in map_test
  ./integration_tests/run_pyspark_from_build.sh -k 'map_test.py and test_map_integration_1'
  ## Running tests marching the keyword "exist" from any module
  ./integration_tests/run_pyspark_from_build.sh -k exist
  ## Running all parametrization of the method arithmetic_ops_test.py::test_addition
  ## and a specific parametrization of array_test.py::test_array_exists
  TESTS="arithmetic_ops_test.py::test_addition array_test.py::test_array_exists[3VL:off-data_gen0]" ./integration_tests/run_pyspark_from_build.sh
  ```

### Spark execution mode

Spark Applications (pytest in this case) can be run against different cluster backends
specified by the configuration `spark.master`. It can be provided by various means such
as via `--master` argument of `spark-submit`.

By default, the [local mode](
https://github.com/apache/spark/blob/v3.1.1/core/src/main/scala/org/apache/spark/deploy/SparkSubmitArguments.scala#L214
) is used to run the Driver and Executors in the same JVM. Albeit convenient, this mode sometimes
masks problems occurring in fully distributed production deployments. These are often bugs related
to object serialization and hash code implementation.

Thus, Apache Spark provides another lightweight way to test applications in the pseudo-distributed
[local-cluster[numWorkers,coresPerWorker,memoryPerWorker]](
https://github.com/apache/spark/blob/v3.1.1/core/src/main/scala/org/apache/spark/SparkContext.scala#L2993
) mode where executors are run in separate JVMs on your local machine.

The following environment variables control the behavior in the `run_pyspark_from_build.sh` script

- `NUM_LOCAL_EXECS` if set to a positive integer value activates the `local-cluster` mode
  and sets the number of workers to `NUM_LOCAL_EXECS`
- `CORES_PER_EXEC` determines the number of cores per executor if `local-cluster` is activated
- `MB_PER_EXEC` determines the amount of memory per executor in megabyte if `local-cluster`
  is activated

### Pytest execution mode

By default the tests try to use the python packages `pytest-xdist` and `findspark` to oversubscribe
your GPU and run the tests in Spark local mode. This can speed up these tests significantly as all
of the tests that run by default process relatively small amounts of data. Be careful because if
you have `SPARK_CONF_DIR` also set the tests will try to use whatever cluster you have configured.
If you do want to run the tests in parallel on an existing cluster it is recommended that you set
`-Dpytest.TEST_PARALLEL` to one less than the number of worker applications that will be
running on the cluster.  This is because `pytest-xdist` will launch one control application that
is not included in that number. All it does is farm out work to the other applications, but because
it needs to know about the Spark cluster to determine which tests to run and how it still shows up
as a Spark application.

To run the tests separate from the build go to the `integration_tests` directory. You can submit
`runtests.py` through `spark-submit`, but if you want to run the tests in parallel with
`pytest-xdist` you will need to submit it as a regular python application and have `findspark`
installed.  Be sure to include the necessary jars for the RAPIDS plugin either with
`spark-submit` or with the cluster when it is
[setup](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/on-premise.html).
The command line arguments to `runtests.py` are the same as for
[pytest](https://docs.pytest.org/en/latest/usage.html). The only reason we have a separate script
is that `spark-submit` uses python if the file name ends with `.py`.

If you want to configure the Spark cluster you may also set environment variables for the tests.
The name of the env var should be in the form `"PYSP_TEST_" + conf_key.replace('.', '_')`. Linux
does not allow '.' in the name of an environment variable so we replace it with an underscore. If
the property contains an underscore, substitute '__' for each original '_'.
For example, `spark.sql.catalog.spark_catalog` is represented by the environment variable
`PYSP_TEST_spark_sql_catalog_spark__catalog`.

We also have a large number of integration tests that currently run as a part of the unit tests
using scala test. Those are in the `src/test/scala` sub-directory and depend on the testing
framework from the `rapids-4-spark-tests_2.x` test jar.

You can run these tests against a cluster similar to how you can run `pytests` against an
existing cluster. To do this you need to launch a cluster with the plugin jars on the
classpath. The tests will enable and disable the plugin as they run.

Next you need to copy over some test files to whatever distributed file system you are using.
The test files are everything under `./integration_tests/src/test/resources/`  Be sure to note
where you placed them because you will need to tell the tests where they are.

When running these tests you will need to include the test jar, the integration test jar,
the scala-test and scalactic. You can find scala-test and scalactic under
`~/.m2/repository`.

It is recommended that you use `spark-shell` and the scalatest shell to run each test
individually, so you don't risk running unit tests along with the integration tests.
http://www.scalatest.org/user_guide/using_the_scalatest_shell

```shell
spark-shell --jars rapids-4-spark-tests_2.12-26.08.0-SNAPSHOT-tests.jar,rapids-4-spark-integration-tests_2.12-26.08.0-SNAPSHOT-tests.jar,scalatest_2.12-3.0.5.jar,scalactic_2.12-3.0.5.jar
```

First you import the `scalatest_shell` and tell the tests where they can find the test files you
just copied over.

```scala
import org.scalatest._
com.nvidia.spark.rapids.TestResourceFinder.setPrefix(PATH_TO_TEST_FILES)
```

Next you can start to run the tests.

```scala
durations.run(new com.nvidia.spark.rapids.JoinsSuite)
```

Most clusters probably will not have the RAPIDS plugin installed in the cluster yet.
If you just want to verify the SQL replacement is working you will need to add the
`rapids-4-spark` jar to your `spark-submit` command. Note the following example
assumes CUDA 12 is being used and the Spark distribution is built with Scala 2.12.

```
$SPARK_HOME/bin/spark-submit --jars "rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar" ./runtests.py
```

You don't have to enable the plugin for this to work, the test framework will do that for you.


You do need to have access to a compatible GPU with the needed CUDA drivers. The exact details of how to set this up are beyond the scope of this document, but the Spark feature for scheduling GPUs does make this very simple if you have it configured.

### Runtime Environment

`--runtime_env` is used to specify the environment you are running the tests in. Valid values are `databricks`,`emr`,`dataproc`,`dataproc_serverless` and `apache`. This is generally used
when certain environments have different behavior, and the tests don't have a good way to auto-detect the environment yet.

### timezone

The RAPIDS plugin currently only supports the UTC time zone. Spark uses the default system time zone unless explicitly set otherwise.
To make sure that the tests work properly you need to configure your cluster or application to run with UTC.
The python framework cannot always do this for you because it risks overwriting other java options in the config.
Please be sure that the following configs are set when running the tests.

  * `spark.driver.extraJavaOptions` should include `-Duser.timezone=UTC`
  * `spark.executor.extraJavaOptions` should include `-Duser.timezone=UTC`
  * `spark.sql.session.timeZone`=`UTC`

### Running in parallel

You may use `pytest-xdist` to run the tests in parallel. This is done by running the tests through `python`, not `spark-submit`,
and setting the parallelism with the `-n` command line parameter. Be aware that `pytest-xdist` will launch one control application
and the given number of worker applications, so your cluster needs to be large enough to handle one more application than the parallelism
you set. Most tests are small and don't need even a full GPU to run. So setting your applications to use a single executor and a single
GPU per executor is typically enough. When running from maven we assume that we are running in local mode and will try to
oversubscribe a single GPU.  Typically we find that the tests don't need more than 2GB of GPU memory so we can speed up the tests significantly
by doing this. It is not easy nor recommended to try and configure an actual cluster so you can oversubscribe GPUs.  Please don't try it.

Under YARN and Kubernetes you can set `spark.executor.instances` to the number of executors you want running in your application
(1 typically). Spark will auto launch a driver for each application too, but if you configured it correctly that would not take
any GPU resources on the cluster. For standalone, Mesos, and Kubernetes you can set `spark.cores.max` to one more than the number
of executors you want to use per application. The extra core is for the driver. Dynamic allocation can mess with these settings
under YARN and even though it is off by default you probably want to be sure it is disabled (spark.dynamicAllocation.enabled=false).

### Running with Alternate Paths

In case your test jars and resources are downloaded to the `local-path` from dependency Repo, and you want to run tests with them
using the shell-script [run_pyspark_from_build.sh](run_pyspark_from_build.sh), then the `LOCAL_JAR_PATH=local-path` must be set to point
to the `local-path`, e.g. `LOCAL_JAR_PATH=local-path bash [run_pyspark_from_build.sh](run_pyspark_from_build.sh)`. By setting `LOCAL_JAR_PATH=local-path`
the shell-script [run_pyspark_from_build.sh](run_pyspark_from_build.sh) can find the test jars and resources in the alternate path. If you only want to override the 
plugin jar then you can just set `PLUGIN_JAR=<path to the jar>`.

When running the shell-script [run_pyspark_from_build.sh](run_pyspark_from_build.sh) under YARN or Kubernetes, the `$SCRIPTPATH` in the python options
`--rootdir $SCRIPTPATH ...` and `--std_input_path $SCRIPTPATH ...` will not work, as the `$SCRIPTPATH` is a local path, you need to overwrite it to the cloud paths.
Basically, you need first to upload the test resources onto the cloud path `resource-path`, then transfer the test resources onto the working directory
`root-dir` of each executor(e.g. via `spark-submit --files root-dir ...`). After that you must set both `LOCAL_ROOTDIR=root-dir` and `INPUT_PATH=resource-path`
to run the shell-script, e.g. `LOCAL_ROOTDIR=root-dir INPUT_PATH=resource-path bash [run_pyspark_from_build.sh](run_pyspark_from_build.sh)`.

### Running with a fixed data generation seed

By default the tests are run with a different random data generator seed to increase the chance of
uncovering bugs due to specific inputs. The seed used for a test is printed as part of the test
name, see the `DATAGEN_SEED=` part of the test name printed as tests are run. If a problem is found
with a specific data generation seed, the seed can be set explicitly when running the tests by
exporting the `DATAGEN_SEED` environment variable to the desired seed before running the
integration tests. For example:

```shell
$ DATAGEN_SEED=1702166057 SPARK_HOME=~/spark-3.4.0-bin-hadoop3 integration_tests/run_pyspark_from_build.sh
```

Tests can override the seed used using the test marker:

```
@datagen_overrides(seed=<new seed here>, [condition=True|False], [permanent=True|False])`.
```

This marker has the following arguments:
- `seed`: a hard coded datagen seed to use.
- `condition`: is used to gate when the override is appropriate, usually used to say that specific shims
               need the special override.
- `permanent`: forces a test to ignore `DATAGEN_SEED` if True. If False, or if absent, the `DATAGEN_SEED` value always wins.

### Randomly selecting tests

To shorten feedback cycles, you can ask the harness to execute only a random subset of the collected
tests by setting the `RANDOM_SELECT` environment variable before invoking
`run_pyspark_from_build.sh`. Values greater than or equal to `1` are treated as an absolute number of
tests to run, values between `0` and `1` are interpreted as a fraction of the collected test set, and
`0` skips all tests. Leave the variable unset to execute the full suite. You can combine `RANDOM_SELECT`
with `TESTS`, `-k`, or `-m` filters to limit the pool of tests that the random selection operates on.

Set `RANDOM_SELECT_SEED` to make the selection deterministic when reproducing runs. If omitted, the
seed defaults to `0`.

Examples:

```shell
# Run 100 random tests out of the collected set
RANDOM_SELECT=100 ./integration_tests/run_pyspark_from_build.sh

# Run roughly 10% of the collected tests
RANDOM_SELECT=0.1 ./integration_tests/run_pyspark_from_build.sh

# Run 100 random tests with a fixed seed for reproducibility
RANDOM_SELECT=100 RANDOM_SELECT_SEED=42 ./integration_tests/run_pyspark_from_build.sh

# Run 100 random tests chosen from cases matching the keyword "aggregate"
RANDOM_SELECT=100 ./integration_tests/run_pyspark_from_build.sh -k 'aggregate'
```

If the requested count or fraction is greater than or equal to the number of collected tests, the full
set is executed and a message is printed indicating that no reduction was applied.

### Controlling OOM injection

Synthetic GPU out-of-memory (OOM) injection helps exercise recovery paths in the plugin. Use the pytest
option `--test_oom_injection_mode` to choose how the harness injects OOMs:

- `random` (default): randomly inject OOMs into a subset of tests.
- `always`: inject OOMs into every eligible test.
- `never`: disable OOM injection.

Pass the option through the wrapper script by appending it after `--`, for example:

```shell
./integration_tests/run_pyspark_from_build.sh -- --test_oom_injection_mode=never
```

The randomness used when the mode is `random` is controlled by the `SPARK_RAPIDS_TEST_INJECT_OOM_SEED`
environment variable. If unset, the launcher script assigns the current timestamp and prints the seed at
startup so that the run can be reproduced.

### Running with non-UTC time zone
For the new added cases, we should check non-UTC time zone is working, or the non-UTC nightly CIs will fail.
The non-UTC nightly CIs are verifing all cases with non-UTC time zone.
But only a small amout of cases are verifing with non-UTC time zone in the pre-merge CI due to limited GPU resources.
When adding cases, should also check non-UTC is working besides the default UTC time zone.
Please test the following time zones:
```shell
$ TZ=Asia/Shanghai ./integration_tests/run_pyspark_from_build.sh
$ TZ=America/Los_Angeles ./integration_tests/run_pyspark_from_build.sh
```
`Asia/Shanghai` is non-DST(Daylight Savings Time) time zone and `America/Los_Angeles` is DST time zone.

If the new added cases failed with non-UTC, then should allow the operator(does not support non-UTC) fallback,
For example, add the following annotation to the case:
```python
non_utc_allow_for_sequence = ['ProjectExec'] # Update after non-utc time zone is supported for sequence
@allow_non_gpu(*non_utc_allow_for_sequence)
test_my_new_added_case_for_sequence_operator()
```

### Running with Hybrid execution
The hybrid tests require extra jars. To enable hybrid tests, the following prerequisites are required::
- Build Gluten bundle jar, Gluten thirdparty jar, refer to [link](../docs/dev/hybrid-execution.md#build)
- Download Hybrid jar, refer to [link](../docs/dev/hybrid-execution.md#download-rapids-hybrid-jar-from-maven-repo)

Execute the following command to run Hybrid tests:
```shell
$ LOAD_HYBRID_BACKEND=1 \
  HYBRID_BACKEND_JARS=/path/to/${GLUTEN_BUNDLE_JAR},/path/to/${GLUTEN_THIRD_PARTY_JAR},/path/to/HYBRID_JAR \
  ./integration_tests/run_pyspark_from_build.sh -m hybrid_test
```
For more information about Hybrid feature, refer to [link](../docs/dev/hybrid-execution.md)

### Reviewing integration tests in Spark History Server

If the integration tests are run using [run_pyspark_from_build.sh](run_pyspark_from_build.sh) we have
the [event log enabled](https://spark.apache.org/docs/latest/monitoring.html) by default. You can opt
out by setting the environment variable `SPARK_EVENTLOG_ENABLED` to `false`.

Compressed event logs will appear under the run directories of the form
`integration_tests/target/run_dir/eventlog_WORKERID`. If xdist is not used (e.g., `TEST_PARALLEL=1`)
the event log directory will be `integration_tests/target/run_dir/eventlog_gw0` as if executed by
worker 0 under xdist.

To review all the tests run by a particular worker you can start the History Server as follows:
```shell
SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=integration_tests/target/run_dir/eventlog_gw0" \
  ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.history.HistoryServer
```

By default, integration tests write event logs using [Zstandard](https://facebook.github.io/zstd/)
(`zstd`) compression codec. It can be changed by setting the environment variable `PYSP_TEST_spark_eventLog_compression_codec` to one of
the SHS supported values for the config key
[`spark.eventLog.compression.codec`](https://spark.apache.org/docs/latest/configuration.html#spark-ui)

With `zstd` it's easy to view / decompress event logs using the CLI `zstd -d [--stdout] <file>`
even without the SHS webUI.

### Worker Logs 

NOTE: Available only in local mode i.e. master URL = local[K, F]  

By default, when using xdist the integration tests will write the tests output to console and to a text file 
that will appear under the run directory of the form 
`integration_tests/target/run_dir-<timestamp>-xxxx/WORKERID_worker_logs.log`. The output format of the log and the log level  
can be changed by modifying the file `integration_tests/src/test/resources/pytest_log4j.properties`.

If xdist is not used (e.g., `TEST_PARALLEL=1`)
the worker log will be `integration_tests/target/run_dir-<timestamp>-xxxx/gw0_worker_logs.log` as if executed by
worker 0 under xdist.

### Enabling cudf_udf Tests

The cudf_udf tests in this framework are testing Pandas UDF(user-defined function) with cuDF. They are disabled by default not only because of the complicated environment setup, but also because GPU resources scheduling for Pandas UDF is an experimental feature now, the performance may not always be better.
The tests can be enabled by just appending the option `--cudf_udf` to the command.

   * `--cudf_udf` (enable the cudf_udf tests when provided, and remove this option if you want to disable the tests)

cudf_udf tests needs a couple of different settings, they may need to run separately.

To enable cudf_udf tests, need following pre requirements:
   * Install cuDF Python library on all the nodes running executors. The instruction could be found at [here](https://rapids.ai/start.html). Please follow the steps to choose the version based on your environment and install the cuDF library via Conda or use other ways like building from source.
   * Disable the GPU exclusive mode on all the nodes running executors. The sample command is `sudo nvidia-smi -c DEFAULT`

To run cudf_udf tests, need following configuration changes:
   * Add configurations `--py-files` and `spark.executorEnv.PYTHONPATH` to specify the plugin jar for python modules 'rapids/daemon' 'rapids/worker'.
   * Decrease `spark.rapids.memory.gpu.allocFraction` to reserve enough GPU memory for Python processes in case of out-of-memory.
   * Add `spark.rapids.python.concurrentPythonWorkers` and `spark.rapids.python.memory.gpu.allocFraction` to reserve enough GPU memory for Python processes in case of out-of-memory.

As an example, here is the `spark-submit` command with the cudf_udf parameter on CUDA 12:

```
$SPARK_HOME/bin/spark-submit --jars "rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar,rapids-4-spark-tests_2.12-26.08.0-SNAPSHOT.jar" --conf spark.rapids.memory.gpu.allocFraction=0.3 --conf spark.rapids.python.memory.gpu.allocFraction=0.3 --conf spark.rapids.python.concurrentPythonWorkers=2 --py-files "rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar" --conf spark.executorEnv.PYTHONPATH="rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar" ./runtests.py --cudf_udf
```

### Enabling fuzz tests

Fuzz tests are intended to find more corner cases in testing. We disable them by default because they might randomly fail.
The tests can be enabled by appending the option `--fuzz_test` to the command.

   * `--fuzz_test` (enable the fuzz tests when provided, and remove this option if you want to disable the tests)

To reproduce an error appearing in the fuzz tests, you also need to add the flag `--debug_tmp_path` to save the test data.

### Enabling Apache Iceberg tests

Some tests require that Apache Iceberg has been configured in the Spark environment and cannot run
properly without it. These tests assume Iceberg is not configured and are disabled by default.
If Spark has been configured to support Iceberg then these tests can be enabled by adding the
`--iceberg` option to the command.

When testing Iceberg package-private access paths, load the local Iceberg runtime jar with
`ICEBERG_EXTRA_CLASSPATH` instead of `PYSP_TEST_spark_jars` or
`PYSP_TEST_spark_jars_packages`. The test driver will place the RAPIDS, test, and Iceberg
jars on `spark.driver.extraClassPath` and `spark.executor.extraClassPath`:

```shell
ICEBERG_EXTRA_CLASSPATH=/path/to/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
PYSP_TEST_spark_sql_extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
PYSP_TEST_spark_sql_catalog_spark__catalog=org.apache.iceberg.spark.SparkSessionCatalog \
PYSP_TEST_spark_sql_catalog_spark__catalog_type=hadoop \
PYSP_TEST_spark_sql_catalog_spark__catalog_warehouse=/tmp/spark-warehouse-$RANDOM \
./integration_tests/run_pyspark_from_build.sh -m iceberg --iceberg
```

#### Disabling Iceberg fanout writer

The Iceberg fanout writer holds all partition writers open simultaneously, which can cause
executor OOM when writing to tables with many partitions (e.g., bucket or truncate transforms).
To avoid this, the CI sets the catalog-level table default to disable fanout:

```
spark.sql.catalog.spark_catalog.table-default.write.spark.fanout.enabled=false
```

In the `PYSP_TEST_` env var format used by the test scripts:

```shell
"PYSP_TEST_spark_sql_catalog_spark__catalog_table-default_write_spark_fanout_enabled=false"
```

With fanout disabled, Iceberg uses the clustered writer which writes one partition at a time
and releases memory between partitions. Dedicated fanout-enabled test cases
(e.g., `test_*_fanout_enabled`) still exercise the fanout writer path with a single
partition type to keep memory usage manageable.

#### Iceberg REST catalog write compression

Some REST catalog deployments can apply a catalog-side default Parquet compression codec that
is not supported by the RAPIDS GPU writer. The REST catalog CI sets table defaults for data
and delete files to use `zstd`, which is supported by the GPU writer:

```shell
"PYSP_TEST_spark_sql_catalog_spark__catalog_table-default_write_parquet_compression-codec=zstd"
"PYSP_TEST_spark_sql_catalog_spark__catalog_table-default_write_delete_parquet_compression-codec=zstd"
```

### Run Apache iceberg s3tables tests

To run iceberg tests against aws s3tables catalog, we need to setup several things:
1. Run `aws configure` to setup aws credentials and region.
2. Create a s3tables [table bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-create.html), and fetch its arn
3. Create a namespace with name `default` under the table bucket created in step 2.
4. Add environment `ICEBERG_TEST_REMOTE_CATALOG=1`
5. Set spark catalog implementation s3 tables: 
   `--conf spark.sql.catalog.spark_catalog.catalog-impl="software.amazon.s3tables.iceberg.S3TablesCatalog"`
6. Set spark warehouse to table bucket arn in step 2: `--conf spark.sql.catalog.spark_catalog.warehouse=<table bucket arn>`

### Enabling Delta Lake tests

Some tests require that Delta Lake has been configured in the Spark environment and cannot run
properly without it. These tests assume Delta Lake is not configured and are disabled by default.
If Spark has been configured to support Delta Lake then these tests can be enabled by adding the
`--delta_lake` option to the command.

### Enabling large data tests
Some tests are testing large data which will take a long time. By default, these tests are disabled.
These tests can be enabled by adding the `--large_data_test` option to the command.

### Enabling Pyarrow tests
Some tests require that Pyarrow is installed. By default, these tests are disabled.
These tests can be enabled by adding the `--pyarrow_test` option to the command.

## Writing tests

There are a number of libraries provided to help someone write new tests.

### `data_gen.py`

`data_gen` allow you to generate data for various spark types. There tends to be a `${Type}Gen` class for every `${Type}Type` class supported by the plugin.  Each of these
has decent default values, so a `DoubleGen` should produce a random set of `DoubleType` including most corner cases like `-0.0`, `NaN`, `Inf` and `-Inf`.

Many of the classes also allow for some customization of the data produced.

All of the classes allow you to add in your own corner cases using `with_special_case`.  So if you want an `IntegerGen` that produces a lot of 5s you could run.

```
IntegerGen().with_special_case(5, weight=200)
```

The value passed in can be a constant value, or it can be a function that takes a `random.Random` instance to generate the data. The `weight` lets you
set a relative priority compared with other data.  By default the randomly distributed data has a weight of 100, and special cases have a weight of 1.

Not everything is documented here and you should look around at the library to see what it supports. Feel free to modify it to add in new options
but be careful because we want to maintain good data coverage.

To generate data from a list of `*Gen` instances or from a single `StructGen` you can use the `gen_df` function.

### `asserts.py` and `marks.py`

`asserts.py` provides 2 APIs that let you run a command and verify that it produced the "same" result on both the CPU and the GPU.

`assert_gpu_and_cpu_are_equal_collect` and `assert_gpu_and_cpu_are_equal_iterator` the difference is in how the results are brought back for
verification. Most tests should use the collect version. If you have a test that will produce a very large amount of data you can use the
iterator version, but it will be much slower.

Each of these take a function as input.  The function will be passed an instance of `spark` that is configured for the given environment
and it should return a data frame.  It also takes a dictionary of config entries that can be set when the test runs.  Most config entries
you care about should be through markers.

pytest uses markers to tag different tests with metadata. This framework uses them to be able to set various configs when running the tests.
Markers were chosen because it provides a lot of flexibility even with parameterized tests.

The marks you care about are all in marks.py

   * `ignore_order` tells the asserts to sort the resulting data because the tests may not produce the results in the same order
   * `incompat` tells the tests to enable incompat operators. It does not enable approximate comparisons for floating point though.
   * `approximate_float` tells the tests to compare floating point values (including double) and allow for an error. This follows `pytest.approx` and will also take `rel` and `abs` args.
   * `allow_non_gpu` tells the tests that not everything in the query will run on the GPU. You can tell it to allow all CPU fallback by `@allow_non_gpu(any=True)` you can also pass in class names that are enabled for CPU operation.

###  `spark_session.py`

For the most part you can ignore this file. It provides the underlying Spark session to operations that need it, but most tests should interact with
it through `asserts.py`.

All data generation and Spark function calls should occur within a Spark session. Typically
this is done by passing a lambda to functions in `asserts.py` such as
`assert_gpu_and_cpu_are_equal_collect`. However, for scalar generation like `gen_scalars`, you
may need to put it in a `with_cpu_session`. It is because negative scale decimals can have
problems when calling `f.lit` from outside of `with_spark_session`.

## Guidelines for Testing

When support for a new operator is added to the Rapids Accelerator for Spark, or when an existing operator is extended
 to support more data types, it is recommended that the following conditions be covered in its corresponding integration tests:

### 1. Cover all supported data types
Ensure that tests cover all data types supported by the added operation. An exhaustive list of data types supported in
Apache Spark is available [here](https://spark.apache.org/docs/latest/sql-ref-datatypes.html). These include:
   * Numeric Types
     * `ByteType`
     * `ShortType`
     * `IntegerType`
     * `LongType`
     * `FloatType`
     * `DoubleType`
     * `DecimalType`
   * Strings
     * `StringType`
     * `VarcharType`
   * Binary (`BinaryType`)
   * Booleans (`BooleanType`)
   * Chrono Types
     * `TimestampType`
     * `DateType`
     * `Interval`
   * Complex Types
     * `ArrayType`
     * `StructType`
     * `MapType`

`data_gen.py` provides `DataGen` classes that help generate test data in integration tests.

The `assert_gpu_and_cpu_are_equal_collect()` function from `asserts.py` may be used to compare that an operator in
the Rapids Accelerator produces the same results as Apache Spark, for a test query.

For data types that are not currently supported for an operator in the Rapids Accelerator,
the `assert_gpu_fallback_collect()` function from `asserts.py` can be used to verify that the query falls back
on the CPU operator from Apache Spark, and produces the right results.

### 2. Nested data types
Complex data types (`ArrayType`, `StructType`, `MapType`) warrant extensive testing for various combinations of nesting.
E.g.
   * `Array<primitive_type>`
   * `Array<Array<primitive_type>>`
   * `Array<Struct<primitive_type>>`
   * `Struct<Array<primitive_type>>`
   * `Array<Struct<Array<primitive_type>>>`
   * `Struct<Array<Struct<primitive_type>>>`

The `ArrayGen` and `StructGen` classes in `data_gen.py` can be configured to support arbitrary nesting.

### 3. Literal (i.e. Scalar) values
Operators and expressions that support literal operands need to be tested with literal inputs, of all
supported types from 1 and 2, above.
For instance, `SUM()` supports numeric columns (e.g. `SUM(a + b)`), or scalars (e.g. `SUM(20)`).
Similarly, `COUNT()` supports the following:
   * Columns: E.g. `COUNT(a)` to count non-null rows for column `a`
   * Scalars: E.g. `COUNT(1)` to count all rows (including nulls)
   * `*`: E.g. `COUNT(*)`, functionally equivalent to `COUNT(1)`
It is advised that tests be added for all applicable literal types, for an operator.

Note that for most operations, if all inputs are literal values, the Spark Catalyst optimizer will evaluate
the expression during the logical planning phase of query compilation, via
[Constant Folding](https://books.japila.pl/spark-sql-internals/logical-optimizations/ConstantFolding/)
E.g. Consider this query:
```sql
SELECT SUM(1+2+3) FROM ...
```
The expression `1+2+3` will not be visible to the Rapids Accelerator for Apache Spark, because it will be evaluated
at query compile time, before the Rapids Accelerator is invoked. Thus, adding multiple combinations of literal inputs
need not necessarily add more test coverage.

### 4. Null values
Ensure that the test data accommodates null values for input columns. This includes null values in columns
and in literal inputs.

Null values in input columns are a frequent source of bugs in the Rapids Accelerator for Spark,
because of mismatches in null-handling and semantics, between RAPIDS `libcudf` (on which
the Rapids Accelerator relies heavily), and Apache Spark.

Tests for aggregations (including group-by, reductions, and window aggregations) should cover cases where
some rows are null, and where *all* input rows are null.

Apart from null rows in columns of primitive types, the following conditions must be covered for nested types:

   * Null rows at the "top" level for `Array`/`Struct` columns.   E.g. `[ [1,2], [3], ∅, [4,5,6] ]`.
   * Non-null rows containing null elements in the child column. E.g. `[ [1,2], [3,∅], ∅, [4,∅,6] ]`.
   * All null rows at a nested level. E.g.
     * All null list rows: `[ ∅, ∅, ∅, ∅ ]`
     * All null elements within list rows: `[ [∅,∅], [∅,∅], [∅,∅], [∅,∅] ]`

The `DataGen` classes in `integration_tests/src/main/python/data_gen.py` can be configured to generate null values
for the cases mentioned above.

### 5. Empty rows in `Array` columns
Operations on `ArrayType` columns must be tested with input columns containing non-null *empty* rows.
E.g.
```
[
    [0,1,2,3],
    [], <------- Empty, non-null row.
    [4,5,6,7],
    ...
]
```
Using the `ArrayGen` data generator in `integration_tests/src/main/python/data_gen.py` will generate
empty rows as mentioned above.

### 6. Degenerate cases with "empty" inputs
Ensure that operations are tested with "empty" input columns (i.e. containing zero rows.)

E.g. `COUNT()` on an empty input column yields `0`. `SUM()` yields `0` for the appropriate numeric type.

### 7. Special floating point values
Apart from `null` values, `FloatType` and `DoubleType` input columns must also include the following special values:
   * +/- Zero
   * +/- Infinity
   * +/- NaN

Note that the special values for floating point numbers might have different bit representations for the same
equivalent values. The [Java documentation for longBitsToDouble()](https://docs.oracle.com/javase/8/docs/api/java/lang/Double.html#longBitsToDouble-long-)
describes this with examples. Operations should be tested with multiple bit-representations for these special values.

The `FloatGen` and `DoubleGen` data generators in `integration_tests/src/main/python/data_gen.py` can be configured
to generate the special float/double values mentioned above.

For most basic floating-point operations like addition, subtraction, multiplication, and division the plugin will
produce a bit for bit identical result as Spark does. For some other functions (like `sin`, `cos`, etc.), the output may
differ slightly, but remain within the rounding error inherent in floating-point calculations. Certain aggregations
might compound those differences. In those cases, the `@approximate_float` test annotation may be used to mark tests
to use "approximate" comparisons for floating-point values.

Refer to the "Floating Point" section of [compatibility.md](../docs/compatibility.md) for details.

### 8. Special values in timestamp columns
Ensure date/timestamp columns include dates before the [epoch](https://en.wikipedia.org/wiki/Epoch_(computing)).

Apache Spark supports dates/timestamps between `0001-01-01 00:00:00.000000` and `9999-12-31 23:59:59.999999`, but at
values close to the minimum value, the format used in Apache Spark causes rounding errors. To avoid such problems,
it is recommended that the minimum value used in a test not actually equal `0001-01-01`. For instance, `0001-01-03` is
acceptable.

It is advised that `DateGen` and `TimestampGen` classes from `data_gen.py` be used to generate valid
(proleptic Gregorian calendar) dates when testing operators that work on dates. This data generator respects
the valid boundaries for dates and timestamps.

## Scale Test

Scale Test is a test suite to do stress test and estimate the stablity of the spark-rapids plugin when running in large
scale data. For more information please refer to [Scale Test](./ScaleTest.md)
