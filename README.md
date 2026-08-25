# NVIDIA cuDF plugin for Apache Spark
NOTE: For the latest stable [README.md](https://github.com/nvidia/cudf-spark/blob/main/README.md) ensure you are on the main branch.

The NVIDIA cuDF plugin for [Apache Spark](https://spark.apache.org) provides a plugin library that
leverages GPUs to accelerate processing via the [cuDF](https://github.com/rapidsai/cudf) (CUDA
DataFrame) libraries.

Documentation on the current release can be found [here](https://nvidia.github.io/cudf-spark/).

To get started and try the plugin out use the [getting started guide](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html).

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/NVIDIA/cudf-spark)

Note: The NVIDIA cuDF plugin for Apache Spark was formerly known as the RAPIDS Accelerator for
Apache Spark.  The RAPIDS name will be sunset over time.  Github links from `spark-rapids` will
redirect to `cudf-spark`.  Artifact names will remain the same for now.

## Compatibility

The SQL plugin tries to produce results that are bit for bit identical with Apache Spark.
Operator compatibility is documented [here](./docs/compatibility.md)

## Tuning

To get started tuning your job and get the most performance out of it please start with the
[tuning guide](https://docs.nvidia.com/spark-rapids/user-guide/latest/tuning-guide.html).

## Configuration

The plugin has a set of Spark configs that control its behavior and are documented
[here](docs/configs.md).

## Issues & Questions

We use github to track bugs, feature requests, and answer questions. File an
[issue](https://github.com/NVIDIA/cudf-spark/issues/new/choose) for a bug or feature request. Ask
or answer a question on the [discussion board](https://github.com/NVIDIA/cudf-spark/discussions).

## Download

The jar files for the most recent release can be retrieved from the [download](https://nvidia.github.io/cudf-spark/docs/download.html)
page.

## Building From Source

See the [build instructions in the contributing guide](CONTRIBUTING.md#building-from-source).

## Testing

Tests are described [here](tests/README.md).

## Integration
The cuDF plugin provides some APIs for doing zero copy data transfer into other GPU enabled
applications.  It is described
[here](https://docs.nvidia.com/spark-rapids/user-guide/latest/additional-functionality/ml-integration.html).

Currently, we are working with XGBoost to try to provide this integration out of the box.

You may need to disable RMM caching when exporting data to an ML library as that library
will likely want to use all of the GPU's memory and if it is not aware of RMM it will not have
access to any of the memory that RMM is holding.

## Qualification and Profiling tools

The Qualification and Profiling tools have been moved to
[nvidia/spark-rapids-tools](https://github.com/NVIDIA/spark-rapids-tools) repo.

Please refer to [Qualification tool documentation](https://docs.nvidia.com/spark-rapids/user-guide/latest/qualification/overview.html)
and [Profiling tool documentation](https://docs.nvidia.com/spark-rapids/user-guide/latest/profiling/overview.html)
for more details on how to use the tools.

## Dependency for External Projects

If you develop functionality on top of the cuDF plugin (we currently limit support to
GPU-accelerated UDFs) we recommend you declare the distribution artifact as a `provided` dependency.

```xml
<dependency>
    <groupId>com.nvidia</groupId>
    <artifactId>rapids-4-spark_2.12</artifactId>
    <version>26.08.1-SNAPSHOT</version>
    <scope>provided</scope>
</dependency>
```
