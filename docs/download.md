---
layout: page
title: Download
nav_order: 3
---

[NVIDIA cuDF plugin for Apache Spark](https://github.com/NVIDIA/cudf-spark) provides a set of
plugins for Apache Spark that leverage GPUs to accelerate Dataframe and SQL processing.

The accelerator is built upon the [cuDF project](https://github.com/rapidsai/cudf) and
[UCX](https://github.com/openucx/ucx/).

The cuDF plugin requires each worker node in the cluster to have an NVIDIA GPU and the [NVIDIA
driver](https://www.nvidia.com/en-us/drivers/) installed.

The cuDF plugin consists of the rapids-4-spark plugin jar.  The jar is either preinstalled in the Spark
classpath on all nodes or submitted with each job that uses the cuDF plugin. See the
[getting-started
guide](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html) for
more details.

Note: The NVIDIA cuDF plugin for Apache Spark was formerly known as the RAPIDS Accelerator for Apache Spark.  The RAPIDS name will be sunset over time.  Github links from
`spark-rapids` will redirect to `cudf-spark`.  Artifact names will remain the same for now.

## Release v26.08.1
### Hardware Requirements:

The plugin is designed to work on NVIDIA Volta, Turing, Ampere, Ada Lovelace, Hopper and Blackwell generation datacenter GPUs.  The plugin jar is tested on the following GPUs:

	GPU Models: NVIDIA V100, T4, A10, A100, L4, H100 and B100 GPUs

### Software Requirements:

    OS: The cuDF plugin is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018.
    Tested on Ubuntu 22.04, Ubuntu 24.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R525+

	Runtime:
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version.

		* Check the Spark documentation for Python and Java version compatibility with your specific
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6, 3.5.7, 3.5.8, 3.5.9
		Apache Spark 4.0.0, 4.0.1, 4.0.2, 4.0.3, 4.0.4
		Apache Spark 4.1.1, 4.1.2, 4.1.3
		Apache Spark 4.2.0
		Scala 2.12: Spark 3.3.0 through 3.5.9
		Scala 2.13: Spark 3.5.0 through 3.5.9, and Spark 4.0.0 through 4.0.4, Spark 4.1.1 through 4.1.3, and Spark 4.2.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
		Databricks 17.3 ML LTS (GPU, Scala 2.13, Spark 4.0.0)

	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.2 LTS
		Spark runtime 2.2 LTS
		Spark runtime 2.3 LTS
		Spark runtime 3.0

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Databricks Support

#### Runtime Compatibility

Use the JDK provided by the Databricks runtime.

| Databricks Runtime | Apache Spark | Scala | JDK runtime | CUDA jar variants | Minimum NVIDIA driver |
|---------------------|--------------|-------|-------------|-------------------|-----------------------|
| 14.3 ML LTS GPU | 3.5.0 | 2.12 | Databricks runtime default | CUDA 12, CUDA 13 | R525+ |
| 17.3 ML LTS GPU | 4.0.0 | 2.13 | Databricks runtime default | CUDA 12, CUDA 13 | R525+ |

Use the Scala artifact that matches the runtime's Spark/Scala line. The CUDA
classifier selects the bundled cuDF native libraries.

#### Delta Lake GPU Support on Databricks

| Delta feature | DBR 14.3 | DBR 17.3 |
|---------------|----------|----------|
| Reads without deletion vectors | GPU | GPU |
| Deletion vector reads | CPU fallback | GPU with metadata row index and cuDF plugin deletion-vector predicate pushdown |
| Delta writes | GPU for append, overwrite, CTAS, and RTAS | GPU for append, overwrite, CTAS, and RTAS |
| Delta writes with deletion vectors | CPU fallback | CPU fallback for paths that create persistent deletion vectors |
| DELETE and UPDATE | GPU for copy-on-write. Operations that write deletion vectors fall back to CPU. | GPU for copy-on-write, including liquid-clustered tables. Operations that write persistent deletion vectors fall back to CPU. |
| MERGE | GPU, including liquid clustering | GPU, including liquid clustering. Persistent deletion-vector writes fall back to CPU. |
| OPTIMIZE | CPU fallback | GPU for supported standard and ordinary liquid-clustering paths |
| Auto compaction | GPU when triggered by supported GPU writes | GPU for supported inline, deletion-vector-free paths |
| Liquid clustering | GPU | GPU for writes, DELETE, UPDATE, MERGE, and ordinary OPTIMIZE |

DBR 17.3 supports GPU data-file writes for qualified liquid-clustering,
CTAS, and RTAS paths while retaining Databricks-native planning and commit
semantics. See [#15278](https://github.com/NVIDIA/cudf-spark/pull/15278) and
[#15320](https://github.com/NVIDIA/cudf-spark/pull/15320) for details and
expected fallback cases.

Databricks may patch existing runtime versions without changing the public
runtime line. If a binary compatibility error such as `NoSuchMethodError`
occurs, verify the cuDF plugin and Databricks runtime combination against this
page and the release notes.

Support is operation-specific; use `spark.rapids.sql.explain=NOT_ON_GPU` to
identify CPU fallback in a query plan.

### cuDF Plugin Support Policy for Apache Spark
The cuDF plugin maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download the NVIDIA cuDF plugin for Apache Spark v26.08.1

#### CUDA 12

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [cuDF plugin v26.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.08.1/rapids-4-spark_2.12-26.08.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.08.1/rapids-4-spark_2.12-26.08.1.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.08.1&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [cuDF plugin v26.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.08.1/rapids-4-spark_2.13-26.08.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.08.1/rapids-4-spark_2.13-26.08.1.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.08.1&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [cuDF plugin v26.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.08.1/rapids-4-spark_2.12-26.08.1-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.08.1/rapids-4-spark_2.12-26.08.1-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.08.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [cuDF plugin v26.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.08.1/rapids-4-spark_2.13-26.08.1-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.08.1/rapids-4-spark_2.13-26.08.1-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.08.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


#### CUDA 13

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [cuDF plugin v26.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.08.1/rapids-4-spark_2.12-26.08.1-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.08.1/rapids-4-spark_2.12-26.08.1-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.08.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [cuDF plugin v26.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.08.1/rapids-4-spark_2.13-26.08.1-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.08.1/rapids-4-spark_2.13-26.08.1-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.08.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [cuDF plugin v26.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.08.1/rapids-4-spark_2.12-26.08.1-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.08.1/rapids-4-spark_2.12-26.08.1-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.08.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [cuDF plugin v26.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.08.1/rapids-4-spark_2.13-26.08.1-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.08.1/rapids-4-spark_2.13-26.08.1-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.08.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


The above packages are built against CUDA 12.9 or CUDA 13.1. They are tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.08.1.jar.asc rapids-4-spark_2.12-26.08.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.08.1.jar.asc rapids-4-spark_2.13-26.08.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
v26.08.1 is a hotfix release on top of v26.08.0 with the following fix:
* Fixed an executor-wide class-initialization deadlock between `GpuOverrides` and `SparkShimImpl` that could permanently block task slots ([#15752](https://github.com/NVIDIA/cudf-spark/pull/15752))

The full v26.08.0 release notes are also included below.

v26.08.0 includes the following updates:
* Added support for Apache Spark 3.5.9, 4.0.3, 4.0.4, 4.1.2, 4.1.3, and 4.2.0; Databricks 13.3 ML LTS is no longer supported ([#15286](https://github.com/NVIDIA/cudf-spark/pull/15286), [#15151](https://github.com/NVIDIA/cudf-spark/pull/15151), [#15313](https://github.com/NVIDIA/cudf-spark/pull/15313), [#15071](https://github.com/NVIDIA/cudf-spark/pull/15071), [#15310](https://github.com/NVIDIA/cudf-spark/pull/15310), [#15279](https://github.com/NVIDIA/cudf-spark/pull/15279), [#15276](https://github.com/NVIDIA/cudf-spark/pull/15276))
* Added full CPU/GPU bridge support and re-enabled the accelerated columnar-to-row path, allowing unsupported portions of a query to fall back while surrounding operators remain accelerated ([#14132](https://github.com/NVIDIA/cudf-spark/pull/14132), [#14651](https://github.com/NVIDIA/cudf-spark/pull/14651))
* Added Iceberg 1.11 support for Spark 4.x, accelerated incremental append scans, optimized S3 Parquet footer reads, and exposed Parquet writer dictionary controls ([#14881](https://github.com/NVIDIA/cudf-spark/pull/14881), [#14882](https://github.com/NVIDIA/cudf-spark/pull/14882), [#14883](https://github.com/NVIDIA/cudf-spark/pull/14883), [#15174](https://github.com/NVIDIA/cudf-spark/pull/15174), [#15384](https://github.com/NVIDIA/cudf-spark/pull/15384), [#14878](https://github.com/NVIDIA/cudf-spark/pull/14878))
* Expanded GPU SQL support with default-comparator `array_sort`, array and map inputs for `array_aggregate`, nested map/array output for `from_json`, multiple ORDER BY columns in RANGE windows, and additional regex compatibility fixes ([#15108](https://github.com/NVIDIA/cudf-spark/pull/15108), [#15149](https://github.com/NVIDIA/cudf-spark/pull/15149), [#15134](https://github.com/NVIDIA/cudf-spark/pull/15134), [#14961](https://github.com/NVIDIA/cudf-spark/pull/14961), [#14862](https://github.com/NVIDIA/cudf-spark/pull/14862))
* Expanded Delta Lake support on Databricks 17.3 with liquid clustering and CTAS/RTAS, and improved OSS Delta deletion-vector reads and GPU MERGE commit summaries ([#15278](https://github.com/NVIDIA/cudf-spark/pull/15278), [#15320](https://github.com/NVIDIA/cudf-spark/pull/15320), [#15368](https://github.com/NVIDIA/cudf-spark/pull/15368), [#15429](https://github.com/NVIDIA/cudf-spark/pull/15429))
* Improved shuffle, spill, and memory reliability by auto-configuring the shuffle manager, preventing multithreaded shuffle-merger and RMM pool-wait deadlocks, and hardening handling of batches and spill files larger than 2 GiB ([#15285](https://github.com/NVIDIA/cudf-spark/pull/15285), [#15208](https://github.com/NVIDIA/cudf-spark/pull/15208), [#15104](https://github.com/NVIDIA/cudf-spark/pull/15104), [#14967](https://github.com/NVIDIA/cudf-spark/pull/14967), [#15327](https://github.com/NVIDIA/cudf-spark/pull/15327))
* Improved SQL UI and History Server visibility for GPU V2 writes and corrected operator-time accounting around file writes and shuffle reads ([#14975](https://github.com/NVIDIA/cudf-spark/pull/14975), [#15089](https://github.com/NVIDIA/cudf-spark/pull/15089), [#14901](https://github.com/NVIDIA/cudf-spark/pull/14901), [#14936](https://github.com/NVIDIA/cudf-spark/pull/14936))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/cudf-spark/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md).
