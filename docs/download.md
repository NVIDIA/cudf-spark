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

## Release v26.10.0
### Hardware Requirements:

The plugin is designed to work on NVIDIA Volta, Turing, Ampere, Ada Lovelace, Hopper and Blackwell generation GPUs.  The plugin jar is tested on the following GPUs:

	GPU Models: NVIDIA V100, T4, A10, A100, L4, H100, B100, RTX PRO 4500 and RTX PRO 6000 GPUs

### Software Requirements:

    OS: The cuDF plugin is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018.
    Tested on Ubuntu 22.04, Ubuntu 24.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*:
		CUDA 12: R525+
		CUDA 13: R580+

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

*These minimum driver versions follow the
[NVIDIA CUDA Compatibility documentation](https://docs.nvidia.com/deploy/cuda-compatibility/minor-version-compatibility.html).
Some hardware may require a newer driver; check the GPU spec sheet for your hardware's minimum
driver version.

*For EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Databricks Support

#### Runtime Compatibility

Use the JDK provided by the Databricks runtime.

| Databricks Runtime | Apache Spark | Scala | JDK runtime | CUDA jar variants | Minimum NVIDIA driver |
|---------------------|--------------|-------|-------------|-------------------|-----------------------|
| 14.3 ML LTS GPU | 3.5.0 | 2.12 | Databricks runtime default | CUDA 12, CUDA 13 | CUDA 12: R525+; CUDA 13: R580+ |
| 17.3 ML LTS GPU | 4.0.0 | 2.13 | Databricks runtime default | CUDA 12, CUDA 13 | CUDA 12: R525+; CUDA 13: R580+ |

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
| OPTIMIZE | GPU for supported native non-clustered write paths; the outer command remains on CPU | GPU for supported native non-clustered and ordinary liquid-clustering write paths; the outer command remains on CPU |
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

### Download the NVIDIA cuDF plugin for Apache Spark v26.10.0

#### CUDA 12

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [cuDF plugin v26.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.10.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [cuDF plugin v26.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/rapids-4-spark_2.13-26.10.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/rapids-4-spark_2.13-26.10.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.10.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [cuDF plugin v26.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.10.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [cuDF plugin v26.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/rapids-4-spark_2.13-26.10.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/rapids-4-spark_2.13-26.10.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.10.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


#### CUDA 13

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [cuDF plugin v26.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.10.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [cuDF plugin v26.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/rapids-4-spark_2.13-26.10.0-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/rapids-4-spark_2.13-26.10.0-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.10.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [cuDF plugin v26.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.10.0/rapids-4-spark_2.12-26.10.0-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.10.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [cuDF plugin v26.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/rapids-4-spark_2.13-26.10.0-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.10.0/rapids-4-spark_2.13-26.10.0-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.10.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


The above packages are built against CUDA 12.9 or CUDA 13.1. They are tested on V100, T4, A10, A100, L4, H100, GB100, RTX PRO 4500 and RTX PRO 6000 GPUs.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.10.0.jar.asc rapids-4-spark_2.12-26.10.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.10.0.jar.asc rapids-4-spark_2.13-26.10.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
v26.10.0 includes the following updates:
* Added GPU acceleration for Spark Variant extraction on Apache Spark 4.x and Databricks 17.3, including nested object and array paths and Boolean, integer, floating-point, and string targets ([#15644](https://github.com/NVIDIA/cudf-spark/pull/15644), [#15645](https://github.com/NVIDIA/cudf-spark/pull/15645), [#16020](https://github.com/NVIDIA/cudf-spark/pull/16020), [#16021](https://github.com/NVIDIA/cudf-spark/pull/16021))
* Expanded Delta Lake support with basic Delta 4.2 and 4.3 compatibility, Change Data Feed reads, GPU-accelerated `REORG TABLE ... APPLY (PURGE)`, and DML writes to deletion-vector-enabled tables ([#15796](https://github.com/NVIDIA/cudf-spark/pull/15796), [#15992](https://github.com/NVIDIA/cudf-spark/pull/15992), [#15790](https://github.com/NVIDIA/cudf-spark/pull/15790), [#15500](https://github.com/NVIDIA/cudf-spark/pull/15500), [#15869](https://github.com/NVIDIA/cudf-spark/pull/15869))
* Expanded Databricks 17.3 support with AutoOptimizedShuffle compatibility, native Delta OPTIMIZE writes, and `NOT MATCHED BY SOURCE` for GPU MERGE ([#15818](https://github.com/NVIDIA/cudf-spark/pull/15818), [#15942](https://github.com/NVIDIA/cudf-spark/pull/15942), [#15884](https://github.com/NVIDIA/cudf-spark/pull/15884))
* Added GPU support for Iceberg v3 deletion-vector reads and writes and row-lineage metadata reads ([#15633](https://github.com/NVIDIA/cudf-spark/pull/15633), [#15717](https://github.com/NVIDIA/cudf-spark/pull/15717), [#15579](https://github.com/NVIDIA/cudf-spark/pull/15579))
* Expanded GPU SQL support with `NTILE`, column patterns in `LIKE`, dynamic expressions in `IN`, case-insensitive inline and scoped regex flags, and `count` on ANSI interval columns ([#15417](https://github.com/NVIDIA/cudf-spark/pull/15417), [#15825](https://github.com/NVIDIA/cudf-spark/pull/15825), [#15710](https://github.com/NVIDIA/cudf-spark/pull/15710), [#15484](https://github.com/NVIDIA/cudf-spark/pull/15484), [#15938](https://github.com/NVIDIA/cudf-spark/pull/15938))
* Added GPU support for `GroupPartitionsExec`, including sorted external-merge execution, and improved broadcast hash join performance by reusing build-side hash tables ([#15604](https://github.com/NVIDIA/cudf-spark/pull/15604), [#15704](https://github.com/NVIDIA/cudf-spark/pull/15704), [#15810](https://github.com/NVIDIA/cudf-spark/pull/15810), [#14680](https://github.com/NVIDIA/cudf-spark/pull/14680))
* Expanded accelerated cloud I/O and observability with PerfIO ORC reads on S3 and GCS, automatic PerfIO enablement for GCS, decoded GPU batch-byte scan metrics, and Arrow Python output metrics ([#15324](https://github.com/NVIDIA/cudf-spark/pull/15324), [#15130](https://github.com/NVIDIA/cudf-spark/pull/15130), [#15584](https://github.com/NVIDIA/cudf-spark/pull/15584), [#15811](https://github.com/NVIDIA/cudf-spark/pull/15811))
* Improved shuffle and execution reliability by upgrading UCX to 1.22, retrying UCX sends after GPU OOM, hardening skip-merge cleanup, and fixing a nested-subquery GPU broadcast deadlock ([#15628](https://github.com/NVIDIA/cudf-spark/pull/15628), [#15982](https://github.com/NVIDIA/cudf-spark/pull/15982), [#15064](https://github.com/NVIDIA/cudf-spark/pull/15064), [#16019](https://github.com/NVIDIA/cudf-spark/pull/16019), [#16034](https://github.com/NVIDIA/cudf-spark/pull/16034))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/cudf-spark/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md).
