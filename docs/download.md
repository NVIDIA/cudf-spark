---
layout: page
title: Download
nav_order: 3
---

The [RAPIDS Accelerator for Apache Spark](https://github.com/NVIDIA/spark-rapids) provides a set of
plugins for Apache Spark that leverage GPUs to accelerate Dataframe and SQL processing.

The accelerator is built upon the [RAPIDS cuDF project](https://github.com/rapidsai/cudf) and
[UCX](https://github.com/openucx/ucx/).

RAPIDS Spark requires each worker node in the cluster to have an NVIDIA GPU and the [NVIDIA
driver](https://www.nvidia.com/en-us/drivers/) installed.

RAPIDS Spark consists of the rapids-4-spark plugin jar.  The jar is either preinstalled in the Spark
classpath on all nodes or submitted with each job that uses the RAPIDS Spark. See the
[getting-started
guide](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html) for
more details.

## Release v26.06.1
### Hardware Requirements:

The plugin is designed to work on NVIDIA Volta, Turing, Ampere, Ada Lovelace, Hopper and Blackwell generation datacenter GPUs.  The plugin jar is tested on the following GPUs:

	GPU Models: NVIDIA V100, T4, A10, A100, L4, H100 and B100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
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
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6, 3.5.7, 3.5.8
		Apache Spark 4.0.0, 4.0.1, 4.0.2, 4.0.3, 4.0.4
		Apache Spark 4.1.1
		Scala 2.12: Spark 3.3.0 through 3.5.8
		Scala 2.13: Spark 3.5.0 through 3.5.8, and Spark 4.0.0, 4.0.1, 4.0.2, 4.0.3, 4.0.4, and 4.1.1
	
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

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v26.06.1

#### CUDA 12

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.06.1/rapids-4-spark_2.12-26.06.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.06.1/rapids-4-spark_2.12-26.06.1.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.06.1&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.06.1/rapids-4-spark_2.13-26.06.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.06.1/rapids-4-spark_2.13-26.06.1.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.06.1&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.06.1/rapids-4-spark_2.12-26.06.1-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.06.1/rapids-4-spark_2.12-26.06.1-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.06.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.06.1/rapids-4-spark_2.13-26.06.1-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.06.1/rapids-4-spark_2.13-26.06.1-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.06.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


#### CUDA 13

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.06.1/rapids-4-spark_2.12-26.06.1-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.06.1/rapids-4-spark_2.12-26.06.1-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.06.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.06.1/rapids-4-spark_2.13-26.06.1-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.06.1/rapids-4-spark_2.13-26.06.1-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.06.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.06.1/rapids-4-spark_2.12-26.06.1-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.06.1/rapids-4-spark_2.12-26.06.1-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.06.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.06.1/rapids-4-spark_2.13-26.06.1-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.06.1/rapids-4-spark_2.13-26.06.1-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.06.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


The above packages are built against CUDA 12.9 or CUDA 13.1. They are tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.06.1.jar.asc rapids-4-spark_2.12-26.06.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.06.1.jar.asc rapids-4-spark_2.13-26.06.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
v26.06.1 is a hotfix release on top of v26.06.0 with the following fixes:
* Use the configured `parquet.read.allocation.size` copy buffer for Hadoop vectored reads, restoring the 8 MiB default for S3A, GCS, and other Hadoop-backed filesystem fallbacks ([#15164](https://github.com/NVIDIA/cudf-spark/pull/15164))
* Restore Databricks 17.3 build compatibility after the `SessionCatalog` partition-listing API change ([#15211](https://github.com/NVIDIA/cudf-spark/pull/15211))

The full v26.06.0 release notes are also included below.

v26.06.0 includes the following updates:
* Databricks 17.3 ML LTS Delta Lake support now includes native deletion vector reads, Delta writes, DELETE, UPDATE, MERGE, OPTIMIZE, and auto compaction on GPU ([#14787](https://github.com/NVIDIA/spark-rapids/pull/14787), [#14716](https://github.com/NVIDIA/spark-rapids/pull/14716), [#14810](https://github.com/NVIDIA/spark-rapids/pull/14810), [#14820](https://github.com/NVIDIA/spark-rapids/pull/14820), [#14847](https://github.com/NVIDIA/spark-rapids/pull/14847))
* Iceberg support adds nested and binary GPU writes, read-path optimizations, per-table scan-option overrides, and fixes for newly-added nested MAP/LIST fields ([#14611](https://github.com/NVIDIA/spark-rapids/pull/14611), [#14674](https://github.com/NVIDIA/spark-rapids/pull/14674), [#14754](https://github.com/NVIDIA/spark-rapids/pull/14754), [#14880](https://github.com/NVIDIA/spark-rapids/pull/14880))
* Added GPU support for array aggregate SUM, PRODUCT, MAX, MIN, ALL, and ANY, plus additional string expression support for `replace(col, targetExpr, replExpr)` and GBK `StringDecode` ([#14652](https://github.com/NVIDIA/spark-rapids/pull/14652), [#14623](https://github.com/NVIDIA/spark-rapids/pull/14623), [#14545](https://github.com/NVIDIA/spark-rapids/pull/14545))
* Improved expression performance for timestamp parsing, complex type casts, null handling, `hypot`, `format_number`, regex extract, and substring operations ([#14706](https://github.com/NVIDIA/spark-rapids/pull/14706), [#14842](https://github.com/NVIDIA/spark-rapids/pull/14842), [#14817](https://github.com/NVIDIA/spark-rapids/pull/14817), [#14818](https://github.com/NVIDIA/spark-rapids/pull/14818), [#14830](https://github.com/NVIDIA/spark-rapids/pull/14830), [#14586](https://github.com/NVIDIA/spark-rapids/pull/14586), [#14647](https://github.com/NVIDIA/spark-rapids/pull/14647), [#14819](https://github.com/NVIDIA/spark-rapids/pull/14819))
* Improved retry and resource handling for GPU project execution, row-to-column transitions, asynchronous cloud output writes, and host column extraction ([#14724](https://github.com/NVIDIA/spark-rapids/pull/14724), [#14865](https://github.com/NVIDIA/spark-rapids/pull/14865), [#14759](https://github.com/NVIDIA/spark-rapids/pull/14759))
* Fixed several query correctness and compatibility issues, including join conditions with casts, non-deterministic expression preservation, JSON/CSV path decoding, and row transitions for final AQE exchanges ([#14793](https://github.com/NVIDIA/spark-rapids/pull/14793), [#14792](https://github.com/NVIDIA/spark-rapids/pull/14792), [#14778](https://github.com/NVIDIA/spark-rapids/pull/14778), [#14914](https://github.com/NVIDIA/spark-rapids/pull/14914))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Archived releases

As new releases come out, previous ones will still be available in [archived releases](./archive.md).
