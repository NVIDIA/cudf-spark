---
layout: page
title: Archive
nav_order: 15
---
Below are archived releases for RAPIDS Accelerator for Apache Spark.

## Release v26.04.2
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
		Apache Spark 4.0.0, 4.0.1, 4.0.2
		Apache Spark 4.1.1
		Scala 2.12: Spark 3.3.0 through 3.5.8
		Scala 2.13: Spark 3.5.0 through 3.5.8, and Spark 4.0.0, 4.0.1, 4.0.2, and 4.1.1

	Supported Databricks runtime versions for Azure and AWS:
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
		Databricks 17.3 ML LTS (GPU, Scala 2.13, Spark 4.0.0)

	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v26.04.2

#### CUDA 12

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.04.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.2/rapids-4-spark_2.12-26.04.2.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.2/rapids-4-spark_2.12-26.04.2.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.2&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.04.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.2/rapids-4-spark_2.13-26.04.2.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.2/rapids-4-spark_2.13-26.04.2.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.2&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.04.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.2/rapids-4-spark_2.12-26.04.2-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.2/rapids-4-spark_2.12-26.04.2-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.04.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.2/rapids-4-spark_2.13-26.04.2-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.2/rapids-4-spark_2.13-26.04.2-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


#### CUDA 13

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.04.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.2/rapids-4-spark_2.12-26.04.2-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.2/rapids-4-spark_2.12-26.04.2-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.04.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.2/rapids-4-spark_2.13-26.04.2-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.2/rapids-4-spark_2.13-26.04.2-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.04.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.2/rapids-4-spark_2.12-26.04.2-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.2/rapids-4-spark_2.12-26.04.2-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.04.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.2/rapids-4-spark_2.13-26.04.2-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.2/rapids-4-spark_2.13-26.04.2-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


The above packages are built against CUDA 12.9 or CUDA 13.1. They are tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.04.2.jar.asc rapids-4-spark_2.12-26.04.2.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.04.2.jar.asc rapids-4-spark_2.13-26.04.2.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
v26.04.2 is a hotfix release on top of v26.04.1 with the following fixes:
* Fix shuffle bytes double-counting in metrics when shuffle blocks are removed at unregister ([#14714](https://github.com/NVIDIA/spark-rapids/pull/14714))

The full v26.04.1 release notes are also included below.

v26.04.1 is a hotfix release on top of v26.04.0 with the following fixes:
* Fix unregister/remove path for the wrapped shuffle resolver to prevent stale shuffle files from accumulating, which could cause `java.io.IOException: No space left on device` on long running jobs ([#14686](https://github.com/NVIDIA/spark-rapids/pull/14686), [#14683](https://github.com/NVIDIA/spark-rapids/issues/14683))
* Rebuilt against the latest Databricks 17.3 ML LTS runtime to resolve a `NoSuchMethodError` on `CatalogTable.copy` in `GpuCreateDataSourceTableAsSelectCommand` ([#14682](https://github.com/NVIDIA/spark-rapids/issues/14682))

The full v26.04.0 release notes are also included below.

* Databricks 17.3 ML LTS (Spark 4.0) support (Delta Lake fallback to CPU); Databricks 12.2 ML LTS no longer supported ([#14518](https://github.com/NVIDIA/spark-rapids/pull/14518))
* Iceberg 1.10.1 support with Spark 4.0.2 ([#14459](https://github.com/NVIDIA/spark-rapids/pull/14459))
* Bloom filter v2 support on Databricks ([#14406](https://github.com/NVIDIA/spark-rapids/pull/14406))
* Parquet GPU reader: defer resource collection until close ([#14490](https://github.com/NVIDIA/spark-rapids/pull/14490)); row-to-column per-batch retry for OOM recovery ([#14428](https://github.com/NVIDIA/spark-rapids/pull/14428))
* Shuffle and spilling: shuffle fanout OOM fix ([#14525](https://github.com/NVIDIA/spark-rapids/pull/14525)), thread-safe shuffle unregister ([#14513](https://github.com/NVIDIA/spark-rapids/pull/14513)), bounded retry logging for shuffle coalesce ([#14537](https://github.com/NVIDIA/spark-rapids/pull/14537))
* Window execution fixes for mixed ROWS windows ([#14533](https://github.com/NVIDIA/spark-rapids/pull/14533))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v26.04.1
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
		Apache Spark 4.0.0, 4.0.1, 4.0.2
		Apache Spark 4.1.1
		Scala 2.12: Spark 3.3.0 through 3.5.8
		Scala 2.13: Spark 3.5.0 through 3.5.8, and Spark 4.0.0, 4.0.1, 4.0.2, and 4.1.1

	Supported Databricks runtime versions for Azure and AWS:
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
		Databricks 17.3 ML LTS (GPU, Scala 2.13, Spark 4.0.0)

	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v26.04.1

#### CUDA 12

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.1/rapids-4-spark_2.12-26.04.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.1/rapids-4-spark_2.12-26.04.1.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.1&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.1/rapids-4-spark_2.13-26.04.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.1/rapids-4-spark_2.13-26.04.1.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.1&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.1/rapids-4-spark_2.12-26.04.1-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.1/rapids-4-spark_2.12-26.04.1-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.1/rapids-4-spark_2.13-26.04.1-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.1/rapids-4-spark_2.13-26.04.1-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


#### CUDA 13

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.1/rapids-4-spark_2.12-26.04.1-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.1/rapids-4-spark_2.12-26.04.1-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.1/rapids-4-spark_2.13-26.04.1-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.1/rapids-4-spark_2.13-26.04.1-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.1/rapids-4-spark_2.12-26.04.1-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.1/rapids-4-spark_2.12-26.04.1-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.1/rapids-4-spark_2.13-26.04.1-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.1/rapids-4-spark_2.13-26.04.1-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


The above packages are built against CUDA 12.9 or CUDA 13.1. They are tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.04.1.jar.asc rapids-4-spark_2.12-26.04.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.04.1.jar.asc rapids-4-spark_2.13-26.04.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
v26.04.1 is a hotfix release on top of v26.04.0 with the following fixes:
* Fix unregister/remove path for the wrapped shuffle resolver to prevent stale shuffle files from accumulating, which could cause `java.io.IOException: No space left on device` on long running jobs ([#14686](https://github.com/NVIDIA/spark-rapids/pull/14686), [#14683](https://github.com/NVIDIA/spark-rapids/issues/14683))
* Rebuilt against the latest Databricks 17.3 ML LTS runtime to resolve a `NoSuchMethodError` on `CatalogTable.copy` in `GpuCreateDataSourceTableAsSelectCommand` ([#14682](https://github.com/NVIDIA/spark-rapids/issues/14682))

The full v26.04.0 release notes are also included below.

* Databricks 17.3 ML LTS (Spark 4.0) support (Delta Lake fallback to CPU); Databricks 12.2 ML LTS no longer supported ([#14518](https://github.com/NVIDIA/spark-rapids/pull/14518))
* Iceberg 1.10.1 support with Spark 4.0.2 ([#14459](https://github.com/NVIDIA/spark-rapids/pull/14459))
* Bloom filter v2 support on Databricks ([#14406](https://github.com/NVIDIA/spark-rapids/pull/14406))
* Parquet GPU reader: defer resource collection until close ([#14490](https://github.com/NVIDIA/spark-rapids/pull/14490)); row-to-column per-batch retry for OOM recovery ([#14428](https://github.com/NVIDIA/spark-rapids/pull/14428))
* Shuffle and spilling: shuffle fanout OOM fix ([#14525](https://github.com/NVIDIA/spark-rapids/pull/14525)), thread-safe shuffle unregister ([#14513](https://github.com/NVIDIA/spark-rapids/pull/14513)), bounded retry logging for shuffle coalesce ([#14537](https://github.com/NVIDIA/spark-rapids/pull/14537))
* Window execution fixes for mixed ROWS windows ([#14533](https://github.com/NVIDIA/spark-rapids/pull/14533))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v26.04.0
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
		Apache Spark 4.0.0, 4.0.1, 4.0.2
		Apache Spark 4.1.1
		Scala 2.12: Spark 3.3.0 through 3.5.8
		Scala 2.13: Spark 3.5.0 through 3.5.8, and Spark 4.0.0, 4.0.1, 4.0.2, and 4.1.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
		Databricks 17.3 ML LTS (GPU, Scala 2.13, Spark 4.0.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v26.04.0

#### CUDA 12

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


#### CUDA 13

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.04.0/rapids-4-spark_2.12-26.04.0-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.04.0/rapids-4-spark_2.13-26.04.0-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.04.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


The above packages are built against CUDA 12.9 or CUDA 13.1. They are tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.04.0.jar.asc rapids-4-spark_2.12-26.04.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.04.0.jar.asc rapids-4-spark_2.13-26.04.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Databricks 17.3 ML LTS (Spark 4.0) support (Delta Lake fallback to CPU); Databricks 12.2 ML LTS no longer supported ([#14518](https://github.com/NVIDIA/spark-rapids/pull/14518))
* Iceberg 1.10.1 support with Spark 4.0.2 ([#14459](https://github.com/NVIDIA/spark-rapids/pull/14459))
* Bloom filter v2 support on Databricks ([#14406](https://github.com/NVIDIA/spark-rapids/pull/14406))
* Parquet GPU reader: defer resource collection until close ([#14490](https://github.com/NVIDIA/spark-rapids/pull/14490)); row-to-column per-batch retry for OOM recovery ([#14428](https://github.com/NVIDIA/spark-rapids/pull/14428))
* Shuffle and spilling: shuffle fanout OOM fix ([#14525](https://github.com/NVIDIA/spark-rapids/pull/14525)), thread-safe shuffle unregister ([#14513](https://github.com/NVIDIA/spark-rapids/pull/14513)), bounded retry logging for shuffle coalesce ([#14537](https://github.com/NVIDIA/spark-rapids/pull/14537))
* Window execution fixes for mixed ROWS windows ([#14533](https://github.com/NVIDIA/spark-rapids/pull/14533))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v26.02.2
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
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6, 3.5.7
		Apache Spark 4.0.0, 4.0.1
		Apache Spark 4.1.1
		Scala 2.12: Spark 3.3.0 through 3.5.7
		Scala 2.13: Spark 3.5.0 through 3.5.7, and Spark 4.0.0, 4.0.1, and 4.1.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v26.02.2

#### CUDA 12

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.02.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.2/rapids-4-spark_2.12-26.02.2.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.2/rapids-4-spark_2.12-26.02.2.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.2&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.02.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.2/rapids-4-spark_2.13-26.02.2.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.2/rapids-4-spark_2.13-26.02.2.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.2&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.02.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.2/rapids-4-spark_2.12-26.02.2-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.2/rapids-4-spark_2.12-26.02.2-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.02.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.2/rapids-4-spark_2.13-26.02.2-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.2/rapids-4-spark_2.13-26.02.2-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


#### CUDA 13

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.02.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.2/rapids-4-spark_2.12-26.02.2-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.2/rapids-4-spark_2.12-26.02.2-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.02.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.2/rapids-4-spark_2.13-26.02.2-cuda13.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.2/rapids-4-spark_2.13-26.02.2-cuda13.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.02.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.2/rapids-4-spark_2.12-26.02.2-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.2/rapids-4-spark_2.12-26.02.2-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.02.2](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.2/rapids-4-spark_2.13-26.02.2-cuda13-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.2/rapids-4-spark_2.13-26.02.2-cuda13-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.2&lt;/version&gt;<br/>    &lt;classifier&gt;cuda13-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |


The above packages are built against CUDA 12.9 or CUDA 13.1. They are tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.02.2.jar.asc rapids-4-spark_2.12-26.02.2.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.02.2.jar.asc rapids-4-spark_2.13-26.02.2.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Add support for Spark 4.1.1 ([#14120](https://github.com/NVIDIA/spark-rapids/pull/14120))
* Iceberg enhancements: identity partitioning support ([#14183](https://github.com/NVIDIA/spark-rapids/pull/14183)), all types for bucket transform ([#14001](https://github.com/NVIDIA/spark-rapids/pull/14001)), and Iceberg 1.9.2 support ([#13986](https://github.com/NVIDIA/spark-rapids/pull/13986))
* SHA-2 hash support ([#14038](https://github.com/NVIDIA/spark-rapids/pull/14038))
* Rapids shuffle manager V2 phase 1 with pipelined write ([#13724](https://github.com/NVIDIA/spark-rapids/pull/13724))
* Enable GPU kudo reads by default ([#14125](https://github.com/NVIDIA/spark-rapids/pull/14125))
* Multiple deadlock and OOM fixes for improved stability ([#14202](https://github.com/NVIDIA/spark-rapids/pull/14202), [#14180](https://github.com/NVIDIA/spark-rapids/pull/14180), [#14175](https://github.com/NVIDIA/spark-rapids/pull/14175))

Note: There is a known issue in the 26.02.2 release where a query that combines running
(UNBOUNDED PRECEDING to CURRENT ROW) and bounded window frames in the same PARTITION BY / ORDER BY
clause, where both use AVG(), may fail with `RuntimeException: Couldn't find _gpu_w* in [...]`.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v26.02.1
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
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6, 3.5.7
		Apache Spark 4.0.0, 4.0.1
		Apache Spark 4.1.1
		Scala 2.12: Spark 3.3.0 through 3.5.7
		Scala 2.13: Spark 3.5.0 through 3.5.7, and Spark 4.0.0, 4.0.1, and 4.1.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v26.02.1

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.02.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.1/rapids-4-spark_2.12-26.02.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.1/rapids-4-spark_2.12-26.02.1.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.1&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.02.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.1/rapids-4-spark_2.13-26.02.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.1/rapids-4-spark_2.13-26.02.1.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.1&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.02.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.1/rapids-4-spark_2.12-26.02.1-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.1/rapids-4-spark_2.12-26.02.1-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.02.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.1/rapids-4-spark_2.13-26.02.1-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.1/rapids-4-spark_2.13-26.02.1-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.1&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |

This package is built against CUDA 12.9. It is tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs with
CUDA 12.9.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.02.1.jar.asc rapids-4-spark_2.12-26.02.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.02.1.jar.asc rapids-4-spark_2.13-26.02.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Add support for Spark 4.1.1 ([#14120](https://github.com/NVIDIA/spark-rapids/pull/14120))
* Iceberg enhancements: identity partitioning support ([#14183](https://github.com/NVIDIA/spark-rapids/pull/14183)), all types for bucket transform ([#14001](https://github.com/NVIDIA/spark-rapids/pull/14001)), and Iceberg 1.9.2 support ([#13986](https://github.com/NVIDIA/spark-rapids/pull/13986))
* SHA-2 hash support ([#14038](https://github.com/NVIDIA/spark-rapids/pull/14038))
* Rapids shuffle manager V2 phase 1 with pipelined write ([#13724](https://github.com/NVIDIA/spark-rapids/pull/13724))
* Enable GPU kudo reads by default ([#14125](https://github.com/NVIDIA/spark-rapids/pull/14125))
* Multiple deadlock and OOM fixes for improved stability ([#14202](https://github.com/NVIDIA/spark-rapids/pull/14202), [#14180](https://github.com/NVIDIA/spark-rapids/pull/14180), [#14175](https://github.com/NVIDIA/spark-rapids/pull/14175))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v26.02.0
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
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6, 3.5.7
		Apache Spark 4.0.0, 4.0.1
		Apache Spark 4.1.1
		Scala 2.12: Spark 3.3.0 through 3.5.7
		Scala 2.13: Spark 3.5.0 through 3.5.7, and Spark 4.0.0, 4.0.1, and 4.1.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v26.02.0

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v26.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.0/rapids-4-spark_2.12-26.02.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.0/rapids-4-spark_2.12-26.02.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v26.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.0/rapids-4-spark_2.13-26.02.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.0/rapids-4-spark_2.13-26.02.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v26.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.0/rapids-4-spark_2.12-26.02.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/26.02.0/rapids-4-spark_2.12-26.02.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v26.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.0/rapids-4-spark_2.13-26.02.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/26.02.0/rapids-4-spark_2.13-26.02.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;26.02.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |

This package is built against CUDA 12.9. It is tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs with
CUDA 12.9.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-26.02.0.jar.asc rapids-4-spark_2.12-26.02.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-26.02.0.jar.asc rapids-4-spark_2.13-26.02.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Add support for Spark 4.1.1 ([#14120](https://github.com/NVIDIA/spark-rapids/pull/14120))
* Iceberg enhancements: identity partitioning support ([#14183](https://github.com/NVIDIA/spark-rapids/pull/14183)), all types for bucket transform ([#14001](https://github.com/NVIDIA/spark-rapids/pull/14001)), and Iceberg 1.9.2 support ([#13986](https://github.com/NVIDIA/spark-rapids/pull/13986))
* SHA-2 hash support ([#14038](https://github.com/NVIDIA/spark-rapids/pull/14038))
* Rapids shuffle manager V2 phase 1 with pipelined write ([#13724](https://github.com/NVIDIA/spark-rapids/pull/13724))
* Enable GPU kudo reads by default ([#14125](https://github.com/NVIDIA/spark-rapids/pull/14125))
* Multiple deadlock and OOM fixes for improved stability ([#14202](https://github.com/NVIDIA/spark-rapids/pull/14202), [#14180](https://github.com/NVIDIA/spark-rapids/pull/14180), [#14175](https://github.com/NVIDIA/spark-rapids/pull/14175))

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v25.12.0
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
        Scala 2.12: Spark 3.3.x to Spark 3.5.x
        Scala 2.13: Spark 3.5.0+, 4.0.0, 4.0.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v25.12.0

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v25.12.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.12.0/rapids-4-spark_2.12-25.12.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.12.0/rapids-4-spark_2.12-25.12.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;25.12.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v25.12.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.12.0/rapids-4-spark_2.13-25.12.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.12.0/rapids-4-spark_2.13-25.12.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;25.12.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v25.12.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.12.0/rapids-4-spark_2.12-25.12.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.12.0/rapids-4-spark_2.12-25.12.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;25.12.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v25.12.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.12.0/rapids-4-spark_2.13-25.12.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.12.0/rapids-4-spark_2.13-25.12.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;25.12.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |

This package is built against CUDA 12.9. It is tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs with 
CUDA 12.9.  

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-25.12.0.jar.asc rapids-4-spark_2.12-25.12.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-25.12.0.jar.asc rapids-4-spark_2.13-25.12.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Iceberg enhancements including DML operations (delete, update, merge) for merge-on-read tables, partition transforms (year/month/day/hour/truncate), and write operations enabled by default.
* Delta Lake clustered tables DML support including update, merge, and delete operations with deletion vector enabled GPU by default.
* Join improvements including support for left-outer joins with no columns, new join strategies with logging and heuristic configurations, and improved gather map ordering.
* CSV support for GBK encoded data.
* Refine GpuTaskMetrics over the spill framework.
* Fix race condition due to premature disk handle exposure.

Note: There is a known issue in the 25.12.0 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

## Release v25.10.0
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
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6
		Apache Spark 4.0.0, 4.0.1 
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v25.10.0

| Processor | Scala Version | Download Jar | Download Signature | Download From Maven |
|-----------|---------------|--------------|--------------------|---------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v25.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.10.0/rapids-4-spark_2.12-25.10.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.10.0/rapids-4-spark_2.12-25.10.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;25.10.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v25.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.10.0/rapids-4-spark_2.13-25.10.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.10.0/rapids-4-spark_2.13-25.10.0.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;25.10.0&lt;/version&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v25.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.10.0/rapids-4-spark_2.12-25.10.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.10.0/rapids-4-spark_2.12-25.10.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.12&lt;/artifactId&gt;<br/>    &lt;version&gt;25.10.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v25.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.10.0/rapids-4-spark_2.13-25.10.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.10.0/rapids-4-spark_2.13-25.10.0-cuda12-arm64.jar.asc) | <pre>&lt;dependency&gt;<br/>    &lt;groupId&gt;com.nvidia&lt;/groupId&gt;<br/>    &lt;artifactId&gt;rapids-4-spark_2.13&lt;/artifactId&gt;<br/>    &lt;version&gt;25.10.0&lt;/version&gt;<br/>    &lt;classifier&gt;cuda12-arm64&lt;/classifier&gt;<br/>&lt;/dependency&gt;</pre> |

This package is built against CUDA 12.9. It is tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs with 
CUDA 12.9.  

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-25.10.0.jar.asc rapids-4-spark_2.12-25.10.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-25.10.0.jar.asc rapids-4-spark_2.13-25.10.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Delta Lake liquid clustering read/write/optimize support
* Delta Lake optimize support
* Delta Lake deletion vector support with two caveats: need to set `useMetadataRowIndex=false` and deletion vector support will fall back to the CPU when using the coalescing file reader (these limitations to be removed in a future release)
* Iceberg insert operations support and improved write job statistics tracking
* Improved performance for stddev and variance operations in hash based group by aggregations
* Support for uuid
* Added Spark 4.0.1 support
* Added CUDA 13 support, in addition to CUDA 12 support. 

Note: There is a known issue in the 25.10.0 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

## Release v25.08.0
### Hardware Requirements:

The plugin is designed to work on NVIDIA Volta, Turing, Ampere, Ada Lovelace, Hopper and Blackwell generation datacenter GPUs.  The plugin jar is tested on the following GPUs:

	GPU Models: NVIDIA V100, T4, A10, A100, L4, H100 and B100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R525+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6
		Apache Spark 4.0.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v25.08.0

| Processor | Scala Version | Download Jar                                                                                                                                                 | Download Signature                                                                                                                             |
|-----------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v25.08.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.08.0/rapids-4-spark_2.12-25.08.0.jar)                         | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.08.0/rapids-4-spark_2.12-25.08.0.jar.asc)                         |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v25.08.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.08.0/rapids-4-spark_2.13-25.08.0.jar)                         | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.08.0/rapids-4-spark_2.13-25.08.0.jar.asc)                         |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v25.08.0](https://edge.urm.nvidia.com/artifactory/sw-spark-maven/com/nvidia/rapids-4-spark_2.12/25.08.0/rapids-4-spark_2.12-25.08.0.jar) | [Signature](https://edge.urm.nvidia.com/artifactory/sw-spark-maven/com/nvidia/rapids-4-spark_2.12/25.08.0/rapids-4-spark_2.12-25.08.0.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v25.08.0](https://edge.urm.nvidia.com/artifactory/sw-spark-maven/com/nvidia/rapids-4-spark_2.13/25.08.0/rapids-4-spark_2.13-25.08.0.jar)                                | [Signature](https://edge.urm.nvidia.com/artifactory/sw-spark-maven/com/nvidia/rapids-4-spark_2.12/25.08.0/rapids-4-spark_2.12-25.08.0.jar.asc)                                    |

This package is built against CUDA 12.9. It is tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs with
CUDA 12.9.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
  `gpg --verify rapids-4-spark_2.12-25.08.0.jar.asc rapids-4-spark_2.12-25.08.0.jar`
* Verify the signature for Scala 2.13 jar:
  `gpg --verify rapids-4-spark_2.13-25.08.0.jar.asc rapids-4-spark_2.13-25.08.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Spark 4.0 support, including ANSI mode support for multiply, AVG and SUM aggregations with improved performance.  Compatibility with Spark 4.0 exception handling and type casting.
* Support for Spark Connect in Spark 3.5.6 and Spark 4.0
* Delta Lake 3.3.x open source support with read, update, merge, delete, optimized write and auto compact functionality.  Read is supported without deletion vectors.
* Iceberg S3 tables support
* Support scalar * scalar overload function for enhanced mathematical operations
* Improved expression combining and side-effect checking for GpuCaseWhen operations
* Better Parquet type conversion for improved data processing
* Improved memory management with fixes for memory leaks and overflow handling
* Performance optimizations for GPU kernel usage and aggregation operations
* Stability improvements and bug fixes for production workloads

Note: There is a known issue in the 25.08.0 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v25.06.0
### Hardware Requirements:

The plugin is designed to work on NVIDIA Volta, Turing, Ampere, Ada Lovelace, Hopper and Blackwell generation datacenter GPUs.  The plugin jar is tested on the following GPUs:

	GPU Models: NVIDIA V100, T4, A10, A100, L4, H100 and B100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
		GCP Dataproc 2.3

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v25.06.0

| Processor | Scala Version | Download Jar                                                                                                                                      | Download Signature                                                                                                                  |
|-----------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v25.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.06.0/rapids-4-spark_2.12-25.06.0.jar)              | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.06.0/rapids-4-spark_2.12-25.06.0.jar.asc)              |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v25.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.06.0/rapids-4-spark_2.13-25.06.0.jar)              | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.06.0/rapids-4-spark_2.13-25.06.0.jar.asc)              |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v25.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.06.0/rapids-4-spark_2.12-25.06.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.06.0/rapids-4-spark_2.12-25.06.0-cuda12-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v25.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.06.0/rapids-4-spark_2.13-25.06.0-cuda12-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.06.0/rapids-4-spark_2.13-25.06.0-cuda12-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs with
CUDA 11.8 and CUDA 12.8.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
  `gpg --verify rapids-4-spark_2.12-25.06.0.jar.asc rapids-4-spark_2.12-25.06.0.jar`
* Verify the signature for Scala 2.13 jar:
  `gpg --verify rapids-4-spark_2.13-25.06.0.jar.asc rapids-4-spark_2.13-25.06.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Support functions that have time zones with daylight savings transitions
* Support Spark Connect for Spark 3.5.6+ (Spark Connect is supported for Spark 3.4-3.5.5 if the plugin jar is built with a single shim)
* Support for Iceberg 1.6.1 on Spark 3.5.x for read with deletions
* Support array_distinct
* Support bit_count
* Support bitwise aggregate functions (bit_and, bit_or and bit_xor) in groupby and reduction
* Support conv
* Support sha1
* Introduce more metrics for troubleshooting (max writers number, memory bookkeepings)
* Fix a GPU Out-of-memory bug when spark.speculation is on
* Fix a get_json_object bug when encounter some special pattern path

Note: There is a known issue in the 25.06.0 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v25.04.0
### Hardware Requirements:

The plugin is designed to work on NVIDIA Volta, Turing, Ampere, Ada Lovelace, Hopper and Blackwell generation datacenter GPUs.  The plugin jar is tested on the following GPUs:

	GPU Models: NVIDIA V100, T4, A10, A100, L4, H100 and B100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5, 3.5.6
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
		Databricks 14.3 ML LTS (GPU, Scala 2.12, Spark 3.5.0)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2

	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 1.2
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v25.04.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v25.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.04.0/rapids-4-spark_2.12-25.04.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.04.0/rapids-4-spark_2.12-25.04.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v25.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.04.0/rapids-4-spark_2.13-25.04.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.04.0/rapids-4-spark_2.13-25.04.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v25.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.04.0/rapids-4-spark_2.12-25.04.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.04.0/rapids-4-spark_2.12-25.04.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v25.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.04.0/rapids-4-spark_2.13-25.04.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.04.0/rapids-4-spark_2.13-25.04.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4, H100 and GB100 GPUs with
CUDA 11.8 and CUDA 12.8.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
  `gpg --verify rapids-4-spark_2.12-25.04.0.jar.asc rapids-4-spark_2.12-25.04.0.jar`
* Verify the signature for Scala 2.13 jar:
  `gpg --verify rapids-4-spark_2.13-25.04.0.jar.asc rapids-4-spark_2.13-25.04.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Support approx_count_distinct
* Support group by on binary type
* Support ArrayPosition function
* Support Databricks 14.3 ML LTS (without support for Deletion Vector reads in Delta Lake)
* Support Slice
* Enable Hive text writer
* Refine split-retry logs when out of memory happens to expose the real reason
* Allow BigSizedJoinIterator#buildPartitioner to produce more sub-partitions to avoid CudfColumnSizeOverflowException

Note: There is a known issue in the 25.04.0 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v25.02.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4, H100 and B100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4, 3.5.5
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v25.02.1

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v25.02.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.02.1/rapids-4-spark_2.12-25.02.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.02.1/rapids-4-spark_2.12-25.02.1.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v25.02.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.02.1/rapids-4-spark_2.13-25.02.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.02.1/rapids-4-spark_2.13-25.02.1.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v25.02.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.02.1/rapids-4-spark_2.12-25.02.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.02.1/rapids-4-spark_2.12-25.02.1-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v25.02.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.02.1/rapids-4-spark_2.13-25.02.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.02.1/rapids-4-spark_2.13-25.02.1-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-25.02.1.jar.asc rapids-4-spark_2.12-25.02.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-25.02.1.jar.asc rapids-4-spark_2.13-25.02.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Support the Spark functions Bin and TruncDate
* Support group-limit optimization for ROW_NUMBER
* Improve Spark metrics: Print the batch size information to executor log
* Refine filter push down to avoid double evaluation
* Grab the GPU Semaphore when reading cached batch data with the GPU to avoid a GPU OOM case
* Add an option to disable measuring buffer copy to improve large shuffle large partition serialization
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)
* Upgraded statically linked CUDA toolkit to 12.8, which includes support for GB100 GPUs

Note: There is a known issue in the 25.02.1 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v25.02.0
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v25.02.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v25.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.02.0/rapids-4-spark_2.12-25.02.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.02.0/rapids-4-spark_2.12-25.02.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v25.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.02.0/rapids-4-spark_2.13-25.02.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.02.0/rapids-4-spark_2.13-25.02.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v25.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.02.0/rapids-4-spark_2.12-25.02.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/25.02.0/rapids-4-spark_2.12-25.02.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v25.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.02.0/rapids-4-spark_2.13-25.02.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/25.02.0/rapids-4-spark_2.13-25.02.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-25.02.0.jar.asc rapids-4-spark_2.12-25.02.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-25.02.0.jar.asc rapids-4-spark_2.13-25.02.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Support the Spark functions Bin and TruncDate
* Support group-limit optimization for ROW_NUMBER
* Improve Spark metrics: Print the batch size information to executor log
* Refine filter push down to avoid double evaluation
* Grab the GPU Semaphore when reading cached batch data with the GPU to avoid a GPU OOM case
* Add an option to disable measuring buffer copy to improve large shuffle large partition serialization
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

Note: There is a known issue in the 25.02.0 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.12.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3, 3.4.4
		Apache Spark 3.5.0, 3.5.1, 3.5.2, 3.5.3, 3.5.4
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.12.1

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.12.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.12.1/rapids-4-spark_2.12-24.12.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.12.1/rapids-4-spark_2.12-24.12.1.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.12.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.12.1/rapids-4-spark_2.13-24.12.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.12.1/rapids-4-spark_2.13-24.12.1.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.12.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.12.1/rapids-4-spark_2.12-24.12.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.12.1/rapids-4-spark_2.12-24.12.1-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.12.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.12.1/rapids-4-spark_2.13-24.12.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.12.1/rapids-4-spark_2.13-24.12.1-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.12.1.jar.asc rapids-4-spark_2.12-24.12.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.12.1.jar.asc rapids-4-spark_2.13-24.12.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Add repartition-based algorithm fallback in hash aggregate  
* Support Spark function months_between
* Support asynchronous writing for Parquet files
* Add retry support to improve sub hash-join stability
* Improve JSON scan and from_json
* Improved performance for CASE WHEN statements comparing a string column against multiple values
* Falling back to the CPU for ORC boolean writes by the GPU due to a bug in cudf's ORC writer
* Fix a device memory leak in timestamp operator in `incompatibleDateFormats` case
* Fix a host memory leak in GpuBroadcastNestedLoopJoinExecBase when `spillableBuiltBatch` is 0
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

Note: There is a known issue in the 24.12.1 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.12.0
### Hardware Requirements:

The plugin is tested on the following architectures:

        GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018.
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

        NVIDIA Driver*: R470+

        Runtime:
                Scala 2.12, 2.13
                Python, Java Virtual Machine (JVM) compatible with your spark-version.

                * Check the Spark documentation for Python and Java version compatibility with your specific
                Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

        Supported Spark versions:
                Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
                Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
                Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3
                Apache Spark 3.5.0, 3.5.1, 3.5.2

        Supported Databricks runtime versions for Azure and AWS:
                Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
                Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
                Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)

        Supported Dataproc versions (Debian/Ubuntu/Rocky):
                GCP Dataproc 2.1
                GCP Dataproc 2.2

        Supported Dataproc Serverless versions:
                Spark runtime 1.1 LTS
                Spark runtime 2.0
                Spark runtime 2.1
                Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.12.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.12.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.12.0/rapids-4-spark_2.12-24.12.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.12.0/rapids-4-spark_2.12-24.12.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.12.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.12.0/rapids-4-spark_2.13-24.12.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.12.0/rapids-4-spark_2.13-24.12.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.12.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.12.0/rapids-4-spark_2.12-24.12.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.12.0/rapids-4-spark_2.12-24.12.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.12.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.12.0/rapids-4-spark_2.13-24.12.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.12.0/rapids-4-spark_2.13-24.12.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.12.0.jar.asc rapids-4-spark_2.12-24.12.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.12.0.jar.asc rapids-4-spark_2.13-24.12.0.jar`

The output of signature verify:

        gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Add repartition-based algorithm fallback in hash aggregate
* Support Spark function months_between
* Support asynchronous writing for Parquet files
* Add retry support to improve sub hash-join stability
* Improve JSON scan and from_json
* Improved performance for CASE WHEN statements comparing a string column against multiple values
* Falling back to the CPU for ORC boolean writes by the GPU due to a bug in cudf's ORC writer
* Fix a device memory leak in timestamp operator in `incompatibleDateFormats` case
* Fix a host memory leak in GpuBroadcastNestedLoopJoinExecBase when `spillableBuiltBatch` is 0
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

Note: There is a known issue in the 24.12.0 release when decompressing gzip files on H100 GPUs.
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.10.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

    OS: Spark RAPIDS is compatible with any Linux distribution with glibc >= 2.28 (Please check ldd --version output).  glibc 2.28 was released August 1, 2018. 
    Tested on Ubuntu 20.04, Ubuntu 22.04, Rocky Linux 8 and Rocky Linux 9

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3
		Apache Spark 3.5.0, 3.5.1, 3.5.2
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.10.1

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.10.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.1/rapids-4-spark_2.12-24.10.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.1/rapids-4-spark_2.12-24.10.1.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.10.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.10.1/rapids-4-spark_2.13-24.10.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.10.1/rapids-4-spark_2.13-24.10.1.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.10.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.1/rapids-4-spark_2.12-24.10.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.1/rapids-4-spark_2.12-24.10.1-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.10.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.10.1/rapids-4-spark_2.13-24.10.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.10.1/rapids-4-spark_2.13-24.10.1-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.10.1.jar.asc rapids-4-spark_2.12-24.10.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.10.1.jar.asc rapids-4-spark_2.13-24.10.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Optimize scheduling policy for GPU Semaphore
* Support distinct join for right outer joins
* Support MinBy and MaxBy for non-float ordering  
* Support ArrayJoin expression
* Optimize Expand and Aggregate expression performance
* Improve JSON related expressions
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

Note: There is a known issue in the 24.10.1 release when decompressing gzip files on H100 GPUs.  
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.10.0
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3
		Apache Spark 3.5.0, 3.5.1, 3.5.2
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.10.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.0/rapids-4-spark_2.12-24.10.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.0/rapids-4-spark_2.12-24.10.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.10.0/rapids-4-spark_2.13-24.10.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.10.0/rapids-4-spark_2.13-24.10.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.0/rapids-4-spark_2.12-24.10.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.10.0/rapids-4-spark_2.12-24.10.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.10.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.10.0/rapids-4-spark_2.13-24.10.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.10.0/rapids-4-spark_2.13-24.10.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.10.0.jar.asc rapids-4-spark_2.12-24.10.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.10.0.jar.asc rapids-4-spark_2.13-24.10.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Optimize scheduling policy for GPU Semaphore
* Support distinct join for right outer joins
* Support MinBy and MaxBy for non-float ordering  
* Support ArrayJoin expression
* Optimize Expand and Aggregate expression performance
* Improve JSON related expressions
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

Note: There is a known issue in the 24.10.0 release when decompressing gzip files on H100 GPUs.  
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.08.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3
		Apache Spark 3.5.0, 3.5.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu/Rocky):
		GCP Dataproc 2.1
		GCP Dataproc 2.2
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.08.1

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.1/rapids-4-spark_2.12-24.08.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.1/rapids-4-spark_2.12-24.08.1.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.08.1/rapids-4-spark_2.13-24.08.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.08.1/rapids-4-spark_2.13-24.08.1.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.1/rapids-4-spark_2.12-24.08.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.08.1/rapids-4-spark_2.12-24.08.1-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.08.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.08.1/rapids-4-spark_2.13-24.08.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.08.1/rapids-4-spark_2.13-24.08.1-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.08.1.jar.asc rapids-4-spark_2.12-24.08.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.08.1.jar.asc rapids-4-spark_2.13-24.08.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Support timezones with daylight savings shifts
* Improve metrics in Spark UI
* Refactor Parquet decode microkernels and support load balancing RLE runs
* Improve get_json performance
* Support dynamic scan filtering
* Improve UCX shuffle 
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

Note: There is a known issue in the 24.08.1 release when decompressing gzip files on H100 GPUs.  
Please find more details in [issue-16661](https://github.com/rapidsai/cudf/issues/16661).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.06.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3
		Apache Spark 3.5.0, 3.5.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu):
		GCP Dataproc 2.0
		GCP Dataproc 2.1
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1
		Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.06.1

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.06.1/rapids-4-spark_2.12-24.06.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.06.1/rapids-4-spark_2.12-24.06.1.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.06.1/rapids-4-spark_2.13-24.06.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.06.1/rapids-4-spark_2.13-24.06.1.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.06.1/rapids-4-spark_2.12-24.06.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.06.1/rapids-4-spark_2.12-24.06.1-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.06.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.06.1/rapids-4-spark_2.13-24.06.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.06.1/rapids-4-spark_2.13-24.06.1-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.06.1.jar.asc rapids-4-spark_2.12-24.06.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.06.1.jar.asc rapids-4-spark_2.13-24.06.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Improve support for Unity Catalog on Databricks
* Added support for parse_url PATH
* Added support for array_filter
* Added support for Spark 3.4.3
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.06.0
### Hardware Requirements:

The plugin is tested on the following architectures:

        GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

        OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

        NVIDIA Driver*: R470+

        Runtime: 
                Scala 2.12, 2.13
                Python, Java Virtual Machine (JVM) compatible with your spark-version. 

                * Check the Spark documentation for Python and Java version compatibility with your specific 
                Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

        Supported Spark versions:
                Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
                Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
                Apache Spark 3.4.0, 3.4.1, 3.4.2, 3.4.3
                Apache Spark 3.5.0, 3.5.1

        Supported Databricks runtime versions for Azure and AWS:
                Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
                Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
                Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)

        Supported Dataproc versions (Debian/Ubuntu):
                GCP Dataproc 2.0
                GCP Dataproc 2.1

        Supported Dataproc Serverless versions:
                Spark runtime 1.1 LTS
                Spark runtime 2.0
                Spark runtime 2.1
                Spark runtime 2.2

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.06.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.06.0/rapids-4-spark_2.12-24.06.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.06.0/rapids-4-spark_2.12-24.06.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.06.0/rapids-4-spark_2.13-24.06.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.06.0/rapids-4-spark_2.13-24.06.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.06.0/rapids-4-spark_2.12-24.06.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.06.0/rapids-4-spark_2.12-24.06.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.06.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.06.0/rapids-4-spark_2.13-24.06.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.06.0/rapids-4-spark_2.13-24.06.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.06.0.jar.asc rapids-4-spark_2.12-24.06.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.06.0.jar.asc rapids-4-spark_2.13-24.06.0.jar`

The output of signature verify:

        gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* Improve support for Unity Catalog on Databricks
* Added support for parse_url PATH
* Added support for array_filter
* Added support for Spark 3.4.3
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases)

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.04.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2
		Apache Spark 3.5.0, 3.5.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu):
		GCP Dataproc 2.0
		GCP Dataproc 2.1
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.04.1

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.04.1/rapids-4-spark_2.12-24.04.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.04.1/rapids-4-spark_2.12-24.04.1.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.04.1/rapids-4-spark_2.13-24.04.1.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.04.1/rapids-4-spark_2.13-24.04.1.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.04.1/rapids-4-spark_2.12-24.04.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.04.1/rapids-4-spark_2.12-24.04.1-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.04.1](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.04.1/rapids-4-spark_2.13-24.04.1-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.04.1/rapids-4-spark_2.13-24.04.1-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.04.1.jar.asc rapids-4-spark_2.12-24.04.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.04.1.jar.asc rapids-4-spark_2.13-24.04.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* New functionality and performance improvements for this release include:
* Performance improvements for S3 reading. 
Refer to perfio.s3.enabled in [advanced_configs](./additional-functionality/advanced_configs.md) for more details.
* Performance improvements when doing a joins on unique keys.
* Enhanced decompression kernels for zstd and snappy.
* Enhanced Parquet reading performance with modular kernels.
* Added compatibility with Spark version 3.5.1.
* Deprecated support for Databricks 10.4 ML LTS.
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.04.0
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3, 3.3.4
		Apache Spark 3.4.0, 3.4.1, 3.4.2
		Apache Spark 3.5.0, 3.5.1
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions (Debian/Ubuntu):
		GCP Dataproc 2.0
		GCP Dataproc 2.1
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.04.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.04.0/rapids-4-spark_2.12-24.04.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.04.0/rapids-4-spark_2.12-24.04.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.04.0/rapids-4-spark_2.13-24.04.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.04.0/rapids-4-spark_2.13-24.04.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.04.0/rapids-4-spark_2.12-24.04.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.04.0/rapids-4-spark_2.12-24.04.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.04.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.04.0/rapids-4-spark_2.13-24.04.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.04.0/rapids-4-spark_2.13-24.04.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.04.0.jar.asc rapids-4-spark_2.12-24.04.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.04.0.jar.asc rapids-4-spark_2.13-24.04.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
* New functionality and performance improvements for this release include:
* Performance improvements for S3 reading. 
Refer to perfio.s3.enabled in [advanced_configs](./additional-functionality/advanced_configs.md) for more details.
* Performance improvements when doing a joins on unique keys.
* Enhanced decompression kernels for zstd and snappy.
* Enhanced Parquet reading performance with modular kernels.
* Added compatibility with Spark version 3.5.1.
* Deprecated support for Databricks 10.4 ML LTS.
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v24.02.0
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3
		Apache Spark 3.4.0, 3.4.1, 3.4.2
		Apache Spark 3.5.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
		Databricks 13.3 ML LTS (GPU, Scala 2.12, Spark 3.4.1)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS
		Spark runtime 2.0
		Spark runtime 2.1

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v24.02.0

| Processor | Scala Version | Download Jar | Download Signature |
|-----------|---------------|--------------|--------------------|
| x86_64    | Scala 2.12    | [RAPIDS Accelerator v24.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.02.0/rapids-4-spark_2.12-24.02.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.02.0/rapids-4-spark_2.12-24.02.0.jar.asc) |
| x86_64    | Scala 2.13    | [RAPIDS Accelerator v24.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.02.0/rapids-4-spark_2.13-24.02.0.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.02.0/rapids-4-spark_2.13-24.02.0.jar.asc) |
| arm64     | Scala 2.12    | [RAPIDS Accelerator v24.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.02.0/rapids-4-spark_2.12-24.02.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/24.02.0/rapids-4-spark_2.12-24.02.0-cuda11-arm64.jar.asc) |
| arm64     | Scala 2.13    | [RAPIDS Accelerator v24.02.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.02.0/rapids-4-spark_2.13-24.02.0-cuda11-arm64.jar) | [Signature](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/24.02.0/rapids-4-spark_2.13-24.02.0-cuda11-arm64.jar.asc) |

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-24.02.0.jar.asc rapids-4-spark_2.12-24.02.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-24.02.0.jar.asc rapids-4-spark_2.13-24.02.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Discontinued support for Nvidia GPUs based on Pascal architecture.
* Set get_json_object functionality to disabled by default.
* Implemented string comparison in AST expressions.
* Expanded timezone support to include options beyond UTC.
* Optional checksums for cached files in the file cache.
* Introduced support for Databricks 13.3 ML LTS.
* Added support for parse_url functionality.
* Introducing Lazy Quantifier support for regular expression functions.
* Added support for the format_number function.
* Enhanced batching support for row-based bounded window functions.
* For updates on RAPIDS Accelerator Tools, please visit [this link](https://github.com/NVIDIA/spark-rapids-tools/releases).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.12.2
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3
		Apache Spark 3.4.0, 3.4.1
		Apache Spark 3.5.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v23.12.2
- **Scala 2.12:**
  - [RAPIDS Accelerator for Apache Spark 23.12.2 - Scala 2.12 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.12.2/rapids-4-spark_2.12-23.12.2.jar)
  - [RAPIDS Accelerator for Apache Spark 23.12.2 - Scala 2.12 jar.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.12.2/rapids-4-spark_2.12-23.12.2.jar.asc)

- **Scala 2.13:**
  - [RAPIDS Accelerator for Apache Spark 23.12.2 - Scala 2.13 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/23.12.2/rapids-4-spark_2.13-23.12.2.jar)
  - [RAPIDS Accelerator for Apache Spark 23.12.2 - Scala 2.13 jar.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/23.12.2/rapids-4-spark_2.13-23.12.2.jar.asc)

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-23.12.2.jar.asc rapids-4-spark_2.12-23.12.2.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-23.12.2.jar.asc rapids-4-spark_2.13-23.12.2.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Introduced support for chunked reading of ORC files.
* Enhanced support for additional time zones and added stack function support.
* Enhanced performance for join and aggregation operations.
* Kernel optimizations have been implemented to improve Parquet read performance.
* RAPIDS Accelerator also built and tested with Scala 2.13.
* Last version to support Pascal-based Nvidia GPUs; discontinued in the next release.
* Introduced support for parquet Legacy rebase mode (spark.sql.parquet.datetimeRebaseModeInRead=LEGACY and spark.sql.parquet.int96RebaseModeInRead=LEGACY)
* Introduced support for Percentile function.
* Delta lake 2.3 support.
* Qualification and Profiling tool:
	* Profiling Tool now processes Spark Driver log for GPU runs, enhancing feature analysis.
	* Auto-tuner recommendations include AQE settings for optimized performance.
	* New configurations in Profiler for enabling off-default features: udfCompiler, incompatibleDateFormats, hasExtendedYearValues.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.12.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3
		Apache Spark 3.4.0, 3.4.1
		Apache Spark 3.5.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v23.12.1
- **Scala 2.12:**
  - [RAPIDS Accelerator for Apache Spark 23.12.1 - Scala 2.12 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.12.1/rapids-4-spark_2.12-23.12.1.jar)
  - [RAPIDS Accelerator for Apache Spark 23.12.1 - Scala 2.12 jar.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.12.1/rapids-4-spark_2.12-23.12.1.jar.asc)

- **Scala 2.13:**
  - [RAPIDS Accelerator for Apache Spark 23.12.1 - Scala 2.13 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/23.12.1/rapids-4-spark_2.13-23.12.1.jar)
  - [RAPIDS Accelerator for Apache Spark 23.12.1 - Scala 2.13 jar.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/23.12.1/rapids-4-spark_2.13-23.12.1.jar.asc)

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-23.12.1.jar.asc rapids-4-spark_2.12-23.12.1.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-23.12.1.jar.asc rapids-4-spark_2.13-23.12.1.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Introduced support for chunked reading of ORC files.
* Enhanced support for additional time zones and added stack function support.
* Enhanced performance for join and aggregation operations.
* Kernel optimizations have been implemented to improve Parquet read performance.
* RAPIDS Accelerator also built and tested with Scala 2.13.
* Last version to support Pascal-based Nvidia GPUs; discontinued in the next release.
* Introduced support for parquet Legacy rebase mode (spark.sql.parquet.datetimeRebaseModeInRead=LEGACY and spark.sql.parquet.int96RebaseModeInRead=LEGACY)
* Introduced support for Percentile function.
* Delta lake 2.3 support.
* Qualification and Profiling tool:
	* Profiling Tool now processes Spark Driver log for GPU runs, enhancing feature analysis.
	* Auto-tuner recommendations include AQE settings for optimized performance.
	* New configurations in Profiler for enabling off-default features: udfCompiler, incompatibleDateFormats, hasExtendedYearValues.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.12.0
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12, 2.13
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3
		Apache Spark 3.4.0, 3.4.1
		Apache Spark 3.5.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1
	
	Supported Dataproc Serverless versions:
		Spark runtime 1.1 LTS

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download RAPIDS Accelerator for Apache Spark v23.12.0
- **Scala 2.12:**
  - [RAPIDS Accelerator for Apache Spark 23.12.0 - Scala 2.12 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.12.0/rapids-4-spark_2.12-23.12.0.jar)
  - [RAPIDS Accelerator for Apache Spark 23.12.0 - Scala 2.12 jar.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.12.0/rapids-4-spark_2.12-23.12.0.jar.asc)

- **Scala 2.13:**
  - [RAPIDS Accelerator for Apache Spark 23.12.0 - Scala 2.13 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/23.12.0/rapids-4-spark_2.13-23.12.0.jar)
  - [RAPIDS Accelerator for Apache Spark 23.12.0 - Scala 2.13 jar.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.13/23.12.0/rapids-4-spark_2.13-23.12.0.jar.asc)

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature for Scala 2.12 jar:
    `gpg --verify rapids-4-spark_2.12-23.12.0.jar.asc rapids-4-spark_2.12-23.12.0.jar`
* Verify the signature for Scala 2.13 jar:
    `gpg --verify rapids-4-spark_2.13-23.12.0.jar.asc rapids-4-spark_2.13-23.12.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Introduced support for chunked reading of ORC files.
* Enhanced support for additional time zones and added stack function support.
* Enhanced performance for join and aggregation operations.
* Kernel optimizations have been implemented to improve Parquet read performance.
* RAPIDS Accelerator also built and tested with Scala 2.13.
* Last version to support Pascal-based Nvidia GPUs; discontinued in the next release.
* Introduced support for parquet Legacy rebase mode (spark.sql.parquet.datetimeRebaseModeInRead=LEGACY and spark.sql.parquet.int96RebaseModeInRead=LEGACY)
* Introduced support for Percentile function.
* Delta lake 2.3 support.
* Qualification and Profiling tool:
	* Profiling Tool now processes Spark Driver log for GPU runs, enhancing feature analysis.
	* Auto-tuner recommendations include AQE settings for optimized performance.
	* New configurations in Profiler for enabling off-default features: udfCompiler, incompatibleDateFormats, hasExtendedYearValues.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.10.0
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1. 
		Please be aware that we do not currently support Spark builds with Scala 2.13.

	Supported Spark versions:
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2, 3.3.3
		Apache Spark 3.4.0, 3.4.1
		Apache Spark 3.5.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

#### RAPIDS Accelerator's Support Policy for Apache Spark
The RAPIDS Accelerator maintains support for Apache Spark versions available for download from [Apache Spark](https://spark.apache.org/downloads.html)

### Download v23.10.0
* Download the [RAPIDS
  Accelerator for Apache Spark 23.10.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.10.0/rapids-4-spark_2.12-23.10.0.jar)

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.10.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.10.0/rapids-4-spark_2.12-23.10.0.jar)
  and [RAPIDS Accelerator for Apache Spark 23.10.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.10.0/rapids-4-spark_2.12-23.10.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.10.0.jar.asc rapids-4-spark_2.12-23.10.0.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Introduced support for Spark 3.5.0.
* Improved memory management for better control in YARN and K8s on CSP.
* Strengthened Parquet and ORC tests for enhanced stability and support.
* Reduce GPU out-of-memory (OOM) occurrences.
* Enhanced driver log with actionable insights.
* Qualification and Profiling tool:
	* Enhanced user experience with the availability of the 'ascli' tool for qualification and 
	profiling across all platforms.
	* The qualification tool now accommodates CPU-fallback transitions and broadens the speedup factor coverage.
	* Extended diagnostic support for user tools to cover EMR, Databricks AWS, and Databricks Azure.
	* Introduced support for cluster configuration recommendations in the profiling tool for supported platforms.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.08.2
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1. 
		Please be aware that we do not currently support Spark builds with Scala 2.13.

	Supported Spark versions:
		Apache Spark 3.1.1, 3.1.2, 3.1.3 
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2
		Apache Spark 3.4.0, 3.4.1
		Apache Spark 3.5.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v23.08.2
* Download the [RAPIDS
  Accelerator for Apache Spark 23.08.2 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.2/rapids-4-spark_2.12-23.08.2.jar)

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

Note that v23.08.1 is deprecated.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.08.2 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.2/rapids-4-spark_2.12-23.08.2.jar)
  and [RAPIDS Accelerator for Apache Spark 23.08.2 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.2/rapids-4-spark_2.12-23.08.2.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.08.2.jar.asc rapids-4-spark_2.12-23.08.2.jar`

The output of signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Compatibility with Databricks AWS & Azure 12.2 ML LTS.
* Enhanced stability and support for ORC and Parquet.
* Reduction of out-of-memory (OOM) occurrences.
* Corner case evaluation for data formats, operators and expressions
* Qualification and Profiling tool:
  * Profiling tool now supports Azure Databricks and AWS Databricks.
  * Qualification tool can provide advice on unaccelerated operations.
  * Improve user experience through CLI design.
  * Qualification tool provides configuration and migration recommendations for Dataproc and EMR.
* Fixes Databricks build issues from the previous 23.08 release.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.08.1
### Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4, A10/A100, L4 and H100 GPUs

### Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	NVIDIA Driver*: R470+

	Runtime: 
		Scala 2.12
		Python, Java Virtual Machine (JVM) compatible with your spark-version. 

		* Check the Spark documentation for Python and Java version compatibility with your specific 
		Spark version. For instance, visit `https://spark.apache.org/docs/3.4.1` for Spark 3.4.1. 
		Please be aware that we do not currently support Spark builds with Scala 2.13.

	Supported Spark versions:
		Apache Spark 3.1.1, 3.1.2, 3.1.3 
		Apache Spark 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4
		Apache Spark 3.3.0, 3.3.1, 3.3.2
		Apache Spark 3.4.0, 3.4.1
		Apache Spark 3.5.0
	
	Supported Databricks runtime versions for Azure and AWS:
		Databricks 10.4 ML LTS (GPU, Scala 2.12, Spark 3.2.1)
		Databricks 11.3 ML LTS (GPU, Scala 2.12, Spark 3.3.0)
		Databricks 12.2 ML LTS (GPU, Scala 2.12, Spark 3.3.2)
	
	Supported Dataproc versions:
		GCP Dataproc 2.0
		GCP Dataproc 2.1

*Some hardware may have a minimum driver version greater than R470. Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v23.08.1
* Download the [RAPIDS
  Accelerator for Apache Spark 23.08.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.1/rapids-4-spark_2.12-23.08.1.jar)

This package is built against CUDA 11.8. It is tested on V100, T4, A10, A100, L4 and H100 GPUs with 
CUDA 11.8 through CUDA 12.0.

Note that v23.08.0 is deprecated.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.08.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.1/rapids-4-spark_2.12-23.08.1.jar)
  and [RAPIDS Accelerator for Apache Spark 23.08.1 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.08.1/rapids-4-spark_2.12-23.08.1.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.08.1.jar.asc rapids-4-spark_2.12-23.08.1.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Compatibility with Databricks AWS & Azure 12.2 ML LTS.
* Enhanced stability and support for ORC and Parquet.
* Reduction of out-of-memory (OOM) occurrences.
* Corner case evaluation for data formats, operators and expressions
* Qualification and Profiling tool:
  * Profiling tool now supports Azure Databricks and AWS Databricks.
  * Qualification tool can provide advice on unaccelerated operations.
  * Improve user experience through CLI design.
  * Qualification tool provides configuration and migration recommendations for Dataproc and EMR.
  
For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.06.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v470+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.2.4, 3.3.0, 3.3.1, 3.3.2, 3.4.0 Databricks 10.4 ML LTS or 11.3 ML LTS Runtime and GCP Dataproc 2.0, Dataproc 2.1

	Python 3.6+, Scala 2.12, Java 8, Java 17

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v23.06.0
* Download the [RAPIDS
  Accelerator for Apache Spark 23.06.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.06.0/rapids-4-spark_2.12-23.06.0.jar)

This package is built against CUDA 11.8, all CUDA 11.x and 12.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30, A100, L4 and H100  GPUs with CUDA 11.8-12.0.  For those using other types of GPUs 
which do not have CUDA forward compatibility (for example, GeForce), CUDA 11.8 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.06.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.06.0/rapids-4-spark_2.12-23.06.0.jar)
  and [RAPIDS Accelerator for Apache Spark 23.06.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.06.0/rapids-4-spark_2.12-23.06.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.06.0.jar.asc rapids-4-spark_2.12-23.06.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Enhanced operator support with an OOM retry framework to minimize OOM or GPU specific config changes 
* Spill framework to reduce OOM issues to minimize OOM or GPU specific config changes 
* AQE for skewed broadcast hash join performance improvement
* Support JSON to struct
* Support StringTranslate 
* Support windows function with string input in order by clause
* Support regular expressions with line anchors in choice input  
* Support rlike function with line anchor input
* Improve the performance of ORC small file reads
* Qualification and Profiling tool:
  * Qualification tool support for Azure Databricks
  * The Qualification and Profiling tools do not require a live cluster, and only require read permissions on clusters
  * Improve Profiling tool recommendations to support more tuning options
  
  
For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.04.1
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 20.04, Ubuntu 22.04, CentOS 7, or Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.3.0, 3.3.1, 3.3.2, Databricks 10.4 ML LTS or 11.3 ML LTS Runtime and GCP Dataproc 2.0, Dataproc 2.1

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v23.04.1
* Download the [RAPIDS
  Accelerator for Apache Spark 23.04.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.04.1/rapids-4-spark_2.12-23.04.1.jar)

This package is built against CUDA 11.8 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.04.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.04.1/rapids-4-spark_2.12-23.04.1.jar)
  and [RAPIDS Accelerator for Apache Spark 23.04.1 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.04.1/rapids-4-spark_2.12-23.04.1.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.04.1.jar.asc rapids-4-spark_2.12-23.04.1.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
The 23.04.1 release patches a possible driver OOM which can occur on an executor broadcast.
New functionality and performance improvements for this release include:
* Introduces OOM retry framework for automatic OOM handling in memory-intensive operators, such as: join, aggregates and windows, coalescing, projections and filters.
* Support dynamic repartitioning in large/skewed hash joins
* Optimize the transpilation in `regexp_extract` function
* Support Delta Lake write with auto-optimization and auto-compaction on Databricks platforms
* Qualification and Profiling tool:
  * Add support to recommend cluster shape options on Dataproc and EMR
  * Add support for Databricks local mode with cost savings based on cluster metadata
  * Add TCO calculator to estimate annualized cost savings, including estimated frequency for applications
  * Add support in the qualification tool to generate estimated speed-up for ML functionality in Spark applications
  
  
For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v23.02.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.3.0, 3.3.1, Databricks 10.4 ML LTS or 11.3 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v23.02.0
* Download the [RAPIDS
  Accelerator for Apache Spark 23.02.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.02.0/rapids-4-spark_2.12-23.02.0.jar)

This package is built against CUDA 11.8 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 23.02.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.02.0/rapids-4-spark_2.12-23.02.0.jar)
  and [RAPIDS Accelerator for Apache Spark 23.02.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/23.02.0/rapids-4-spark_2.12-23.02.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-23.02.0.jar.asc rapids-4-spark_2.12-23.02.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Delta Lake MERGE/DELETE/UPDATE (experimental feature, can be enabled with a config flag)
* Function `from_json`
* Hive text table write 
* Databricks 11.3 ML LTS support
* Support batched full join to improve full join's performance
* Qualification and Profiling tool:
  * EMR user tools support for qualification
  * EMR user tools support for bootstrap
  * Updated estimated speedup factors for on-prem, Dataproc, and EMR environments for qualification
  
  
For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.12.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.2.3, 3.3.0, 3.3.1, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v22.12.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.12.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.12.0/rapids-4-spark_2.12-22.12.0.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.12.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.12.0/rapids-4-spark_2.12-22.12.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.12.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.12.0/rapids-4-spark_2.12-22.12.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.12.0.jar.asc rapids-4-spark_2.12-22.12.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Zstandard compression for Parquet and ORC
* Support for Hive text reading
* Improved performance on `like` operations
* Tiered projections for more expressions to optimize performance
* Support for mapInArrow, instr and array_remove operations
* z-ordering capability on Databricks Delta Lake
* Dynamic Partition Pruning (DPP) on Databricks
* Qualification and Profiling tool:
  * Support cost estimations for Dataproc 1.5 and Dataproc2.x
  * Added new Github [repo](https://github.com/NVIDIA/spark-rapids-tools/tree/dev/user_tools) for user tools functionality

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.10.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA P100, V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.3.0, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v22.10.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.10.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.10.0/rapids-4-spark_2.12-22.10.0.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.10.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.10.0/rapids-4-spark_2.12-22.10.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.10.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.10.0/rapids-4-spark_2.12-22.10.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.10.0.jar.asc rapids-4-spark_2.12-22.10.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Dataproc qualification, profiling, bootstrap and diagnostic tool
* Databricks custom docker container
* AQE support on Databricks
* MultiThreaded Shuffle feature
* Binary write support for parquet
* Cast binary to string  
* Hive parquet table write support
* Qualification and Profiling tool:
  * Print Databricks cluster/job information
  * AutoTuner for Profiling tool

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.08.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, Rocky Linux 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.2.2, 3.3.0, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v22.08.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.08.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.08.0/rapids-4-spark_2.12-22.08.0.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.08.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.08.0/rapids-4-spark_2.12-22.08.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.08.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.08.0/rapids-4-spark_2.12-22.08.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.08.0.jar.asc rapids-4-spark_2.12-22.08.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Rocky Linux 8 support
* Ability to build Spark RAPIDS jars for Java versions 9+
* Zstandard Parquet and ORC read support
* Binary read support from parquet
* Apache Iceberg 0.13 support
* Array function support: array_intersect, array_union, array_except and arrays_overlap
* Support nth_value, first and last in windowing function
* Alluxio auto mount for AWS S3 buckets
* Qualification tool:
  * SQL level qualification
  * Add application details view

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.06.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, 3.3.0, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v22.06.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.06.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/rapids-4-spark_2.12-22.06.0.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.06.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/rapids-4-spark_2.12-22.06.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.06.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/rapids-4-spark_2.12-22.06.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.06.0.jar.asc rapids-4-spark_2.12-22.06.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Combined cuDF jar and rapids-4-spark jar to a single rapids-4-spark jar.
  The RAPIDS Accelerator jar (rapids-4-spark jar) is the only jar that needs to be passed to Spark.  
  The cuDF jar is now bundled with the rapids-4-spark jar and should not be specified.
* Enable CSV read by default
* Enable regular expression by default
* Enable some float related configurations by default
* Improved ANSI support
* Add a UI for the Qualification tool
* Support function map_filter
* Enable MIG with YARN on Dataproc 2.0
* Changed to ASYNC allocator from ARENA by default

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.04.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.1.1, 3.1.2, 3.1.3, 3.2.0, 3.2.1, Databricks 9.1 ML LTS or 10.4 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v22.04.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.04.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar)
* Download the [RAPIDS cuDF 22.04.0 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/22.04.0/cudf-22.04.0-cuda11.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.04.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.04.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.04.0.jar.asc rapids-4-spark_2.12-22.04.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Avro reader for primitive types
* ExistenceJoin support
* ArrayExists support
* GetArrayStructFields support
* Function str_to_map support
* Function percent_rank support
* Regular expression support for function split on string
* Support function approx_percentile in reduction context
* Support function element_at with non-literal index
* Spark cuSpatial UDF

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v22.02.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.0.3, 3.1.1, 3.1.2, 3.2.0, 3.2.1, Databricks 7.3 ML LTS or 9.1 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v22.02.0
* Download the [RAPIDS
  Accelerator for Apache Spark 22.02.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar)
* Download the [RAPIDS cuDF 22.02.0 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/22.02.0/cudf-22.02.0-cuda11.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 22.02.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar)
  and [RAPIDS Accelerator for Apache Spark 22.02.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.02.0/rapids-4-spark_2.12-22.02.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-22.02.0.jar.asc rapids-4-spark_2.12-22.02.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Parquet reader and writer support for decimal precision up to 38 digits (128-bits)
* Decimal 128-bits casting
    * Casting of decimal 128-bits values in nested types
    * Casting to String from decimal 128-bits
    * Casting from String to decimal 128-bits
* MIG on YARN support
* GPU explain only mode for Spark 3.x and 2.x
* JSON reader support
* Sequence function support
* regexp_extract function support
* Min and max on single-level struct
* CreateMap updates and enable CreateMap by default
* Cast from array to string
* Add regular expression support to regexp_replace function
* Support for conditional joins using libcudf's mixed join feature

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.12.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.x & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.0.3, 3.1.1, 3.1.2, 3.2.0, Databricks 7.3 ML LTS or 9.1 ML LTS Runtime and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v21.12.0
* Download the [RAPIDS
  Accelerator for Apache Spark 21.12.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.12.0/rapids-4-spark_2.12-21.12.0.jar)
* Download the [RAPIDS cuDF 21.12.2 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.12.2/cudf-21.12.2-cuda11.jar)

This package is built against CUDA 11.5 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.5.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.5 or later is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Verify signature
* Download the [RAPIDS Accelerator for Apache Spark 21.12.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.12.0/rapids-4-spark_2.12-21.12.0.jar)
  and [RAPIDS Accelerator for Apache Spark 21.12.0 jars.asc](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.12.0/rapids-4-spark_2.12-21.12.0.jar.asc)
* Download the [PUB_KEY](https://keys.openpgp.org/search?q=sw-spark@nvidia.com).
* Import the public key: `gpg --import PUB_KEY`
* Verify the signature: `gpg --verify rapids-4-spark_2.12-21.12.0.jar.asc rapids-4-spark_2.12-21.12.0.jar`

The output if signature verify:

	gpg: Good signature from "NVIDIA Spark (For the signature of spark-rapids release jars) <sw-spark@nvidia.com>"

### Release Notes
New functionality and performance improvements for this release include:
* Support decimal precision up to 38 digits (128-bits)
* Support stddev on double in window context
* Support CPU row-based UDF
* CreateArray outputs array of struct
* collect_set outputs array of struct
* ORC reader and writer support for decimal precision up to 38 digits (128-bits)
* ORC writer supports array, map, and struct
* Support SampleExec, rlike
* regexp_replace supports more patterns such as replacing null
* ParquetCachedBatchSerializer supports map
* Add function explainPotentialGpuPlan to print GPU query plan in a CPU Spark cluster
* Qualification Tool
    * Detect RDD APIs and JDBC Scan
* Profiling Tool
    * Catch OOM errors and log a hint to increase java heap size
    * Print potential problems

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.10.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A2/A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.0-11.4 & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.0.3, 3.1.1, 3.1.2, 3.2.0, Databricks 7.3 ML LTS or 8.2 ML Runtime, GCP Dataproc 2.0, and Azure Synapse

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v21.10.0
* Download the [RAPIDS
  Accelerator for Apache Spark 21.10.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.10.0/rapids-4-spark_2.12-21.10.0.jar)
* Download the [RAPIDS cuDF 21.10.0 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.10.0/cudf-21.10.0-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A2, A10, A30 and A100 GPUs with CUDA 11.0-11.4.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
New functionality and performance improvements for this release include:
* Support collect_list and collect_set in group-by aggregation
* Support stddev, percentile_approx in group-by aggregation
* RunningWindow operations on map
* HashAggregate on struct and nested struct
* Sorting on nested structs
* Explode on map, array, struct
* Union-all on map, array and struct of maps
* Parquet writing of map
* ORC reader supports reading map/struct columns
* ORC reader support decimal64
* Qualification Tool
    * Add conjunction and disjunction filters
    * Filtering specific configuration values
    * Filtering user name
    * Reporting nested data types
    * Reporting write data formats
* Profiling Tool
    * Generating structured output format
    * Improved profiling tool performance

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.08.0
Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.0-11.4 & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.0.3, 3.1.1, 3.1.2, Databricks 7.3 ML LTS or 8.2 ML Runtime, and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v21.08.0
* Download the [RAPIDS
  Accelerator for Apache Spark 21.08.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.08.0/rapids-4-spark_2.12-21.08.0.jar)
* Download the [RAPIDS cuDF 21.08.2 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.08.2/cudf-21.08.2-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A30 and A100 GPUs with CUDA 11.0-11.4.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
New functionality and performance improvements for this release include:
* Handling data sets that spill out of GPU memory for group by and windowing operations
* Running window rank and dense rank operations on the GPU
* Support for the `LEGACY` timestamp
* Unioning of nested structs
* Adoption of UCX 1.11 for improved error handling for RAPIDS Spark Accelerated Shuffle
* Ability to read cached data from the GPU on the supported Databricks runtimes
* Enabling Parquet writing of array data types from the GPU
* Optimized reads for small files for ORC
* Qualification and Profiling Tools
    * Additional filtering capabilities
    * Reporting on data types
    * Reporting on read data formats
    * Ability to run the Qualification tool on Spark 2.x logs
    * Ability to run the tool on Apache Spark 3.x, AWS EMR 6.3.0, Dataproc 2.0, Microsoft Azure, and
      Databricks 7.3 and 8.2 logs
    * Improved Qualification tool performance

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.06.2
This is a patch release to address an issue with the plugin in the Databricks 8.2 ML runtime.

Hardware Requirements:

	GPU Models: NVIDIA V100, T4 or A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.0 or 11.2 & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.1.1, 3.1.2, Databricks 7.3 ML LTS or 8.2 ML Runtime, and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v21.06.2
* Download the [RAPIDS
  Accelerator for Apache Spark 21.06.2 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.06.2/rapids-4-spark_2.12-21.06.2.jar)
* Download the [RAPIDS cuDF 21.06.1 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.06.1/cudf-21.06.1-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on Tesla datacenter GPUs with CUDA 11.0 and 11.2.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
This release patches the plugin to address a backwards incompatible change to Parquet filters made
by Databricks in the Databricks 8.2 ML runtime.  More information is in [issue
3191](https://github.com/NVIDIA/spark-rapids/issues/3191) in the RAPIDS Spark repository.  See the
[Release v21.06.0](#release-v21060) release notes for more detail about new features in 21.06.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.06.1
This is a patch release to address an issue with the plugin in the Databricks 7.3 ML LTS runtime.

Hardware Requirements:

	GPU Models: NVIDIA V100, T4 or A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8

	CUDA & NVIDIA Drivers*: 11.0 or 11.2 & v450.80.02+

	Apache Spark 3.0.1, 3.0.2, 3.1.1, 3.1.2, and GCP Dataproc 2.0

	Python 3.6+, Scala 2.12, Java 8

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v21.06.1
* Download the [RAPIDS
  Accelerator for Apache Spark 21.06.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.06.1/rapids-4-spark_2.12-21.06.1.jar)
* Download the [RAPIDS cuDF 21.06.1 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.06.1/cudf-21.06.1-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on Tesla datacenter GPUs with CUDA 11.0 and 11.2.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
This release patches the plugin to address a backwards incompatible change to Parquet filters made
by Databricks in the Databricks 7.3 ML LTS runtime.  More information is in [issue
3098](https://github.com/NVIDIA/spark-rapids/issues/3098) in the RAPIDS Spark repository.  See the
[Release v21.06.0](#release-v21060) release notes for more detail about new features in 21.06.

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v21.06.0
Starting with release 21.06.0, the project is moving to calendar versioning, with the first two
digits representing the year, the second two digits representing the month, and the last digit
representing the patch version of the release.

Hardware Requirements:

The plugin is tested on the following architectures:

	GPU Models: NVIDIA V100, T4 and A10/A30/A100 GPUs

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS 8
	
	CUDA & NVIDIA Drivers*: 11.0 or 11.2 & v450.80.02+
	
	Apache Spark 3.0.1, 3.0.2, 3.1.1, 3.1.2, Databricks 8.2 ML Runtime, and GCP Dataproc 2.0
		
	Python 3.6+, Scala 2.12, Java 8 

*Some hardware may have a minimum driver version greater than v450.80.02+.  Check the GPU spec sheet
for your hardware's minimum driver version.

*For Cloudera and EMR support, please refer to the
[Distributions](https://docs.nvidia.com/spark-rapids/user-guide/latest/faq.html#which-distributions-are-supported) section of the FAQ.

### Download v21.06.0
* Download the [RAPIDS
  Accelerator for Apache Spark 21.06.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/21.06.0/rapids-4-spark_2.12-21.06.0.jar)
* Download the [RAPIDS cuDF 21.06.1 jar](https://repo1.maven.org/maven2/ai/rapids/cudf/21.06.1/cudf-21.06.1-cuda11.jar)

This package is built against CUDA 11.2 and all CUDA 11.x versions are supported through [CUDA forward
compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/index.html). It is tested
on V100, T4, A30 and A100 GPUs with CUDA 11.0 and 11.2.  For those using other types of GPUs which
do not have CUDA forward compatibility (for example, GeForce), CUDA 11.2 is required. Users will
need to ensure the minimum driver (450.80.02) and CUDA toolkit are installed on each Spark node.

### Release Notes
New functionality for this release includes:
* Support for running on Cloudera CDP 7.1.6, CDP 7.1.7 and Databricks 8.2 ML
* New functionality related to arrays:
    * Concatenation of array columns
    * Casting arrays of floats to arrays of doubles
    * Creation of 2D array types
    * Hash partitioning with arrays
    * Explode takes expressions that generate arrays
* New functionality for struct types:
    * Sorting on struct keys
    * Structs with map values
    * Caching of structs
* New windowing functionality:
    * Window lead / lag for arrays
    * Range windows supporting non-timestamp order by expressions
* Enabling large joins that can spill out of memory
* Support for the `concat_ws` operator
* Qualification and Profiling Tools
    * Qualification tool looks at a set of applications to determine if the RAPIDS Accelerator for
      Apache Spark is a good fit
    * Profiling tool to generate information used for debugging and profiling applications

Performance improvements for this release include:
* Moving RAPIDS Shuffle out of beta
* Updates to UCX error handling
* GPUDirect Storage for spilling

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

## Release v0.5.0

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

Hardware Requirements:

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)

Software Requirements:

	OS: Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS8
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
		
	Python 3.6+, Scala 2.12, Java 8 

### Download v0.5.0
* Download the [RAPIDS Accelerator for Apache Spark v0.5.0 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.5.0/rapids-4-spark_2.12-0.5.0.jar)
* Download the RAPIDS cuDF 0.19.2 jar for your system:
    * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda11.jar)
    * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda10-2.jar)
    * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda10-1.jar)

### Release Notes
New functionality for this release includes:
* Additional support for structs, including casting structs to string, hashing structs, unioning
  structs, and allowing array types and structs to pass through when doing joins
* Support for `get_json_object`, `pivot`, `explode` operators
* Casting string to decimal and decimal to string

Performance improvements for this release include:
* Optimizing unnecessary columnar->row->columnar transitions with AQE
* Supporting out of core sorts
* Initial support for cost based optimization
* Decimal32 support
* Accelerate data transfer for map Pandas UDF
* Allow spilled buffers to be unspilled


## Release v0.4.1
### Download v0.4.1
* Download the [RAPIDS Accelerator For Apache Spark v0.4.1 jar](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.4.1/rapids-4-spark_2.12-0.4.1.jar)
* Download the RAPIDS cuDF 0.18.1 jar for your system:
    * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda11.jar)
    * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-2.jar)
    * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-1.jar)

### Requirements
Hardware Requirements:

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)

Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
		
	Python 3.6+, Scala 2.12, Java 8 

### Release Notes
This is a patch release based on version 0.4.0 with the following additional fixes:
* Broadcast exchange can fail when job group is set

The release is supported on Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS and
Google Cloud Platform Dataproc 2.0.

The list of all supported operations is provided [here](supported_ops.md).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

**_Note:_** Using NVIDIA driver release 450.80.02, 450.102.04 or 460.32.03 in combination with the
CUDA 10.1 or 10.2 toolkit may result in long read times when reading a file that is snappy
compressed.  In those cases we recommend either running with the CUDA 11.0 toolkit or using a newer
driver.  This issue is resolved in the 0.5.0 and higher releases.

## Release v0.4.0
### Download v0.4.0
* Download [RAPIDS Accelerator For Apache Spark v0.4.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.4.0/rapids-4-spark_2.12-0.4.0.jar)
* Download RAPIDS cuDF 0.18.1 for your system:
    * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda11.jar)
    * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-2.jar)
    * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda10-1.jar)

### Requirements
Hardware Requirements:

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)

Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
		
	Python 3.6+, Scala 2.12, Java 8 

### Release Notes
New functionality for the release includes
* Decimal support up to 64 bit, including reading and writing decimal from Parquet (can be enabled
  by setting `spark.rapids.sql.decimalType.enabled` to True)
* Ability for users to provide GPU versions of Scala, Java or Hive UDFs
* Shuffle and sort support for `struct` data types
* `array_contains` for list operations
* `collect_list` and `average` for windowing operations
* Murmur3 `hash` operation
* Improved performance when reading from DataSource v2 when the source produces data in the Arrow format

This release includes additional performance improvements, including
* RAPIDS Shuffle with UCX performance improvements
* Instructions on how to use Alluxio caching with Spark to leverage caching.

The release is supported on Apache Spark 3.0.0, 3.0.1, 3.0.2, 3.1.1, Databricks 7.3 ML LTS and
Google Cloud Platform Dataproc 2.0.

The list of all supported operations is provided [here](supported_ops.md).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

**_Note:_** Using NVIDIA driver release 450.80.02, 450.102.04 or 460.32.03 in combination with the
CUDA 10.1 or 10.2 toolkit may result in long read times when reading a file that is snappy
compressed.  In those cases we recommend either running with the CUDA 11.0 toolkit or using a newer
driver.  This issue is resolved in the 0.5.0 and higher releases.

## Release v0.3.0
### Download v0.3.0
* Download [RAPIDS Accelerator For Apache Spark v0.3.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.3.0/rapids-4-spark_2.12-0.3.0.jar)
* Download RAPIDS cuDF 0.17 for your system:
    * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda11.jar)
    * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda10-2.jar)
    * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda10-1.jar)

### Requirements
Hardware Requirements:

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)

Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1, Databricks 7.3 ML LTS Runtime, or GCP Dataproc 2.0 
		
	Python 3.6+, Scala 2.12, Java 8 

### Release Notes
This release includes additional performance improvements, including
* Use of per thread default stream to make more efficient use of the GPU
* Further supporting Spark's adaptive query execution, with more rewritten query plans now able to
  run on the GPU
* Performance improvements for reading small Parquet files
* RAPIDS Shuffle with UCX updated to UCX 1.9.0

New functionality for the release includes
* Parquet reading for lists and structs,
* Lead/lag for windows, and
* Greatest/least operators

The release is supported on Apache Spark 3.0.0, 3.0.1, Databricks 7.3 ML LTS and Google Cloud
Platform Dataproc 2.0.

The list of all supported operations is provided [here](supported_ops.md).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

**_Note:_** Using NVIDIA driver release 450.80.02, 450.102.04 or 460.32.03 in combination with the
CUDA 10.1 or 10.2 toolkit may result in long read times when reading a file that is snappy
compressed.  In those cases we recommend either running with the CUDA 11.0 toolkit or using a newer
driver.  This issue is resolved in the 0.5.0 and higher releases.

## Release v0.2.0
### Download v0.2.0
* Download [RAPIDS Accelerator For Apache Spark v0.2.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.2.0/rapids-4-spark_2.12-0.2.0.jar)
* Download RAPIDS cuDF 0.15 for your system:
    * [For CUDA 11.0 & NVIDIA driver 450.36+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda11.jar)
    * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda10-2.jar)
    * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.15/cudf-0.15-cuda10-1.jar)

### Requirements
Hardware Requirements:

	GPU Architecture: NVIDIA Pascal™ or better (Tested on V100, T4 and A100 GPU)

Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
	
	CUDA & NVIDIA Drivers: 10.1.2 & v418.87+, 10.2 & v440.33+ or 11.0 & v450.36+
	
	Apache Spark 3.0, 3.0.1
		
	Python 3.x, Scala 2.12, Java 8 

### Release Notes
This is the second release of the RAPIDS Accelerator for Apache Spark.  Adaptive Query Execution
[SPARK-31412](https://issues.apache.org/jira/browse/SPARK-31412) is a new enhancement that was
included in Spark 3.0 that alters the physical execution plan dynamically to improve the performance
of the query.  The RAPIDS Accelerator v0.2 introduces Adaptive Query Execution (AQE) for GPUs and
leverages columnar processing [SPARK-32332](https://issues.apache.org/jira/browse/SPARK-32332)
starting from Spark 3.0.1.

Another enhancement in v0.2 is improvement in reading small Parquet files.  This feature takes into
account the scenario where input data can be stored across many small files.  By leveraging multiple
CPU threads v0.2 delivers up to 6x performance improvement over the previous release for small
Parquet file reads.

The RAPIDS Accelerator introduces a beta feature that accelerates
[Spark shuffle for GPUs](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/on-premise.html#enabling-rapids-shuffle-manager).
Accelerated shuffle makes use of high bandwidth transfers between GPUs (NVLink or p2p over PCIe) and
leverages RDMA (RoCE or Infiniband) for remote transfers.

The list of all supported operations is provided
[here](configs.md#supported-gpu-operators-and-fine-tuning).

For a detailed list of changes, please refer to the
[CHANGELOG](https://github.com/NVIDIA/spark-rapids/blob/main/CHANGELOG.md).

**_Note:_** Using NVIDIA driver release 450.80.02, 450.102.04 or 460.32.03 in combination with the
CUDA 10.1 or 10.2 toolkit may result in long read times when reading a file that is snappy
compressed.  In those cases we recommend either running with the CUDA 11.0 toolkit or using a newer
driver.  This issue is resolved in the 0.5.0 and higher releases.

## Release v0.1.0
### Download v0.1.0
* Download [RAPIDS Accelerator For Apache Spark v0.1.0](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.1.0/rapids-4-spark_2.12-0.1.0.jar)
* Download RAPIDS cuDF 0.14 for your system:
    * [For CUDA 10.2 & NVIDIA driver 440.33+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-2.jar)
    * [For CUDA 10.1 & NVIDIA driver 418.87+](https://repo1.maven.org/maven2/ai/rapids/cudf/0.14/cudf-0.14-cuda10-1.jar)

### Requirements
Hardware Requirements:

    GPU Architecture: NVIDIA Pascal™ or better (Tested on V100 and T4 GPU)

Software Requirements:

	OS: Ubuntu 16.04, Ubuntu 18.04 or CentOS 7
    (RHEL 7 support is provided through CentOS 7 builds/installs)

    CUDA & NVIDIA Drivers: 10.1.2 & v418.87+ or 10.2 & v440.33+
    
    Apache Spark 3.0

    Python 3.x, Scala 2.12, Java 8



