/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.jni.kudo.DumpOption

import org.apache.spark.network.util.ByteUnit

private[rapids] trait RapidsConfEntries extends RapidsConfSqlEntries {

  // INTERNAL TEST AND DEBUG CONFIGS

  val TEST_RETRY_OOM_INJECTION_MODE = conf("spark.rapids.sql.test.injectRetryOOM")
    .doc("Only to be used in tests. If `true` the retry iterator will inject a GpuRetryOOM " +
         "or CpuRetryOOM once per invocation. Furthermore an extended config " +
         "`num_ooms=<int>,skip=<int>,type=CPU|GPU|CPU_OR_GPU,split=<bool>` can be provided to " +
         "specify the number of OOMs to generate; how many to skip before doing so; whether to " +
         "filter by allocation events by host(CPU), device(GPU), or both (CPU_OR_GPU); " +
         "whether to inject *SplitAndRetryOOM instead of plain *RetryOOM exceptions." +
         "Note *SplitAndRetryOOM exceptions are not always handled - use with care.")
    .internal()
    .stringConf
    .createWithDefault(false.toString)

  val FOLDABLE_NON_LIT_ALLOWED = conf("spark.rapids.sql.test.isFoldableNonLitAllowed")
    .doc("Only to be used in tests. If `true` the foldable expressions that are not literals " +
      "will be allowed")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val TEST_RETRY_CONTEXT_CHECK_ENABLED = conf("spark.rapids.sql.test.retryContextCheck.enabled")
    .doc("Only to be used in tests. When set to true, enable the context check for " +
      "GPU nondeterministic expressions but declaring to be retryable. A GPU retryable " +
      "nondeterministic expression should run inside a checkpoint-restore context. And it " +
      "will blow up when the context does not satisfy.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val TEST_CONF = conf("spark.rapids.sql.test.enabled")
    .doc("Intended to be used by unit tests, if enabled all operations must run on the " +
      "GPU or an error happens.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val TEST_ALLOWED_NONGPU = conf("spark.rapids.sql.test.allowedNonGpu")
    .doc("Comma separate string of exec or expression class names that are allowed " +
      "to not be GPU accelerated for testing.")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val TEST_VALIDATE_EXECS_ONGPU = conf("spark.rapids.sql.test.validateExecsInGpuPlan")
    .doc("Comma separate string of exec class names to validate they " +
      "are GPU accelerated. Used for testing.")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val HASH_SUB_PARTITION_TEST_ENABLED = conf("spark.rapids.sql.test.subPartitioning.enabled")
    .doc("Setting to true will force hash joins to use the sub-partitioning algorithm if " +
      s"${TEST_CONF.key} is also enabled, while false means always disabling it. This is " +
      "intended for tests. Do not set any value under production environments, since it " +
      "will override the default behavior that will choose one automatically according to " +
      "the input batch size")
    .internal()
    .booleanConf
    .createOptional

  val LOG_TRANSFORMATIONS = conf("spark.rapids.sql.debug.logTransformations")
    .doc("When enabled, all query transformations will be logged.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val OUTPUT_DEBUG_DUMP_PREFIX = conf("spark.rapids.sql.output.debug.dumpPrefix")
    .doc("A path prefix where data that is intended to be written out as the result " +
      "of a query should be dumped for debugging. The format of this is based on " +
      "JCudfSerialization and is trying to capture the underlying table so that if " +
      "there are errors in the output format we can try to recreate it.")
    .internal()
    .stringConf
    .createOptional

  val PARQUET_DEBUG_DUMP_PREFIX = conf("spark.rapids.sql.parquet.debug.dumpPrefix")
    .doc("A path prefix where Parquet split file data is dumped for debugging.")
    .internal()
    .stringConf
    .createOptional

  val PARQUET_DEBUG_DUMP_ALWAYS = conf("spark.rapids.sql.parquet.debug.dumpAlways")
    .doc(s"This only has an effect if $PARQUET_DEBUG_DUMP_PREFIX is set. If true then " +
      "Parquet data is dumped for every read operation otherwise only on a read error.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val ORC_DEBUG_DUMP_PREFIX = conf("spark.rapids.sql.orc.debug.dumpPrefix")
    .doc("A path prefix where ORC split file data is dumped for debugging.")
    .internal()
    .stringConf
    .createOptional

  val ORC_DEBUG_DUMP_ALWAYS = conf("spark.rapids.sql.orc.debug.dumpAlways")
    .doc(s"This only has an effect if $ORC_DEBUG_DUMP_PREFIX is set. If true then " +
      "ORC data is dumped for every read operation otherwise only on a read error.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val AVRO_DEBUG_DUMP_PREFIX = conf("spark.rapids.sql.avro.debug.dumpPrefix")
    .doc("A path prefix where AVRO split file data is dumped for debugging.")
    .internal()
    .stringConf
    .createOptional

  val AVRO_DEBUG_DUMP_ALWAYS = conf("spark.rapids.sql.avro.debug.dumpAlways")
    .doc(s"This only has an effect if $AVRO_DEBUG_DUMP_PREFIX is set. If true then " +
      "Avro data is dumped for every read operation otherwise only on a read error.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val HYBRID_PARQUET_READER = conf("spark.rapids.sql.hybrid.parquet.enabled")
    .doc("Use HybridScan to read Parquet data using CPUs. The underlying implementation " +
      "leverages both Gluten and Velox. Supports Spark 3.2.2, 3.3.1, 3.4.2, and 3.5.1 " +
      "as Gluten does, also supports other versions but not fully tested.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val HYBRID_PARQUET_PRELOAD_CAP = conf("spark.rapids.sql.hybrid.parquet.numPreloadedBatches")
    .doc("Preloading capacity of HybridParquetScan. If > 0, will enable preloading" +
      " the result of HybridParquetScan asynchronously in a separate thread")
    .internal()
    .integerConf
    .createWithDefault(0)

  // This config name is the same as HybridPluginWrapper in Hybrid jar,
  // can not refer to Hybrid jar because of the jar is optional.
  val LOAD_HYBRID_BACKEND = conf("spark.rapids.sql.hybrid.loadBackend")
    .doc("Load hybrid backend as an extra plugin of cuDF plugin during launch time")
    .internal()
    .startupOnly()
    .booleanConf
    .createWithDefault(false)

  val PUSH_DOWN_FILTERS_TO_HYBRID = conf("spark.rapids.sql.hybrid.parquet.filterPushDown")
    .doc("Push down all supported filters to CPU if set to CPU. " +
      "If set to GPU, no filters will be pushed down so all filters are on the GPU. " +
      "If set to OFF, filters will be both pushed down and keeped on the GPU. " +
      "OFF is to make the behavior same as before.")
    .internal()
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(Set("CPU", "GPU", "OFF"))
    .createWithDefault("CPU")

  val HYBRID_EXPRS_WHITELIST = conf("spark.rapids.sql.hybrid.whitelistExprs")
    .doc("White list of expressions that can be pushed down to CPU. " +
      "The expressions are separated by comma.")
    .internal()
    .stringConf
    .createWithDefault("")

  val HASH_AGG_REPLACE_MODE = conf("spark.rapids.sql.hashAgg.replaceMode")
    .doc("Only when hash aggregate exec has these modes (\"all\" by default): " +
      "\"all\" (try to replace all aggregates, default), " +
      "\"complete\" (exclusively replace complete aggregates), " +
      "\"partial\" (exclusively replace partial aggregates), " +
      "\"final\" (exclusively replace final aggregates)." +
      " These modes can be connected with &(AND) or |(OR) to form sophisticated patterns.")
    .internal()
    .stringConf
    .createWithDefault("all")

  val PARTIAL_MERGE_DISTINCT_ENABLED = conf("spark.rapids.sql.partialMerge.distinct.enabled")
    .doc("Enables aggregates that are in PartialMerge mode to run on the GPU if true")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_MANAGER_ENABLED = conf("spark.rapids.shuffle.enabled")
    .doc("Enable or disable the RAPIDS Shuffle Manager implementation at runtime. On supported " +
      "Spark versions, including Spark 4.0.0 and later, the " +
      "[RAPIDS Shuffle Manager](https://docs.nvidia.com/spark-rapids/user-guide/latest" +
      "/additional-functionality/rapids-shuffle.html) is configured automatically unless " +
      "spark.shuffle.manager is explicitly set. Note: Databricks runtimes may explicitly set " +
      "spark.shuffle.manager, preventing automatic configuration from taking effect. To " +
      "override it, explicitly set spark.shuffle.manager to the RAPIDS Shuffle Manager class. " +
      "This automatic configuration is skipped on Dataproc runtimes that set " +
      "spark.dataproc.engine, including Lightning Engine runtimes; on those runtimes, " +
      "spark.shuffle.manager remains unset unless explicitly configured. " +
      "On earlier Spark versions, the RAPIDS Shuffle Manager must already be configured. When " +
      "set to `false`, the built-in Spark shuffle implementation will be used. ")
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_MANAGER_MODE = conf("spark.rapids.shuffle.mode")
    .doc("RAPIDS Shuffle Manager mode. " +
      "\"MULTITHREADED\": shuffle file writes and reads are parallelized using a thread pool. " +
      "\"UCX\": (requires UCX installation) uses accelerated transports for " +
      "transferring shuffle blocks. " +
      "\"CACHE_ONLY\": use when running a single executor, for short-circuit cached " +
      "shuffle (for testing purposes).")
    .startupOnly()
    .stringConf
    .checkValues(Set("UCX", "CACHE_ONLY", "MULTITHREADED"))
    .createWithDefault("MULTITHREADED")

  val MULTITHREADED_SHUFFLE_SKIP_MERGE = conf("spark.rapids.shuffle.multithreaded.skipMerge")
    .doc("When using MULTITHREADED shuffle mode, skip merging partial shuffle files and " +
      "instead serve data directly from the MultithreadedShuffleBufferCatalog. " +
      "This avoids I/O overhead from merging but requires: (1) External Shuffle Service (ESS) " +
      "to be disabled, and (2) spark.rapids.memory.host.offHeapLimit.enabled=true (off-heap " +
      "memory limits enabled) to prevent OOM from unbounded buffer growth. " +
      "When set to false (default), partial files will be merged into a single " +
      "shuffle file per map task as in standard Spark shuffle. " +
      "Set to true when both requirements are met and shuffle data is not reused across " +
      "SQL queries (e.g., avoid on Databricks with shuffle reuse enabled).")
    .startupOnly()
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_TRANSPORT_EARLY_START = conf("spark.rapids.shuffle.transport.earlyStart")
    .doc("Enable early connection establishment for RAPIDS Shuffle")
    .startupOnly()
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_INTERVAL =
    conf("spark.rapids.shuffle.transport.earlyStart.heartbeatInterval")
      .doc("Shuffle early start heartbeat interval (milliseconds). " +
        "Executors will send a heartbeat RPC message to the driver at this interval")
      .startupOnly()
      .integerConf
      .createWithDefault(5000)

  val SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_TIMEOUT =
    conf("spark.rapids.shuffle.transport.earlyStart.heartbeatTimeout")
      .doc(s"Shuffle early start heartbeat timeout (milliseconds). " +
        s"Executors that don't heartbeat within this timeout will be considered stale. " +
        s"This timeout must be higher than the value for " +
        s"${SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_INTERVAL.key}")
      .startupOnly()
      .integerConf
      .createWithDefault(10000)

  val SHUFFLE_TRANSPORT_CLASS_NAME = conf("spark.rapids.shuffle.transport.class")
    .doc("The class of the specific RapidsShuffleTransport to use during the shuffle.")
    .internal()
    .startupOnly()
    .stringConf
    .createWithDefault("com.nvidia.spark.rapids.shuffle.ucx.UCXShuffleTransport")

  val SHUFFLE_TRANSPORT_MAX_RECEIVE_INFLIGHT_BYTES =
    conf("spark.rapids.shuffle.transport.maxReceiveInflightBytes")
      .doc("Maximum aggregate amount of bytes that be fetched at any given time from peers " +
        "during shuffle")
      .startupOnly()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(1024 * 1024 * 1024)

  val SHUFFLE_UCX_ACTIVE_MESSAGES_FORCE_RNDV =
    conf("spark.rapids.shuffle.ucx.activeMessages.forceRndv")
      .doc("Set to true to force 'rndv' mode for all UCX Active Messages. " +
        "This should only be required with UCX 1.10.x. UCX 1.11.x deployments should " +
        "set to false.")
      .startupOnly()
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_UCX_USE_WAKEUP = conf("spark.rapids.shuffle.ucx.useWakeup")
    .doc("When set to true, use UCX's event-based progress (epoll) in order to wake up " +
      "the progress thread when needed, instead of a hot loop.")
    .startupOnly()
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_UCX_LISTENER_START_PORT = conf("spark.rapids.shuffle.ucx.listenerStartPort")
    .doc("Starting port to try to bind the UCX listener.")
    .internal()
    .startupOnly()
    .integerConf
    .createWithDefault(0)

  val SHUFFLE_UCX_MGMT_SERVER_HOST = conf("spark.rapids.shuffle.ucx.managementServerHost")
    .doc("The host to be used to start the management server")
    .startupOnly()
    .stringConf
    .createWithDefault(null)

  val SHUFFLE_UCX_MGMT_CONNECTION_TIMEOUT =
    conf("spark.rapids.shuffle.ucx.managementConnectionTimeout")
    .doc("The timeout for client connections to a remote peer")
    .internal()
    .startupOnly()
    .integerConf
    .createWithDefault(0)

  val SHUFFLE_UCX_BOUNCE_BUFFERS_SIZE = conf("spark.rapids.shuffle.ucx.bounceBuffers.size")
    .doc("The size of bounce buffer to use in bytes. Note that this size will be the same " +
      "for device and host memory")
    .internal()
    .startupOnly()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(4 * 1024  * 1024)

  val SHUFFLE_UCX_BOUNCE_BUFFERS_DEVICE_COUNT =
    conf("spark.rapids.shuffle.ucx.bounceBuffers.device.count")
    .doc("The number of bounce buffers to pre-allocate from device memory")
    .internal()
    .startupOnly()
    .integerConf
    .createWithDefault(32)

  val SHUFFLE_UCX_BOUNCE_BUFFERS_HOST_COUNT =
    conf("spark.rapids.shuffle.ucx.bounceBuffers.host.count")
    .doc("The number of bounce buffers to pre-allocate from host memory")
    .internal()
    .startupOnly()
    .integerConf
    .createWithDefault(32)

  val SHUFFLE_MAX_CLIENT_THREADS = conf("spark.rapids.shuffle.maxClientThreads")
    .doc("The maximum number of threads that the shuffle client should be allowed to start")
    .internal()
    .startupOnly()
    .integerConf
    .createWithDefault(50)

  val SHUFFLE_MAX_CLIENT_TASKS = conf("spark.rapids.shuffle.maxClientTasks")
    .doc("The maximum number of tasks shuffle clients will queue before adding threads " +
      s"(up to spark.rapids.shuffle.maxClientThreads), or slowing down the transport")
    .internal()
    .startupOnly()
    .integerConf
    .createWithDefault(100)

  val SHUFFLE_CLIENT_THREAD_KEEPALIVE = conf("spark.rapids.shuffle.clientThreadKeepAlive")
    .doc("The number of seconds that the ThreadPoolExecutor will allow an idle client " +
      "shuffle thread to stay alive, before reclaiming.")
    .internal()
    .startupOnly()
    .integerConf
    .createWithDefault(30)

  val SHUFFLE_MAX_SERVER_TASKS = conf("spark.rapids.shuffle.maxServerTasks")
    .doc("The maximum number of tasks the shuffle server will queue up for its thread")
    .internal()
    .startupOnly()
    .integerConf
    .createWithDefault(1000)

  val SHUFFLE_MAX_METADATA_SIZE = conf("spark.rapids.shuffle.maxMetadataSize")
    .doc("The maximum size of a metadata message that the shuffle plugin will keep in its " +
      "direct message pool. ")
    .internal()
    .startupOnly()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(500 * 1024)

  val SHUFFLE_COMPRESSION_CODEC = conf("spark.rapids.shuffle.compression.codec")
    .doc("The GPU codec used to compress shuffle data when using RAPIDS shuffle. " +
      "Supported codecs: zstd, lz4, copy, none")
    .internal()
    .startupOnly()
    .stringConf
    .createWithDefault("none")

val SHUFFLE_COMPRESSION_LZ4_CHUNK_SIZE = conf("spark.rapids.shuffle.compression.lz4.chunkSize")
    .doc("A configurable chunk size to use when compressing with LZ4.")
    .internal()
    .startupOnly()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(64 * 1024)

  val SHUFFLE_COMPRESSION_ZSTD_CHUNK_SIZE =
    conf("spark.rapids.shuffle.compression.zstd.chunkSize")
      .doc("A configurable chunk size to use when compressing with ZSTD.")
      .internal()
      .startupOnly()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(64 * 1024)

  val FORCE_HIVE_HASH_FOR_BUCKETED_WRITE =
    conf("spark.rapids.sql.format.write.forceHiveHashForBucketing")
      .doc("Hive write commands before Spark 330 use Murmur3Hash for bucketed write. " +
        "When enabled, HiveHash will be always used for this instead of Murmur3. This is " +
        "used to align with some customized Spark binaries before 330.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_MULTITHREADED_MAX_BYTES_IN_FLIGHT =
    conf("spark.rapids.shuffle.multiThreaded.maxBytesInFlight")
      .doc(
        "The size limit, in bytes, that the RAPIDS shuffle manager configured in " +
        "\"MULTITHREADED\" mode will allow to be serialized or deserialized concurrently " +
        "per task. This is also the maximum amount of memory that will be used per task. " +
        "This should be set larger " +
        "than Spark's default maxBytesInFlight (48MB). The larger this setting is, the " +
        "more compressed shuffle chunks are processed concurrently. In practice, " +
        "care needs to be taken to not go over the amount of off-heap memory that Netty has " +
        "available. See https://github.com/NVIDIA/cudf-spark/issues/9153.")
      .startupOnly()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(128 * 1024 * 1024)

  val SHUFFLE_MULTITHREADED_WRITER_THREADS =
    conf("spark.rapids.shuffle.multiThreaded.writer.threads")
      .doc("The number of threads to use for writing shuffle blocks per executor in the " +
          "RAPIDS shuffle manager configured in \"MULTITHREADED\" mode. " +
          "There are two special values: " +
          "0 = feature is disabled, falls back to Spark built-in shuffle writer; " +
          "1 = our implementation of Spark's built-in shuffle writer with extra metrics.")
      .startupOnly()
      .integerConf
      .createWithDefault(20)

  val SHUFFLE_PARTITIONING_MAX_CPU_BATCH_SIZE =
    conf("spark.rapids.shuffle.partitioning.maxCpuBatchSize")
      .doc("The maximum size of a sliced batch output to the CPU side " +
        "when GPU partitioning shuffle data. This can be used to limit the peak on-heap memory " +
        "used by CPU to serialize the shuffle data, especially for skew data cases. " +
        "The default value is maximum size of an Array minus 2k overhead (2147483639L - 2048L), " +
        "user should only set a smaller value than default value to avoid subsequent failures.")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v > 0 && v <= 2147483639L - 2048L,
        s"maxCpuBatchSize must be positive and not exceed ${2147483639L - 2048L} bytes.")
      // The maximum size of an Array minus a bit for overhead for metadata
      .createWithDefault(2147483639L - 2048L)

  val SHUFFLE_MULTITHREADED_READER_THREADS =
    conf("spark.rapids.shuffle.multiThreaded.reader.threads")
        .doc("The number of threads to use for reading shuffle blocks per executor in the " +
            "RAPIDS shuffle manager configured in \"MULTITHREADED\" mode. " +
            "There are two special values: " +
            "0 = feature is disabled, falls back to Spark built-in shuffle reader; " +
            "1 = our implementation of Spark's built-in shuffle reader with extra metrics.")
        .startupOnly()
        .integerConf
        .createWithDefault(20)

  val SHUFFLE_KUDO_SERIALIZER_ENABLED = conf("spark.rapids.shuffle.kudo.serializer.enabled")
    .doc("Enable or disable the Kudo serializer for the shuffle.")
    .internal()
    .startupOnly()
    .booleanConf
    .createWithDefault(true)

  val SHUFFLE_KUDO_WRITE_MODE = conf("spark.rapids.shuffle.kudo.serializer.write.mode")
    .doc("Kudo serializer mode. " +
      "\"CPU\": serialize shuffle outputs on the cpu. " +
      "\"GPU\": serialize shuffle outputs on the gpu. ")
    .internal()
    .startupOnly()
    .stringConf
    .createWithDefault("CPU")

  val SHUFFLE_KUDO_READ_MODE = conf("spark.rapids.shuffle.kudo.serializer.read.mode")
    .doc("Kudo serializer read mode. " +
      "\"CPU\": deserialize shuffle inputs on the cpu. " +
      "\"GPU\": deserialize shuffle inputs on the gpu. ")
    .internal()
    .startupOnly()
    .stringConf
    .createWithDefault("GPU")

  val SHUFFLE_KUDO_SERIALIZER_MEASURE_BUFFER_COPY_ENABLED =
    conf("spark.rapids.shuffle.kudo.serializer.measure.buffer.copy.enabled")
    .doc("Enable or disable measuring buffer copy time when using Kudo serializer for the shuffle.")
    .internal()
    .startupOnly()
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_ASYNC_READ_ENABLED = conf("spark.rapids.sql.asyncRead.shuffle.enabled")
    .doc("Enable or disable the asynchronous read for Shuffle. If you turn this on you should " +
      "also consider increasing spark.rapids.sql.asyncRead.maxInFlightHostMemoryBytes so that " +
      "async threads won't be blocked by the memory limit. If By default this in off now.")
    .internal()
    .startupOnly()
    .booleanConf
    .createWithDefault(false)

  // ["NEVER", "ALWAYS", "ONFAILURE"]
  private val KudoDebugModes =
    DumpOption.values.map(_.toString.toUpperCase(java.util.Locale.ROOT)).toSet

  val SHUFFLE_KUDO_SERIALIZER_DEBUG_MODE = conf("spark.rapids.shuffle.kudo.serializer.debug.mode")
    .doc("Debug mode for Kudo serializer for the shuffle. If Always, it will dump the " +
      "kudo tables to a file in spark.rapids.shuffle.kudo.serializer.debug.dump.path.prefix. " +
      "If Never, it will not dump the kudo tables data. If OnFailure, it will only dump the " +
      "kudo tables data when the shuffle fails.")
    .internal()
    .startupOnly()
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(KudoDebugModes)
    .createWithDefault("NEVER")

  val SHUFFLE_KUDO_SERIALIZER_DEBUG_DUMP_PREFIX =
    conf("spark.rapids.shuffle.kudo.serializer.debug.dump.path.prefix")
    .doc("The path prefix to use for the kudo tables when using Kudo serializer for the shuffle.")
    .internal()
    .startupOnly()
    .stringConf
    .createOptional

  // USER FACING DEBUG CONFIGS

  val SHUFFLE_COMPRESSION_MAX_BATCH_MEMORY =
    conf("spark.rapids.shuffle.compression.maxBatchMemory")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(1024 * 1024 * 1024)

  val EXPLAIN = conf("spark.rapids.sql.explain")
    .doc("Explain why some parts of a query were not placed on a GPU or not. Possible " +
      "values are ALL: print everything, NONE: print nothing, NOT_ON_GPU: print only parts of " +
      "a query that did not go on the GPU")
    .commonlyUsed()
    .stringConf
    .createWithDefault("NOT_ON_GPU")

  val SHIMS_PROVIDER_OVERRIDE = conf("spark.rapids.shims-provider-override")
    .internal()
    .startupOnly()
    .doc("Overrides the automatic Spark shim detection logic and forces a specific shims " +
      "provider class to be used. Set to the fully qualified shims provider class to use. " +
      "If you are using a custom Spark version such as Spark 3.2.0 then this can be used to " +
      "specify the shims provider that matches the base Spark version of Spark 3.2.0, i.e.: " +
      "com.nvidia.spark.rapids.shims.spark320.SparkShimServiceProvider. If you modified Spark " +
      "then there is no guarantee the cuDF plugin will function properly." +
      "When tested in a combined jar with other Shims, it's expected that the provided " +
      "implementation follows the same convention as existing Spark shims. If its class" +
      " name has the form com.nvidia.spark.rapids.shims.<shimId>.YourSparkShimServiceProvider. " +
      "The last package name component, i.e., shimId, can be used in the combined jar as the root" +
      " directory /shimId for any incompatible classes. When tested in isolation, no special " +
      "jar root is required"
    )
    .stringConf
    .createOptional

  val CUDF_VERSION_OVERRIDE = conf("spark.rapids.cudfVersionOverride")
    .internal()
    .startupOnly()
    .doc("Overrides the cudf version compatibility check between cudf jar and cuDF plugin " +
      "jar. If you are sure that the cudf jar which is mentioned in the classpath is compatible " +
      "with the cuDF plugin version, then set this to true.")
    .booleanConf
    .createWithDefault(false)

  val ALLOW_MULTIPLE_JARS = conf("spark.rapids.sql.allowMultipleJars")
    .startupOnly()
    .doc("Allow multiple rapids-4-spark, spark-rapids-jni, and cudf jars on the classpath. " +
      "Spark will take the first one it finds, so the version may not be expected. Possisble " +
      "values are ALWAYS: allow all jars, SAME_REVISION: only allow jars with the same " +
      "revision, NEVER: do not allow multiple jars at all.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(Set("ALWAYS", "SAME_REVISION", "NEVER"))
    .createWithDefault("SAME_REVISION")

  val ALLOW_DISABLE_ENTIRE_PLAN = conf("spark.rapids.allowDisableEntirePlan")
    .internal()
    .doc("The plugin has the ability to detect possibe incompatibility with some specific " +
      "queries and cluster configurations. In those cases the plugin will disable GPU support " +
      "for the entire query. Set this to false if you want to override that behavior, but use " +
      "with caution.")
    .booleanConf
    .createWithDefault(true)

  val OPTIMIZER_ENABLED = conf("spark.rapids.sql.optimizer.enabled")
      .internal()
      .doc("Enable cost-based optimizer that will attempt to avoid " +
          "transitions to GPU for operations that will not result in improved performance " +
          "over CPU")
      .booleanConf
      .createWithDefault(false)

  val OPTIMIZER_EXPLAIN = conf("spark.rapids.sql.optimizer.explain")
      .internal()
      .doc("Explain why some parts of a query were not placed on a GPU due to " +
          "optimization rules. Possible values are ALL: print everything, NONE: print nothing")
      .stringConf
      .createWithDefault("NONE")

  val OPTIMIZER_DEFAULT_ROW_COUNT = conf("spark.rapids.sql.optimizer.defaultRowCount")
    .internal()
    .doc("The cost-based optimizer uses estimated row counts to calculate costs and sometimes " +
      "there is no row count available so we need a default assumption to use in this case")
    .longConf
    .createWithDefault(1000000)

  val OPTIMIZER_CLASS_NAME = conf("spark.rapids.sql.optimizer.className")
    .internal()
    .doc("Optimizer implementation class name. The class must implement the " +
      "com.nvidia.spark.rapids.Optimizer trait")
    .stringConf
    .createWithDefault("com.nvidia.spark.rapids.CostBasedOptimizer")

  val OPTIMIZER_DEFAULT_CPU_OPERATOR_COST = conf("spark.rapids.sql.optimizer.cpu.exec.default")
    .internal()
    .doc("Default per-row CPU cost of executing an operator, in seconds")
    .doubleConf
    .createWithDefault(0.0002)

  val OPTIMIZER_DEFAULT_CPU_EXPRESSION_COST = conf("spark.rapids.sql.optimizer.cpu.expr.default")
    .internal()
    .doc("Default per-row CPU cost of evaluating an expression, in seconds")
    .doubleConf
    .createWithDefault(0.0)

  val OPTIMIZER_DEFAULT_GPU_OPERATOR_COST = conf("spark.rapids.sql.optimizer.gpu.exec.default")
      .internal()
      .doc("Default per-row GPU cost of executing an operator, in seconds")
      .doubleConf
      .createWithDefault(0.0001)

  val OPTIMIZER_DEFAULT_GPU_EXPRESSION_COST = conf("spark.rapids.sql.optimizer.gpu.expr.default")
      .internal()
      .doc("Default per-row GPU cost of evaluating an expression, in seconds")
      .doubleConf
      .createWithDefault(0.0)

  val OPTIMIZER_CPU_READ_SPEED = conf(
    "spark.rapids.sql.optimizer.cpuReadSpeed")
      .internal()
      .doc("Speed of reading data from CPU memory in GB/s")
      .doubleConf
      .createWithDefault(30.0)

  val OPTIMIZER_CPU_WRITE_SPEED = conf(
    "spark.rapids.sql.optimizer.cpuWriteSpeed")
    .internal()
    .doc("Speed of writing data to CPU memory in GB/s")
    .doubleConf
    .createWithDefault(30.0)

  val OPTIMIZER_GPU_READ_SPEED = conf(
    "spark.rapids.sql.optimizer.gpuReadSpeed")
    .internal()
    .doc("Speed of reading data from GPU memory in GB/s")
    .doubleConf
    .createWithDefault(320.0)

  val OPTIMIZER_GPU_WRITE_SPEED = conf(
    "spark.rapids.sql.optimizer.gpuWriteSpeed")
    .internal()
    .doc("Speed of writing data to GPU memory in GB/s")
    .doubleConf
    .createWithDefault(320.0)

  val USE_ARROW_OPT = conf("spark.rapids.arrowCopyOptimizationEnabled")
    .doc("Option to turn off using the optimized Arrow copy code when reading from " +
      "ArrowColumnVector in HostColumnarToGpu. Left as internal as user shouldn't " +
      "have to turn it off, but its convenient for testing.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val SPARK_GPU_RESOURCE_NAME = conf("spark.rapids.gpu.resourceName")
    .doc("The name of the Spark resource that represents a GPU that you want the plugin to use " +
      "if using custom resources with Spark.")
    .startupOnly()
    .stringConf
    .createWithDefault("gpu")

  val SUPPRESS_PLANNING_FAILURE = conf("spark.rapids.sql.suppressPlanningFailure")
    .doc("Option to fallback an individual query to CPU if an unexpected condition prevents the " +
      "query plan from being converted to a GPU-enabled one. Note this is different from " +
      "a normal CPU fallback for a yet-to-be-supported Spark SQL feature. If this happens " +
      "the error should be reported and investigated as a GitHub issue.")
    .booleanConf
    .createWithDefault(value = false)

  val ENABLE_FAST_SAMPLE = conf("spark.rapids.sql.fast.sample")
    .doc("Option to turn on fast sample. If enable it is inconsistent with CPU sample " +
      "because of GPU sample algorithm is inconsistent with CPU.")
    .booleanConf
    .createWithDefault(value = false)

  val DETECT_DELTA_LOG_QUERIES = conf("spark.rapids.sql.detectDeltaLogQueries")
    .doc("Queries against Delta Lake _delta_log JSON files are not efficient on the GPU. When " +
      "this option is enabled, the plugin will attempt to detect these queries and fall back " +
      "to the CPU.")
    .booleanConf
    .createWithDefault(value = true)

  val DETECT_DELTA_CHECKPOINT_QUERIES = conf("spark.rapids.sql.detectDeltaCheckpointQueries")
    .doc("Queries against Delta Lake _delta_log checkpoint Parquet files are not efficient on " +
      "the GPU. When this option is enabled, the plugin will attempt to detect these queries " +
      "and fall back to the CPU.")
    .booleanConf
    .createWithDefault(value = true)

  val NUM_FILES_FILTER_PARALLEL = conf("spark.rapids.sql.coalescing.reader.numFilterParallel")
    .doc("This controls the number of files the coalescing reader will run " +
      "in each thread when it filters blocks for reading. If this value is greater than zero " +
      "the files will be filtered in a multithreaded manner where each thread filters " +
      "the number of files set by this config. If this is set to zero the files are " +
      "filtered serially. This uses the same thread pool as the multithreaded reader, " +
      s"see $MULTITHREAD_READ_NUM_THREADS.")
    .integerConf
    .createWithDefault(value = 0)

  val CONCURRENT_WRITER_PARTITION_FLUSH_SIZE =
    conf("spark.rapids.sql.concurrentWriterPartitionFlushSize")
        .doc("The flush size of the concurrent writer cache in bytes for each partition. " +
            "If specified spark.sql.maxConcurrentOutputFileWriters, use concurrent writer to " +
            "write data. Concurrent writer first caches data for each partition and begins to " +
            "flush the data if it finds one partition with a size that is greater than or equal " +
            "to this config. The default value is 0, which will try to select a size based off " +
            "of file type specific configs. E.g.: It uses `write.parquet.row-group-size-bytes` " +
            "config for Parquet type and `orc.stripe.size` config for Orc type. " +
            "If the value is greater than 0, will use this positive value." +
            "Max value may get better performance but not always, because concurrent writer uses " +
            "spillable cache and big value may cause more IO swaps.")
        .bytesConf(ByteUnit.BYTE)
        .createWithDefault(0L)

  val NUM_SUB_PARTITIONS = conf("spark.rapids.sql.join.hash.numSubPartitions")
    .doc("The number of partitions for the repartition in each partition for big hash join. " +
      "GPU will try to repartition the data into smaller partitions in each partition when the " +
      "data from the build side is too large to fit into a single batch.")
    .internal()
    .integerConf
    .createWithDefault(16)

  val SIZED_JOIN_PARTITION_AMPLIFICATION =
    conf("spark.rapids.sql.join.sizedJoin.buildPartitionNumberAmplification")
      .doc("In sized join, by default we'll use bytes_of_build_size/batch_size + 1 as the number " +
        "of partitions for the build side. This config is used to amplify the number of " +
        "partitions for the build side. The default value is 1, which means we'll use the " +
        "default number of partitions. If the value is greater than 1, we'll amplify the " +
        "number of partitions by this value.")
      .internal()
      .doubleConf
      .checkValue(v => v >= 1, "The amplification factor must be greater than or equal to 1")
      .createWithDefault(1)

  val ENABLE_AQE_EXCHANGE_REUSE_FIXUP = conf("spark.rapids.sql.aqeExchangeReuseFixup.enable")
      .doc("Option to turn on the fixup of exchange reuse when running with " +
          "adaptive query execution.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP =
    conf("spark.rapids.sql.nonAqeBroadcastReuseFixup.enable")
      .doc("Option to turn on the fixup of broadcast exchange reuse for DPP " +
          "subqueries when AQE is disabled. The DPP-side GpuBroadcastExchange is built " +
          "during GpuOverrides and bypasses GpuTransitionOverrides, so it does not match " +
          "the join-side broadcast canonically. This fixup builds a per-query signature map " +
          "of join-side GpuBroadcastExchangeExec nodes in the main plan and rewrites a " +
          "matching DPP-side broadcast to ReusedExchangeExec.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val CHUNKED_PACK_POOL_SIZE = conf("spark.rapids.sql.chunkedPack.poolSize")
      .doc("Amount of GPU memory (in bytes) to set aside at startup for the chunked pack " +
           "scratch space, needed during spill from GPU to host memory. As a rule of thumb, each " +
           "column should see around 200B that will be allocated from this pool. " +
           "With the default of 10MB, a table of ~60,000 columns can be spilled using only this " +
           "pool. If this config is 0B, or if allocations fail, the plugin will retry with " +
           "the regular GPU memory resource.")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(10L*1024*1024)

  val CHUNKED_PACK_BOUNCE_BUFFER_SIZE = conf("spark.rapids.sql.chunkedPack.bounceBufferSize")
      .doc("Amount of GPU memory (in bytes) to set aside at startup per chunked pack " +
          "bounce buffer, needed during spill from GPU to host memory. ")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v >= 1L*1024*1024,
        "The chunked pack bounce buffer must be at least 1MB in size")
      .createWithDefault(32L * 1024 * 1024)

  val CHUNKED_PACK_BOUNCE_BUFFER_COUNT = conf("spark.rapids.sql.chunkedPack.bounceBuffers")
    .doc("Number of chunked pack bounce buffers, needed during spill from GPU to host memory. ")
    .internal()
    .integerConf
    .checkValue(v => v >= 1,
      "The chunked pack bounce buffer count must be at least 1")
    .createWithDefault(4)

  val SPILL_TO_DISK_BOUNCE_BUFFER_SIZE =
    conf("spark.rapids.memory.host.spillToDiskBounceBufferSize")
      .doc("Amount of host memory (in bytes) to set aside at startup for the " +
        "bounce buffer used for gpu to disk spill that bypasses the host store.")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v >= 1,
        "The gpu to disk spill bounce buffer must have a positive size")
      .createWithDefault(32L * 1024 * 1024)

  val SPILL_TO_DISK_BOUNCE_BUFFER_COUNT =
    conf("spark.rapids.memory.host.spillToDiskBounceBuffers")
      .doc("Number of bounce buffers used for gpu to disk spill that bypasses the host store.")
      .internal()
      .integerConf
      .checkValue(v => v >= 1,
        "The gpu to disk spill bounce buffer count must be positive")
      .createWithDefault(4)

  val SPLIT_UNTIL_SIZE_OVERRIDE = conf("spark.rapids.sql.test.overrides.splitUntilSize")
      .doc("Only for tests: override the value of GpuDeviceManager.splitUntilSize")
      .internal()
      .longConf
      .createOptional

  val PROJECT_SPLIT_RETRY_ENABLED = conf("spark.rapids.sql.projectExec.splitRetry.enabled")
      .doc("When true, GpuProjectExec uses split-and-retry on GPU OOM for retryable " +
          "projections: the input batch is halved by rows and the projection is re-run on " +
          "each half. Projections that include non-retryable expressions fall back to the " +
          "existing withRetryNoSplit path because those expressions cannot be safely " +
          "re-evaluated on row-split inputs. Disable this to revert to the prior behavior.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val TEST_IO_ENCRYPTION = conf("spark.rapids.test.io.encryption")
    .doc("Only for tests: verify for IO encryption")
    .internal()
    .booleanConf
    .createOptional

  val SKIP_GPU_ARCH_CHECK = conf("spark.rapids.skipGpuArchitectureCheck")
    .doc("When true, skips GPU architecture compatibility check. Note that this check " +
      "might still be present in cuDF.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val TEST_GET_JSON_OBJECT_SAVE_PATH = conf("spark.rapids.sql.expression.GetJsonObject.debugPath")
    .doc("Only for tests: specify a directory to save CSV debug output for get_json_object " +
      "if the output differs from the CPU version. Multiple files may be saved")
    .internal()
    .stringConf
    .createOptional

  val TEST_GET_JSON_OBJECT_SAVE_ROWS =
    conf("spark.rapids.sql.expression.GetJsonObject.debugSaveRows")
      .doc("Only for tests: when a debugPath is provided this is the number " +
        "of rows that is saved per file. There may be multiple files if there " +
        "are multiple tasks or multiple batches within a task")
      .internal()
      .integerConf
      .createWithDefault(1024)

  val DELTA_LOW_SHUFFLE_MERGE_SCATTER_DEL_VECTOR_BATCH_SIZE =
    conf("spark.rapids.sql.delta.lowShuffleMerge.deletion.scatter.max.size")
      .doc("Option to set max batch size when scattering deletion vector")
      .internal()
      .integerConf
      .createWithDefault(32 * 1024)

  val DELTA_LOW_SHUFFLE_MERGE_DEL_VECTOR_BROADCAST_THRESHOLD =
    conf("spark.rapids.sql.delta.lowShuffleMerge.deletionVector.broadcast.threshold")
      .doc("Currently we need to broadcast deletion vector to all executors to perform low " +
        "shuffle merge. When we detect the deletion vector broadcast size is larger than this " +
        "value, we will fallback to normal shuffle merge.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(20 * 1024 * 1024)

  val ENABLE_DELTA_LOW_SHUFFLE_MERGE =
    conf("spark.rapids.sql.delta.lowShuffleMerge.enabled")
    .doc("Option to turn on the low shuffle merge for Delta Lake. Currently there are some " +
      "limitations for this feature: " +
      "1. We only support Delta Lake 2.4. " +
      s"2. The file scan mode must be set to ${RapidsReaderType.PERFILE} " +
      "3. The deletion vector size must be smaller than " +
      s"${DELTA_LOW_SHUFFLE_MERGE_DEL_VECTOR_BROADCAST_THRESHOLD.key} ")
    .booleanConf
    .createWithDefault(false)

    val DELTA_DELETION_VECTOR_PREDICATE_PUSHDOWN =
    conf("spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled")
      .doc("When true, the deletion vector processing will be pushed down to " +
        "the GPU Delta Lake scans. The result of the scan will contain only the rows " +
        "that are not deleted according to the deletion vector. When false, " +
        "the deletion vectors will be materialized as a boolean column and " +
        "the GPU filter operator will process it together with other filters. " +
        "This setting is effective only when " +
        "spark.databricks.delta.deletionVectors.useMetadataRowIndex is true. " +
        "Otherwise, this setting is fixed to false regardless of its actual value.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val ENABLE_HASH_FUNCTION_IN_PARTITIONING =
    conf("spark.rapids.sql.partitioning.hashFunction.enabled")
      .doc("When false, Only Murmur3Hash is used for GPU hash partitioning to " +
        "align with the regular Spark. When enabled, GPU will try to infer the hash " +
        "function from the CPU hash partitioning and use the same one. This is for " +
        "a customized Spark supporting multiple hash functions in hash partitioning. " +
        "So far only 'HiveHash' and 'Murmur3Hash' are supported on GPU. This requires " +
        s"'${INCOMPATIBLE_OPS.key}' to also be set to true.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val TAG_LORE_ID_ENABLED = conf("spark.rapids.sql.lore.tag.enabled")
    .doc("Enable add a LORE id to each gpu plan node")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val LORE_DUMP_IDS = conf("spark.rapids.sql.lore.idsToDump")
    .doc("Specify the LORE ids of operators to dump. The format is a comma separated list of " +
      "LORE ids. For example: \"1[0]\" will dump partition 0 of input of gpu operator " +
      "with lore id 1. For more details, please refer to " +
      "[the LORE documentation](../dev/lore.md). If this is not set, no data will be dumped.")
    .stringConf
    .createOptional

  val LORE_DUMP_PATH = conf("spark.rapids.sql.lore.dumpPath")
    .doc(s"The path to dump the LORE nodes' input data. This must be set if ${LORE_DUMP_IDS.key} " +
      "has been set. The data of each LORE node will be dumped to a subfolder with name " +
      "'loreId-<LORE id>' under this path. For more details, please refer to " +
      "[the LORE documentation](../dev/lore.md).")
    .stringConf
    .createOptional

  val LORE_SKIP_DUMPING_PLAN = conf("spark.rapids.sql.lore.skip.plan.dump")
    .doc("Skip dumping plan metadata when doing lore dump")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val LORE_NON_STRICT_MODE = conf("spark.rapids.sql.lore.nonStrictMode.enabled")
    .doc("Allow LoRE dumping to continue when a selected lore id fails. When enabled, failing " +
      "lore ids are skipped with a warning, previously dumped data under the dump path is kept, " +
      "and the rest of the query continues executing.")
    .booleanConf
    .createWithDefault(false)

  val OP_TIME_TRACKING_RDD_ENABLED = conf("spark.rapids.sql.exec.opTimeTrackingRDD.enabled")
    .doc("Enable OpTimeTrackingRDD for all GPU operations. When true, OpTimeTrackingRDD " +
      "wrappers will be created to track operation time. When false, can improve " +
      "performance by avoiding overhead of operation time tracking.")
    .booleanConf
    .createWithDefault(true)

  val LORE_PARQUET_USE_ORIGINAL_NAMES =
    conf("spark.rapids.sql.lore.parquet.useOriginalSchemaNames")
      .doc("When enabled, LORE writes Parquet files using the original Spark schema names " +
        "instead of auto-generated type-based names. This makes the dumped Parquet data " +
        "easier to consume directly via Spark/other tools.")
      .booleanConf
      .createWithDefault(true)

  val CASE_WHEN_FUSE =
    conf("spark.rapids.sql.case_when.fuse")
      .doc("If when branches is greater than 2 and all then/else values in case when are string " +
        "scalar, fuse mode improves the performance. By default this is enabled.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val TRACE_TASK_GPU_OWNERSHIP = conf("spark.rapids.sql.nvtx.traceTaskGpuOwnership")
    .doc("Enable tracing of the GPU ownership of tasks. This can be useful for debugging " +
      "deadlocks and other issues related to GPU semaphore.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val ENABLE_ASYNC_OUTPUT_WRITE =
    conf("spark.rapids.sql.asyncWrite.queryOutput.enabled")
      .doc("Option to turn on the async query output write. During the final output write, the " +
        "task first copies the output to the host memory, and then writes it into the storage. " +
        "When this option is enabled, the task will asynchronously write the output in the host " +
        "memory to the storage. Only the Parquet and ORC formats are supported currently.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val ASYNC_QUERY_OUTPUT_WRITE_HOLD_GPU_IN_TASK =
    conf("spark.rapids.sql.queryOutput.holdGpuInTask")
      .doc("Option to hold GPU semaphore between batch processing during the final output write. " +
        "This option could degrade query performance if it is enabled without the async query " +
        "output write. It is recommended to consider enabling this option only when " +
        s"${ENABLE_ASYNC_OUTPUT_WRITE.key} is set. This option is off by default when the async " +
        "query output write is disabled; otherwise, it is on.")
      .internal()
      .booleanConf
      .createOptional

  val ASYNC_WRITE_MAX_IN_FLIGHT_HOST_MEMORY_BYTES =
    conf("spark.rapids.sql.asyncWrite.maxInFlightHostMemoryBytes")
      .doc("Maximum number of host memory bytes per executor that can be in-flight for async " +
        "write. Tasks may be blocked if the total host memory bytes in-flight " +
        "exceeds this value. Today this config only covers file output write, but in future" +
        "it may cover other writes like shuffle write as well. If set to <= 0 it means unlimited " +
        "memory")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(2L * 1024 * 1024 * 1024)

  val ASYNC_READ_MAX_IN_FLIGHT_HOST_MEMORY_BYTES =
    conf("spark.rapids.sql.asyncRead.maxInFlightHostMemoryBytes")
      .doc("Maximum number of host memory bytes per executor that can be in-flight for async " +
        "read. Tasks may be blocked if the total host memory bytes in-flight " +
        "exceeds this value. Today this config only covers shuffle read, but in future" +
        "it may cover other reads like file read as well. If set to <= 0 it means unlimited " +
        "memory")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      // Why by default set to unlimited? The reasons are:
      // 1. For async shuffle read (done) or async file read (already there but need to integrate
      // into this unified throttling), the host memory usage is bounded: N * batchSize * 2, where N
      // is the number of total task number in executor, and "*2" is for prefetch + concatenation.
      // 2. Even without asyncRead, today each task is already allowed to use host memory to prepare
      // its data in CPU before it acquires GPU semaphore. Take shuffle read for example
      // the host memory usage is already high, actually async shuffle read is adding just a
      // little more memory pressure (concurrentGpuTasks * batchSize * 2), note concurrentGpuTasks
      // is typically much smaller than N.
      // 3. The read in data will be spillable, so it won't have deadly consequences.
      // 4. We have not yet implemented unified thread priority, so there's deadlock risks if this
      // value is improperly set.
      .createWithDefault(-1)
}
