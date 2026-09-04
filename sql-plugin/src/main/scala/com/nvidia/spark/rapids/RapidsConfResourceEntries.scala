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

import scala.util.Try

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.rapids.execution.{JoinBuildSideSelection, JoinStrategy}

private[rapids] trait RapidsConfResourceEntries {
  def conf(key: String): ConfBuilder

  val MULTITHREAD_READ_NUM_THREADS_DEFAULT = 20

  // Resource Configuration

  val PINNED_POOL_SIZE = conf("spark.rapids.memory.pinnedPool.size")
    .doc("The size of the pinned memory pool in bytes unless otherwise specified. " +
      "Use 0 to disable the pool.")
    .startupOnly()
    .commonlyUsed()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(0)

  val PINNED_POOL_SET_CUIO_DEFAULT = conf("spark.rapids.memory.pinnedPool.setCuioDefault")
    .doc("If set to true, the pinned pool configured for the plugin will be shared with " +
      "cuIO for small pinned allocations.")
    .startupOnly()
    .internal()
    .booleanConf
    .createWithDefault(true)

  val PINNED_POOL_PARALLEL_INIT_THREADS =
    conf("spark.rapids.memory.pinnedPool.parallelInit.threads")
      .doc("Number of CPU threads used to initialize the pinned pool's backing memory, capped at " +
        "the number of executor cores. Set to 'all' to use the number of executor cores. A value " +
        "of 1 initializes the backing memory using cudaHostAlloc. Values greater than 1 instead " +
        "pre-touch pages concurrently before pinning for faster initialization. This does not " +
        "affect subsequent suballocator behavior. Note: on multi-NUMA systems, multithreaded " +
        "initialization can scatter pages across nodes if you do not constrain placement in " +
        "advance. Pages cannot be migrated once pinned.")
      .startupOnly()
      .stringConf
      .transform(_.trim.toLowerCase(java.util.Locale.ROOT))
      .checkValue(value => value == "all" || Try(value.toInt).map(_ > 0).getOrElse(false),
        "Pinned-pool initialization threads must be a positive integer or 'all'.")
      .createWithDefault("all")

  val OFF_HEAP_LIMIT_ENABLED = conf("spark.rapids.memory.host.offHeapLimit.enabled")
      .doc("Should the off heap limit be enforced or not.")
      .startupOnly()
      // This might change as a part of https://github.com/NVIDIA/spark-rapids/issues/8878
      .internal()
      .booleanConf
      .createWithDefault(false)

  val OFF_HEAP_LIMIT_SIZE = conf("spark.rapids.memory.host.offHeapLimit.size")
      .doc("The maximum amount of off heap memory that the plugin will use. " +
          "This includes pinned memory and some overhead memory. If pinned is larger " +
          "than this - overhead pinned will be truncated.")
      .startupOnly()
      .internal() // https://github.com/NVIDIA/spark-rapids/issues/8878 should be replaced with
      // .commonlyUsed()
      .bytesConf(ByteUnit.BYTE)
      .createOptional // The default

  val CGROUPS_MEMORY_LIMIT_PATH = conf("spark.rapids.cgroups.memory.limit.path")
    .doc("The filepath of the local file on host that stores the memory limit " +
      "for the process. If omitted, attempts to detect the file from common locations.")
    .startupOnly()
    .stringConf
    .createOptional

  val CGROUPS_MEMORY_USAGE_PATH = conf("spark.rapids.cgroups.memory.usage.path")
    .doc("The filepath of the local file on host that stores the memory usage " +
      "for the process. If omitted, attempts to detect the file from common locations.")
    .startupOnly()
    .stringConf
    .createOptional

  val TASK_OVERHEAD_SIZE = conf("spark.rapids.memory.host.taskOverhead.size")
      .doc("The amount of off heap memory reserved per task for overhead activities " +
          "like C++ heap/stack and a few other small things that are hard to control for.")
      .startupOnly()
      .internal() // https://github.com/NVIDIA/spark-rapids/issues/8878
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(15L * 1024 * 1024) // 15 MiB

  val RMM_DEBUG = conf("spark.rapids.memory.gpu.debug")
    .doc("Provides a log of GPU memory allocations and frees. If set to " +
      "STDOUT or STDERR the logging will go there. Setting it to NONE disables logging. " +
      "All other values are reserved for possible future expansion and in the mean time will " +
      "disable logging.")
    .startupOnly()
    .stringConf
    .createWithDefault("NONE")

  val SPARK_RMM_STATE_DEBUG = conf("spark.rapids.memory.gpu.state.debug")
      .doc("To better recover from out of memory errors, RMM will track several states for " +
          "the threads that interact with the GPU. This provides a log of those state " +
          "transitions to aid in debugging it. STDOUT or STDERR will have the logging go there " +
          "empty string will disable logging and anything else will be treated as a file to " +
          "write the logs to.")
      .startupOnly()
      .stringConf
      .createWithDefault("")

  val SPARK_RMM_STATE_ENABLE = conf("spark.rapids.memory.gpu.state.enable")
      .doc("Enabled or disable using the SparkRMM state tracking to improve " +
          "OOM response. This includes possibly retrying parts of the processing in " +
          "the case of an OOM")
      .startupOnly()
      .internal()
      .booleanConf
      .createWithDefault(true)

  val GPU_OOM_DUMP_DIR = conf("spark.rapids.memory.gpu.oomDumpDir")
    .doc("The path to a local directory where a heap dump will be created if the GPU " +
      "encounters an unrecoverable out-of-memory (OOM) error. The filename will be of the " +
      "form: \"gpu-oom-<pid>-<dumpId>.hprof\" where <pid> is the process ID, and " +
      "the dumpId is a sequence number to disambiguate multiple heap dumps " +
      "per process lifecycle")
    .startupOnly()
    .stringConf
    .createOptional

  val GPU_OOM_MAX_RETRIES =
    conf("spark.rapids.memory.gpu.oomMaxRetries")
      .doc("The number of times that an OOM will be re-attempted after the device store " +
        "can't spill anymore. In practice, we can use Cuda.deviceSynchronize to allow temporary " +
        "state in the allocator and in the various streams to catch up, in hopes we can satisfy " +
        "an allocation which was failing due to the interim state of memory.")
      .internal()
      .integerConf
      .createWithDefault(2)

  val ENABLE_R2C_RETRY = conf("spark.rapids.sql.rowToColumnar.retry.enabled")
    .doc("When true (default), the row-to-columnar conversion uses a per-batch retry block " +
      "so that host OOM during conversion can be recovered with negligible overhead. " +
      "Set to false to disable retry and let host OOM fail the task immediately.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val GPU_COREDUMP_DIR = conf("spark.rapids.gpu.coreDump.dir")
    .doc("The URI to a directory where a GPU core dump will be created if the GPU encounters " +
      "an exception. The URI can reference a distributed filesystem. The filename will be of the " +
      "form gpucore-<appID>-<executorID>.nvcudmp, where <appID> is the Spark application ID and " +
      "<executorID> is the executor ID.")
    .internal()
    .stringConf
    .createOptional

val GPU_COREDUMP_PIPE_PATTERN = conf("spark.rapids.gpu.coreDump.pipePattern")
    .doc("The pattern to use to generate the named pipe path. Occurrences of %p in the pattern " +
      "will be replaced with the process ID of the executor.")
    .internal
    .stringConf
    .createWithDefault("gpucorepipe.%p")

  val GPU_COREDUMP_FULL = conf("spark.rapids.gpu.coreDump.full")
    .doc("If true, GPU coredumps will be a full coredump (i.e.: with local, shared, and global " +
      "memory).")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val ENABLE_CPU_BRIDGE = conf("spark.rapids.sql.expression.cpuBridge.enabled")
    .doc("Enable CPU-GPU bridge expressions that allow CPU expression subtrees " +
      "to run while keeping the overall plan on GPU. When enabled, expressions that have no " +
      "GPU implementation will automatically be wrapped in bridge expressions instead of " +
      "causing plan fallbacks.")
    .booleanConf
    .createWithDefault(true)

  val BRIDGE_DISALLOW_LIST = conf("spark.rapids.sql.expression.cpuBridge.disallowList")
    .doc("Comma separated list of expression class names that should not use CPU bridge " +
      "expressions even when bridge is enabled.")
    .internal()
    .stringConf
    .createWithDefault("")

  val CPU_BRIDGE_THREAD_POOL_SIZE = conf("spark.rapids.sql.cpuBridge.threadPoolSize")
    .doc("Override the default CPU bridge thread pool size. When set to a positive value, " +
      "uses this specific number of threads instead of the default calculation based on " +
      "task slots. This is an internal config primarily for testing and debugging.")
    .internal()
    .integerConf
    .checkValue(v => v > 0, "Thread pool size must be positive")
    .createOptional

  val GPU_COREDUMP_COMPRESSION_CODEC = conf("spark.rapids.gpu.coreDump.compression.codec")
    .doc("The codec used to compress GPU core dumps. Spark provides the codecs " +
      "lz4, lzf, snappy, and zstd.")
    .internal()
    .stringConf
    .createWithDefault("zstd")

  val GPU_COREDUMP_COMPRESS = conf("spark.rapids.gpu.coreDump.compress")
    .doc("If true, GPU coredumps will be compressed using the compression codec specified " +
      s"in $GPU_COREDUMP_COMPRESSION_CODEC")
    .internal()
    .booleanConf
    .createWithDefault(true)

  private val RMM_ALLOC_MAX_FRACTION_KEY = "spark.rapids.memory.gpu.maxAllocFraction"
  private val RMM_ALLOC_MIN_FRACTION_KEY = "spark.rapids.memory.gpu.minAllocFraction"
  private val RMM_ALLOC_RESERVE_KEY = "spark.rapids.memory.gpu.reserve"
  private val INTEGRATED_GPU_MEMORY_FRACTION_KEY = "spark.rapids.memory.integratedGpuMemoryFraction"

  val RMM_ALLOC_FRACTION = conf("spark.rapids.memory.gpu.allocFraction")
    .doc("The fraction of available (free) GPU memory that should be allocated for pooled " +
      "memory. This must be less than or equal to the maximum limit configured via " +
      s"$RMM_ALLOC_MAX_FRACTION_KEY, and greater than or equal to the minimum limit configured " +
      s"via $RMM_ALLOC_MIN_FRACTION_KEY.")
    .startupOnly()
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(1)

  val RMM_EXACT_ALLOC = conf("spark.rapids.memory.gpu.allocSize")
      .doc("The exact size in byte that RMM should allocate. This is intended to only be " +
          "used for testing.")
      .internal() // If this becomes public we need to add in checks for the value when it is used.
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val RMM_ALLOC_MAX_FRACTION = conf(RMM_ALLOC_MAX_FRACTION_KEY)
    .doc("The fraction of total GPU memory that limits the maximum size of the RMM pool. " +
        s"The value must be greater than or equal to the setting for $RMM_ALLOC_FRACTION. " +
        "Note that this limit will be reduced by the reserve memory configured in " +
        s"$RMM_ALLOC_RESERVE_KEY.")
    .startupOnly()
    .commonlyUsed()
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(1)

  val RMM_ALLOC_MIN_FRACTION = conf(RMM_ALLOC_MIN_FRACTION_KEY)
    .doc("The fraction of total GPU memory that limits the minimum size of the RMM pool. " +
      s"The value must be less than or equal to the setting for $RMM_ALLOC_FRACTION.")
    .startupOnly()
    .commonlyUsed()
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(0.25)

  val RMM_ALLOC_RESERVE = conf(RMM_ALLOC_RESERVE_KEY)
      .doc("The amount of GPU memory that should remain unallocated by RMM and left for " +
          "system use such as memory needed for kernels and kernel launches.")
      .startupOnly()
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(ByteUnit.MiB.toBytes(640))

  val INTEGRATED_GPU_MEMORY_FRACTION = conf(INTEGRATED_GPU_MEMORY_FRACTION_KEY)
    .doc("The fraction of total physical memory that should be allocated to the GPU on " +
        "integrated GPU systems where memory is shared between CPU and GPU. The remaining " +
        "fraction (1 - this value) will be allocated to CPU memory. Only applies when " +
        "DeviceAttr.isIntegratedGPU == 1.")
    .internal()
    .startupOnly()
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value must be in [0, 1].")
    .createWithDefault(0.6)

  val HOST_SPILL_STORAGE_SIZE = conf("spark.rapids.memory.host.spillStorageSize")
    .doc("Amount of off-heap host memory to use for buffering spilled GPU data before spilling " +
        "to local disk. Use -1 to set the amount to the combined size of pinned and pageable " +
        "memory pools. This config is deprecated in favor of " +
        "spark.rapids.memory.host.offHeapLimit.enabled/" +
        "spark.rapids.memory.host.offHeapLimit.size, which will take precedence if set.")
    .startupOnly()
    .commonlyUsed()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(-1)

  val PARTIAL_FILE_BUFFER_INITIAL_SIZE =
    conf("spark.rapids.memory.host.partialFileBufferInitialSize")
    .doc("The initial size in bytes for a host memory buffer used by " +
        "SpillablePartialFileHandle during shuffle write. This buffer allows shuffle " +
        "data to be kept in memory instead of writing to disk immediately, reducing " +
        "I/O overhead. The buffer can expand dynamically up to partialFileBufferMaxSize. " +
        "A smaller initial size reduces upfront memory allocation but may require more " +
        "expansions. When used with " +
        "RapidsLocalDiskShuffleMapOutputWriter, the buffer expansion uses predictive " +
        "sizing based on partition write statistics to minimize expansion operations.")
    .startupOnly()
    .internal()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(32L * 1024 * 1024)  // 32MB default, expanded predictively

  val PARTIAL_FILE_BUFFER_MAX_SIZE =
    conf("spark.rapids.memory.host.partialFileBufferMaxSize")
    .doc("The maximum size in bytes for a single host memory buffer used by " +
        "SpillablePartialFileHandle during shuffle write. When a buffer needs to " +
        "expand beyond this limit, it will be spilled to disk instead. This prevents " +
        "excessive memory usage for large shuffle partitions. Note: Due to ByteBuffer " +
        "constraints, the effective maximum is Int.MaxValue (~2GB).")
    .startupOnly()
    .internal()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(Int.MaxValue.toLong)  // ~2GB, limited by ByteBuffer

  val PARTIAL_FILE_BUFFER_MEMORY_THRESHOLD =
    conf("spark.rapids.memory.host.partialFileBufferMemoryThreshold")
    .doc("The host memory usage threshold (as a fraction from 0.0 to 1.0) for deciding " +
        "whether to use memory-based buffering for partial files during shuffle write. " +
        "When host memory usage exceeds this threshold, file-based storage will be used " +
        "directly. This threshold also applies when expanding buffers dynamically. " +
        "Setting this too high may cause threads holding the GPU semaphore to block on " +
        "spilling, which wastes valuable GPU resources. Setting it too low reduces the " +
        "shuffle write optimization benefit. A value around 0.5-0.6 typically provides " +
        "optimal performance. As a guideline, ensure that (1 - threshold) * total_host_mem " +
        "is greater than num_threads * gpu_batch_size to leave enough memory for other " +
        "threads to operate without forcing spills.")
    .startupOnly()
    .internal()
    .doubleConf
    .checkValue(v => v > 0.0 && v <= 1.0,
      "The memory threshold must be in the range (0.0, 1.0]")
    .createWithDefault(0.5)

  val UNSPILL = conf("spark.rapids.memory.gpu.unspill.enabled")
    .doc("When a spilled GPU buffer is needed again, should it be unspilled, or only copied " +
        "back into GPU memory temporarily. Unspilling may be useful for GPU buffers that are " +
        "needed frequently, for example, broadcast variables; however, it may also increase GPU " +
        "memory usage")
    .startupOnly()
    .booleanConf
    .createWithDefault(false)

  val RMM_POOL = conf("spark.rapids.memory.gpu.pool")
    .doc("Select the RMM pooling allocator to use. Valid values are \"DEFAULT\", \"ARENA\", " +
      "\"ASYNC\", and \"NONE\". With \"DEFAULT\", the RMM pool allocator is used; with " +
      "\"ARENA\", the RMM arena allocator is used; with \"ASYNC\", the new CUDA stream-ordered " +
      "memory allocator in CUDA 11.2+ is used. If set to \"NONE\", pooling is disabled and RMM " +
      "just passes through to CUDA memory allocation directly.")
    .startupOnly()
    .stringConf
    .createWithDefault("ASYNC")

  val CONCURRENT_GPU_TASKS = conf("spark.rapids.sql.concurrentGpuTasks")
      .doc("Set the initial number of tasks that can execute concurrently per GPU. " +
        "By default the number of tasks allowed on the GPU will adjust dynamically " +
        "to try and provide optimal performance. This sets the starting point for each " +
        "stage. If this is not set the amount of GPU memory will be used to come up " +
        "with a starting estimate.")
      .integerConf
      .createOptional

  val DYNAMIC_CONCURRENT_GPU_TASKS = conf("spark.rapids.sql.concurrentGpuTasks.dynamic")
      .doc("Set to false if the system should not dynamically adjust the concurrent task " +
        "amount, but keep it to be a static number")
      .booleanConf
      .createWithDefault(true)

  val MAX_CONCURRENT_GPU_TASKS = conf("spark.rapids.sql.maxConcurrentGpuTasks")
      .doc("The maximum number of tasks that can execute concurrently per GPU. " +
        "This sets an upper bound on concurrent task execution regardless of " +
        "available GPU memory permits. Set to 0 for no limit.")
      .internal()
      .integerConf
      .createWithDefault(0)

  val GPU_BATCH_SIZE_BYTES = conf("spark.rapids.sql.batchSizeBytes")
    .doc("Set the target number of bytes for a GPU batch. Splits sizes for input data " +
      "is covered by separate configs.")
    .commonlyUsed()
    .bytesConf(ByteUnit.BYTE)
    .checkValue(v => v > 0, "Batch size must be positive")
    .createWithDefault(1 * 1024 * 1024 * 1024) // 1 GiB is the default

  val CHUNKED_READER = conf("spark.rapids.sql.reader.chunked")
    .doc("Enable a chunked reader where possible. A chunked reader allows " +
      "reading highly compressed data that could not be read otherwise, but at the expense " +
      "of more GPU memory, and in some cases more GPU computation. "+
      "Currently this only supports ORC and Parquet formats.")
    .booleanConf
    .createWithDefault(true)

  val CHUNKED_READER_MEMORY_USAGE_RATIO = conf("spark.rapids.sql.reader.chunked.memoryUsageRatio")
    .doc("A value to compute soft limit on the internal memory usage of the chunked reader " +
      "(if being used). Such limit is calculated as the multiplication of this value and " +
      s"'${GPU_BATCH_SIZE_BYTES.key}'.")
    .internal()
    .startupOnly()
    .doubleConf
    .checkValue(v => v > 0, "The ratio value must be positive.")
    .createWithDefault(4)

  val LIMIT_CHUNKED_READER_MEMORY_USAGE = conf("spark.rapids.sql.reader.chunked.limitMemoryUsage")
    .doc("Enable a soft limit on the internal memory usage of the chunked reader " +
      "(if being used). Such limit is calculated as the multiplication of " +
      s"'${GPU_BATCH_SIZE_BYTES.key}' and '${CHUNKED_READER_MEMORY_USAGE_RATIO.key}'." +
      "For example, if batchSizeBytes is set to 1GB and memoryUsageRatio is 4, " +
      "the chunked reader will try to keep its memory usage under 4GB.")
    .booleanConf
    .createOptional

  val CHUNKED_SUBPAGE_READER = conf("spark.rapids.sql.reader.chunked.subPage")
    .doc("Enable a chunked reader where possible for reading data that is smaller " +
      "than the typical row group/page limit. Currently deprecated and replaced by " +
      s"'${LIMIT_CHUNKED_READER_MEMORY_USAGE}'.")
    .booleanConf
    .createOptional

  val MAX_GPU_COLUMN_SIZE_BYTES = conf("spark.rapids.sql.columnSizeBytes")
    .doc("Limit the max number of bytes for a GPU column. It is same as the cudf " +
      "row count limit of a column. It is used by the multi-file readers. " +
      "See com.nvidia.spark.rapids.BatchWithPartitionDataUtils.")
    .internal()
    .bytesConf(ByteUnit.BYTE)
    .checkValue(v => v >= 0 && v <= Integer.MAX_VALUE,
      s"Column size must be positive and not exceed ${Integer.MAX_VALUE} bytes.")
    .createWithDefault(Integer.MAX_VALUE) // 2 GiB is the default

  val MAX_READER_BATCH_SIZE_ROWS = conf("spark.rapids.sql.reader.batchSizeRows")
    .doc("Soft limit on the maximum number of rows the reader will read per batch. " +
      "The orc and parquet readers will read row groups until this limit is met or exceeded. " +
      "The limit is respected by the csv reader.")
    .commonlyUsed()
    .integerConf
    .createWithDefault(Integer.MAX_VALUE)

  val MAX_READER_BATCH_SIZE_BYTES = conf("spark.rapids.sql.reader.batchSizeBytes")
    .doc("Soft limit on the maximum number of bytes the reader reads per batch. " +
      "The readers will read chunks of data until this limit is met or exceeded. " +
      "Note that the reader may estimate the number of bytes that will be used on the GPU " +
      "in some cases based on the schema and number of rows in each batch.")
    .commonlyUsed()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(Integer.MAX_VALUE)

  val DRIVER_TIMEZONE = conf("spark.rapids.driver.user.timezone")
    .doc("This config is used to inform the executor plugin about the driver's timezone " +
      "and is not intended to be set by the user.")
    .internal()
    .stringConf
    .createOptional

  // Internal Features

  val UVM_ENABLED = conf("spark.rapids.memory.uvm.enabled")
    .doc("UVM or universal memory can allow main host memory to act essentially as swap " +
      "for device(GPU) memory. This allows the GPU to process more data than fits in memory, but " +
      "can result in slower processing. This is an experimental feature.")
    .internal()
    .startupOnly()
    .booleanConf
    .createWithDefault(false)

  val EXPORT_COLUMNAR_RDD = conf("spark.rapids.sql.exportColumnarRdd")
    .doc("Spark has no simply way to export columnar RDD data.  This turns on special " +
      "processing/tagging that allows the RDD to be picked back apart into a Columnar RDD.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val JOIN_STRATEGY = conf("spark.rapids.sql.join.strategy")
    .doc("Specifies the join strategy to use for GPU joins. Options are: " +
      "AUTO (default) - automatically determine the best join strategy using heuristics; " +
      "INNER_HASH_WITH_POST - use inner hash join with post-processing to convert to other " +
      "join types and apply join filtering; " +
      "INNER_SORT_WITH_POST - use inner sort-merge join with post-processing, falls back to " +
      "INNER_HASH_WITH_POST for ARRAY/STRUCT key types; " +
      "HASH_ONLY - use traditional hash join only.")
    .internal()
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(JoinStrategy.values.map(_.toString))
    .createWithDefault(JoinStrategy.AUTO.toString)

  val JOIN_BUILD_SIDE = conf("spark.rapids.sql.join.buildSide")
    .doc("Specifies the physical build side selection strategy for GPU join algorithms. " +
      "This controls which side the join algorithm uses as its internal build table, " +
      "which is distinct from the data movement build side (which side is materialized/" +
      "buffered/broadcast, determined by the query plan). Options are: " +
      "AUTO (default) - automatically determine the best physical build side using heuristics, " +
      "currently behaves the same as SMALLEST but may evolve to use additional factors; " +
      "FIXED - use the build side as suggested by the query plan without dynamic selection; " +
      "SMALLEST - always select the side with the smallest row count as the physical build side, " +
      "determined on a batch-by-batch basis at join time. When AUTO or SMALLEST is used, " +
      "the physical build side may differ from the data movement build side.")
    .internal()
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(JoinBuildSideSelection.values.map(_.toString))
    .createWithDefault(JoinBuildSideSelection.AUTO.toString)

  val LOG_JOIN_CARDINALITY = conf("spark.rapids.sql.join.logCardinality")
    .doc("Enable logging of join cardinality statistics to help diagnose performance issues. " +
      "When enabled, logs task context, key data types, join condition, row counts, and " +
      "distinct key counts for both left and right sides of joins. This can help identify " +
      "problematic join patterns but may impact performance due to the additional computation " +
      "required to calculate distinct counts.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD =
    conf("spark.rapids.sql.join.gatherer.sizeEstimateThreshold")
    .doc("When a join is gathered we try to output a batch that is close to the target batch " +
      "size. But that can be expensive so we use a heuristic to estimate the size. It is based " +
      "on the average size of left and right rows. If that average size times the number of " +
      "output rows is less than the target batch size times this threshold, we assume that " +
      "output will fit and output it. Otherwise we do an expensive calculation to get the " +
      "real size of the output. This value can be between 0.0, which disables the " +
      "cheap heuristic, and 2.0, which allows the cheap heuristic to exceed the target batch size.")
    .internal()
    .doubleConf
    .checkValue(v => v >= 0.0 && v <= 2.0, "The threshold must be between 0.0 and 2.0.")
    .createWithDefault(0.75)

  val SHUFFLED_HASH_JOIN_OPTIMIZE_SHUFFLE =
    conf("spark.rapids.sql.shuffledHashJoin.optimizeShuffle")
      .doc("Enable or disable an optimization where shuffled build side batches are kept " +
        "on the host while the first stream batch is loaded onto the GPU. The optimization " +
        "increases off-heap host memory usage to avoid holding onto the GPU semaphore while " +
        "waiting for stream side IO.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val USE_SHUFFLED_SYMMETRIC_HASH_JOIN = conf("spark.rapids.sql.join.useShuffledSymmetricHashJoin")
    .doc("Use the experimental shuffle symmetric hash join designed to improve handling of large " +
      "symmetric joins. Requires spark.rapids.sql.shuffledHashJoin.optimizeShuffle=true.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val USE_SHUFFLED_ASYMMETRIC_HASH_JOIN =
    conf("spark.rapids.sql.join.useShuffledAsymmetricHashJoin")
      .doc("Use the experimental shuffle asymmetric hash join designed to improve handling of " +
        "large joins for left and right outer joins. Requires " +
        "spark.rapids.sql.shuffledHashJoin.optimizeShuffle=true and " +
        "spark.rapids.sql.join.useShuffledSymmetricHashJoin=true")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val JOIN_OUTER_MAGNIFICATION_THRESHOLD =
    conf("spark.rapids.sql.join.outer.magnificationFactorThreshold")
      .doc("The magnification factor threshold at which outer joins will consider using the " +
        "unnatural side of the join to build the hash table")
      .internal()
      .integerConf
      .createWithDefault(10000)

  val BUCKET_JOIN_IO_PREFETCH =
    conf("spark.rapids.sql.join.bucket.IOPrefetch")
      .doc("Enable I/O prefetch of the upstream bucket scans if there is a SizedHashJoin " +
        "in downstream. Please notice the prefetch will only take affect with " +
        "MultiFileCloudPartitionReader")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val STABLE_SORT = conf("spark.rapids.sql.stableSort.enabled")
      .doc("Enable or disable stable sorting. Apache Spark's sorting is typically a stable " +
          "sort, but sort stability cannot be guaranteed in distributed work loads because the " +
          "order in which upstream data arrives to a task is not guaranteed. Sort stability then " +
          "only matters when reading and sorting data from a file using a single task/partition. " +
          "Because of limitations in the plugin when you enable stable sorting all of the data " +
          "for a single task will be combined into a single batch before sorting. This currently " +
          "disables spilling from GPU memory if the data size is too large.")
      .booleanConf
      .createWithDefault(false)

  val FILE_SCAN_PRUNE_PARTITION_ENABLED = conf("spark.rapids.sql.fileScanPrunePartition.enabled")
    .doc("Enable or disable the partition column pruning for v1 file scan. Spark always asks " +
        "for all the partition columns even a query doesn't need them. Generation of " +
        "partition columns is relatively expensive for the GPU. Enabling this allows the " +
        "GPU to generate only required partition columns to save time and GPU " +
        "memory.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  // METRICS

  val METRICS_LEVEL = conf("spark.rapids.sql.metrics.level")
      .doc("GPU plans can produce a lot more metrics than CPU plans do. In very large " +
          "queries this can sometimes result in going over the max result size limit for the " +
          "driver. Supported values include " +
          "DEBUG which will enable all metrics supported and typically only needs to be enabled " +
          "when debugging the plugin. " +
          "MODERATE which should output enough metrics to understand how long each part of the " +
          "query is taking and how much data is going to each part of the query. " +
          "ESSENTIAL which disables most metrics except those Apache Spark CPU plans will also " +
          "report or their equivalents.")
      .commonlyUsed()
      .stringConf
      .transform(_.toUpperCase(java.util.Locale.ROOT))
      .checkValues(Set("DEBUG", "MODERATE", "ESSENTIAL"))
      .createWithDefault("MODERATE")

  val PROFILE_PATH = conf("spark.rapids.profile.pathPrefix")
    .doc("Enables profiling and specifies a URI path to use when writing profile data")
    .internal()
    .stringConf
    .createOptional

  val PROFILE_EXECUTORS = conf("spark.rapids.profile.executors")
    .doc("Comma-separated list of executors IDs and hyphenated ranges of executor IDs to " +
      "profile when profiling is enabled")
    .internal()
    .stringConf
    .createWithDefault("0")

  val PROFILE_TIME_RANGES_SECONDS = conf("spark.rapids.profile.timeRangesInSeconds")
    .doc("Comma-separated list of start-end ranges of time, in seconds, since executor startup " +
      "to start and stop profiling. For example, a value of 10-30,100-110 will have the profiler " +
      "wait for 10 seconds after executor startup then profile for 20 seconds, then wait for " +
      "70 seconds then profile again for the next 10 seconds")
    .internal()
    .stringConf
    .createOptional

  val PROFILE_JOBS = conf("spark.rapids.profile.jobs")
    .doc("Comma-separated list of job IDs and hyphenated ranges of job IDs to " +
      "profile when profiling is enabled")
    .internal()
    .stringConf
    .createOptional

  val PROFILE_STAGES = conf("spark.rapids.profile.stages")
    .doc("Comma-separated list of stage IDs and hyphenated ranges of stage IDs to " +
      "profile when profiling is enabled")
    .internal()
    .stringConf
    .createOptional

  val PROFILE_TASK_LIMIT_PER_STAGE = conf("spark.rapids.profile.taskLimitPerStage")
    .doc("Limit the number of tasks to profile per stage. A value <= 0 will profile all tasks.")
    .internal()
    .integerConf
    .createWithDefault(0)

  val PROFILE_ASYNC_ALLOC_CAPTURE = conf("spark.rapids.profile.asyncAllocCapture")
    .doc("Whether the profiler should capture async CUDA allocation and free events")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val PROFILE_DRIVER_POLL_MILLIS = conf("spark.rapids.profile.driverPollMillis")
    .doc("Interval in milliseconds the executors will poll for job and stage completion when " +
      "stage-level profiling is used.")
    .internal()
    .integerConf
    .createWithDefault(1000)

  val PROFILE_COMPRESSION = conf("spark.rapids.profile.compression")
    .doc("Specifies the compression codec to use when writing profile data, one of " +
      "zstd or none")
    .internal()
    .stringConf
    .transform(_.toLowerCase(java.util.Locale.ROOT))
    .checkValues(Set("zstd", "none"))
    .createWithDefault("zstd")

  val PROFILE_FLUSH_PERIOD_MILLIS = conf("spark.rapids.profile.flushPeriodMillis")
    .doc("Specifies the time period in milliseconds to flush profile records. " +
      "A value <= 0 will disable time period flushing.")
    .internal()
    .integerConf
    .createWithDefault(0)

  val PROFILE_WRITE_BUFFER_SIZE = conf("spark.rapids.profile.writeBufferSize")
    .doc("Buffer size to use when writing profile records.")
    .internal()
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(8 * 1024 * 1024)

  // ASYNC PROFILER (FOR FLAME GRAPH)

  val ASYNC_PROFILER_PATH_PREFIX = conf("spark.rapids.flameGraph.pathPrefix")
    .doc("Enables collecting flame graph (with async profiler) and specifies " +
      "a file prefix to use when writing the JFR file by async-profiler. " +
      "The async-profiler will write a flame graph file for each stage. " +
      "It is strongly recommended to set 'spark.scheduler.mode' to 'FIFO' " +
      "so that there is a clean boundary between stages, " +
      "and then we can better understand each stage.")
    .stringConf
    .createOptional

  val ASYNC_PROFILER_EXECUTORS = conf("spark.rapids.flameGraph.executors")
    .doc("Comma-separated list of executors IDs and hyphenated ranges of executor IDs to " +
      "profile when async-profiler (for flame graph) is enabled. " +
      "The default value '*' means all executors")
    .stringConf
    .createWithDefault("*")

  val ASYNC_PROFILER_PROFILE_OPTIONS = conf("spark.rapids.flameGraph.asyncProfiler.options")
    .doc("The cuDF plugin uses the async profiler to generate flame graphs. " +
      "You can specify profiler options via this property. " +
      "The plugin supports all options except for the 'file' option listed in " +
      "https://github.com/async-profiler/async-profiler/blob/" +
      "b3f58429f5c0252e9ced3f0fcb444fed17671321/" +
      "docs/ProfilerOptions.md#options-applicable-to-any-output-format ." +
      "The default values is 'jfr,event=cpu,wall=10ms'.")
    .stringConf
    .createWithDefault("jfr,event=cpu,wall=10ms")

  val ASYNC_PROFILER_JFR_COMPRESSION = conf("spark.rapids.flameGraph.jfr.compression")
    .doc("Enable compression for JFR files generated by async profiler. " +
      "When enabled, JFR files will be compressed after generation to save disk space.")
    .booleanConf
    .createWithDefault(false)

  val ASYNC_PROFILER_STAGE_EPOCH_INTERVAL = conf("spark.rapids.flameGraph.stageEpochInterval")
    .doc("Interval in seconds to determine the current stage epoch based on running task " +
      "counts. The profiler will check which stage has the most running tasks and profile " +
      "that stage during each epoch. This allows profiling when multiple stages run " +
      "concurrently even if FIFO scheduling is already chosen.")
    .integerConf
    .createWithDefault(5)

}
