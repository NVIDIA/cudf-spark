/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import java.io.{File, FileOutputStream}

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, NvtxRange}

import org.apache.spark.internal.Logging

object RangeDebugger extends Logging {
  val threadLocalStack = new ThreadLocal[mutable.ArrayStack[NvtxId]] {
    override def initialValue(): mutable.ArrayStack[NvtxId] = mutable.ArrayStack[NvtxId]()
  }

  private def dumpOrderErrorMessage(popped: Option[NvtxId], elem: NvtxId): Unit = {
    logError(s"OUT OF ORDER POP of $elem")
    logError(s"TOP OF STACK IS ${popped.getOrElse("<nil>")}")
    val stackTrace = Thread.currentThread.getStackTrace
    stackTrace.foreach(elem => logError(elem.toString))
  }

  def push(elem: NvtxId): Unit = {
    threadLocalStack.get().push(elem)
  }

  def pop(elem: NvtxId): Unit = {
    val stack = threadLocalStack.get()
    if (stack.nonEmpty) {
      val popped = stack.pop()
      if (!popped.equals(elem)) {
        dumpOrderErrorMessage(Some(popped), elem)
      }
    } else {
      dumpOrderErrorMessage(None, elem)
    }
  }
}

sealed case class NvtxId private(name: String, color: NvtxColor, doc: String) {
  private val isEnabled = java.lang.Boolean.getBoolean("ai.rapids.cudf.nvtx.enabled")
  private val isDebug = java.lang.Boolean.getBoolean("ai.rapids.cudf.nvtx.debug")

  def help(): Unit = println(s"$name|$doc")

  def push(): NvtxId = {
    if (isEnabled) {
      NvtxRange.pushRange(name, color)
      if (isDebug) {
        RangeDebugger.push(this)
      }
    }
    this
  }

  def pop(): Unit = {
    if (isEnabled) {
      if (isDebug) {
        RangeDebugger.pop(this)
      }
      NvtxRange.popRange()
    }
  }

  def apply[V](block: => V): V = {
    try {
      push()
      block
    } finally {
      pop()
    }
  }
}

object NvtxRegistry {
  val registeredRanges: mutable.Map[String, NvtxId] = mutable.Map[String, NvtxId]()

  private def register(id: NvtxId): Unit = {
    if (registeredRanges.contains(id.name)) {
      throw new IllegalArgumentException(s"Collision detected for key: ${id.name}")
    } else {
      registeredRanges += (id.name -> id)
    }
  }

  val ACQUIRE_GPU: NvtxId = NvtxId("Acquire GPU", NvtxColor.RED,
      "Time waiting for GPU semaphore to be acquired")

  val RELEASE_GPU: NvtxId = NvtxId("Release GPU", NvtxColor.RED,
    "Releasing the GPU semaphore")

  val THREADED_WRITER_WRITE: NvtxId = NvtxId("ThreadedWriter.write", NvtxColor.RED,
    "Rapids Shuffle Manager (multi threaded) writing")

  val THREADED_READER_READ: NvtxId = NvtxId("ThreadedReader.read", NvtxColor.PURPLE,
    "Rapids Shuffle Manager (multi threaded) reading")

  val WAITING_FOR_WRITES: NvtxId = NvtxId("WaitingForWrites", NvtxColor.PURPLE,
    "Rapids Shuffle Manager (multi threaded) is waiting for any queued writes to finish before " +
      "finalizing the map output writer")

  val COMMIT_SHUFFLE: NvtxId = NvtxId("CommitShuffle", NvtxColor.RED,
    "After all temporary shuffle writes are done, produce a single file " +
      "(shuffle_[map_id]_0) in the commit phase")

  val PARALLEL_DESERIALIZER_ITERATOR_NEXT: NvtxId = NvtxId("ParallelDeserializerIterator.next",
    NvtxColor.CYAN, "Calling next on the MT shuffle reader iterator")

  val BATCH_WAIT: NvtxId = NvtxId("BatchWait", NvtxColor.CYAN,
    "Rapids Shuffle Manager (multi threaded) reader blocked waiting for batches to finish decoding")

  val QUEUE_FETCHED: NvtxId = NvtxId("queueFetched", NvtxColor.YELLOW, "MT shuffle manager is " +
    "using the RapidsShuffleBlockFetcherIterator to queue the next set of fetched results")

  val RAPIDS_CACHING_WRITER_WRITE: NvtxId = NvtxId("RapidsCachingWriter.write", NvtxColor.CYAN,
    "Rapids Shuffle Manager (ucx) writing")

  val GET_MAP_SIZES_BY_EXEC_ID: NvtxId = NvtxId("getMapSizesByExecId", NvtxColor.CYAN,
    "Call to internal Spark API for retrieving size and location of shuffle map output blocks")

  val GPU_COALESCE_BATCHES_COLLECT: NvtxId = NvtxId("GpuCoalesceBatches: collect", NvtxColor.BLUE,
    "GPU combining of small batches post-kernel processing")

  val BUILD_BATCH_COLLECT: NvtxId = NvtxId("build batch: collect", NvtxColor.BLUE,
    "Perform a join where the build side fits in a single GPU batch")

  val GPU_COALESCE_ITERATOR: NvtxId = NvtxId("AbstractGpuCoalesceIterator", NvtxColor.BLUE,
    "Default range for a code path in the AbstractGpuCoalesceIterator for an op which " +
      "is not explicitly documented in its own range")

  val SHUFFLED_JOIN_STREAM: NvtxId = NvtxId("shuffled join stream", NvtxColor.BLUE,
    "GpuShuffledHashJoinExec op is preparing build batches for join")

  val HASH_JOIN_BUILD: NvtxId = NvtxId("hash join build", NvtxColor.BLUE,
    "IO time on the build side data for the following join")

  val PROBE_LEFT: NvtxId = NvtxId("probe left", NvtxColor.BLUE,
    "Probing the left side of a join input iterator to get the data size for preparing the join")

  val PROBE_RIGHT: NvtxId = NvtxId("probe right", NvtxColor.BLUE,
    "Probing the right side of a join input iterator to get the data size for preparing the join")

  val FETCH_JOIN_STREAM: NvtxId = NvtxId("fetch join stream", NvtxColor.BLUE,
    "IO time on the stream side data for the following join")

  val BROADCAST_JOIN_STREAM: NvtxId = NvtxId("broadcast join stream", NvtxColor.BLUE,
    "time it takes to materialize a broadcast batch on the host")

  val GPU_KUDO_SERIALIZE: NvtxId = NvtxId("gpuKudoSerialize", NvtxColor.YELLOW,
    "Perform kudo serialization on the gpu")

  val GPU_KUDO_COPY_TO_HOST: NvtxId = NvtxId("gpuKudoCopyToHost", NvtxColor.GREEN,
    "copy gpu kudo serialized outputs back to the host")

  val GPU_KUDO_SLICE_BUFFERS: NvtxId = NvtxId("gpuKudoSliceBuffers", NvtxColor.RED,
    "slice kudo serialized buffers on host into partitions")

  val GPU_KUDO_WRITE_BUFFERS: NvtxId = NvtxId("gpuKudoWriteBuffers", NvtxColor.CYAN,
    "write sliced kudo serialized buffers to output blocks")

  val AGG_PRE_PROCESS: NvtxId = NvtxId("agg pre-process", NvtxColor.DARK_GREEN,
    "Pre-processing step for aggregation before calling cuDF aggregate, " +
      "including casting and struct creation")

  val AGG_REDUCE: NvtxId = NvtxId("agg reduce", NvtxColor.BLUE,
    "Reduction aggregation for operations without grouping keys")

  val AGG_GROUPBY: NvtxId = NvtxId("agg groupby", NvtxColor.BLUE,
    "Group-by aggregation using cuDF groupBy operation")

  val AGG_POST_PROCESS: NvtxId = NvtxId("agg post-process", NvtxColor.ORANGE,
    "Post-processing step for aggregation, including casting and struct decomposition")

  val HASH_PARTITION: NvtxId = NvtxId("hash partition", NvtxColor.PURPLE,
    "Partitioning data based on hash values")

  val HASH_PARTITION_SLICE: NvtxId = NvtxId("hash partition slice", NvtxColor.BLUE,
    "Slicing partitioned table into individual partitions")

  val SORT_COPY_BOUNDARIES: NvtxId = NvtxId("sort copy boundaries", NvtxColor.PURPLE,
    "Copying boundary data for sort operation")

  val SORT_TO_UNSAFE_ROW: NvtxId = NvtxId("sort to unsafe row", NvtxColor.RED,
    "Converting sorted data to unsafe row format")

  val SORT_LOWER_BOUNDARIES: NvtxId = NvtxId("sort lower boundaries", NvtxColor.ORANGE,
    "Computing lower boundaries for sort operation")

  val JOIN_FIRST_STREAM_BATCH: NvtxId = NvtxId("join first stream batch", NvtxColor.RED,
    "Fetching and processing first batch from stream side of join")

  val JOIN_ASYMMETRIC_PROBE_FETCH: NvtxId = NvtxId("join asymmetric probe fetch",
    NvtxColor.YELLOW, "Asymmetric join probe side data fetch")

  val JOIN_ASYMMETRIC_FETCH: NvtxId = NvtxId("join asymmetric fetch", NvtxColor.YELLOW,
    "Asymmetric join data fetch")

  val PARQUET_READ_FOOTER_BYTES: NvtxId = NvtxId("parquet read footer bytes",
    NvtxColor.YELLOW, "Reading raw footer bytes from Parquet file")

  val PARQUET_PARSE_FILTER_FOOTER: NvtxId = NvtxId("parquet parse filter footer",
    NvtxColor.RED, "Parsing and filtering Parquet footer by range")

  val PARQUET_READ_FOOTER: NvtxId = NvtxId("parquet read footer", NvtxColor.YELLOW,
    "Reading and parsing complete Parquet footer")

  val PARQUET_FILTER_BLOCKS: NvtxId = NvtxId("parquet filter blocks", NvtxColor.PURPLE,
    "Filtering Parquet row group blocks based on predicates")

  val PARQUET_READ_FILTERED_FOOTER: NvtxId = NvtxId("parquet read filtered footer",
    NvtxColor.YELLOW, "Reading filtered Parquet footer with selected row groups")

  val PARQUET_GET_BLOCKS_WITH_FILTER: NvtxId = NvtxId("parquet get blocks with filter",
    NvtxColor.CYAN, "Retrieving Parquet blocks after applying filters")

  val PARQUET_CLIP_SCHEMA: NvtxId = NvtxId("parquet clip schema", NvtxColor.DARK_GREEN,
    "Clipping Parquet schema to required columns")

  val PARQUET_BUFFER_FILE_SPLIT: NvtxId = NvtxId("parquet buffer file split",
    NvtxColor.YELLOW, "Splitting Parquet file into buffer chunks for reading")

  val ORC_BUFFER_FILE_SPLIT: NvtxId = NvtxId("orc buffer file split", NvtxColor.YELLOW,
    "Splitting ORC file into buffer chunks for reading")

  val AVRO_BUFFER_FILE_SPLIT: NvtxId = NvtxId("avro buffer file split", NvtxColor.YELLOW,
    "Splitting Avro file into buffer chunks for reading")

  val JSON_CONVERT_TABLE: NvtxId = NvtxId("json convert table", NvtxColor.RED,
    "Converting JSON table to desired schema type")

  val JSON_CONVERT_DATETIME: NvtxId = NvtxId("json convert datetime", NvtxColor.RED,
    "Converting JSON datetime types to Spark datetime types")

  val SHUFFLE_FETCH_FIRST_BATCH: NvtxId = NvtxId("shuffle fetch first batch",
    NvtxColor.YELLOW, "Fetching first batch in shuffle coalesce operation")

  val SHUFFLE_CONCAT_LOAD_BATCH: NvtxId = NvtxId("shuffle concat load batch",
    NvtxColor.YELLOW, "Concatenating and loading batch in shuffle operation")

  val CARTESIAN_PRODUCT_SERIALIZE: NvtxId = NvtxId("cartesian product serialize",
    NvtxColor.PURPLE, "Serializing batch for cartesian product operation")

  val CARTESIAN_PRODUCT_DESERIALIZE: NvtxId = NvtxId("cartesian product deserialize",
    NvtxColor.PURPLE, "Deserializing batch from cartesian product operation")

  val ROW_TO_COLUMNAR: NvtxId = NvtxId("row to columnar", NvtxColor.CYAN,
    "Converting row-based data to columnar format")

  val GENERATE_GET_ROW_BYTE_COUNT: NvtxId = NvtxId("generate get row byte count",
    NvtxColor.GREEN, "Computing byte count for generated rows")

  val GENERATE_ESTIMATE_REPETITION: NvtxId = NvtxId("generate estimate repetition",
    NvtxColor.BLUE, "Estimating repetition count for generate operation")

  val REDUCTION_MERGE_M2: NvtxId = NvtxId("reduction merge m2", NvtxColor.ORANGE,
    "Merging M2 values during variance/stddev reduction")

  val PROJECT_TIER: NvtxId = NvtxId("project tier", NvtxColor.ORANGE,
    "Executing tiered projection operation")

  val RANDOM_EXPR: NvtxId = NvtxId("random expr", NvtxColor.RED,
    "Generating random values in expression evaluation")

  val RAPIDS_CACHING_READER_READ: NvtxId = NvtxId("RapidsCachingReader.read",
    NvtxColor.DARK_GREEN, "Reading shuffle data from cache or remote transport")

  val RAPIDS_CACHING_READER_READ_LOCAL: NvtxId = NvtxId("RapidsCachingReader read local",
    NvtxColor.GREEN, "Reading shuffle blocks from local cache")

  val RAPIDS_SHUFFLE_ITERATOR_PREP: NvtxId = NvtxId("RapidsShuffleIterator prep",
    NvtxColor.BLUE, "Preparing shuffle iterator with cached and remote blocks")

  val RAPIDS_SHUFFLE_ITERATOR_NEXT: NvtxId = NvtxId("RapidsShuffleIterator.next",
    NvtxColor.RED, "Fetching next batch from Rapids shuffle iterator")

  val RAPIDS_SHUFFLE_ITERATOR_GOT_BATCH: NvtxId = NvtxId("RapidsShuffleIterator.gotBatch",
    NvtxColor.PURPLE, "Processing batch received from Rapids shuffle")

  val RAPIDS_CACHING_WRITER_CLOSE: NvtxId = NvtxId("RapidsCachingWriter.close",
    NvtxColor.CYAN, "Closing Rapids caching writer and finalizing shuffle output")

  val COLUMNAR_BATCH_SERIALIZE: NvtxId = NvtxId("Columnar batch serialize",
    NvtxColor.YELLOW, "Serializing columnar batch for shuffle or storage")

  val COLUMNAR_BATCH_SERIALIZE_ROW_ONLY: NvtxId = NvtxId("Columnar batch serialize row only",
    NvtxColor.YELLOW, "Serializing row-only batch (no GPU data)")

  val TRANSPORT_COPY_BUFFER: NvtxId = NvtxId("Transport copy buffer", NvtxColor.RED,
    "Copying buffer for Rapids shuffle transport")

  val BRING_BACK_TO_HOST: NvtxId = NvtxId("Bring back to host", NvtxColor.RED,
    "Copying GPU data back to host memory")

  val ROUND_ROBIN_PARTITION: NvtxId = NvtxId("Round robin partition", NvtxColor.PURPLE,
    "Partitioning data using round-robin strategy")

  val ROUND_ROBIN_PARTITION_SLICE: NvtxId = NvtxId("Round robin partition slice",
    NvtxColor.BLUE, "Slicing data for round-robin partitioning")

  // Filter operations
  val FILTER_BATCH: NvtxId = NvtxId("filter batch", NvtxColor.YELLOW,
    "Filtering rows from a columnar batch")

  // Project operations
  val PROJECT_EXEC: NvtxId = NvtxId("ProjectExec", NvtxColor.CYAN,
    "Executing projection operation on columnar batch")

  val PROJECT_AST: NvtxId = NvtxId("Project AST", NvtxColor.CYAN,
    "Applying AST-based projection to batch")

  val COMPILE_ASTS: NvtxId = NvtxId("Compile ASTs", NvtxColor.ORANGE,
    "Compiling abstract syntax trees for expression evaluation")

  // Aggregate operations
  val COMPUTE_AGGREGATE: NvtxId = NvtxId("computeAggregate", NvtxColor.CYAN,
    "Computing aggregation on input batch")

  val FINALIZE_AGG: NvtxId = NvtxId("finalize agg", NvtxColor.DARK_GREEN,
    "Finalizing aggregation results")

  val POST_PROCESS_AGG: NvtxId = NvtxId("post-process", NvtxColor.ORANGE,
    "Post-processing aggregation results")

  val CONCATENATE_BATCHES: NvtxId = NvtxId("concatenateBatches", NvtxColor.BLUE,
    "Concatenating multiple batches into one")

  val AGG_REPARTITION: NvtxId = NvtxId("agg repartition", NvtxColor.PURPLE,
    "Repartitioning data for aggregation")

  val REPARTITION_AGG_ITERATOR_NEXT: NvtxId = NvtxId("RepartitionAggregateIterator.next",
    NvtxColor.CYAN, "Fetching next batch from repartition aggregate iterator")

  val DYNAMIC_SORT_HEURISTIC: NvtxId = NvtxId("dynamic sort heuristic", NvtxColor.BLUE,
    "Applying dynamic sort heuristic for aggregation")

  // Sort operations
  val SORT: NvtxId = NvtxId("sort", NvtxColor.DARK_GREEN,
    "Sorting columnar data")

  val MERGE_SORT: NvtxId = NvtxId("merge sort", NvtxColor.DARK_GREEN,
    "Merge sorting multiple sorted batches")

  val SORT_OP: NvtxId = NvtxId("sort op", NvtxColor.WHITE,
    "General sort operation")

  val SORT_ORDER: NvtxId = NvtxId("sort_order", NvtxColor.DARK_GREEN,
    "Computing sort order for data")

  val GATHER_SORT: NvtxId = NvtxId("gather", NvtxColor.DARK_GREEN,
    "Gathering sorted data based on indices")

  val SPLIT_INPUT_BATCH: NvtxId = NvtxId("split input batch", NvtxColor.CYAN,
    "Splitting input batch for sorting")

  val SORT_NEXT_OUTPUT_BATCH: NvtxId = NvtxId("Sort next output batch", NvtxColor.CYAN,
    "Fetching next sorted output batch")

  // Join operations
  val HASH_JOIN_GATHER_MAP: NvtxId = NvtxId("hash join gather map", NvtxColor.ORANGE,
    "Gathering hash join results using gather map")

  val FULL_HASH_JOIN_GATHER_MAP: NvtxId = NvtxId("full hash join gather map",
    NvtxColor.ORANGE, "Gathering full hash join results")

  val UPDATE_TRACKING_MASK: NvtxId = NvtxId("update tracking mask", NvtxColor.DARK_GREEN,
    "Updating tracking mask for join operation")

  val GET_FINAL_BATCH: NvtxId = NvtxId("get final batch", NvtxColor.ORANGE,
    "Getting final batch from join operation")

  val EXISTENCE_JOIN_SCATTER_MAP: NvtxId = NvtxId("existence join scatter map",
    NvtxColor.ORANGE, "Creating scatter map for existence join")

  val EXISTENCE_JOIN_BATCH: NvtxId = NvtxId("existence join batch", NvtxColor.ORANGE,
    "Processing batch for existence join")

  val BUILD_JOIN_TABLE: NvtxId = NvtxId("build join table", NvtxColor.GREEN,
    "Building hash table for join operation")

  // Window operations
  val WINDOW: NvtxId = NvtxId("window", NvtxColor.CYAN,
    "Computing window function results")

  val RUNNING_WINDOW: NvtxId = NvtxId("RunningWindow", NvtxColor.CYAN,
    "Computing running window aggregation")

  val DOUBLE_BATCHED_WINDOW_PRE: NvtxId = NvtxId("DoubleBatchedWindow_PRE", NvtxColor.CYAN,
    "Pre-processing for double-batched window operation")

  val DOUBLE_BATCHED_WINDOW_POST: NvtxId = NvtxId("DoubleBatchedWindow_POST",
    NvtxColor.BLUE, "Post-processing for double-batched window operation")

  // I/O decode operations
  val PARQUET_DECODE: NvtxId = NvtxId("Parquet decode", NvtxColor.DARK_GREEN,
    "Decoding Parquet data to columnar format")

  val ORC_DECODE: NvtxId = NvtxId("ORC decode", NvtxColor.DARK_GREEN,
    "Decoding ORC data to columnar format")

  val AVRO_DECODE: NvtxId = NvtxId("Avro decode", NvtxColor.DARK_GREEN,
    "Decoding Avro data to columnar format")

  val BUFFER_FILE_SPLIT: NvtxId = NvtxId("Buffer file split", NvtxColor.YELLOW,
    "Buffering file split for reading")

  val FILE_FORMAT_READ_BATCH: NvtxId = NvtxId("file format readBatch", NvtxColor.GREEN,
    "Reading batch of data from file format (Parquet/ORC/Avro/CSV/JSON)")

  val FILE_FORMAT_WRITE: NvtxId = NvtxId("GPU file format write", NvtxColor.BLUE,
    "Writing batch of data to file format on GPU")

  val TASK_RANGE: NvtxId = NvtxId("Spark Task", NvtxColor.DARK_GREEN,
    "Spark task execution range for stage and task tracking")

  val SHUFFLE_TRANSFER_REQUEST: NvtxId = NvtxId("Shuffle Transfer Request", NvtxColor.CYAN,
    "Handling shuffle data transfer request")

  val ASYNC_SHUFFLE_READ: NvtxId = NvtxId("Async Shuffle Read", NvtxColor.BLUE,
    "Asynchronous shuffle read operation")

  val ASYNC_SHUFFLE_BUFFER: NvtxId = NvtxId("Async Shuffle Buffer", NvtxColor.ORANGE,
    "Asynchronous shuffle buffering operation")

  val SHUFFLE_CONCAT_CPU: NvtxId = NvtxId("Shuffle Concat CPU", NvtxColor.PURPLE,
    "Concatenating shuffle data on CPU")

  val SLICE_INTERNAL_GPU: NvtxId = NvtxId("sliceInternalOnGpu", NvtxColor.CYAN,
    "Slicing partition data on GPU")

  val SLICE_INTERNAL_CPU: NvtxId = NvtxId("sliceInternalOnCpu", NvtxColor.CYAN,
    "Slicing partition data on CPU")

  // Serialization and deserialization
  val READ_HEADER: NvtxId = NvtxId("Read Header", NvtxColor.YELLOW,
    "Reading serialized batch header")

  val READ_BATCH: NvtxId = NvtxId("Read Batch", NvtxColor.YELLOW,
    "Reading serialized batch data")

  val SERIALIZE_BATCH: NvtxId = NvtxId("Serialize Batch", NvtxColor.YELLOW,
    "Serializing columnar batch")

  val SERIALIZE_ROW_ONLY_BATCH: NvtxId = NvtxId("Serialize Row Only Batch", NvtxColor.YELLOW,
    "Serializing row-only batch")

  // Broadcast operations
  val BROADCAST_MANIFEST_BATCH: NvtxId = NvtxId("broadcast manifest batch", NvtxColor.PURPLE,
    "Creating broadcast manifest batch")

  val DESERIALIZE_BATCH: NvtxId = NvtxId("DeserializeBatch", NvtxColor.PURPLE,
    "Deserializing broadcast batch")

  val SERIALIZE_BROADCAST_BATCH: NvtxId = NvtxId("SerializeBatch", NvtxColor.PURPLE,
    "Serializing broadcast batch")

  val HOST_DESERIALIZE_BATCH: NvtxId = NvtxId("HostDeserializeBatch", NvtxColor.PURPLE,
    "Deserializing batch on host")

  val GET_BROADCAST_BATCH: NvtxId = NvtxId("getBroadcastBatch", NvtxColor.YELLOW,
    "Getting broadcast batch")

  val FIRST_STREAM_BATCH: NvtxId = NvtxId("first stream batch", NvtxColor.RED,
    "Processing first stream batch")

  // Compression operations
  val BATCH_COMPRESS: NvtxId = NvtxId("batch compress", NvtxColor.ORANGE,
    "Compressing batch data")

  val COPY_COMPRESSED_BUFFERS: NvtxId = NvtxId("copy compressed buffers", NvtxColor.PURPLE,
    "Copying compressed buffer data")

  val BATCH_DECOMPRESS: NvtxId = NvtxId("batch decompress", NvtxColor.ORANGE,
    "Decompressing batch data")

  val ZSTD_POST_PROCESS: NvtxId = NvtxId("zstd post process", NvtxColor.YELLOW,
    "Post-processing ZSTD compressed data")

  val LZ4_POST_PROCESS: NvtxId = NvtxId("lz4 post process", NvtxColor.YELLOW,
    "Post-processing LZ4 compressed data")

  val ALLOC_OUTPUT_BUFS: NvtxId = NvtxId("alloc output bufs", NvtxColor.YELLOW,
    "Allocating output buffers for compression")

  // Shuffle client/server operations
  val BATCH_RECEIVED: NvtxId = NvtxId("BATCH RECEIVED", NvtxColor.DARK_GREEN,
    "Processing received shuffle batch")

  val CLIENT_FETCH: NvtxId = NvtxId("Client.fetch", NvtxColor.PURPLE,
    "Fetching data from shuffle server")

  val CLIENT_HANDLE_META: NvtxId = NvtxId("Client.handleMeta", NvtxColor.CYAN,
    "Handling metadata from shuffle server")

  val BUFFER_CALLBACK: NvtxId = NvtxId("Buffer Callback", NvtxColor.RED,
    "Processing buffer callback")

  val HANDLE_META_REQUEST: NvtxId = NvtxId("Handle Meta Request", NvtxColor.PURPLE,
    "Handling metadata request on shuffle server")

  val DO_HANDLE_META: NvtxId = NvtxId("doHandleMeta", NvtxColor.PURPLE,
    "Processing metadata handling")

  val CONSUME_WINDOW: NvtxId = NvtxId("consumeWindow", NvtxColor.PURPLE,
    "Consuming transfer window")

  // Join operations
  val CALC_GATHER_SIZE: NvtxId = NvtxId("calc gather size", NvtxColor.YELLOW,
    "Calculating gather operation size")

  val GET_JOIN_BATCH: NvtxId = NvtxId("get batch", NvtxColor.RED,
    "Getting join batch")

  val SPILL_JOIN_BATCH: NvtxId = NvtxId("spill batch", NvtxColor.RED,
    "Spilling join batch")

  val GET_JOIN_MAP: NvtxId = NvtxId("get map", NvtxColor.RED,
    "Getting join map")

  val SPILL_JOIN_MAP: NvtxId = NvtxId("spill map", NvtxColor.RED,
    "Spilling join map")

  // File format read operations
  val BUFFER_FILE_SPLIT_TEXT: NvtxId = NvtxId("Buffer file split text", NvtxColor.YELLOW,
    "Buffering text file split")

  val ORC_READ_BATCHES: NvtxId = NvtxId("ORC readBatches", NvtxColor.GREEN,
    "Reading ORC batches")

  val PARQUET_READ_BATCH: NvtxId = NvtxId("Parquet readBatch", NvtxColor.GREEN,
    "Reading Parquet batch")

  val AVRO_READ_BATCH: NvtxId = NvtxId("Avro readBatch", NvtxColor.GREEN,
    "Reading Avro batch")

  // Python/Arrow operations
  val READ_PYTHON_BATCH: NvtxId = NvtxId("read python batch", NvtxColor.DARK_GREEN,
    "Reading Python batch")

  val WRITE_PYTHON_BATCH: NvtxId = NvtxId("write python batch", NvtxColor.DARK_GREEN,
    "Writing Python batch")

  // Z-order operations
  val INTERLEAVE_BITS: NvtxId = NvtxId("interleaveBits", NvtxColor.PURPLE,
    "Interleaving bits for Z-order")

  val HILBERT_INDEX: NvtxId = NvtxId("HILBERT INDEX", NvtxColor.PURPLE,
    "Computing Hilbert index")

  val GPU_PARTITIONER: NvtxId = NvtxId("GpuPartitioner", NvtxColor.GREEN,
    "GPU partitioner operation")

  // Concatenation operations
  val CONCAT_PENDING: NvtxId = NvtxId("concat pending", NvtxColor.CYAN,
    "Concatenating pending batches")

  val GPU_COALESCE_BATCHES_CONCAT: NvtxId = NvtxId("GpuCoalesceBatches concat", NvtxColor.CYAN,
    "Concatenating batches in GpuCoalesceBatches")

  val SINGLE_BUILD_BATCH_CONCAT: NvtxId = NvtxId("single build batch concat", NvtxColor.CYAN,
    "Concatenating batches for single build batch")

  val HOST_COLUMNAR_TO_GPU_CONCAT: NvtxId = NvtxId("HostColumnarToGpu concat", NvtxColor.CYAN,
    "Concatenating batches in HostColumnarToGpu")

  // Decode operations
  val HIVE_DECODE: NvtxId = NvtxId("HIVE decode", NvtxColor.DARK_GREEN,
    "Decoding Hive table data")

  val JSON_DECODE_SCAN: NvtxId = NvtxId("JSON decode", NvtxColor.DARK_GREEN,
    "Decoding JSON data")

  val CSV_DECODE: NvtxId = NvtxId("CSV decode", NvtxColor.DARK_GREEN,
    "Decoding CSV data")

  // Join operations
  val JOIN_GATHER: NvtxId = NvtxId("Join gather", NvtxColor.DARK_GREEN,
    "Gathering join results")

  // Miscellaneous operations
  val JSON_TO_STRUCTS: NvtxId = NvtxId("GpuJsonToStructs", NvtxColor.YELLOW,
    "Converting JSON to structs")

  val PARTITION_FOR_JOIN: NvtxId = NvtxId("partition for join", NvtxColor.CYAN,
    "Hash partitioning data for join operation")

  val CALCULATE_PART: NvtxId = NvtxId("Calculate part", NvtxColor.CYAN,
    "Calculating hash partition assignments")

  val SUB_JOIN_PART: NvtxId = NvtxId("Sub-join part", NvtxColor.CYAN,
    "Hash partitioning for sub-join operation")

  val WINDOW_EXEC: NvtxId = NvtxId("windowExec", NvtxColor.CYAN,
    "Executing window operation on batch")

  val EXPAND_EXEC_PROJECTIONS: NvtxId = NvtxId("ExpandExec projections", NvtxColor.GREEN,
    "Projecting expand operation on batch")

  // Limit and sample operations
  val LIMIT_AND_OFFSET: NvtxId = NvtxId("limit and offset", NvtxColor.ORANGE,
    "Applying limit and offset to data")

  val READ_N_CONCAT: NvtxId = NvtxId("readNConcat", NvtxColor.CYAN,
    "Reading and concatenating N batches")

  val TOP_N: NvtxId = NvtxId("TOP N", NvtxColor.ORANGE,
    "Computing top N rows")

  val TOP_N_OFFSET: NvtxId = NvtxId("TOP N Offset", NvtxColor.ORANGE,
    "Computing top N rows with offset")

  val SAMPLE_EXEC: NvtxId = NvtxId("Sample Exec", NvtxColor.YELLOW,
    "Sampling rows from data")

  val FAST_SAMPLE_EXEC: NvtxId = NvtxId("Fast Sample Exec", NvtxColor.YELLOW,
    "Fast sampling rows from data")

  // Data conversion operations
  val COLUMNAR_TO_ROW_BATCH: NvtxId = NvtxId("ColumnarToRow: batch", NvtxColor.RED,
    "Converting columnar batch to row format")

  val COLUMNAR_TO_ROW_FETCH: NvtxId = NvtxId("ColumnarToRow: fetch", NvtxColor.BLUE,
    "Fetching data during columnar to row conversion")

  // Broadcast operations
  val BROADCAST_COLLECT: NvtxId = NvtxId("broadcast collect", NvtxColor.GREEN,
    "Collecting data for broadcast")

  val BROADCAST_BUILD: NvtxId = NvtxId("broadcast build", NvtxColor.DARK_GREEN,
    "Building broadcast data structure")

  val BROADCAST: NvtxId = NvtxId("broadcast", NvtxColor.CYAN,
    "Broadcasting data to executors")

  // Generate operations
  val GPU_GENERATE_EXEC: NvtxId = NvtxId("GpuGenerateExec", NvtxColor.PURPLE,
    "Executing generate operation on GPU")

  val GPU_GENERATE_PROJECT_SPLIT: NvtxId = NvtxId("GpuGenerate project split",
    NvtxColor.ORANGE, "Splitting projection in generate operation")

  val GPU_GENERATE_ITERATOR: NvtxId = NvtxId("GpuGenerateIterator", NvtxColor.PURPLE,
    "Iterating through generated data")

  // Partition operations
  val PARTITION_D2H: NvtxId = NvtxId("PartitionD2H", NvtxColor.CYAN,
    "Copying partition data from device to host")

  // Spill operations
  val DISK_SPILL: NvtxId = NvtxId("disk spill", NvtxColor.RED,
    "Spilling data from host memory to disk")

  val DEVICE_SPILL: NvtxId = NvtxId("device spill", NvtxColor.ORANGE,
    "Spilling data from device memory to host")

  // Range operation
  val GPU_RANGE: NvtxId = NvtxId("GpuRange", NvtxColor.DARK_GREEN,
    "Generating range of values on GPU")

  // Hybrid CPU/GPU operations
  val WAIT_FOR_CPU: NvtxId = NvtxId("waitForCPU", NvtxColor.RED,
    "Waiting for CPU batch in hybrid execution")

  val GPU_ACQUIRE_C2C: NvtxId = NvtxId("gpuAcquireC2C", NvtxColor.GREEN,
    "Acquiring GPU for coalesce-to-coalesce operation")

  val PINNED_H2D: NvtxId = NvtxId("pinnedH2D", NvtxColor.DARK_GREEN,
    "Copying from pinned host memory to device")

  val PAGEABLE_H2D: NvtxId = NvtxId("PageableH2D", NvtxColor.GREEN,
    "Copying from pageable host memory to device")

  def init(): Unit = {
    register(ACQUIRE_GPU)
    register(RELEASE_GPU)
    register(THREADED_WRITER_WRITE)
    register(THREADED_READER_READ)
    register(WAITING_FOR_WRITES)
    register(COMMIT_SHUFFLE)
    register(PARALLEL_DESERIALIZER_ITERATOR_NEXT)
    register(BATCH_WAIT)
    register(QUEUE_FETCHED)
    register(RAPIDS_CACHING_WRITER_WRITE)
    register(GET_MAP_SIZES_BY_EXEC_ID)
    register(GPU_COALESCE_BATCHES_COLLECT)
    register(BUILD_BATCH_COLLECT)
    register(GPU_COALESCE_ITERATOR)
    register(SHUFFLED_JOIN_STREAM)
    register(HASH_JOIN_BUILD)
    register(PROBE_LEFT)
    register(PROBE_RIGHT)
    register(FETCH_JOIN_STREAM)
    register(BROADCAST_JOIN_STREAM)
    register(GPU_KUDO_SERIALIZE)
    register(GPU_KUDO_COPY_TO_HOST)
    register(GPU_KUDO_SLICE_BUFFERS)
    register(AGG_PRE_PROCESS)
    register(AGG_REDUCE)
    register(AGG_GROUPBY)
    register(AGG_POST_PROCESS)
    register(HASH_PARTITION)
    register(HASH_PARTITION_SLICE)
    register(SORT_COPY_BOUNDARIES)
    register(SORT_TO_UNSAFE_ROW)
    register(SORT_LOWER_BOUNDARIES)
    register(JOIN_FIRST_STREAM_BATCH)
    register(JOIN_ASYMMETRIC_PROBE_FETCH)
    register(JOIN_ASYMMETRIC_FETCH)
    register(PARQUET_READ_FOOTER_BYTES)
    register(PARQUET_PARSE_FILTER_FOOTER)
    register(PARQUET_READ_FOOTER)
    register(PARQUET_FILTER_BLOCKS)
    register(PARQUET_READ_FILTERED_FOOTER)
    register(PARQUET_GET_BLOCKS_WITH_FILTER)
    register(PARQUET_CLIP_SCHEMA)
    register(PARQUET_BUFFER_FILE_SPLIT)
    register(ORC_BUFFER_FILE_SPLIT)
    register(AVRO_BUFFER_FILE_SPLIT)
    register(JSON_CONVERT_TABLE)
    register(JSON_CONVERT_DATETIME)
    register(SHUFFLE_FETCH_FIRST_BATCH)
    register(SHUFFLE_CONCAT_LOAD_BATCH)
    register(CARTESIAN_PRODUCT_SERIALIZE)
    register(CARTESIAN_PRODUCT_DESERIALIZE)
    register(ROW_TO_COLUMNAR)
    register(GENERATE_GET_ROW_BYTE_COUNT)
    register(GENERATE_ESTIMATE_REPETITION)
    register(REDUCTION_MERGE_M2)
    register(PROJECT_TIER)
    register(RANDOM_EXPR)
    register(RAPIDS_CACHING_READER_READ)
    register(RAPIDS_CACHING_READER_READ_LOCAL)
    register(RAPIDS_SHUFFLE_ITERATOR_PREP)
    register(RAPIDS_SHUFFLE_ITERATOR_NEXT)
    register(RAPIDS_SHUFFLE_ITERATOR_GOT_BATCH)
    register(RAPIDS_CACHING_WRITER_CLOSE)
    register(COLUMNAR_BATCH_SERIALIZE)
    register(COLUMNAR_BATCH_SERIALIZE_ROW_ONLY)
    register(TRANSPORT_COPY_BUFFER)
    register(BRING_BACK_TO_HOST)
    register(ROUND_ROBIN_PARTITION)
    register(ROUND_ROBIN_PARTITION_SLICE)
    register(FILTER_BATCH)
    register(PROJECT_EXEC)
    register(PROJECT_AST)
    register(COMPILE_ASTS)
    register(COMPUTE_AGGREGATE)
    register(FINALIZE_AGG)
    register(POST_PROCESS_AGG)
    register(CONCATENATE_BATCHES)
    register(AGG_REPARTITION)
    register(REPARTITION_AGG_ITERATOR_NEXT)
    register(DYNAMIC_SORT_HEURISTIC)
    register(SORT)
    register(MERGE_SORT)
    register(SORT_OP)
    register(SORT_ORDER)
    register(GATHER_SORT)
    register(SPLIT_INPUT_BATCH)
    register(SORT_NEXT_OUTPUT_BATCH)
    register(HASH_JOIN_GATHER_MAP)
    register(FULL_HASH_JOIN_GATHER_MAP)
    register(UPDATE_TRACKING_MASK)
    register(GET_FINAL_BATCH)
    register(EXISTENCE_JOIN_SCATTER_MAP)
    register(EXISTENCE_JOIN_BATCH)
    register(BUILD_JOIN_TABLE)
    register(WINDOW)
    register(RUNNING_WINDOW)
    register(DOUBLE_BATCHED_WINDOW_PRE)
    register(DOUBLE_BATCHED_WINDOW_POST)
    register(PARQUET_DECODE)
    register(ORC_DECODE)
    register(AVRO_DECODE)
    register(BUFFER_FILE_SPLIT)
    register(FILE_FORMAT_READ_BATCH)
    register(FILE_FORMAT_WRITE)
    register(TASK_RANGE)
    register(SHUFFLE_TRANSFER_REQUEST)
    register(ASYNC_SHUFFLE_READ)
    register(ASYNC_SHUFFLE_BUFFER)
    register(SHUFFLE_CONCAT_CPU)
    register(SLICE_INTERNAL_GPU)
    register(SLICE_INTERNAL_CPU)
    register(BATCH_RECEIVED)
    register(READ_HEADER)
    register(READ_BATCH)
    register(SERIALIZE_BATCH)
    register(SERIALIZE_ROW_ONLY_BATCH)
    register(BROADCAST_MANIFEST_BATCH)
    register(DESERIALIZE_BATCH)
    register(SERIALIZE_BROADCAST_BATCH)
    register(HOST_DESERIALIZE_BATCH)
    register(GET_BROADCAST_BATCH)
    register(FIRST_STREAM_BATCH)
    register(BATCH_COMPRESS)
    register(COPY_COMPRESSED_BUFFERS)
    register(BATCH_DECOMPRESS)
    register(ZSTD_POST_PROCESS)
    register(LZ4_POST_PROCESS)
    register(ALLOC_OUTPUT_BUFS)
    register(CLIENT_FETCH)
    register(CLIENT_HANDLE_META)
    register(BUFFER_CALLBACK)
    register(HANDLE_META_REQUEST)
    register(DO_HANDLE_META)
    register(CONSUME_WINDOW)
    register(CALC_GATHER_SIZE)
    register(GET_JOIN_BATCH)
    register(SPILL_JOIN_BATCH)
    register(GET_JOIN_MAP)
    register(SPILL_JOIN_MAP)
    register(BUFFER_FILE_SPLIT_TEXT)
    register(ORC_READ_BATCHES)
    register(PARQUET_READ_BATCH)
    register(AVRO_READ_BATCH)
    register(READ_PYTHON_BATCH)
    register(WRITE_PYTHON_BATCH)
    register(INTERLEAVE_BITS)
    register(HILBERT_INDEX)
    register(GPU_PARTITIONER)
    register(CONCAT_PENDING)
    register(GPU_COALESCE_BATCHES_CONCAT)
    register(SINGLE_BUILD_BATCH_CONCAT)
    register(HOST_COLUMNAR_TO_GPU_CONCAT)
    register(HIVE_DECODE)
    register(JSON_DECODE_SCAN)
    register(CSV_DECODE)
    register(JOIN_GATHER)
    register(JSON_TO_STRUCTS)
    register(PARTITION_FOR_JOIN)
    register(CALCULATE_PART)
    register(SUB_JOIN_PART)
    register(WINDOW_EXEC)
    register(EXPAND_EXEC_PROJECTIONS)
    register(LIMIT_AND_OFFSET)
    register(READ_N_CONCAT)
    register(TOP_N)
    register(TOP_N_OFFSET)
    register(SAMPLE_EXEC)
    register(FAST_SAMPLE_EXEC)
    register(COLUMNAR_TO_ROW_BATCH)
    register(COLUMNAR_TO_ROW_FETCH)
    register(BROADCAST_COLLECT)
    register(BROADCAST_BUILD)
    register(BROADCAST)
    register(GPU_GENERATE_EXEC)
    register(GPU_GENERATE_PROJECT_SPLIT)
    register(GPU_GENERATE_ITERATOR)
    register(PARTITION_D2H)
    register(DISK_SPILL)
    register(DEVICE_SPILL)
    register(GPU_RANGE)
    register(WAIT_FOR_CPU)
    register(GPU_ACQUIRE_C2C)
    register(PINNED_H2D)
    register(PAGEABLE_H2D)
  }
}

object NvtxRangeDocs {
  def helpCommon(): Unit = {
    println("---")
    println("layout: page")
    println("title: NVTX Ranges")
    println("nav_order: 5")
    println("parent: Developer Overview")
    println("---")
    MarkdownUtils.printApacheSparkVersion("NvtxRangeDocs.help")
    // scalastyle:off line.size.limit
    println("""# RAPIDS Accelerator for Apache Spark Nvtx Range Glossary
              |The following is the list of Nvtx ranges that are used throughout
              |the plugin. To add your own Nvtx range to the code, create an NvtxId
              |entry in NvtxRangeWithDoc.scala and create an `NvtxRangeWithDoc` in the
              |code location that you want to cover, passing in the newly created NvtxId.
              |
              |See [nvtx_profiling.md](https://nvidia.github.io/cudf-spark/docs/dev/nvtx_profiling.html) for more info.
              |
              |""".stripMargin)
    // scalastyle:on line.size.limit
    println("\n## Nvtx Ranges\n")
    println("Name | Description")
    println("-----|-------------")
  }

  def main(args: Array[String]): Unit = {
    NvtxRegistry.init()
    val configs = new FileOutputStream(new File(args(0)))
    Console.withOut(configs) {
      Console.withErr(configs) {
        helpCommon()
        NvtxRegistry.registeredRanges.values.foreach(_.help())
      }
    }
  }
}
