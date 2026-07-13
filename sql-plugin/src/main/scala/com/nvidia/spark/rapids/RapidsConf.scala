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

import java.io.{File, FileOutputStream}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

import ai.rapids.cudf.{Cuda, ParquetWriterOptions}
import com.nvidia.spark.rapids.jni.RmmSpark.OomInjectionType
import com.nvidia.spark.rapids.jni.kudo.DumpOption
import com.nvidia.spark.rapids.lore.{LoreId, OutputLoreId}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.RapidsPrivateUtil
import org.apache.spark.sql.rapids.execution.{JoinBuildSideSelection, JoinOptions, JoinStrategy}

object ConfHelper {
  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  def toInteger(s: String, key: String): Integer = {
    try {
      s.trim.toInt
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be integer, but was $s")
    }
  }

  def toLong(s: String, key: String): Long = {
    try {
      s.trim.toLong
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be long, but was $s")
    }
  }

  def toDouble(s: String, key: String): Double = {
    try {
      s.trim.toDouble
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be double, but was $s")
    }
  }

  def stringToSeq(str: String): Seq[String] = {
    // Here 'split' returns a mutable array, 'toList' will convert it into a immutable list
    str.split(",").map(_.trim()).filter(_.nonEmpty).toList
  }

  def stringToSeq[T](str: String, converter: String => T): Seq[T] = {
    stringToSeq(str).map(converter)
  }

  def seqToString[T](v: Seq[T], stringConverter: T => String): String = {
    v.map(stringConverter).mkString(",")
  }

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.nonEmpty && str.head == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def makeConfAnchor(key: String, text: String = null): String = {
    val t = if (text != null) text else key
    // The anchor cannot be too long, so for now
    val a = key.replaceFirst("spark.rapids.", "")
    "<a name=\"" + s"$a" + "\"></a>" + t
  }

  def getSqlFunctionsForClass[T](exprClass: Class[T]): Option[Seq[String]] = {
    sqlFunctionsByClass.get(exprClass.getCanonicalName)
  }

  lazy val sqlFunctionsByClass: Map[String, Seq[String]] = {
    val functionsByClass = new HashMap[String, Seq[String]]
    FunctionRegistry.expressions.foreach { case (sqlFn, (expressionInfo, _)) =>
      val className = expressionInfo.getClassName
      val fnSeq = functionsByClass.getOrElse(className, Seq[String]())
      val fnCleaned = if (sqlFn != "|") {
        sqlFn
      } else {
        "\\|"
      }
      functionsByClass.update(className, fnSeq :+ s"`$fnCleaned`")
    }
    functionsByClass.mapValues(_.sorted).toMap
  }
}

abstract class ConfEntry[T](val key: String, val converter: String => T, val doc: String,
    val isInternal: Boolean, val isStartUpOnly: Boolean, val isCommonlyUsed: Boolean) {

  def get(conf: Map[String, String]): T
  def get(conf: SQLConf): T
  def help(asTable: Boolean = false): Unit

  override def toString: String = key
}

class ConfEntryWithDefault[T](key: String, converter: String => T, doc: String,
    isInternal: Boolean, isStartupOnly: Boolean, isCommonlyUsed: Boolean = false,
    val defaultValue: T)
  extends ConfEntry[T](key, converter, doc, isInternal, isStartupOnly, isCommonlyUsed) {

  override def get(conf: Map[String, String]): T = {
    conf.get(key).map(converter).getOrElse(defaultValue)
  }

  override def get(conf: SQLConf): T = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      defaultValue
    } else {
      converter(tmp)
    }
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      val startupOnlyStr = if (isStartupOnly) "Startup" else "Runtime"
      if (asTable) {
        import ConfHelper.makeConfAnchor
        println(s"${makeConfAnchor(key)}|$doc|$defaultValue|$startupOnlyStr")
      } else {
        println(s"$key:")
        println(s"\t$doc")
        println(s"\tdefault $defaultValue")
        println(s"\ttype $startupOnlyStr")
        println()
      }
    }
  }
}

class OptionalConfEntry[T](key: String, val rawConverter: String => T, doc: String,
    isInternal: Boolean, isStartupOnly: Boolean, isCommonlyUsed: Boolean = false)
  extends ConfEntry[Option[T]](key, s => Some(rawConverter(s)), doc, isInternal,
  isStartupOnly, isCommonlyUsed) {

  override def get(conf: Map[String, String]): Option[T] = {
    conf.get(key).map(rawConverter)
  }

  override def get(conf: SQLConf): Option[T] = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      None
    } else {
      Some(rawConverter(tmp))
    }
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      val startupOnlyStr = if (isStartupOnly) "Startup" else "Runtime"
      if (asTable) {
        import ConfHelper.makeConfAnchor
        println(s"${makeConfAnchor(key)}|$doc|None|$startupOnlyStr")
      } else {
        println(s"$key:")
        println(s"\t$doc")
        println("\tNone")
        println(s"\ttype $startupOnlyStr")
        println()
      }
    }
  }
}

class TypedConfBuilder[T](
    val parent: ConfBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

  def this(parent: ConfBuilder, converter: String => T) = {
    this(parent, converter, Option(_).map(_.toString).orNull)
  }

  /** Apply a transformation to the user-provided values of the config entry. */
  def transform(fn: T => T): TypedConfBuilder[T] = {
    new TypedConfBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  /** Checks if the user-provided value for the config matches the validator. */
  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfBuilder[T] = {
    transform { v =>
      if (!validator(v)) {
        throw new IllegalArgumentException(errorMsg)
      }
      v
    }
  }

  /** Check that user-provided values for the config match a pre-defined set. */
  def checkValues(validValues: Set[T]): TypedConfBuilder[T] = {
    transform { v =>
      if (!validValues.contains(v)) {
        throw new IllegalArgumentException(
          s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, but was $v")
      }
      v
    }
  }

  def createWithDefault(value: T): ConfEntryWithDefault[T] = {
    // 'converter' will check the validity of default 'value', if it's not valid,
    // then 'converter' will throw an exception
    val transformedValue = converter(stringConverter(value))
    val ret = new ConfEntryWithDefault[T](parent.key, converter,
      parent.doc, parent.isInternal, parent.isStartupOnly, parent.isCommonlyUsed, transformedValue)
    parent.register(ret)
    ret
  }

  /** Turns the config entry into a sequence of values of the underlying type. */
  def toSequence: TypedConfBuilder[Seq[T]] = {
    new TypedConfBuilder(parent, ConfHelper.stringToSeq(_, converter),
      ConfHelper.seqToString(_, stringConverter))
  }

  def createOptional: OptionalConfEntry[T] = {
    val ret = new OptionalConfEntry[T](parent.key, converter,
      parent.doc, parent.isInternal, parent.isStartupOnly, parent.isCommonlyUsed)
    parent.register(ret)
    ret
  }
}

class ConfBuilder(val key: String, val register: ConfEntry[_] => Unit) {

  import ConfHelper._

  var doc: String = null
  var isInternal: Boolean = false
  var isStartupOnly: Boolean = false
  var isCommonlyUsed: Boolean = false

  def doc(data: String): ConfBuilder = {
    this.doc = data
    this
  }

  def internal(): ConfBuilder = {
    this.isInternal = true
    this
  }

  def startupOnly(): ConfBuilder = {
    this.isStartupOnly = true
    this
  }

  def commonlyUsed(): ConfBuilder = {
    this.isCommonlyUsed = true
    this
  }

  def booleanConf: TypedConfBuilder[Boolean] = {
    new TypedConfBuilder[Boolean](this, toBoolean(_, key))
  }

  def bytesConf(unit: ByteUnit): TypedConfBuilder[Long] = {
    new TypedConfBuilder[Long](this, byteFromString(_, unit))
  }

  def integerConf: TypedConfBuilder[Integer] = {
    new TypedConfBuilder[Integer](this, toInteger(_, key))
  }

  def longConf: TypedConfBuilder[Long] = {
    new TypedConfBuilder[Long](this, toLong(_, key))
  }

  def doubleConf: TypedConfBuilder[Double] = {
    new TypedConfBuilder(this, toDouble(_, key))
  }

  def stringConf: TypedConfBuilder[String] = {
    new TypedConfBuilder[String](this, identity[String])
  }
}

object RapidsReaderType extends Enumeration {
  type RapidsReaderType = Value
  val AUTO, COALESCING, MULTITHREADED, PERFILE = Value
}

object RapidsConf extends Logging {
  val MULTITHREAD_READ_NUM_THREADS_DEFAULT = 20

  private val registeredConfs = new ListBuffer[ConfEntry[_]]()

  private def register(entry: ConfEntry[_]): Unit = {
    registeredConfs += entry
  }

  def conf(key: String): ConfBuilder = {
    new ConfBuilder(key, register)
  }

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
    .doc("Spark-RAPIDS plugin uses the async profiler to generate flame graphs. " +
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

  // ENABLE/DISABLE PROCESSING

  val SQL_ENABLED = conf("spark.rapids.sql.enabled")
    .doc("Enable (true) or disable (false) sql operations on the GPU")
    .commonlyUsed()
    .booleanConf
    .createWithDefault(true)

  val SQL_MODE = conf("spark.rapids.sql.mode")
    .doc("Set the mode for the Rapids Accelerator. The supported modes are explainOnly and " +
         "executeOnGPU. This config can not be changed at runtime, you must restart the " +
         "application for it to take affect. The default mode is executeOnGPU, which means " +
         "the RAPIDS Accelerator plugin convert the Spark operations and execute them on the " +
         "GPU when possible. The explainOnly mode allows running queries on the CPU and the " +
         "RAPIDS Accelerator will evaluate the queries as if it was going to run on the GPU. " +
         "The explanations of what would have run on the GPU and why are output in log " +
         "messages. When using explainOnly mode, the default explain output is ALL, this can " +
         "be changed by setting spark.rapids.sql.explain. See that config for more details.")
    .startupOnly()
    .stringConf
    .transform(_.toLowerCase(java.util.Locale.ROOT))
    .checkValues(Set("explainonly", "executeongpu"))
    .createWithDefault("executeongpu")

  val UDF_COMPILER_ENABLED = conf("spark.rapids.sql.udfCompiler.enabled")
    .doc("When set to true, Scala UDFs will be considered for compilation as Catalyst expressions")
    .commonlyUsed()
    .booleanConf
    .createWithDefault(false)

  val DFUDF_ENABLED = conf("spark.rapids.sql.dfudf.enabled")
    .doc("When set to false, the DataFrame UDF plugin is disabled. True enables it.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val INCOMPATIBLE_OPS = conf("spark.rapids.sql.incompatibleOps.enabled")
    .doc("For operations that work, but are not 100% compatible with the Spark equivalent " +
      "set if they should be enabled by default or disabled by default.")
    .booleanConf
    .createWithDefault(true)

  val INCOMPATIBLE_DATE_FORMATS = conf("spark.rapids.sql.incompatibleDateFormats.enabled")
    .doc("When parsing strings as dates and timestamps in functions like unix_timestamp, some " +
         "formats are fully supported on the GPU and some are unsupported and will fall back to " +
         "the CPU.  Some formats behave differently on the GPU than the CPU.  Spark on the CPU " +
         "interprets date formats with unsupported trailing characters as nulls, while Spark on " +
         "the GPU will parse the date with invalid trailing characters. More detail can be found " +
         "at [parsing strings as dates or timestamps]" +
         "(../compatibility.md#parsing-strings-as-dates-or-timestamps).")
      .booleanConf
      .createWithDefault(false)

  val IMPROVED_FLOAT_OPS = conf("spark.rapids.sql.improvedFloatOps.enabled")
    .doc("For some floating point operations spark uses one way to compute the value " +
      "and the underlying cudf implementation can use an improved algorithm. " +
      "In some cases this can result in cudf producing an answer when spark overflows.")
    .booleanConf
    .createWithDefault(true)

  val NEED_DECIMAL_OVERFLOW_GUARANTEES = conf("spark.rapids.sql.decimalOverflowGuarantees")
      .doc("FOR TESTING ONLY. DO NOT USE IN PRODUCTION. Please see the decimal section of " +
          "the compatibility documents for more information on this config.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_WINDOW_COLLECT_LIST = conf("spark.rapids.sql.window.collectList.enabled")
      .doc("The output size of collect list for a window operation is proportional to " +
          "the window size squared. The current GPU implementation does not handle this well " +
          "and is disabled by default. If you know that your window size is very small you " +
          "can try to enable it.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_WINDOW_COLLECT_SET = conf("spark.rapids.sql.window.collectSet.enabled")
      .doc("The output size of collect set for a window operation can be proportional to " +
          "the window size squared. The current GPU implementation does not handle this well " +
          "and is disabled by default. If you know that your window size is very small you " +
          "can try to enable it.")
      .booleanConf
      .createWithDefault(false)

  val ENABLE_WINDOW_UNBOUNDED_AGG = conf("spark.rapids.sql.window.unboundedAgg.enabled")
      .doc("This is a temporary internal config to turn on an unbounded to unbounded " +
          "window optimization that is still a work in progress. It should eventually replace " +
          "the double pass window exec.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val ENABLE_WINDOW_GROUP_LIMIT_OPT = conf("spark.rapids.sql.window.groupLimit.opt.enabled")
      .doc("When enabled, the plugin will skip redundant Final WindowGroupLimit operators " +
          "when they are followed by a WindowExec and FilterExec that perform the same " +
          "rank computation and filtering. This avoids computing the rank twice and " +
          "improves performance for queries with rank-based window limits.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val ENABLE_FLOAT_AGG = conf("spark.rapids.sql.variableFloatAgg.enabled")
    .doc("Spark assumes that all operations produce the exact same result each time. " +
      "This is not true for some floating point aggregations, which can produce slightly " +
      "different results on the GPU as the aggregation is done in parallel.  This can enable " +
      "those operations if you know the query is only computing it once.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_REPLACE_SORTMERGEJOIN = conf("spark.rapids.sql.replaceSortMergeJoin.enabled")
    .doc("Allow replacing sortMergeJoin with HashJoin")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_HASH_OPTIMIZE_SORT = conf("spark.rapids.sql.hashOptimizeSort.enabled")
    .doc("Whether sorts should be inserted after some hashed operations to improve " +
      "output ordering. This can improve output file sizes when saving to columnar formats.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_FOLD_LOCAL_AGGREGATE = conf("spark.rapids.sql.foldLocalAggregate.enabled")
    .doc("Whether to fold two-stages local aggregate into one-shot Complete aggregate.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CAST_FLOAT_TO_DECIMAL = conf("spark.rapids.sql.castFloatToDecimal.enabled")
    .doc("Casting from floating point types to decimal on the GPU returns results that have " +
      "tiny difference compared to results returned from CPU.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CAST_FLOAT_TO_STRING = conf("spark.rapids.sql.castFloatToString.enabled")
    .doc("Casting from floating point types to string on the GPU returns results that have " +
      "a different precision than the default results of Spark.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_FLOAT_FORMAT_NUMBER = conf("spark.rapids.sql.formatNumberFloat.enabled")
    .doc("format_number with floating point types on the GPU returns results that have " +
      "a different precision than the default results of Spark.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES =
    conf("spark.rapids.sql.castFloatToIntegralTypes.enabled")
      .doc("Casting from floating point types to integral types on the GPU supports a " +
          "slightly different range of values when using Spark 3.1.0 or later. Refer to the CAST " +
          "documentation for more details.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_CAST_DECIMAL_TO_FLOAT = conf("spark.rapids.sql.castDecimalToFloat.enabled")
      .doc("Casting from decimal to floating point types on the GPU returns results that have " +
          "tiny difference compared to results returned from CPU.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_CAST_STRING_TO_FLOAT = conf("spark.rapids.sql.castStringToFloat.enabled")
    .doc("When set to true, enables casting from strings to float types (float, double) " +
      "on the GPU. Currently hex values aren't supported on the GPU. Also note that casting from " +
      "string to float types on the GPU returns incorrect results when the string represents any " +
      "number \"1.7976931348623158E308\" <= x < \"1.7976931348623159E308\" " +
      "and \"-1.7976931348623158E308\" >= x > \"-1.7976931348623159E308\" in both these cases " +
      "the GPU returns Double.MaxValue while CPU returns \"+Infinity\" and \"-Infinity\" " +
      "respectively")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CAST_STRING_TO_TIMESTAMP = conf("spark.rapids.sql.castStringToTimestamp.enabled")
    .doc("When set to false, this disables casting from string to timestamp on the GPU.")
    .booleanConf
    .createWithDefault(true)

  val HAS_EXTENDED_YEAR_VALUES = conf("spark.rapids.sql.hasExtendedYearValues")
      .doc("Spark 3.2.0+ extended parsing of years in dates and " +
          "timestamps to support the full range of possible values. Prior " +
          "to this it was limited to a positive 4 digit year. The Accelerator does not " +
          "support the extended range yet. This config indicates if your data includes " +
          "this extended range or not, or if you don't care about getting the correct " +
          "values on values with the extended range.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_INNER_JOIN = conf("spark.rapids.sql.join.inner.enabled")
      .doc("When set to true inner joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_CROSS_JOIN = conf("spark.rapids.sql.join.cross.enabled")
      .doc("When set to true cross joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_OUTER_JOIN = conf("spark.rapids.sql.join.leftOuter.enabled")
      .doc("When set to true left outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RIGHT_OUTER_JOIN = conf("spark.rapids.sql.join.rightOuter.enabled")
      .doc("When set to true right outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_FULL_OUTER_JOIN = conf("spark.rapids.sql.join.fullOuter.enabled")
      .doc("When set to true full outer joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_SEMI_JOIN = conf("spark.rapids.sql.join.leftSemi.enabled")
      .doc("When set to true left semi joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_LEFT_ANTI_JOIN = conf("spark.rapids.sql.join.leftAnti.enabled")
      .doc("When set to true left anti joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_EXISTENCE_JOIN = conf("spark.rapids.sql.join.existence.enabled")
      .doc("When set to true existence joins are enabled on the GPU")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_PROJECT_AST = conf("spark.rapids.sql.projectAstEnabled")
      .doc("Enable project operations to use cudf AST expressions when possible.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val ENABLE_TIERED_PROJECT = conf("spark.rapids.sql.tiered.project.enabled")
      .doc("Enable tiered projections.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val ENABLE_COMBINED_EXPR_PREFIX = "spark.rapids.sql.expression.combined."

  val ENABLE_COMBINED_EXPRESSIONS = conf("spark.rapids.sql.combined.expressions.enabled")
    .doc("For some expressions it can be much more efficient to combine multiple " +
      "expressions together into a single kernel call. This enables or disables that " +
      s"feature. Note that this requires that $ENABLE_TIERED_PROJECT is turned on or " +
      "else there is no performance improvement. You can also disable this feature for " +
      "expressions that support it. Each config is expression specific and starts with " +
      s"$ENABLE_COMBINED_EXPR_PREFIX followed by the name of the GPU expression class " +
      s"similar to what we do for enabling converting individual expressions to the GPU.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_RLIKE_REGEX_REWRITE = conf("spark.rapids.sql.rLikeRegexRewrite.enabled")
      .doc("Enable the optimization to rewrite rlike regex to contains in some cases.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  // FILE FORMATS
  val MULTITHREAD_READ_NUM_THREADS = conf("spark.rapids.sql.multiThreadedRead.numThreads")
      .doc("The maximum number of threads on each executor to use for reading small " +
        "files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with COALESCING and MULTITHREADED readers, see " +
        "spark.rapids.sql.format.parquet.reader.type, " +
        "spark.rapids.sql.format.orc.reader.type, or " +
        "spark.rapids.sql.format.avro.reader.type for a discussion of reader types. " +
        "If it is not set explicitly and spark.executor.cores is set, it will be tried to " +
        "assign value of `max(MULTITHREAD_READ_NUM_THREADS_DEFAULT, spark.executor.cores)`, " +
        s"where MULTITHREAD_READ_NUM_THREADS_DEFAULT = $MULTITHREAD_READ_NUM_THREADS_DEFAULT" +
        ".")
      .startupOnly()
      .commonlyUsed()
      .integerConf
      .checkValue(v => v > 0, "The thread count must be greater than zero.")
      .createWithDefault(MULTITHREAD_READ_NUM_THREADS_DEFAULT)

  // Memory-bounded multithreaded reading
  val MULTITHREAD_READ_MEMORY_LIMIT_ENABLED = {
    // TODO: This conf should be adjusted during the runtime
    conf("spark.rapids.sql.multiThreadedRead.memoryLimit.enabled")
        .doc("Enable memory-bounded multi-threaded reading. When enabled, the concurrency of " +
            "multi-threaded reading will be dynamically controlled based on the available memory " +
            s"budget per Spark executor.")
        .startupOnly()
        .internal()
        .booleanConf
        .createWithDefault(false)
  }

  val MULTITHREAD_READ_MEMORY_LIMIT_SIZE = {
    // TODO: This conf should be adjusted during the runtime
    conf("spark.rapids.sql.multiThreadedRead.memoryLimit.size")
        .doc("The maximum memory capacity in bytes to use for reading files in parallel. " +
            "This can not be changed at runtime after the executor has started. And if 0, it " +
            "will be set with 90% of executor's off-heap memory. This config only takes effect " +
            s"when ${MULTITHREAD_READ_MEMORY_LIMIT_ENABLED.key} is enabled.")
        .startupOnly()
        .internal()
        .bytesConf(ByteUnit.BYTE)
        .checkValue(v => v >= 0, s"The memory capacity must be greatThanOrEqual zero")
        .createWithDefault(0)
  }

  val MULTITHREAD_READ_MEMORY_LIMIT_ACQUISITION_TIMEOUT  = {
    conf("spark.rapids.sql.multiThreadedRead.memoryLimit.acquisitionTimeout")
        .doc("The maximum time in milliseconds to wait for a task to acquire required resource " +
            "before giving up and put off the run, by re-appending the task back into the queue " +
            "with a priority penality.")
        .startupOnly()
        .internal()
        .longConf
        .checkValue(v => v >= 0, "The timeout must be greater than zero")
        .createWithDefault(30 * 1000L)
  } // 30 seconds

  val MULTITHREAD_READ_MEMORY_LIMIT_TEST_PER_STAGE_POOL = {
    conf("spark.rapids.sql.multiThreadedRead.memoryLimit.tests.perStagePool")
        .doc("Enable test mode for the multi-threaded read. This will create different " +
            "threadpools for each stage, so as to verify threadpools with different configs " +
            "as independent test cases which can be run in parallel.")
        .startupOnly()
        .internal()
        .booleanConf
        .createWithDefault(false)
  }

  val ENABLE_PARQUET = conf("spark.rapids.sql.format.parquet.enabled")
    .doc("When set to false disables all parquet input and output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ACCELERATED_COLUMNAR_TO_ROW_ENABLED =
    conf("spark.rapids.sql.acceleratedColumnarToRow.enabled")
      .doc("When set to true (default) the GPU columnar-to-row transition uses the GPU " +
        "transpose kernel (AcceleratedColumnarToRowIterator) for wide fixed-width / STRING " +
        "schemas. Setting it to false forces the slower per-row CPU iterator " +
        "(ColumnarToRowIterator). Mainly useful for troubleshooting and performance " +
        "comparisons; production workloads should leave this on.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_PARQUET_INT96_WRITE = conf("spark.rapids.sql.format.parquet.writer.int96.enabled")
    .doc("When set to false, disables accelerated parquet write if the " +
      "spark.sql.parquet.outputTimestampType is set to INT96")
    .booleanConf
    .createWithDefault(true)

  val PARQUET_WRITER_ROW_GROUP_SIZE_ROWS =
    conf("spark.rapids.sql.format.parquet.writer.rowGroupSizeRows")
      .doc("Maximum number of rows in a Parquet row group written by the GPU writer. " +
        "This is a best-effort limit because the cuDF writer does not split row groups " +
        "below its page fragment granularity. If not set, the cuDF writer default is used.")
      .internal()
      .integerConf
      .checkValue(v => v > 0, "The Parquet writer row group size rows must be positive.")
      .createOptional

  val PARQUET_WRITER_ROW_GROUP_SIZE_BYTES =
    conf("spark.rapids.sql.format.parquet.writer.rowGroupSizeBytes")
      .doc("Maximum size in bytes of a Parquet row group written by the GPU writer. " +
        "This is a best-effort limit because the cuDF writer does not split row groups " +
        "below its page fragment granularity. If not set, the cuDF writer default is used.")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v >= 1024L,
        "The Parquet writer row group size bytes must be at least 1024.")
      .createOptional

  val PARQUET_WRITER_DICTIONARY_POLICY =
    conf("spark.rapids.sql.format.parquet.writer.dictionaryPolicy")
      .doc("Dictionary policy for the GPU Parquet writer. NEVER disables dictionary " +
        "encoding for every column. ADAPTIVE (cuDF default) uses dictionary encoding " +
        "unless the per-column-chunk dictionary would exceed " +
        "spark.rapids.sql.format.parquet.writer.maxDictionarySize. ALWAYS allows " +
        "dictionary encoding even when it would prevent compression of a column chunk. " +
        "If not set, the cuDF writer default (ADAPTIVE) is used.")
      .internal()
      .stringConf
      .transform(_.toUpperCase(java.util.Locale.ROOT))
      .checkValues(ParquetWriterOptions.DictionaryPolicy.values.map(_.toString).toSet)
      .createOptional

  val PARQUET_WRITER_MAX_DICTIONARY_SIZE =
    conf("spark.rapids.sql.format.parquet.writer.maxDictionarySize")
      .doc("Maximum size in bytes of a per-column-chunk dictionary in a Parquet row " +
        "group written by the GPU writer. Only consulted when the dictionary policy is " +
        "ADAPTIVE (see spark.rapids.sql.format.parquet.writer.dictionaryPolicy); column " +
        "chunks whose dictionary payload would exceed this limit fall back to PLAIN " +
        "encoding. Raising this can reduce GPU-written file size for high-cardinality " +
        "columns at the cost of larger dictionary pages. If not set, the cuDF writer " +
        "default (1 MiB) is used.")
      .internal()
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v >= 0L && v <= Integer.MAX_VALUE.toLong,
        s"The Parquet writer max dictionary size must be in [0, ${Integer.MAX_VALUE}] " +
          "(cuDF requires the dictionary size to fit in an int32).")
      .createOptional

  object ParquetFooterReaderType extends Enumeration {
    val JAVA, NATIVE, AUTO = Value
  }

  val PARQUET_READER_FOOTER_TYPE =
    conf("spark.rapids.sql.format.parquet.reader.footer.type")
      .doc("In some cases reading the footer of the file is very expensive. Typically this " +
          "happens when there are a large number of columns and relatively few " +
          "of them are being read on a large number of files. " +
          "This provides the ability to use a different path to parse and filter the footer. " +
          "AUTO is the default and decides which path to take using a heuristic. JAVA " +
          "follows closely with what Apache Spark does. NATIVE will parse and " +
          "filter the footer using C++.")
      .stringConf
      .transform(_.toUpperCase(java.util.Locale.ROOT))
      .checkValues(ParquetFooterReaderType.values.map(_.toString))
      .createWithDefault(ParquetFooterReaderType.AUTO.toString)

  // Deprecated legacy row-based UDF path. CPU bridge is the preferred replacement when it can
  // bridge the expression safely.
  val ENABLE_CPU_BASED_UDF = conf("spark.rapids.sql.rowBasedUDF.enabled")
    .doc("Deprecated. When set to true, enables the legacy row-based UDF path in a GPU " +
      "operation by transferring only the data it needs between GPU and CPU inside a query " +
      "operation, instead of falling this operation back to CPU. CPU bridge is the preferred " +
      "replacement when it can safely bridge the expression, and this config might be removed " +
      "in the future.")
    .booleanConf
    .createWithDefault(false)

  val PARQUET_READER_TYPE = conf("spark.rapids.sql.format.parquet.reader.type")
    .doc("Sets the Parquet reader type. We support different types that are optimized for " +
      "different environments. The original Spark style reader can be selected by setting this " +
      "to PERFILE which individually reads and copies files to the GPU. Loading many small files " +
      "individually has high overhead, and using either COALESCING or MULTITHREADED is " +
      "recommended instead. The COALESCING reader is good when using a local file system where " +
      "the executors are on the same nodes or close to the nodes the data is being read on. " +
      "This reader coalesces all the files assigned to a task into a single host buffer before " +
      "sending it down to the GPU. It copies blocks from a single file into a host buffer in " +
      s"separate threads in parallel, see $MULTITHREAD_READ_NUM_THREADS. " +
      "MULTITHREADED is good for cloud environments where you are reading from a blobstore " +
      "that is totally separate and likely has a higher I/O read cost. Many times the cloud " +
      "environments also get better throughput when you have multiple readers in parallel. " +
      "This reader uses multiple threads to read each file in parallel and each file is sent " +
      "to the GPU separately. This allows the CPU to keep reading while GPU is also doing work. " +
      s"See $MULTITHREAD_READ_NUM_THREADS and " +
      "spark.rapids.sql.format.parquet.multiThreadedRead.maxNumFilesParallel to control " +
      "the number of threads and amount of memory used. " +
      "By default this is set to AUTO so we select the reader we think is best. This will " +
      "either be the COALESCING or the MULTITHREADED based on whether we think the file is " +
      "in the cloud. See spark.rapids.cloudSchemes.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(RapidsReaderType.values.map(_.toString))
    .createWithDefault(RapidsReaderType.AUTO.toString)

  val PARQUET_DECOMPRESS_CPU =
    conf("spark.rapids.sql.format.parquet.decompressCpu")
      .doc("If true then the CPU is eligible to decompress Parquet data rather than the GPU. " +
          s"See other spark.rapids.sql.format.parquet.decompressCpu.* configuration settings " +
          "to control this for specific compression codecs.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val PARQUET_DECOMPRESS_CPU_SNAPPY =
    conf("spark.rapids.sql.format.parquet.decompressCpu.snappy")
      .doc(s"If true and $PARQUET_DECOMPRESS_CPU is true then the CPU decompresses " +
          "Parquet Snappy data rather than the GPU")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val PARQUET_DECOMPRESS_CPU_ZSTD =
    conf("spark.rapids.sql.format.parquet.decompressCpu.zstd")
      .doc(s"If true and $PARQUET_DECOMPRESS_CPU is true then the CPU decompresses " +
          "Parquet Zstandard data rather than the GPU")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val READER_MULTITHREADED_COMBINE_THRESHOLD =
    conf("spark.rapids.sql.reader.multithreaded.combine.sizeBytes")
      .doc("The target size in bytes to combine multiple small files together when using the " +
        "MULTITHREADED parquet or orc reader. With combine disabled, the MULTITHREADED reader " +
        "reads the files in parallel and sends individual files down to the GPU, but that can " +
        "be inefficient for small files. When combine is enabled, files that are ready within " +
        "spark.rapids.sql.reader.multithreaded.combine.waitTime together, up to this " +
        "threshold size, are combined before sending down to GPU. This can be disabled by " +
        "setting it to 0. Note that combine also will not go over the " +
        "spark.rapids.sql.reader.batchSizeRows or spark.rapids.sql.reader.batchSizeBytes limits.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(64 * 1024 * 1024) // 64M

  val READER_MULTITHREADED_COMBINE_WAIT_TIME =
    conf("spark.rapids.sql.reader.multithreaded.combine.waitTime")
      .doc("When using the multithreaded parquet or orc reader with combine mode, how long " +
        "to wait, in milliseconds, for more files to finish if haven't met the size threshold. " +
        "Note that this will wait this amount of time from when the last file was available, " +
        "so total wait time could be larger then this.")
      .integerConf
      .createWithDefault(200) // ms

  val READER_MULTITHREADED_READ_KEEP_ORDER =
    conf("spark.rapids.sql.reader.multithreaded.read.keepOrder")
      .doc("When using the MULTITHREADED reader, if this is set to true we read " +
        "the files in the same order Spark does, otherwise the order may not be the same. " +
        "Now it is supported only for parquet and orc.")
      .booleanConf
      .createWithDefault(true)

  val PARQUET_MULTITHREADED_COMBINE_THRESHOLD =
    conf("spark.rapids.sql.format.parquet.multithreaded.combine.sizeBytes")
      .doc("The target size in bytes to combine multiple small files together when using the " +
        "MULTITHREADED parquet reader. With combine disabled, the MULTITHREADED reader reads the " +
        "files in parallel and sends individual files down to the GPU, but that can be " +
        "inefficient for small files. When combine is enabled, files that are ready within " +
        "spark.rapids.sql.format.parquet.multithreaded.combine.waitTime together, up to this " +
        "threshold size, are combined before sending down to GPU. This can be disabled by " +
        "setting it to 0. Note that combine also will not go over the " +
        "spark.rapids.sql.reader.batchSizeRows or spark.rapids.sql.reader.batchSizeBytes " +
        s"limits. DEPRECATED: use $READER_MULTITHREADED_COMBINE_THRESHOLD instead.")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val PARQUET_MULTITHREADED_COMBINE_WAIT_TIME =
    conf("spark.rapids.sql.format.parquet.multithreaded.combine.waitTime")
      .doc("When using the multithreaded parquet reader with combine mode, how long " +
        "to wait, in milliseconds, for more files to finish if haven't met the size threshold. " +
        "Note that this will wait this amount of time from when the last file was available, " +
        "so total wait time could be larger then this. " +
        s"DEPRECATED: use $READER_MULTITHREADED_COMBINE_WAIT_TIME instead.")
      .integerConf
      .createOptional

  val PARQUET_MULTITHREADED_READ_KEEP_ORDER =
    conf("spark.rapids.sql.format.parquet.multithreaded.read.keepOrder")
      .doc("When using the MULTITHREADED reader, if this is set to true we read " +
        "the files in the same order Spark does, otherwise the order may not be the same. " +
        s"DEPRECATED: use $READER_MULTITHREADED_READ_KEEP_ORDER instead.")
      .booleanConf
      .createOptional

  /** List of schemes that are always considered cloud storage schemes */
  private lazy val DEFAULT_CLOUD_SCHEMES =
    Seq("abfs", "abfss", "dbfs", "gs", "s3", "s3a", "s3n", "wasbs", "cosn")

  val CLOUD_SCHEMES = conf("spark.rapids.cloudSchemes")
    .doc("Comma separated list of additional URI schemes that are to be considered cloud based " +
      s"filesystems. Schemes already included: ${DEFAULT_CLOUD_SCHEMES.mkString(", ")}. Cloud " +
      "based stores generally would be total separate from the executors and likely have a " +
      "higher I/O read cost. Many times the cloud filesystems also get better throughput when " +
      "you have multiple readers in parallel. This is used with " +
      "spark.rapids.sql.format.parquet.reader.type")
    .commonlyUsed()
    .stringConf
    .toSequence
    .createOptional

  val PARQUET_MULTITHREAD_READ_NUM_THREADS =
    conf("spark.rapids.sql.format.parquet.multiThreadedRead.numThreads")
      .doc("The maximum number of threads, on the executor, to use for reading small " +
        "Parquet files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with COALESCING and MULTITHREADED reader, see " +
        s"$PARQUET_READER_TYPE. DEPRECATED: use $MULTITHREAD_READ_NUM_THREADS")
      .startupOnly()
      .integerConf
      .createOptional

  val PARQUET_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL =
    conf("spark.rapids.sql.format.parquet.multiThreadedRead.maxNumFilesParallel")
      .doc("A limit on the maximum number of files per task processed in parallel on the CPU " +
        "side before the file is sent to the GPU. This affects the amount of host memory used " +
        "when reading the files in parallel. Used with MULTITHREADED reader, see " +
        s"$PARQUET_READER_TYPE.")
      .integerConf
      .checkValue(v => v > 0, "The maximum number of files must be greater than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_PARQUET_READ = conf("spark.rapids.sql.format.parquet.read.enabled")
    .doc("When set to false disables parquet input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_PARQUET_WRITE = conf("spark.rapids.sql.format.parquet.write.enabled")
    .doc("When set to false disables parquet output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC = conf("spark.rapids.sql.format.orc.enabled")
    .doc("When set to false disables all orc input and output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_READ = conf("spark.rapids.sql.format.orc.read.enabled")
    .doc("When set to false disables orc input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_WRITE = conf("spark.rapids.sql.format.orc.write.enabled")
    .doc("When set to false disables orc output acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_BOOL = conf("spark.rapids.sql.format.orc.write.boolType.enabled")
    .doc("When set to false disables boolean columns for ORC writes. " +
      "Set to true if you want to experiment. " +
      "See https://github.com/NVIDIA/spark-rapids/issues/11736.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val TEST_ORC_STRIPE_SIZE_ROWS = conf("spark.rapids.sql.test.orc.write.stripeSizeRows")
    .doc("Only to be used in tests. When set to a valid value(no less than 512, as specified " +
      "in cudf), the ORC writer will use this value as the maximum number of rows per stripe. " +
      "Additionally, the ORC writer rounds the number of elements in each stripe " +
      "(except the last) to a multiple of 8 to efficiently encode the validity masks. " +
      "If not set, the ORC writer will use the default value 1,000,000.")
    .internal()
    .integerConf
    .checkValue(v => v >= 512, "The stripe size rows must be no less than 512.")
    .createOptional

  val ORC_READ_IGNORE_WRITE_TIMEZONE =
    conf("spark.rapids.sql.orc.read.ignore.write.timezone")
      .doc("This is an experimental feature. When set to true, the ORC reader ignores the writer " +
        "timezones in the stripe footers and use the reader timezone as writer timezone. " +
        "When set to true, the user should guarantee the the writer timezones in the ORC " +
        "file is the same as the reader.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val ENABLE_EXPAND_PREPROJECT = conf("spark.rapids.sql.expandPreproject.enabled")
    .doc("When set to false disables the pre-projection for GPU Expand. " +
      "Pre-projection leverages the tiered projection to evaluate expressions that " +
      "semantically equal across Expand projection lists before expanding, to avoid " +
      s"duplicate evaluations. '${ENABLE_TIERED_PROJECT.key}' should also set to true " +
      "to enable this.")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_COALESCE_AFTER_EXPAND = conf("spark.rapids.sql.coalesceAfterExpand.enabled")
    .doc("When set to false disables the coalesce after GPU Expand. ")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ORC_FLOAT_TYPES_TO_STRING =
    conf("spark.rapids.sql.format.orc.floatTypesToString.enable")
    .doc("When reading an ORC file, the source data schemas(schemas of ORC file) may differ " +
      "from the target schemas (schemas of the reader), we need to handle the castings from " +
      "source type to target type. Since float/double numbers in GPU have different precision " +
      "with CPU, when casting float/double to string, the result of GPU is different from " +
      "result of CPU spark. Its default value is `true` (this means the strings result will " +
      "differ from result of CPU). If it's set `false` explicitly and there exists casting " +
      "from float/double to string in the job, then such behavior will cause an exception, " +
      "and the job will fail.")
    .booleanConf
    .createWithDefault(true)

  val ORC_READER_TYPE = conf("spark.rapids.sql.format.orc.reader.type")
    .doc("Sets the ORC reader type. We support different types that are optimized for " +
      "different environments. The original Spark style reader can be selected by setting this " +
      "to PERFILE which individually reads and copies files to the GPU. Loading many small files " +
      "individually has high overhead, and using either COALESCING or MULTITHREADED is " +
      "recommended instead. The COALESCING reader is good when using a local file system where " +
      "the executors are on the same nodes or close to the nodes the data is being read on. " +
      "This reader coalesces all the files assigned to a task into a single host buffer before " +
      "sending it down to the GPU. It copies blocks from a single file into a host buffer in " +
      s"separate threads in parallel, see $MULTITHREAD_READ_NUM_THREADS. " +
      "MULTITHREADED is good for cloud environments where you are reading from a blobstore " +
      "that is totally separate and likely has a higher I/O read cost. Many times the cloud " +
      "environments also get better throughput when you have multiple readers in parallel. " +
      "This reader uses multiple threads to read each file in parallel and each file is sent " +
      "to the GPU separately. This allows the CPU to keep reading while GPU is also doing work. " +
      s"See $MULTITHREAD_READ_NUM_THREADS and " +
      "spark.rapids.sql.format.orc.multiThreadedRead.maxNumFilesParallel to control " +
      "the number of threads and amount of memory used. " +
      "By default this is set to AUTO so we select the reader we think is best. This will " +
      "either be the COALESCING or the MULTITHREADED based on whether we think the file is " +
      "in the cloud. See spark.rapids.cloudSchemes.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(RapidsReaderType.values.map(_.toString))
    .createWithDefault(RapidsReaderType.AUTO.toString)

  val ORC_MULTITHREAD_READ_NUM_THREADS =
    conf("spark.rapids.sql.format.orc.multiThreadedRead.numThreads")
      .doc("The maximum number of threads, on the executor, to use for reading small " +
        "ORC files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with MULTITHREADED reader, see " +
        s"$ORC_READER_TYPE. DEPRECATED: use $MULTITHREAD_READ_NUM_THREADS")
      .startupOnly()
      .integerConf
      .createOptional

  val ORC_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL =
    conf("spark.rapids.sql.format.orc.multiThreadedRead.maxNumFilesParallel")
      .doc("A limit on the maximum number of files per task processed in parallel on the CPU " +
        "side before the file is sent to the GPU. This affects the amount of host memory used " +
        "when reading the files in parallel. Used with MULTITHREADED reader, see " +
        s"$ORC_READER_TYPE.")
      .integerConf
      .checkValue(v => v > 0, "The maximum number of files must be greater than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_CSV = conf("spark.rapids.sql.format.csv.enabled")
    .doc("When set to false disables all csv input and output acceleration. " +
      "(only input is currently supported anyways)")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_CSV_READ = conf("spark.rapids.sql.format.csv.read.enabled")
    .doc("When set to false disables csv input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_CSV_FLOATS = conf("spark.rapids.sql.csv.read.float.enabled")
    .doc("CSV reading is not 100% compatible when reading floats.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_CSV_DOUBLES = conf("spark.rapids.sql.csv.read.double.enabled")
    .doc("CSV reading is not 100% compatible when reading doubles.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_CSV_DECIMALS = conf("spark.rapids.sql.csv.read.decimal.enabled")
    .doc("CSV reading is not 100% compatible when reading decimals.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_JSON = conf("spark.rapids.sql.format.json.enabled")
    .doc("When set to true enables all json input and output acceleration. " +
      "(only input is currently supported anyways)")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_JSON_READ = conf("spark.rapids.sql.format.json.read.enabled")
    .doc("When set to true enables json input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_JSON_FLOATS = conf("spark.rapids.sql.json.read.float.enabled")
    .doc("JSON reading is not 100% compatible when reading floats.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_JSON_DOUBLES = conf("spark.rapids.sql.json.read.double.enabled")
    .doc("JSON reading is not 100% compatible when reading doubles.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_JSON_DECIMALS = conf("spark.rapids.sql.json.read.decimal.enabled")
    .doc("When reading a quoted string as a decimal Spark supports reading non-ascii " +
        "unicode digits, and the RAPIDS Accelerator does not.")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_READ_JSON_DATE_TIME = conf("spark.rapids.sql.json.read.datetime.enabled")
    .doc("JSON reading is not 100% compatible when reading dates and timestamps.")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_AVRO = conf("spark.rapids.sql.format.avro.enabled")
    .doc("When set to true enables all avro input and output acceleration. " +
      "(only input is currently supported anyways)")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_AVRO_READ = conf("spark.rapids.sql.format.avro.read.enabled")
    .doc("When set to true enables avro input acceleration")
    .booleanConf
    .createWithDefault(false)

  val AVRO_READER_TYPE = conf("spark.rapids.sql.format.avro.reader.type")
    .doc("Sets the Avro reader type. We support different types that are optimized for " +
      "different environments. The original Spark style reader can be selected by setting this " +
      "to PERFILE which individually reads and copies files to the GPU. Loading many small files " +
      "individually has high overhead, and using either COALESCING or MULTITHREADED is " +
      "recommended instead. The COALESCING reader is good when using a local file system where " +
      "the executors are on the same nodes or close to the nodes the data is being read on. " +
      "This reader coalesces all the files assigned to a task into a single host buffer before " +
      "sending it down to the GPU. It copies blocks from a single file into a host buffer in " +
      s"separate threads in parallel, see $MULTITHREAD_READ_NUM_THREADS. " +
      "MULTITHREADED is good for cloud environments where you are reading from a blobstore " +
      "that is totally separate and likely has a higher I/O read cost. Many times the cloud " +
      "environments also get better throughput when you have multiple readers in parallel. " +
      "This reader uses multiple threads to read each file in parallel and each file is sent " +
      "to the GPU separately. This allows the CPU to keep reading while GPU is also doing work. " +
      s"See $MULTITHREAD_READ_NUM_THREADS and " +
      "spark.rapids.sql.format.avro.multiThreadedRead.maxNumFilesParallel to control " +
      "the number of threads and amount of memory used. " +
      "By default this is set to AUTO so we select the reader we think is best. This will " +
      "either be the COALESCING or the MULTITHREADED based on whether we think the file is " +
      "in the cloud. See spark.rapids.cloudSchemes.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(RapidsReaderType.values.map(_.toString))
    .createWithDefault(RapidsReaderType.AUTO.toString)

  val AVRO_MULTITHREAD_READ_NUM_THREADS =
    conf("spark.rapids.sql.format.avro.multiThreadedRead.numThreads")
      .doc("The maximum number of threads, on one executor, to use for reading small " +
        "Avro files in parallel. This can not be changed at runtime after the executor has " +
        "started. Used with MULTITHREADED reader, see " +
        s"$AVRO_READER_TYPE. DEPRECATED: use $MULTITHREAD_READ_NUM_THREADS")
      .startupOnly()
      .integerConf
      .createOptional

  val AVRO_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL =
    conf("spark.rapids.sql.format.avro.multiThreadedRead.maxNumFilesParallel")
      .doc("A limit on the maximum number of files per task processed in parallel on the CPU " +
        "side before the file is sent to the GPU. This affects the amount of host memory used " +
        "when reading the files in parallel. Used with MULTITHREADED reader, see " +
        s"$AVRO_READER_TYPE.")
      .integerConf
      .checkValue(v => v > 0, "The maximum number of files must be greater than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  val ENABLE_DELTA_WRITE = conf("spark.rapids.sql.format.delta.write.enabled")
      .doc("When set to false disables Delta Lake output acceleration.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_ICEBERG = conf("spark.rapids.sql.format.iceberg.enabled")
    .doc("When set to false disables all Iceberg acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ICEBERG_READ = conf("spark.rapids.sql.format.iceberg.read.enabled")
    .doc("When set to false disables Iceberg input acceleration")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_ICEBERG_WRITE = conf("spark.rapids.sql.format.iceberg.write.enabled")
    .doc("When set to false disables Iceberg write acceleration")
    .booleanConf
    .createWithDefault(true)

  val ICEBERG_S3_ASYNC_MAX_CONCURRENCY =
    conf("spark.rapids.iceberg.s3.async.max-concurrency")
      .doc("Max concurrent connections for the AwsCrtAsyncHttpClient used by the " +
        "spark-rapids Iceberg S3 byte-range reader. Used only when the Iceberg " +
        "FileIO property `s3.crt.max-concurrency` is not set.")
      .startupOnly()
      .integerConf
      .createWithDefault(200)

  val ICEBERG_S3_ASYNC_CONNECTION_MAX_IDLE_MS =
    conf("spark.rapids.iceberg.s3.async.connection-max-idle-time-ms")
      .doc("Connection-max-idle-time (ms) for the AwsCrtAsyncHttpClient used by the " +
        "spark-rapids Iceberg S3 byte-range reader. No equivalent Iceberg property.")
      .startupOnly()
      .longConf
      .createWithDefault(5L * 60 * 1000)

  val ICEBERG_S3_ASYNC_TCP_KEEPALIVE_INTERVAL_MS =
    conf("spark.rapids.iceberg.s3.async.tcp-keepalive-interval-ms")
      .doc("TCP keep-alive probe interval (ms) for the AwsCrtAsyncHttpClient used by " +
        "the spark-rapids Iceberg S3 byte-range reader. No equivalent Iceberg property.")
      .startupOnly()
      .longConf
      .createWithDefault(60L * 1000)

  val ICEBERG_S3_ASYNC_TCP_KEEPALIVE_TIMEOUT_MS =
    conf("spark.rapids.iceberg.s3.async.tcp-keepalive-timeout-ms")
      .doc("TCP keep-alive probe timeout (ms) for the AwsCrtAsyncHttpClient used by " +
        "the spark-rapids Iceberg S3 byte-range reader. No equivalent Iceberg property.")
      .startupOnly()
      .longConf
      .createWithDefault(30L * 1000)

  val ENABLE_HIVE_TEXT: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.format.hive.text.enabled")
      .doc("When set to false disables Hive text table acceleration. " +
           "Array/Struct/Map columns are unsupported for acceleration.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_HIVE_TEXT_READ: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.format.hive.text.read.enabled")
      .doc("When set to false disables Hive text table read acceleration. " +
           "Array/Struct/Map columns are unsupported for read acceleration.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_HIVE_TEXT_WRITE: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.format.hive.text.write.enabled")
      .doc("When set to false disables Hive text table write acceleration. " +
           "Array/Struct/Map columns are unsupported for write acceleration.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_READ_HIVE_FLOATS = conf("spark.rapids.sql.format.hive.text.read.float.enabled")
      .doc("Hive text file reading is not 100% compatible when reading floats.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_READ_HIVE_DOUBLES = conf("spark.rapids.sql.format.hive.text.read.double.enabled")
      .doc("Hive text file reading is not 100% compatible when reading doubles.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_READ_HIVE_DECIMALS = conf("spark.rapids.sql.format.hive.text.read.decimal.enabled")
      .doc("Hive text file reading is not 100% compatible when reading decimals. Hive has " +
          "more limitations on what is valid compared to the GPU implementation in some corner " +
          "cases. See https://github.com/NVIDIA/spark-rapids/issues/7246")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_BYTES = conf("spark.rapids.sql.window.range.byte.enabled")
    .doc("When the order-by column of a range based window is byte type and " +
      "the range boundary calculated for a value has overflow, CPU and GPU will get " +
      "the different results. When set to false disables the range window acceleration for the " +
      "byte type order-by column")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_RANGE_WINDOW_SHORT = conf("spark.rapids.sql.window.range.short.enabled")
    .doc("When the order-by column of a range based window is short type and " +
      "the range boundary calculated for a value has overflow, CPU and GPU will get " +
      "the different results. When set to false disables the range window acceleration for the " +
      "short type order-by column")
    .booleanConf
    .createWithDefault(false)

  val ENABLE_RANGE_WINDOW_INT = conf("spark.rapids.sql.window.range.int.enabled")
    .doc("When the order-by column of a range based window is int type and " +
      "the range boundary calculated for a value has overflow, CPU and GPU will get " +
      "the different results. When set to false disables the range window acceleration for the " +
      "int type order-by column")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_LONG = conf("spark.rapids.sql.window.range.long.enabled")
    .doc("When the order-by column of a range based window is long type and " +
      "the range boundary calculated for a value has overflow, CPU and GPU will get " +
      "the different results. When set to false disables the range window acceleration for the " +
      "long type order-by column")
    .booleanConf
    .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_FLOAT: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.window.range.float.enabled")
      .doc("When set to false, this disables the range window acceleration for the " +
        "FLOAT type order-by column")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_DOUBLE: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.window.range.double.enabled")
      .doc("When set to false, this disables the range window acceleration for the " +
        "double type order-by column")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_RANGE_WINDOW_DECIMAL: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.window.range.decimal.enabled")
    .doc("When set to false, this disables the range window acceleration for the " +
      "DECIMAL type order-by column")
    .booleanConf
    .createWithDefault(true)

  val BATCHED_BOUNDED_ROW_WINDOW_MAX: ConfEntryWithDefault[Integer] =
    conf("spark.rapids.sql.window.batched.bounded.row.max")
      .doc("Max value for bounded row window preceding/following extents " +
      "permissible for the window to be evaluated in batched mode. This value affects " +
      "both the preceding and following bounds, potentially doubling the window size " +
      "permitted for batched execution")
      .integerConf
      .createWithDefault(value = 100)

  val ENABLE_SINGLE_PASS_PARTIAL_SORT_AGG: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.agg.singlePassPartialSortEnabled")
    .doc("Enable or disable a single pass partial sort optimization where if a heuristic " +
        "indicates it would be good we pre-sort the data before a partial agg and then " +
        "do the agg in a single pass with no merge, so there is no spilling")
    .internal()
    .booleanConf
    .createWithDefault(true)

  val SKIP_AGG_PASS_REDUCTION_RATIO = conf("spark.rapids.sql.agg.skipAggPassReductionRatio")
    .doc("In non-final aggregation stages, if the previous pass has a row retention ratio " +
        "greater than this value, the next aggregation pass will be skipped." +
        "Setting this to 1 essentially disables this feature. For Historical reasons it is " +
        "called skipAggPassReductionRatio, actually skipAggPassRetentionRatio would have " +
        "been better")
    .internal()
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The ratio value must be in [0, 1].")
    .createWithDefault(0.85)

  val FORCE_SINGLE_PASS_PARTIAL_SORT_AGG: ConfEntryWithDefault[Boolean] =
    conf("spark.rapids.sql.agg.forceSinglePassPartialSort")
    .doc("Force a single pass partial sort agg to happen in all cases that it could, " +
        "no matter what the heuristic says. This is really just for testing.")
    .internal()
    .booleanConf
    .createWithDefault(false)

  val ENABLE_REGEXP = conf("spark.rapids.sql.regexp.enabled")
    .doc("Specifies whether supported regular expressions will be evaluated on the GPU. " +
      "Unsupported expressions will fall back to CPU. However, there are some known edge cases " +
      "that will still execute on GPU and produce incorrect results and these are documented in " +
      "the compatibility guide. Setting this config to false will make all regular expressions " +
      "run on the CPU instead.")
    .booleanConf
    .createWithDefault(true)

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
    .doc("Load hybrid backend as an extra plugin of spark-rapids during launch time")
    .internal()
    .startupOnly()
    .booleanConf
    .createWithDefault(false)

  object HybridFilterPushdownType extends Enumeration {
    val CPU, GPU, OFF = Value
  }

  val PUSH_DOWN_FILTERS_TO_HYBRID = conf("spark.rapids.sql.hybrid.parquet.filterPushDown")
    .doc("Push down all supported filters to CPU if set to CPU. " +
      "If set to GPU, no filters will be pushed down so all filters are on the GPU. " +
      "If set to OFF, filters will be both pushed down and keeped on the GPU. " +
      "OFF is to make the behavior same as before.")
    .internal()
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(HybridFilterPushdownType.values.map(_.toString))
    .createWithDefault(HybridFilterPushdownType.CPU.toString)

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
    .doc("Enable or disable the RAPIDS Shuffle Manager at runtime. " +
      "The [RAPIDS Shuffle Manager](https://docs.nvidia.com/spark-rapids/user-guide/latest" +
      "/additional-functionality/rapids-shuffle.html) must " +
      "already be configured. When set to `false`, the built-in Spark shuffle will be used. ")
    .booleanConf
    .createWithDefault(true)

  object RapidsShuffleManagerMode extends Enumeration {
    val UCX, CACHE_ONLY, MULTITHREADED = Value
  }

  val SHUFFLE_MANAGER_MODE = conf("spark.rapids.shuffle.mode")
    .doc("RAPIDS Shuffle Manager mode. " +
      "\"MULTITHREADED\": shuffle file writes and reads are parallelized using a thread pool. " +
      "\"UCX\": (requires UCX installation) uses accelerated transports for " +
      "transferring shuffle blocks. " +
      "\"CACHE_ONLY\": use when running a single executor, for short-circuit cached " +
      "shuffle (for testing purposes).")
    .startupOnly()
    .stringConf
    .checkValues(RapidsShuffleManagerMode.values.map(_.toString))
    .createWithDefault(RapidsShuffleManagerMode.MULTITHREADED.toString)

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
        "available. See https://github.com/NVIDIA/spark-rapids/issues/9153.")
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

  object ShuffleKudoMode extends Enumeration {
    val CPU, GPU = Value
  }

  val SHUFFLE_KUDO_WRITE_MODE = conf("spark.rapids.shuffle.kudo.serializer.write.mode")
    .doc("Kudo serializer mode. " +
      "\"CPU\": serialize shuffle outputs on the cpu. " +
      "\"GPU\": serialize shuffle outputs on the gpu. ")
    .internal()
    .startupOnly()
    .stringConf
    .createWithDefault(ShuffleKudoMode.CPU.toString)

  val SHUFFLE_KUDO_READ_MODE = conf("spark.rapids.shuffle.kudo.serializer.read.mode")
    .doc("Kudo serializer read mode. " +
      "\"CPU\": deserialize shuffle inputs on the cpu. " +
      "\"GPU\": deserialize shuffle inputs on the gpu. ")
    .internal()
    .startupOnly()
    .stringConf
    .createWithDefault(ShuffleKudoMode.GPU.toString)

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
      "then there is no guarantee the RAPIDS Accelerator will function properly." +
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
    .doc("Overrides the cudf version compatibility check between cudf jar and RAPIDS Accelerator " +
      "jar. If you are sure that the cudf jar which is mentioned in the classpath is compatible " +
      "with the RAPIDS Accelerator version, then set this to true.")
    .booleanConf
    .createWithDefault(false)

  object AllowMultipleJars extends Enumeration {
    val ALWAYS, SAME_REVISION, NEVER = Value
  }

  val ALLOW_MULTIPLE_JARS = conf("spark.rapids.sql.allowMultipleJars")
    .startupOnly()
    .doc("Allow multiple rapids-4-spark, spark-rapids-jni, and cudf jars on the classpath. " +
      "Spark will take the first one it finds, so the version may not be expected. Possisble " +
      "values are ALWAYS: allow all jars, SAME_REVISION: only allow jars with the same " +
      "revision, NEVER: do not allow multiple jars at all.")
    .stringConf
    .transform(_.toUpperCase(java.util.Locale.ROOT))
    .checkValues(AllowMultipleJars.values.map(_.toString))
    .createWithDefault(AllowMultipleJars.SAME_REVISION.toString)

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

  // default value for the OOM injection logic (no injection, for regular operation)
  private val noInjection = OomInjectionConf(
    numOoms = 0,
    skipCount = 0,
    oomInjectionFilter = OomInjectionType.CPU_OR_GPU,
    withSplit = false)

  // A java property that is set to true when we are running in tests.
  // We will use this property to check for oom injection configs in SQLConf, and to turn on
  // the rapids-specific assertInTests function, only if we are running tests. 
  // This is set to true in integration_tests/run_pyspark_from_build.sh, and in the 
  // pom for both scalatest and javatest.
  lazy val runningTests = {
    val res = System.getProperty("com.nvidia.spark.rapids.runningTests", "false")
      .toLowerCase.toBoolean
    if (!res) {
      logDebug("OOM injection in tests disable. To enable, set java property " +
        "`-Dcom.nvidia.spark.rapids.runningTest=true`, and configure using " +
        s"${TEST_RETRY_OOM_INJECTION_MODE.key}.")
    }
    res
  }

  /**
   * Convert a configured injection string to the injection configuration OomInjection,
   * if enabled via com.nvidia.spark.rapids.runningTests java property.
   *
   * The new format is a CSV in any order
   *  "num_ooms=<integer>,skip=<integer>,type=<string value of OomInjectionType>"
   *
   * "type" maps to OomInjectionType to run count against oomCount and skipCount
   * "num_ooms" maps to oomCount (default 1), the number of allocations resulting in an OOM
   * "skip" maps to skipCount (default 0), the number of matching  allocations to skip before
   * injecting an OOM at the skip+1st allocation.
   * *split* maps to withSplit (default false), determining whether to inject
   * *SplitAndRetryOOM instead of plain *RetryOOM exceptions
   *
   * For backwards compatibility support existing binary configuration
   *   "false", disabled, i.e. oomCount=0, skipCount=0, injectionType=None
   *   "true" or anything else but "false"  yields the default
   *      oomCount=1, skipCount=0, injectionType=CPU_OR_GPU, withSplit=false
   */
  def testRetryOOMInjectionMode(): OomInjectionConf = {
    if (!runningTests) {
      // this is the common case
      noInjection
    } else {
      TEST_RETRY_OOM_INJECTION_MODE.get(SQLConf.get).toLowerCase match {
        case "false" => noInjection
        case "true" =>
          OomInjectionConf(numOoms = 1, skipCount = 0,
            oomInjectionFilter = OomInjectionType.CPU_OR_GPU, withSplit = false)
        case injectConfStr =>
          val injectConfMap = injectConfStr.split(',').map(_.split('=')).collect {
            case Array(k, v) => k -> v
          }.toMap
          val numOoms = injectConfMap.getOrElse("num_ooms", 1.toString)
          val skipCount = injectConfMap.getOrElse("skip", 0.toString)
          val oomFilterStr = injectConfMap
            .getOrElse("type", OomInjectionType.CPU_OR_GPU.toString)
            .toUpperCase()
          val oomFilter = OomInjectionType.valueOf(oomFilterStr)
          val withSplit = injectConfMap.getOrElse("split", false.toString)
          val ret = OomInjectionConf(
            numOoms = numOoms.toInt,
            skipCount = skipCount.toInt,
            oomInjectionFilter = oomFilter,
            withSplit = withSplit.toBoolean
          )
          logDebug(s"Parsed ${ret} from ${injectConfStr} via injectConfMap=${injectConfMap}");
          ret
      }
    }
  }

  private def printSectionHeader(category: String): Unit =
    println(s"\n### $category")

  private def printToggleHeader(category: String): Unit = {
    printSectionHeader(category)
    println("Name | Description | Default Value | Notes")
    println("-----|-------------|---------------|------------------")
  }

  private def printToggleHeaderWithSqlFunction(category: String): Unit = {
    printSectionHeader(category)
    println("Name | SQL Function(s) | Description | Default Value | Notes")
    println("-----|-----------------|-------------|---------------|------")
  }

  def help(asTable: Boolean = false): Unit = {
    helpCommon(asTable)
    helpAdvanced(asTable)
  }

  def helpCommon(asTable: Boolean = false): Unit = {
    if (asTable) {
      println("---")
      println("layout: page")
      println("title: Configuration")
      println("nav_order: 4")
      println("---")
      MarkdownUtils.printApacheSparkVersion("RapidsConf.helpCommon")
      // scalastyle:off line.size.limit
      println("""# RAPIDS Accelerator for Apache Spark Configuration
        |The following is the list of options that `rapids-plugin-4-spark` supports.
        |
        |On startup use: `--conf [conf key]=[conf value]`. For example:
        |
        |```
        |${SPARK_HOME}/bin/spark-shell --jars rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar \
        |--conf spark.plugins=com.nvidia.spark.SQLPlugin \
        |--conf spark.rapids.sql.concurrentGpuTasks=2
        |```
        |
        |At runtime use: `spark.conf.set("[conf key]", [conf value])`. For example:
        |
        |```
        |scala> spark.conf.set("spark.rapids.sql.concurrentGpuTasks", 2)
        |```
        |
        | All configs can be set on startup, but some configs, especially for shuffle, will not
        | work if they are set at runtime. Please check the column of "Applicable at" to see
        | when the config can be set. "Startup" means only valid on startup, "Runtime" means
        | valid on both startup and runtime.
        |""".stripMargin)
      // scalastyle:on line.size.limit
      println("\n## General Configuration\n")
      println("Name | Description | Default Value | Applicable at")
      println("-----|-------------|--------------|--------------")
    } else {
      println("Commonly Used Rapids Configs:")
    }
    val allConfs = registeredConfs.clone()
    allConfs.append(RapidsPrivateUtil.getPrivateConfigs(): _*)
    val outputConfs = allConfs.filter(_.isCommonlyUsed)
    outputConfs.sortBy(_.key).foreach(_.help(asTable))
    if (asTable) {
      // scalastyle:off line.size.limit
      println("""
        |For more advanced configs, please refer to the [RAPIDS Accelerator for Apache Spark Advanced Configuration](./additional-functionality/advanced_configs.md) page.
        |""".stripMargin)
      // scalastyle:on line.size.limit
    }
  }

  def helpAdvanced(asTable: Boolean = false): Unit = {
    if (asTable) {
      println("---")
      println("layout: page")
      // print advanced configuration
      println("title: Advanced Configuration")
      println("parent: Additional Functionality")
      println("nav_order: 10")
      println("---")
      MarkdownUtils.printApacheSparkVersion("RapidsConf.helpAdvanced")
      // scalastyle:off line.size.limit
      println("""# RAPIDS Accelerator for Apache Spark Advanced Configuration
        |Most users will not need to modify the configuration options listed below.
        |They are documented here for completeness and advanced usage.
        |
        |The following configuration options are supported by the RAPIDS Accelerator for Apache Spark.
        |
        |For commonly used configurations and examples of setting options, please refer to the
        |[RAPIDS Accelerator for Configuration](../configs.md) page.
        |""".stripMargin)
      // scalastyle:on line.size.limit
      println("\n## Advanced Configuration\n")

      println("Name | Description | Default Value | Applicable at")
      println("-----|-------------|--------------|--------------")
    } else {
      println("Advanced Rapids Configs:")
    }
    val allConfs = registeredConfs.clone()
    allConfs.append(RapidsPrivateUtil.getPrivateConfigs(): _*)
    val outputConfs = allConfs.filterNot(_.isCommonlyUsed)
    outputConfs.sortBy(_.key).foreach(_.help(asTable))
    if (asTable) {
      println("")
      // scalastyle:off line.size.limit
      println("""## Supported GPU Operators and Fine Tuning
        |_The RAPIDS Accelerator for Apache Spark_ can be configured to enable or disable specific
        |GPU accelerated expressions.  Enabled expressions are candidates for GPU execution. If the
        |expression is configured as disabled, the accelerator plugin will not attempt replacement,
        |and it will run on the CPU.
        |
        |Please leverage the [`spark.rapids.sql.explain`](#sql.explain) setting to get
        |feedback from the plugin as to why parts of a query may not be executing on the GPU.
        |
        |**NOTE:** Setting
        |[`spark.rapids.sql.incompatibleOps.enabled=true`](#sql.incompatibleOps.enabled)
        |will enable all the settings in the table below which are not enabled by default due to
        |incompatibilities.""".stripMargin)
      // scalastyle:on line.size.limit

      printToggleHeaderWithSqlFunction("Expressions\n")
    }
    GpuOverrides.expressions.values.toSeq.sortBy(_.tag.toString).foreach { rule =>
      val sqlFunctions =
        ConfHelper.getSqlFunctionsForClass(rule.tag.runtimeClass).map(_.mkString(", "))

      // this is only for formatting, this is done to ensure the table has a column for a
      // row where there isn't a SQL function
      rule.confHelp(asTable, Some(sqlFunctions.getOrElse(" ")))
    }
    if (asTable) {
      printToggleHeader("Execution\n")
    }
    GpuOverrides.execs.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Commands\n")
    }
    GpuOverrides.commonRunnableCmds.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Scans\n")
    }
    GpuOverrides.scans.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
    if (asTable) {
      printToggleHeader("Partitioning\n")
    }
    GpuOverrides.parts.values.toSeq.sortBy(_.tag.toString).foreach(_.confHelp(asTable))
  }
  def main(args: Array[String]): Unit = {
    // Include the configs in PythonConfEntries
    com.nvidia.spark.rapids.python.PythonConfEntries.init()
    val configs = new FileOutputStream(new File(args(0)))
    Console.withOut(configs) {
      Console.withErr(configs) {
        RapidsConf.helpCommon(true)
      }
    }
    val advanced = new FileOutputStream(new File(args(1)))
    Console.withOut(advanced) {
      Console.withErr(advanced) {
        RapidsConf.helpAdvanced(true)
      }
    }
  }

  /**
   * Get join options based on the configuration.
   * @param conf the SQL configuration
   * @param targetSize the target batch size in bytes to use for the join
   * @return JoinOptions configured based on the join strategy and targetSize
   */
  def getJoinOptions(
      conf: SQLConf,
      targetSize: Long): JoinOptions = {
    val strategyStr = JOIN_STRATEGY.get(conf)
    val strategy = JoinStrategy.withName(strategyStr)
    val buildSideStr = JOIN_BUILD_SIDE.get(conf)
    val buildSideSelection = JoinBuildSideSelection.withName(buildSideStr)
    val logCardinality = LOG_JOIN_CARDINALITY.get(conf)
    val sizeEstimateThreshold = JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD.get(conf)
    JoinOptions(strategy, buildSideSelection, targetSize, logCardinality, sizeEstimateThreshold)
  }
}

class RapidsConf(conf: Map[String, String]) extends Logging {

  import ConfHelper._
  import RapidsConf._

  def this(sqlConf: SQLConf) = {
    this(sqlConf.getAllConfs)
  }

  def this(sparkConf: SparkConf) = {
    this(Map(sparkConf.getAll: _*))
  }

  def get[T](entry: ConfEntry[T]): T = {
    entry.get(conf)
  }

  def getStr(key: String): Option[String] = conf.get(key)

  lazy val rapidsConfMap: util.Map[String, String] = conf.filterKeys(
    _.startsWith("spark.rapids.")).toMap.asJava

  lazy val metricsLevel: String = get(METRICS_LEVEL)

  lazy val profilePath: Option[String] = get(PROFILE_PATH)

  lazy val profileExecutors: String = get(PROFILE_EXECUTORS)

  lazy val profileTimeRangesSeconds: Option[String] = get(PROFILE_TIME_RANGES_SECONDS)

  lazy val profileJobs: Option[String] = get(PROFILE_JOBS)

  lazy val profileStages: Option[String] = get(PROFILE_STAGES)

  lazy val profileTaskLimitPerStage: Int = get(PROFILE_TASK_LIMIT_PER_STAGE)

  lazy val profileDriverPollMillis: Int = get(PROFILE_DRIVER_POLL_MILLIS)

  lazy val profileAsyncAllocCapture: Boolean = get(PROFILE_ASYNC_ALLOC_CAPTURE)

  lazy val profileCompression: String = get(PROFILE_COMPRESSION)

  lazy val profileFlushPeriodMillis: Int = get(PROFILE_FLUSH_PERIOD_MILLIS)

  lazy val profileWriteBufferSize: Long = get(PROFILE_WRITE_BUFFER_SIZE)

  lazy val asyncProfilerPathPrefix: Option[String] = get(ASYNC_PROFILER_PATH_PREFIX)

  lazy val asyncProfilerExecutors: String = get(ASYNC_PROFILER_EXECUTORS)

  lazy val asyncProfilerProfileOptions: String = get(ASYNC_PROFILER_PROFILE_OPTIONS)

  lazy val asyncProfilerJfrCompression: Boolean = get(ASYNC_PROFILER_JFR_COMPRESSION)

  lazy val asyncProfilerStageEpochInterval: Int = get(ASYNC_PROFILER_STAGE_EPOCH_INTERVAL)

  lazy val isSqlEnabled: Boolean = get(SQL_ENABLED)

  lazy val isAcceleratedColumnarToRowEnabled: Boolean = get(ACCELERATED_COLUMNAR_TO_ROW_ENABLED)

  lazy val isSqlExecuteOnGPU: Boolean = get(SQL_MODE).equals("executeongpu")

  lazy val isSqlExplainOnlyEnabled: Boolean = get(SQL_MODE).equals("explainonly")

  lazy val isUdfCompilerEnabled: Boolean = get(UDF_COMPILER_ENABLED)

  lazy val isDfUdfEnabled: Boolean = get(DFUDF_ENABLED)

  lazy val exportColumnarRdd: Boolean = get(EXPORT_COLUMNAR_RDD)

  lazy val shuffledHashJoinOptimizeShuffle: Boolean = get(SHUFFLED_HASH_JOIN_OPTIMIZE_SHUFFLE)

  lazy val useShuffledSymmetricHashJoin: Boolean = get(USE_SHUFFLED_SYMMETRIC_HASH_JOIN)

  lazy val useShuffledAsymmetricHashJoin: Boolean =
    get(USE_SHUFFLED_ASYMMETRIC_HASH_JOIN)

  lazy val joinOuterMagnificationThreshold: Int = get(JOIN_OUTER_MAGNIFICATION_THRESHOLD)

  lazy val bucketJoinIoPrefetch: Boolean = get(BUCKET_JOIN_IO_PREFETCH)

  lazy val logJoinCardinality: Boolean = get(LOG_JOIN_CARDINALITY)

  lazy val joinGathererSizeEstimateThreshold: Double = get(JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD)

  /**
   * Get join options based on the current configuration.
   * @param targetSize the target batch size in bytes to use for the join
   * @return JoinOptions configured based on the join strategy and targetSize
   */
  def getJoinOptions(targetSize: Long): JoinOptions = {
    val strategyStr = get(JOIN_STRATEGY)
    val strategy = JoinStrategy.withName(strategyStr)
    val buildSideStr = get(JOIN_BUILD_SIDE)
    val buildSideSelection = JoinBuildSideSelection.withName(buildSideStr)
    val logCardinality = get(LOG_JOIN_CARDINALITY)
    val sizeEstimateThreshold = get(JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD)
    JoinOptions(strategy, buildSideSelection, targetSize, logCardinality, sizeEstimateThreshold)
  }

  lazy val sizedJoinPartitionAmplification: Double = get(SIZED_JOIN_PARTITION_AMPLIFICATION)

  lazy val stableSort: Boolean = get(STABLE_SORT)

  lazy val isFileScanPrunePartitionEnabled: Boolean = get(FILE_SCAN_PRUNE_PARTITION_ENABLED)

  lazy val isIncompatEnabled: Boolean = get(INCOMPATIBLE_OPS)

  lazy val incompatDateFormats: Boolean = get(INCOMPATIBLE_DATE_FORMATS)

  lazy val includeImprovedFloat: Boolean = get(IMPROVED_FLOAT_OPS)

  lazy val pinnedPoolSize: Long = get(PINNED_POOL_SIZE)

  lazy val pinnedPoolCuioDefault: Boolean = get(PINNED_POOL_SET_CUIO_DEFAULT)

  lazy val offHeapLimitEnabled: Boolean = get(OFF_HEAP_LIMIT_ENABLED)

  lazy val offHeapLimit: Option[Long] = get(OFF_HEAP_LIMIT_SIZE)

  lazy val cgroupsMemoryLimitPath: Option[String] = get(CGROUPS_MEMORY_LIMIT_PATH)

  lazy val cgroupsMemoryUsagePath: Option[String] = get(CGROUPS_MEMORY_USAGE_PATH)

  lazy val perTaskOverhead: Long = get(TASK_OVERHEAD_SIZE)

  lazy val concurrentGpuTasks: Option[Integer] = get(CONCURRENT_GPU_TASKS)

  lazy val maxConcurrentGpuTasks: Integer = get(MAX_CONCURRENT_GPU_TASKS)

  lazy val isTestEnabled: Boolean = get(TEST_CONF)

  lazy val isRetryContextCheckEnabled: Boolean = get(TEST_RETRY_CONTEXT_CHECK_ENABLED)

  lazy val isFoldableNonLitAllowed: Boolean = get(FOLDABLE_NON_LIT_ALLOWED)

  lazy val asyncWriteMaxInFlightHostMemoryBytes: Long =
    get(ASYNC_WRITE_MAX_IN_FLIGHT_HOST_MEMORY_BYTES)

  lazy val asyncReadMaxInFlightHostMemoryBytes: Long =
    get(ASYNC_READ_MAX_IN_FLIGHT_HOST_MEMORY_BYTES)

  lazy val testingAllowedNonGpu: Seq[String] = get(TEST_ALLOWED_NONGPU)

  lazy val validateExecsInGpuPlan: Seq[String] = get(TEST_VALIDATE_EXECS_ONGPU)

  lazy val logQueryTransformations: Boolean = get(LOG_TRANSFORMATIONS)

  lazy val rmmDebugLocation: String = get(RMM_DEBUG)

  lazy val sparkRmmDebugLocation: String = get(SPARK_RMM_STATE_DEBUG)

  lazy val sparkRmmStateEnable: Boolean = get(SPARK_RMM_STATE_ENABLE)

  lazy val gpuOomDumpDir: Option[String] = get(GPU_OOM_DUMP_DIR)

  lazy val gpuOomMaxRetries: Int = get(GPU_OOM_MAX_RETRIES)

  lazy val isR2cRetryEnabled: Boolean = get(ENABLE_R2C_RETRY)

  lazy val gpuCoreDumpDir: Option[String] = get(GPU_COREDUMP_DIR)

  lazy val gpuCoreDumpPipePattern: String = get(GPU_COREDUMP_PIPE_PATTERN)

  lazy val isGpuCoreDumpFull: Boolean = get(GPU_COREDUMP_FULL)

  lazy val isGpuCoreDumpCompressed: Boolean = get(GPU_COREDUMP_COMPRESS)

  lazy val gpuCoreDumpCompressionCodec: String = get(GPU_COREDUMP_COMPRESSION_CODEC)

  lazy val isUvmEnabled: Boolean = get(UVM_ENABLED)

  lazy val rmmPool: String = {
    var pool = get(RMM_POOL)
    if ("ASYNC".equalsIgnoreCase(pool)) {
      val driverVersion = Cuda.getDriverVersion
      val runtimeVersion = Cuda.getRuntimeVersion
      var fallbackMessage: Option[String] = None
      if (runtimeVersion < 11020 || driverVersion < 11020) {
        fallbackMessage = Some("CUDA runtime/driver does not support the ASYNC allocator")
      } else if (driverVersion < 11050) {
        fallbackMessage = Some("CUDA drivers before 11.5 have known incompatibilities with " +
          "the ASYNC allocator")
      }
      if (fallbackMessage.isDefined) {
        logWarning(s"${fallbackMessage.get}, falling back to ARENA")
        pool = "ARENA"
      }
    }
    pool
  }

  lazy val rmmExactAlloc: Option[Long] = get(RMM_EXACT_ALLOC)

  lazy val rmmAllocFraction: Double = get(RMM_ALLOC_FRACTION)

  lazy val rmmAllocMaxFraction: Double = get(RMM_ALLOC_MAX_FRACTION)

  lazy val rmmAllocMinFraction: Double = get(RMM_ALLOC_MIN_FRACTION)

  lazy val rmmAllocReserve: Long = get(RMM_ALLOC_RESERVE)

  lazy val integratedGpuMemoryFraction: Double = get(INTEGRATED_GPU_MEMORY_FRACTION)

  lazy val hostSpillStorageSize: Long = get(HOST_SPILL_STORAGE_SIZE)

  lazy val partialFileBufferInitialSize: Long = get(PARTIAL_FILE_BUFFER_INITIAL_SIZE)

  lazy val partialFileBufferMaxSize: Long = get(PARTIAL_FILE_BUFFER_MAX_SIZE)

  lazy val partialFileBufferMemoryThreshold: Double = get(PARTIAL_FILE_BUFFER_MEMORY_THRESHOLD)

  lazy val isUnspillEnabled: Boolean = get(UNSPILL)

  lazy val needDecimalGuarantees: Boolean = get(NEED_DECIMAL_OVERFLOW_GUARANTEES)

  lazy val gpuTargetBatchSizeBytes: Long = get(GPU_BATCH_SIZE_BYTES)

  lazy val isWindowCollectListEnabled: Boolean = get(ENABLE_WINDOW_COLLECT_LIST)

  lazy val isWindowCollectSetEnabled: Boolean = get(ENABLE_WINDOW_COLLECT_SET)

  lazy val isWindowUnboundedAggEnabled: Boolean = get(ENABLE_WINDOW_UNBOUNDED_AGG)

  lazy val isWindowGroupLimitOptEnabled: Boolean = get(ENABLE_WINDOW_GROUP_LIMIT_OPT)

  lazy val isFloatAggEnabled: Boolean = get(ENABLE_FLOAT_AGG)

  lazy val explain: String = get(EXPLAIN)

  lazy val shouldExplain: Boolean = !explain.equalsIgnoreCase("NONE")

  lazy val shouldExplainAll: Boolean = explain.equalsIgnoreCase("ALL")

  lazy val chunkedReaderEnabled: Boolean = get(CHUNKED_READER)

  lazy val limitChunkedReaderMemoryUsage: Boolean = {
    val hasLimit = get(LIMIT_CHUNKED_READER_MEMORY_USAGE)
    val deprecatedConf = get(CHUNKED_SUBPAGE_READER)
    if (deprecatedConf.isDefined) {
      logWarning(s"'${CHUNKED_SUBPAGE_READER.key}' is deprecated and is replaced by " +
        s"'${LIMIT_CHUNKED_READER_MEMORY_USAGE}'.")
      if (hasLimit.isDefined && hasLimit.get != deprecatedConf.get) {
        throw new IllegalStateException(s"Both '${CHUNKED_SUBPAGE_READER.key}' and " +
          s"'${LIMIT_CHUNKED_READER_MEMORY_USAGE.key}' are set but using different values.")
      }
    }
    hasLimit.getOrElse(deprecatedConf.getOrElse(true))
  }

  lazy val chunkedReaderMemoryUsageRatio: Double = get(CHUNKED_READER_MEMORY_USAGE_RATIO)

  lazy val maxReadBatchSizeRows: Int = get(MAX_READER_BATCH_SIZE_ROWS)

  lazy val maxReadBatchSizeBytes: Long = get(MAX_READER_BATCH_SIZE_BYTES)

  lazy val maxGpuColumnSizeBytes: Long = get(MAX_GPU_COLUMN_SIZE_BYTES)

  lazy val outputDebugDumpPrefix: Option[String] = get(OUTPUT_DEBUG_DUMP_PREFIX)

  lazy val parquetDebugDumpPrefix: Option[String] = get(PARQUET_DEBUG_DUMP_PREFIX)

  lazy val parquetDebugDumpAlways: Boolean = get(PARQUET_DEBUG_DUMP_ALWAYS)

  lazy val orcDebugDumpPrefix: Option[String] = get(ORC_DEBUG_DUMP_PREFIX)

  lazy val orcDebugDumpAlways: Boolean = get(ORC_DEBUG_DUMP_ALWAYS)

  lazy val avroDebugDumpPrefix: Option[String] = get(AVRO_DEBUG_DUMP_PREFIX)

  lazy val avroDebugDumpAlways: Boolean = get(AVRO_DEBUG_DUMP_ALWAYS)

  lazy val useHybridParquetReader: Boolean = get(HYBRID_PARQUET_READER)

  lazy val hybridParquetPreloadBatches: Int = get(HYBRID_PARQUET_PRELOAD_CAP)

  lazy val loadHybridBackend: Boolean = get(LOAD_HYBRID_BACKEND)

  lazy val pushDownFiltersToHybrid: String = get(PUSH_DOWN_FILTERS_TO_HYBRID)

  lazy val hybridExprsWhitelist: String = get(HYBRID_EXPRS_WHITELIST)

  lazy val hashAggReplaceMode: String = get(HASH_AGG_REPLACE_MODE)

  lazy val partialMergeDistinctEnabled: Boolean = get(PARTIAL_MERGE_DISTINCT_ENABLED)

  lazy val enableReplaceSortMergeJoin: Boolean = get(ENABLE_REPLACE_SORTMERGEJOIN)

  lazy val enableHashOptimizeSort: Boolean = get(ENABLE_HASH_OPTIMIZE_SORT)

  lazy val enableFoldLocalAggregate: Boolean = get(ENABLE_FOLD_LOCAL_AGGREGATE)

  lazy val areInnerJoinsEnabled: Boolean = get(ENABLE_INNER_JOIN)

  lazy val areCrossJoinsEnabled: Boolean = get(ENABLE_CROSS_JOIN)

  lazy val areLeftOuterJoinsEnabled: Boolean = get(ENABLE_LEFT_OUTER_JOIN)

  lazy val areRightOuterJoinsEnabled: Boolean = get(ENABLE_RIGHT_OUTER_JOIN)

  lazy val areFullOuterJoinsEnabled: Boolean = get(ENABLE_FULL_OUTER_JOIN)

  lazy val areLeftSemiJoinsEnabled: Boolean = get(ENABLE_LEFT_SEMI_JOIN)

  lazy val areLeftAntiJoinsEnabled: Boolean = get(ENABLE_LEFT_ANTI_JOIN)

  lazy val areExistenceJoinsEnabled: Boolean = get(ENABLE_EXISTENCE_JOIN)

  lazy val isCastDecimalToFloatEnabled: Boolean = get(ENABLE_CAST_DECIMAL_TO_FLOAT)

  lazy val isCastFloatToDecimalEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_DECIMAL)

  lazy val isCastFloatToStringEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_STRING)

  lazy val isFloatFormatNumberEnabled: Boolean = get(ENABLE_FLOAT_FORMAT_NUMBER)

  lazy val isCastStringToTimestampEnabled: Boolean = get(ENABLE_CAST_STRING_TO_TIMESTAMP)

  lazy val hasExtendedYearValues: Boolean = get(HAS_EXTENDED_YEAR_VALUES)

  lazy val isCastStringToFloatEnabled: Boolean = get(ENABLE_CAST_STRING_TO_FLOAT)

  lazy val isCastFloatToIntegralTypesEnabled: Boolean = get(ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES)

  lazy val isProjectAstEnabled: Boolean = get(ENABLE_PROJECT_AST)

  lazy val isTieredProjectEnabled: Boolean = get(ENABLE_TIERED_PROJECT)

  lazy val isCombinedExpressionsEnabled: Boolean = get(ENABLE_COMBINED_EXPRESSIONS)

  lazy val isRlikeRegexRewriteEnabled: Boolean = get(ENABLE_RLIKE_REGEX_REWRITE)

  lazy val isExpandPreprojectEnabled: Boolean = get(ENABLE_EXPAND_PREPROJECT)

  lazy val isCoalesceAfterExpandEnabled: Boolean = get(ENABLE_COALESCE_AFTER_EXPAND)

  lazy val multiThreadReadNumThreads: Int = {
    // Use the largest value set among all the options.
    val deprecatedConfs = Seq(
      PARQUET_MULTITHREAD_READ_NUM_THREADS,
      ORC_MULTITHREAD_READ_NUM_THREADS,
      AVRO_MULTITHREAD_READ_NUM_THREADS)
    val values = get(MULTITHREAD_READ_NUM_THREADS) +: deprecatedConfs.flatMap { deprecatedConf =>
      val confValue = get(deprecatedConf)
      confValue.foreach { _ =>
        logWarning(s"$deprecatedConf is deprecated, use $MULTITHREAD_READ_NUM_THREADS. " +
          "Conflicting multithreaded read thread count settings will use the largest value.")
      }
      confValue
    }
    values.max
  }

  lazy val numFilesFilterParallel: Int = get(NUM_FILES_FILTER_PARALLEL)

  lazy val enableMultiThreadReadMemoryLimit: Boolean = get(MULTITHREAD_READ_MEMORY_LIMIT_ENABLED)

  lazy val multiThreadReadMemoryLimit: Long = get(MULTITHREAD_READ_MEMORY_LIMIT_SIZE)

  lazy val multiThreadReadMemoryAcquireTimeout: Long =
    get(MULTITHREAD_READ_MEMORY_LIMIT_ACQUISITION_TIMEOUT)

  lazy val multiThreadReadStageLevelPool: Boolean =
    get(MULTITHREAD_READ_MEMORY_LIMIT_TEST_PER_STAGE_POOL)

  lazy val isParquetEnabled: Boolean = get(ENABLE_PARQUET)

  lazy val isParquetInt96WriteEnabled: Boolean = get(ENABLE_PARQUET_INT96_WRITE)

  lazy val parquetWriterRowGroupSizeRows: Option[Integer] =
    get(PARQUET_WRITER_ROW_GROUP_SIZE_ROWS)

  lazy val parquetWriterRowGroupSizeBytes: Option[Long] =
    get(PARQUET_WRITER_ROW_GROUP_SIZE_BYTES)

  lazy val parquetReaderFooterType: ParquetFooterReaderType.Value = {
    get(PARQUET_READER_FOOTER_TYPE) match {
      case "AUTO" => ParquetFooterReaderType.AUTO
      case "NATIVE" => ParquetFooterReaderType.NATIVE
      case "JAVA" => ParquetFooterReaderType.JAVA
      case other =>
        throw new IllegalArgumentException(s"Internal Error $other is not supported for " +
            s"${PARQUET_READER_FOOTER_TYPE.key}")
    }
  }

  lazy val isParquetPerFileReadEnabled: Boolean =
    RapidsReaderType.withName(get(PARQUET_READER_TYPE)) == RapidsReaderType.PERFILE

  lazy val isParquetAutoReaderEnabled: Boolean =
    RapidsReaderType.withName(get(PARQUET_READER_TYPE)) == RapidsReaderType.AUTO

  lazy val isParquetCoalesceFileReadEnabled: Boolean = isParquetAutoReaderEnabled ||
    RapidsReaderType.withName(get(PARQUET_READER_TYPE)) == RapidsReaderType.COALESCING

  lazy val isParquetMultiThreadReadEnabled: Boolean = isParquetAutoReaderEnabled ||
    RapidsReaderType.withName(get(PARQUET_READER_TYPE)) == RapidsReaderType.MULTITHREADED

  lazy val parquetDecompressCpu: Boolean = get(PARQUET_DECOMPRESS_CPU)

  lazy val parquetDecompressCpuSnappy: Boolean = get(PARQUET_DECOMPRESS_CPU_SNAPPY)

  lazy val parquetDecompressCpuZstd: Boolean = get(PARQUET_DECOMPRESS_CPU_ZSTD)

  lazy val maxNumParquetFilesParallel: Int = get(PARQUET_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL)

  lazy val isParquetReadEnabled: Boolean = get(ENABLE_PARQUET_READ)

  lazy val getMultithreadedCombineThreshold: Long =
    get(READER_MULTITHREADED_COMBINE_THRESHOLD)

  lazy val getMultithreadedCombineWaitTime: Int =
    get(READER_MULTITHREADED_COMBINE_WAIT_TIME)

  lazy val getMultithreadedReaderKeepOrder: Boolean =
    get(READER_MULTITHREADED_READ_KEEP_ORDER)

  lazy val isParquetWriteEnabled: Boolean = get(ENABLE_PARQUET_WRITE)

  lazy val isOrcEnabled: Boolean = get(ENABLE_ORC)

  lazy val isOrcReadEnabled: Boolean = get(ENABLE_ORC_READ)

  lazy val isOrcWriteEnabled: Boolean = get(ENABLE_ORC_WRITE)

  lazy val isOrcFloatTypesToStringEnable: Boolean = get(ENABLE_ORC_FLOAT_TYPES_TO_STRING)

  lazy val isOrcPerFileReadEnabled: Boolean =
    RapidsReaderType.withName(get(ORC_READER_TYPE)) == RapidsReaderType.PERFILE

  lazy val isOrcAutoReaderEnabled: Boolean =
    RapidsReaderType.withName(get(ORC_READER_TYPE)) == RapidsReaderType.AUTO

  lazy val isOrcCoalesceFileReadEnabled: Boolean = isOrcAutoReaderEnabled ||
    RapidsReaderType.withName(get(ORC_READER_TYPE)) == RapidsReaderType.COALESCING

  lazy val isOrcMultiThreadReadEnabled: Boolean = isOrcAutoReaderEnabled ||
    RapidsReaderType.withName(get(ORC_READER_TYPE)) == RapidsReaderType.MULTITHREADED

  lazy val maxNumOrcFilesParallel: Int = get(ORC_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL)

  lazy val testOrcStripeSizeRows: Option[Integer] = get(TEST_ORC_STRIPE_SIZE_ROWS)

  lazy val orcReadIgnoreWriterTimezone: Boolean = get(ORC_READ_IGNORE_WRITE_TIMEZONE)

  lazy val isOrcBoolTypeEnabled: Boolean = get(ENABLE_ORC_BOOL)

  lazy val isCsvEnabled: Boolean = get(ENABLE_CSV)

  lazy val isCsvReadEnabled: Boolean = get(ENABLE_CSV_READ)

  lazy val isCsvFloatReadEnabled: Boolean = get(ENABLE_READ_CSV_FLOATS)

  lazy val isCsvDoubleReadEnabled: Boolean = get(ENABLE_READ_CSV_DOUBLES)

  lazy val isCsvDecimalReadEnabled: Boolean = get(ENABLE_READ_CSV_DECIMALS)

  lazy val isJsonEnabled: Boolean = get(ENABLE_JSON)

  lazy val isJsonReadEnabled: Boolean = get(ENABLE_JSON_READ)

  lazy val isJsonFloatReadEnabled: Boolean = get(ENABLE_READ_JSON_FLOATS)

  lazy val isJsonDoubleReadEnabled: Boolean = get(ENABLE_READ_JSON_DOUBLES)

  lazy val isJsonDecimalReadEnabled: Boolean = get(ENABLE_READ_JSON_DECIMALS)

  lazy val isJsonDateTimeReadEnabled: Boolean = get(ENABLE_READ_JSON_DATE_TIME)

  lazy val isAvroEnabled: Boolean = get(ENABLE_AVRO)

  lazy val isAvroReadEnabled: Boolean = get(ENABLE_AVRO_READ)

  lazy val isAvroPerFileReadEnabled: Boolean =
    RapidsReaderType.withName(get(AVRO_READER_TYPE)) == RapidsReaderType.PERFILE

  lazy val isAvroAutoReaderEnabled: Boolean =
    RapidsReaderType.withName(get(AVRO_READER_TYPE)) == RapidsReaderType.AUTO

  lazy val isAvroCoalesceFileReadEnabled: Boolean = isAvroAutoReaderEnabled ||
    RapidsReaderType.withName(get(AVRO_READER_TYPE)) == RapidsReaderType.COALESCING

  lazy val isAvroMultiThreadReadEnabled: Boolean = isAvroAutoReaderEnabled ||
    RapidsReaderType.withName(get(AVRO_READER_TYPE)) == RapidsReaderType.MULTITHREADED

  lazy val maxNumAvroFilesParallel: Int = get(AVRO_MULTITHREAD_READ_MAX_NUM_FILES_PARALLEL)

  lazy val isDeltaWriteEnabled: Boolean = get(ENABLE_DELTA_WRITE)

  lazy val isIcebergEnabled: Boolean = get(ENABLE_ICEBERG)

  lazy val isIcebergReadEnabled: Boolean = get(ENABLE_ICEBERG_READ)

  lazy val isIcebergWriteEnabled: Boolean = get(ENABLE_ICEBERG_WRITE)

  lazy val isHiveDelimitedTextEnabled: Boolean = get(ENABLE_HIVE_TEXT)

  lazy val isHiveDelimitedTextReadEnabled: Boolean = get(ENABLE_HIVE_TEXT_READ)

  lazy val isHiveDelimitedTextWriteEnabled: Boolean = get(ENABLE_HIVE_TEXT_WRITE)

  lazy val shouldHiveReadFloats: Boolean = get(ENABLE_READ_HIVE_FLOATS)

  lazy val shouldHiveReadDoubles: Boolean = get(ENABLE_READ_HIVE_DOUBLES)

  lazy val shouldHiveReadDecimals: Boolean = get(ENABLE_READ_HIVE_DECIMALS)

  lazy val shuffleManagerEnabled: Boolean = get(SHUFFLE_MANAGER_ENABLED)

  lazy val shuffleManagerMode: String = get(SHUFFLE_MANAGER_MODE)

  lazy val shuffleTransportClassName: String = get(SHUFFLE_TRANSPORT_CLASS_NAME)

  lazy val shuffleTransportEarlyStartHeartbeatInterval: Int = get(
    SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_INTERVAL)

  lazy val shuffleTransportEarlyStartHeartbeatTimeout: Int = get(
    SHUFFLE_TRANSPORT_EARLY_START_HEARTBEAT_TIMEOUT)

  lazy val shuffleTransportEarlyStart: Boolean = get(SHUFFLE_TRANSPORT_EARLY_START)

  lazy val shuffleTransportMaxReceiveInflightBytes: Long = get(
    SHUFFLE_TRANSPORT_MAX_RECEIVE_INFLIGHT_BYTES)

  lazy val shuffleUcxActiveMessagesForceRndv: Boolean = get(SHUFFLE_UCX_ACTIVE_MESSAGES_FORCE_RNDV)

  lazy val shuffleUcxUseWakeup: Boolean = get(SHUFFLE_UCX_USE_WAKEUP)

  lazy val shuffleUcxListenerStartPort: Int = get(SHUFFLE_UCX_LISTENER_START_PORT)

  lazy val shuffleUcxMgmtHost: String = get(SHUFFLE_UCX_MGMT_SERVER_HOST)

  lazy val shuffleUcxMgmtConnTimeout: Int = get(SHUFFLE_UCX_MGMT_CONNECTION_TIMEOUT)

  lazy val shuffleUcxBounceBuffersSize: Long = get(SHUFFLE_UCX_BOUNCE_BUFFERS_SIZE)

  lazy val shuffleUcxDeviceBounceBuffersCount: Int = get(SHUFFLE_UCX_BOUNCE_BUFFERS_DEVICE_COUNT)

  lazy val shuffleUcxHostBounceBuffersCount: Int = get(SHUFFLE_UCX_BOUNCE_BUFFERS_HOST_COUNT)

  lazy val shuffleMaxClientThreads: Int = get(SHUFFLE_MAX_CLIENT_THREADS)

  lazy val shuffleMaxClientTasks: Int = get(SHUFFLE_MAX_CLIENT_TASKS)

  lazy val shuffleClientThreadKeepAliveTime: Int = get(SHUFFLE_CLIENT_THREAD_KEEPALIVE)

  lazy val shuffleMaxServerTasks: Int = get(SHUFFLE_MAX_SERVER_TASKS)

  lazy val shuffleMaxMetadataSize: Long = get(SHUFFLE_MAX_METADATA_SIZE)

  lazy val shuffleCompressionCodec: String = get(SHUFFLE_COMPRESSION_CODEC)

  lazy val shuffleCompressionLz4ChunkSize: Long = get(SHUFFLE_COMPRESSION_LZ4_CHUNK_SIZE)

  lazy val shuffleCompressionZstdChunkSize: Long = get(SHUFFLE_COMPRESSION_ZSTD_CHUNK_SIZE)

  lazy val shuffleCompressionMaxBatchMemory: Long = get(SHUFFLE_COMPRESSION_MAX_BATCH_MEMORY)

  lazy val shuffleMultiThreadedMaxBytesInFlight: Long =
    get(SHUFFLE_MULTITHREADED_MAX_BYTES_IN_FLIGHT)

  lazy val shuffleMultiThreadedWriterThreads: Int = get(SHUFFLE_MULTITHREADED_WRITER_THREADS)

  lazy val shuffleMultiThreadedReaderThreads: Int = get(SHUFFLE_MULTITHREADED_READER_THREADS)

  lazy val shuffleParitioningMaxCpuBatchSize: Long = get(SHUFFLE_PARTITIONING_MAX_CPU_BATCH_SIZE)

  lazy val shuffleKudoSerializerEnabled: Boolean = get(SHUFFLE_KUDO_SERIALIZER_ENABLED)

  lazy val shuffleKudoWriteMode: ShuffleKudoMode.Value =
    ShuffleKudoMode.withName(get(SHUFFLE_KUDO_WRITE_MODE))

  lazy val shuffleKudoReadMode: ShuffleKudoMode.Value =
    ShuffleKudoMode.withName(get(SHUFFLE_KUDO_READ_MODE))

  lazy val shuffleKudoMeasureBufferCopyEnabled: Boolean =
    get(SHUFFLE_KUDO_SERIALIZER_MEASURE_BUFFER_COPY_ENABLED)

  lazy val shuffleAsyncReadEnabled: Boolean = get(SHUFFLE_ASYNC_READ_ENABLED)

  lazy val shuffleKudoSerializerDebugMode: DumpOption = {
    val mode = get(SHUFFLE_KUDO_SERIALIZER_DEBUG_MODE)
    mode match {
      case "NEVER" => DumpOption.Never
      case "ALWAYS" => DumpOption.Always
      case "ONFAILURE" => DumpOption.OnFailure
      case _ => {
        logWarning(s"Invalid value for ${SHUFFLE_KUDO_SERIALIZER_DEBUG_MODE.key}: $mode. " +
          "Using default value: Never")
        DumpOption.Never
      }
    }
  }

  lazy val shuffleKudoSerializerDebugDumpPrefix: Option[String] = {
    val prefix = get(SHUFFLE_KUDO_SERIALIZER_DEBUG_DUMP_PREFIX)
    shuffleKudoSerializerDebugMode match {
      case DumpOption.Never => prefix
      case _ => {
        prefix match {
          case None => {
            logWarning("spark.rapids.shuffle.kudo.serializer.debug.dump.path.prefix is not set, " +
              "so Kudo serializer debug will not be enabled")
            None
          }
          case Some(p) => {
            if (p.isEmpty) {
              logWarning("spark.rapids.shuffle.kudo.serializer.debug.dump.path.prefix is empty, " +
                "so Kudo serializer debug will not be enabled")
              Some("")
            } else {
              Some(p)
            }
          }
        }
      }
    }
  }

  def isUCXShuffleManagerMode: Boolean =
    RapidsShuffleManagerMode
      .withName(get(SHUFFLE_MANAGER_MODE)) == RapidsShuffleManagerMode.UCX

  def isMultiThreadedShuffleManagerMode: Boolean =
    RapidsShuffleManagerMode
      .withName(get(SHUFFLE_MANAGER_MODE)) == RapidsShuffleManagerMode.MULTITHREADED

  def isMultithreadedShuffleSkipMergeEnabled: Boolean = get(MULTITHREADED_SHUFFLE_SKIP_MERGE)

  def isCacheOnlyShuffleManagerMode: Boolean =
    RapidsShuffleManagerMode
      .withName(get(SHUFFLE_MANAGER_MODE)) == RapidsShuffleManagerMode.CACHE_ONLY

  def isGPUShuffle: Boolean = isUCXShuffleManagerMode || isCacheOnlyShuffleManagerMode

  def shuffleKudoGpuSerializerEnabled: Boolean = shuffleKudoSerializerEnabled &&
    shuffleKudoWriteMode == ShuffleKudoMode.GPU

  def shuffleKudoGpuSerializerReadEnabled: Boolean = shuffleKudoSerializerEnabled &&
    shuffleKudoReadMode == ShuffleKudoMode.GPU

  lazy val shimsProviderOverride: Option[String] = get(SHIMS_PROVIDER_OVERRIDE)

  lazy val cudfVersionOverride: Boolean = get(CUDF_VERSION_OVERRIDE)

  lazy val allowMultipleJars: AllowMultipleJars.Value = {
    get(ALLOW_MULTIPLE_JARS) match {
      case "ALWAYS" => AllowMultipleJars.ALWAYS
      case "NEVER" => AllowMultipleJars.NEVER
      case "SAME_REVISION" => AllowMultipleJars.SAME_REVISION
      case other =>
        throw new IllegalArgumentException(s"Internal Error $other is not supported for " +
            s"${ALLOW_MULTIPLE_JARS.key}")
    }
  }

  lazy val allowDisableEntirePlan: Boolean = get(ALLOW_DISABLE_ENTIRE_PLAN)

  lazy val useArrowCopyOptimization: Boolean = get(USE_ARROW_OPT)

  lazy val getCloudSchemes: Seq[String] =
    DEFAULT_CLOUD_SCHEMES ++ get(CLOUD_SCHEMES).getOrElse(Seq.empty)

  lazy val optimizerEnabled: Boolean = get(OPTIMIZER_ENABLED)

  lazy val optimizerExplain: String = get(OPTIMIZER_EXPLAIN)

  lazy val optimizerShouldExplainAll: Boolean = optimizerExplain.equalsIgnoreCase("ALL")

  lazy val optimizerClassName: String = get(OPTIMIZER_CLASS_NAME)

  lazy val defaultRowCount: Long = get(OPTIMIZER_DEFAULT_ROW_COUNT)

  lazy val defaultCpuOperatorCost: Double = get(OPTIMIZER_DEFAULT_CPU_OPERATOR_COST)

  lazy val defaultCpuExpressionCost: Double = get(OPTIMIZER_DEFAULT_CPU_EXPRESSION_COST)

  lazy val defaultGpuOperatorCost: Double = get(OPTIMIZER_DEFAULT_GPU_OPERATOR_COST)

  lazy val defaultGpuExpressionCost: Double = get(OPTIMIZER_DEFAULT_GPU_EXPRESSION_COST)

  lazy val cpuReadMemorySpeed: Double = get(OPTIMIZER_CPU_READ_SPEED)

  lazy val cpuWriteMemorySpeed: Double = get(OPTIMIZER_CPU_WRITE_SPEED)

  lazy val gpuReadMemorySpeed: Double = get(OPTIMIZER_GPU_READ_SPEED)

  lazy val gpuWriteMemorySpeed: Double = get(OPTIMIZER_GPU_WRITE_SPEED)

  lazy val driverTimeZone: Option[String] = get(DRIVER_TIMEZONE)

  lazy val isRangeWindowByteEnabled: Boolean = get(ENABLE_RANGE_WINDOW_BYTES)

  lazy val isRangeWindowShortEnabled: Boolean = get(ENABLE_RANGE_WINDOW_SHORT)

  lazy val isRangeWindowIntEnabled: Boolean = get(ENABLE_RANGE_WINDOW_INT)

  lazy val isRangeWindowLongEnabled: Boolean = get(ENABLE_RANGE_WINDOW_LONG)

  lazy val isRangeWindowFloatEnabled: Boolean = get(ENABLE_RANGE_WINDOW_FLOAT)

  lazy val isRangeWindowDoubleEnabled: Boolean = get(ENABLE_RANGE_WINDOW_DOUBLE)

  lazy val isRangeWindowDecimalEnabled: Boolean = get(ENABLE_RANGE_WINDOW_DECIMAL)

  lazy val batchedBoundedRowsWindowMax: Int = get(BATCHED_BOUNDED_ROW_WINDOW_MAX)

  lazy val allowSinglePassPartialSortAgg: Boolean = get(ENABLE_SINGLE_PASS_PARTIAL_SORT_AGG)

  lazy val forceSinglePassPartialSortAgg: Boolean = get(FORCE_SINGLE_PASS_PARTIAL_SORT_AGG)

  lazy val skipAggPassReductionRatio: Double = get(SKIP_AGG_PASS_REDUCTION_RATIO)

  lazy val isRegExpEnabled: Boolean = get(ENABLE_REGEXP)

  lazy val getSparkGpuResourceName: String = get(SPARK_GPU_RESOURCE_NAME)

  lazy val isCpuBasedUDFEnabled: Boolean = get(ENABLE_CPU_BASED_UDF)

  lazy val isFastSampleEnabled: Boolean = get(ENABLE_FAST_SAMPLE)

  lazy val isForceHiveHashForBucketedWrite: Boolean = get(FORCE_HIVE_HASH_FOR_BUCKETED_WRITE)

  lazy val isDetectDeltaLogQueries: Boolean = get(DETECT_DELTA_LOG_QUERIES)

  lazy val isDetectDeltaCheckpointQueries: Boolean = get(DETECT_DELTA_CHECKPOINT_QUERIES)

  lazy val concurrentWriterPartitionFlushSize: Long = get(CONCURRENT_WRITER_PARTITION_FLUSH_SIZE)

  lazy val isAqeExchangeReuseFixupEnabled: Boolean = get(ENABLE_AQE_EXCHANGE_REUSE_FIXUP)

  lazy val isNonAqeBroadcastReuseFixupEnabled: Boolean =
    get(ENABLE_NON_AQE_BROADCAST_REUSE_FIXUP)

  lazy val chunkedPackPoolSize: Long = get(CHUNKED_PACK_POOL_SIZE)

  lazy val chunkedPackBounceBufferSize: Long = get(CHUNKED_PACK_BOUNCE_BUFFER_SIZE)

  lazy val isProjectSplitRetryEnabled: Boolean = get(PROJECT_SPLIT_RETRY_ENABLED)

  lazy val chunkedPackBounceBufferCount: Int = get(CHUNKED_PACK_BOUNCE_BUFFER_COUNT)

  lazy val spillToDiskBounceBufferSize: Long = get(SPILL_TO_DISK_BOUNCE_BUFFER_SIZE)

  lazy val spillToDiskBounceBufferCount: Int = get(SPILL_TO_DISK_BOUNCE_BUFFER_COUNT)

  lazy val splitUntilSizeOverride: Option[Long] = get(SPLIT_UNTIL_SIZE_OVERRIDE)

  lazy val skipGpuArchCheck: Boolean = get(SKIP_GPU_ARCH_CHECK)

  lazy val testGetJsonObjectSavePath: Option[String] = get(TEST_GET_JSON_OBJECT_SAVE_PATH)

  lazy val testGetJsonObjectSaveRows: Int = get(TEST_GET_JSON_OBJECT_SAVE_ROWS)

  lazy val isDeltaLowShuffleMergeEnabled: Boolean = get(ENABLE_DELTA_LOW_SHUFFLE_MERGE)

  lazy val isDeltaDeletionVectorPredicatePushdownEnabled: Boolean =
    get(DELTA_DELETION_VECTOR_PREDICATE_PUSHDOWN)

  lazy val isHashFuncPartitioningEnabled: Boolean = get(ENABLE_HASH_FUNCTION_IN_PARTITIONING)

  lazy val isTagLoreIdEnabled: Boolean = get(TAG_LORE_ID_ENABLED)

  lazy val loreDumpIds: Map[LoreId, OutputLoreId] = get(LORE_DUMP_IDS)
    .map(OutputLoreId.parse)
    .getOrElse(Map.empty)

  lazy val loreDumpPath: Option[String] = get(LORE_DUMP_PATH)

  lazy val loreSkipDumpingPlan: Boolean = get(LORE_SKIP_DUMPING_PLAN)

  lazy val loreDumpNonStrictMode: Boolean = get(LORE_NON_STRICT_MODE)

  lazy val loreParquetUseOriginalNames: Boolean = get(LORE_PARQUET_USE_ORIGINAL_NAMES)

  lazy val caseWhenFuseEnabled: Boolean = get(CASE_WHEN_FUSE)

  lazy val isAsyncOutputWriteEnabled: Boolean = get(ENABLE_ASYNC_OUTPUT_WRITE)

  private val optimizerDefaults = Map(
    // this is not accurate because CPU projections do have a cost due to appending values
    // to each row that is produced, but this needs to be a really small number because
    // GpuProject cost is zero (in our cost model) and we don't want to encourage moving to
    // the GPU just to do a trivial projection, so we pretend the overhead of a
    // CPU projection (beyond evaluating the expressions) is also zero
    "spark.rapids.sql.optimizer.cpu.exec.ProjectExec" -> "0",
    // The cost of a GPU projection is mostly the cost of evaluating the expressions
    // to produce the projected columns
    "spark.rapids.sql.optimizer.gpu.exec.ProjectExec" -> "0",
    // union does not further process data produced by its children
    "spark.rapids.sql.optimizer.cpu.exec.UnionExec" -> "0",
    "spark.rapids.sql.optimizer.gpu.exec.UnionExec" -> "0"
  )

  def isOperatorEnabled(key: String, incompat: Boolean, isDisabledByDefault: Boolean): Boolean = {
    val default = !(isDisabledByDefault || incompat) || (incompat && isIncompatEnabled)
    conf.get(key).map(toBoolean(_, key)).getOrElse(default)
  }

  /**
   * Get the GPU cost of an expression, for use in the cost-based optimizer.
   */
  def getGpuExpressionCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.gpu.expr.$operatorName"
    getOptionalCost(key)
  }

  /**
   * Get the GPU cost of an operator, for use in the cost-based optimizer.
   */
  def getGpuOperatorCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.gpu.exec.$operatorName"
    getOptionalCost(key)
  }

  /**
   * Get the CPU cost of an expression, for use in the cost-based optimizer.
   */
  def getCpuExpressionCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.cpu.expr.$operatorName"
    getOptionalCost(key)
  }

  /**
   * Get the CPU cost of an operator, for use in the cost-based optimizer.
   */
  def getCpuOperatorCost(operatorName: String): Option[Double] = {
    val key = s"spark.rapids.sql.optimizer.cpu.exec.$operatorName"
    getOptionalCost(key)
  }

  private def getOptionalCost(key: String) = {
    // user-provided value takes precedence, then look in defaults map
    conf.get(key).orElse(optimizerDefaults.get(key)).map(toDouble(_, key))
  }

  /**
   * To judge whether "key" is explicitly set by the users.
   */
  def isConfExplicitlySet(key: String): Boolean = {
    conf.contains(key)
  }

  // CPU-GPU Bridge Configuration accessors

  lazy val isCpuBridgeEnabled: Boolean = get(ENABLE_CPU_BRIDGE)

  lazy val bridgeDisallowList: Set[String] = {
    val listString = get(BRIDGE_DISALLOW_LIST)
    if (listString.nonEmpty) {
      listString.split(",").map(_.trim).filter(_.nonEmpty).toSet
    } else {
      Set.empty
    }
  }

  /**
   * Get the CPU bridge thread pool size override, if configured.
   */
  def getCpuBridgeThreadPoolSize: Option[Int] = get(CPU_BRIDGE_THREAD_POOL_SIZE).map(_.toInt)
}

case class OomInjectionConf(
  numOoms: Int,
  skipCount: Int,
  withSplit: Boolean,
  oomInjectionFilter: OomInjectionType
)
