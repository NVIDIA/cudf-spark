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

package org.apache.spark.sql.rapids.execution

import java.io._
import java.util.UUID
import java.util.concurrent._

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.ref.WeakReference
import scala.util.control.NonFatal

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization}
import ai.rapids.cudf.JCudfSerialization.HostConcatResult
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.lore.{GpuLoreDumpRDD, SimpleRDD}
import com.nvidia.spark.rapids.lore.GpuLore.LORE_DUMP_RDD_TAG
import com.nvidia.spark.rapids.shims.{BroadcastExchangeShims, ShimBroadcastExchangeLike, ShimUnaryExecNode, SparkShimImpl}

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Class that is used to broadcast results (a contiguous host batch) to executors.
 *
 * This is instantiated in the driver, serialized to an output stream provided by Spark
 * to broadcast, and deserialized on the executor. Both the driver's and executor's copies
 * are cleaned via GC. Because Spark closes `AutoCloseable` broadcast results after spilling
 * to disk, this class does not subclass `AutoCloseable`. Instead we implement a `closeInternal`
 * method only to be triggered via GC.
 *
 * @param data HostConcatResult populated for a broadcast that has column, otherwise it is null.
 *             It is transient because we want the executor to deserialize its `data` from Spark's
 *             torrent-backed input stream.
 * @param output used to find the schema for this broadcast batch
 * @param numRows number of rows for this broadcast batch
 * @param dataLen size in bytes for this broadcast batch
 */
// scalastyle:off no.finalize
@SerialVersionUID(100L)
class SerializeConcatHostBuffersDeserializeBatch(
    @transient var data: HostConcatResult,
    output: Seq[Attribute],
    var numRows: Int,
    var dataLen: Long)
  extends Serializable with Logging {
  @transient private var dataTypes = output.map(_.dataType).toArray

  // used for memoization of deserialization to GPU on Executor
  @transient private var batchInternal: SpillableColumnarBatch = null

  private def maybeGpuBatch: Option[SpillableColumnarBatch] = Option(batchInternal)

  def batch: SpillableColumnarBatch = this.synchronized {
    maybeGpuBatch.getOrElse {
      NvtxRegistry.BROADCAST_MANIFEST_BATCH {
        val spillable =
          if (data == null || data.getTableHeader.getNumColumns == 0) {
            // If `data` is null or there are no columns, this is a rows-only batch
            SpillableColumnarBatch(
              new ColumnarBatch(Array.empty, numRows),
              SpillPriorities.ACTIVE_BATCHING_PRIORITY)
          } else if (data.getTableHeader.getNumRows == 0) {
            // If we have columns but no rows, we can use the emptyBatchFromTypes optimization
            SpillableColumnarBatch(
              GpuColumnVector.emptyBatchFromTypes(dataTypes),
              SpillPriorities.ACTIVE_BATCHING_PRIORITY)
          } else {
            // Regular GPU batch with rows/cols
            SpillableColumnarBatch(
              data.toContiguousTable,
              dataTypes,
              SpillPriorities.ACTIVE_BATCHING_PRIORITY)
          }
        // At this point we no longer need the host data and should not need to touch it again.
        // Note that we don't close this using `withResources` around the creation of the
        // `SpillableColumnarBatch`. That is because if a retry exception is thrown we want to
        // still be able to recreate this broadcast batch, so we can't close the host data
        // until we are at this line.
        data.safeClose()
        data = null
        batchInternal = spillable
        spillable
      }
    }
  }

  /**
   * Create host columnar batches from either serialized buffers or device columnar batch. This
   * method can be safely called in both driver node and executor nodes. For now, it is used on
   * the driver side for reusing GPU broadcast results in the CPU.
   *
   * NOTE: The caller is responsible to release these host columnar batches.
   */
  def hostBatch: ColumnarBatch = this.synchronized {
    maybeGpuBatch.map { spillable =>
      withResource(spillable.getColumnarBatch()) { batch =>
        val hostColumns: Array[ColumnVector] = GpuColumnVector
          .extractColumns(batch)
          .safeMap(_.copyToHost())
        new ColumnarBatch(hostColumns, numRows)
      }
    }.getOrElse {
      NvtxRegistry.BROADCAST_MANIFEST_BATCH {
        if (data == null) {
          new ColumnarBatch(Array.empty, numRows)
        } else {
          val header = data.getTableHeader
          val buffer = data.getHostBuffer
          val hostColumns = SerializedHostTableUtils.buildHostColumns(
            header, buffer, dataTypes)
          val rowCount = header.getNumRows
          new ColumnarBatch(hostColumns.toArray, rowCount)
        }
      }
    }
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    doWriteObject(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    doReadObject(in)
  }

  /**
   * doWriteObject is invoked from both the driver, when it is trying to write
   * a collected broadcast result on an stream to torrent broadcast to executors, and also
   * when the executor MemoryStore evicts a "broadcast_[id]" block to make room in host memory.
   *
   * The driver will have `data` populated on construction and the executor will deserialize
   * the object and, as part of the deserialization, invoke `doReadObject`.
   * This will populate `data` before any task has had a chance to call `.batch` on this class.
   *
   * If `batchInternal` is defined we are in the executor, and there is no work to be done.
   * This broadcast has been materialized on the GPU/spill store, and it is completely
   * managed by the plugin.
   *
   * Public for unit tests.
   *
   * @param out the stream to write to
   */
  def doWriteObject(out: ObjectOutputStream): Unit = this.synchronized {
    maybeGpuBatch.map {
      case justRows: JustRowsColumnarBatch =>
        JCudfSerialization.writeRowsToStream(out, justRows.numRows())
      case scb: SpillableColumnarBatch =>
        val table = withResource(scb.getColumnarBatch()) { cb =>
          GpuColumnVector.from(cb)
        }
        withResource(table) { _ =>
          JCudfSerialization.writeToStream(table, out, 0, table.getRowCount)
        }
        out.writeObject(dataTypes)
    }.getOrElse {
      if (data == null || data.getTableHeader.getNumColumns == 0) {
        JCudfSerialization.writeRowsToStream(out, numRows)
      } else if (numRows == 0) {
        // We didn't get any data back, but we need to write out an empty table that matches
        withResource(GpuColumnVector.emptyHostColumns(dataTypes)) { hostVectors =>
          JCudfSerialization.writeToStream(hostVectors, out, 0, 0)
        }
        out.writeObject(dataTypes)
      } else {
        val headers = Array(data.getTableHeader)
        val buffers = Array(data.getHostBuffer)
        JCudfSerialization.writeConcatedStream(headers, buffers, out)
        out.writeObject(dataTypes)
      }
    }
  }

  /**
   * Deserializes a broadcast result in the host into `data`, `numRows` and `dataLen`.
   *
   * Public for unit tests.
   */
  def doReadObject(in: ObjectInputStream): Unit = this.synchronized {
    // no-op if we already have `batchInternal` or `data` set
    if (batchInternal == null && data == null) {
      NvtxRegistry.DESERIALIZE_BATCH {
        val (header, buffer) = SerializedHostTableUtils.readTableHeaderAndBuffer(in)
        withResource(buffer) { _ =>
          dataTypes = if (header.getNumColumns > 0) {
            in.readObject().asInstanceOf[Array[DataType]]
          } else {
            Array.empty
          }
          // for a rowsOnly broadcast, null out the `data` member.
          val rowsOnly = dataTypes.isEmpty
          numRows = header.getNumRows
          dataLen = header.getDataLen
          data = if (!rowsOnly) {
            JCudfSerialization.concatToHostBuffer(Array(header), Array(buffer))
          } else {
            null
          }
        }
      }
    }
  }

  def dataSize: Long = dataLen

  /**
   * This method is meant to only be called from `finalize` and it is not a regular
   * AutoCloseable.close because we do not want Spark to close `batchInternal` when it spills
   * the broadcast block's host torrent data.
   *
   * Reference: https://github.com/NVIDIA/spark-rapids/issues/8602
   *
   * Public for tests.
   */
  def closeInternal(): Unit = this.synchronized {
    Seq(data, batchInternal).safeClose()
    data = null
    batchInternal = null
  }

  @scala.annotation.nowarn("msg=method finalize in class Object is deprecated")
  override def finalize(): Unit = {
    super.finalize()
    closeInternal()
  }
}
// scalastyle:on no.finalize

// scalastyle:off no.finalize
/**
 * Object used for executors to serialize a result for their partition that will be collected
 * on the driver to be broadcasted out as part of the exchange.
 * @param batch - GPU batch to be serialized and sent to the driver.
 */
@SerialVersionUID(100L)
class SerializeBatchDeserializeHostBuffer(batch: ColumnarBatch)
  extends Serializable with AutoCloseable {
  @transient private var columns = GpuColumnVector.extractBases(batch).map(_.copyToHost())
  @transient var header: JCudfSerialization.SerializedTableHeader = null
  @transient var buffer: HostMemoryBuffer = null
  @transient private var numRows = batch.numRows()

  private def writeObject(out: ObjectOutputStream): Unit = {
    NvtxRegistry.SERIALIZE_BROADCAST_BATCH {
      if (buffer != null) {
        throw new IllegalStateException("Cannot re-serialize a batch this way...")
      } else {
        JCudfSerialization.writeToStream(columns, out, 0, numRows)
        // In this case an RDD, we want to close the batch once it is serialized out or we will
        // leak GPU memory (technically it will just wait for GC to release it and probably
        // not a lot because this is used for a broadcast that really should be small)
        // In the case of broadcast the life cycle of the object is tied to GC and there is no clean
        // way to separate the two right now.  So we accept the leak.
        columns.safeClose()
        columns = null
      }
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    NvtxRegistry.HOST_DESERIALIZE_BATCH {
      val (h, b) = SerializedHostTableUtils.readTableHeaderAndBuffer(in)
      // buffer will only be cleaned up on GC, so cannot warn about leaks
      b.noWarnLeakExpected()
      header = h
      buffer = b
      numRows = h.getNumRows
    }
  }

  def dataSize: Long = {
    JCudfSerialization.getSerializedSizeInBytes(columns, 0, numRows)
  }

  override def close(): Unit = {
    columns.safeClose()
    columns = null
    buffer.safeClose()
    buffer = null
  }

  @scala.annotation.nowarn("msg=method finalize in class Object is deprecated")
  override def finalize(): Unit = {
    super.finalize()
    close()
  }
}

class GpuBroadcastMeta(
    exchange: BroadcastExchangeExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends
  SparkPlanMeta[BroadcastExchangeExec](exchange, conf, parent, rule) with Logging {

  override def tagPlanForGpu(): Unit = {
    if (!TrampolineUtil.isSupportedRelation(exchange.mode)) {
      willNotWorkOnGpu(
        s"unsupported BroadcastMode: ${exchange.mode}. " +
          s"GPU supports only IdentityBroadcastMode and HashedRelationBroadcastMode")
    }
    def isSupported(rm: RapidsMeta[_, _, _]): Boolean = rm.wrapped match {
      case _: BroadcastHashJoinExec => true
      case _: BroadcastNestedLoopJoinExec => true
      case _ => false
    }
    if (parent.isDefined) {
      if (!parent.exists(isSupported)) {
        willNotWorkOnGpu("BroadcastExchange only works on the GPU if being used " +
            "with a GPU version of BroadcastHashJoinExec or BroadcastNestedLoopJoinExec")
      }
    }
  }

  override def convertToGpu(): GpuExec = {
    val gpuBroadcast = GpuBroadcastExchangeExec(exchange.mode, childPlans.head.convertIfNeeded())(
      exchange.canonicalized.asInstanceOf[BroadcastExchangeExec])
    GpuTransitionOverrides.copyAqeQueryStageExchangeTag(exchange, gpuBroadcast)
    gpuBroadcast
  }
}

abstract class GpuBroadcastExchangeExecBase(
    mode: BroadcastMode,
    child: SparkPlan) extends ShimBroadcastExchangeLike with ShimUnaryExecNode with GpuExec {

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics = Map(
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL, "data size"),
    COLLECT_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_COLLECT_TIME),
    BUILD_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_TIME),
    "broadcastTime" -> createNanoTimingMetric(ESSENTIAL_LEVEL, "time to broadcast"))

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  // For now all broadcasts produce a single batch. We might need to change that at some point
  override def outputBatching: CoalesceGoal = RequireSingleBatch

  @transient
  protected val timeout: Long = SQLConf.get.broadcastTimeout

  // prior to Spark 3.5.0, runId is defined as `def` rather than `val` so
  // produces a new ID on each reference. We override with a `val` so that
  // the value is assigned once.
  override val runId: UUID = UUID.randomUUID

  @transient
  lazy val relationFuture: Future[Broadcast[Any]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val dataSize = gpuLongMetric("dataSize")
    val collectTime = gpuLongMetric(COLLECT_TIME)
    val buildTime = gpuLongMetric(BUILD_TIME)
    val broadcastTime = gpuLongMetric("broadcastTime")

    SQLExecution.withThreadLocalCaptured[Broadcast[Any]](
        session, GpuBroadcastExchangeExecBase.executionContext) {
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(runId.toString, s"broadcast exchange (runId ${runId})",
          interruptOnCancel = true)
        val broadcastResult = {
          val collected =
            NvtxIdWithMetrics(NvtxRegistry.BROADCAST_COLLECT, collectTime) {
              val childRdd = child.executeColumnar()

              // collect batches from the executors
              val data = childRdd.map(withResource(_) { cb =>
                new SerializeBatchDeserializeHostBuffer(cb)
              })
              data.collect()
            }
          NvtxIdWithMetrics(NvtxRegistry.BROADCAST_BUILD, buildTime) {
            val emptyRelation = if (collected.isEmpty) {
              SparkShimImpl.tryTransformIfEmptyRelation(mode)
            } else {
              None
            }
            emptyRelation.getOrElse {
              GpuBroadcastExchangeExecBase.makeBroadcastBatch(
                collected, output, numOutputBatches, numOutputRows, dataSize, conf)
            }
          }
        }
        val broadcasted =
          NvtxIdWithMetrics(NvtxRegistry.BROADCAST, broadcastTime) {
            // Broadcast the relation
            sparkContext.broadcast(broadcastResult)
        }
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        promise.success(broadcasted)
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = createOutOfMemoryException(oe)
          promise.failure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new Exception(e)
          promise.failure(ex)
          throw ex
        case e: Throwable =>
          promise.failure(e)
          throw e
      }
    }
  }


  protected def createOutOfMemoryException(oe: OutOfMemoryError) = {
    new Exception(
      new OutOfMemoryError("Not enough memory to build and broadcast the table to all " +
        "worker nodes. As a workaround, you can either disable broadcast by setting " +
        s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
        s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
        .initCause(oe.getCause))
  }

  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuBroadcastExchange does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(s"Could not execute broadcast in $timeout secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }

  final def executeColumnarBroadcast[T](): Broadcast[T] = {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("A canonicalized plan is not supposed to be executed.")
    }
    try {
      val ret = relationFuture.get(timeout, TimeUnit.SECONDS)
      doLoreDump(ret)
      ret.asInstanceOf[Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(s"Could not execute broadcast in $timeout secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }

  // We have to do this explicitly here rather than similar to the general version one in
  // [[GpuExec]] since in adaptive execution, the broadcast value has already been calculated
  // before we tag this plan to dump.
  private def doLoreDump(result: Broadcast[Any]): Unit = {
    val inner = new SimpleRDD(session.sparkContext, result, schema)
    getTagValue(LORE_DUMP_RDD_TAG).foreach { info =>
      val rdd = new GpuLoreDumpRDD(info, inner)
      rdd.saveMeta()
      rdd.foreach(_.close())
    }
  }

  override def runtimeStatistics: Statistics = {
    Statistics(
      sizeInBytes = metrics("dataSize").value,
      rowCount = Some(metrics(GpuMetric.NUM_OUTPUT_ROWS).value))
  }

  override def resetMetrics(): Unit = {
    // no-op
    // BroadcastExchangeExec after materialized won't be materialized again, so we should not
    // reset the metrics. Otherwise, we will lose the metrics collected in the broadcast job.
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
        s" mismatch:\n$this")
  }
}

object GpuBroadcastExchangeExecBase {
  val executionContext = ExecutionContext.fromExecutorService(
    org.apache.spark.util.ThreadUtils.newDaemonCachedThreadPool("gpu-broadcast-exchange",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))

  protected def checkRowLimit(numRows: Int) = {
    // Spark restricts the size of broadcast relations to be less than 512000000 rows and we
    // enforce the same limit
    // scalastyle:off line.size.limit
    // https://github.com/apache/spark/blob/v3.1.1/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashedRelation.scala#L586
    // scalastyle:on line.size.limit
    if (numRows >= 512000000) {
      throw new SparkException(
        s"Cannot broadcast the table with 512 million or more rows: $numRows rows")
    }
  }

  protected def checkSizeLimit(sizeInBytes: Long, conf: SQLConf) = {
    // Spark restricts the size of broadcast relations
    val maxBytes = BroadcastExchangeShims.getMaxBroadcastTableBytes(conf)
    if (sizeInBytes >= maxBytes) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than" +
            s"${maxBytes >> 30}GB: ${sizeInBytes >> 30} GB")
    }
  }

  /**
   * Concatenate deserialized host buffers into a single HostConcatResult that is then
   * passed to a `SerializeConcatHostBuffersDeserializeBatch`.
   *
   * This result will in turn be broadcasted from the driver to the executors.
   */
  def makeBroadcastBatch(
      buffers: Array[SerializeBatchDeserializeHostBuffer],
      output: Seq[Attribute],
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      dataSize: GpuMetric,
      conf: SQLConf): SerializeConcatHostBuffersDeserializeBatch = {
    val rowsOnly = buffers.isEmpty || buffers.head.header.getNumColumns == 0
    var numRows = 0
    var dataLen: Long = 0
    val hostConcatResult = if (rowsOnly) {
      numRows = withResource(buffers) { _ =>
        require(output.isEmpty,
          "Rows-only broadcast resolved had non-empty " +
              s"output ${output.mkString(",")}")
        buffers.map(_.header.getNumRows).sum
      }
      checkRowLimit(numRows)
      null
    } else {
      val hostConcatResult = withResource(buffers) { _ =>
        JCudfSerialization.concatToHostBuffer(
          buffers.map(_.header), buffers.map(_.buffer))
      }
      closeOnExcept(hostConcatResult) { _ =>
        checkRowLimit(hostConcatResult.getTableHeader.getNumRows)
        checkSizeLimit(hostConcatResult.getTableHeader.getDataLen, conf)
      }
      // this result will be GC'ed later, so we mark it as such
      hostConcatResult.getHostBuffer.noWarnLeakExpected()
      numRows = hostConcatResult.getTableHeader.getNumRows
      dataLen = hostConcatResult.getTableHeader.getDataLen
      hostConcatResult
    }
    numOutputBatches += 1
    numOutputRows += numRows
    dataSize += dataLen

    // create the batch we will broadcast out
    new SerializeConcatHostBuffersDeserializeBatch(
      hostConcatResult, output, numRows, dataLen)
  }
}

case class GpuBroadcastExchangeExec(
    mode: BroadcastMode,
    child: SparkPlan)
    (val cpuCanonical: BroadcastExchangeExec)
    extends GpuBroadcastExchangeExecBase(mode, child) {
  override def otherCopyArgs: Seq[AnyRef] = Seq(cpuCanonical)

  private var _isGpuPlanningComplete = false

  /**
   * Returns true if this node and children are finished being optimized by the RAPIDS Accelerator.
   */
  def isGpuPlanningComplete: Boolean = _isGpuPlanningComplete

  /**
   * Method to call after all RAPIDS Accelerator optimizations have been applied
   * to indicate this node and its children are done being planned by the RAPIDS Accelerator.
   * Some optimizations, such as AQE exchange reuse fixup, need to know when a node will no longer
   * be updated so it can be tracked for reuse.
   */
  def markGpuPlanningComplete(): Unit = {
    if (!_isGpuPlanningComplete) {
      _isGpuPlanningComplete = true
      ExchangeMappingCache.trackExchangeMapping(cpuCanonical, this)
    }
  }

  override def doCanonicalize(): SparkPlan = {
    GpuBroadcastExchangeExec(mode.canonicalized, child.canonicalized)(cpuCanonical)
  }
}

/** Caches the mappings from canonical CPU exchanges to the GPU exchanges that replaced them */
object ExchangeMappingCache extends Logging {
  // Cache is a mapping from CPU broadcast plan to GPU broadcast plan. The cache should not
  // artificially hold onto unused plans, so we make both the keys and values weak. The values
  // point to their corresponding keys, so the keys will not be collected unless the value
  // can be collected. The values will be held during normal Catalyst planning until those
  // plans are no longer referenced, allowing both the key and value to be reaped at that point.
  private val cache = new mutable.WeakHashMap[Exchange, WeakReference[Exchange]]

  /** Try to find a recent GPU exchange that has replaced the specified CPU canonical plan. */
  def findGpuExchangeReplacement(cpuCanonical: Exchange): Option[Exchange] = {
    cache.get(cpuCanonical).flatMap(_.get)
  }

  /** Add a GPU exchange to the exchange cache */
  def trackExchangeMapping(cpuCanonical: Exchange, gpuExchange: Exchange): Unit = {
    val old = findGpuExchangeReplacement(cpuCanonical)
    if (!old.exists(_.asInstanceOf[GpuBroadcastExchangeExec].isGpuPlanningComplete)) {
      cache.put(cpuCanonical, WeakReference(gpuExchange))
    }
  }
}
