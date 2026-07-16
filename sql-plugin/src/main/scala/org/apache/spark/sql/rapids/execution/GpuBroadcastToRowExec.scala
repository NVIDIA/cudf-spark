/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import java.util.concurrent._

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.ExecutionContext

import com.nvidia.spark.rapids.{GpuExec, GpuMetric}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric.{COLLECT_TIME, DESCRIPTION_COLLECT_TIME, ESSENTIAL_LEVEL, NUM_OUTPUT_ROWS}
import com.nvidia.spark.rapids.shims.{ShimBroadcastExchangeLike, ShimUnaryExecNode, SparkShimImpl}

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

// a version of GpuSubqueryBroadcastExec that implements doExecuteBroadcast
// and uses the same algorithm to pull columns from the GPU to rows on the CPU.
case class GpuBroadcastToRowExec(
  buildKeys: Seq[Expression],
  broadcastMode: BroadcastMode,
  child: SparkPlan)
  extends ShimBroadcastExchangeLike with ShimUnaryExecNode with GpuExec with Logging {

  @transient
  private val timeout: Long = conf.broadcastTimeout

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL, "data size"),
    COLLECT_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_COLLECT_TIME))

  @transient
  override lazy val relationFuture: Future[Broadcast[Any]] = {
    // This will run in another thread. Set the execution id so that we can connect these jobs
    // with the correct execution.
    SQLExecution.withThreadLocalCaptured[Broadcast[Any]](
      session, GpuBroadcastToRowExec.executionContext) {
      val broadcastBatch = child.executeBroadcast[Any]()
      val rows: Array[InternalRow] = broadcastBatch.value match {
        case b: SerializeConcatHostBuffersDeserializeBatch => projectSerializedBatch(b)
        case b if SparkShimImpl.isEmptyRelation(b) => Array.empty
        case b => throw new IllegalStateException(s"Unexpected broadcast type: ${b.getClass}")
      }

      val result = SparkShimImpl.broadcastModeTransform(broadcastMode, rows)
      val broadcasted = sparkContext.broadcast(result)
      promise.trySuccess(broadcasted)
      broadcasted
    }
  }

  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    GpuBroadcastToRowExec(keys, broadcastMode, child.canonicalized)
  }

  private def projectSerializedBatch(
      serBatch: SerializeConcatHostBuffersDeserializeBatch): Array[InternalRow] = {
    val beforeCollect = System.nanoTime()

    // Deserializes the batch on the host. Then, transforms it to rows and performs row-wise
    // projection. We should NOT run any device operation on the driver node.
    val result = withResource(serBatch.hostBatch) { hostBatch =>
      val projection = UnsafeProjection.create((0 until hostBatch.numCols()).map { idx =>
        BoundReference(idx, hostBatch.column(idx).dataType, nullable = true)
      }.toSeq)
      hostBatch.rowIterator().asScala.map { row =>
        projection(row).copy().asInstanceOf[InternalRow]
      }.toArray // force evaluation so we don't close hostBatch too soon
    }

    gpuLongMetric("dataSize") += serBatch.dataSize
    gpuLongMetric(COLLECT_TIME) += System.nanoTime() - beforeCollect

    result
  }

  protected[sql] override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuBroadcastToRowExec does not support the execute() code path."
    )
  }

  protected[sql] override def doExecuteBroadcast[T](): Broadcast[T] = {
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

  override def runtimeStatistics: Statistics = {
    Statistics(
      sizeInBytes = metrics("dataSize").value,
      rowCount = Some(metrics(NUM_OUTPUT_ROWS).value))
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
        s" mismatch:\n$this")
  }
}

object GpuBroadcastToRowExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-rows", 16))
}