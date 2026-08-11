/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable

import ai.rapids.cudf.JCudfSerialization
import com.nvidia.spark.rapids.{GpuBindReferences, GpuBuildLeft, GpuColumnVector, GpuExec, GpuExpression, GpuMetric, GpuSemaphore, LazySpillableColumnarBatch, MetricsLevel, NvtxRegistry, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimBinaryExecNode

import org.apache.spark.{Dependency, NarrowDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.Cross
import org.apache.spark.sql.execution.{ExplainUtils, SparkPlan}
import org.apache.spark.sql.rapids.execution.GpuBroadcastNestedLoopJoinExecBase
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

@SerialVersionUID(100L)
class GpuSerializableBatch(batch: ColumnarBatch)
    extends Serializable with AutoCloseable {

  assert(batch != null)
  @transient private var internalBatch: ColumnarBatch = batch

  def getBatch: ColumnarBatch = {
    assert(internalBatch != null)
    internalBatch
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    NvtxRegistry.CARTESIAN_PRODUCT_SERIALIZE {
      if (internalBatch == null) {
        throw new IllegalStateException("Cannot re-serialize a batch this way...")
      } else {
        val schemaArray = (0 until batch.numCols()).map(batch.column(_).dataType()).toArray
        out.writeObject(schemaArray)
        val numRows = internalBatch.numRows()
        val columns = GpuColumnVector.extractBases(internalBatch).map(_.copyToHost())
        try {
          internalBatch.close()
          internalBatch = null
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          JCudfSerialization.writeToStream(columns, out, 0, numRows)
        } finally {
          columns.safeClose()
        }
      }
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    // There is no good way to tie this object deserialization to a specific metric, and I am not
    // sure it is worth trying.
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    NvtxRegistry.CARTESIAN_PRODUCT_DESERIALIZE {
      val schemaArray = in.readObject().asInstanceOf[Array[DataType]]
      withResource(JCudfSerialization.readTableFrom(in)) { tableInfo =>
        val tmp = tableInfo.getTable
        if (tmp == null) {
          throw new IllegalStateException("Empty Batch???")
        }
        this.internalBatch = GpuColumnVector.from(tmp, schemaArray)
      }
    }
  }

  override def close(): Unit = {
    if (internalBatch != null) {
      internalBatch.close()
    }
  }
}

class GpuCartesianPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
) extends Partition {
  var s1: Partition = rdd1.partitions(s1Index)
  var s2: Partition = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

class GpuCartesianRDD(
    sc: SparkContext,
    boundCondition: Option[GpuExpression],
    numFirstTableColumns: Int,
    streamAttributes: Seq[Attribute],
    targetSize: Long,
    sizeEstimateThreshold: Double,
    opTime: GpuMetric,
    joinTime: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    var rdd1: RDD[GpuSerializableBatch],
    var rdd2: RDD[GpuSerializableBatch])
    extends RDD[ColumnarBatch](sc, Nil) with Serializable {

  private val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new GpuCartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[GpuCartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val currSplit = split.asInstanceOf[GpuCartesianPartition]

    // create a buffer to cache stream-side data in a spillable manner
    val spillBatchBuffer = mutable.ArrayBuffer[LazySpillableColumnarBatch]()
    // sentinel variable to label whether stream-side data is cached or not
    var streamSideCached = false

    def close(): Unit = {
      spillBatchBuffer.safeClose()
      spillBatchBuffer.clear()
    }

    // Add a taskCompletionListener to ensure the release of GPU memory. This listener will work
    // if the CompletionIterator does not fully iterate before the task completes, which may
    // happen if there exists specific plans like `LimitExec`.
    onTaskCompletion(context)(close())

    rdd1.iterator(currSplit.s1, context).flatMap { lhs =>
      val batch = withResource(lhs.getBatch) { lhsBatch =>
        LazySpillableColumnarBatch(lhsBatch, "cross_lhs")
      }
      // Introduce sentinel `streamSideCached` to record whether stream-side data is cached or
      // not, because predicate `spillBatchBuffer.isEmpty` will always be true if
      // `rdd2.iterator` is an empty iterator.
      val streamIterator = if (!streamSideCached) {
        streamSideCached = true
        // lazily compute and cache stream-side data
        rdd2.iterator(currSplit.s2, context).map { serializableBatch =>
          withResource(serializableBatch.getBatch) { batch =>
            val lzyBatch = LazySpillableColumnarBatch(batch, "cross_rhs")
            spillBatchBuffer += lzyBatch
            // return a spill only version so we don't close it until the end
            LazySpillableColumnarBatch.spillOnly(lzyBatch)
          }
        }
      } else {
        // fetch cached stream-side data, and make it spill only so we don't close it until the end
        spillBatchBuffer.toIterator.map(LazySpillableColumnarBatch.spillOnly)
      }

      GpuBroadcastNestedLoopJoinExecBase.nestedLoopJoin(
        Cross, GpuBuildLeft, numFirstTableColumns, batch, streamIterator, streamAttributes,
        targetSize, sizeEstimateThreshold, boundCondition,
        numOutputRows = numOutputRows,
        numOutputBatches = numOutputBatches,
        opTime = opTime,
        joinTime = joinTime)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}

case class GpuCartesianProductExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression],
    targetSizeBytes: Long) extends ShimBinaryExecNode with GpuExec {

  import GpuMetric._

  override def output: Seq[Attribute] = left.output ++ right.output

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) s"${condition.get}" else "None"
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
     """.stripMargin
  }

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_LEGACY -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_OP_TIME_LEGACY),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME))

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("This should only be called from columnar")

  protected override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)

    val boundCondition = condition.map(GpuBindReferences.bindGpuReference(_, output,
      allMetrics))

    if (output.isEmpty && boundCondition.isEmpty) {
      // special case for crossJoin.count.  Doing it this way
      // because it is more readable then trying to fit it into the
      // existing join code.
      def getRowCountAndClose(cb: ColumnarBatch): Long = {
        val ret = cb.numRows()
        cb.close()
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        ret
      }

      val l = left.executeColumnar().map(getRowCountAndClose)
      val r = right.executeColumnar().map(getRowCountAndClose)
      // TODO here too it would probably be best to avoid doing any re-computation
      //  that happens with the built in cartesian, but writing another custom RDD
      //  just for this use case is not worth it without an explicit use case.
      GpuBroadcastNestedLoopJoinExecBase.divideIntoBatches(
        l.cartesian(r).map(p => p._1 * p._2),
        targetSizeBytes,
        numOutputRows,
        numOutputBatches)
    } else {
      val numFirstTableColumns = left.output.size
      val sizeEstimateThreshold = RapidsConf.JOIN_GATHERER_SIZE_ESTIMATE_THRESHOLD.get(conf)

      new GpuCartesianRDD(sparkContext,
        boundCondition,
        numFirstTableColumns,
        right.output,
        targetSizeBytes,
        sizeEstimateThreshold,
        opTime,
        joinTime,
        numOutputRows,
        numOutputBatches,
        left.executeColumnar().map(cb => new GpuSerializableBatch(cb)),
        right.executeColumnar().map(cb => new GpuSerializableBatch(cb)))
    }
  }
}
