/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.rapids.shims

import com.databricks.sql.transaction.tahoe.perf.DeltaOptimizedWritePartitioning
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.{GpuBoundReference, GpuOverrides, RapidsConf,
  SparkQueryCompareTestSuite, SparkSessionHolder}
import com.nvidia.spark.rapids.shims.GpuHashPartitioning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.execution.{LeafExecNode, RangeExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveRepartitioningStatus
import org.apache.spark.sql.execution.exchange.{DELTA_OPTIMIZED_WRITE, ShuffleExchangeExec}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuDeltaOptimizedWriteShuffleSuite extends SparkQueryCompareTestSuite {

  private case class EmptyColumnarLeaf(
      override val output: Seq[Attribute],
      rdd: RDD[ColumnarBatch]) extends LeafExecNode {
    override def supportsColumnar: Boolean = true

    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("EmptyColumnarLeaf only supports columnar")

    override protected def doExecuteColumnar(): RDD[ColumnarBatch] = rdd
  }

  private def newExchange(
      inputPartitions: Int,
      overridingSQLConfs: Map[String, String]):
      (GpuShuffleExchangeExec, DeltaOptimizedWritePartitioning) = {
    val attr = AttributeReference("k", IntegerType)()
    val gpuPartitioning = GpuHashPartitioning(
      Seq(GpuBoundReference(0, IntegerType, nullable = true)(attr.exprId, attr.name)),
      numPartitions = 0)
    val target = new DeltaOptimizedWritePartitioning(Seq(attr), overridingSQLConfs)
    val rdd = SparkSessionHolder.sparkSession.sparkContext
      .parallelize(0 until inputPartitions, inputPartitions)
      .mapPartitions(_ => Iterator.empty[ColumnarBatch])
    val child = EmptyColumnarLeaf(Seq(attr), rdd)
    (GpuShuffleExchangeExec(
      gpuPartitioning,
      child,
      DELTA_OPTIMIZED_WRITE)(target), target)
  }

  private val markerOverrides = Map(
    DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS.key -> "40",
    DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS.key -> "7")

  test("production shuffle meta preserves the Delta target marker during GPU conversion") {
    // DBR's ShuffleExchangeExec constructor reads SparkEnv eagerly.
    val _ = SparkSessionHolder.sparkSession.sparkContext
    val range = RangeExec(Range(0L, 10L, 1L, Some(10)))
    val target = new DeltaOptimizedWritePartitioning(range.output, markerOverrides)
    val cpuExchange = ShuffleExchangeExec(
      target,
      range,
      DELTA_OPTIMIZED_WRITE)

    assert(cpuExchange.targetOutputPartitioning eq target)
    assert(cpuExchange.outputPartitioning == target.getPhysicalPartitioning)
    assert(cpuExchange.outputPartitioning.numPartitions == 0)

    val meta = GpuOverrides.wrapAndTagPlan(
      cpuExchange,
      new RapidsConf(Map.empty[String, String]))
    assert(meta.canThisBeReplaced, meta.explain(false))
    val converted = meta.convertIfNeeded() match {
      case exchange: GpuShuffleExchangeExec => exchange
      case other => fail(s"Expected GpuShuffleExchangeExec, found ${other.getClass.getName}")
    }

    assert(converted.shuffleOrigin == DELTA_OPTIMIZED_WRITE)
    assert(converted.targetOutputPartitioning eq target)
    assert(converted.outputPartitioning == cpuExchange.outputPartitioning)
    assert(converted.gpuOutputPartitioning.numPartitions == 0)
    val convertedTarget = converted.targetOutputPartitioning
      .asInstanceOf[DeltaOptimizedWritePartitioning]
    assert(convertedTarget.overridingSQLConfs == markerOverrides)
    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS.key -> "1000",
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS.key -> "2") {
      assert(convertedTarget.createDynamicPhysicalPartitioning(10).numPartitions == 4)
    }
  }

  test("Delta optimized write keeps target and physical output distinct through tree copies") {
    val (exchange, target) = newExchange(inputPartitions = 10, markerOverrides)

    assert(exchange.targetOutputPartitioning eq target)
    assert(exchange.outputPartitioning == target.getPhysicalPartitioning)
    assert(exchange.outputPartitioning.numPartitions == 0)

    val replacementChild = EmptyColumnarLeaf(
      exchange.child.output,
      SparkSessionHolder.sparkSession.sparkContext.emptyRDD[ColumnarBatch])
    val copied = exchange.withNewChildren(Seq(replacementChild))
      .asInstanceOf[GpuShuffleExchangeExec]
    assert(copied ne exchange)
    assert(copied.child eq replacementChild)
    assert(copied.targetOutputPartitioning eq target)
    assert(copied.outputPartitioning == target.getPhysicalPartitioning)
  }

  test("Delta optimized write uses marker overrides for its dynamic shuffle count") {
    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_SHUFFLE_BLOCKS.key -> "1000",
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_MAX_SHUFFLE_PARTITIONS.key -> "2") {
      val (exchange, target) = newExchange(inputPartitions = 10, markerOverrides)

      assert(target.createDynamicPhysicalPartitioning(10).numPartitions == 4)
      assert(exchange.executeColumnar().partitions.length == 4)
    }
  }

  test("AQE resize keeps target, output, GPU partitioning, and dependency in sync") {
    val (exchange, _) = newExchange(inputPartitions = 10, markerOverrides)

    val resized = exchange.withNewNumPartitions(6).asInstanceOf[GpuShuffleExchangeExec]
    assert(resized.targetOutputPartitioning.numPartitions == 6)
    assert(resized.outputPartitioning.numPartitions == 6)
    assert(resized.gpuOutputPartitioning.numPartitions == 6)
    assert(resized.executeColumnar().partitions.length == 6)

    val repartitioned = exchange.repartition(
      5,
      AdaptiveRepartitioningStatus.DEFAULT_STATUS).asInstanceOf[GpuShuffleExchangeExec]
    assert(repartitioned.targetOutputPartitioning.numPartitions == 5)
    assert(repartitioned.outputPartitioning.numPartitions == 5)
    assert(repartitioned.gpuOutputPartitioning.numPartitions == 5)
    assert(repartitioned.executeColumnar().partitions.length == 5)
  }
}
