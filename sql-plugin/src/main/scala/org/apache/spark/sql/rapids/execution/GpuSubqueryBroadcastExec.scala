/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import java.util.concurrent.{Future => JFuture}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuColumnarToRowExec, GpuExec, GpuMetric, RapidsConf, RapidsMeta, SparkPlanMeta, TargetSize}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric.{COLLECT_TIME, DESCRIPTION_COLLECT_TIME, ESSENTIAL_LEVEL}
import com.nvidia.spark.rapids.shims.{ShimBaseSubqueryExec, ShimUnaryExecNode, SparkShimImpl}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Cast, Expression, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{HashedRelationBroadcastMode, HashJoin}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils


abstract class GpuSubqueryBroadcastMetaBase(
    s: SubqueryBroadcastExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends
    SparkPlanMeta[SubqueryBroadcastExec](s, conf, p, r) {

  protected var broadcastBuilder: () => SparkPlan = _

  override val childExprs: Seq[BaseExprMeta[_]] = Nil

  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] = Nil

  override def tagPlanForGpu(): Unit = s.child match {

    // For AQE off:
    //
    // The rule PlanDynamicPruningFilters will insert SubqueryBroadcast if there exists
    // available broadcast exchange for reuse. The plan stack of SubqueryBroadcast:
    //
    // +- SubqueryBroadcast
    //    +- BroadcastExchange (can be reused)
    //       +- [executed subquery...]
    //
    // Since the GPU overrides rule has been applied on executedSubQuery, if the
    // executedSubQuery can be replaced by GPU overrides, the plan stack becomes:
    //
    // +- SubqueryBroadcast
    //    +- BroadcastExchange
    //       +- GpuColumnarToRow
    //          +- [GPU overrides of executed subquery...]
    //
    // To reuse BroadcastExchange on the GPU, we shall transform above pattern into:
    //
    // +- GpuSubqueryBroadcast
    //    +- GpuBroadcastExchange (can be reused)
    //       +- [GPU overrides of executed subquery...]
    //
    case ex @ BroadcastExchangeExec(_, c2r: GpuColumnarToRowExec) =>
      val exMeta = new GpuBroadcastMeta(ex.copy(child = c2r.child), conf, p, r)
      exMeta.tagForGpu()
      if (exMeta.canThisBeReplaced) {
        broadcastBuilder = () => exMeta.convertToGpu()
      } else {
        willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
      }

    // For AQE on:
    //
    // In Spark 320+, DPP can cooperate with AQE. The insertion of SubqueryBroadcast is
    // similar with non-AQE circumstance. During the creation of AdaptiveSparkPlan, the
    // rule PlanAdaptiveSubqueries insert an intermediate plan SubqueryAdaptiveBroadcast to
    // preserve the initial physical plan of DPP subquery filters. During the optimization,
    // the rule PlanAdaptiveDynamicPruningFilters inserts the SubqueryBroadcast as the parent
    // of adaptive subqueries:
    //
    // +- SubqueryBroadcast
    //    +- AdaptiveSparkPlan (supportColumnar=false)
    //    +- == Initial Plan ==
    //       BroadcastExchange
    //       +- [executed subquery...]
    //
    // Since AdaptiveSparkPlan can be explicitly set as a columnar plan from Spark 320+,
    // we can simply build GpuSubqueryBroadcast on the base of columnar adaptive plans
    // whose root plan are GpuBroadcastExchange:
    //
    // +- GpuSubqueryBroadcast
    //    +- AdaptiveSparkPlan (supportColumnar=true)
    //    +- == Final Plan ==
    //       BroadcastQueryStage
    //       +- GpuBroadcastExchange (can be reused)
    //          +- [GPU overrides of executed subquery...]
    //
    case a: AdaptiveSparkPlanExec =>
      SparkShimImpl.getAdaptiveInputPlan(a) match {
        case ex: BroadcastExchangeExec =>
          val exMeta = new GpuBroadcastMeta(ex, conf, p, r)
          exMeta.tagForGpu()
          if (exMeta.canThisBeReplaced) {
            broadcastBuilder = () =>
              SparkShimImpl.columnarAdaptivePlan(
                a, TargetSize(conf.gpuTargetBatchSizeBytes))
          } else {
            willNotWorkOnGpu("underlying BroadcastExchange can not run in the GPU.")
          }

        case unexpected =>
          throw new AssertionError("Unexpected child exec in AdaptiveSparkPlan: " +
            s"${unexpected.getClass.getName}")
      }

    case _ =>
      willNotWorkOnGpu("the subquery to broadcast can not entirely run in the GPU.")
  }

  /**
   * Simply returns the original plan. Because its only child, BroadcastExchange, doesn't
   * need to change if SubqueryBroadcastExec falls back to the CPU.
   */
  override def convertToCpu(): SparkPlan = s

  /** Extract the broadcast mode key expressions if there are any. */
  protected def getBroadcastModeKeyExprs: Option[Seq[Expression]] = {
    val broadcastMode = s.child match {
      case b: BroadcastExchangeExec =>
        b.mode
      case a: AdaptiveSparkPlanExec =>
        SparkShimImpl.getAdaptiveInputPlan(a) match {
          case b: BroadcastExchangeExec =>
            b.mode
          case _ =>
            throw new AssertionError("should not reach here")
        }
    }

    broadcastMode match {
      case HashedRelationBroadcastMode(keys, _) => Some(keys)
      case IdentityBroadcastMode => None
      case m => throw new UnsupportedOperationException(s"Unknown broadcast mode $m")
    }
  }
}


case class GpuSubqueryBroadcastExec(
    name: String,
    indices: Seq[Int],
    buildKeys: Seq[Expression],
    child: SparkPlan)(modeKeys: Option[Seq[Expression]])
    extends ShimBaseSubqueryExec with GpuExec with ShimUnaryExecNode {

  override def otherCopyArgs: Seq[AnyRef] = modeKeys :: Nil

  // As `SubqueryBroadcastExec`, `GpuSubqueryBroadcastExec` is only used with `InSubqueryExec`.
  // No one would reference this output, so the exprId doesn't matter here. But it's important to
  // correctly report the output length, so that `InSubqueryExec` can know it's the single-column
  // execution mode, not multi-column.
  override def output: Seq[Attribute] = {
    indices.map { index =>
      val key = buildKeys(index)
      val name = key match {
        case n: NamedExpression =>
          n.name
        case cast: Cast if cast.child.isInstanceOf[NamedExpression] =>
          cast.child.asInstanceOf[NamedExpression].name
        case _ =>
          "key"
      }
      AttributeReference(name, key.dataType, key.nullable)()
    }
  }

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL, "data size"),
    COLLECT_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_COLLECT_TIME))

  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    GpuSubqueryBroadcastExec("dpp", indices, keys, child.canonicalized)(modeKeys)
  }

  @transient
  private lazy val relationFuture: JFuture[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)

    SQLExecution.withThreadLocalCaptured[Array[InternalRow]](
        session, GpuSubqueryBroadcastExec.executionContext) {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(session, executionId) {
        val broadcastBatch = child.executeBroadcast[Any]()
        val result: Array[InternalRow] = broadcastBatch.value match {
          case b: SerializeConcatHostBuffersDeserializeBatch =>  projectSerializedBatchToRows(b)
          case b if SparkShimImpl.isEmptyRelation(b) => Array.empty
          case b => throw new IllegalStateException(s"Unexpected broadcast type: ${b.getClass}")
        }

        result
      }
    }
  }

  private def projectSerializedBatchToRows(
      serBatch: SerializeConcatHostBuffersDeserializeBatch): Array[InternalRow] = {
    val beforeCollect = System.nanoTime()

    // Creates projection to extract target field from Row, as what Spark does.
    // Note that unlike Spark, the GPU broadcast data has not applied the key expressions from
    // the HashedRelation, so that is applied here if necessary to ensure the proper values
    // are being extracted. The CPU already has the key projections applied in the broadcast
    // data and thus does not have similar logic here.
    val broadcastModeProject = modeKeys.map { keyExprs =>
      val exprs = if (GpuHashJoin.canRewriteAsLongType(buildKeys)) {
        // in this case, there is only 1 key expression since it's a packed version that encompasses
        // multiple integral values into a single long using bit logic. In CPU Spark, the broadcast
        // would create a LongHashedRelation instead of a standard HashedRelation.
        indices.map { _ => keyExprs.head }
      } else {
        indices.map { idx => keyExprs(idx) }
      }
      UnsafeProjection.create(exprs)
    }

    val rowExprs = if (GpuHashJoin.canRewriteAsLongType(buildKeys)) {
      // Since this is the expected output for a LongHashedRelation, we can extract the key from the
      // long packed key using bit logic, using this method available in HashJoin to give us the
      // correct key expression.
      indices.map { idx => HashJoin.extractKeyExprAt(buildKeys, idx) }
    } else {
      indices.map { idx =>
        // Use the single output of the broadcast mode projection if it exists
        val rowProjectIndex = if (broadcastModeProject.isDefined) 0 else idx
        BoundReference(rowProjectIndex, buildKeys(idx).dataType, buildKeys(idx).nullable)
      }
    }
    val rowProject = UnsafeProjection.create(rowExprs)

    // Deserializes the batch on the host. Then, transforms it to rows and performs row-wise
    // projection. We should NOT run any device operation on the driver node.
    val result = withResource(serBatch.hostBatch) { hostBatch =>
      hostBatch.rowIterator().asScala.map { row =>
        val broadcastRow = broadcastModeProject.map(_(row)).getOrElse(row)
        rowProject(broadcastRow).copy().asInstanceOf[InternalRow]
      }.toArray // force evaluation so we don't close hostBatch too soon
    }

    gpuLongMetric("dataSize") += serBatch.dataSize
    gpuLongMetric(COLLECT_TIME) += System.nanoTime() - beforeCollect

    result
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuSubqueryBroadcastExec does not support the execute() code path.")
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
        s" mismatch:\n$this")
  }
}

object GpuSubqueryBroadcastExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("dynamicpruning",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
