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
package org.apache.spark.sql.rapids.execution

import java.lang.{Boolean => JBoolean}
import java.util.{Collections, IdentityHashMap}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ENSURE_REQUIREMENTS, ReusedExchangeExec}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Each consumer reads reducer zero, containing all mapper shards, through GPU shuffle.
 * The saved broadcast is not a child: preparing this plan must not collect its payload at
 * the driver. It retains AQE stage metadata; a standard AQE consumer resolves its payload
 * through the original exchange. Non-AQE execution does not invoke the saved exchange.
 */
case class GpuDriverlessBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    @transient fallbackBroadcast: GpuBroadcastExchangeExec)
    extends GpuBroadcastHashJoinExecBase(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right, false) {

  import GpuMetric._

  override def broadcastExchange: GpuBroadcastExchangeExec = fallbackBroadcast

  override def stringArgs: Iterator[Any] =
    Iterator(leftKeys, rightKeys, joinType, buildSide, condition)

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(UnspecifiedDistribution, UnspecifiedDistribution)

  override lazy val additionalMetrics: Map[String, GpuMetric] = broadcastJoinMetrics ++ Map(
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    "executorBuilds" -> createMetric(ESSENTIAL_LEVEL, "executor broadcast builds")) ++
    (if (conf.adaptiveExecutionEnabled) Map(
      "buildStageTime" -> fallbackBroadcast.gpuLongMetric("driverlessStageTime"),
      "buildStageBytes" -> fallbackBroadcast.gpuLongMetric("driverlessBuildBytes"))
    else Map.empty[String, GpuMetric])

  override protected def doColumnarBroadcastJoin(): RDD[ColumnarBatch] = {
    val exchange = buildPlan match {
      case shuffle: GpuShuffleExchangeExec => shuffle
      case ReusedExchangeExec(_, shuffle: GpuShuffleExchangeExec) => shuffle
      case _ => throw new IllegalStateException("Expected a direct GPU build shuffle")
    }
    // Both upstreams belong to this RDD graph. The scheduler can submit independent map
    // stages together; constructing this plan must not wait for the build map stage.
    val build = buildPlan.executeColumnar()
    val stream = streamedPlan.executeColumnar()
    val shuffleId = exchange.shuffleId
    val rapidsConf = new RapidsConf(conf)
    val maxNodes = rapidsConf.driverlessBroadcastMaxNodes
    val schema = buildPlan.schema
    val output = buildPlan.output
    val localMetrics = allMetrics
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME_LEGACY)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val builds = gpuLongMetric("executorBuilds")
    val joinOptions = RapidsConf.getJoinOptions(conf, RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf))
    val codecConfigs = TableCompressionCodec.makeCodecConfig(rapidsConf)
    val maxDecompressBatchMemory = rapidsConf.shuffleCompressionMaxBatchMemory
    new GpuDriverlessBroadcastJoinRDD[ColumnarBatch, ColumnarBatch, ColumnarBatch](
      stream, build, (input, openBuild) => {
        val context = TaskContext.get()
        val streamIter = new CollectTimeIterator(
          NvtxRegistry.BROADCAST_JOIN_STREAM, input, streamTime)
        val built = GpuShuffleEnv.getBroadcastCache.getOrBuild[ColumnarBatch](shuffleId, () => {
          DriverlessBroadcastPolicy.checkExecutorLimit(maxNodes)
          builds += 1
          val batch = GpuDriverlessBroadcastHelper.assemble(
            openBuild(), schema, output, localMetrics, codecConfigs, maxDecompressBatchMemory)
          closeOnExcept(batch)(
            SpillableColumnarBatch(_, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
        }, () => GpuSemaphore.releaseIfNecessary(context),
          () => context.killTaskIfInterrupted(),
          () => GpuSemaphore.acquireIfNecessary(context),
          generation = ExecutorBroadcastCache.generation(
            context.stageId(), context.stageAttemptNumber()))(
          GpuDriverlessBroadcastHelper.borrow, batch => withResource(batch)(_ => ()))
        doJoin(built, streamIter, joinOptions, numOutputRows, numOutputBatches,
          opTime, joinTime)
      })
  }
}

object GpuDriverlessBroadcastHashJoinExec extends Logging {
  def gpuBuildReadRejections(conf: RapidsConf): Seq[String] =
    DriverlessBroadcastPolicy.rejections(conf) ++
      (if (!GpuShuffleEnv.useGPUShuffle(conf)) Seq("GPU shuffle is unavailable") else Seq.empty)

  private[execution] def planRejections(plan: SparkPlan, conf: RapidsConf): Seq[String] = {
    gpuBuildReadRejections(conf) ++ Seq(
      // ContextCleaner unregisters build shuffles and retires executor cache entries.
      if (SparkSessionUtils.sessionFromPlan(plan).sparkContext.cleaner.isEmpty) {
        Some("Spark context cleaner is disabled")
      } else None,
      if (conf.get(RapidsConf.HASH_TABLE_REUSE)) Some("hash-table reuse is enabled") else None
    ).flatten
  }

  private[execution] def joinRejections(join: GpuBroadcastHashJoinExec): Seq[String] = Seq(
    // The custom RDD does not execute the base join's empty/all-null NAAJ handling.
    if (join.isNullAwareAntiJoin) Some("null-aware anti join") else None,
    if (join.isSkewJoin) Some("skew-optimized broadcast join") else None,
    if (join.exists(_.expressions.exists(_.exists(_.isInstanceOf[ExecSubqueryExpression])))) {
      Some("join contains a subquery")
    } else None
  ).flatten

  /** Broadcast consumers other than eligible GPU joins require a real payload, not a token. */
  private[execution] def sharedBuilds(plan: SparkPlan): Seq[SparkPlan] = {
    val blocked = ArrayBuffer.empty[SparkPlan]
    val normal = Collections.newSetFromMap(
      new IdentityHashMap[SparkPlan, JBoolean]())
    val subqueries = Collections.newSetFromMap(
      new IdentityHashMap[SparkPlan, JBoolean]())
    val builds = Collections.newSetFromMap(
      new IdentityHashMap[SparkPlan, JBoolean]())
    def canonical(build: SparkPlan): Unit = build match {
      case exchange: GpuBroadcastExchangeExec => blocked += exchange.cpuCanonical.canonicalized
      case exchange: BroadcastExchangeExec =>
        blocked += exchange.canonicalized
      case stage: BroadcastQueryStageExec => canonical(stage.plan)
      case reused: ReusedExchangeExec => canonical(reused.child)
      case _ =>
    }
    def visit(node: SparkPlan, inSubquery: Boolean, gpuBuild: Boolean = false): Unit = {
      val seen = if (inSubquery) subqueries else if (gpuBuild) builds else normal
      if (seen.add(node)) {
        if (inSubquery || !gpuBuild) canonical(node)
        node match {
          case join: GpuBroadcastHashJoinExec if joinRejections(join).isEmpty =>
            val (build, stream) = join.buildSide match {
              case GpuBuildLeft => (join.left, join.right)
              case GpuBuildRight => (join.right, join.left)
            }
            visit(build, inSubquery, gpuBuild = true)
            visit(stream, inSubquery)
          case stage: QueryStageExec => visit(stage.plan, inSubquery, gpuBuild)
          case reused: ReusedExchangeExec => visit(reused.child, inSubquery, gpuBuild)
          case _ => node.children.foreach(visit(_, inSubquery))
        }
        node.expressions.foreach(_.foreach {
          case subquery: ExecSubqueryExpression => visit(subquery.plan, inSubquery = true)
          case _ =>
        })
      }
    }
    visit(plan, inSubquery = false)
    blocked.toSeq
  }

  /** Preserve the standard path for unsupported environments and exchange wrappers. */
  def rewrite(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    if (!conf.isDriverlessBroadcastEnabled) {
      plan
    } else {
      val reasons = planRejections(plan, conf)
      val shared = sharedBuilds(plan)
      if (reasons.nonEmpty) {
        logInfo(s"Driverless broadcast not selected: ${reasons.mkString("; ")}")
        plan
      } else {
        var sawBroadcastJoin = false
        val rewritten = plan.transformUp {
          case join: GpuBroadcastHashJoinExec =>
            sawBroadcastJoin = true
            val build = join.buildSide match {
              case GpuBuildLeft => join.left
              case GpuBuildRight => join.right
            }
            val broadcast = build match {
              case direct: GpuBroadcastExchangeExec if !plan.conf.adaptiveExecutionEnabled =>
                Some(direct)
              case stage: BroadcastQueryStageExec => stage.plan match {
                case direct: GpuBroadcastExchangeExec
                    if direct.admittedDriverlessShuffle.isDefined => Some(direct)
                case ReusedExchangeExec(_, direct: GpuBroadcastExchangeExec)
                    if direct.admittedDriverlessShuffle.isDefined => Some(direct)
                case _ => None
              }
              case _ => None
            }
            val rejection = joinRejections(join).headOption.orElse {
              broadcast match {
                case Some(exchange)
                    if shared.exists(_.fastEquals(exchange.cpuCanonical.canonicalized)) =>
                  Some("broadcast has a reused or subquery consumer")
                case Some(exchange) =>
                  DriverlessBroadcastBuildAdmission.rejectionReason(exchange.child)
                case None => Some(s"no driverless build stage: ${build.nodeName}")
              }
            }
            rejection match {
              case Some(reason) =>
                logInfo(s"Driverless broadcast not selected for join ${join.id}: $reason")
                join
              case None =>
                val exchange = broadcast.get
                val shuffle = exchange.admittedDriverlessShuffle.getOrElse {
                  GpuShuffleExchangeExec(GpuSinglePartitioning, exchange.child,
                    ENSURE_REQUIREMENTS)(GpuSinglePartitioning)
                }
                // AQE reuse changes expression IDs without changing the build's column order.
                // Keep that consumer mapping when replacing its broadcast stage with a shuffle.
                val gpuBuild = build match {
                  case stage: BroadcastQueryStageExec
                      if stage.plan.isInstanceOf[ReusedExchangeExec] =>
                    ReusedExchangeExec(stage.output, shuffle)
                  case _ => shuffle
                }
                val (left, right) = join.buildSide match {
                  case GpuBuildLeft => (gpuBuild, join.right)
                  case GpuBuildRight => (join.left, gpuBuild)
                }
                logInfo(s"Driverless broadcast planned for join ${join.id}")
                GpuDriverlessBroadcastHashJoinExec(join.leftKeys, join.rightKeys, join.joinType,
                  join.buildSide, join.condition, left, right, exchange)
            }
        }
        if (!sawBroadcastJoin) {
          logInfo("Driverless broadcast not selected: no GPU broadcast hash join in this plan")
        }
        rewritten
      }
    }
  }
}
