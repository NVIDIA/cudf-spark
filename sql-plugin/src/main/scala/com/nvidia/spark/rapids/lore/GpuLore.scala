/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuDataWritingCommandExec, GpuExec, RapidsConf, ShimLoader}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, ReusedSubqueryExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuCustomShuffleReaderExec}
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.SerializableConfiguration

case class LoreRDDMeta(numPartitions: Int, outputPartitions: Seq[Int], attrs: Seq[Attribute])

case class LoreRDDPartitionMeta(numBatches: Int, dataType: Seq[DataType])

trait GpuLoreRDD {
  def rootPath: Path

  def pathOfMeta: Path = new Path(rootPath, "rdd.meta")

  def pathOfPartition(partitionIndex: Int): Path = {
    new Path(rootPath, s"partition-$partitionIndex")
  }

  def pathOfPartitionMeta(partitionIndex: Int): Path = {
    new Path(pathOfPartition(partitionIndex), "partition.meta")
  }

  def pathOfBatch(partitionIndex: Int, batchIndex: Int): Path = {
    new Path(pathOfPartition(partitionIndex), s"batch-$batchIndex.parquet")
  }
}

object GpuLore extends Logging {
  /**
   * Lore id of a plan node.
   */
  val LORE_ID_TAG: TreeNodeTag[String] = new TreeNodeTag[String]("rapids.gpu.lore.id")
  /**
   * When a [[GpuExec]] node has this tag, it means that this node is a root node whose meta and
   * input should be dumped.
   */
  val LORE_DUMP_PATH_TAG: TreeNodeTag[String] = new TreeNodeTag[String]("rapids.gpu.lore.dump.path")
  /**
   * When a [[GpuExec]] node has this tag, it means that this node is a child node whose data
   * should be dumped.
   */
  val LORE_DUMP_RDD_TAG: TreeNodeTag[LoreDumpRDDInfo] = new TreeNodeTag[LoreDumpRDDInfo](
    "rapids.gpu.lore.dump.rdd.info")

  private type TagRollback = () => Unit

  private def registerTag[T](plan: SparkPlan, tag: TreeNodeTag[T], value: T,
      tracker: mutable.ArrayBuffer[TagRollback]): Unit = {
    plan.setTagValue(tag, value)
    tracker += (() => plan.unsetTagValue(tag))
  }

  def pathOfRootPlanMeta(rootPath: Path): Path = {
    new Path(rootPath, "plan.meta")
  }

  def dumpPlan[T <: SparkPlan : ClassTag](plan: T, rootPath: Path): Unit = {
    dumpObject(plan, pathOfRootPlanMeta(rootPath),
      SparkSessionUtils.sessionFromPlan(plan).sparkContext.hadoopConfiguration)
  }

  def dumpObject[T: ClassTag](obj: T, path: Path, hadoopConf: Configuration): Unit = {
    val fs = path.getFileSystem(hadoopConf)
    withResource(fs.create(path, true)) { fout =>
      val serializerStream = SparkEnv.get.serializer.newInstance().serializeStream(fout)
      withResource(serializerStream) { ser =>
        ser.writeObject(obj)
      }
    }
  }

  def loadObject[T: ClassTag](path: Path, hadoopConf: Configuration): T = {
    val fs = path.getFileSystem(hadoopConf)
    withResource(fs.open(path)) { fin =>
      val serializerStream = SparkEnv.get.serializer.newInstance().deserializeStream(fin)
      withResource(serializerStream) { ser =>
        ser.readObject().asInstanceOf[T]
      }
    }
  }

  def pathOfChild(rootPath: Path, childIndex: Int): Path = {
    new Path(rootPath, s"input-$childIndex")
  }

  def restoreGpuExec(rootPath: Path, spark: SparkSession): GpuExec = {
    val rootExec = loadObject[SparkPlan](pathOfRootPlanMeta(rootPath),
      spark.sparkContext.hadoopConfiguration)

    checkUnsupportedOperator(rootExec)

    val broadcastHadoopConf = {
      val sc = spark.sparkContext
      sc.broadcast(new SerializableConfiguration(spark.sparkContext.hadoopConfiguration))
    }

    rootExec match {
      case gpuExec: GpuExec =>
        // Handle regular GpuExec (existing logic)
        restoreGpuExecInternal(gpuExec, rootPath, broadcastHadoopConf)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported plan type for restoration: ${rootExec.getClass.getSimpleName}")
    }
  }

  private def restoreGpuExecInternal(rootExec: GpuExec, rootPath: Path,
      broadcastHadoopConf: Broadcast[SerializableConfiguration]): GpuExec = {
    // Load children
    val newChildren = rootExec.children.zipWithIndex.map { case (plan, idx) =>
      val newChild = GpuLoreReplayExec(idx, rootPath.toString, broadcastHadoopConf)
      plan match {
        case b: GpuBroadcastExchangeExec =>
          b.withNewChildren(Seq(newChild))
        case b: BroadcastQueryStageExec =>
          b.broadcast.withNewChildren(Seq(newChild))
        case _ => newChild
      }
    }

    var nextId = rootExec.children.length

    rootExec.transformExpressionsUp {
      case sub: ExecSubqueryExpression =>
        val newSub = restoreSubqueryPlan(nextId, sub, rootPath, broadcastHadoopConf)
        nextId += 1
        newSub
    }.withNewChildren(newChildren).asInstanceOf[GpuExec]
  }

  private def restoreSubqueryPlan(id: Int, sub: ExecSubqueryExpression,
      rootPath: Path, hadoopConf: Broadcast[SerializableConfiguration]): ExecSubqueryExpression = {
    val innerPlan = sub.plan.child

    if (innerPlan.isInstanceOf[GpuExec]) {
      var newChild: SparkPlan = GpuLoreReplayExec(id, rootPath.toString, hadoopConf)

      if (!innerPlan.supportsColumnar) {
        newChild = GpuColumnarToRowExec(newChild)
      }
      val newSubqueryExec = sub.plan match {
        case ReusedSubqueryExec(subqueryExec) => subqueryExec.withNewChildren(Seq(newChild))
          .asInstanceOf[BaseSubqueryExec]
        case p: BaseSubqueryExec => p.withNewChildren(Seq(newChild))
          .asInstanceOf[BaseSubqueryExec]
      }
      sub.withNewPlan(newSubqueryExec)
    } else {
      throw new IllegalArgumentException(s"Subquery plan ${innerPlan.getClass.getSimpleName} " +
        s"is not a GpuExec")
    }
  }

  /**
   * Lore id generator. Key is [[SQLExecution.EXECUTION_ID_KEY]].
   */
  private val idGen: ConcurrentMap[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]()

  private def nextLoreIdOf(plan: SparkPlan): Option[Int] = {
    // When the execution id is not set, it means there is no actual execution happening, in this
    // case we don't need to generate lore id.
    Option(SparkSessionUtils.sessionFromPlan(plan)
      .sparkContext
      .getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map { executionId =>
        idGen.computeIfAbsent(executionId, _ => new AtomicInteger(0)).getAndIncrement()
      }
  }
  /**
   * Executions that have checked the lore output root path.
   * Key is [[SQLExecution.EXECUTION_ID_KEY]].
   */
  private val loreOutputRootPathChecked: ConcurrentHashMap[String, Boolean] =
    new ConcurrentHashMap[String, Boolean]()

  def tagForLore(sparkPlan: SparkPlan, rapidsConf: RapidsConf): SparkPlan = {
    val loreDumpIds = rapidsConf.loreDumpIds
    val allowNonStrictMode = rapidsConf.loreDumpNonStrictMode

    val newPlan = if (loreDumpIds.nonEmpty) {
      // We need to dump the output of nodes with the lore id in the dump ids
      val loreOutputRootPath = rapidsConf.loreDumpPath.getOrElse(throw
        new IllegalArgumentException(s"${RapidsConf.LORE_DUMP_PATH.key} must be set " +
          s"when ${RapidsConf.LORE_DUMP_IDS.key} is set."))

      val spark = SparkSessionUtils.sessionFromPlan(sparkPlan)

      Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).foreach {
        executionId =>
        loreOutputRootPathChecked.computeIfAbsent(executionId, _ => {
          val path = new Path(loreOutputRootPath)
          val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
          if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
            val msg =
              s"LORE dump path $loreOutputRootPath already exists and is not empty."
            if (allowNonStrictMode) {
              logWarning(s"$msg Continuing because non-strict mode is enabled.")
            } else {
              throw new IllegalArgumentException(msg)
            }
          }
          true
        })
      }

      val hadoopConf = {
        val sc = spark.sparkContext
        sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))
      }

      val subqueries = mutable.Set.empty[SparkPlan]

      sparkPlan.foreachUp {
        case g: GpuExec =>
          nextLoreIdOf(g).foreach { loreId =>
            g.setTagValue(LORE_ID_TAG, loreId.toString)

            loreDumpIds.get(loreId).foreach { outputLoreIds =>
              val tagRollbacks = mutable.ArrayBuffer.empty[TagRollback]
              val addedSubqueries = mutable.ArrayBuffer.empty[SparkPlan]
              try {
                checkUnsupportedOperator(g)
                val currentExecRootPath = new Path(loreOutputRootPath, s"loreId-$loreId")
                registerTag(g, LORE_DUMP_PATH_TAG, currentExecRootPath.toString, tagRollbacks)
                val loreOutputInfo = LoreOutputInfo(outputLoreIds,
                  currentExecRootPath.toString)

                g.children.zipWithIndex.foreach {
                  case (child, idx) =>
                    val dumpRDDInfo = LoreDumpRDDInfo(idx, loreOutputInfo, child.output, hadoopConf,
                      useOriginalSchemaNames = rapidsConf.loreParquetUseOriginalNames,
                      nonStrictMode = allowNonStrictMode)
                    child match {
                      case c: BroadcastQueryStageExec =>
                        registerTag(c.broadcast, LORE_DUMP_RDD_TAG, dumpRDDInfo, tagRollbacks)
                      case o =>
                        registerTag(o, LORE_DUMP_RDD_TAG, dumpRDDInfo, tagRollbacks)
                    }
                }

                var nextId = g.children.length
                g.transformExpressionsUp {
                  case sub: ExecSubqueryExpression =>
                    if (spark.sessionState.conf.subqueryReuseEnabled) {
                      val canonicalPlan = sub.plan.canonicalized
                      if (!subqueries.contains(canonicalPlan)) {
                        subqueries += canonicalPlan
                        addedSubqueries += canonicalPlan
                      } else {
                        throw new IllegalArgumentException("Subquery reuse is enabled, and we " +
                          "found a duplicated subquery, which is currently not supported by LORE.")
                      }
                    }
                    tagSubqueryPlan(nextId, sub, loreOutputInfo, hadoopConf, tagRollbacks,
                      allowNonStrictMode)
                    nextId += 1
                    sub
                }
              } catch {
                case NonFatal(e) if allowNonStrictMode =>
                  tagRollbacks.reverseIterator.foreach(_.apply())
                  addedSubqueries.foreach(subqueries -= _)
                  logWarning(
                    s"Skipping LORE dump for loreId $loreId on ${g.nodeName}: ${e.getMessage}", e)
              }
            }
          }
        case _ =>
      }

      sparkPlan

    } else {
      // We don't need to dump the output of the nodes, just tag the lore id
      sparkPlan.foreachUp {
        case g: GpuExec =>
          nextLoreIdOf(g).foreach { loreId =>
            g.setTagValue(LORE_ID_TAG, loreId.toString)
          }
        case _ =>
      }

      sparkPlan
    }

    newPlan
  }

  def loreIdOf(node: SparkPlan): Option[String] = {
    node.getTagValue(LORE_ID_TAG)
  }

  private def tagSubqueryPlan(id: Int, sub: ExecSubqueryExpression,
      loreOutputInfo: LoreOutputInfo, hadoopConf: Broadcast[SerializableConfiguration],
      tagRollbacks: mutable.ArrayBuffer[TagRollback], nonStrictMode: Boolean) = {
    val innerPlan = sub.plan.child
    if (innerPlan.isInstanceOf[GpuExec]) {
      val dumpRDDInfo = LoreDumpRDDInfo(id, loreOutputInfo, innerPlan.output,
        hadoopConf,
        useOriginalSchemaNames = RapidsConf.LORE_PARQUET_USE_ORIGINAL_NAMES
          .get(SparkSessionUtils.sessionFromPlan(innerPlan).sessionState.conf),
        nonStrictMode = nonStrictMode)
      innerPlan match {
        case p: GpuColumnarToRowExec =>
          registerTag(p.child, LORE_DUMP_RDD_TAG, dumpRDDInfo, tagRollbacks)
        case c =>
          registerTag(c, LORE_DUMP_RDD_TAG, dumpRDDInfo, tagRollbacks)
      }
    } else {
      throw new IllegalArgumentException(s"Subquery plan ${innerPlan.getClass.getSimpleName} " +
        s"is not a GpuExec")
    }
  }

  private def checkUnsupportedOperator(plan: SparkPlan): Unit = {
    plan match {
      // Allow GpuDataWritingCommandExec
      case _: GpuDataWritingCommandExec =>
        checkGpuDataWritingCommandSupportedVersion()
      case _ =>
        if (plan.children.isEmpty ||
          plan.isInstanceOf[GpuCustomShuffleReaderExec]
        ) {
          throw new UnsupportedOperationException(s"Currently we don't support dumping input of " +
            s"${plan.getClass.getSimpleName} operator.")
        }
    }
  }

  /**
   * Disable LORE dumping when the current shim enables GPU WriteFiles.
   */
  private def checkGpuDataWritingCommandSupportedVersion(): Unit = {
    if (SparkShimImpl.hasGpuWriteFiles) {
      val currentShimVersion = ShimLoader.getShimVersion
      throw new UnsupportedOperationException(
        s"LORE dump is not supported for GpuDataWritingCommandExec on Spark" +
          s" version $currentShimVersion because GPU WriteFiles is enabled on this shim.")
    }
  }
}
