/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of ExecutedCommandExec.
 *
 * This class is essentially identical to ExecutedCommandExec but marked with GpuExec so
 * it's clear this is replacing a CPU operation with a GPU operation. The GPU operation
 * is not performed directly here, rather it is the underlying command that will ultimately
 * execute on the GPU.
 */
case class GpuExecutedCommandExec(cmd: RunnableCommand) extends LeafExecNode with GpuExec {
  override def supportsColumnar: Boolean = false

  override lazy val allMetrics: Map[String, GpuMetric] = GpuMetric.wrap(cmd.metrics)

  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    cmd.run(SparkSession.getActiveSession.orNull).map(converter(_).asInstanceOf[InternalRow])
  }

  override def innerChildren: Seq[QueryPlan[_]] = cmd :: Nil

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "GpuExecute " + cmd.nodeName

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeToIterator(): Iterator[InternalRow] = sideEffectResult.toIterator

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  override def executeTail(limit: Int): Array[InternalRow] = {
    sideEffectResult.takeRight(limit).toArray
  }

  protected override def doExecute(): RDD[InternalRow] = {
    sparkContext.parallelize(sideEffectResult, 1)
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
        s" mismatch:\n$this")
  }
}

class ExecutedCommandExecMeta(
    ece: ExecutedCommandExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[ExecutedCommandExec](ece, conf, parent, rule) {

  override val childRunnableCmds: Seq[RunnableCommandMeta[RunnableCommand]] =
    Seq(GpuOverrides.wrapRunnableCmd(ece.cmd, conf, Some(this)))

  /**
   * We don't want to spam the user with messages about these operators
   * if they are not expected to be replaced.
   */
  override def suppressWillWorkOnGpuInfo: Boolean = {
    childRunnableCmds.head.isInstanceOf[RuleNotFoundRunnableCommandMeta[_]]
  }

  override def tagPlanForGpu(): Unit = {
    // nothing to do, all tagging is in the child command
  }

  override def convertToGpu(): GpuExec = {
    GpuExecutedCommandExec(childRunnableCmds.head.convertToGpu())
  }
}
