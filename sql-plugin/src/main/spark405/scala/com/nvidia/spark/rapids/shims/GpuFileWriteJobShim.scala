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
{"spark":["405","421"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.GpuDataWritingCommandExec

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{
  CommandExecutionMode,
  SQLExecution
}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.rapids.GpuInsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.rapids.shims.QueryExecutionShims

object GpuFileWriteJobShim {
  val shouldDelayOverwriteDeletion: Boolean = true

  def setBeforeWriteJob(
      command: InsertIntoHadoopFsRelationCommand,
      callback: () => Unit): Unit = {
    command.setTagValue(InsertIntoHadoopFsRelationCommand.BEFORE_WRITE_JOB, callback)
  }

  def copyBeforeWriteJob(
      from: InsertIntoHadoopFsRelationCommand,
      to: GpuInsertIntoHadoopFsRelationCommand): Unit = {
    from.getTagValue(InsertIntoHadoopFsRelationCommand.BEFORE_WRITE_JOB)
      .foreach { callback =>
        to.setTagValue(InsertIntoHadoopFsRelationCommand.BEFORE_WRITE_JOB, callback)
      }
  }

  def runBeforeWriteJob(command: GpuInsertIntoHadoopFsRelationCommand): Unit = {
    command.getTagValue(InsertIntoHadoopFsRelationCommand.BEFORE_WRITE_JOB).foreach(_())
  }

  def executeWriteCommand(
      sparkSession: SparkSession,
      command: InsertIntoHadoopFsRelationCommand,
      beforeWrite: () => Unit): Unit = {
    val qe = sparkSession.sessionState.executePlan(command, CommandExecutionMode.SKIP)
    QueryExecutionShims.withInternalError("Eagerly executed command failed.") {
      SQLExecution.withNewExecutionId(qe, Some("command")) {
        val runsCommand = qe.executedPlan.exists {
          case DataWritingCommandExec(c, _) =>
            c.getTagValue(InsertIntoHadoopFsRelationCommand.BEFORE_WRITE_JOB).isDefined
          case GpuDataWritingCommandExec(c: GpuInsertIntoHadoopFsRelationCommand, _) =>
            c.getTagValue(InsertIntoHadoopFsRelationCommand.BEFORE_WRITE_JOB).isDefined
          case _ => false
        }
        if (!runsCommand) {
          beforeWrite()
        }
        qe.executedPlan.executeCollect()
      }
    }
  }
}
