/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.hive.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, DataWritingCommandMeta, GpuDataWritingCommand, GpuOverrides, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.shims.{BucketingUtilsShim, GpuCreateHiveTableAsSelectBase}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.rapids.execution.TrampolineUtil

final class GpuCreateHiveTableAsSelectCommandMeta(cmd: CreateHiveTableAsSelectCommand,
                                                  conf: RapidsConf,
                                                  parent: Option[RapidsMeta[_,_,_]],
                                                  rule: DataFromReplacementRule)
  extends DataWritingCommandMeta[CreateHiveTableAsSelectCommand](cmd, conf, parent, rule) {

    private var cpuWritingCommand: Option[InsertIntoHiveTable] = None

    override def tagSelfForGpuInternal(): Unit = {

      val spark = SparkSession.active
      val tableDesc = cmd.tableDesc // For the *new* table.

      if (tableDesc.partitionColumnNames.nonEmpty) {
        willNotWorkOnGpu("partitioned writes are not supported")
      }

      BucketingUtilsShim.tagForHiveBucketingWrite(this, tableDesc.bucketSpec,
        cmd.outputColumns, conf.isForceHiveHashForBucketedWrite)

      val catalog = spark.sessionState.catalog
      val tableExists = catalog.tableExists(tableDesc.identifier)
      cpuWritingCommand = cmd.getWritingCommand(catalog, tableDesc, tableExists) match {
        case insertToHiveTable: InsertIntoHiveTable => Some(insertToHiveTable)
        case c =>
          willNotWorkOnGpu("Unsupported write command " + c)
          None
      }

      if (cpuWritingCommand.isDefined) {
        val insertMeta = new GpuInsertIntoHiveTableMeta(
          cpuWritingCommand.get,
          conf,
          None,
          GpuOverrides.dataWriteCmds(cpuWritingCommand.get.getClass))

        insertMeta.tagForGpu()
        if (!insertMeta.canThisBeReplaced) {
          insertMeta.getCannotBeReplacedReasons().get.foreach( reason => {
            willNotWorkOnGpu(reason)
          })
        }
      }
    }

    override def convertToGpu(): GpuDataWritingCommand = GpuCreateHiveTableAsSelectCommand(
      tableDesc = wrapped.tableDesc,
      query = wrapped.query,
      outputColumnNames = wrapped.outputColumnNames,
      mode = wrapped.mode,
      cpuCmd = wrapped
    )
}

case class GpuCreateHiveTableAsSelectCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    outputColumnNames: Seq[String],
    mode: SaveMode,
    cpuCmd: CreateHiveTableAsSelectCommand) extends GpuCreateHiveTableAsSelectBase {
  override def getWritingCommand(
      catalog: SessionCatalog,
      tableDesc: CatalogTable,
      tableExists: Boolean): GpuDataWritingCommand = {
    // Leverage GpuInsertIntoHiveTable to do the write
    cpuCmd.getWritingCommand(catalog, tableDesc, tableExists) match {
      case c: InsertIntoHiveTable =>
        val rapidsConf = new RapidsConf(conf)
        val rule = GpuOverrides.dataWriteCmds(c.getClass)
        val meta = new GpuInsertIntoHiveTableMeta(c, rapidsConf, None, rule)
        meta.tagForGpu() // Initializes the cpuWritingCommand.
        // Note: The notion that the GpuInsertIntoHiveTable could have been constructed
        // ahead of time, in [[GpuCreateHiveTableAsSelectCommandMeta.tagSelfForGpu()]]
        // is illusory. The `tableDesc` available earlier is from *before* the Hive
        // table was created. As such, it is incomplete. For instance, it might not
        // contain table location information.
        // The [[tableDesc]] argument to getWritingCommand() is the one constructed
        // *after* table creation. So the GpuInsertIntoHiveTable needs to be constructed
        // at runtime, i.e. in the runColumnar() path, i.e. here in getWritingCommand().
        meta.convertToGpu()
      case c =>
        // Assertion failure, really. The writing command has to be `InsertIntoHiveTable`,
        // as per checks above, in `tagSelfForGpu()`.
        throw new UnsupportedOperationException(s"Unsupported write command: $c")
    }
  }

  override def writingCommandClassName: String =
    TrampolineUtil.getSimpleName(classOf[GpuInsertIntoHiveTable])

  // Do not support partitioned or bucketed writes
  override def requireSingleBatch: Boolean = false
}
