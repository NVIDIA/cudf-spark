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
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import java.util.Locale

import scala.util.control.NonFatal

import com.nvidia.spark.rapids._

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuInsertIntoHadoopFsRelationCommand, GpuOrcFileFormat}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch


/** GPU version of Spark's CreateHiveTableAsSelectBase */
trait GpuCreateHiveTableAsSelectBase extends GpuRunnableCommand {
  val tableDesc: CatalogTable
  val query: LogicalPlan
  val outputColumnNames: Seq[String]
  val mode: SaveMode

  protected val tableIdentifier: TableIdentifier = tableDesc.identifier

  override def runColumnar(sparkSession: SparkSession, child: SparkPlan): Seq[ColumnarBatch] = {
    val catalog = sparkSession.sessionState.catalog
    val tableExists = catalog.tableExists(tableIdentifier)

    if (tableExists) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableIdentifier has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw RapidsErrorUtils.tableIdentifierExistsError(tableIdentifier)
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      val command = getWritingCommand(catalog, tableDesc, tableExists = true)
      command.runColumnar(sparkSession, child)
      GpuDataWritingCommand.propogateMetrics(sparkSession.sparkContext, command, metrics)
    } else {
      tableDesc.storage.locationUri.foreach { p =>
        GpuDataWritingCommand.assertEmptyRootPath(p, mode, sparkSession.sessionState.newHadoopConf)
      }
      // TODO ideally, we should get the output data ready first and then
      // add the relation into catalog, just in case of failure occurs while data
      // processing.
      val outputColumns = DataWritingCommand.logicalPlanOutputWithNames(query, outputColumnNames)
      val tableSchema = CharVarcharUtilsShims.getRawSchema(
        outputColumns.toStructType, sparkSession.sessionState.conf)
      assert(tableDesc.schema.isEmpty)
      catalog.createTable(
        tableDesc.copy(schema = tableSchema), ignoreIfExists = false)

      try {
        // Read back the metadata of the table which was created just now.
        val createdTableMeta = catalog.getTableMetadata(tableDesc.identifier)
        val command = getWritingCommand(catalog, createdTableMeta, tableExists = false)
        command.runColumnar(sparkSession, child)
        GpuDataWritingCommand.propogateMetrics(sparkSession.sparkContext, command, metrics)
      } catch {
        case NonFatal(e) =>
          // drop the created table.
          catalog.dropTable(tableIdentifier, ignoreIfNotExists = true, purge = false)
          throw e
      }
    }

    Seq.empty[ColumnarBatch]
  }

  // Returns `GpuDataWritingCommand` which actually writes data into the table.
  def getWritingCommand(
      catalog: SessionCatalog,
      tableDesc: CatalogTable,
      tableExists: Boolean): GpuDataWritingCommand

  // A subclass should override this with the Class name of the concrete type expected to be
  // returned from `getWritingCommand`.
  def writingCommandClassName: String

  override def argString(maxFields: Int): String = {
    s"[Database: ${tableDesc.database}, " +
        s"TableName: ${tableDesc.identifier.table}, " +
        s"$writingCommandClassName]"
  }
}

case class GpuOptimizedCreateHiveTableAsSelectCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan,
    outputColumnNames: Seq[String],
    mode: SaveMode,
    cpuCmd: CreateHiveTableAsSelectCommand) extends GpuCreateHiveTableAsSelectBase {
  // Copy from `CreateHiveTableAsSelectCommand.getWritingCommand`, 
  // because it is private in Spark
  private def cpuGetWritingCommand(
      tableDesc: CatalogTable,
      tableExists: Boolean): DataWritingCommand = {
    // For CTAS, there is no static partition values to insert.
    val partition = tableDesc.partitionColumnNames.map(_ -> None).toMap
    InsertIntoHiveTable(
      tableDesc,
      partition,
      query,
      overwrite = false,
      ifPartitionNotExists = false,
      outputColumnNames = outputColumnNames)
  }

  override def getWritingCommand(
      catalog: SessionCatalog,
      tableDesc: CatalogTable,
      tableExists: Boolean): GpuDataWritingCommand = {
    // Leverage the existing support for InsertIntoHadoopFsRelationCommand to do the write
    cpuGetWritingCommand(tableDesc, tableExists) match {
      case c: InsertIntoHadoopFsRelationCommand =>
        val rapidsConf = new RapidsConf(conf)
        val rule = GpuOverrides.dataWriteCmds(c.getClass)
        val meta = new InsertIntoHadoopFsRelationCommandMeta(c, rapidsConf, None, rule)
        meta.tagForGpu()
        if (!meta.canThisBeReplaced) {
          throw new IllegalStateException("Unable to convert writing command: " +
              meta.explain(all = false))
        }
        meta.convertToGpu()
      case c =>
        throw new UnsupportedOperationException(s"Unsupported write command: $c")
    }
  }

  override def writingCommandClassName: String =
    TrampolineUtil.getSimpleName(classOf[GpuInsertIntoHadoopFsRelationCommand])

  // Do not support partitioned or bucketed writes
  override def requireSingleBatch: Boolean = false

  override def child = query
}

final class OptimizedCreateHiveTableAsSelectCommandMeta(
    cmd: CreateHiveTableAsSelectCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RunnableCommandMeta[CreateHiveTableAsSelectCommand](
      cmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    // It would be cleaner if we could simply call `cmd.getWritingCommand` and let
    // InsertIntoHadoopFsRelationCommandMeta tag the result, but calling getWritingCommand
    // before the table exists will crash. So this ends up replicating a portion of the logic
    // from OptimizedCreateHiveTableAsSelectCommand.getWritingCommand and underlying
    // utility methods to be able to tag whether we can support the optimized Hive write.
    val spark = TrampolineConnectShims.getActiveSession
    val tableDesc = cmd.tableDesc

    if (tableDesc.partitionColumnNames.nonEmpty) {
      willNotWorkOnGpu("partitioned writes are not supported")
    }

    val outputColumns =
      DataWritingCommand.logicalPlanOutputWithNames(cmd.query, cmd.outputColumnNames)
    BucketingUtilsShim.tagForHiveBucketingWrite(this, tableDesc.bucketSpec, outputColumns,
      conf.isForceHiveHashForBucketedWrite)

    val serde = tableDesc.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    if (serde.contains("parquet")) {
      val mergeSchemaConfKey = "spark.sql.hive.convertMetastoreParquet.mergeSchema"
      val shouldMergeSchema = SQLConf.get.getConfString(mergeSchemaConfKey, "false").toBoolean
      if (shouldMergeSchema) {
        willNotWorkOnGpu("Merging Parquet schemas across part files is not supported, " +
            s"see $mergeSchemaConfKey")
      }
      val options = tableDesc.properties.filterKeys(isParquetProperty) ++
          tableDesc.storage.properties
      GpuParquetFileFormat.tagGpuSupport(this, spark, options.toMap, cmd.query.schema)
    } else if (serde.contains("orc")) {
      val options = tableDesc.properties.filterKeys(isOrcProperty) ++
          tableDesc.storage.properties
      GpuOrcFileFormat.tagGpuSupport(this, spark, options.toMap, cmd.query.schema)
    } else {
      willNotWorkOnGpu(s"unsupported serde detected: $serde")
    }
  }

  override def convertToGpu(): GpuRunnableCommand = {
    GpuOptimizedCreateHiveTableAsSelectCommand(
      wrapped.tableDesc,
      wrapped.query,
      wrapped.outputColumnNames,
      wrapped.mode,
      cmd)
  }

  // Return true for Apache ORC and Hive ORC-related configuration names.
  // Note that Spark doesn't support configurations like `hive.merge.orcfile.stripe.level`.
  private def isOrcProperty(key: String) =
    key.startsWith("orc.") || key.contains(".orc.")

  private def isParquetProperty(key: String) =
    key.startsWith("parquet.") || key.contains(".parquet.")
}
