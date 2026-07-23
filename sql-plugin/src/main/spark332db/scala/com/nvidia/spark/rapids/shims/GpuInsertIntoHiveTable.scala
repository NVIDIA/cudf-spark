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

package org.apache.spark.sql.hive.rapids.shims

import java.util.Locale

import com.nvidia.spark.rapids.{ColumnarFileFormat, DataFromReplacementRule, DataWritingCommandMeta, GpuDataWritingCommand, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.shims.BucketingUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, ExternalCatalog, ExternalCatalogUtils, ExternalCatalogWithListener}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.hive.rapids.{GpuHiveFileFormat, GpuSaveAsHiveFile, RapidsHiveErrors}
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

final class GpuInsertIntoHiveTableMeta(cmd: InsertIntoHiveTable,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends DataWritingCommandMeta[InsertIntoHiveTable](cmd, conf, parent, rule) {

  private var fileFormat: Option[ColumnarFileFormat] = None

  override def tagSelfForGpuInternal(): Unit = {
    fileFormat = GpuHiveFileFormat.tagGpuSupport(this)
  }

  override def convertToGpu(): GpuDataWritingCommand = {
    val format = fileFormat.getOrElse(
      throw new IllegalStateException("fileFormat missing, tagSelfForGpu not called?"))

    GpuInsertIntoHiveTable(
      table = wrapped.table,
      partition = wrapped.partition,
      fileFormat = format,
      query = wrapped.query,
      overwrite = wrapped.overwrite,
      ifPartitionNotExists = wrapped.ifPartitionNotExists,
      outputColumnNames = wrapped.outputColumnNames,
      tmpLocation = cmd.hiveTmpPath.externalTempPath,
      baseOutputDebugPath = conf.outputDebugDumpPrefix
    )
  }

  def getCannotBeReplacedReasons(): Option[collection.mutable.Set[String]] = cannotBeReplacedReasons
}

case class GpuInsertIntoHiveTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    fileFormat: ColumnarFileFormat,
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String],
    tmpLocation: Path,
    baseOutputDebugPath: Option[String]) extends GpuSaveAsHiveFile {

  /**
   * Inserts all the rows in the table into Hive.  Row objects are properly serialized with the
   * `org.apache.hadoop.hive.serde2.SerDe` and the
   * `org.apache.hadoop.mapred.OutputFormat` provided by the table definition.
   */
  override def runColumnar(sparkSession: SparkSession, child: SparkPlan): Seq[ColumnarBatch] = {
    val externalCatalog = sparkSession.sharedState.externalCatalog
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val hiveQlTable = HiveClientImpl.toHiveTable(table)
    // Have to pass the TableDesc object to RDD.mapPartitions and then instantiate new serializer
    // instances within the closure, since Serializer is not serializable while TableDesc is.
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
      // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
      // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
      // HiveSequenceFileOutputFormat.
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata
    )

    try {
      processInsert(sparkSession,
                    externalCatalog,
                    hadoopConf,
                    tableDesc,
                    fileFormat,
                    tmpLocation,
                    child)
    } finally {
      // Attempt to delete the staging directory and the inclusive files. If failed, the files are
      // expected to be dropped at the normal termination of VM since deleteOnExit is used.
      HiveFileUtil.deleteExternalTmpPath(hadoopConf, tmpLocation)
    }

    // un-cache this table.
    CommandUtilsShim.uncacheTableOrView(sparkSession, table.identifier)
    sparkSession.sessionState.catalog.refreshTable(table.identifier)

    CommandUtils.updateTableStats(sparkSession, table)

    // It would be nice to just return the childRdd unchanged so insert operations could be chained,
    // however for now we return an empty list to simplify compatibility checks with hive, which
    // does not return anything for insert operations.
    // TODO (From Apache Spark): implement hive compatibility as rules.
    Seq.empty[ColumnarBatch]
  }

  private def processInsert(
      sparkSession: SparkSession,
      externalCatalog: ExternalCatalog,
      hadoopConf: Configuration,
      tableDesc: TableDesc,
      fileFormat: ColumnarFileFormat,
      tmpLocation: Path,
      child: SparkPlan): Unit = {

    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val numStaticPartitions = partition.values.count(_.nonEmpty)
    val partitionSpec = partition.map {
      case (key, Some(null)) => key -> ExternalCatalogUtils.DEFAULT_PARTITION_NAME
      case (key, Some(value)) => key -> value
      case (key, None) => key -> ""
    }

    // All partition column names in the format of "<column name 1>/<column name 2>/..."
    val partitionColumns = FileSinkDescShim.getPartitionColumns(tmpLocation, tableDesc)
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    // By this time, the partition map must match the table's partition columns
    if (partitionColumnNames.toSet != partition.keySet) {
      throw RapidsHiveErrors.requestedPartitionsMismatchTablePartitionsError(table, partition)
    }

    // Validate partition spec if there exist any dynamic partitions
    if (numDynamicPartitions > 0) {
      // Report error if dynamic partitioning is not enabled
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw new SparkException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      // Report error if any static partition appears after a dynamic partition
      val isDynamic = partitionColumnNames.map(partitionSpec(_).isEmpty)
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw RapidsErrorUtils.dynamicPartitionParentError
      }
    }

    val partitionAttributes = partitionColumnNames.takeRight(numDynamicPartitions).map { name =>
      val attr = query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse {
        throw RapidsHiveErrors.cannotResolveAttributeError(
          name, query.output.map(_.name).mkString(", "))
      }.asInstanceOf[Attribute]
      // SPARK-28054: Hive metastore is not case preserving and keeps partition columns
      // with lower cased names. Hive will validate the column names in the partition directories
      // during `loadDynamicPartitions`. Spark needs to write partition directories with lower-cased
      // column names in order to make `loadDynamicPartitions` work.
      attr.withName(name.toLowerCase(Locale.ROOT))
    }
    val forceHiveHashForBucketing =
      RapidsConf.FORCE_HIVE_HASH_FOR_BUCKETED_WRITE.get(sparkSession.sessionState.conf)

    val writtenParts = gpuSaveAsHiveFile(
      sparkSession = sparkSession,
      plan = child,
      hadoopConf = hadoopConf,
      fileFormat = fileFormat,
      outputLocation = tmpLocation.toString,
      forceHiveHashForBucketing = forceHiveHashForBucketing,
      partitionAttributes = partitionAttributes,
      bucketSpec = table.bucketSpec,
      options = BucketingUtilsShim.getOptionsWithHiveBucketWrite(table.bucketSpec),
      baseDebugOutputPath = baseOutputDebugPath)

    if (partition.nonEmpty) {
      if (numDynamicPartitions > 0) {
        if (overwrite && table.tableType == CatalogTableType.EXTERNAL) {
          val numWrittenParts = writtenParts.size
          val maxDynamicPartitionsKey = HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
          val maxDynamicPartitions = hadoopConf.getInt(maxDynamicPartitionsKey,
            HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.defaultIntVal)
          if (numWrittenParts > maxDynamicPartitions) {
            throw RapidsHiveErrors.writePartitionExceedConfigSizeWhenDynamicPartitionError(
              numWrittenParts, maxDynamicPartitions, maxDynamicPartitionsKey)
          }
          // SPARK-29295: When insert overwrite to a Hive external table partition, if the
          // partition does not exist, Hive will not check if the external partition directory
          // exists or not before copying files. So if users drop the partition, and then do
          // insert overwrite to the same partition, the partition will have both old and new
          // data. We construct partition path. If the path exists, we delete it manually.
          writtenParts.foreach { partPath =>
            val dpMap = partPath.split("/").map { part =>
              val splitPart = part.split("=")
              assert(splitPart.size == 2, s"Invalid written partition path: $part")
              ExternalCatalogUtils.unescapePathName(splitPart(0)) ->
                ExternalCatalogUtils.unescapePathName(splitPart(1))
            }.toMap

            val caseInsensitiveDpMap = CaseInsensitiveMap(dpMap)

            val updatedPartitionSpec = partition.map {
              case (key, Some(null)) => key -> ExternalCatalogUtils.DEFAULT_PARTITION_NAME
              case (key, Some(value)) => key -> value
              case (key, None) if caseInsensitiveDpMap.contains(key) =>
                key -> caseInsensitiveDpMap(key)
              case (key, _) =>
                throw RapidsHiveErrors.dynamicPartitionKeyNotAmongWrittenPartitionPathsError(
                  key)
            }
            val partitionColumnNames = table.partitionColumnNames
            val tablePath = new Path(table.location)
            val partitionPath = ExternalCatalogUtils.generatePartitionPath(updatedPartitionSpec,
              partitionColumnNames, tablePath)

            val fs = partitionPath.getFileSystem(hadoopConf)
            if (fs.exists(partitionPath)) {
              if (!fs.delete(partitionPath, true)) {
                throw RapidsHiveErrors.cannotRemovePartitionDirError(partitionPath)
              }
            }
          }
        }

        externalCatalog.loadDynamicPartitions(
          db = table.database,
          table = table.identifier.table,
          tmpLocation.toString,
          partitionSpec,
          overwrite,
          numDynamicPartitions)
      } else {
        // scalastyle:off
        // ifNotExists is only valid with static partition, refer to
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
        // scalastyle:on
        val oldPart =
          externalCatalog.getPartitionOption(
            table.database,
            table.identifier.table,
            partitionSpec)

        var doHiveOverwrite = overwrite

        if (oldPart.isEmpty || !ifPartitionNotExists) {
          // SPARK-29295: When insert overwrite to a Hive external table partition, if the
          // partition does not exist, Hive will not check if the external partition directory
          // exists or not before copying files. So if users drop the partition, and then do
          // insert overwrite to the same partition, the partition will have both old and new
          // data. We construct partition path. If the path exists, we delete it manually.
          val partitionPath = if (oldPart.isEmpty && overwrite
              && table.tableType == CatalogTableType.EXTERNAL) {
            val partitionColumnNames = table.partitionColumnNames
            val tablePath = new Path(table.location)
            Some(ExternalCatalogUtils.generatePartitionPath(partitionSpec,
              partitionColumnNames, tablePath))
          } else {
            oldPart.flatMap(_.storage.locationUri.map(uri => new Path(uri)))
          }

          // SPARK-18107: Insert overwrite runs much slower than hive-client.
          // Newer Hive largely improves insert overwrite performance. As Spark uses older Hive
          // version and we may not want to catch up new Hive version every time. We delete the
          // Hive partition first and then load data file into the Hive partition.
          val hiveVersion = externalCatalog.asInstanceOf[ExternalCatalogWithListener]
            .unwrapped.asInstanceOf[HiveExternalCatalog]
            .client
            .version
          // SPARK-31684:
          // For Hive 2.0.0 and onwards, as https://issues.apache.org/jira/browse/HIVE-11940
          // has been fixed, and there is no performance issue anymore. We should leave the
          // overwrite logic to hive to avoid failure in `FileSystem#checkPath` when the table
          // and partition locations do not belong to the same `FileSystem`
          // TODO(SPARK-31675): For Hive 2.2.0 and earlier, if the table and partition locations
          // do not belong together, we will still get the same error thrown by hive encryption
          // check. see https://issues.apache.org/jira/browse/HIVE-14380.
          // So we still disable for Hive overwrite for Hive 1.x for better performance because
          // the partition and table are on the same cluster in most cases.
          if (partitionPath.nonEmpty && overwrite && hiveVersion.fullVersion < "2.0") {
            partitionPath.foreach { path =>
              val fs = path.getFileSystem(hadoopConf)
              if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                  throw RapidsHiveErrors.cannotRemovePartitionDirError(path)
                }
                // Don't let Hive do overwrite operation since it is slower. But still give a
                // chance to forcely override this for some customized cases when this
                // operation is optimized.
                doHiveOverwrite = hadoopConf.getBoolean("hive.movetask.enable.dir.move", false)
              }
            }
          }

          // inheritTableSpecs is set to true. It should be set to false for an IMPORT query
          // which is currently considered as a Hive native command.
          val inheritTableSpecs = true
          externalCatalog.loadPartition(
            table.database,
            table.identifier.table,
            tmpLocation.toString,
            partitionSpec,
            isOverwrite = doHiveOverwrite,
            inheritTableSpecs = inheritTableSpecs,
            isSrcLocal = false)
        }
      }
    } else {
      externalCatalog.loadTable(
        table.database,
        table.identifier.table,
        tmpLocation.toString, // TODO (From Apache Spark): URI
        overwrite,
        isSrcLocal = false)
    }
  }

  override def requireSingleBatch: Boolean = false // TODO: Re-evaluate. If partitioned or bucketed?
}
