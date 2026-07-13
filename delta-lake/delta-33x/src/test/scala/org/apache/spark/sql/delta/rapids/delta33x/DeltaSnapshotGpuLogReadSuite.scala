/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */
package org.apache.spark.sql.delta.rapids.delta33x

import java.nio.file.Files
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import java.util.{Base64, Random}

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils

import com.nvidia.spark.rapids.GpuColumnVector

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{col, lit, struct, typedLit, when}
import org.apache.spark.sql.delta.{DeltaLog, DeltaLogFileIndex, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, SetTransaction}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{MapType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.vectorized.ColumnVector

/** Standalone configuration-only experiment for GPU reads during cold Snapshot construction. */
object DeltaSnapshotGpuLogReadSuite {
  private case class ManualHostResult(
      rows: Long,
      batches: Long,
      addRows: Long,
      removeRows: Long,
      txnRows: Long,
      protocolRows: Long,
      metadataRows: Long,
      pathHash: Long,
      payloadBytes: Long,
      mapEntries: Long,
      copyNs: Long,
      accessNs: Long) {
    def +(other: ManualHostResult): ManualHostResult = ManualHostResult(
      rows + other.rows,
      batches + other.batches,
      addRows + other.addRows,
      removeRows + other.removeRows,
      txnRows + other.txnRows,
      protocolRows + other.protocolRows,
      metadataRows + other.metadataRows,
      pathHash + other.pathHash,
      payloadBytes + other.payloadBytes,
      mapEntries + other.mapEntries,
      copyNs + other.copyNs,
      accessNs + other.accessNs)
  }

  private val EmptyManualHostResult = ManualHostResult(
    rows = 0L,
    batches = 0L,
    addRows = 0L,
    removeRows = 0L,
    txnRows = 0L,
    protocolRows = 0L,
    metadataRows = 0L,
    pathHash = 0L,
    payloadBytes = 0L,
    mapEntries = 0L,
    copyNs = 0L,
    accessNs = 0L)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.rapids.sql.detectDeltaLogQueries", "false")
      .config("spark.rapids.sql.explain", "ALL")
      .config("spark.rapids.sql.metrics.level", "DEBUG")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.shuffle.partitions", "2").getOrCreate()
    val tableDir = Files.createTempDirectory("delta-gpu-checkpoint").toFile
    val plans = new CopyOnWriteArrayList[String]()
    val runLabel = new AtomicReference[String]("setup")
    val listener = new QueryExecutionListener {
      override def onSuccess(name: String, qe: QueryExecution, durationNs: Long): Unit = {
        val metrics = qe.executedPlan.collect {
          case plan if plan.metrics.nonEmpty => s"${plan.nodeName}:" + plan.metrics.toSeq
            .sortBy(_._1).map { case (metric, value) => s"$metric=${value.value}" }.mkString(",")
        }.mkString("\n")
        plans.add(s"RUN: ${runLabel.get()} FUNCTION: $name DURATION_NS: $durationNs\n" +
          s"ANALYZED:\n${qe.analyzed.treeString}\nOPTIMIZED:\n${qe.optimizedPlan.treeString}\n" +
          s"PHYSICAL:\n${qe.executedPlan.treeString}\nMETRICS:\n$metrics\n")
      }
      override def onFailure(name: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      val path = tableDir.getCanonicalPath
      val actionCount = args.headOption.map(_.toInt).getOrElse(20000)
      val tagPayloadBytes = args.lift(1).map(_.toInt).getOrElse(4096)
      val checkpointParts = args.lift(2).map(_.toInt).getOrElse(1)
      val scanOnly = args.lift(3).contains("scanOnly")
      val removeCount = args.lift(4).map(_.toInt).getOrElse(math.max(1, actionCount / 10))
      if (checkpointParts > 1) {
        val checkpointRowsForPartSizing = actionCount + removeCount + 4
        val partSize = math.ceil(checkpointRowsForPartSizing.toDouble / checkpointParts).toLong
        spark.conf.set("spark.databricks.delta.checkpoint.partSize", partSize)
      }
      spark.conf.set("spark.rapids.sql.enabled", false)
      spark.range(1).write.format("delta").save(path)
      val random = new Random(0L)
      val bytes = new Array[Byte](tagPayloadBytes)
      def nextPayload(): String = {
        random.nextBytes(bytes)
        Base64.getEncoder.encodeToString(bytes)
      }
      val activeAdds: Seq[Action] = (0 until actionCount).map { index =>
        AddFile(
          f"synthetic/part-$index%08d.parquet",
          Map.empty,
          size = 1L,
          modificationTime = index.toLong,
          dataChange = false,
          stats = "{\"numRecords\":1}",
          tags = Map("benchmarkPayload" -> nextPayload()))
      }
      val removedAdds = (0 until removeCount).map { index =>
        AddFile(
          f"synthetic/removed-$index%08d.parquet",
          Map.empty,
          size = 1L,
          modificationTime = (actionCount + index).toLong,
          dataChange = false,
          stats = "{\"numRecords\":1}",
          tags = Map("benchmarkPayload" -> nextPayload()))
      }
      val removedActions: Seq[Action] = removedAdds.flatMap { add =>
        Seq[Action](add, add.removeWithTimestamp(dataChange = false))
      }
      val actions = activeAdds ++ removedActions ++ Seq[Action](
        SetTransaction("delta-snapshot-gpu-log-read", version = 1L,
          lastUpdated = Some(System.currentTimeMillis())))
      val warmLog = DeltaLog.forTable(spark, path)
      warmLog.startTransaction(catalogTableOpt = None).commit(actions, DeltaOperations.ManualUpdate)
      warmLog.checkpoint(warmLog.update())
      warmLog.startTransaction(catalogTableOpt = None).commit(Seq(AddFile(
        "synthetic/trailing.parquet", Map.empty, 1L, actionCount.toLong,
        dataChange = false, stats = "{\"numRecords\":1}")), DeltaOperations.ManualUpdate)
      val logDir = new org.apache.hadoop.fs.Path(path, "_delta_log")
      val checkpointBytes = logDir.getFileSystem(spark.sessionState.newHadoopConf()).listStatus(logDir)
        .filter(_.getPath.getName.contains("checkpoint")).map(_.getLen).sum
      println(s"CHECKPOINT actionCount=$actionCount payloadBytes=$tagPayloadBytes " +
        s"parts=$checkpointParts removeTombstones=$removeCount fileBytes=$checkpointBytes")
      spark.conf.set("spark.rapids.sql.enabled", true)

      def compactMetrics(plan: org.apache.spark.sql.execution.SparkPlan): String = {
        val metricNames = Seq(
          "numFiles",
          "filesSize",
          "readBufferSize",
          "numOutputRows",
          "numOutputBatches",
          "numInputBatches",
          "scanTime",
          "gpuDecodeTime",
          "readFsTime",
          "writeBufferTime",
          "bufferTime",
          "opTimeNew",
          "opTimeLegacy",
          "streamTime",
          "metadataTime",
          "numPartitions")
        plan.collect {
          case node if node.metrics.nonEmpty =>
            val values = metricNames.flatMap { metric =>
              node.metrics.get(metric).map(value => metric + "=" + value.value)
            }
            if (values.nonEmpty) {
              Some(node.nodeName + "{" + values.mkString(",") + "}")
            } else {
              None
            }
        }.flatten.mkString("; ")
      }

      val ignoredTopLevelActionFields = Set("commitInfo", "cdc", "checkpointMetadata", "sidecar")
      val ignoredClassicRemoveFields = Set("tags", "stats")

      def pruneActionSchema(
          topLevelIgnored: Set[String] = ignoredTopLevelActionFields,
          addIgnored: Set[String] = Set.empty,
          removeIgnored: Set[String] = Set.empty): StructType = {
        StructType(Action.logSchema.fields.filterNot { field =>
          topLevelIgnored.contains(field.name)
        }.map {
          case field if field.name == "add" && addIgnored.nonEmpty =>
            val addSchema = field.dataType.asInstanceOf[StructType]
            field.copy(dataType = StructType(addSchema.fields.filterNot { nested =>
              addIgnored.contains(nested.name)
            }))
          case field if field.name == "remove" && removeIgnored.nonEmpty =>
            val removeSchema = field.dataType.asInstanceOf[StructType]
            field.copy(dataType = StructType(removeSchema.fields.filterNot { nested =>
              removeIgnored.contains(nested.name)
            }))
          case field => field
        })
      }

      val topLevelDroppedSchema = pruneActionSchema()
      val classicPhysicalCheckpointSchema = pruneActionSchema(
        removeIgnored = ignoredClassicRemoveFields)
      val noAddTagsCheckpointSchema = pruneActionSchema(
        addIgnored = Set("tags"),
        removeIgnored = ignoredClassicRemoveFields)
      val noAddPayloadCheckpointSchema = pruneActionSchema(
        addIgnored = Set("tags", "stats"),
        removeIgnored = ignoredClassicRemoveFields)
      val protocolMetadataCheckpointSchema = Action.logSchema(Set("protocol", "metaData", "commitInfo"))
      val protocolMetadataClassicCheckpointSchema = Action.logSchema(Set("protocol", "metaData"))

      def dropMapFields(schema: StructType): StructType = StructType(schema.fields.flatMap {
        case field if field.dataType.isInstanceOf[MapType] => None
        case field if field.dataType.isInstanceOf[StructType] =>
          Some(field.copy(dataType = dropMapFields(field.dataType.asInstanceOf[StructType])))
        case field => Some(field)
      })

      // Upper-bound experiment only: the production Delta commit schema contains nested maps,
      // which the GPU JSON file reader cannot currently decode.
      val mapFreeCommitSchema = dropMapFields(classicPhysicalCheckpointSchema)

      def missingSchemaFields(
          expected: StructType,
          actual: StructType,
          prefix: String = ""): Seq[String] = {
        val actualByName = actual.fields.map(field => field.name -> field).toMap
        expected.fields.flatMap { expectedField =>
          val fieldPath =
            if (prefix.isEmpty) expectedField.name else s"$prefix.${expectedField.name}"
          actualByName.get(expectedField.name) match {
            case None => Seq(fieldPath)
            case Some(actualField) =>
              (expectedField.dataType, actualField.dataType) match {
                case (expectedStruct: StructType, actualStruct: StructType) =>
                  missingSchemaFields(expectedStruct, actualStruct, fieldPath)
                case _ => Seq.empty
              }
          }
        }
      }

      Seq(
        "full-action-log-schema" -> Action.logSchema,
        "top-level-dropped-schema" -> topLevelDroppedSchema,
        "classic-physical-checkpoint-schema" -> classicPhysicalCheckpointSchema,
        "no-add-tags-checkpoint-schema" -> noAddTagsCheckpointSchema,
        "no-add-payload-checkpoint-schema" -> noAddPayloadCheckpointSchema,
        "protocol-metadata-checkpoint-schema" -> protocolMetadataCheckpointSchema,
        "protocol-metadata-classic-checkpoint-schema" -> protocolMetadataClassicCheckpointSchema
      ).foreach { case (label, schema) =>
        val missing = missingSchemaFields(Action.logSchema, schema)
        println(s"SCHEMA_DIFF $label missingFromFull=${missing.mkString("[", ",", "]")}")
        println(s"SCHEMA_TREE $label\n${schema.treeString}")
      }

      def expandToFullActionSchema(
          df: DataFrame,
          emptyMissingAddPartitionValues: Boolean = false): DataFrame = {
        def projectField(
            expectedField: StructField,
            actualField: Option[StructField],
            fieldPath: String,
            topLevelName: String): Column = {
          actualField match {
            case None if emptyMissingAddPartitionValues && topLevelName == "add" &&
                fieldPath == "add.partitionValues" =>
              typedLit(Map.empty[String, String]).cast(expectedField.dataType)
                .as(expectedField.name)
            case None =>
              lit(null).cast(expectedField.dataType).as(expectedField.name)
            case Some(actual) =>
              (expectedField.dataType, actual.dataType) match {
                case (expectedStruct: StructType, actualStruct: StructType) =>
                  val actualNestedByName =
                    actualStruct.fields.map(field => field.name -> field).toMap
                  val nestedColumns = expectedStruct.fields.map { nestedField =>
                    projectField(nestedField, actualNestedByName.get(nestedField.name),
                      fieldPath + "." + nestedField.name, topLevelName)
                  }
                  when(col(fieldPath).isNull, lit(null).cast(expectedStruct))
                    .otherwise(struct(nestedColumns: _*))
                    .as(expectedField.name)
                case _ =>
                  col(fieldPath).as(expectedField.name)
              }
          }
        }

        val actualByName = df.schema.fields.map(field => field.name -> field).toMap
        df.select(Action.logSchema.fields.map { field =>
          projectField(field, actualByName.get(field.name), field.name, field.name)
        }: _*)
      }

      def snapshotWithCheckpointSchema(
          log: DeltaLog,
          baseSnapshot: Snapshot,
          checkpointReadSchema: StructType,
          permissiveMapFreeGpuJson: Boolean): Snapshot = {
        new Snapshot(baseSnapshot.path, baseSnapshot.version, baseSnapshot.logSegment, log,
          baseSnapshot.checksumOpt) {
          override protected def loadActions: DataFrame = {
            val checkpointActions = checkpointProvider.allActionsFileIndexes().map { index =>
              expandToFullActionSchema(deltaLog.loadIndex(index, checkpointReadSchema))
            }
            val deltaActions = deltaFileIndexOpt.map { index =>
              if (permissiveMapFreeGpuJson) {
                val options = Map(
                  "mode" -> "PERMISSIVE",
                  "ignoreCorruptFiles" -> "false",
                  "ignoreMissingFiles" -> "false")
                val relation = HadoopFsRelation(index, index.partitionSchema,
                  mapFreeCommitSchema, None, index.format, options)(spark)
                expandToFullActionSchema(
                  Dataset.ofRows(spark, LogicalRelation(relation)),
                  emptyMissingAddPartitionValues = true)
              } else {
                deltaLog.loadIndex(index)
              }
            }.toSeq
            (checkpointActions ++ deltaActions).reduceOption { (left, right) =>
              left.unionByName(right, allowMissingColumns = true)
            }.getOrElse(emptyDF).withColumn(Snapshot.ADD_STATS_TO_USE_COL_NAME, col("add.stats"))
          }
        }
      }

      def checkpointRelation(log: DeltaLog, checkpointIndex: DeltaLogFileIndex, pruneIgnoredActions: Boolean): DataFrame = {
        val full = log.loadIndex(checkpointIndex)
        if (!pruneIgnoredActions) {
          full
        } else {
          val ignored = Set("commitInfo", "cdc", "checkpointMetadata", "sidecar")
          val projected = full.schema.fields.map { field =>
            if (ignored.contains(field.name)) {
              lit(null).cast(field.dataType).as(field.name)
            } else {
              col(field.name)
            }
          }
          full.select(projected: _*)
        }
      }

      def checkpointColumnarScan(label: String, pruneIgnoredActions: Boolean = false): Long = {
        DeltaLog.clearCache()
        spark.sharedState.cacheManager.clearCache()
        runLabel.set(label)
        spark.conf.set("spark.rapids.sql.detectDeltaLogQueries", false)
        val log = DeltaLog.forTable(spark, path)
        val snapshot = log.update()
        val checkpointIndex = snapshot.logSegment.checkpointProvider.topLevelFileIndex.get
        val checkpointPlan = checkpointRelation(log, checkpointIndex, pruneIgnoredActions).queryExecution.executedPlan
        assert(checkpointPlan.nodeName == "GpuColumnarToRow", checkpointPlan.treeString)
        val columnarPlan = checkpointPlan.children.head
        assert(columnarPlan.supportsColumnar, checkpointPlan.treeString)
        println(s"CHECKPOINT_COLUMNAR_PLAN $label\n${checkpointPlan.treeString}")
        val start = System.nanoTime()
        val rows = columnarPlan.executeColumnar().mapPartitions { batches =>
          var totalRows = 0L
          batches.foreach { batch =>
            try {
              totalRows += batch.numRows()
            } finally {
              batch.close()
            }
          }
          Iterator.single(totalRows)
        }.sum()
        val elapsed = System.nanoTime() - start
        println("CHECKPOINT_COLUMNAR_SCAN " + label + " ROWS " + rows + " ELAPSED_NS " + elapsed)
        println("CHECKPOINT_COLUMNAR_METRICS " + label + " " + compactMetrics(columnarPlan))
        elapsed
      }

      def checkpointDirectColumnarScan(
          label: String,
          schemaLabel: String,
          readSchema: StructType): Long = {
        DeltaLog.clearCache()
        spark.sharedState.cacheManager.clearCache()
        runLabel.set(label)
        spark.conf.set("spark.rapids.sql.detectDeltaLogQueries", false)
        val log = DeltaLog.forTable(spark, path)
        val snapshot = log.update()
        val checkpointIndex = snapshot.logSegment.checkpointProvider.topLevelFileIndex.get
        val checkpointPlan = log.loadIndex(checkpointIndex, readSchema).queryExecution.executedPlan
        assert(checkpointPlan.nodeName == "GpuColumnarToRow", checkpointPlan.treeString)
        val columnarPlan = checkpointPlan.children.head
        assert(columnarPlan.supportsColumnar, checkpointPlan.treeString)
        println(s"CHECKPOINT_DIRECT_COLUMNAR_PLAN $label schema=$schemaLabel\n" +
          checkpointPlan.treeString)
        val start = System.nanoTime()
        val rows = columnarPlan.executeColumnar().mapPartitions { batches =>
          var totalRows = 0L
          batches.foreach { batch =>
            try {
              totalRows += batch.numRows()
            } finally {
              batch.close()
            }
          }
          Iterator.single(totalRows)
        }.sum()
        val elapsed = System.nanoTime() - start
        println("CHECKPOINT_DIRECT_COLUMNAR_SCAN " + label + " SCHEMA " + schemaLabel +
          " ROWS " + rows + " ELAPSED_NS " + elapsed)
        println("CHECKPOINT_DIRECT_COLUMNAR_METRICS " + label + " " +
          compactMetrics(columnarPlan))
        elapsed
      }

      def checkpointDirectScan(
          label: String,
          schemaLabel: String,
          readSchema: StructType,
          gpu: Boolean = true): Long = {
        DeltaLog.clearCache()
        spark.sharedState.cacheManager.clearCache()
        runLabel.set(label)
        spark.conf.set("spark.rapids.sql.detectDeltaLogQueries", !gpu)
        val log = DeltaLog.forTable(spark, path)
        val snapshot = log.update()
        val checkpointIndex = snapshot.logSegment.checkpointProvider.topLevelFileIndex.get
        val checkpointPlan = log.loadIndex(checkpointIndex, readSchema).queryExecution.executedPlan
        println(s"CHECKPOINT_DIRECT_SCAN_PLAN $label schema=$schemaLabel\n" +
          checkpointPlan.treeString)
        val start = System.nanoTime()
        val rows = checkpointPlan.execute().count()
        val elapsed = System.nanoTime() - start
        println(s"CHECKPOINT_DIRECT_SCAN $label ENGINE ${if (gpu) "gpu" else "cpu"} " +
          s"SCHEMA $schemaLabel ROWS $rows ELAPSED_NS $elapsed")
        println(s"CHECKPOINT_DIRECT_SCAN_METRICS $label ${compactMetrics(checkpointPlan)}")
        elapsed
      }

      def checkpointManualHostMaterialize(
          label: String,
          schemaLabel: String,
          readSchema: StructType,
          readAddPayload: Boolean): Long = {
        DeltaLog.clearCache()
        spark.sharedState.cacheManager.clearCache()
        runLabel.set(label)
        spark.conf.set("spark.rapids.sql.detectDeltaLogQueries", false)
        val topLevelIndexes = readSchema.fields.map(_.name).zipWithIndex.toMap
        def nestedIndexes(topLevel: String): Map[String, Int] = {
          topLevelIndexes.get(topLevel).map { index =>
            readSchema.fields(index).dataType.asInstanceOf[StructType]
              .fields.map(_.name).zipWithIndex.toMap
          }.getOrElse(Map.empty[String, Int])
        }
        val addIdx = topLevelIndexes.getOrElse("add", -1)
        val removeIdx = topLevelIndexes.getOrElse("remove", -1)
        val txnIdx = topLevelIndexes.getOrElse("txn", -1)
        val protocolIdx = topLevelIndexes.getOrElse("protocol", -1)
        val metadataIdx = topLevelIndexes.getOrElse("metaData", -1)
        val addIndexes = nestedIndexes("add")
        val removeIndexes = nestedIndexes("remove")
        val txnIndexes = nestedIndexes("txn")
        val protocolIndexes = nestedIndexes("protocol")
        val metadataIndexes = nestedIndexes("metaData")
        val addPathIdx = addIndexes.getOrElse("path", -1)
        val addPartitionValuesIdx = addIndexes.getOrElse("partitionValues", -1)
        val addSizeIdx = addIndexes.getOrElse("size", -1)
        val addModificationTimeIdx = addIndexes.getOrElse("modificationTime", -1)
        val addDataChangeIdx = addIndexes.getOrElse("dataChange", -1)
        val addStatsIdx = addIndexes.getOrElse("stats", -1)
        val addTagsIdx = addIndexes.getOrElse("tags", -1)
        val removePathIdx = removeIndexes.getOrElse("path", -1)
        val removeDeletionTimestampIdx = removeIndexes.getOrElse("deletionTimestamp", -1)
        val removeDataChangeIdx = removeIndexes.getOrElse("dataChange", -1)
        val txnAppIdIdx = txnIndexes.getOrElse("appId", -1)
        val txnVersionIdx = txnIndexes.getOrElse("version", -1)
        val protocolMinReaderVersionIdx = protocolIndexes.getOrElse("minReaderVersion", -1)
        val protocolMinWriterVersionIdx = protocolIndexes.getOrElse("minWriterVersion", -1)
        val metadataIdIdx = metadataIndexes.getOrElse("id", -1)
        val log = DeltaLog.forTable(spark, path)
        val snapshot = log.update()
        val checkpointIndex = snapshot.logSegment.checkpointProvider.topLevelFileIndex.get
        val checkpointPlan = log.loadIndex(checkpointIndex, readSchema).queryExecution.executedPlan
        assert(checkpointPlan.nodeName == "GpuColumnarToRow", checkpointPlan.treeString)
        val columnarPlan = checkpointPlan.children.head
        assert(columnarPlan.supportsColumnar, checkpointPlan.treeString)
        println(s"CHECKPOINT_MANUAL_HOST_PLAN $label schema=$schemaLabel " +
          s"readAddPayload=$readAddPayload\n${checkpointPlan.treeString}")
        val start = System.nanoTime()
        val result = columnarPlan.executeColumnar().mapPartitions { batches =>
          def column(cols: Array[ColumnVector], index: Int): ColumnVector = {
            if (index >= 0) cols(index) else null
          }
          def child(parent: ColumnVector, index: Int): ColumnVector = {
            if (parent != null && index >= 0) parent.getChild(index) else null
          }
          def addUtf8Hash(currentHash: Long, col: ColumnVector, row: Int): Long = {
            if (col != null && !col.isNullAt(row)) {
              currentHash * 31L + col.getUTF8String(row).hashCode().toLong
            } else {
              currentHash
            }
          }

          var partitionResult = EmptyManualHostResult
          batches.foreach { batch =>
            val batchRows = batch.numRows()
            var hostCols: Array[ColumnVector] = null
            var batchClosed = false
            var copyNs = 0L
            var accessNs = 0L
            try {
              val copyStart = System.nanoTime()
              val gpuCols = GpuColumnVector.extractColumns(batch)
              hostCols = new Array[ColumnVector](gpuCols.length)
              var columnIndex = 0
              while (columnIndex < gpuCols.length) {
                hostCols(columnIndex) = gpuCols(columnIndex).copyToHost()
                columnIndex += 1
              }
              copyNs = System.nanoTime() - copyStart
              batch.close()
              batchClosed = true

              val accessStart = System.nanoTime()
              val addCol = column(hostCols, addIdx)
              val removeCol = column(hostCols, removeIdx)
              val txnCol = column(hostCols, txnIdx)
              val protocolCol = column(hostCols, protocolIdx)
              val metadataCol = column(hostCols, metadataIdx)
              val addPathCol = child(addCol, addPathIdx)
              val addPartitionValuesCol = child(addCol, addPartitionValuesIdx)
              val addSizeCol = child(addCol, addSizeIdx)
              val addModificationTimeCol = child(addCol, addModificationTimeIdx)
              val addDataChangeCol = child(addCol, addDataChangeIdx)
              val addStatsCol = child(addCol, addStatsIdx)
              val addTagsCol = child(addCol, addTagsIdx)
              val removePathCol = child(removeCol, removePathIdx)
              val removeDeletionTimestampCol = child(removeCol, removeDeletionTimestampIdx)
              val removeDataChangeCol = child(removeCol, removeDataChangeIdx)
              val txnAppIdCol = child(txnCol, txnAppIdIdx)
              val txnVersionCol = child(txnCol, txnVersionIdx)
              val protocolMinReaderVersionCol = child(protocolCol, protocolMinReaderVersionIdx)
              val protocolMinWriterVersionCol = child(protocolCol, protocolMinWriterVersionIdx)
              val metadataIdCol = child(metadataCol, metadataIdIdx)
              var addRows = 0L
              var removeRows = 0L
              var txnRows = 0L
              var protocolRows = 0L
              var metadataRows = 0L
              var pathHash = 0L
              var payloadBytes = 0L
              var mapEntries = 0L
              var row = 0
              while (row < batchRows) {
                if (addCol != null && !addCol.isNullAt(row)) {
                  addRows += 1
                  pathHash = addUtf8Hash(pathHash, addPathCol, row)
                  if (addSizeCol != null && !addSizeCol.isNullAt(row)) {
                    pathHash = pathHash * 31L + addSizeCol.getLong(row)
                  }
                  if (addModificationTimeCol != null && !addModificationTimeCol.isNullAt(row)) {
                    pathHash = pathHash * 31L + addModificationTimeCol.getLong(row)
                  }
                  if (addDataChangeCol != null && !addDataChangeCol.isNullAt(row)) {
                    pathHash = pathHash * 31L + (if (addDataChangeCol.getBoolean(row)) 1L else 0L)
                  }
                  if (addPartitionValuesCol != null && !addPartitionValuesCol.isNullAt(row)) {
                    val partitionValues = addPartitionValuesCol.getMap(row)
                    mapEntries += partitionValues.numElements()
                  }
                  if (readAddPayload) {
                    if (addStatsCol != null && !addStatsCol.isNullAt(row)) {
                      payloadBytes += addStatsCol.getUTF8String(row).numBytes().toLong
                    }
                    if (addTagsCol != null && !addTagsCol.isNullAt(row)) {
                      val tags = addTagsCol.getMap(row)
                      mapEntries += tags.numElements()
                      val keys = tags.keyArray()
                      val values = tags.valueArray()
                      var tagIndex = 0
                      while (tagIndex < tags.numElements()) {
                        if (!keys.isNullAt(tagIndex)) {
                          payloadBytes += keys.getUTF8String(tagIndex).numBytes().toLong
                        }
                        if (!values.isNullAt(tagIndex)) {
                          payloadBytes += values.getUTF8String(tagIndex).numBytes().toLong
                        }
                        tagIndex += 1
                      }
                    }
                  }
                }
                if (removeCol != null && !removeCol.isNullAt(row)) {
                  removeRows += 1
                  pathHash = addUtf8Hash(pathHash, removePathCol, row)
                  if (removeDeletionTimestampCol != null &&
                      !removeDeletionTimestampCol.isNullAt(row)) {
                    pathHash = pathHash * 31L + removeDeletionTimestampCol.getLong(row)
                  }
                  if (removeDataChangeCol != null && !removeDataChangeCol.isNullAt(row)) {
                    pathHash = pathHash * 31L +
                      (if (removeDataChangeCol.getBoolean(row)) 1L else 0L)
                  }
                }
                if (txnCol != null && !txnCol.isNullAt(row)) {
                  txnRows += 1
                  pathHash = addUtf8Hash(pathHash, txnAppIdCol, row)
                  if (txnVersionCol != null && !txnVersionCol.isNullAt(row)) {
                    pathHash = pathHash * 31L + txnVersionCol.getLong(row)
                  }
                }
                if (protocolCol != null && !protocolCol.isNullAt(row)) {
                  protocolRows += 1
                  if (protocolMinReaderVersionCol != null &&
                      !protocolMinReaderVersionCol.isNullAt(row)) {
                    pathHash = pathHash * 31L + protocolMinReaderVersionCol.getInt(row).toLong
                  }
                  if (protocolMinWriterVersionCol != null &&
                      !protocolMinWriterVersionCol.isNullAt(row)) {
                    pathHash = pathHash * 31L + protocolMinWriterVersionCol.getInt(row).toLong
                  }
                }
                if (metadataCol != null && !metadataCol.isNullAt(row)) {
                  metadataRows += 1
                  pathHash = addUtf8Hash(pathHash, metadataIdCol, row)
                }
                row += 1
              }
              accessNs = System.nanoTime() - accessStart
              partitionResult = partitionResult + ManualHostResult(
                rows = batchRows,
                batches = 1L,
                addRows = addRows,
                removeRows = removeRows,
                txnRows = txnRows,
                protocolRows = protocolRows,
                metadataRows = metadataRows,
                pathHash = pathHash,
                payloadBytes = payloadBytes,
                mapEntries = mapEntries,
                copyNs = copyNs,
                accessNs = accessNs)
            } finally {
              if (!batchClosed) {
                batch.close()
              }
              if (hostCols != null) {
                hostCols.foreach { hostCol =>
                  if (hostCol != null) {
                    hostCol.close()
                  }
                }
              }
            }
          }
          Iterator.single(partitionResult)
        }.collect().foldLeft(EmptyManualHostResult)(_ + _)
        val elapsed = System.nanoTime() - start
        println(s"CHECKPOINT_MANUAL_HOST_MATERIALIZE $label SCHEMA $schemaLabel " +
          s"READ_ADD_PAYLOAD $readAddPayload ROWS ${result.rows} BATCHES ${result.batches} " +
          s"ADD_ROWS ${result.addRows} REMOVE_ROWS ${result.removeRows} " +
          s"TXN_ROWS ${result.txnRows} PROTOCOL_ROWS ${result.protocolRows} " +
          s"METADATA_ROWS ${result.metadataRows} PATH_HASH ${result.pathHash} " +
          s"PAYLOAD_BYTES ${result.payloadBytes} MAP_ENTRIES ${result.mapEntries} " +
          s"COPY_NS ${result.copyNs} ACCESS_NS ${result.accessNs} ELAPSED_NS $elapsed")
        println(s"CHECKPOINT_MANUAL_HOST_METRICS $label ${compactMetrics(columnarPlan)}")
        elapsed
      }

      def checkpointScan(label: String, gpu: Boolean, pruneIgnoredActions: Boolean = false): Long = {
        DeltaLog.clearCache()
        spark.sharedState.cacheManager.clearCache()
        runLabel.set(label)
        spark.conf.set("spark.rapids.sql.detectDeltaLogQueries", !gpu)
        val log = DeltaLog.forTable(spark, path)
        val snapshot = log.update()
        val checkpointIndex = snapshot.logSegment.checkpointProvider.topLevelFileIndex.get
        val checkpointPlan = checkpointRelation(log, checkpointIndex, pruneIgnoredActions).queryExecution.executedPlan
        println(s"CHECKPOINT_SCAN_PLAN $label\n${checkpointPlan.treeString}")
        val start = System.nanoTime()
        val rows = checkpointPlan.execute().count()
        val elapsed = System.nanoTime() - start
        println(s"CHECKPOINT_SCAN $label ROWS $rows ELAPSED_NS $elapsed")
        println(s"CHECKPOINT_SCAN_METRICS $label ${compactMetrics(checkpointPlan)}")
        elapsed
      }

      def sortedAllFiles(files: Array[AddFile]): Seq[AddFile] = {
        files.toSeq.sortBy { file =>
          (file.path, Option(file.getDeletionVectorUniqueId).flatten.getOrElse(""))
        }
      }

      def verifyAllFiles(
          label: String,
          reference: Option[Seq[AddFile]],
          files: Seq[AddFile]): Option[Seq[AddFile]] = {
        reference match {
          case Some(expected) =>
            assert(files == expected,
              s"$label allFiles differed from CPU reference: " +
                s"expected=${expected.size} actual=${files.size}")
            println(s"CORRECTNESS $label allFilesEqualCpu=true count=${files.size}")
            reference
          case None =>
            println(s"CORRECTNESS $label cpuReferenceCount=${files.size}")
            Some(files)
        }
      }

      def coldSnapshot(
          label: String,
          isolateCheckpointScan: Boolean,
          checkpointReadSchema: Option[(String, StructType)] = None,
          gpuReplayOperators: Boolean = false,
          permissiveMapFreeGpuJson: Boolean = false): (Long, Seq[AddFile]) = {
        spark.conf.set("spark.sql.adaptive.enabled", false)
        DeltaLog.clearCache()
        spark.sharedState.cacheManager.clearCache()
        runLabel.set(label)
        val start = System.nanoTime()
        spark.conf.set("spark.rapids.sql.detectDeltaLogQueries", !isolateCheckpointScan)
        val log = DeltaLog.forTable(spark, path)
        val baseSnapshot = log.update()
        val snapshot = checkpointReadSchema.map { case (_, schema) =>
          snapshotWithCheckpointSchema(log, baseSnapshot, schema, permissiveMapFreeGpuJson)
        }.getOrElse(baseSnapshot)
        if (isolateCheckpointScan) {
          val cpuBoundaryConfs = Seq(
            "spark.rapids.sql.exec.ProjectExec",
            "spark.rapids.sql.exec.UnionExec",
            "spark.rapids.sql.exec.ShuffleExchangeExec",
            "spark.rapids.sql.exec.SortExec")
          if (!gpuReplayOperators) {
            cpuBoundaryConfs.foreach(spark.conf.set(_, false))
          }
          // Build checkpoint/replay lineage while the requested Delta log operators are eligible.
          snapshot.stateDS
          if (!gpuReplayOperators) {
            cpuBoundaryConfs.foreach(spark.conf.set(_, true))
          }
          // Restore the metadata guard before the outer Delta Table State scan is planned.
          spark.conf.set("spark.rapids.sql.detectDeltaLogQueries", true)
        }
        val files = sortedAllFiles(snapshot.allFiles.collect())
        assert(files.nonEmpty)
        val elapsed = System.nanoTime() - start
        val schemaLabel = checkpointReadSchema.map(_._1).getOrElse("delta-default")
        println(s"COLD_SNAPSHOT $label CHECKPOINT_SCHEMA $schemaLabel " +
          s"GPU_REPLAY_OPERATORS $gpuReplayOperators FILES ${files.size} ELAPSED_NS $elapsed")
        (elapsed, files)
      }

      def coldSnapshotWithGpuJson(
          label: String,
          checkpointReadSchema: Option[(String, StructType)]): (Long, Seq[AddFile]) = {
        val jsonDateTimeConf = "spark.rapids.sql.json.read.datetime.enabled"
        val previousTimeZone = spark.conf.get("spark.sql.session.timeZone")
        val previousJsonDateTime = spark.conf.getOption(jsonDateTimeConf)
        try {
          spark.conf.set("spark.sql.session.timeZone", "UTC")
          spark.conf.set(jsonDateTimeConf, true)
          coldSnapshot(label, isolateCheckpointScan = true,
            checkpointReadSchema = checkpointReadSchema, gpuReplayOperators = true,
            permissiveMapFreeGpuJson = true)
        } finally {
          spark.conf.set("spark.sql.session.timeZone", previousTimeZone)
          previousJsonDateTime match {
            case Some(value) => spark.conf.set(jsonDateTimeConf, value)
            case None => spark.conf.unset(jsonDateTimeConf)
          }
        }
      }

      plans.clear()
      (1 to 3).foreach { iteration =>
        if (iteration % 2 == 1) {
          checkpointScan(s"cpu-scan-$iteration", gpu = false)
          checkpointColumnarScan(s"gpu-columnar-scan-$iteration")
          checkpointColumnarScan(s"gpu-pruned-columnar-scan-$iteration", pruneIgnoredActions = true)
          checkpointDirectColumnarScan(s"gpu-top-level-dropped-columnar-scan-$iteration",
            "top-level-dropped", topLevelDroppedSchema)
          checkpointDirectColumnarScan(s"gpu-classic-physical-columnar-scan-$iteration",
            "classic-physical", classicPhysicalCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-no-add-tags-columnar-scan-$iteration",
            "no-add-tags", noAddTagsCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-no-add-payload-columnar-scan-$iteration",
            "no-add-payload", noAddPayloadCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-protocol-metadata-columnar-scan-$iteration",
            "protocol-metadata", protocolMetadataCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-protocol-metadata-classic-columnar-scan-$iteration",
            "protocol-metadata-classic", protocolMetadataClassicCheckpointSchema)
          checkpointScan(s"gpu-scan-$iteration", gpu = true)
          checkpointScan(s"gpu-pruned-scan-$iteration", gpu = true, pruneIgnoredActions = true)
          checkpointDirectScan(s"gpu-top-level-dropped-scan-$iteration",
            "top-level-dropped", topLevelDroppedSchema)
          checkpointDirectScan(s"gpu-classic-physical-scan-$iteration",
            "classic-physical", classicPhysicalCheckpointSchema)
          checkpointManualHostMaterialize(s"gpu-classic-manual-keys-$iteration",
            "classic-physical", classicPhysicalCheckpointSchema, readAddPayload = false)
          checkpointManualHostMaterialize(s"gpu-classic-manual-payload-$iteration",
            "classic-physical", classicPhysicalCheckpointSchema, readAddPayload = true)
          checkpointDirectScan(s"gpu-no-add-tags-scan-$iteration",
            "no-add-tags", noAddTagsCheckpointSchema)
          checkpointDirectScan(s"gpu-no-add-payload-scan-$iteration",
            "no-add-payload", noAddPayloadCheckpointSchema)
          checkpointDirectScan(s"cpu-no-add-payload-scan-$iteration",
            "no-add-payload", noAddPayloadCheckpointSchema, gpu = false)
          checkpointDirectScan(s"gpu-protocol-metadata-scan-$iteration",
            "protocol-metadata", protocolMetadataCheckpointSchema)
          checkpointDirectScan(s"gpu-protocol-metadata-classic-scan-$iteration",
            "protocol-metadata-classic", protocolMetadataClassicCheckpointSchema)
          checkpointDirectScan(s"cpu-protocol-metadata-classic-scan-$iteration",
            "protocol-metadata-classic", protocolMetadataClassicCheckpointSchema, gpu = false)
        } else {
          checkpointDirectScan(s"cpu-protocol-metadata-classic-scan-$iteration",
            "protocol-metadata-classic", protocolMetadataClassicCheckpointSchema, gpu = false)
          checkpointDirectScan(s"gpu-protocol-metadata-classic-scan-$iteration",
            "protocol-metadata-classic", protocolMetadataClassicCheckpointSchema)
          checkpointDirectScan(s"gpu-protocol-metadata-scan-$iteration",
            "protocol-metadata", protocolMetadataCheckpointSchema)
          checkpointDirectScan(s"cpu-no-add-payload-scan-$iteration",
            "no-add-payload", noAddPayloadCheckpointSchema, gpu = false)
          checkpointDirectScan(s"gpu-no-add-payload-scan-$iteration",
            "no-add-payload", noAddPayloadCheckpointSchema)
          checkpointDirectScan(s"gpu-no-add-tags-scan-$iteration",
            "no-add-tags", noAddTagsCheckpointSchema)
          checkpointDirectScan(s"gpu-classic-physical-scan-$iteration",
            "classic-physical", classicPhysicalCheckpointSchema)
          checkpointManualHostMaterialize(s"gpu-classic-manual-payload-$iteration",
            "classic-physical", classicPhysicalCheckpointSchema, readAddPayload = true)
          checkpointManualHostMaterialize(s"gpu-classic-manual-keys-$iteration",
            "classic-physical", classicPhysicalCheckpointSchema, readAddPayload = false)
          checkpointDirectScan(s"gpu-top-level-dropped-scan-$iteration",
            "top-level-dropped", topLevelDroppedSchema)
          checkpointScan(s"gpu-pruned-scan-$iteration", gpu = true, pruneIgnoredActions = true)
          checkpointScan(s"gpu-scan-$iteration", gpu = true)
          checkpointDirectColumnarScan(s"gpu-protocol-metadata-classic-columnar-scan-$iteration",
            "protocol-metadata-classic", protocolMetadataClassicCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-protocol-metadata-columnar-scan-$iteration",
            "protocol-metadata", protocolMetadataCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-no-add-payload-columnar-scan-$iteration",
            "no-add-payload", noAddPayloadCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-no-add-tags-columnar-scan-$iteration",
            "no-add-tags", noAddTagsCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-classic-physical-columnar-scan-$iteration",
            "classic-physical", classicPhysicalCheckpointSchema)
          checkpointDirectColumnarScan(s"gpu-top-level-dropped-columnar-scan-$iteration",
            "top-level-dropped", topLevelDroppedSchema)
          checkpointColumnarScan(s"gpu-pruned-columnar-scan-$iteration", pruneIgnoredActions = true)
          checkpointColumnarScan(s"gpu-columnar-scan-$iteration")
          checkpointScan(s"cpu-scan-$iteration", gpu = false)
        }
      }
      if (!scanOnly) {
        var cpuReference: Option[Seq[AddFile]] = None
        (1 to 3).foreach { iteration =>
          if (iteration % 2 == 1) {
            val (_, cpuFiles) = coldSnapshot(s"cpu-baseline-$iteration",
              isolateCheckpointScan = false)
            cpuReference = verifyAllFiles(s"cpu-baseline-$iteration", cpuReference, cpuFiles)
            val (_, gpuFiles) = coldSnapshot(s"isolated-gpu-checkpoint-$iteration",
              isolateCheckpointScan = true)
            verifyAllFiles(s"isolated-gpu-checkpoint-$iteration", cpuReference, gpuFiles)
            val (_, classicFiles) =
              coldSnapshot(s"isolated-gpu-checkpoint-classic-physical-$iteration",
                isolateCheckpointScan = true,
                checkpointReadSchema = Some("classic-physical" -> classicPhysicalCheckpointSchema))
            verifyAllFiles(s"isolated-gpu-checkpoint-classic-physical-$iteration",
              cpuReference, classicFiles)
            val (_, broadClassicFiles) =
              coldSnapshot(s"broad-gpu-log-operators-classic-physical-$iteration",
                isolateCheckpointScan = true,
                checkpointReadSchema = Some("classic-physical" -> classicPhysicalCheckpointSchema),
                gpuReplayOperators = true)
            verifyAllFiles(s"broad-gpu-log-operators-classic-physical-$iteration",
              cpuReference, broadClassicFiles)
            val (_, gpuJsonClassicFiles) = coldSnapshotWithGpuJson(
              s"broad-gpu-json-operators-classic-physical-$iteration",
              Some("classic-physical" -> classicPhysicalCheckpointSchema))
            verifyAllFiles(s"broad-gpu-json-operators-classic-physical-$iteration",
              cpuReference, gpuJsonClassicFiles)
          } else {
            val (_, classicFiles) =
              coldSnapshot(s"isolated-gpu-checkpoint-classic-physical-$iteration",
                isolateCheckpointScan = true,
                checkpointReadSchema = Some("classic-physical" -> classicPhysicalCheckpointSchema))
            verifyAllFiles(s"isolated-gpu-checkpoint-classic-physical-$iteration",
              cpuReference, classicFiles)
            val (_, broadClassicFiles) =
              coldSnapshot(s"broad-gpu-log-operators-classic-physical-$iteration",
                isolateCheckpointScan = true,
                checkpointReadSchema = Some("classic-physical" -> classicPhysicalCheckpointSchema),
                gpuReplayOperators = true)
            verifyAllFiles(s"broad-gpu-log-operators-classic-physical-$iteration",
              cpuReference, broadClassicFiles)
            val (_, gpuJsonClassicFiles) = coldSnapshotWithGpuJson(
              s"broad-gpu-json-operators-classic-physical-$iteration",
              Some("classic-physical" -> classicPhysicalCheckpointSchema))
            verifyAllFiles(s"broad-gpu-json-operators-classic-physical-$iteration",
              cpuReference, gpuJsonClassicFiles)
            val (_, gpuFiles) = coldSnapshot(s"isolated-gpu-checkpoint-$iteration",
              isolateCheckpointScan = true)
            verifyAllFiles(s"isolated-gpu-checkpoint-$iteration", cpuReference, gpuFiles)
            val (_, cpuFiles) = coldSnapshot(s"cpu-baseline-$iteration",
              isolateCheckpointScan = false)
            cpuReference = verifyAllFiles(s"cpu-baseline-$iteration", cpuReference, cpuFiles)
          }
        }
        Thread.sleep(1000)
        val captured = plans.asScala.mkString("\n--- QUERY ---\n")
        println(captured)
        assert(captured.contains("GpuFileGpuScan") && captured.contains("Parquet"), captured)
        val cpuJsonPlans = plans.asScala
          .filterNot(_.contains("RUN: broad-gpu-json-operators-classic-physical-"))
          .mkString
        assert(cpuJsonPlans.contains("FileScan json") &&
          !cpuJsonPlans.contains("GpuFileGpuScan json"), cpuJsonPlans)
        val gpuJsonPlans = plans.asScala
          .filter(_.contains("RUN: broad-gpu-json-operators-classic-physical-"))
          .mkString
        assert(gpuJsonPlans.contains("GpuFileGpuScan json"), gpuJsonPlans)
        assert(captured.contains("MapPartitions"), captured)
        val isolatedPlans = plans.asScala
          .filter(_.contains("RUN: isolated-gpu-checkpoint-")).mkString
        assert(isolatedPlans.contains("Scan ExistingRDD Delta Table State"), isolatedPlans)
        assert(!isolatedPlans.contains("GpuRowToColumnar"), isolatedPlans)
      } else {
        Thread.sleep(1000)
      }
    } finally {
      spark.listenerManager.unregister(listener)
      spark.stop()
      FileUtils.deleteDirectory(tableDir)
    }
  }
}
