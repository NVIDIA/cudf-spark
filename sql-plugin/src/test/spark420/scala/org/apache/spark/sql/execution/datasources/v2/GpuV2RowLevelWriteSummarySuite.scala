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
{"spark":["420"],"carryForward":"all"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import java.util.{Collections, Set => JSet}

import com.nvidia.spark.rapids.{FQSuiteName, GpuDeltaWrite, GpuMetric, GpuRowToColumnarExec,
  GpuScan, GpuWrite, RapidsConf, TargetSize}
import com.nvidia.spark.rapids.shims.{GpuBatchScanExec, ReplaceDataExecShim, WriteDeltaExecShim}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.{ReplaceDataProjections, WriteDeltaProjections}
import org.apache.spark.sql.connector.catalog.{SupportsRowLevelOperations, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, DeleteSummary,
  DeltaBatchWrite, LogicalWriteInfo, MergeSummary, PhysicalWriteInfo, RowLevelOperation,
  RowLevelOperationBuilder, RowLevelOperationInfo, RowLevelOperationTable, UpdateSummary,
  WriteBuilder, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.metric.SQLLastAttemptMetric
import org.apache.spark.sql.types.{BooleanType, LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class GpuV2RowLevelWriteSummarySuite extends AnyFunSuite with FQSuiteName {

  private val idSchema = new StructType().add("id", LongType)
  private val emptyReplaceProjections = ReplaceDataProjections(
    ProjectingInternalRow(idSchema, Seq(0)),
    None)
  private val emptyDeltaProjections = WriteDeltaProjections(
    Some(ProjectingInternalRow(idSchema, Seq(0))),
    ProjectingInternalRow(idSchema, Seq(0)),
    None)

  test("GpuReplaceDataExec UPDATE registers metrics and commits UpdateSummary across retries") {
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan
      val exec = GpuReplaceDataExec(
        child, () => (), emptyReplaceProjections, new StubGpuWrite,
        Some(RowLevelOperation.Command.UPDATE))

      assert(exec.metrics(GpuRowLevelWriteExec.NUM_UPDATED_ROWS)
        .isInstanceOf[SQLLastAttemptMetric])
      assert(exec.metrics(GpuRowLevelWriteExec.NUM_COPIED_ROWS)
        .isInstanceOf[SQLLastAttemptMetric])
      assert(!exec.metrics.contains(GpuRowLevelWriteExec.NUM_DELETED_ROWS))

      val updatedMetric = exec.metrics(GpuRowLevelWriteExec.NUM_UPDATED_ROWS)
      val copiedMetric = exec.metrics(GpuRowLevelWriteExec.NUM_COPIED_ROWS)
      runWithRetriedMapStage(spark) { () =>
        updatedMetric.add(3L)
        copiedMetric.add(5L)
      }
      assert(updatedMetric.value === 6L)
      assert(copiedMetric.value === 10L)

      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(exec, recorder, Array.empty, child)
      assert(recorder.summaryCommitCount === 1)
      assert(recorder.plainCommitCount === 0)
      val summary = recorder.lastSummary.get.asInstanceOf[UpdateSummary]
      assert(summary.numUpdatedRows() === 3L)
      assert(summary.numCopiedRows() === 5L)
    }
  }

  test("GpuReplaceDataExec DELETE derives numDeletedRows from GpuBatchScanExec across retries") {
    withSparkSession { spark =>
      val output = Seq(AttributeReference("id", LongType, nullable = false)())
      val deleteTable = RowLevelOperationTable(
        new StubTable("t"), new StubRowLevelOperation(RowLevelOperation.Command.DELETE))
      val gpuScan = GpuBatchScanExec(output = output, scan = new StubGpuScan, table = deleteTable)
      val root = ProjectExec(output, gpuScan)
      val exec = GpuReplaceDataExec(
        root, () => (), emptyReplaceProjections, new StubGpuWrite,
        Some(RowLevelOperation.Command.DELETE))

      assert(gpuScan.metrics(GpuMetric.NUM_OUTPUT_ROWS).isInstanceOf[SQLLastAttemptMetric])
      assert(exec.metrics(GpuRowLevelWriteExec.NUM_DELETED_ROWS)
        .isInstanceOf[SQLLastAttemptMetric])
      assert(exec.metrics(GpuRowLevelWriteExec.NUM_COPIED_ROWS)
        .isInstanceOf[SQLLastAttemptMetric])

      val scannedMetric = gpuScan.metrics(GpuMetric.NUM_OUTPUT_ROWS)
      val copiedMetric = exec.metrics(GpuRowLevelWriteExec.NUM_COPIED_ROWS)
      runWithRetriedMapStage(spark) { () =>
        scannedMetric.add(10L)
        copiedMetric.add(4L)
      }
      assert(scannedMetric.value === 20L)
      assert(copiedMetric.value === 8L)

      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(exec, recorder, Array.empty, root)
      assert(recorder.summaryCommitCount === 1)
      val summary = recorder.lastSummary.get.asInstanceOf[DeleteSummary]
      assert(summary.numDeletedRows() === 6L)
      assert(summary.numCopiedRows() === 4L)
      assert(exec.metrics(GpuRowLevelWriteExec.NUM_DELETED_ROWS).value === 6L)
    }
  }

  test("GpuReplaceDataExec DELETE derives numDeletedRows from CPU BatchScanExec fallback") {
    withSparkSession { spark =>
      val output = Seq(AttributeReference("id", LongType, nullable = false)())
      val deleteTable = RowLevelOperationTable(
        new StubTable("t"), new StubRowLevelOperation(RowLevelOperation.Command.DELETE))
      val cpuScan = BatchScanExec(
        output = output,
        scan = new StubCpuScan,
        runtimeFilters = Seq.empty,
        table = deleteTable)
      val root = GpuRowToColumnarExec(cpuScan, TargetSize(1L << 20))
      val exec = GpuReplaceDataExec(
        root, () => (), emptyReplaceProjections, new StubGpuWrite,
        Some(RowLevelOperation.Command.DELETE))

      cpuScan.metrics(GpuMetric.NUM_OUTPUT_ROWS).add(9L)
      exec.metrics(GpuRowLevelWriteExec.NUM_COPIED_ROWS).add(2L)

      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(exec, recorder, Array.empty, root)
      val summary = recorder.lastSummary.get.asInstanceOf[DeleteSummary]
      assert(summary.numDeletedRows() === 7L)
      assert(summary.numCopiedRows() === 2L)
    }
  }

  test("GpuReplaceDataExec DELETE preserves -1 numDeletedRows when scan is absent") {
    withSparkSession { spark =>
      val output = Seq(AttributeReference("id", LongType, nullable = false)())
      // Plain non-RowLevelOperationTable scan must not be mistaken for the target DELETE scan.
      val plainScan = GpuBatchScanExec(
        output = output, scan = new StubGpuScan, table = new StubTable("other"))
      plainScan.metrics(GpuMetric.NUM_OUTPUT_ROWS).add(50L)

      val exec = GpuReplaceDataExec(
        plainScan, () => (), emptyReplaceProjections, new StubGpuWrite,
        Some(RowLevelOperation.Command.DELETE))
      exec.metrics(GpuRowLevelWriteExec.NUM_COPIED_ROWS).add(3L)

      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(exec, recorder, Array.empty, plainScan)
      assert(recorder.summaryCommitCount === 1)
      val summary = recorder.lastSummary.get.asInstanceOf[DeleteSummary]
      // SQLMetric.set(-1) is a no-op (value stays 0), so DeleteSummary must receive -1 directly.
      assert(exec.metrics(GpuRowLevelWriteExec.NUM_DELETED_ROWS).value === 0L)
      assert(summary.numDeletedRows() === -1L)
      assert(summary.numCopiedRows() === 3L)
    }
  }

  test("non-row-level GpuAppendDataExec commits without summary even over MergeRowsExec") {
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan
      val merge = GpuMergeRowsExec(
        Literal(true, BooleanType), Literal(true, BooleanType), Nil, Nil, Nil,
        checkCardinality = false, child.output, child)
      val appendExec = GpuAppendDataExec(merge, () => (), new StubGpuWrite)

      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(appendExec, recorder, Array.empty, merge)
      assert(recorder.summaryCommitCount === 0)
      assert(recorder.plainCommitCount === 1)
      assert(recorder.lastSummary.isEmpty)
    }
  }

  test("getUpdateSummary returns -1 sentinels when metrics are absent") {
    val summary = GpuV2WriteCommitShims.getUpdateSummary(Map.empty).get
    assert(summary.numUpdatedRows === -1L)
    assert(summary.numCopiedRows === -1L)
  }

  test("GpuWriteDeltaExec UPDATE and DELETE commit summaries and survive stage retries") {
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan

      val updateExec = GpuWriteDeltaExec(
        child, () => (), emptyDeltaProjections, new StubGpuDeltaWrite,
        Some(RowLevelOperation.Command.UPDATE))
      val updatedMetric = updateExec.metrics(GpuRowLevelWriteExec.NUM_UPDATED_ROWS)
      runWithRetriedMapStage(spark) { () =>
        updatedMetric.add(4L)
      }
      assert(updatedMetric.value === 8L)

      val updateRecorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(updateExec, updateRecorder, Array.empty, child)
      val updateSummary = updateRecorder.lastSummary.get.asInstanceOf[UpdateSummary]
      assert(updateSummary.numUpdatedRows() === 4L)
      assert(updateSummary.numCopiedRows() === 0L)

      val deleteExec = GpuWriteDeltaExec(
        child, () => (), emptyDeltaProjections, new StubGpuDeltaWrite,
        Some(RowLevelOperation.Command.DELETE))
      val deletedMetric = deleteExec.metrics(GpuRowLevelWriteExec.NUM_DELETED_ROWS)
      runWithRetriedMapStage(spark) { () =>
        deletedMetric.add(7L)
      }
      assert(deletedMetric.value === 14L)

      val deleteRecorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(deleteExec, deleteRecorder, Array.empty, child)
      val deleteSummary = deleteRecorder.lastSummary.get.asInstanceOf[DeleteSummary]
      assert(deleteSummary.numDeletedRows() === 7L)
      assert(deleteSummary.numCopiedRows() === 0L)
    }
  }

  test("MERGE command registers no UPDATE/DELETE metrics and commits MergeSummary") {
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan
      val merge = GpuMergeRowsExec(
        Literal(true, BooleanType), Literal(true, BooleanType), Nil, Nil, Nil,
        checkCardinality = false, child.output, child)
      merge.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_UPDATED).add(2L)
      merge.metrics(GpuMergeRowsExec.NUM_TARGET_ROWS_DELETED).add(3L)

      val replaceExec = GpuReplaceDataExec(
        merge, () => (), emptyReplaceProjections, new StubGpuWrite,
        Some(RowLevelOperation.Command.MERGE))
      assert(!replaceExec.metrics.contains(GpuRowLevelWriteExec.NUM_UPDATED_ROWS))
      assert(!replaceExec.metrics.contains(GpuRowLevelWriteExec.NUM_DELETED_ROWS))
      assert(!replaceExec.metrics.contains(GpuRowLevelWriteExec.NUM_COPIED_ROWS))

      val recorder = new RecordingBatchWrite
      GpuV2WriteCommitShims.commit(replaceExec, recorder, Array.empty, merge)
      val summary = recorder.lastSummary.get.asInstanceOf[MergeSummary]
      assert(summary.numTargetRowsUpdated() === 2L)
      assert(summary.numTargetRowsDeleted() === 3L)
    }
  }

  test("ReplaceDataExecShim and WriteDeltaExecShim propagate rowLevelCommand") {
    withSparkSession { spark =>
      val child = spark.range(1).queryExecution.executedPlan
      Seq(
        RowLevelOperation.Command.UPDATE,
        RowLevelOperation.Command.DELETE,
        RowLevelOperation.Command.MERGE
      ).foreach { cmd =>
        val cpuReplace = ReplaceDataExec(
          child, () => (), emptyReplaceProjections, new StubGpuWrite, cmd, "t")
        val gpuReplace = ReplaceDataExecShim.convertToGpu(
          cpuReplace, child, new StubGpuWrite).asInstanceOf[GpuReplaceDataExec]
        assert(gpuReplace.rowLevelCommand === Some(cmd))

        val cpuDelta = WriteDeltaExec(
          child, () => (), emptyDeltaProjections, new StubGpuDeltaWrite, cmd, "t")
        val gpuDelta = WriteDeltaExecShim.convertToGpu(
          cpuDelta, child, new StubGpuDeltaWrite).asInstanceOf[GpuWriteDeltaExec]
        assert(gpuDelta.rowLevelCommand === Some(cmd))
      }
    }
  }

  private def runWithRetriedMapStage(spark: SparkSession)(recordAttempt: () => Unit): Unit = {
    val rows = spark.sparkContext.parallelize(1 to 4, 1).mapPartitions { iter =>
      recordAttempt()
      iter.map(i => (0, i))
    }
    val shuffled = rows.reduceByKey(_ + _, 1)
    val shuffleId = shuffled.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    val result = shuffled.mapPartitions { iter =>
      if (TaskContext.get().stageAttemptNumber() == 0) {
        throw new FetchFailedException(SparkEnv.get.blockManager.blockManagerId,
          shuffleId, 0L, 0, 0, "Retry the map stage")
      }
      iter
    }
    assert(result.collect().toSeq === Seq(0 -> 10))
  }

  private def withSparkSession[T](f: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      // ESSENTIAL must still expose UPDATE/DELETE metrics used as commit summaries.
      .config(RapidsConf.METRICS_LEVEL.key, "ESSENTIAL")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }

  private class StubGpuWrite extends GpuWrite {
    override def toBatch: BatchWrite = throw new UnsupportedOperationException
  }

  private class StubGpuDeltaWrite extends GpuDeltaWrite {
    override def toBatch: DeltaBatchWrite = throw new UnsupportedOperationException
  }

  private class StubCpuScan extends Scan {
    override def readSchema(): StructType = idSchema
    override def toBatch: Batch = throw new UnsupportedOperationException
  }

  private class StubGpuScan extends GpuScan {
    override def readSchema(): StructType = idSchema
    override def toBatch: Batch = throw new UnsupportedOperationException
    override def withInputFile(): GpuScan = this
  }

  private class StubTable(tableName: String) extends Table with SupportsRowLevelOperations {
    override def name(): String = tableName
    override def schema(): StructType = idSchema
    override def capabilities(): JSet[TableCapability] = Collections.emptySet()
    override def newRowLevelOperationBuilder(
        info: RowLevelOperationInfo): RowLevelOperationBuilder =
      throw new UnsupportedOperationException
  }

  private class StubRowLevelOperation(cmd: RowLevelOperation.Command) extends RowLevelOperation {
    override def command(): RowLevelOperation.Command = cmd
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      throw new UnsupportedOperationException
    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
      throw new UnsupportedOperationException
  }

  private class RecordingBatchWrite extends BatchWrite {
    var plainCommitCount = 0
    var summaryCommitCount = 0
    var lastSummary: Option[WriteSummary] = None

    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      throw new UnsupportedOperationException
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      plainCommitCount += 1
    }

    override def commit(messages: Array[WriterCommitMessage], summary: WriteSummary): Unit = {
      summaryCommitCount += 1
      lastSummary = Some(summary)
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = ()
  }
}
