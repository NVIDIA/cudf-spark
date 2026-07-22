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

package org.apache.spark.sql.rapids

import java.util.{Date, UUID}

import com.nvidia.spark.TimingUtils
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.AssertUtils.assertInTests
import com.nvidia.spark.rapids.shims.{BucketingUtilsShim, RapidsFileSourceMetaUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Expression, SortOrder}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.datasources.{GpuWriteFiles, GpuWriteFilesExec, GpuWriteFilesSpec, WriteTaskResult, WriteTaskStats}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.OutputSpec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.rapids.GpuFileFormatWriter.GpuConcurrentOutputWriterSpec
import org.apache.spark.sql.rapids.execution.RapidsAnalysisException
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, Utils}

trait GpuFileFormatWriterBase extends Serializable with Logging {

  /**
   * Basic work flow of this command is:
   * 1. Driver side setup, including output committer initialization and data source specific
   *    preparation work for the write job to be issued.
   * 2. Issues a write job consists of one or more executor side tasks, each of which writes all
   *    rows within an RDD partition.
   * 3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
   *    exception is thrown during task commitment, also aborts that task.
   * 4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
   *    thrown during job commitment, also aborts the job.
   * 5. If the job is successfully committed, perform post-commit operations such as
   *    processing statistics.
   * @return The set of all partition paths that were updated during this write job.
   */
  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: ColumnarFileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[ColumnarWriteJobStatsTracker],
      options: Map[String, String],
      useStableSort: Boolean,
      concurrentWriterPartitionFlushSize: Long,
      forceHiveHashForBucketing: Boolean = false,
      numStaticPartitionCols: Int = 0,
      baseDebugOutputPath: Option[String]): Set[String] = {
    require(partitionColumns.size >= numStaticPartitionCols)

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    // The data is being written as columnar batches, but those are not serializable. Using the same
    // InternalRow type that Spark uses here, as it should not really matter. The columnar path
    // should not be executing the output format code that depends on this setting. Instead specific
    // output formats are detected and replaced with a different code path, otherwise the code
    // needs to fallback to the row-based write path.
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(outputSpec.outputPath))

    val partitionSet = AttributeSet(partitionColumns)
    // cleanup the internal metadata information of
    // the file source metadata attribute if any before write out when needed.
    val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns
      .map(RapidsFileSourceMetaUtils.cleanupFileSourceMetadataInformation))
    val dataColumns = finalOutputSpec.outputColumns.filterNot(partitionSet.contains)

    val writerBucketSpec = BucketingUtilsShim.getWriterBucketSpec(bucketSpec, dataColumns,
      options, forceHiveHashForBucketing)
    val sortColumns = bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }

    val caseInsensitiveOptions = CaseInsensitiveMap(options)

    val dataSchema = dataColumns.toStructType
    verifySchema(fileFormat, dataSchema)

    // NOTE: prepareWrite has side effects as it modifies the job configuration.
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, caseInsensitiveOptions, dataSchema)

    val description = new GpuWriteJobDescription(
      uuid = UUID.randomUUID.toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = finalOutputSpec.outputColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketSpec = writerBucketSpec,
      path = finalOutputSpec.outputPath,
      customPartitionLocations = finalOutputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = statsTrackers,
      concurrentWriterPartitionFlushSize = concurrentWriterPartitionFlushSize
    )

    // We should first sort by dynamic partition columns, then bucket id, and finally
    // sorting columns.
    val requiredOrdering = partitionColumns.drop(numStaticPartitionCols) ++
      writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns
    val writeFilesOpt = GpuWriteFiles.getWriteFilesOpt(plan)
    // the sort order doesn't matter
    val actualOrdering = writeFilesOpt.map(_.child).getOrElse(plan).outputOrdering.map(_.child)

    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    // propagate the description UUID into the jobs, so that committers
    // get an ID guaranteed to be unique.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", description.uuid)

    if (writeFilesOpt.isDefined) {
      // Typically plan is like:
      //   Execute InsertIntoHadoopFsRelationCommand
      //     +- WriteFiles
      //       +- Sort // already sorted
      //         +- Sub plan
      // No need to sort again when execute `WriteFiles`

      // build `WriteFilesSpec` for `WriteFiles`
      val concurrentOutputWriterSpecFunc = (plan: SparkPlan) => {
        // Using Internal method: simple SortOrder expressions for file writing
        val orderingExpr = GpuBindReferences.bindReferencesNoMetrics(requiredOrdering
          .map(attr => SortOrder(attr, Ascending)), outputSpec.outputColumns)
        // this sort plan does not execute, only use its output
        val sortPlan = createSortPlan(plan, orderingExpr, useStableSort, statsTrackers)
        val batchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(sparkSession.sessionState.conf)
        GpuWriteFiles.createConcurrentOutputWriterSpec(sparkSession, sortColumns,
          sortPlan.output, batchSize, orderingExpr)
      }
      val writeSpec = GpuWriteFilesSpec(
        description = description,
        committer = committer,
        concurrentOutputWriterSpecFunc = concurrentOutputWriterSpecFunc
      )
      executeWrite(sparkSession, plan.asInstanceOf[GpuWriteFilesExec], writeSpec, job)
    } else {
      // In this path, Spark version is less than 340 or 'spark.sql.optimizer.plannedWrite.enabled'
      // is disabled, should sort the data if necessary.
      executeWrite(sparkSession, plan, job, description, committer, outputSpec,
        requiredOrdering, partitionColumns, sortColumns, orderingMatched, useStableSort,
        baseDebugOutputPath)
    }
  }

  private def executeWrite(
      sparkSession: SparkSession,
      plan: SparkPlan,
      job: Job,
      description: GpuWriteJobDescription,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      requiredOrdering: Seq[Expression],
      partitionColumns: Seq[Attribute],
      sortColumns: Seq[Attribute],
      orderingMatched: Boolean,
      useStableSort: Boolean,
      baseDebugOutputPath: Option[String]): Set[String] = {
    val partitionSet = AttributeSet(partitionColumns)
    val hasGpuEmpty2Null = plan.find(p => GpuV1WriteUtils.hasGpuEmptyToNull(p.expressions))
      .isDefined
    val empty2NullPlan = if (hasGpuEmpty2Null) {
      // Empty2Null has been inserted during logic optimization.
      plan
    } else {
      val projectList = GpuV1WriteUtils.convertGpuEmptyToNull(plan.output, partitionSet)
      if (projectList.nonEmpty) GpuProjectExec(projectList, plan) else plan
    }

    writeAndCommit(job, description, committer) {
      val (rdd, concurrentOutputWriterSpec) = if (orderingMatched) {
        (empty2NullPlan.executeColumnar(), None)
      } else {
        // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
        // the physical plan may have different attribute ids due to optimizer removing some
        // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
        // Using Internal method: simple SortOrder expressions for file writing
        val orderingExpr = GpuBindReferences.bindReferencesNoMetrics(
          requiredOrdering
            .map(attr => SortOrder(attr, Ascending)), outputSpec.outputColumns)
        val batchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(sparkSession.sessionState.conf)
        val concurrentOutputWriterSpec = GpuWriteFiles.createConcurrentOutputWriterSpec(
          sparkSession, sortColumns, empty2NullPlan.output, batchSize, orderingExpr)

        if (concurrentOutputWriterSpec.isDefined) {
          // concurrent write
          (empty2NullPlan.executeColumnar(), concurrentOutputWriterSpec)
        } else {
          // sort, then write
          val sortPlan = createSortPlan(empty2NullPlan, orderingExpr, useStableSort,
            description.statsTrackers)
          val sort = sortPlan.executeColumnar()
          (sort, concurrentOutputWriterSpec) // concurrentOutputWriterSpec is None
        }
      }

      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
        sparkSession.sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        rdd
      }

      // Collect exclude metrics from the plan. The child may be wrapped in
      // non-GpuExec nodes -- AdaptiveSparkPlanExec (AQE) and the query-stage
      // wrappers it finalizes into -- so a plain `plan match { case GpuExec }`
      // returns Seq.empty and the Insert op_time then double-counts every
      // descendant. Descend through wrappers (unwrapping AQE to its
      // already-finalized executedPlan and query stages to their plan) to the
      // GpuExec roots before collecting.
      def collectExcludeMetrics(p: SparkPlan, visited: Set[SparkPlan]): Seq[GpuMetric] = {
        if (visited.contains(p)) {
          // Guard against cycles / re-visiting shared (reused) sub-plans.
          Seq.empty
        } else {
          val seen = visited + p
          p match {
            case aqe: AdaptiveSparkPlanExec => collectExcludeMetrics(aqe.executedPlan, seen)
            case qs: QueryStageExec => collectExcludeMetrics(qs.plan, seen)
            case gpuExec: GpuExec => gpuExec.getOpTimeNewMetric.toSeq ++
              gpuExec.getDescendantOpTimeMetrics
            // Stop at a stage boundary: a (non-GPU) Exchange under a CPU subtree leads
            // into a different stage whose op_time is not part of this write task's
            // interval. Mirror the Exchange stop in GpuExec.getDescendantOpTimeMetrics.
            case _: Exchange => Seq.empty
            case other => other.children.flatMap(collectExcludeMetrics(_, seen))
          }
        }
      }
      // distinct: a reused descendant reached via multiple GpuExec roots must be
      // excluded once, not once per path (double-exclusion would under-count).
      val excludeMetrics = collectExcludeMetrics(plan, Set.empty).distinct

      // SPARK-41448 map reduce job IDs need to consistent across attempts for correctness
      val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
      val ret = new Array[WriteTaskResult](rddWithNonEmptyPartitions.partitions.length)
      sparkSession.sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (taskContext: TaskContext, iter: Iterator[ColumnarBatch]) => {
          executeTask(
            description = description,
            jobTrackerID = jobTrackerID,
            sparkStageId = taskContext.stageId(),
            sparkPartitionId = taskContext.partitionId(),
            sparkAttemptNumber = taskContext.taskAttemptId().toInt & Integer.MAX_VALUE,
            committer,
            iterator = iter,
            concurrentOutputWriterSpec = concurrentOutputWriterSpec,
            baseDebugOutputPath = baseDebugOutputPath,
            excludeMetrics = excludeMetrics)
        },
        rddWithNonEmptyPartitions.partitions.indices,
        (index, res: WriteTaskResult) => {
          committer.onTaskCommit(res.commitMsg)
          ret(index) = res
        })
      ret
    }
  }

  private def writeAndCommit(
      job: Job,
      description: GpuWriteJobDescription,
      committer: FileCommitProtocol)(f: => Array[WriteTaskResult]): Set[String] = {
    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)
    try {
      val ret = f
      val commitMsgs = ret.map(_.commitMsg)

      val (_, duration) = TimingUtils.timeTakenMs {
        committer.commitJob(job, commitMsgs)
      }
      logInfo(s"Write Job ${description.uuid} committed. Elapsed time: $duration ms.")

      processStats(description.statsTrackers, ret.map(_.summary.stats), duration)
      logInfo(s"Finished processing stats for write job ${description.uuid}.")

      // return a set of all the partition paths that were updated during this job
      ret.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    } catch {
      case cause: Throwable =>
        logError(s"Aborting job ${description.uuid}.", cause)
        committer.abortJob(job)
        throw new SparkException("Job aborted.", cause)
    }
  }

  /**
   * Write files using [[SparkPlan.executeWrite]]
   */
  def executeWrite(
      session: SparkSession,
      planForWrites: GpuWriteFilesExec,
      writeFilesSpec: GpuWriteFilesSpec,
      job: Job): Set[String] = {
    val committer = writeFilesSpec.committer
    val description = writeFilesSpec.description

    writeAndCommit(job, description, committer) {
      // columnar write
      val rdd = planForWrites.executeColumnarWrite(writeFilesSpec)
      val ret = new Array[WriteTaskResult](rdd.partitions.length)
      session.sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[WriterCommitMessage]) => {
          assertInTests(iter.hasNext)
          val commitMessage = iter.next()
          assertInTests(!iter.hasNext)
          commitMessage
        },
        rdd.partitions.indices,
        (index, res: WriterCommitMessage) => {
          assert(res.isInstanceOf[WriteTaskResult])
          val writeTaskResult = res.asInstanceOf[WriteTaskResult]
          committer.onTaskCommit(writeTaskResult.commitMsg)
          ret(index) = writeTaskResult
        })
      ret
    }
  }

  private def createSortPlan(
      child: SparkPlan,
      orderingExpr: Seq[SortOrder],
      useStableSort: Boolean,
      statsTrackers: Seq[ColumnarWriteJobStatsTracker]): GpuSortExec = {
    // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
    // the physical plan may have different attribute ids due to optimizer removing some
    // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.

    // sort, then write
    val sortType = if (useStableSort) {
      FullSortSingleBatch
    } else {
      OutOfCoreSort
    }
    // TODO: Using a GPU ordering as a CPU ordering here. Should be OK for now since we do not
    //       support bucket expressions yet and the rest should be simple attributes.
    val sortTrackers = statsTrackers.filter(_.isInstanceOf[GpuWriteJobStatsTracker])
    GpuSortExec(
      orderingExpr,
      global = false,
      child = child,
      sortType = sortType
    )(orderingExpr, Some(sortTrackers.asInstanceOf[Seq[GpuWriteJobStatsTracker]]))
  }

  /** Writes data out in a single Spark task. */
  def executeTask(
      description: GpuWriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[ColumnarBatch],
      concurrentOutputWriterSpec: Option[GpuConcurrentOutputWriterSpec],
      baseDebugOutputPath: Option[String],
      excludeMetrics: Seq[GpuMetric] = Seq.empty): WriteTaskResult = {

    val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)

      createTaskAttemptContext(description, hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    // Resolve the Insert op_time metric up front so the `.ns(excludeMetrics)`
    // wrap can activate before the empty-partition `iterator.hasNext` check
    // below. Without this, that hasNext cascades through every descendant
    // operator's `.ns` wrap *outside* the Insert wrap, so the descendant
    // op_time those wraps accumulate is never subtracted from Insert's
    // op_time.
    val opTimeMetric: GpuMetric = description.statsTrackers
      .collectFirst { case t: GpuWriteJobStatsTracker => t.opTimeNewMetric }
      .getOrElse(NoopMetric)

    opTimeMetric.ns(excludeMetrics) {
      val dataWriter =
        if (sparkPartitionId != 0 && !iterator.hasNext) {
          // In case of empty job, leave first partition to save meta for file format like parquet.
          new GpuEmptyDirectoryDataWriter(description, taskAttemptContext, committer)
        } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
          new GpuSingleDirectoryDataWriter(description, taskAttemptContext, committer,
            baseDebugOutputPath)
        } else {
          concurrentOutputWriterSpec match {
            case Some(spec) =>
              new GpuDynamicPartitionDataConcurrentWriter(description, taskAttemptContext,
                committer, spec, baseDebugOutputPath)
            case _ =>
              new GpuDynamicPartitionDataSingleWriter(description, taskAttemptContext, committer,
                baseDebugOutputPath)
          }
        }

      try {
        Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
          // Execute the task to write rows out and commit the task.
          dataWriter.writeWithIterator(iterator)
          dataWriter.commit()
        })(catchBlock = {
          // If there is an error, abort the task
          dataWriter.abort()
          logError(s"Job $jobId aborted.")
        }, finallyBlock = {
          dataWriter.close()
        })
      } catch {
        case e: FetchFailedException =>
          throw e
        case t: Throwable =>
          throw new SparkException("Task failed while writing rows.", t)
      }
    }
  }

  /**
   * For every registered [[WriteJobStatsTracker]], call `processStats()` on it, passing it
   * the corresponding [[WriteTaskStats]] from all executors.
   */
  private def processStats(
      statsTrackers: Seq[ColumnarWriteJobStatsTracker],
      statsPerTask: Seq[Seq[WriteTaskStats]],
      jobCommitDuration: Long)
  : Unit = {

    val numStatsTrackers = statsTrackers.length
    assertInTests(statsPerTask.forall(_.length == numStatsTrackers),
      s"""Every WriteTask should have produced one `WriteTaskStats` object for every tracker.
         |There are $numStatsTrackers statsTrackers, but some task returned
         |${statsPerTask.find(_.length != numStatsTrackers).get.length} results instead.
       """.stripMargin)

    val statsPerTracker = if (statsPerTask.nonEmpty) {
      statsPerTask.transpose
    } else {
      statsTrackers.map(_ => Seq.empty)
    }

    statsTrackers.zip(statsPerTracker).foreach {
      case (statsTracker, stats) => statsTracker.processStats(stats, jobCommitDuration)
    }
  }


  private def verifySchema(format: ColumnarFileFormat, schema: StructType): Unit = {
    schema.foreach { field =>
      if (!format.supportDataType(field.dataType)) {
        throw new RapidsAnalysisException(
          s"$format data source does not support ${field.dataType.catalogString} data type.")
      }
    }
  }

  def createTaskAttemptContext(description: GpuWriteJobDescription,
      hadoopConf: Configuration,
      taskAttemptId: TaskAttemptID): TaskAttemptContext
}

/** A helper object for writing columnar data out to a location. */
object GpuFileFormatWriter extends GpuFileFormatWriterBase {
  /** Describes how concurrent output writers should be executed. */
  case class GpuConcurrentOutputWriterSpec(maxWriters: Int, output: Seq[Attribute],
      batchSize: Long, sortOrder: Seq[SortOrder])

  def createTaskAttemptContext(description: GpuWriteJobDescription,
      hadoopConf: Configuration,
      taskAttemptId: TaskAttemptID): TaskAttemptContext = {

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }
}
