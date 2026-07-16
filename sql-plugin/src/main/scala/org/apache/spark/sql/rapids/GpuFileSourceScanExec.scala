/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.HashMap

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.filecache.FileCacheLocalityManager
import com.nvidia.spark.rapids.shims.{GpuDataSourceRDD, PartitionedFileUtilsShim, SparkShimImpl, StaticPartitionShims}
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, Attribute, AttributeReference, BoundReference, DynamicPruningExpression, Expression, Literal, PlanExpression, Predicate, SortOrder}
import org.apache.spark.sql.catalyst.json.rapids.GpuReadJsonFileFormat
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.{ExecSubqueryExpression, ExplainUtils, FileSourceScanExec, SQLExecution}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.rapids.shims.FilePartitionShims
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

/**
 * GPU version of Spark's `FileSourceScanExec`
 *
 * @param relation The file-based relation to scan.
 * @param originalOutput Output attributes of the scan, including data attributes and partition
 *                       attributes.
 * @param requiredSchema Required schema of the underlying relation, excluding partition columns.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param optionalBucketSet Bucket ids for bucket pruning.
 * @param optionalNumCoalescedBuckets Number of coalesced buckets.
 * @param dataFilters Filters on non-partition columns.
 * @param tableIdentifier identifier for the table in the metastore.
 * @param rapidsConf Rapids conf
 * @param queryUsesInputFile This is a parameter to easily allow turning it
 *                               off in GpuTransitionOverrides if InputFileName,
 *                               InputFileBlockStart, or InputFileBlockLength are used
 * @param disableBucketedScan Disable bucketed scan based on physical query plan.
 */
case class GpuFileSourceScanExec(
    @transient relation: HadoopFsRelation,
    originalOutput: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false,
    queryUsesInputFile: Boolean = false,
    requiredPartitionSchema: Option[StructType] = None)(@transient val rapidsConf: RapidsConf)
    extends GpuDataSourceScanExec with GpuExec {

  import GpuMetric._

  override val output: Seq[Attribute] = requiredPartitionSchema.map { requiredPartSchema =>
    // output attrs = data attrs ++ partition attrs
    val (dataOutAttrs, partOutAttrs) = originalOutput.splitAt(requiredSchema.length)
    val prunedPartOutAttrs = requiredPartSchema.map { f =>
      partOutAttrs(relation.partitionSchema.indexOf(f))
    }
    dataOutAttrs ++ prunedPartOutAttrs
  }.getOrElse(originalOutput)

  val readPartitionSchema = requiredPartitionSchema.getOrElse(relation.partitionSchema)

  @transient private val gpuFormat = relation.fileFormat match {
    case g: GpuReadFileFormatWithMetrics => g
    case f => throw new IllegalStateException(s"${f.getClass} is not a GPU format with metrics")
  }

  private val isPerFileReadEnabled = gpuFormat.isPerFileReadEnabled(rapidsConf)

  override def otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)

  // All expressions are filter expressions used on the CPU.
  override def gpuExpressions: Seq[Expression] = Nil

  override val nodeName: String = {
    s"GpuScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  private lazy val driverMetrics: HashMap[String, Long] = HashMap.empty

  private var prefetchEagerly = false

  def applyEagerPrefetch(): Unit = prefetchEagerly = true

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has
   * been initialized. See SPARK-26327 for more details.
   */
  private def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  @transient lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val pds = relation.location.listFiles(
        partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    logDebug(s"File listing took: ${System.nanoTime() - startTime}")
    setFilesNumAndSizeMetric(pds, true)
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    pds
  }.toArray

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  // Visible to org.apache.spark.sql.rapids.* so tests can assert on runtime partition pruning,
  // mirroring CPU's FileSourceScanExec.dynamicallySelectedPartitions (which is package-private
  // to org.apache.spark.sql.execution).
  @transient private[rapids] lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      }, Nil)
      val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      setFilesNumAndSizeMetric(ret, false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      driverMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedPartitions
    }
  }

  /**
   * [[partitionFilters]] can contain subqueries whose results are available only at runtime so
   * accessing [[selectedPartitions]] should be guarded by this method during planning
   */
  private def hasPartitionsAvailableAtRunTime: Boolean = {
    partitionFilters.exists(ExecSubqueryExpression.hasSubquery)
  }

  private def toAttribute(colName: String): Option[Attribute] =
    output.find(_.name == colName)

  // exposed for testing
  lazy val bucketedScan: Boolean = {
    if (relation.sparkSession.sessionState.conf.bucketingEnabled
      && relation.bucketSpec.isDefined
      && !disableBucketedScan) {
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      bucketColumns.size == spec.bucketColumnNames.size
    } else {
      false
    }
  }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    if (bucketedScan) {
      // For bucketed columns:
      // -----------------------
      // `HashPartitioning` would be used only when:
      // 1. ALL the bucketing columns are being read from the table
      //
      // For sorted columns:
      // ---------------------
      // Sort ordering should be used when ALL these criteria's match:
      // 1. `HashPartitioning` is being used
      // 2. A prefix (or all) of the sort columns are being read from the table.
      //
      // Sort ordering would be over the prefix subset of `sort columns` being read
      // from the table.
      // eg.
      // Assume (col0, col2, col3) are the columns read from the table
      // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
      // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
      // above
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      val numPartitions = optionalNumCoalescedBuckets.getOrElse(spec.numBuckets)
      val partitioning = HashPartitioning(bucketColumns, numPartitions)
      val sortColumns =
        spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)
      val shouldCalculateSortOrder =
        conf.getConf(SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING) &&
          sortColumns.nonEmpty &&
          !hasPartitionsAvailableAtRunTime

      val sortOrder = if (shouldCalculateSortOrder) {
        // In case of bucketing, its possible to have multiple files belonging to the
        // same bucket in a given relation. Each of these files are locally sorted
        // but those files combined together are not globally sorted. Given that,
        // the RDD partition will not be sorted even if the relation has sort columns set
        // Current solution is to check if all the buckets have a single file in it

        val files = selectedPartitions.flatMap(partition => partition.files)
        val bucketToFilesGrouping =
          files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
        val singleFilePartitions = bucketToFilesGrouping.forall(p => p._2.length <= 1)

        // TODO SPARK-24528 Sort order is currently ignored if buckets are coalesced.
        if (singleFilePartitions && optionalNumCoalescedBuckets.isEmpty) {
          // TODO Currently Spark does not support writing columns sorting in descending order
          // so using Ascending order. This can be fixed in future
          sortColumns.map(attribute => SortOrder(attribute, Ascending))
        } else {
          Nil
        }
      } else {
        Nil
      }
      (partitioning, sortOrder)
    } else {
      (UnknownPartitioning(0), Nil)
    }
  }

  @transient
  private lazy val pushedDownFilters = {
    val supportNestedPredicatePushdown =
      DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")

    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName +
        GpuDataSourceScanExec.buildLocationMetadata(location.rootPaths, maxMetadataValueLength)
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsColumnar.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedDownFilters),
        "DataFilters" -> seqToString(dataFilters),
        "Location" -> locationDesc,
        "Eager_IO_Prefetch" -> prefetchEagerly.toString)

    relation.bucketSpec.map { spec =>
      val bucketedKey = "Bucketed"
      if (bucketedScan) {
        val numSelectedBuckets = optionalBucketSet.map { b =>
          b.cardinality()
        } getOrElse {
          spec.numBuckets
        }

        metadata ++ Map(
          bucketedKey -> "true",
          "SelectedBucketsCount" -> (s"$numSelectedBuckets out of ${spec.numBuckets}" +
            optionalNumCoalescedBuckets.map { b => s" (Coalesced to $b)" }.getOrElse("")))
      } else if (!relation.sparkSession.sessionState.conf.bucketingEnabled) {
        metadata + (bucketedKey -> "false (disabled by configuration)")
      } else if (disableBucketedScan) {
        metadata + (bucketedKey -> "false (disabled by query planner)")
      } else {
        metadata + (bucketedKey -> "false (bucket column(s) not read)")
      }
    } getOrElse {
      metadata
    }
  }

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, _) if (key.equals("Location")) =>
        val location = relation.location
        val numPaths = location.rootPaths.length
        val abbreviatedLocation = if (numPaths <= 1) {
          location.rootPaths.mkString("[", ", ", "]")
        } else {
          "[" + location.rootPaths.head + s", ... ${numPaths - 1} entries]"
        }
        s"$key: ${location.getClass.getSimpleName} ${redact(abbreviatedLocation)}"
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  /**
   * If the small file optimization is enabled then we read all the files before sending down
   * to the GPU. If it is disabled then we use the standard Spark logic of reading one file
   * at a time.
   */
  lazy val inputRDD: RDD[InternalRow] = {
    val readFile: Option[(PartitionedFile) => Iterator[InternalRow]] =
      if (isPerFileReadEnabled) {
        val reader = gpuFormat.buildReaderWithPartitionValuesAndMetrics(
          sparkSession = relation.sparkSession,
          dataSchema = relation.dataSchema,
          partitionSchema = readPartitionSchema,
          requiredSchema = requiredSchema,
          filters = pushedDownFilters,
          options = relation.options,
          hadoopConf =
            relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options),
          metrics = allMetrics)
        Some(reader)
      } else {
        None
      }

    val readRDD = if (bucketedScan) {
      createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions,
        relation)
    } else {
      createNonBucketedReadRDD(readFile, relation)
    }
    sendDriverMetrics()
    readRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  /** SQL metrics generated only for scans using dynamic partition pruning. */
  private lazy val staticMetrics = if (partitionFilters.exists(isDynamicPruningFilter)) {
    Map("staticFilesNum" -> createMetric(ESSENTIAL_LEVEL, "static number of files read"),
      "staticFilesSize" -> createSizeMetric(ESSENTIAL_LEVEL, "static size of files read"))
  } else {
    Map.empty[String, GpuMetric]
  }

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions") = partitions.length
    }
  }

  override lazy val allMetrics = Map(
    OP_TIME_NEW -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW),
    NUM_OUTPUT_ROWS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES),
    "numFiles" -> createMetric(ESSENTIAL_LEVEL, "number of files read"),
    "metadataTime" -> createTimingMetric(ESSENTIAL_LEVEL, "metadata time"),
    "filesSize" -> createSizeMetric(ESSENTIAL_LEVEL, "size of files read"),
    GPU_DECODE_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_DECODE_TIME),
    BUFFER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    FILTER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME),
    SCHEDULE_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SCHEDULE_TIME),
    BUFFER_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_BUFFER_TIME_BUBBLE),
    FILTER_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME_BUBBLE),
    SCHEDULE_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SCHEDULE_TIME_BUBBLE),
    DELETION_VECTOR_SCATTER_TIME -> createNanoTimingMetric(MODERATE_LEVEL,
      DESCRIPTION_DELETION_VECTOR_SCATTER_TIME),
    DELETION_VECTOR_SIZE -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_DELETION_VECTOR_SIZE)
  ) ++ fileCacheMetrics ++ {
    relation.fileFormat match {
      case _: GpuReadParquetFileFormat | _: GpuOrcFileFormat =>
        val bf = Map.newBuilder[String, GpuMetric]
        bf += READ_FS_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_READ_FS_TIME)
        bf += WRITE_BUFFER_TIME -> createNanoTimingMetric(DEBUG_LEVEL,
          DESCRIPTION_WRITE_BUFFER_TIME)
        if (rapidsConf.isParquetMultiThreadReadEnabled) {
          // Track the task number of multithreaded asynchronous I/O workloads
          bf += "numPartedFiles" -> createMetric(DEBUG_LEVEL, "number of PartitionedFiles")
        }
        if (relation.fileFormat.isInstanceOf[GpuReadParquetFileFormat]) {
          // Track the actual data size read from the file system, excluding data being pruned
          // by meta-level pruning.
          bf += "readBufferSize" -> createSizeMetric(DEBUG_LEVEL, "size of read buffer")
        }
        if (ExternalSource.isSupportedFormat(relation.fileFormat.getClass)) {
          // This metric is used to post the time spent in generating the `skip_row` column
          // in Delta Lake 3.3.0+
          bf += "isRowDeletedColumnGenTime" ->
            createNanoTimingMetric(ESSENTIAL_LEVEL, "time skiprow gen")
          // This metric is used to post the time spent in generating the `row_index` column
          // in Delta Lake 3.3.0+
          bf += "rowIndexColumnGenTime" ->
              createNanoTimingMetric(ESSENTIAL_LEVEL, "time row index gen")
        }
        bf.result()
      case _ =>
        Map.empty[String, GpuMetric]
    }
  } ++ {
    // Tracking scan time has overhead, we can't afford to do it for each row, and can only do
    // it for each batch.
    if (supportsColumnar) {
      Some(SCAN_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_SCAN_TIME))
    } else {
      None
    }
  } ++ {
    if (relation.partitionSchema.nonEmpty) {
      Map(
        NUM_PARTITIONS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_PARTITIONS),
        "pruningTime" -> createTimingMetric(ESSENTIAL_LEVEL, "dynamic partition pruning time"))
    } else {
      Map.empty[String, GpuMetric]
    }
  } ++ staticMetrics

  private lazy val fileCacheMetrics: Map[String, GpuMetric] = {
    // File cache only supported on Parquet files for now.
    relation.fileFormat match {
      case _: GpuReadParquetFileFormat | _: GpuReadOrcFileFormat => createFileCacheMetrics()
      case _ => Map.empty
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val scanTime = gpuLongMetric(SCAN_TIME)
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          scanTime.ns {
            val res = batches.hasNext
            res
          }
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  override val nodeNamePrefix: String = "GpuFile"

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createNonBucketedReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile an optional function to read each (part of a) file. Used
   *                 when not using the small file optimization.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: Option[(PartitionedFile) => Iterator[InternalRow]],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")

    val partitionedFiles = FilePartitionShims.getPartitions(selectedPartitions)

    val filesGroupedToBuckets = partitionedFiles.groupBy { f =>
      BucketingUtils
        .getBucketId(new Path(f.filePath.toString()).getName)
        .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
    }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = optionalNumCoalescedBuckets.map { numCoalescedBuckets =>
      logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
      val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
      Seq.tabulate(numCoalescedBuckets) { bucketId =>
        val partitionedFiles = coalescedBuckets.get(bucketId).map {
          _.values.flatten.toArray
        }.getOrElse(Array.empty)
        FilePartition(bucketId, partitionedFiles)
      }
    }.getOrElse {
      Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
        FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
      }
    }
    getFinalRDD(readFile, filePartitions)
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   *
   * @param readFile an optional function to read each (part of a) file. Used when
   *                 not using the small file optimization.
   * @param fsRelation [[HadoopFsRelation]] associated with the read.
   */
  private def createNonBucketedReadRDD(
      readFile: Option[(PartitionedFile) => Iterator[InternalRow]],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    val partitions = StaticPartitionShims.getStaticPartitions(fsRelation).getOrElse {
      val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
      val maxSplitBytes =
        FilePartition.maxSplitBytes(fsRelation.sparkSession, dynamicallySelectedPartitions)
      logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")

      val splitFiles = FilePartitionShims.splitFiles(dynamicallySelectedPartitions, fsRelation,
        maxSplitBytes)

      FilePartitionShims.getFilePartitions(fsRelation, splitFiles, maxSplitBytes)
    }
    getFinalRDD(readFile, partitions)
  }

  private def getFinalRDD(
      readFile: Option[(PartitionedFile) => Iterator[InternalRow]],
      partitions: Seq[FilePartition]): RDD[InternalRow] = {

    // Prune the partition values for each partition
    val prunedPartitions = requiredPartitionSchema.map { partSchema =>
      val idsAndTypes = partSchema.map(f => (relation.partitionSchema.indexOf(f), f.dataType))
      partitions.map { p =>
        val partFiles = FilePartitionShims.getFiles(p).map { pf =>
          val prunedPartValues = idsAndTypes.map { case (id, dType) =>
            pf.partitionValues.get(id, dType)
          }
          pf.copy(partitionValues = InternalRow.fromSeq(prunedPartValues))
        }
        FilePartitionShims.copyWithFiles(p, partFiles)
      }
    }.getOrElse(partitions)

    // Update the preferred locations based on the file cache locality
    val locatedPartitions = prunedPartitions.map { partition =>
      val newFiles = FilePartitionShims.getFiles(partition).map { partFile =>
        val cacheLocations = FileCacheLocalityManager.get.getLocations(partFile.filePath.toString)
        if (cacheLocations.nonEmpty) {
          val newLocations = cacheLocations ++ partFile.locations
          PartitionedFileUtilsShim.withNewLocations(partFile, newLocations)
        } else {
          partFile
        }
      }
      FilePartitionShims.copyWithFiles(partition, newFiles)
    }

    if (isPerFileReadEnabled) {
      logInfo("Using the original per file reader")
      SparkShimImpl.getFileScanRDD(relation.sparkSession, readFile.get, locatedPartitions,
        requiredSchema, fileFormat = Some(relation.fileFormat))
    } else {
      logDebug(s"Using Datasource RDD, files are: " +
        s"${prunedPartitions.flatMap(FilePartitionShims.getFiles).mkString(",")}")
      // note we use the v2 DataSourceRDD instead of FileScanRDD so we don't have to copy more code
      GpuDataSourceRDD(relation.sparkSession.sparkContext, locatedPartitions, readerFactory)
    }
  }

  /** visible for testing */
  lazy val readerFactory: PartitionReaderFactory = {
    // here we are making an optimization to read more then 1 file at a time on the CPU side
    // if they are small files before sending it down to the GPU
    val hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
    if (prefetchEagerly) {
      hadoopConf.setBoolean("rapids.sql.scan.prefetch", prefetchEagerly)
    }
    val broadcastedHadoopConf =
      relation.sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    gpuFormat.createMultiFileReaderFactory(
      broadcastedHadoopConf,
      pushedDownFilters.toArray,
      this)
  }

  // Filters unused DynamicPruningExpression expressions - one which has been replaced
  // with DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
  private def filterUnusedDynamicPruningExpressions(
      predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  override def doCanonicalize(): GpuFileSourceScanExec = {
    GpuFileSourceScanExec(
      relation,
      originalOutput.map(QueryPlan.normalizeExpressions(_, originalOutput)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters), originalOutput),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, originalOutput),
      None,
      queryUsesInputFile)(rapidsConf)
  }
}

object GpuFileSourceScanExec {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val cls = meta.wrapped.relation.fileFormat.getClass
    if (ExternalSource.isSupportedFormat(cls)) {
      ExternalSource.tagSupportForGpuFileSourceScan(meta)
    } else if (classOf[CSVFileFormat].isAssignableFrom(cls)) {
      GpuReadCSVFileFormat.tagSupport(meta)
    } else if (GpuOrcFileFormat.isSparkOrcFormat(cls)) {
      GpuReadOrcFileFormat.tagSupport(meta)
    } else if (classOf[ParquetFileFormat].isAssignableFrom(cls)) {
      GpuReadParquetFileFormat.tagSupport(meta)
    } else if (classOf[JsonFileFormat].isAssignableFrom(cls)) {
      GpuReadJsonFileFormat.tagSupport(meta)
    } else {
      meta.willNotWorkOnGpu(s"unsupported file format: ${cls.getCanonicalName}")
    }
  }

  def convertFileFormat(relation: HadoopFsRelation, rapidsConf: RapidsConf): FileFormat = {
    val format = relation.fileFormat
    val cls = format.getClass
    if (ExternalSource.isSupportedFormat(cls)) {
      ExternalSource.getReadFileFormat(relation, rapidsConf)
    } else if (classOf[CSVFileFormat].isAssignableFrom(cls)) {
      new GpuReadCSVFileFormat
    } else if (GpuOrcFileFormat.isSparkOrcFormat(cls)) {
      new GpuReadOrcFileFormat
    } else if (classOf[ParquetFileFormat].isAssignableFrom(cls)) {
      new GpuReadParquetFileFormat
    } else if (classOf[JsonFileFormat].isAssignableFrom(cls)) {
      new GpuReadJsonFileFormat
    } else {
      throw new IllegalArgumentException(s"${cls.getCanonicalName} is not supported")
    }
  }
}
