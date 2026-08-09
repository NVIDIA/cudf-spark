/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import scala.collection.mutable

import ai.rapids.cudf.{CaptureGroups, ColumnVector, DType, RegexProgram, Scalar, Schema, Table}
import com.nvidia.spark.rapids.{ColumnarPartitionReaderWithPartitionValues, CSVPartitionReaderBase, DateUtils, GpuColumnVector, GpuExec, GpuMetric, HostStringColBufferer, HostStringColBuffererFactory, NvtxIdWithMetrics, NvtxRegistry, PartitionReaderIterator, PartitionReaderWithBytesRead, RapidsConf}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.CastStrings
import com.nvidia.spark.rapids.shims.{ShimFilePartitionReaderFactory, ShimSparkPlan, SparkShimImpl}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeReference, AttributeSeq, AttributeSet, BindReferences, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.{ExecSubqueryExpression, LeafExecNode, SQLExecution}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.rapids.shims.FilePartitionShims
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.{BooleanType, DataType, DecimalType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * RAPIDS replacement for the [[org.apache.spark.sql.hive.execution.HiveTableScanExec]],
 * for supporting reading Hive delimited text format.
 *
 * While the HiveTableScanExec supports all the data formats that Hive does,
 * the GpuHiveTableScanExec currently supports only text tables.
 *
 * This GpuExec supports reading from Hive tables under the following conditions:
 *   1. The table is stored as TEXTFILE (i.e. input-format == TextInputFormat,
 *      serde == LazySimpleSerDe).
 *   2. The table contains only columns of primitive types. Specifically, STRUCT,
 *      ARRAY, MAP, and BINARY are not supported.
 *   3. The table uses Hive's default record delimiters ('Ctrl-A'),
 *      and line delimiters ('\n').
 *
 * @param requestedAttributes Columns to be read from the table
 * @param hiveTableRelation The Hive table to be scanned
 * @param partitionPruningPredicate Partition-pruning predicate for Hive partitioned tables
 */
case class GpuHiveTableScanExec(requestedAttributes: Seq[Attribute],
                                hiveTableRelation: HiveTableRelation,
                                partitionPruningPredicate: Seq[Expression])
  extends GpuExec with ShimSparkPlan with LeafExecNode with CastSupport {

  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPredicate.flatMap(_.references))

  private val originalAttributes = AttributeMap(hiveTableRelation.output.map(a => a -> a))

  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    requestedAttributes.map(originalAttributes)
  }

  val partitionAttributes: Seq[AttributeReference] = hiveTableRelation.partitionCols

  // CPU expression to prune Hive partitions, based on [[partitionPruningPredicate]].
  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private lazy val boundPartitionPruningPredOnCPU =
    partitionPruningPredicate.reduceLeftOption(And).map { pred =>
      require(pred.dataType == BooleanType,
        s"Data type of predicate $pred must be ${BooleanType.catalogString} rather than " +
          s"${pred.dataType.catalogString}.")

    BindReferences.bindReference(pred, hiveTableRelation.partitionCols)
  }

  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(hiveTableRelation.tableMeta)

  private def castFromString(value: String, dataType: DataType) = {
    cast(Literal(value), dataType).eval(null)
  }

  /**
   * Prunes partitions not involve the query plan.
   *
   * @param partitions All partitions of the relation.
   * @return Partitions that are involved in the query plan.
   */
  private[hive] def prunePartitions(partitions: Seq[HivePartition]): Seq[HivePartition] = {
    boundPartitionPruningPredOnCPU match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = hiveTableRelation.partitionCols.map(_.dataType)
        val castedValues = part.getValues.asScala.zip(dataTypes)
          .map { case (value, dataType) => castFromString(value, dataType) }.toSeq

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = InternalRow.fromSeq(castedValues)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  @transient lazy val prunedPartitions: Seq[HivePartition] = {
    if (hiveTableRelation.prunedPartitions.nonEmpty) {
      val hivePartitions =
        hiveTableRelation.prunedPartitions.get.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
      if (partitionPruningPredicate.forall(!ExecSubqueryExpression.hasSubquery(_))) {
        hivePartitions
      } else {
        prunePartitions(hivePartitions)
      }
    } else {
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPredicate.nonEmpty) {
        rawPartitions
      } else {
        prunePartitions(rawPartitions)
      }
    }
  }

  // exposed for tests
  @transient lazy val rawPartitions: Seq[HivePartition] = {
    val prunedPartitions =
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPredicate.nonEmpty) {
        // Retrieve the original attributes based on expression ID so that capitalization matches.
        val normalizedFilters = partitionPruningPredicate.map(_.transform {
          case a: AttributeReference => originalAttributes(a)
        })
        SparkShimImpl.listPartitionsByFilter(
          sparkSession,
          hiveTableRelation.tableMeta.identifier,
          normalizedFilters,
          Some(hiveTableRelation.tableMeta))
      } else {
        SparkShimImpl.listPartitions(
          sparkSession,
          hiveTableRelation.tableMeta.identifier,
          None,
          Some(hiveTableRelation.tableMeta))
      }
    prunedPartitions.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    "numFiles" -> createMetric(ESSENTIAL_LEVEL, "number of files read"),
    "metadataTime" -> createTimingMetric(ESSENTIAL_LEVEL, "metadata time"),
    "filesSize" -> createSizeMetric(ESSENTIAL_LEVEL, "size of files read"),
    GPU_DECODE_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_DECODE_TIME),
    BUFFER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    FILTER_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME),
    BUFFER_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_BUFFER_TIME_BUBBLE),
    FILTER_TIME_BUBBLE -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_FILTER_TIME_BUBBLE),
    SCAN_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_SCAN_TIME)
  )

  private lazy val driverMetrics: mutable.HashMap[String, Long] = mutable.HashMap.empty

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

  private def buildReader(sqlConf: SQLConf,
                          broadcastConf: Broadcast[SerializableConfiguration],
                          rapidsConf: RapidsConf,
                          dataSchema: StructType,
                          partitionSchema: StructType,
                          readSchema: StructType,
                          options: Map[String, String]
              ): PartitionedFile => Iterator[InternalRow] = {
    val readerFactory = GpuHiveTextPartitionReaderFactory(
      sqlConf = sqlConf,
      broadcastConf = broadcastConf,
      inputFileSchema = dataSchema,
      partitionSchema = partitionSchema,
      requestedOutputDataSchema = readSchema,
      requestedAttributes = requestedAttributes,
      maxReaderBatchSizeRows = rapidsConf.maxReadBatchSizeRows,
      maxReaderBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes,
      maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes,
      metrics = allMetrics,
      params = options
    )

    PartitionReaderIterator.buildReader(readerFactory)
  }

  /**
   * Prune output projection to those columns that are to be read from
   * the input file/buffer, in the same order as [[requestedAttributes]]
   * Removes partition columns, and returns the resultant schema
   * as a [[StructType]].
   */
  private def getRequestedOutputDataSchema(tableSchema: StructType,
                                           partitionAttributes: Seq[Attribute],
                                           requestedAttributes: Seq[Attribute]): StructType = {
    // Read schema in the same order as requestedAttributes.
    // Note: This might differ from the column order in `tableSchema`.
    //       In fact, HiveTableScanExec specifies `requestedAttributes` alphabetically.
    val partitionKeys: HashSet[String] = HashSet() ++ partitionAttributes.map(_.name)
    val requestedCols = requestedAttributes.filter(a => !partitionKeys.contains(a.name))
                                           .toList
    val distinctColumns = requestedCols.distinct
    // In hive column names are case-insensitive but the default tableSchema lookup is
    // case-sensitive
    val fieldMap = CaseInsensitiveMap(tableSchema.map(f => (f.name, f)).toMap)
    val distinctFields  = distinctColumns.map(a => fieldMap(a.name))
    StructType(distinctFields)
  }

  private def createReadRDDForDirectories(readFile: PartitionedFile => Iterator[InternalRow],
                                          directories: Seq[(URI, InternalRow)],
                                          readSchema: StructType,
                                          sparkSession: SparkSession,
                                          hadoopConf: Configuration): RDD[ColumnarBatch] = {
    def isNonEmptyDataFile(f: FileStatus): Boolean = {
      if (!f.isFile || f.getLen == 0) {
        false
      } else {
        val name = f.getPath.getName
        !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
      }
    }

    val selectedPartitions: Array[PartitionDirectory] = directories.map {
      case (directory, partValues) =>
        val path               = new Path(directory)
        val fs                 = path.getFileSystem(hadoopConf)
        val dirContents        = fs.listStatus(path).filter(isNonEmptyDataFile)
        PartitionDirectory(partValues, dirContents)
    }.toArray

    val maxSplitBytes      = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)

    val splitFiles = FilePartitionShims.splitFiles(sparkSession, hadoopConf,
      selectedPartitions, maxSplitBytes)

    val filePartitions = FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)

    // TODO [future]: Handle small-file optimization.
    //                (https://github.com/NVIDIA/spark-rapids/issues/7017)
    //                Currently assuming per-file reading.
    SparkShimImpl.getFileScanRDD(sparkSession, readFile, filePartitions, readSchema)
                 .asInstanceOf[RDD[ColumnarBatch]]
  }

  private def createReadRDDForTable(
                readFile: PartitionedFile => Iterator[InternalRow],
                hiveTableRelation: HiveTableRelation,
                readSchema: StructType,
                sparkSession: SparkSession,
                hadoopConf: Configuration
              ): RDD[ColumnarBatch] = {
    val tableLocation: URI = hiveTableRelation.tableMeta.storage.locationUri.getOrElse{
      throw new UnsupportedOperationException("Table path not set.")
    }
    // No need to check if table directory exists.
    // FileSystem.listStatus() handles this for GpuHiveTableScanExec,
    // just like for Apache Spark.
    createReadRDDForDirectories(readFile,
                                Array((tableLocation, InternalRow.empty)),
                                readSchema,
                                sparkSession,
                                hadoopConf)
  }

  private def createReadRDDForPartitions(
                readFile: PartitionedFile => Iterator[InternalRow],
                hiveTableRelation: HiveTableRelation,
                readSchema: StructType,
                sparkSession: SparkSession,
                hadoopConf: Configuration
              ): RDD[ColumnarBatch] = {
    val partitionColTypes = hiveTableRelation.partitionCols.map(_.dataType)
    val dirsWithPartValues = prunedPartitions.map { p =>
      // No need to check if partition directory exists.
      // FileSystem.listStatus() handles this for GpuHiveTableScanExec,
      // just like for Apache Spark.
      val uri = p.getDataLocation.toUri
      val partValues: Seq[Any] = {
        p.getValues.asScala.zip(partitionColTypes).map {
          case (value, dataType) => castFromString(value, dataType)
        }.toSeq
      }
      val partValuesAsInternalRow = InternalRow.fromSeq(partValues)

      (uri, partValuesAsInternalRow)
    }

    createReadRDDForDirectories(readFile,
      dirsWithPartValues, readSchema, sparkSession, hadoopConf)
  }

  lazy val inputRDD: RDD[ColumnarBatch] = {
    // Assume Delimited text.
    val options                   = hiveTableRelation.tableMeta.properties ++
                                    hiveTableRelation.tableMeta.storage.properties
    val hadoopConf                = sparkSession.sessionState.newHadoopConf()
    // In the CPU HiveTableScanExec the config will have a bunch of confs set for S3 keys
    // and predicate push down/etc. We don't need this because we are getting that information
    // directly.
    val broadcastHadoopConf       = sparkSession.sparkContext.broadcast(
                                      new SerializableConfiguration(hadoopConf))
    val sqlConf                   = sparkSession.sessionState.conf
    val rapidsConf                = new RapidsConf(sqlConf)
    val requestedOutputDataSchema = getRequestedOutputDataSchema(hiveTableRelation.tableMeta.schema,
                                                                 partitionAttributes,
                                                                 requestedAttributes)
    val reader                    = buildReader(sqlConf,
                                                broadcastHadoopConf,
                                                rapidsConf,
                                                hiveTableRelation.tableMeta.dataSchema,
                                                hiveTableRelation.tableMeta.partitionSchema,
                                                requestedOutputDataSchema,
                                                options)
    val rdd = if (hiveTableRelation.isPartitioned) {
      createReadRDDForPartitions(reader, hiveTableRelation, requestedOutputDataSchema,
                                 sparkSession, hadoopConf)
    } else {
      createReadRDDForTable(reader, hiveTableRelation, requestedOutputDataSchema,
                            sparkSession, hadoopConf)
    }
    sendDriverMetrics()
    rdd
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val scanTime = gpuLongMetric(SCAN_TIME)
    inputRDD.mapPartitionsInternal { batches =>
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

  override def doCanonicalize(): GpuHiveTableScanExec = {
    val input: AttributeSeq = hiveTableRelation.output
    GpuHiveTableScanExec(
      requestedAttributes.map(QueryPlan.normalizeExpressions(_, input)),
      hiveTableRelation.canonicalized.asInstanceOf[HiveTableRelation],
      QueryPlan.normalizePredicates(partitionPruningPredicate, input))
  }
}

/**
 * Partition-reader styled similarly to `ColumnarPartitionReaderWithPartitionValues`,
 * but orders the output columns alphabetically.
 * This is required since the [[GpuHiveTableScanExec.requestedAttributes]] have the columns
 * ordered alphabetically by name, even though the table schema (and hence, the file-schema)
 * need not.
 */
class AlphabeticallyReorderingColumnPartitionReader(fileReader: PartitionReader[ColumnarBatch],
                                                    partitionValues: InternalRow,
                                                    partitionSchema: StructType,
                                                    maxGpuColumnSizeBytes: Long,
                                                    requestedAttributes: Seq[Attribute])
  extends ColumnarPartitionReaderWithPartitionValues(fileReader,
                                                     partitionValues,
                                                     partitionSchema,
                                                     maxGpuColumnSizeBytes) {
  override def get(): ColumnarBatch = {
    val fileBatch: ColumnarBatch = super.get()
    if (partitionSchema.isEmpty) {
      return fileBatch
    }

    // super.get() returns columns specified in `requestedAttributes`,
    // but ordered according to `tableSchema`. Must reorder, based on `requestedAttributes`.
    // Also, super.get() appends *all* partition keys, even if they do not belong
    // in the output projection. Must discard unused partition keys here.
    withResource(fileBatch) { fileBatch =>
      var dataColumnIndex = 0
      val partitionColumnStartIndex = fileBatch.numCols() - partitionValues.numFields
      val partitionKeys = partitionSchema.map(_.name).toList
      val reorderedColumns = requestedAttributes.map { a =>
        val partIndex = partitionKeys.indexOf(a.name)
        if (partIndex == -1) {
          // Not a partition column.
          dataColumnIndex += 1
          fileBatch.column(dataColumnIndex - 1)
        }
        else {
          // Partition key.
          fileBatch.column(partitionColumnStartIndex + partIndex)
        }
      }.toArray
      for (col <- reorderedColumns) { col.asInstanceOf[GpuColumnVector].incRefCount() }
      new ColumnarBatch(reorderedColumns, fileBatch.numRows())
    }
  }
}

// Factory to build the columnar reader.
case class GpuHiveTextPartitionReaderFactory(sqlConf: SQLConf,
                                             broadcastConf: Broadcast[SerializableConfiguration],
                                             inputFileSchema: StructType,
                                             partitionSchema: StructType,
                                             requestedOutputDataSchema: StructType,
                                             requestedAttributes: Seq[Attribute],
                                             maxReaderBatchSizeRows: Integer,
                                             maxReaderBatchSizeBytes: Long,
                                             maxGpuColumnSizeBytes: Long,
                                             metrics: Map[String, GpuMetric],
                                             params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("Row-based text parsing is not supported on GPU.")
  }

  private val csvOptions = new CSVOptions(params,
                                          sqlConf.csvColumnPruning,
                                          sqlConf.sessionLocalTimeZone,
                                          sqlConf.columnNameOfCorruptRecord)

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastConf.value.value
    val reader = new PartitionReaderWithBytesRead(
                   new GpuHiveDelimitedTextPartitionReader(
                     conf, csvOptions, params, partFile, inputFileSchema,
                     requestedOutputDataSchema, maxReaderBatchSizeRows,
                     maxReaderBatchSizeBytes, metrics))
    new AlphabeticallyReorderingColumnPartitionReader(reader,
                                                      partFile.partitionValues,
                                                      partitionSchema,
                                                      maxGpuColumnSizeBytes,
                                                      requestedAttributes)
  }
}

// Reader that converts from chunked data buffers into cudf.Table.
class GpuHiveDelimitedTextPartitionReader(conf: Configuration,
                                          csvOptions: CSVOptions,
                                          params: Map[String, String],
                                          partFile: PartitionedFile,
                                          inputFileSchema: StructType,
                                          requestedOutputDataSchema: StructType,
                                          maxRowsPerChunk: Integer,
                                          maxBytesPerChunk: Long,
                                          execMetrics: Map[String, GpuMetric]) extends
    CSVPartitionReaderBase[HostStringColBufferer, HostStringColBuffererFactory.type](conf, partFile,
      inputFileSchema, requestedOutputDataSchema, csvOptions, maxRowsPerChunk,
      maxBytesPerChunk, execMetrics, HostStringColBuffererFactory) {

  override def readToTable(dataBufferer: HostStringColBufferer,
                           cudfDataSchema: Schema,
                           requestedOutputDataSchema: StructType,
                           cudfReadDataSchema: Schema,
                           isFirstChunk: Boolean,
                           decodeTime: GpuMetric): Table = {
    NvtxIdWithMetrics(NvtxRegistry.HIVE_DECODE, decodeTime) {
      // The delimiter is currently hard coded to ^A. This should be able to support any format
      //  but we don't want to test that yet
      val splitTable = withResource(dataBufferer.getColumnAndRelease) { cv =>
        cv.stringSplit("\u0001")
      }

      // inputFileCudfSchema       == Schema of the input file/buffer.
      //                              Presented in the order of input columns in the file.
      // requestedOutputDataSchema == Spark output schema. This is inexplicably sorted
      //                              alphabetically in HiveTSExec, unlike FileSourceScanExec
      //                              (which has file-input ordering).
      //                              This trips up the downstream string->numeric casts in
      //                              GpuTextBasedPartitionReader.readToTable().
      // Given that Table.readCsv presents the output columns in the order of the input file,
      // we need to reorder the table read from the input file in the order specified in
      // [[requestedOutputDataSchema]] (i.e. requiredAttributes).

      withResource(splitTable) { _ =>
        val nullFormat = params.getOrElse("serialization.null.format", "\\N")
        withResource(Scalar.fromString(nullFormat)) { nullTag =>
          withResource(Scalar.fromNull(DType.STRING)) { nullVal =>
            // This is a bit different because we are dropping columns/etc ourselves
            val requiredColumnSequence = requestedOutputDataSchema.map(_.name).toList
            val outputColumnNames = cudfDataSchema.getColumnNames
            val reorderedColumns = requiredColumnSequence.safeMap { colName =>
              val colIndex = outputColumnNames.indexOf(colName)
              if (splitTable.getNumberOfColumns > colIndex) {
                val col = splitTable.getColumn(colIndex)
                withResource(col.equalTo(nullTag)) { shouldBeNull =>
                  shouldBeNull.ifElse(nullVal, col)
                }
              } else {
                // the column didn't exist in the output, so we need to make an all null one
                ColumnVector.fromScalar(nullVal, splitTable.getRowCount.toInt)
              }
            }
            withResource(reorderedColumns) { _ =>
              new Table(reorderedColumns: _*)
            }
          }
        }
      }
    }
  }

  override def castStringToBool(input: ColumnVector): ColumnVector = {
    // This is here to try and make it simple to support extends boolean support in the future.
    val (trueVals, falseVals) = (Array("true"), Array("false"))

    val (isTrue, isFalse) = withResource(input.lower()) { lowered =>
      // True if it is a true value, false if it is not
      val isTrue = withResource(ColumnVector.fromStrings(trueVals: _*)) { trueValsCol =>
        lowered.contains(trueValsCol)
      }
      closeOnExcept(isTrue) { _ =>
        val isFalse = withResource(ColumnVector.fromStrings(falseVals: _*)) { falseValsCol =>
          lowered.contains(falseValsCol)
        }
        (isTrue, isFalse)
      }
    }
    withResource(isTrue) { _ =>
      val tOrF = withResource(isFalse) { _ =>
        isTrue.or(isFalse)
      }
      withResource(tOrF) { _ =>
        withResource(Scalar.fromNull(DType.BOOL8)) { ns =>
          tOrF.ifElse(isTrue, ns)
        }
      }
    }
  }

  override def castStringToInt(input: ColumnVector, intType: DType): ColumnVector =
    CastStrings.toInteger(input, false, false, intType)

  override def castStringToDecimal(input: ColumnVector, dt: DecimalType): ColumnVector =
    CastStrings.toDecimal(input, false, false, dt.precision, -dt.scale)

  /**
   * Override of [[com.nvidia.spark.rapids.GpuTextBasedPartitionReader.castStringToDate()]],
   * to convert parsed string columns to Dates.
   * Two key differences from the base implementation, to comply with Hive LazySimpleSerDe
   * semantics:
   *   1. The input strings are not trimmed of whitespace.
   *   2. Invalid date strings do not cause exceptions.
   */
  override def castStringToDate(input: ColumnVector, dt: DType): ColumnVector = {
    // Filter out any dates that do not conform to the `yyyy-MM-dd` format.
    val supportedDateRegex = raw"\A\d{4}-\d{2}-\d{2}\Z"
    val prog = new RegexProgram(supportedDateRegex, CaptureGroups.NON_CAPTURE)
    val regexFiltered = withResource(input.matchesRe(prog)) { matchesRegex =>
      withResource(Scalar.fromNull(DType.STRING)) { nullString =>
        matchesRegex.ifElse(input, nullString)
      }
    }

    withResource(regexFiltered) { _ =>
      val cudfFormat = DateUtils.toStrf("yyyy-MM-dd", parseString = true)
      withResource(regexFiltered.isTimestamp(cudfFormat)) { isDate =>
        withResource(regexFiltered.asTimestamp(dt, cudfFormat)) { asDate =>
          withResource(Scalar.fromNull(dt)) { nullScalar =>
            isDate.ifElse(asDate, nullScalar)
          }
        }
      }
    }
  }

  override def castStringToTimestamp(lhs: ColumnVector, sparkFormat: String, dType: DType)
  : ColumnVector = {
    // Currently, only the following timestamp pattern is supported:
    //  "uuuu-MM-dd HH:mm:ss[.SSS...]"
    // Note: No support for "uuuu-MM-dd'T'HH:mm:ss[.SSS...][Z]", or any customization.
    // See https://github.com/NVIDIA/spark-rapids/issues/7289.

    // Input strings that do not match this format strictly must be replaced with nulls.
    //                 yyyy-  MM -  dd    HH  :  mm  :  ss [SSS...     ]
    val regex = raw"\A\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2}(?:\.\d{1,9})?\Z"
    val prog = new RegexProgram(regex, CaptureGroups.NON_CAPTURE)
    val regexFiltered = withResource(lhs.matchesRe(prog)) { matchesRegex =>
      withResource(Scalar.fromNull(DType.STRING)) { nullString =>
        matchesRegex.ifElse(lhs, nullString)
      }
    }

    // For rows that pass the regex check, parse them as timestamp.
    def asTimestamp(format: String) = {
      withResource(regexFiltered.isTimestamp(format)) { isTimestamp =>
        withResource(regexFiltered.asTimestamp(dType, format)) { timestamp =>
          withResource(Scalar.fromNull(dType)) { nullTimestamp =>
            isTimestamp.ifElse(timestamp, nullTimestamp)
          }
        }
      }
    }

    // Attempt to parse at "sub-second" level first.
    // Substitute rows that fail at "sub-second" with "second" level.
    // Those that fail both should remain as nulls.
    withResource(regexFiltered) { _ =>
      withResource(asTimestamp(format = "%Y-%m-%d %H:%M:%S.%f")) { timestampsSubSecond =>
        withResource(asTimestamp(format = "%Y-%m-%d %H:%M:%S")) { timestampsSecond =>
          timestampsSubSecond.replaceNulls(timestampsSecond)
        }
      }
    }
  }
}
