/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import java.io.{ByteArrayInputStream, FileNotFoundException, IOException, OutputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util
import java.util.regex.Pattern

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.collection.mutable
import scala.language.implicitConversions

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.SchemaUtils._
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.fileio.hadoop.HadoopFileIO
import com.nvidia.spark.rapids.io.async._
import com.nvidia.spark.rapids.jni.{CastStrings, RmmSpark}
import com.nvidia.spark.rapids.shims.{ColumnDefaultValuesShims, GpuOrcDataReader, NullOutputStreamShim, OrcCastingShims, OrcReadingShims, OrcShims, ShimFilePartitionReaderFactory}
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.hadoop.io.Text
import org.apache.orc.{CompressionKind, DataReader, FileFormatException, OrcConf, OrcFile, OrcProto, PhysicalWriter, Reader, StripeInformation, TypeDescription}
import org.apache.orc.impl._
import org.apache.orc.impl.RecordReaderImpl.SargApplier
import org.apache.orc.mapred.OrcInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeConstants}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.datasources.rapids.OrcFiltersWrapper
import org.apache.spark.sql.execution.datasources.v2.{EmptyPartitionReader, FileScan}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, CharType, DataType, DecimalType, MapType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

case class GpuOrcScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean = false)
  extends FileScan with GpuScan with Logging {

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    // Unset any serialized search argument setup by Spark's OrcScanBuilder as
    // it will be incompatible due to shading and potential ORC classifier mismatch.
    hadoopConf.unset(OrcConf.KRYO_SARG.getAttribute)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    if (rapidsConf.isOrcPerFileReadEnabled) {
      logInfo("Using the original per file orc reader")
      GpuOrcPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        options.asScala.toMap)
    } else {
      val poolConfBuilder = ThreadPoolConfBuilder(rapidsConf)
      GpuOrcMultiFilePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf,
        poolConfBuilder,
        metrics, queryUsesInputFile)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: GpuOrcScan =>
      super.equals(o) && dataSchema == o.dataSchema && options == o.options &&
          equivalentFilters(pushedFilters, o.pushedFilters) &&
          rapidsConf.isOrcFloatTypesToStringEnable ==
              o.rapidsConf.isOrcFloatTypesToStringEnable &&
          queryUsesInputFile == o.queryUsesInputFile
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def withInputFile(): GpuScan = copy(queryUsesInputFile = true)
}

object GpuOrcScan {

  def tagSupport(scanMeta: ScanMeta[OrcScan]): Unit = {
    val scan = scanMeta.wrapped
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    tagSupport(scan.sparkSession, schema, scanMeta)
  }

  def tagSupport(
      sparkSession: SparkSession,
      schema: StructType,
      meta: RapidsMeta[_, _, _]): Unit = {
    if (!meta.conf.isOrcEnabled) {
      meta.willNotWorkOnGpu("ORC input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_ORC} to true")
    }

    if (!meta.conf.isOrcReadEnabled) {
      meta.willNotWorkOnGpu("ORC input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_ORC_READ} to true")
    }

    if (ColumnDefaultValuesShims.hasExistenceDefaultValues(schema)) {
      meta.willNotWorkOnGpu("GpuOrcScan does not support default values in schema")
    }

    if (!meta.conf.orcReadIgnoreWriterTimezone) {
      // For timestamp type, timezone needs to be checked.
      // This is because JVM timezone and UTC timezone offset is considered when
      // reading timestamp type from ORC file.
      val types = schema.map(_.dataType).toSet
      if (types.exists(GpuOverrides.isOrContainsTimestamp)) {
        if (!GpuOverrides.isUTCTimezone()) {
          // When reading timestamp type from an ORC file, it's not related to the
          // Spark session timezone but only the JVM timezone.
          meta.willNotWorkOnGpu("Only UTC timezone is supported for ORC. " +
            s"Current timezone settings: (JVM : ${ZoneId.systemDefault()}")
        }
      }
    } else {
      // Ignore the write timezones in the stripe footers, we support, skip the checks.
    }

    FileFormatChecks.tag(meta, schema, OrcFormatType, ReadFileOp)
  }

  private lazy val numericLevels = Seq(
    DType.DTypeEnum.BOOL8,
    DType.DTypeEnum.INT8,
    DType.DTypeEnum.INT16,
    DType.DTypeEnum.INT32,
    DType.DTypeEnum.INT64,
    DType.DTypeEnum.FLOAT32,
    DType.DTypeEnum.FLOAT64,
    DType.DTypeEnum.DECIMAL32,
    DType.DTypeEnum.DECIMAL64,
    DType.DTypeEnum.DECIMAL128
  ).zipWithIndex.toMap

  /**
   * Cast the input column to the target type, and replace overflow rows with nulls.
   * Only for conversion between integral types.
   */
  private def downCastAnyInteger(col: ColumnView, toType: DType): ColumnVector = {
    // Overflow happens in b when
    //    val b = (toDt)a
    //    val c = (fromDt)b
    //    c != a
    withResource(col.castTo(toType)) { casted =>
      val overflowFlags = withResource(casted.castTo(col.getType)) { backed =>
        col.equalTo(backed)
      }
      // Replace values that cause overflow with nulls, same with CPU ORC.
      withResource(overflowFlags) { _ =>
        // This is an integer type so we don't have to worry about
        // nested DTypes here.
        withResource(Scalar.fromNull(toType)) { nullVal =>
          overflowFlags.ifElse(casted, nullVal)
        }
      }
    }
  }

  /**
   * Get the overflow flags in booleans.
   * true means no overflow, while false means getting overflow.
   *
   * @param doubleMillis the input double column
   * @param millis the long column casted from the doubleMillis
   */
  private def getOverflowFlags(doubleMillis: ColumnView, millis: ColumnView): ColumnView = {
    // No overflow when
    //     doubleMillis <= Long.MAX_VALUE &&
    //     doubleMillis >= Long.MIN_VALUE &&
    //     ((millis >= 0) == (doubleMillis >= 0))
    val rangeCheck = withResource(Scalar.fromLong(Long.MaxValue)) { max =>
      withResource(doubleMillis.lessOrEqualTo(max)) { upperCheck =>
        withResource(Scalar.fromLong(Long.MinValue)) { min =>
          withResource(doubleMillis.greaterOrEqualTo(min)) { lowerCheck =>
            upperCheck.and(lowerCheck)
          }
        }
      }
    }
    withResource(rangeCheck) { _ =>
      val signCheck = withResource(Scalar.fromInt(0)) { zero =>
        withResource(millis.greaterOrEqualTo(zero)) { longSign =>
          withResource(doubleMillis.greaterOrEqualTo(zero)) { doubleSign =>
            longSign.equalTo(doubleSign)
          }
        }
      }
      withResource(signCheck) { _ =>
        rangeCheck.and(signCheck)
      }
    }
  }

  /**
   * Borrowed from ORC "ConvertTreeReaderFactory"
   * Scala does not support such numeric literal, so parse from string.
   */
  private val MIN_LONG_AS_DOUBLE = java.lang.Double.valueOf("-0x1p63")

  /**
   * We cannot store Long.MAX_VALUE as a double without losing precision. Instead, we store
   * Long.MAX_VALUE + 1 == -Long.MIN_VALUE, and then offset all comparisons by 1.
   */
  private val MAX_LONG_AS_DOUBLE_PLUS_ONE = java.lang.Double.valueOf("0x1p63")

  /**
   * Return a boolean column indicates whether the rows in col can fix in a long.
   * It assumes the input type is float or double.
   */
  private def doubleCanFitInLong(col: ColumnView): ColumnVector = {
    // It is true when
    //   (MIN_LONG_AS_DOUBLE - doubleValue < 1.0) &&
    //   (doubleValue < MAX_LONG_AS_DOUBLE_PLUS_ONE)
    val lowRet = withResource(Scalar.fromDouble(MIN_LONG_AS_DOUBLE)) { sMin =>
      withResource(Scalar.fromDouble(1.0)) { sOne =>
        withResource(sMin.sub(col)) { diff =>
          diff.lessThan(sOne)
        }
      }
    }
    withResource(lowRet) { _ =>
      withResource(Scalar.fromDouble(MAX_LONG_AS_DOUBLE_PLUS_ONE)) { sMax =>
        withResource(col.lessThan(sMax)) { highRet =>
          lowRet.and(highRet)
        }
      }
    }
  }


  /**
   * Cast the column to the target type for ORC schema evolution.
   * It is designed to support all the cases that `canCast` returns true.
   * Both of the column type and target type should be primitive.
   *
   * The returned column may be either the input or a new one, users should check and
   * close it when needed.
   */
  def castColumnTo(col: ColumnView, targetType: DataType, originalFromDt: DataType)
  : ColumnView = {
    val fromDt = col.getType
    val toDt = GpuColumnVector.getNonNestedRapidsType(targetType)
    if (fromDt == toDt &&
       !(targetType == StringType && originalFromDt.isInstanceOf[CharType])) {
      return col
    }
    (fromDt, toDt) match {
      // integral to integral
      case (DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32 | DType.INT64,
          DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32 | DType.INT64) =>
        if (numericLevels(fromDt.getTypeId) <= numericLevels(toDt.getTypeId) ||
            toDt == DType.BOOL8) {
          // no downcast
          col.castTo(toDt)
        } else {
          downCastAnyInteger(col, toDt)
        }

      // bool to float, double(float64)
      case (DType.BOOL8, DType.FLOAT32 | DType.FLOAT64) =>
        col.castTo(toDt)

      // bool to string
      case (DType.BOOL8, DType.STRING) =>
        withResource(col.castTo(toDt)) { casted =>
          // cuDF produces "true"/"false" while CPU outputs "TRUE"/"FALSE".
          casted.upper()
        }

      // integer to float, double(float64), string
      case (DType.INT8 | DType.INT16 | DType.INT32 | DType.INT64,
      DType.FLOAT32 | DType.FLOAT64 | DType.STRING) =>
        col.castTo(toDt)

      // {bool, integer types} to timestamp(micro seconds)
      case (DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32 | DType.INT64,
      DType.TIMESTAMP_MICROSECONDS) =>
        OrcCastingShims.castIntegerToTimestamp(col, fromDt)

      // float to bool/integral
      case (DType.FLOAT32 | DType.FLOAT64, DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32
                                           | DType.INT64) =>
        // Follow the CPU ORC conversion:
        //   First replace rows that cannot fit in long with nulls,
        //   next convert to long,
        //   then down cast long to the target integral type.
        val longDoubles = withResource(doubleCanFitInLong(col)) { fitLongs =>
          withResource(Scalar.fromNull(fromDt)) { nullVal =>
            fitLongs.ifElse(col, nullVal)
          }
        }
        withResource(longDoubles) { _ =>
          withResource(longDoubles.castTo(DType.INT64)) { longs =>
            toDt match {
              case DType.BOOL8 => longs.castTo(toDt)
              case DType.INT64 => longs.incRefCount()
              case _ => downCastAnyInteger(longs, toDt)
            }
          }
        }

      // float/double to double/float
      case (DType.FLOAT32 | DType.FLOAT64, DType.FLOAT32 | DType.FLOAT64) =>
        col.castTo(toDt)

      // float/double to string
      // When casting float/double to string, the result of GPU is different from CPU.
      // We let a conf 'spark.rapids.sql.format.orc.floatTypesToString.enable' to control it's
      // enable or not.
      case (DType.FLOAT32 | DType.FLOAT64, DType.STRING) =>
        CastStrings.fromFloat(col)

      // float/double -> timestamp
      case (DType.FLOAT32 | DType.FLOAT64, DType.TIMESTAMP_MICROSECONDS) =>
        // Follow the CPU ORC conversion.
        //     val doubleMillis = doubleValue * 1000,
        //     val milliseconds = Math.round(doubleMillis)
        //     if (noOverflow) { milliseconds } else { null }

        // java.lang.Math.round is a true half up, meaning rounding towards positive infinity
        // even for negative numbers
        //   assert(Math.round(-1.5) = -1
        //   assert(Math.round(1.5) = 2
        //
        // libcudf, Spark implement it half up in a half away from zero fashion
        // >> sql("SELECT ROUND(-1.5D, 0), ROUND(-0.5D, 0), ROUND(0.5D, 0)").show(truncate=False)
        // +--------------+--------------+-------------+
        // |round(-1.5, 0)|round(-0.5, 0)|round(0.5, 0)|
        // +--------------+--------------+-------------+
        // |-2.0          |-1.0          |1.0          |
        // +--------------+--------------+-------------+
        //
        // Math.round half up can be implemented in terms of floor
        // Math.round(x) = n iff x is in [n-0.5, n+0.5) iff x+0.5 is in [n,n+1) iff floor(x+0.5) = n
        //
        val milliseconds = withResource(Scalar.fromDouble(DateTimeConstants.MILLIS_PER_SECOND)) {
          thousand =>
          // ORC assumes value is in seconds
          withResource(col.mul(thousand, DType.FLOAT64)) { doubleMillis =>
            withResource(Scalar.fromDouble(0.5)) { half =>
              withResource(doubleMillis.add(half)) { doubleMillisPlusHalf =>
                withResource(doubleMillisPlusHalf.floor()) { millis =>
                  withResource(getOverflowFlags(doubleMillis, millis)) { overflowFlags =>
                    withResource(Scalar.fromNull(millis.getType)) { nullVal =>
                      overflowFlags.ifElse(millis, nullVal)
                    }
                  }
                }
              }
            }
          }
        }

        // Cast milli-seconds to micro-seconds
        // We need to pay attention that when convert (milliSeconds * 1000) to INT64, there may be
        // INT64-overflow.
        // In this step, ORC casting of CPU throw an exception rather than replace such values with
        // null. We followed the CPU code here.
        withResource(milliseconds) { _ =>
          // Test whether if there is long-overflow towards positive and negative infinity
          withResource(milliseconds.max()) { maxValue =>
            withResource(milliseconds.min()) { minValue =>
              Seq(maxValue, minValue).foreach { extremum =>
                if (extremum.isValid) {
                  testLongMultiplicationOverflow(extremum.getDouble.toLong,
                    DateTimeConstants.MICROS_PER_MILLIS)
                }
              }
            }
          }
          withResource(Scalar.fromDouble(DateTimeConstants.MICROS_PER_MILLIS)) { thousand =>
            withResource(milliseconds.mul(thousand)) { microseconds =>
                withResource(microseconds.castTo(DType.INT64)) { longVec =>
                  longVec.castTo(DType.TIMESTAMP_MICROSECONDS)
                }
            }
          }
        }

      case (f: DType, t: DType) if f.isDecimalType && t.isDecimalType =>
        val fromDataType = DecimalType(f.getDecimalMaxPrecision, -f.getScale)
        val toDataType = DecimalType(t.getDecimalMaxPrecision, -t.getScale)
        GpuCast.doCast(col, fromDataType, toDataType)

      case (DType.STRING, DType.STRING) if originalFromDt.isInstanceOf[CharType] =>
        // Trim trailing whitespace off of output strings, to match CPU output.
        col.rstrip()

      // TODO more types, tracked in https://github.com/NVIDIA/spark-rapids/issues/5895
      case (f, t) =>
        throw new QueryExecutionException(s"Unsupported type casting: $f -> $t")
    }
  }

  /**
   * Whether the type casting is supported by GPU ORC reading.
   *
   * No need to support the whole list that CPU does in "ConvertTreeReaderFactory.canConvert",
   * but the ones between GPU supported types.
   * Each supported casting is implemented in "castColumnTo".
   */
  def canCast(from: TypeDescription, to: TypeDescription,
              isOrcFloatTypesToStringEnable: Boolean): Boolean = {
    import org.apache.orc.TypeDescription.Category._
    if (!to.getCategory.isPrimitive || !from.getCategory.isPrimitive) {
      // Don't convert from any to complex, or from complex to any.
      // Align with what CPU does.
      return false
    }
    val toType = to.getCategory
    from.getCategory match {
      case BOOLEAN | BYTE | SHORT | INT | LONG =>
        toType match {
          case BOOLEAN | BYTE | SHORT | INT | LONG | FLOAT | DOUBLE | STRING |
               TIMESTAMP => true
          // BINARY and DATE are not supported by design.
          // The 'to' type (aka read schema) is from Spark, and VARCHAR and CHAR will
          // be replaced by STRING. Meanwhile, cuDF doesn't support them as output
          // types, and also replaces them with STRING.
          // TIMESTAMP_INSTANT is not supported by cuDF.
          case _ => false
        }
      case VARCHAR | CHAR =>
        toType == STRING

      case FLOAT | DOUBLE =>
        toType match {
          case BOOLEAN | BYTE | SHORT | INT | LONG | FLOAT | DOUBLE | TIMESTAMP => true
          case STRING => isOrcFloatTypesToStringEnable
          case _ => false
        }

      case DECIMAL => toType == DECIMAL

      // TODO more types, tracked in https://github.com/NVIDIA/spark-rapids/issues/5895
      case _ =>
        false
    }
  }

  /**
   * Test whether if a * b will cause Long-overflow.
   * In Math.multiplyExact, if there is an integer-overflow, then it will throw an
   * ArithmeticException.
   */
  private def testLongMultiplicationOverflow(a: Long, b: Long) = {
    Math.multiplyExact(a, b)
  }

  /**
   * Convert the integer vector into timestamp(microseconds) vector.
   * @param col The integer columnar vector.
   * @param colType Specific integer type, it should be BOOL/INT8/INT16/INT32/INT64.
   * @param timeUnit It should be one of {DType.TIMESTAMP_SECONDS, DType.TIMESTAMP_MILLISECONDS}.
   *                 If timeUnit == SECONDS, then we consider the integers as seconds.
   *                 If timeUnit == MILLISECONDS, then we consider the integers as milliseconds.
   *                 This parameter is determined by the shims.
   * @return A timestamp vector.
   */
  def castIntegersToTimestamp(col: ColumnView, colType: DType,
                              timeUnit: DType): ColumnVector = {
    assert(colType == DType.BOOL8 || colType == DType.INT8 || colType == DType.INT16
      || colType == DType.INT32 || colType == DType.INT64)
    assert(timeUnit == DType.TIMESTAMP_SECONDS || timeUnit == DType.TIMESTAMP_MILLISECONDS)

    colType match {
      case DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32 =>
        // cuDF requires casting to Long first, then we can cast Long to Timestamp(in microseconds)
        withResource(col.castTo(DType.INT64)) { longs =>
          // bitCastTo will re-interpret the long values as 'timeUnit', and it will zero-copy cast
          // between types with the same underlying length.
          withResource(longs.bitCastTo(timeUnit)) { timeView =>
            timeView.castTo(DType.TIMESTAMP_MICROSECONDS)
          }
        }
      case DType.INT64 =>
        // In CPU code of ORC casting, if the integers are consider as seconds, then the conversion
        // is 'integer -> milliseconds -> microseconds', and it checks the long-overflow when
        // casting 'milliseconds -> microseconds', here we follow it.
        val milliseconds = withResource(col.bitCastTo(timeUnit)) { timeView =>
          timeView.castTo(DType.TIMESTAMP_MILLISECONDS)
        }
        withResource(milliseconds) { _ =>
          // Check long-multiplication overflow
          withResource(milliseconds.max()) { maxValue =>
            // If the elements in 'milliseconds' are all nulls, then 'maxValue' and 'minValue' will
            // be null. We should check their validity.
            if (maxValue.isValid) {
              testLongMultiplicationOverflow(maxValue.getLong, DateTimeConstants.MICROS_PER_MILLIS)
            }
          }
          withResource(milliseconds.min()) { minValue =>
            if (minValue.isValid) {
              testLongMultiplicationOverflow(minValue.getLong, DateTimeConstants.MICROS_PER_MILLIS)
            }
          }
          milliseconds.castTo(DType.TIMESTAMP_MICROSECONDS)
        }
    }
  }
}

/**
 * The multi-file partition reader factory for creating cloud reading or coalescing reading for
 * ORC file format.
 *
 * @param sqlConf             the SQLConf
 * @param broadcastedConf     the Hadoop configuration
 * @param dataSchema          schema of the data
 * @param readDataSchema      the Spark schema describing what will be read
 * @param partitionSchema     schema of partitions.
 * @param filters             filters on non-partition columns
 * @param rapidsConf          the Rapids configuration
 * @param poolConfBuilder     the builder of ThreadPoolConf carries the driver-side settings
 * @param metrics             the metrics
 * @param queryUsesInputFile  this is a parameter to easily allow turning it
 *                            off in GpuTransitionOverrides if InputFileName,
 *                            InputFileBlockStart, or InputFileBlockLength are used
 */
case class GpuOrcMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    poolConfBuilder: ThreadPoolConfBuilder,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  private val debugDumpPrefix = rapidsConf.orcDebugDumpPrefix
  private val debugDumpAlways = rapidsConf.orcDebugDumpAlways
  private val maxNumFileProcessed = rapidsConf.maxNumOrcFilesParallel
  private val filterHandler = GpuOrcFileFilterHandler(sqlConf, metrics, broadcastedConf, filters,
    rapidsConf.isOrcFloatTypesToStringEnable)
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles
  private val combineThresholdSize = rapidsConf.getMultithreadedCombineThreshold
  private val combineWaitTime = rapidsConf.getMultithreadedCombineWaitTime
  private val keepReadsInOrder = rapidsConf.getMultithreadedReaderKeepOrder

  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  override val canUseCoalesceFilesReader: Boolean =
    rapidsConf.isOrcCoalesceFileReadEnabled && !(queryUsesInputFile || ignoreCorruptFiles)

  override val canUseMultiThreadReader: Boolean = rapidsConf.isOrcMultiThreadReadEnabled

  /**
   * Build the PartitionReader for cloud reading
   *
   * @param files files to be read
   * @param conf  configuration
   * @return cloud reading PartitionReader
   */
  override def buildBaseColumnarReaderForCloud(files: Array[PartitionedFile], conf: Configuration):
      PartitionReader[ColumnarBatch] = {
    val combineConf = CombineConf(combineThresholdSize, combineWaitTime)
    val poolConf = poolConfBuilder.build()
    val reader = new MultiFileCloudOrcPartitionReader(
      conf, files, dataSchema, readDataSchema, partitionSchema,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes,
      useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes, poolConf,
      maxNumFileProcessed,
      debugDumpPrefix, debugDumpAlways, filters, filterHandler, metrics, ignoreMissingFiles,
      ignoreCorruptFiles, queryUsesInputFile, keepReadsInOrder, combineConf)
    // NOTE: Initialize must happen after the initialization of the reader, to ensure everything
    // inside the reader being fully initialized.
    if (conf.getBoolean("rapids.sql.scan.prefetch", false)) {
      reader.eagerPrefetchInit()
    }
    reader
  }

  /**
   * Build the PartitionReader for coalescing reading
   *
   * @param files files to be read
   * @param conf  the configuration
   * @return coalescing reading PartitionReader
   */
  override def buildBaseColumnarReaderForCoalescing(files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    // Coalescing reading can't coalesce orc files with different compression kind, which means
    // we must split the different compress files into different ColumnarBatch.
    // So here try the best to group the same compression files together before hand.
    val compressionAndStripes = LinkedHashMap[CompressionKind, ArrayBuffer[OrcSingleStripeMeta]]()

    metrics.getOrElse(FILTER_TIME, NoopMetric).ns {
      metrics.getOrElse(SCAN_TIME, NoopMetric).ns {
        files.foreach { file =>
          try {
            val orcPartitionReaderContext = filterHandler.filterStripes(file, dataSchema,
              readDataSchema, partitionSchema)
            // filterStripes returns null for an empty ORC file; it has no stripes to
            // contribute, so skip it (the single-file path uses an EmptyPartitionReader).
            if (orcPartitionReaderContext != null) {
              compressionAndStripes.getOrElseUpdate(orcPartitionReaderContext.compressionKind,
                new ArrayBuffer[OrcSingleStripeMeta]) ++=
                orcPartitionReaderContext.blockIterator.map(block =>
                  OrcSingleStripeMeta(
                    orcPartitionReaderContext.filePath,
                    OrcDataStripe(OrcStripeWithMeta(block, orcPartitionReaderContext)),
                    file.partitionValues,
                    OrcSchemaWrapper(orcPartitionReaderContext.updatedReadSchema),
                    readDataSchema,
                    OrcExtraInfo(orcPartitionReaderContext.requestedMapping)))
            }
          } catch {
            case e: FileNotFoundException if ignoreMissingFiles =>
              logWarning(s"Skipped missing file: ${file.filePath}", e)
          }
        }
      }
    }

    val clippedStripes = compressionAndStripes.values.flatten.toSeq
    val poolConf = poolConfBuilder.build()
    new MultiFileOrcPartitionReader(conf, files, clippedStripes, readDataSchema,
      debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
      targetBatchSizeBytes, maxGpuColumnSizeBytes, useChunkedReader,
      maxChunkedReaderMemoryUsageSizeBytes,
      metrics, partitionSchema, poolConf, filterHandler.isCaseSensitive)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override final def getFileFormatShortName: String = "ORC"
}

case class GpuOrcPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    pushedFilters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics : Map[String, GpuMetric],
    @transient params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.orcDebugDumpPrefix
  private val debugDumpAlways = rapidsConf.orcDebugDumpAlways
  private val maxReadBatchSizeRows: Integer = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val targetBatchSizeBytes = rapidsConf.gpuTargetBatchSizeBytes
  private val maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes
  private val useChunkedReader = rapidsConf.chunkedReaderEnabled
  private val maxChunkedReaderMemoryUsageSizeBytes =
    if(rapidsConf.limitChunkedReaderMemoryUsage) {
      (rapidsConf.chunkedReaderMemoryUsageRatio * targetBatchSizeBytes).toLong
    } else {
      0L
    }
  private val filterHandler = GpuOrcFileFilterHandler(sqlConf, metrics, broadcastedConf,
    pushedFilters, rapidsConf.isOrcFloatTypesToStringEnable)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val startTime = System.nanoTime()
    val ctx = filterHandler.filterStripes(partFile, dataSchema, readDataSchema,
      partitionSchema)
    metrics.get(FILTER_TIME).foreach {
      _ += (System.nanoTime() - startTime)
    }
    if (ctx == null) {
      new EmptyPartitionReader[ColumnarBatch]
    } else {
      val conf = broadcastedConf.value.value
      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)
      val reader = new PartitionReaderWithBytesRead(new GpuOrcPartitionReader(conf, partFile, ctx,
        readDataSchema, debugDumpPrefix, debugDumpAlways,  maxReadBatchSizeRows,
        maxReadBatchSizeBytes, targetBatchSizeBytes,
        useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
        metrics, filterHandler.isCaseSensitive))
      ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema,
        maxGpuColumnSizeBytes)
    }
  }
}

/**
 * This class describes a stripe that will appear in the ORC output memory file.
 *
 * @param infoBuilder builder for output stripe info that has been populated with
 *                    all fields except those that can only be known when the file
 *                    is being written (e.g.: file offset, compressed footer length)
 * @param footer stripe footer
 * @param inputDataRanges input file ranges (based at file offset 0) of stripe data
 */
case class OrcOutputStripe(
    infoBuilder: OrcProto.StripeInformation.Builder,
    footer: OrcProto.StripeFooter,
    inputDataRanges: DiskRangeList)

/**
 * This class holds fields needed to read and iterate over the OrcFile
 *
 * @param filePath  ORC file path
 * @param conf      the Hadoop configuration
 * @param fileSchema the schema of the whole ORC file
 * @param updatedReadSchema read schema mapped to the file's field names
 * @param evolution  infer and track the evolution between the schema as stored in the file and
 *                   the schema that has been requested by the reader.
 * @param fileTail   the ORC FileTail
 * @param compressionSize  the ORC compression size
 * @param compressionKind  the ORC compression type
 * @param readerOpts  options for creating a RecordReader.
 * @param blockIterator an iterator over the ORC output stripes
 * @param requestedMapping the optional requested column ids
 */
case class OrcPartitionReaderContext(
    filePath: Path,
    conf: Configuration,
    fileSchema: TypeDescription,
    updatedReadSchema: TypeDescription,
    evolution: SchemaEvolution,
    fileTail: OrcProto.FileTail,
    compressionSize: Int,
    compressionKind: CompressionKind,
    readerOpts: Reader.Options,
    blockIterator: BufferedIterator[OrcOutputStripe],
    requestedMapping: Option[Array[Int]])

case class OrcBlockMetaForSplitCheck(
    filePath: Path,
    typeDescription: TypeDescription,
    compressionKind: CompressionKind,
    requestedMapping: Option[Array[Int]]) {
}

object OrcBlockMetaForSplitCheck {
  def apply(singleBlockMeta: OrcSingleStripeMeta): OrcBlockMetaForSplitCheck = {
    OrcBlockMetaForSplitCheck(
      singleBlockMeta.filePath,
      singleBlockMeta.schema.schema,
      singleBlockMeta.dataBlock.stripeMeta.ctx.compressionKind,
      singleBlockMeta.extraInfo.requestedMapping)
  }

  def apply(filePathStr: String, typeDescription: TypeDescription,
      compressionKind: CompressionKind,
      requestedMapping: Option[Array[Int]]): OrcBlockMetaForSplitCheck = {
    OrcBlockMetaForSplitCheck(new Path(new URI(filePathStr)), typeDescription,
      compressionKind, requestedMapping)
  }
}

/** Collections of some common functions for ORC */
trait OrcCommonFunctions extends OrcCodecWritingHelper { self: FilePartitionReaderBase =>
  /** Whether debug dumping is enabled and the path prefix where to dump */
  val debugDumpPrefix: Option[String]

  /** Whether to always debug dump or only on errors */
  val debugDumpAlways: Boolean

  val conf: Configuration

  // The Spark schema describing what will be read
  def readDataSchema: StructType

  /** Copy the stripe to the channel */
  protected def copyStripeData(
      dataReader: GpuOrcDataReader,
      out: HostMemoryOutputStream,
      inputDataRanges: DiskRangeList): Unit = {

    val start = System.nanoTime()
    dataReader.copyFileDataToHostStream(out, inputDataRanges)
    val end = System.nanoTime()
    metrics.get(READ_FS_TIME).foreach(_.add(end - start))
  }

  /** Get the ORC schema corresponding to the file being constructed for the GPU */
  protected def buildReaderSchema(ctx: OrcPartitionReaderContext): TypeDescription =
    buildReaderSchema(ctx.updatedReadSchema, ctx.requestedMapping)

  protected def buildReaderSchema(
      updatedSchema: TypeDescription,
      requestedMapping: Option[Array[Int]]): TypeDescription = {
    requestedMapping.map { colIds =>
      // filter top-level schema based on requested mapping
      val filedNames = updatedSchema.getFieldNames
      val fieldTypes = updatedSchema.getChildren
      val resultSchema = TypeDescription.createStruct()
      colIds.filter(_ >= 0).foreach { colIdx =>
        resultSchema.addField(filedNames.get(colIdx), fieldTypes.get(colIdx).clone())
      }
      resultSchema
    }.getOrElse(updatedSchema)
  }

  protected final def writeOrcFileHeader(outStream: HostMemoryOutputStream): Long = {
    val startOffset = outStream.getPos
    outStream.write(OrcTools.ORC_MAGIC)
    outStream.getPos - startOffset
  }

  /** write the ORC file footer and PostScript  */
  protected final def writeOrcFileTail(
      outStream: HostMemoryOutputStream,
      ctx: OrcPartitionReaderContext,
      footerStartOffset: Long,
      stripes: Seq[OrcOutputStripe]): Unit = {

    // 1) Build and write the file footer
    val allStripes = stripes.map(_.infoBuilder.build())
    val footer = OrcProto.Footer.newBuilder
      .addAllStripes(allStripes.asJava)
      .setHeaderLength(OrcTools.ORC_MAGIC.length)
      .setContentLength(footerStartOffset) // the content length is all before file footer
      .addAllTypes(org.apache.orc.OrcUtils.getOrcTypes(buildReaderSchema(ctx)))
      .setNumberOfRows(allStripes.map(_.getNumberOfRows).sum)
      .build()

    val posBeforeFooter = outStream.getPos
    withCodecOutputStream(ctx, outStream) { protoWriter =>
      protoWriter.writeAndFlush(footer)
    }

    // 2) Write the PostScript (uncompressed)
    val footerLen = outStream.getPos - posBeforeFooter
    val postscript = OrcProto.PostScript.newBuilder(ctx.fileTail.getPostscript)
      .setFooterLength(footerLen)
      .setMetadataLength(0)
      .build()
    postscript.writeTo(outStream)
    val postScriptLength = outStream.getPos - posBeforeFooter - footerLen
    if (postScriptLength > 255) {
      throw new IllegalStateException(s"PostScript length is too large at $postScriptLength")
    }

    // 3) Write length of the PostScript
    outStream.write(postScriptLength.toInt)
  }

  protected final def calculateFileTailSize(
      ctx: OrcPartitionReaderContext,
      footerStartOffset: Long,
      stripes: Seq[OrcOutputStripe]): Long = {
    withResource(new NullHostMemoryOutputStream) { nullStream =>
      writeOrcFileTail(nullStream, ctx, footerStartOffset, stripes)
      nullStream.getPos
    }
  }

  /**
   * Extracts all fields(columns) of DECIMAL128, including child columns of nested types,
   * and returns the names of all fields.
   * The names of nested children are prefixed with their parents' information, which is the
   * acceptable format of cuDF reader options.
   */
  private def filterDecimal128Fields(readColumns: Array[String],
      readSchema: StructType): Array[String] = {
    val buffer = mutable.ArrayBuffer.empty[String]

    def findImpl(prefix: String, fieldName: String, fieldType: DataType): Unit = fieldType match {
      case dt: DecimalType if DecimalType.isByteArrayDecimalType(dt) =>
        buffer.append(prefix + fieldName)
      case dt: StructType =>
        dt.fields.foreach(f => findImpl(prefix + fieldName + ".", f.name, f.dataType))
      case dt: ArrayType =>
        findImpl(prefix + fieldName + ".", "1", dt.elementType)
      case MapType(kt: DataType, vt: DataType, _) =>
        findImpl(prefix + fieldName + ".", "0", kt)
        findImpl(prefix + fieldName + ".", "1", vt)
      case _ =>
    }

    val rootFields = readColumns.toSet
    readSchema.fields.foreach {
      case f if rootFields.contains(f.name) => findImpl("", f.name, f.dataType)
      case _ =>
    }

    buffer.toArray
  }

  def getORCOptionsAndSchema(
      memFileSchema: TypeDescription,
      requestedMapping: Option[Array[Int]],
      readDataSchema: StructType): (ORCOptions, TypeDescription) = {
    val tableSchema = buildReaderSchema(memFileSchema, requestedMapping)
    val includedColumns = tableSchema.getFieldNames.asScala.toSeq
    val decimal128Fields = filterDecimal128Fields(includedColumns.toArray, readDataSchema)
    val parseOpts = ORCOptions.builder()
      .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
      .withNumPyTypes(false)
      .includeColumn(includedColumns: _*)
      .decimal128Column(decimal128Fields: _*)
      // Set reading timestamp ignoring the writer timezones,
      // read timestamp as it is without using cuDF rebasing timezone.
      // And then will use `GpuOrcTimezoneUtils.rebaseTimeZone` to rebase to get UTC timestamp.
      .ignoreTimezoneInStripeFooter()
      .build()
    (parseOpts, tableSchema)
  }

  protected final def isNeedToSplitDataBlock(
      curMeta: OrcBlockMetaForSplitCheck,
      nextMeta: OrcBlockMetaForSplitCheck): Boolean = {
    if (!nextMeta.typeDescription.equals(curMeta.typeDescription)) {
      logInfo(s"ORC schema for the next file ${nextMeta.filePath}" +
        s" schema ${nextMeta.typeDescription} doesn't match current ${curMeta.filePath}" +
        s" schema ${curMeta.typeDescription}, splitting it into another batch!")
      return true
    }

    if (nextMeta.compressionKind != curMeta.compressionKind) {
      logInfo(s"ORC File compression for the next file ${nextMeta.filePath}" +
        s" doesn't match current ${curMeta.filePath}, splitting it into another batch!")
      return true
    }

    val ret = (nextMeta.requestedMapping, curMeta.requestedMapping) match {
      case (None, None) => true
      case (Some(cols1), Some(cols2)) =>
        if (cols1.sameElements(cols2)) true else false
      case (_, _) => {
        false
      }
    }

    if (!ret) {
      logInfo(s"ORC requested column ids for the next file ${nextMeta.filePath}" +
        s" doesn't match current ${curMeta.filePath}, splitting it into another batch!")
      return true
    }

    false
  }

  protected implicit def toStripe(block: DataBlockBase): OrcStripeWithMeta =
    block.asInstanceOf[OrcDataStripe].stripeMeta

  protected implicit def toDataStripes(stripes: Seq[DataBlockBase]): Seq[OrcStripeWithMeta] =
    stripes.map(_.asInstanceOf[OrcDataStripe].stripeMeta)

  protected final def estimateOutputSizeFromBlocks(blocks: Seq[OrcStripeWithMeta]): Long = {
    // Start with header magic
    val headerLen = OrcTools.ORC_MAGIC.length
    val stripesLen = blocks.map { block =>
      // Account for the size of every stripe, and
      // the StripeInformation size in advance which should be calculated in Footer.
      block.stripeLength + OrcTools.sizeOfStripeInformation
    }.sum

    val footerLen = if (blocks.nonEmpty) {
      // Add the first orc file's footer length to cover ORC schema and other info.
      // Here uses the file footer size from the OrcPartitionReaderContext of the first
      // stripe as the worst-case.
      blocks.head.ctx.fileTail.getPostscript.getFooterLength
    } else {
      0L
    }

    val fileLen = headerLen + stripesLen + footerLen +
      256 + // Per ORC v1 spec, the size of Postscript must be less than 256 bytes.
      1 // And the single-byte postscript length at the end of the file.
    // Add in a bit of fudging in case the whole file is being consumed and
    // our codec version isn't as efficient as the original writer's codec.
    fileLen + OrcTools.INEFFICIENT_CODEC_BUF_SIZE
  }

}

/**
 * A base ORC partition reader which compose of some common methods
 */
trait OrcPartitionReaderBase extends OrcCommonFunctions with Logging
  with ScanWithMetrics { self: FilePartitionReaderBase =>

  def populateCurrentBlockChunk(
      blockIterator: BufferedIterator[OrcOutputStripe],
      maxReadBatchSizeRows: Int,
      maxReadBatchSizeBytes: Long): Seq[OrcOutputStripe] = {
    val currentChunk = new ArrayBuffer[OrcOutputStripe]

    var numRows: Long = 0
    var numBytes: Long = 0
    var numOrcBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIterator.hasNext) {
        val peekedStripe = blockIterator.head
        if (peekedStripe.infoBuilder.getNumberOfRows > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 ||
          numRows + peekedStripe.infoBuilder.getNumberOfRows <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedStripe.infoBuilder.getNumberOfRows)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIterator.next()
            numRows += currentChunk.last.infoBuilder.getNumberOfRows
            numOrcBytes += currentChunk.last.infoBuilder.getDataLength
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }

    readNextBatch()

    logDebug(s"Loaded $numRows rows from Orc. Orc bytes read: $numOrcBytes. " +
      s"Estimated GPU bytes: $numBytes")

    currentChunk.toSeq
  }

  /**
   * Read the stripes into HostMemoryBuffer.
   *
   * @param ctx     the context to provide some necessary information
   * @param stripes a sequence of Stripe to be read into HostMemeoryBuffer
   * @return HostMemeoryBuffer and its data size
   */
  protected def readPartFile(ctx: OrcPartitionReaderContext, stripes: Seq[OrcOutputStripe]):
      (SpillableHostBuffer, Long) = {
    NvtxRegistry.ORC_BUFFER_FILE_SPLIT {
      if (stripes.isEmpty) {
        return (null, 0L)
      }

      val hostBufferSize = estimateOutputSize(ctx, stripes)
      val hostBuffer = withRetryNoSplit[HostMemoryBuffer] {
        HostMemoryBuffer.allocate(hostBufferSize)
      }
      closeOnExcept(hostBuffer) { hmb =>
        withResource(new HostMemoryOutputStream(hmb)) { out =>
          writeOrcOutputFile(ctx, out, stripes)
          (SpillableHostBuffer(hmb, out.getPos, SpillPriorities.ACTIVE_BATCHING_PRIORITY),
            out.getPos)
        }
      }
    }
  }

  /**
   * Estimate how many bytes when writing the Stripes including HEADDER + STRIPES + FOOTER
   * @param ctx     the context to provide some necessary information
   * @param stripes a sequence of Stripe to be estimated
   * @return the estimated size
   */
  private def estimateOutputSize(ctx: OrcPartitionReaderContext, stripes: Seq[OrcOutputStripe]):
      Long = {
    // start with header magic
    var size: Long = OrcFile.MAGIC.length

    // account for the size of every stripe
    stripes.foreach { stripe =>
      size += stripe.infoBuilder.getIndexLength + stripe.infoBuilder.getDataLength
      // The true footer length is unknown since it may be compressed.
      // Use the uncompressed size as an upper bound.
      size += stripe.footer.getSerializedSize
    }

    // the original file's footer and postscript should be worst-case
    size += ctx.fileTail.getPostscript.getFooterLength
    size += ctx.fileTail.getPostscriptLength

    // and finally the single-byte postscript length at the end of the file
    size += 1

    // Add in a bit of fudging in case the whole file is being consumed and
    // our codec version isn't as efficient as the original writer's codec.
    size + OrcTools.INEFFICIENT_CODEC_BUF_SIZE
  }

  /**
   * Read the Stripes to the HostMemoryBuffer with a new full ORC-format including
   * HEADER + STRIPES + FOOTER
   *
   * @param ctx     the context to provide some necessary information
   * @param rawOut  the out stream for HostMemoryBuffer
   * @param stripes a sequence of Stripe to be read
   */
  private def writeOrcOutputFile(
      ctx: OrcPartitionReaderContext,
      rawOut: HostMemoryOutputStream,
      stripes: Seq[OrcOutputStripe]): Unit = {

    // write ORC header
    writeOrcFileHeader(rawOut)

    // write the stripes
    withCodecOutputStream(ctx, rawOut) { protoWriter =>
      withResource(OrcTools.buildDataReader(ctx, metrics)) { dataReader =>
        stripes.foreach { stripe =>
          stripe.infoBuilder.setOffset(rawOut.getPos)
          copyStripeData(dataReader, rawOut, stripe.inputDataRanges)
          val stripeFooterStartOffset = rawOut.getPos
          protoWriter.writeAndFlush(stripe.footer)
          stripe.infoBuilder.setFooterLength(rawOut.getPos - stripeFooterStartOffset)
        }
      }
    }
    // write the file tail (file footer + postscript)
    writeOrcFileTail(rawOut, ctx, rawOut.getPos, stripes)
  }
}

/**
 * A PartitionReader that reads an ORC file split on the GPU.
 *
 * Efficiently reading an ORC split on the GPU requires rebuilding the ORC file
 * in memory such that only relevant data is present in the memory file.
 * This avoids sending unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf Hadoop configuration
 * @param partFile file split to read
 * @param ctx     the context to provide some necessary information
 * @param readDataSchema Spark schema of what will be read from the file
 * @param debugDumpPrefix path prefix for dumping the memory file or null
 * @param debugDumpAlways whether to always debug dump or only on errors
 * @param maxReadBatchSizeRows maximum number of rows to read in a batch
 * @param maxReadBatchSizeBytes maximum number of bytes to read in a batch
 * @param targetBatchSizeBytes the target size of a batch
 * @param useChunkedReader whether to read Parquet by chunks or read all at once
 * @param maxChunkedReaderMemoryUsageSizeBytes soft limit on the number of bytes of internal memory
 *                                             usage that the reader will use
 * @param execMetrics metrics to update during read
 * @param isCaseSensitive whether the name check should be case sensitive or not
 */
class GpuOrcPartitionReader(
    override val conf: Configuration,
    partFile: PartitionedFile,
    ctx: OrcPartitionReaderContext,
    override val readDataSchema: StructType,
    override val debugDumpPrefix: Option[String],
    override val debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    useChunkedReader: Boolean,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    execMetrics : Map[String, GpuMetric],
    isCaseSensitive: Boolean) extends FilePartitionReaderBase(conf, execMetrics)
  with OrcPartitionReaderBase {

  override def next(): Boolean = {
    if (batchIter.hasNext) {
      return true
    }
    batchIter = EmptyGpuColumnarBatchIterator
    if (ctx.blockIterator.hasNext) {
      batchIter = readBatches()
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batchIter.hasNext
  }

  private def readBatches(): Iterator[ColumnarBatch] = {
    NvtxRegistry.ORC_READ_BATCHES {
      val currentStripes = populateCurrentBlockChunk(ctx.blockIterator, maxReadBatchSizeRows,
        maxReadBatchSizeBytes)
      if (ctx.updatedReadSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentStripes.map(_.infoBuilder.getNumberOfRows).sum.toInt
        if (numRows == 0) {
          EmptyGpuColumnarBatchIterator
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val nullColumns = readDataSchema.safeMap(f =>
            GpuColumnVector.fromNull(numRows, f.dataType).asInstanceOf[SparkVector])
          new SingleGpuColumnarBatchIterator(new ColumnarBatch(nullColumns.toArray, numRows))
        }
      } else {
        val colTypes = readDataSchema.fields.map(f => f.dataType)
        val iter = if(currentStripes.isEmpty) {
          CachedGpuBatchIterator(EmptyTableReader, colTypes)
        } else {
          val (dataBuffer, dataSize) = metrics(BUFFER_TIME).ns {
            readPartFile(ctx, currentStripes)
          }
          if (dataSize == 0) {
            dataBuffer.close()
            CachedGpuBatchIterator(EmptyTableReader, colTypes)
          } else {
            val (parseOpts, tableSchema) = getORCOptionsAndSchema(ctx.updatedReadSchema,
              ctx.requestedMapping, readDataSchema)
            RmmRapidsRetryIterator.withRetryNoSplit(dataBuffer) { _ =>
              // MakeOrcTableProducer will try to close the dataBuf
              val dataBuf = dataBuffer.getDataHostBuffer()
              // Duplicate request is ok and start to use the GPU just after the host
              // buffer is ready to not block CPU things.
              GpuSemaphore.acquireIfNecessary(TaskContext.get())
              val producer = MakeOrcTableProducer(useChunkedReader,
                maxChunkedReaderMemoryUsageSizeBytes, conf, targetBatchSizeBytes, parseOpts,
                dataBuf, 0, dataSize, metrics, isCaseSensitive, readDataSchema,
                tableSchema, Array(partFile), debugDumpPrefix, debugDumpAlways)
              CachedGpuBatchIterator(producer, colTypes)
            }
          }
        }
        iter.map { batch =>
          logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          batch
        }
      }
    } // end of withResource(new NvtxRange)
  } // end of readBatch

}

private object OrcTools {

  /** Build a GPU ORC data reader using OrcPartitionReaderContext */
  def buildDataReader(
      ctx: OrcPartitionReaderContext,
      metrics: Map[String, GpuMetric]): GpuOrcDataReader = {
    val fs = ctx.filePath.getFileSystem(ctx.conf)
    buildDataReader(ctx.compressionSize, ctx.compressionKind, ctx.fileSchema, ctx.readerOpts,
      ctx.filePath, fs, ctx.conf, metrics)
  }

  /** Build a GPU ORC data reader */
  def buildDataReader(
      compressionSize: Int,
      compressionKind: CompressionKind,
      fileSchema: TypeDescription,
      readerOpts: Reader.Options,
      filePath: Path,
      fs: FileSystem,
      conf: Configuration,
      metrics: Map[String, GpuMetric]): GpuOrcDataReader = {
    require(readerOpts.getDataReader == null, "unexpected data reader")
    val zeroCopy: Boolean = if (readerOpts.getUseZeroCopy != null) {
      readerOpts.getUseZeroCopy
    } else {
      OrcConf.USE_ZEROCOPY.getBoolean(conf)
    }
    val maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(conf)

    val typeCount = org.apache.orc.OrcUtils.getOrcTypes(fileSchema).size
    //noinspection ScalaDeprecation
    val reader = new GpuOrcDataReader(
      OrcShims.newDataReaderPropertiesBuilder(compressionSize, compressionKind, typeCount)
          .withFileSystem(fs)
          .withPath(filePath)
          .withZeroCopy(zeroCopy)
          .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
          .build(), conf, metrics)
    reader.open()
    reader
  }

  // 128k buffer in case of inefficient codec on GPU
  val INEFFICIENT_CODEC_BUF_SIZE: Int = 128 * 1024

  // Estimate the size of StripeInformation with the worst case.
  // The serialized size may be different because of the different values.
  // Here set most of values to "Long.MaxValue" to get the worst case.
  lazy val sizeOfStripeInformation: Int = {
    OrcProto.StripeInformation.newBuilder()
      .setOffset(Long.MaxValue)
      .setIndexLength(0) // Index stream is pruned
      .setDataLength(Long.MaxValue)
      .setFooterLength(Int.MaxValue) // StripeFooter size should be small
      .setNumberOfRows(Long.MaxValue)
      .build().getSerializedSize
  }

  val ORC_MAGIC: Array[Byte] = OrcFile.MAGIC.getBytes(StandardCharsets.US_ASCII)
}

/**
 * A tool to filter stripes
 *
 * @param sqlConf           SQLConf
 * @param broadcastedConf   the Hadoop configuration
 * @param pushedFilters     the PushDown filters
 */
private case class GpuOrcFileFilterHandler(
    @transient sqlConf: SQLConf,
    metrics: Map[String, GpuMetric],
    broadcastedConf: Broadcast[SerializableConfiguration],
    pushedFilters: Array[Filter],
    isOrcFloatTypesToStringEnable: Boolean) {

  private[rapids] val isCaseSensitive = sqlConf.caseSensitiveAnalysis

  def filterStripes(
      partFile: PartitionedFile,
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType): OrcPartitionReaderContext = {

    val conf = broadcastedConf.value.value
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)

    val filePath = new Path(new URI(partFile.filePath.toString()))
    val fs = filePath.getFileSystem(conf)
    val orcFileReaderOpts = OrcFile.readerOptions(conf)
        .filesystem(fs)
        .orcTail(GpuOrcFileFilterHandler.getOrcTail(filePath, fs, conf,  metrics))

    // After getting the necessary information from ORC reader, we must close the ORC reader
    OrcShims.withReader(OrcFile.createReader(filePath, orcFileReaderOpts)) { orcReader =>
    val resultedColPruneInfo = OrcReadingShims.requestedColumnIds(isCaseSensitive, dataSchema,
        readDataSchema, orcReader, conf)
      if (resultedColPruneInfo.isEmpty) {
        // Be careful when the OrcPartitionReaderContext is null, we should change
        // reader to EmptyPartitionReader for throwing exception
        null
      } else {
        val requestedColIds = resultedColPruneInfo.get._1
        // Normally without column names we cannot prune the file schema to the read schema,
        // but if no columns are requested from the file (e.g.: row count) then we can prune.
        val canPruneCols = resultedColPruneInfo.get._2 || requestedColIds.isEmpty
        OrcUtils.orcResultSchemaString(canPruneCols, dataSchema, readDataSchema,
          partitionSchema, conf)
        assert(requestedColIds.length == readDataSchema.length,
          "[BUG] requested column IDs do not match required schema")

        // Create a local copy of broadcastedConf before we set task-local configs.
        val taskConf = new Configuration(conf)
        // Following SPARK-35783, set requested columns as OrcConf. This setting may not make
        // any difference. Just in case it might be important for the ORC methods called by us,
        // either today or in the future.
        val includeColumns = requestedColIds.filter(_ != -1).sorted.mkString(",")
        taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, includeColumns)

        // Only need to filter ORC's schema evolution if it cannot prune directly
        val requestedMapping = if (canPruneCols) {
          None
        } else {
          Some(requestedColIds)
        }
        val fullSchema = StructType(dataSchema ++ partitionSchema)
        val readerOpts = buildOrcReaderOpts(taskConf, orcReader, partFile, fullSchema)

        withResource(OrcTools.buildDataReader(orcReader.getCompressionSize,
          orcReader.getCompressionKind, orcReader.getSchema, readerOpts, filePath, fs, taskConf,
          metrics)) {
          dataReader =>
            new GpuOrcPartitionReaderUtils(filePath, taskConf, partFile, orcFileReaderOpts,
              orcReader, readerOpts, dataReader, requestedMapping).getOrcPartitionReaderContext
        }
      }
    }
  }

  private def buildOrcReaderOpts(
      conf: Configuration,
      orcReader: Reader,
      partFile: PartitionedFile,
      fullSchema: StructType): Reader.Options = {
    val readerOpts = OrcInputFormat.buildOptions(
      conf, orcReader, partFile.start, partFile.length)
    // create the search argument if we have pushed filters
    OrcFiltersWrapper.createFilter(fullSchema, pushedFilters).foreach { f =>
      readerOpts.searchArgument(f, fullSchema.fieldNames)
    }
    readerOpts
  }

  /**
   * An utility to get OrcPartitionReaderContext which contains some necessary information
   */
  private class GpuOrcPartitionReaderUtils(
      filePath: Path,
      conf: Configuration,
      partFile: PartitionedFile,
      orcFileReaderOpts: OrcFile.ReaderOptions,
      orcReader: Reader,
      readerOpts: Reader.Options,
      dataReader: DataReader,
      requestedMapping: Option[Array[Int]]) {

    private val ORC_STREAM_KINDS_IGNORED = util.EnumSet.of(
      OrcProto.Stream.Kind.BLOOM_FILTER,
      OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
      OrcProto.Stream.Kind.ROW_INDEX)

    def getOrcPartitionReaderContext: OrcPartitionReaderContext = {
      val isCaseSensitive = readerOpts.getIsSchemaEvolutionCaseAware

      val (updatedReadSchema, fileIncluded) = checkSchemaCompatibility(orcReader.getSchema,
        readerOpts.getSchema, isCaseSensitive, isOrcFloatTypesToStringEnable)
      // GPU has its own read schema, so unset the reader include to read all the columns
      // specified by its read schema.
      readerOpts.include(null)
      val evolution = new SchemaEvolution(orcReader.getSchema, updatedReadSchema, readerOpts)
      val (sargApp, sargColumns) = getSearchApplier(evolution,
        orcFileReaderOpts.getUseUTCTimestamp,
        orcReader.writerUsedProlepticGregorian(), orcFileReaderOpts.getConvertToProlepticGregorian)

      val splitStripes = orcReader.getStripes.asScala.filter( s =>
        s.getOffset >= partFile.start && s.getOffset < partFile.start + partFile.length)
      val stripes = buildOutputStripes(splitStripes.toSeq, evolution,
        sargApp, sargColumns, OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(conf),
        orcReader.getWriterVersion, updatedReadSchema,
        resolveMemFileIncluded(fileIncluded, requestedMapping))
      OrcPartitionReaderContext(filePath, conf, orcReader.getSchema, updatedReadSchema, evolution,
        orcReader.getFileTail, orcReader.getCompressionSize, orcReader.getCompressionKind,
        readerOpts, stripes.iterator.buffered, requestedMapping)
    }

    /**
     * Compute an array of booleans, one for each column in the ORC file, indicating whether the
     * corresponding ORC column ID should be included in the file to be loaded by the GPU.
     *
     * @return per-column inclusion flags
     */
    protected def resolveMemFileIncluded(
        fileIncluded: Array[Boolean],
        requestedMapping: Option[Array[Int]]): Array[Boolean] = {
      requestedMapping.map { mappings =>
        // filter top-level schema based on requested mapping
        val orcFileSchema = orcReader.getSchema
        val orcSchemaChildren = orcFileSchema.getChildren
        val resultIncluded = new Array[Boolean](orcFileSchema.getMaximumId + 1)
        // first column is the top-level schema struct, always add it
        resultIncluded(0) = true
        mappings.foreach { orcColIdx =>
          if (orcColIdx >= 0) {
            // find each top-level column requested by top-level index and add it and
            // all child columns
            val fieldType = orcSchemaChildren.get(orcColIdx)
            (fieldType.getId to fieldType.getMaximumId).foreach { i =>
              resultIncluded(i) = true
            }
          }
        }
        resultIncluded
      }.getOrElse(fileIncluded)
    }

    /**
     * Build an integer array that maps the original ORC file's column IDs
     * to column IDs in the memory file. Columns that are not present in
     * the memory file will have a mapping of -1.
     *
     * @param fileIncluded indicator per column in the ORC file whether it should be included
     * @return column mapping array
     */
    private def columnRemap(fileIncluded: Array[Boolean]): Array[Int] = {
      var nextOutputColumnId = 0
      val result = new Array[Int](fileIncluded.length)
      fileIncluded.indices.foreach { i =>
        if (fileIncluded(i)) {
          result(i) = nextOutputColumnId
          nextOutputColumnId += 1
        } else {
          result(i) = -1
        }
      }
      result
    }

    /**
     * Build the output stripe descriptors for what will appear in the ORC memory file.
     *
     * @param stripes descriptors for the ORC input stripes, filtered to what is in the split
     * @param evolution ORC SchemaEvolution
     * @param sargApp ORC search argument applier
     * @param sargColumns mapping of ORC search argument columns
     * @param ignoreNonUtf8BloomFilter true if bloom filters other than UTF8 should be ignored
     * @param writerVersion writer version from the original ORC input file
     * @param updatedReadSchema the read schema
     * @return output stripes descriptors
     */
    private def buildOutputStripes(
        stripes: Seq[StripeInformation],
        evolution: SchemaEvolution,
        sargApp: SargApplier,
        sargColumns: Array[Boolean],
        ignoreNonUtf8BloomFilter: Boolean,
        writerVersion: OrcFile.WriterVersion,
        updatedReadSchema: TypeDescription,
        fileIncluded: Array[Boolean]): Seq[OrcOutputStripe] = {
      val columnMapping = columnRemap(fileIncluded)
      OrcShims.filterStripes(stripes, conf, orcReader, dataReader,
        buildOutputStripe, evolution,
        sargApp, sargColumns, ignoreNonUtf8BloomFilter,
        writerVersion, fileIncluded, columnMapping).toSeq
    }

    /**
     * Build the output stripe descriptor for a corresponding input stripe
     * that should be copied to the ORC memory file.
     *
     * @param inputStripe input stripe descriptor
     * @param inputFooter input stripe footer
     * @param columnMapping mapping of input column IDs to output column IDs
     * @return output stripe descriptor
     */
    private def buildOutputStripe(
        inputStripe: StripeInformation,
        inputFooter: OrcProto.StripeFooter,
        columnMapping: Array[Int]): OrcOutputStripe = {
      val rangeCreator = new DiskRangeList.CreateHelper
      val footerBuilder = OrcProto.StripeFooter.newBuilder()
      var inputFileOffset = inputStripe.getOffset
      var outputStripeDataLength = 0L

      // copy stream descriptors for columns that are requested
      inputFooter.getStreamsList.asScala.foreach { stream =>
        val streamEndOffset = inputFileOffset + stream.getLength

        if (stream.hasKind && stream.hasColumn) {
          val outputColumn = columnMapping(stream.getColumn)
          val wantKind = !ORC_STREAM_KINDS_IGNORED.contains(stream.getKind)
          if (outputColumn >= 0 && wantKind) {
            // remap the column ID when copying the stream descriptor
            footerBuilder.addStreams(
              OrcProto.Stream.newBuilder(stream).setColumn(outputColumn).build)
            outputStripeDataLength += stream.getLength
            rangeCreator.addOrMerge(inputFileOffset, streamEndOffset,
              GpuOrcDataReader.shouldMergeDiskRanges, true)
          }
        }

        inputFileOffset = streamEndOffset
      }

      // add the column encodings that are relevant
      for (i <- 0 until inputFooter.getColumnsCount) {
        if (columnMapping(i) >= 0) {
          footerBuilder.addColumns(inputFooter.getColumns(i))
        }
      }

      // copy over the timezone
      if (inputFooter.hasWriterTimezone) {
        footerBuilder.setWriterTimezoneBytes(inputFooter.getWriterTimezoneBytes)
      }

      val outputStripeFooter = footerBuilder.build()

      // Fill out everything for StripeInformation except the file offset and footer length
      // which will be calculated when the stripe data is finally written.
      val infoBuilder = OrcProto.StripeInformation.newBuilder()
        .setIndexLength(0)
        .setDataLength(outputStripeDataLength)
        .setNumberOfRows(inputStripe.getNumberOfRows)

      OrcOutputStripe(infoBuilder, outputStripeFooter, rangeCreator.get)
    }

    /**
     * Check if the read schema is compatible with the file schema. Meanwhile, recursively
     * prune all incompatible required fields in terms of both ORC schema and file include
     * status.
     *
     * Only do the check for columns that can be found in the file schema, either by index
     * or column name. And the missing ones are ignored, since a null column will be added
     * in the final output for each of them.
     *
     * It takes care of both the top and nested columns.
     *
     * @param fileSchema  input file's ORC schema
     * @param readSchema  ORC schema for what will be read
     * @param isCaseAware true if field names are case-sensitive
     * @return A tuple contains the pruned read schema and the updated file include status.
     */
    private def checkSchemaCompatibility(
        fileSchema: TypeDescription,
        readSchema: TypeDescription,
        isCaseAware: Boolean,
        isOrcFloatTypesToStringEnable: Boolean): (TypeDescription, Array[Boolean]) = {
      // all default to false
      val fileIncluded = new Array[Boolean](fileSchema.getMaximumId + 1)
      val isForcePos = if (OrcShims.forcePositionalEvolution(conf)) {
        true
      } else if (GpuOrcPartitionReaderUtils.isMissingColumnNames(fileSchema)) {
        if (OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf)) {
          true
        } else {
          throw new RuntimeException("Found that schema metadata is missing"
              + " from file. This is likely caused by"
              + " a writer earlier than HIVE-4243. Will"
              + " not try to reconcile schemas")
        }
      } else {
        false
      }

      (checkTypeCompatibility(fileSchema, readSchema, isCaseAware, fileIncluded, isForcePos,
        isOrcFloatTypesToStringEnable),
        fileIncluded)
    }

    /**
     * Check if the file type is compatible with the read type.
     * Return the file type (Will be pruned for struct type) if the check result is positive,
     * otherwise blows up.
     */
    private def checkTypeCompatibility(
        fileType: TypeDescription,
        readType: TypeDescription,
        isCaseAware: Boolean,
        fileIncluded: Array[Boolean],
        isForcePos: Boolean,
        isOrcFloatTypesToStringEnable: Boolean): TypeDescription = {
      (fileType.getCategory, readType.getCategory) match {
        case (TypeDescription.Category.STRUCT, TypeDescription.Category.STRUCT) =>
          // Check for the top or nested struct types.
          val readFieldNames = readType.getFieldNames.asScala
          val readField2Type = readFieldNames.zip(readType.getChildren.asScala)

          val getReadFieldType: (String, Int) => Option[(String, TypeDescription)] =
            if (isForcePos) {
              // Match the top level columns using position rather than column names.
              (_, fileFieldIdx) => readField2Type.lift(fileFieldIdx)
            } else {
              // match by column names
              val caseSensitiveReadTypes = readFieldNames.zip(readField2Type).toMap
              val readTypesMap = if (isCaseAware) {
                caseSensitiveReadTypes
              } else {
                CaseInsensitiveMap[(String, TypeDescription)](caseSensitiveReadTypes)
              }
              (fileFieldName, _) => readTypesMap.get(fileFieldName)
            }
          // buffer to cache the result schema
          val prunedReadSchema = TypeDescription.createStruct()

          fileType.getFieldNames.asScala
            .zip(fileType.getChildren.asScala)
            .zipWithIndex.foreach { case ((fileFieldName, fType), idx) =>
            getReadFieldType(fileFieldName, idx).foreach { case (rField, rType) =>
              val newChild = checkTypeCompatibility(fType, rType,
                isCaseAware, fileIncluded, isForcePos, isOrcFloatTypesToStringEnable)
              prunedReadSchema.addField(rField, newChild)
            }
          }
          fileIncluded(fileType.getId) = true
          prunedReadSchema
        // Go into children for LIST, MAP to filter out the missing names
        // for struct children.
        case (TypeDescription.Category.LIST, TypeDescription.Category.LIST) =>
          val newChild = checkTypeCompatibility(fileType.getChildren.get(0),
            readType.getChildren.get(0), isCaseAware, fileIncluded, isForcePos,
            isOrcFloatTypesToStringEnable)
          fileIncluded(fileType.getId) = true
          TypeDescription.createList(newChild)
        case (TypeDescription.Category.MAP, TypeDescription.Category.MAP) =>
          val newKey = checkTypeCompatibility(fileType.getChildren.get(0),
            readType.getChildren.get(0), isCaseAware, fileIncluded, isForcePos,
            isOrcFloatTypesToStringEnable)
          val newValue = checkTypeCompatibility(fileType.getChildren.get(1),
            readType.getChildren.get(1), isCaseAware, fileIncluded, isForcePos,
            isOrcFloatTypesToStringEnable)
          fileIncluded(fileType.getId) = true
          TypeDescription.createMap(newKey, newValue)
        case (ft, rt) if ft.isPrimitive && rt.isPrimitive =>
          if (OrcShims.typeDescriptionEqual(fileType, readType) ||
            GpuOrcScan.canCast(fileType, readType, isOrcFloatTypesToStringEnable)) {
            // Since type casting is supported, here should return the file type.
            fileIncluded(fileType.getId) = true
            fileType.clone()
          } else {
            throw new QueryExecutionException("GPU ORC does not support type conversion" +
              s" from file type $fileType (${fileType.getId}) to" +
              s" reader type $readType (${readType.getId})")
          }
        case (f, r) =>
          // e.g. Union type is not supported yet
          throw new QueryExecutionException("Unsupported type pair of " +
            s"(file type, read type)=($f, $r)")
      }
    }

    /**
     * Build an ORC search argument applier that can filter input file splits
     * when predicate push-down filters have been specified.
     *
     * @param evolution ORC SchemaEvolution
     * @param useUTCTimestamp true if timestamps are UTC
     * @return the search argument applier and search argument column mapping
     */
    private def getSearchApplier(
        evolution: SchemaEvolution,
        useUTCTimestamp: Boolean,
        writerUsedProlepticGregorian: Boolean,
        convertToProlepticGregorian: Boolean): (SargApplier, Array[Boolean]) = {
      val searchArg = readerOpts.getSearchArgument
      if (searchArg != null && orcReader.getRowIndexStride != 0) {
        val sa = new SargApplier(searchArg, orcReader.getRowIndexStride, evolution,
          orcReader.getWriterVersion, useUTCTimestamp,
          writerUsedProlepticGregorian, convertToProlepticGregorian)
        // SargApplier.sargColumns is unfortunately not visible so we redundantly compute it here.
        val filterCols = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(searchArg.getLeaves,
          evolution)
        val saCols = new Array[Boolean](evolution.getFileIncluded.length)
        filterCols.foreach { i =>
          if (i > 0) {
            saCols(i) = true
          }
        }
        (sa, saCols)
      } else {
        (null, null)
      }
    }
  }

  private object GpuOrcPartitionReaderUtils {
    private val missingColumnNamePattern = Pattern.compile("_col\\d+")

    private def isMissingColumnNames(t: TypeDescription): Boolean = {
      t.getFieldNames.asScala.forall(f => missingColumnNamePattern.matcher(f).matches())
    }
  }
}

private object GpuOrcFileFilterHandler {
  // footer buffer is prefixed by a file size and a file modification timestamp
  private val TAIL_PREFIX_SIZE = 2 * java.lang.Long.BYTES

  private def getOrcTail(
      filePath: Path,
      fs: FileSystem,
      conf: Configuration,
      metrics: Map[String, GpuMetric]): OrcTail = {
    val fileIO = new HadoopFileIO(conf)
    val inputFile = fileIO.newInputFile(filePath)
    val cachedFooter = FileCache.get.getFooter(inputFile)
    val bb = cachedFooter.map { hmb =>
      // ORC can only deal with on-heap buffers
      val bb = withResource(hmb) { _ =>
        val bb = ByteBuffer.allocate(hmb.getLength.toInt)
        hmb.getBytes(bb.array(), 0, 0, hmb.getLength.toInt)
        bb
      }
      metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_HITS, NoopMetric) += 1
      metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_HITS_SIZE, NoopMetric) += hmb.getLength
      bb
    }.getOrElse {
      metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES, NoopMetric) += 1
      val bb = readOrcTailBuffer(filePath, fs)
      val bbSize = bb.remaining()
      metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES_SIZE, NoopMetric) += bbSize
      // footer was not cached, so try to cache it
      // If we get a filecache token then we can complete the caching by providing the data.
      // If we do not get a token then we should not cache this data.
      val cacheToken = FileCache.get.startFooterCache(inputFile)
      cacheToken.foreach { t =>
        val hmb = closeOnExcept(HostMemoryBuffer.allocate(bbSize, false)) { hmb =>
          hmb.setBytes(0, bb.array(), 0, bbSize)
          hmb
        }
        t.complete(hmb)
      }
      bb
    }
    loadOrcTailFromBuffer(bb)
  }

  private def loadOrcTailFromBuffer(bb: ByteBuffer): OrcTail = {
    if (bb.remaining == 0) {
      buildEmptyTail()
    } else {
      // Beginning of cached buffer is the file length and modification timestamp
      val fileSize = bb.getLong
      val modificationTime = bb.getLong
      val serializedTail = bb.slice()
      bb.position(0)
      // last byte is the size of the postscript section
      val psSize = bb.get(bb.limit() - 1) & 0xff
      val ps = loadPostScript(bb, psSize)
      val footer = OrcShims.parseFooterFromBuffer(bb, ps, psSize)
      val fileTail = OrcProto.FileTail.newBuilder()
          .setFileLength(fileSize)
          .setPostscriptLength(psSize)
          .setPostscript(ps)
          .setFooter(footer)
          .build()
      new OrcTail(fileTail, serializedTail, modificationTime)
    }
  }

  private def loadPostScript(bb: ByteBuffer, psSize: Int): OrcProto.PostScript = {
    val psOffset = bb.limit() - 1 - psSize
    val in = new ByteArrayInputStream(bb.array(), bb.arrayOffset() + psOffset, psSize)
    OrcProto.PostScript.parseFrom(in)
  }

  private def readOrcTailBuffer(filePath: Path, fs: FileSystem): ByteBuffer = {
    withResource(fs.open(filePath)) { in =>
      val fileStatus = fs.getFileStatus(filePath)
      val fileSize = fileStatus.getLen
      val modificationTime = fileStatus.getModificationTime
      if (fileSize == 0) {
        // file is empty
        ByteBuffer.allocate(0)
      } else {
        val footerSizeGuess = 16 * 1024
        val bb = ByteBuffer.allocate(footerSizeGuess)
        val readSize = fileSize.min(footerSizeGuess).toInt
        in.readFully(fileSize - readSize, bb.array(), bb.arrayOffset(), readSize)
        bb.position(0)
        bb.limit(readSize)
        val psLen = bb.get(readSize - 1) & 0xff
        ensureOrcFooter(in, filePath, psLen, bb)
        val psOffset = readSize - 1 - psLen
        val ps = extractPostScript(bb, filePath, psLen, psOffset)
        val tailSize = (1 + psLen + ps.getFooterLength + ps.getMetadataLength +
            OrcShims.getStripeStatisticsLength(ps)).toInt
        val tailBuffer = ByteBuffer.allocate(tailSize + TAIL_PREFIX_SIZE)
        // calculate the amount of tail data that was missed in the speculative initial read
        val unreadRemaining = Math.max(0, tailSize - readSize)
        // copy tail bytes from original buffer
        bb.position(Math.max(0, readSize - tailSize))
        tailBuffer.position(TAIL_PREFIX_SIZE + unreadRemaining)
        tailBuffer.put(bb)
        if (unreadRemaining > 0) {
          // first read did not grab the entire tail, need to read more
          tailBuffer.position(TAIL_PREFIX_SIZE)
          in.readFully(fileSize - readSize - unreadRemaining, tailBuffer.array(),
            tailBuffer.arrayOffset() + tailBuffer.position(), unreadRemaining)
        }
        tailBuffer.putLong(0, fileSize)
        tailBuffer.putLong(java.lang.Long.BYTES, modificationTime)
        tailBuffer.position(0)
        tailBuffer
      }
    }
  }

  private def extractPostScript(
      bb: ByteBuffer,
      filePath: Path,
      psLen: Int,
      psAbsOffset: Int): OrcProto.PostScript = {
    // TODO: when PB is upgraded to 2.6, newInstance(ByteBuffer) method should be used here.
    assert(bb.hasArray)
    val in = new ByteArrayInputStream(bb.array(), bb.arrayOffset() + psAbsOffset, psLen)
    val ps = OrcProto.PostScript.parseFrom(in)
    checkOrcVersion(filePath, ps)
    ps
  }

  /**
   * Ensure this is an ORC file to prevent users from trying to read text
   * files or RC files as ORC files.
   *
   * @param in     the file being read
   * @param path   the filename for error messages
   * @param psLen  the postscript length
   * @param buffer the tail of the file
   */
  private def ensureOrcFooter(
      in: FSDataInputStream,
      path: Path,
      psLen: Int,
      buffer: ByteBuffer): Unit = {
    val magicLength = OrcFile.MAGIC.length
    val fullLength = magicLength + 1
    if (psLen < fullLength || buffer.remaining < fullLength) {
      throw new FileFormatException("Malformed ORC file " + path +
          ". Invalid postscript length " + psLen)
    }
    val offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - fullLength
    val array = buffer.array
    // now look for the magic string at the end of the postscript.
    if (!Text.decode(array, offset, magicLength).equals(OrcFile.MAGIC)) {
      // If it isn't there, this may be the 0.11.0 version of ORC.
      // Read the first 3 bytes of the file to check for the header
      val header = new Array[Byte](magicLength)
      in.readFully(0, header, 0, magicLength)
      // if it isn't there, this isn't an ORC file
      if (!Text.decode(header, 0, magicLength).equals(OrcFile.MAGIC)) {
        throw new FileFormatException("Malformed ORC file " + path +
            ". Invalid postscript.")
      }
    }
  }

  private def checkOrcVersion(path: Path, postscript: OrcProto.PostScript): Unit = {
    val versionList = postscript.getVersionList
    if (ReaderImpl.getFileVersion(versionList) == OrcFile.Version.FUTURE) {
      throw new IOException(path + " was written by a future ORC version " +
          versionList.asScala.mkString(".") +
          ". This file is not readable by this version of ORC.\n")
    }
  }

  private def buildEmptyTail(): OrcTail = {
    val postscript = OrcProto.PostScript.newBuilder()
    val version = OrcFile.Version.CURRENT
    postscript.setMagic(OrcFile.MAGIC)
        .setCompression(OrcProto.CompressionKind.NONE)
        .setFooterLength(0)
        .addVersion(version.getMajor)
        .addVersion(version.getMinor)
        .setMetadataLength(0)
        .setWriterVersion(OrcFile.CURRENT_WRITER.getId)

    // Use a struct with no fields
    val struct = OrcProto.Type.newBuilder()
    struct.setKind(OrcProto.Type.Kind.STRUCT)

    val footer = OrcProto.Footer.newBuilder()
    footer.setHeaderLength(0)
        .setContentLength(0)
        .addTypes(struct)
        .setNumberOfRows(0)
        .setRowIndexStride(0)

    val result = OrcProto.FileTail.newBuilder()
    result.setFooter(footer)
    result.setPostscript(postscript)
    result.setFileLength(0)
    result.setPostscriptLength(0)
    new OrcTail(result.build(), null)
  }
}

/**
 * A PartitionReader that can read multiple ORC files in parallel. This is most efficient
 * running in a cloud environment where the I/O of reading is slow.
 *
 * Efficiently reading a ORC split on the GPU requires re-constructing the ORC file
 * in memory that contains just the Stripes that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param files the partitioned files to read
 * @param dataSchema schema of the data
 * @param readDataSchema the Spark schema describing what will be read
 * @param partitionSchema Schema of partitions.
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param targetBatchSizeBytes the target size of a batch
 * @param maxGpuColumnSizeBytes maximum number of bytes for a GPU column
 * @param useChunkedReader whether to read Parquet by chunks or read all at once
 * @param maxChunkedReaderMemoryUsageSizeBytes soft limit on the number of bytes of internal memory
 *                                             usage that the reader will use
 * @param poolConf thread pool configurations
 * @param maxNumFileProcessed threshold to control the maximum file number to be
 *                            submitted to threadpool
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated ORC data or null
 * @param debugDumpAlways whether to always debug dump or only on errors
 * @param filters filters passed into the filterHandler
 * @param filterHandler used to filter the ORC stripes
 * @param execMetrics the metrics
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
 */
class MultiFileCloudOrcPartitionReader(
    override val conf: Configuration,
    files: Array[PartitionedFile],
    dataSchema: StructType,
    override val readDataSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    useChunkedReader: Boolean,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    override val debugDumpPrefix: Option[String],
    override val debugDumpAlways: Boolean,
    filters: Array[Filter],
    filterHandler: GpuOrcFileFilterHandler,
    execMetrics: Map[String, GpuMetric],
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    queryUsesInputFile: Boolean,
    keepReadsInOrder: Boolean,
    combineConf: CombineConf)
  extends MultiFileCloudPartitionReaderBase(conf, files, poolConf, maxNumFileProcessed, filters,
    execMetrics, maxReadBatchSizeRows, maxReadBatchSizeBytes, ignoreCorruptFiles,
    keepReadsInOrder = keepReadsInOrder, combineConf = combineConf)
  with MultiFileReaderFunctions with OrcPartitionReaderBase {

  private case class HostMemoryEmptyMetaData(
      override val partitionedFile: PartitionedFile,
      numRows: Long,
      override val bytesRead: Long,
      readSchema: StructType,
      override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
    extends HostMemoryBuffersWithMetaDataBase {

    override def memBuffersAndSizes: Array[SingleHMBAndMeta] =
      Array(SingleHMBAndMeta.empty(numRows))
  }

  private case class HostMemoryBuffersWithMetaData(
      override val partitionedFile: PartitionedFile,
      override val memBuffersAndSizes: Array[SingleHMBAndMeta],
      override val bytesRead: Long,
      updatedReadSchema: TypeDescription,
      compressionKind: CompressionKind,
      requestedMapping: Option[Array[Int]],
      override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
    extends HostMemoryBuffersWithMetaDataBase

  private class ReadBatchRunner(
      taskContext: TaskContext,
      partFile: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]) extends UnboundedAsyncRunner[HostMemoryBuffersWithMetaDataBase]  {

    private var blockChunkIter: BufferedIterator[OrcOutputStripe] = null

    override def callImpl(): HostMemoryBuffersWithMetaDataBase = {
      TrampolineUtil.setTaskContext(taskContext)
      // Mark the async thread as a pool thread within the RetryFramework
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        doRead()
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${partFile.filePath}", e)
          HostMemoryEmptyMetaData(partFile, 0, 0, null)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${partFile.filePath}", e)
          HostMemoryEmptyMetaData(partFile, 0, 0, null)
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()

      val hostBuffers = new ArrayBuffer[SingleHMBAndMeta]
      val filterStartTime = System.nanoTime()
      val ctx = filterHandler.filterStripes(partFile, dataSchema, readDataSchema,
        partitionSchema)
      val filterTime = System.nanoTime() - filterStartTime
      val bufferTimeStart = System.nanoTime()
      val result = try {
        if (ctx == null || ctx.blockIterator.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          logDebug(s"Read no blocks from file: ${partFile.filePath.toString}")
          HostMemoryEmptyMetaData(partFile, 0, bytesRead, readDataSchema)
        } else {
          blockChunkIter = ctx.blockIterator
          if (isDone) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            // got close before finishing
            logDebug("Reader is closed, return empty buffer for the current read for " +
              s"file: ${partFile.filePath.toString}")
            HostMemoryEmptyMetaData(partFile, 0, bytesRead, readDataSchema)
          } else {
            if (ctx.updatedReadSchema.isEmpty) {
              val bytesRead = fileSystemBytesRead() - startingBytesRead
              val numRows = ctx.blockIterator.map(_.infoBuilder.getNumberOfRows).sum
              logDebug(s"Return empty buffer but with row number: $numRows for " +
                s"file: ${partFile.filePath.toString}")
              HostMemoryEmptyMetaData(partFile, numRows, bytesRead, readDataSchema)
            } else {
              while (blockChunkIter.hasNext) {
                val blocksToRead = populateCurrentBlockChunk(blockChunkIter, maxReadBatchSizeRows,
                  maxReadBatchSizeBytes)
                val (hostBuf, bufSize) = readPartFile(ctx, blocksToRead)
                val numRows = blocksToRead.map(_.infoBuilder.getNumberOfRows).sum
                val metas = blocksToRead.map(b => OrcDataStripe(OrcStripeWithMeta(b, ctx)))
                hostBuffers += SingleHMBAndMeta(Array(hostBuf), bufSize, numRows, metas)
              }
              val bytesRead = fileSystemBytesRead() - startingBytesRead
              if (isDone) {
                // got close before finishing
                hostBuffers.safeClose()
                logDebug("Reader is closed, return empty buffer for the current read for " +
                  s"file: ${partFile.filePath.toString}")
                HostMemoryEmptyMetaData(partFile, 0, bytesRead, readDataSchema)
              } else {
                HostMemoryBuffersWithMetaData(partFile, hostBuffers.toArray, bytesRead,
                  ctx.updatedReadSchema, ctx.compressionKind, ctx.requestedMapping)
              }
            }
          }
        }
      } catch {
        case e: Throwable =>
          hostBuffers.safeClose(e)
          throw e
      }
      val bufferTime = System.nanoTime() - bufferTimeStart
      result.setExecutionTime(filterTime, bufferTime)
      result
    }
  }

  private case class CombinedMeta(
    combinedEmptyMeta: Option[HostMemoryEmptyMetaData],
    allPartValues: Array[(Long, InternalRow)],
    toCombine: Array[HostMemoryBuffersWithMetaDataBase])

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param tc      task context to use
   * @param file    file to be read
   * @param conf    the Configuration parameters
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]): AsyncRunner[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(tc, file, conf, filters)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override def getFileFormatShortName: String = "ORC"

  override def canUseCombine: Boolean = {
    if (queryUsesInputFile) {
      logDebug("Can't use combine mode because query uses 'input_file_xxx' function(s)")
      false
    } else {
      val canUse = combineConf.combineThresholdSize > 0
      if (!canUse) {
        logDebug("Can not use combine mode because the threshold size <= 0")
      }
      canUse
    }
  }

  override def combineHMBs(
      buffers: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    if (buffers.length == 1) {
      logDebug("No need to combine because there is only one buffer.")
      buffers.head
    } else {
      assert(buffers.length > 1)
      logDebug(s"Got ${buffers.length} buffers, combine them")
      doCombineHmbs(buffers)
    }
  }

  /**
   * Decode HostMemoryBuffers in GPU
   *
   * @param fileBufsAndMeta the file HostMemoryBuffer read from a PartitionedFile
   * @return the decoded batches
   */
  override def readBatches(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase):
      Iterator[ColumnarBatch] = {
    fileBufsAndMeta match {
      case meta: HostMemoryEmptyMetaData =>
        // Not reading any data, but add in partition data if needed
        val rows = meta.numRows.toInt
        val batch = if (rows == 0) {
          new ColumnarBatch(Array.empty, 0)
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val nullColumns = meta.readSchema.fields.safeMap(f =>
            GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector])
          new ColumnarBatch(nullColumns, rows)
        }
        meta.allPartValues match {
          case Some(partRowsAndValues) =>
            val (rowsPerPart, partValues) = partRowsAndValues.unzip
            BatchWithPartitionDataUtils.addPartitionValuesToBatch(batch, rowsPerPart,
              partValues, partitionSchema, maxGpuColumnSizeBytes)
          case None =>
            BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(batch,
              meta.partitionedFile.partitionValues, partitionSchema, maxGpuColumnSizeBytes)
        }

      case buffer: HostMemoryBuffersWithMetaData =>
        val memBuffersAndSize = buffer.memBuffersAndSizes
        val hmbInfo = memBuffersAndSize.head
        require(hmbInfo.hmbs.length == 1)
        val batchIter = readBufferToBatches(hmbInfo.hmbs.head, hmbInfo.bytes,
          buffer.updatedReadSchema, buffer.requestedMapping, filterHandler.isCaseSensitive,
          buffer.partitionedFile, buffer.allPartValues)
        if (memBuffersAndSize.length > 1) {
          val updatedBuffers = memBuffersAndSize.drop(1)
          currentFileHostBuffers = Some(buffer.copy(memBuffersAndSizes = updatedBuffers))
        } else {
          currentFileHostBuffers = None
        }
        batchIter

      case _ => throw new RuntimeException("Wrong HostMemoryBuffersWithMetaData")
    }
  }

  private def readBufferToBatches(
      hostBuffer: SpillableHostBuffer,
      bufferSize: Long,
      memFileSchema: TypeDescription,
      requestedMapping: Option[Array[Int]],
      isCaseSensitive: Boolean,
      partedFile: PartitionedFile,
      allPartValues: Option[Array[(Long, InternalRow)]]) : Iterator[ColumnarBatch] = {
    val (parseOpts, tableSchema) = closeOnExcept(hostBuffer) { _ =>
      getORCOptionsAndSchema(memFileSchema, requestedMapping, readDataSchema)
    }
    val colTypes = readDataSchema.fields.map(f => f.dataType)

    RmmRapidsRetryIterator.withRetryNoSplit(hostBuffer) { _ =>
      // The MakeOrcTableProducer will close the input buffer
      val dataBuf = hostBuffer.getDataHostBuffer()
      // Duplicate request is ok, and start to use the GPU just after the host
      // buffer is ready to not block CPU things.
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      val producer = MakeOrcTableProducer(useChunkedReader,
        maxChunkedReaderMemoryUsageSizeBytes, conf, targetBatchSizeBytes, parseOpts,
        dataBuf, 0, bufferSize, metrics, isCaseSensitive, readDataSchema,
        tableSchema, files, debugDumpPrefix, debugDumpAlways)
      val batchIter = CachedGpuBatchIterator(producer, colTypes)

      if (allPartValues.isDefined) {
        val allPartInternalRows = allPartValues.get.map(_._2)
        val rowsPerPartition = allPartValues.get.map(_._1)
        new GpuColumnarBatchWithPartitionValuesIterator(batchIter, allPartInternalRows,
          rowsPerPartition, partitionSchema, maxGpuColumnSizeBytes)
      } else {
        // this is a bit weird, we don't have number of rows when allPartValues isn't
        // filled in so can't use GpuColumnarBatchWithPartitionValuesIterator
        batchIter.flatMap { batch =>
          // we have to add partition values here for this batch, we already verified that
          // its not different for all the blocks in this batch
          BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(batch,
            partedFile.partitionValues, partitionSchema, maxGpuColumnSizeBytes)
        }
      }
    }
  }

  private def doCombineHmbs(
      input: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    val combinedMeta = computeCombinedHmbMeta(input)
    if (combinedMeta.combinedEmptyMeta.isDefined) {
      val ret = combinedMeta.combinedEmptyMeta.get
      logDebug(s"Got an empty buffer after combination, number rows ${ret.numRows}")
      ret
    } else { // There is at least one nonempty buffer
      // ignore the empty buffers
      val toCombine = combinedMeta.toCombine.filterNot(_.isInstanceOf[HostMemoryEmptyMetaData])
      logDebug(s"Using Combine mode and actually combining, number files ${toCombine.length}" +
        s" , files: ${toCombine.map(_.partitionedFile.filePath).mkString(",")}")

      val startCombineTime = System.currentTimeMillis()
      val metaToUse = toCombine.head.asInstanceOf[HostMemoryBuffersWithMetaData]
      val blockMetas = toCombine.flatMap(_.memBuffersAndSizes.flatMap(_.blockMeta)).toSeq

      // 1) Estimate the host buffer size: header + stripes + tail (footer + postscript)
      val combinedBufSize = estimateOutputSizeFromBlocks(blockMetas)

      // 2) Allocate the buffer with the estimated size
      val combined = closeOnExcept(HostMemoryBuffer.allocate(combinedBufSize)) { combinedBuf =>
        // 3) Build the combined memory file:
        var offset = withResource(new HostMemoryOutputStream(combinedBuf)) { outStream =>
          // a: Write the ORC header
          writeOrcFileHeader(outStream)
        }

        // b: Copy the stripes from read buffers
        val allOutputStripes = new ArrayBuffer[OrcOutputStripe]()
        toCombine.foreach { hmbWithMeta =>
          hmbWithMeta.memBuffersAndSizes.foreach { buf =>
            val dataCopyAmount = buf.blockMeta.map(_.getBlockSize).sum
            if (dataCopyAmount > 0 && buf.hmbs.nonEmpty) {
              require(buf.hmbs.length == 1)
              val headBuf = withRetryNoSplit[HostMemoryBuffer] {
                buf.hmbs.head.getDataHostBuffer()
              }
              withResource(headBuf) { _ =>
                combinedBuf.copyFromHostBuffer(
                  offset, headBuf, OrcTools.ORC_MAGIC.length, dataCopyAmount)
              }
            }
            // update the offset for each stripe
            var stripeOffset = offset
            buf.blockMeta.foreach { block =>
              block.stripe.infoBuilder.setOffset(stripeOffset)
              stripeOffset += block.getBlockSize
            }
            offset += dataCopyAmount
            buf.close()
            allOutputStripes ++= buf.blockMeta.map(_.stripe)
          }
        }

        // c: check if there is enough buffer for file tail, and reallocate the buf if needed
        val actualTailSize = calculateFileTailSize(blockMetas.head.ctx, offset,
            allOutputStripes.toSeq)
        val maybeNewBuf = if ((combinedBufSize - offset) < actualTailSize) {
          val newBufferSize = offset + actualTailSize
          logWarning(s"The original estimated size $combinedBufSize is too small, " +
            s"reallocating and copying data to bigger buffer size: $newBufferSize")
          // Copy the old buffer to a new allocated bigger buffer and close the old buffer
          withResource(combinedBuf) { _ =>
            withResource(new HostMemoryInputStream(combinedBuf, offset)) { in =>
              // realloc memory and copy
              closeOnExcept(HostMemoryBuffer.allocate(newBufferSize)) { newhmb =>
                withResource(new HostMemoryOutputStream(newhmb)) { out =>
                  IOUtils.copy(in, out)
                }
                newhmb
              }
            }
          }
        } else {
          combinedBuf
        }

        withResource(new HostMemoryOutputStream(maybeNewBuf)) { outStream =>
          // d: Write the ORC footer
          // Use the context of the first meta for codec type and schema, it's OK
          // because we have checked the compatibility for them.
          outStream.seek(offset)
          writeOrcFileTail(outStream, blockMetas.head.ctx, offset, allOutputStripes.toSeq)

          // e: Create the new meta for the combined buffer
          val numRows = combinedMeta.allPartValues.map(_._1).sum
          val finalBuf = SpillableHostBuffer(maybeNewBuf, maybeNewBuf.getLength,
            SpillPriorities.ACTIVE_BATCHING_PRIORITY)
          val combinedRet = SingleHMBAndMeta(Array(finalBuf), outStream.getPos, numRows,
            blockMetas)
          val newHmbWithMeta = metaToUse.copy(
            memBuffersAndSizes = Array(combinedRet),
            allPartValues = Some(combinedMeta.allPartValues))
          val filterTime = combinedMeta.toCombine.map(_.getFilterTime).sum
          val bufferTime = combinedMeta.toCombine.map(_.getBufferTime).sum
          newHmbWithMeta.setExecutionTime(filterTime, bufferTime)
          newHmbWithMeta
        }
      }

      logDebug(s"Took ${(System.currentTimeMillis() - startCombineTime)} " +
        s"ms to do combine of ${toCombine.length} files, " +
        s"task id: ${TaskContext.get().taskAttemptId()}")
      combined
    }
  }

  private def checkIfNeedToSplitDataBlock(
      curMeta: HostMemoryBuffersWithMetaData,
      nextMeta: HostMemoryBuffersWithMetaData): Boolean = {
    isNeedToSplitDataBlock(
      OrcBlockMetaForSplitCheck(curMeta.partitionedFile.filePath.toString(),
        curMeta.updatedReadSchema, curMeta.compressionKind, curMeta.requestedMapping),
      OrcBlockMetaForSplitCheck(nextMeta.partitionedFile.filePath.toString(),
        nextMeta.updatedReadSchema, nextMeta.compressionKind, nextMeta.requestedMapping))
  }

  private def computeCombinedHmbMeta(
      input: Array[HostMemoryBuffersWithMetaDataBase]): CombinedMeta = {
    // common vars
    val allPartValues = new ArrayBuffer[(Long, InternalRow)]()
    val toCombine = ArrayBuffer[HostMemoryBuffersWithMetaDataBase]()
    var allEmpty = true
    var needsSplit = false
    var numCombined, iterLoc = 0
    // vars for non empty meta
    val leftOversWhenNotKeepReadsInOrder = ArrayBuffer[HostMemoryBuffersWithMetaDataBase]()
    var firstNonEmpty: HostMemoryBuffersWithMetaData = null
    // vars for empty meta
    var metaForEmpty: HostMemoryEmptyMetaData = null
    var emptyNumRows, emptyTotalBytesRead = 0L
    // iterate through this to handle the case of keeping the files in the same order as Spark
    while (!needsSplit && iterLoc < input.length) {
      val bufAndMeta = input(iterLoc)
      val partValues = bufAndMeta.partitionedFile.partitionValues
      bufAndMeta match {
        case emptyHmbData: HostMemoryEmptyMetaData =>
          if (metaForEmpty == null || emptyHmbData.numRows > 0) {
            // Empty metadata is due to either ignoring missing files or row counts,
            // and we want to make sure to take the metadata from the ones with row counts
            // because the ones from ignoring missing files has less information with it.
            metaForEmpty = emptyHmbData
          }
          allPartValues.append((emptyHmbData.numRows, partValues))
          emptyNumRows += emptyHmbData.numRows
          emptyTotalBytesRead += emptyHmbData.bytesRead
          numCombined += 1
          toCombine += emptyHmbData
        case hmbData: HostMemoryBuffersWithMetaData =>
          allEmpty = false
          if (firstNonEmpty != null && checkIfNeedToSplitDataBlock(firstNonEmpty, hmbData)) {
            // if we need to keep the same order as Spark we just stop here and put rest in
            // leftOverFiles, but if we don't then continue so we combine as much as possible
            if (keepReadsInOrder) {
              needsSplit = true
              combineLeftOverFiles = Some(input.drop(numCombined))
            } else {
              leftOversWhenNotKeepReadsInOrder += hmbData
            }
          } else {
            if (firstNonEmpty == null) {
              firstNonEmpty = hmbData
            }
            val totalNumRows = hmbData.memBuffersAndSizes.map(_.numRows).sum
            allPartValues.append((totalNumRows, partValues))
            numCombined += 1
            toCombine += hmbData
          }
        case _ => throw new RuntimeException("Unknown HostMemoryBuffersWithMetaDataBase")
      }
      iterLoc += 1
    }

    if (!keepReadsInOrder && leftOversWhenNotKeepReadsInOrder.nonEmpty) {
      combineLeftOverFiles = Some(leftOversWhenNotKeepReadsInOrder.toArray)
    }

    val combinedEmptyMeta = if (allEmpty) {
      // metaForEmpty should not be null here
      Some(HostMemoryEmptyMetaData(
        metaForEmpty.partitionedFile, // not used, so pick one
        emptyNumRows, emptyTotalBytesRead,
        metaForEmpty.readSchema,
        Some(allPartValues.toArray)))
    } else {
      None
    }
    CombinedMeta(combinedEmptyMeta, allPartValues.toArray, toCombine.toArray)
  }
}

trait OrcCodecWritingHelper {

  /** Executes the provided code block in the codec environment */
  def withCodecOutputStream[T](
      ctx: OrcPartitionReaderContext,
      out: OutputStream)
    (block: shims.OrcProtoWriterShim => T): T = {

    withResource(Channels.newChannel(out)) { outChannel =>
      val outReceiver = new PhysicalWriter.OutputReceiver {
        override def output(buffer: ByteBuffer): Unit = outChannel.write(buffer)
        override def suppress(): Unit = throw new UnsupportedOperationException(
          "suppress should not be called")
      }
      val codec = OrcCodecPool.getCodec(ctx.compressionKind)
      try {
        // buffer size must be greater than zero or writes hang (ORC-381)
        val orcBufferSize = if (ctx.compressionSize > 0) {
          ctx.compressionSize
        } else {
          // note that this buffer is just for writing meta-data
          OrcConf.BUFFER_SIZE.getDefaultValue.asInstanceOf[Int]
        }
        withResource(OrcShims.newOrcOutStream(
          getClass.getSimpleName, orcBufferSize, codec, outReceiver)) { codecStream =>
          val protoWriter = shims.OrcProtoWriterShim(codecStream)
          block(protoWriter)
        }
      } finally {
        OrcCodecPool.returnCodec(ctx.compressionKind, codec)
      }
    }
  }
}

// Orc schema wrapper
private case class OrcSchemaWrapper(schema: TypeDescription) extends SchemaBase {

  override def isEmpty: Boolean = schema.getFieldNames.isEmpty
}

case class OrcStripeWithMeta(stripe: OrcOutputStripe, ctx: OrcPartitionReaderContext)
    extends OrcCodecWritingHelper {

  lazy val stripeLength: Long = {
    // calculate the true stripe footer size
    val out = new CountingOutputStream(NullOutputStreamShim.INSTANCE)
    val footerLen = withCodecOutputStream(ctx, out) { protoWriter =>
      protoWriter.writeAndFlush(stripe.footer)
      out.getByteCount
    }
    // The stripe size in ORC should be equal to (INDEX + DATA + STRIPE_FOOTER)
    stripe.infoBuilder.getIndexLength + stripe.infoBuilder.getDataLength + footerLen
  }
}

// OrcOutputStripe wrapper
private[rapids] case class OrcDataStripe(stripeMeta: OrcStripeWithMeta) extends DataBlockBase {

  override def getRowCount: Long = stripeMeta.stripe.infoBuilder.getNumberOfRows

  override def getReadDataSize: Long =
    stripeMeta.stripe.infoBuilder.getIndexLength + stripeMeta.stripe.infoBuilder.getDataLength

  override def getBlockSize: Long = stripeMeta.stripeLength
}

/** Orc extra information containing the requested column ids for the current coalescing stripes */
case class OrcExtraInfo(requestedMapping: Option[Array[Int]]) extends ExtraInfo

// Contains meta about a single stripe of an ORC file
private case class OrcSingleStripeMeta(
  filePath: Path, // Orc file path
  dataBlock: OrcDataStripe, // Orc stripe information with the OrcPartitionReaderContext
  partitionValues: InternalRow, // partitioned values
  schema: OrcSchemaWrapper, // Orc schema
  readSchema: StructType, // Orc read schema
  extraInfo: OrcExtraInfo // Orc ExtraInfo containing the requested column ids
) extends SingleDataBlockInfo

/**
 *
 * @param conf                  Configuration
 * @param files                 files to be read
 * @param clippedStripes        the stripe metadata from the original Orc file that has been clipped
 *                              to only contain the column chunks to be read
 * @param readDataSchema        the Spark schema describing what will be read
 * @param debugDumpPrefix       a path prefix to use for dumping the fabricated Orc data or null
 * @param debugDumpAlways       whether to always debug dump or only on errors
 * @param maxReadBatchSizeRows  soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param targetBatchSizeBytes  the target size of a batch
 * @param maxGpuColumnSizeBytes the maximum size of a GPU column
 * @param useChunkedReader      whether to read Parquet by chunks or read all at once
 * @param maxChunkedReaderMemoryUsageSizeBytes soft limit on the number of bytes of internal memory
 *                                             usage that the reader will use
 * @param execMetrics           metrics
 * @param partitionSchema       schema of partitions
 * @param poolConf              the thread pool configuration
 * @param isCaseSensitive       whether the name check should be case sensitive or not
 */
class MultiFileOrcPartitionReader(
    override val conf: Configuration,
    files: Array[PartitionedFile],
    clippedStripes: Seq[OrcSingleStripeMeta],
    override val readDataSchema: StructType,
    override val debugDumpPrefix: Option[String],
    override val debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    useChunkedReader: Boolean,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    poolConf: ThreadPoolConf,
    isCaseSensitive: Boolean)
  extends MultiFileCoalescingPartitionReaderBase(conf, clippedStripes,
    partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, maxGpuColumnSizeBytes,
    poolConf, execMetrics)
    with OrcCommonFunctions {

  // implicit to convert SchemaBase to Orc TypeDescription
  implicit def toTypeDescription(schema: SchemaBase): TypeDescription =
    schema.asInstanceOf[OrcSchemaWrapper].schema

  implicit def toOrcExtraInfo(in: ExtraInfo): OrcExtraInfo =
    in.asInstanceOf[OrcExtraInfo]

  // The runner to copy stripes to the offset of HostMemoryBuffer and update
  // the StripeInformation to construct the file Footer
  class OrcCopyStripesRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      stripes: ArrayBuffer[DataBlockBase],
      offset: Long)
    extends UnboundedAsyncRunner[(Seq[DataBlockBase], Long)] {

    override def callImpl(): (Seq[DataBlockBase], Long) = {
      TrampolineUtil.setTaskContext(taskContext)
      // Mark the async thread as a pool thread within the RetryFramework
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        doRead()
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): (Seq[DataBlockBase], Long) = {
      val startBytesRead = fileSystemBytesRead()
      // copy stripes to the HostMemoryBuffer
      withResource(outhmb) { _ =>
        withResource(new HostMemoryOutputStream(outhmb)) { rawOut =>
          // All stripes are from the same file, so it's safe to use the first stripe's ctx
          val ctx = stripes(0).ctx
          withCodecOutputStream(ctx, rawOut) { protoWriter =>
            withResource(OrcTools.buildDataReader(ctx, metrics)) { dataReader =>
              // write the stripes including INDEX+DATA+STRIPE_FOOTER
              stripes.foreach { stripeWithMeta =>
                val stripe = stripeWithMeta.stripe
                stripe.infoBuilder.setOffset(offset + rawOut.getPos)
                copyStripeData(dataReader, rawOut, stripe.inputDataRanges)
                val stripeFooterStartOffset = rawOut.getPos
                protoWriter.writeAndFlush(stripe.footer)
                stripe.infoBuilder.setFooterLength(rawOut.getPos - stripeFooterStartOffset)
              }
            }
          }
        }
      }
      val bytesRead = fileSystemBytesRead() - startBytesRead
      // the stripes returned has been updated, eg, stripe offset, stripe footer length
      (stripes.toSeq, bytesRead)
    }
  }

  /**
   * To check if the next block will be split into another ColumnarBatch
   *
   * @param currentBlockInfo current SingleDataBlockInfo
   * @param nextBlockInfo    next SingleDataBlockInfo
   * @return true: split the next block into another ColumnarBatch and vice versa
   */
  override def checkIfNeedToSplitDataBlock(
      currentBlockInfo: SingleDataBlockInfo,
      nextBlockInfo: SingleDataBlockInfo): Boolean = {
    isNeedToSplitDataBlock(
      OrcBlockMetaForSplitCheck(currentBlockInfo.asInstanceOf[OrcSingleStripeMeta]),
      OrcBlockMetaForSplitCheck(nextBlockInfo.asInstanceOf[OrcSingleStripeMeta]))
  }

  /**
   * Calculate the output size according to the block chunks and the schema, and the
   * estimated output size will be used as the initialized size of allocating HostMemoryBuffer
   *
   * Please be note, the estimated size should be at least equal to size of HEAD + Blocks + FOOTER
   *
   * @param batchContext the batch building context
   * @return Long, the estimated output size
   */
  override def calculateEstimatedBlocksOutputSize(batchContext: BatchContext): Long = {
    val filesAndBlocks = batchContext.origChunkedBlocks
    estimateOutputSizeFromBlocks(filesAndBlocks.values.flatten.toSeq)
  }

  /**
   * Calculate the final block output size which will be used to decide
   * if re-allocate HostMemoryBuffer
   *
   * For now, we still don't know the ORC file footer size, so we can't get the final size.
   *
   * Since calculateEstimatedBlocksOutputSize has over-estimated the size, it's safe to
   * use it and it will not cause HostMemoryBuffer re-allocating.
   *
   * @param footerOffset  footer offset
   * @param stripes       stripes to be evaluated
   * @param batchContext  the batch building context
   * @return the output size
   */
  override def calculateFinalBlocksOutputSize(
      footerOffset: Long,
      stripes: collection.Seq[DataBlockBase],
      batchContext: BatchContext): Long = {

    // In calculateEstimatedBlocksOutputSize, we have got the true size for
    // HEADER + All STRIPES + the estimated the FileFooter size with the worst-case.
    // We return a size that is smaller than the initial size to avoid the re-allocate

    footerOffset
  }

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param tc     task context to use
   * @param file   file to be read
   * @param outhmb the sliced HostMemoryBuffer to hold the blocks, and the implementation
   *               is in charge of closing it in sub-class
   * @param blocks blocks meta info to specify which blocks to be read
   * @param offset used as the offset adjustment
   * @param batchContext the batch building context
   * @return Callable[(Seq[DataBlockBase], Long)], which will be submitted to a
   *         ThreadPoolExecutor, and the Callable will return a tuple result and
   *         result._1 is block meta info with the offset adjusted
   *         result._2 is the bytes read
   */
  override def getBatchRunner(
      tc: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long,
      batchContext: BatchContext): AsyncRunner[(Seq[DataBlockBase], Long)] = {
    new OrcCopyStripesRunner(tc, file, outhmb, blocks, offset)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override final def getFileFormatShortName: String = "ORC"

  /**
   * Sent host memory to GPU to decode
   *
   * @param dataBuffer  the data which can be decoded in GPU
   * @param dataSize    data size
   * @param clippedSchema the clipped schema
   * @return Table
   */
  override def readBufferToTablesAndClose(
      dataBuffer: HostMemoryBuffer,
      dataSize: Long,
      clippedSchema: SchemaBase,
      readSchema: StructType,
      extraInfo: ExtraInfo): GpuDataProducer[Table] = {
    val (parseOpts, tableSchema) = getORCOptionsAndSchema(clippedSchema,
      extraInfo.requestedMapping, readDataSchema)

    // About to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    MakeOrcTableProducer(useChunkedReader,
      maxChunkedReaderMemoryUsageSizeBytes, conf, targetBatchSizeBytes, parseOpts,
      dataBuffer, 0, dataSize, metrics, isCaseSensitive, readDataSchema,
      tableSchema, files, debugDumpPrefix, debugDumpAlways)
  }

  /**
   * Write a header for a specific file format. If there is no header for the file format,
   * just ignore it and return 0
   *
   * @param buffer where the header will be written
   * @param batchContext the batch building context
   * @return how many bytes written
   */
  override def writeFileHeader(buffer: HostMemoryBuffer, batchContext: BatchContext): Long = {
    withResource(new HostMemoryOutputStream(buffer)) { stream =>
      writeOrcFileHeader(stream)
    }
  }

  /**
   * Writer a footer for a specific file format. If there is no footer for the file format,
   * just return (hmb, offset)
   *
   * Please be note, some file formats may re-allocate the HostMemoryBuffer because of the
   * estimated initialized buffer size may be a little smaller than the actual size. So in
   * this case, the hmb should be closed in the implementation.
   *
   * @param buffer         The buffer holding (header + data blocks)
   * @param bufferSize     The total buffer size which equals to size of (header + blocks + footer)
   * @param footerOffset   Where begin to write the footer
   * @param stripes        The data block meta info
   * @param batchContext   The batch building context
   * @return the buffer and the buffer size
   */
  override def writeFileFooter(
      buffer: HostMemoryBuffer,
      bufferSize: Long,
      footerOffset: Long,
      stripes: Seq[DataBlockBase],
      batchContext: BatchContext): (HostMemoryBuffer, Long) = {
    closeOnExcept(buffer) { _ =>
      withResource(new HostMemoryOutputStream(buffer)) { rawOut =>
        rawOut.seek(footerOffset)
        writeOrcFileTail(rawOut, stripes.head.ctx, footerOffset, stripes.map(_.stripe))
        (buffer, rawOut.getPos)
      }
    }
  }
}

object MakeOrcTableProducer extends Logging {
  def apply(
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      conf: Configuration,
      chunkSizeByteLimit: Long,
      parseOpts: ORCOptions,
      buffer: HostMemoryBuffer,
      offset: Long,
      bufferSize: Long,
      metrics : Map[String, GpuMetric],
      isSchemaCaseSensitive: Boolean,
      readDataSchema: StructType,
      tableSchema: TypeDescription,
      splits: Array[PartitionedFile],
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean
  ): GpuDataProducer[Table] = {
    debugDumpPrefix.foreach { prefix =>
      if (debugDumpAlways) {
        val p = DumpUtils.dumpBuffer(conf, buffer, offset, bufferSize, prefix, ".orc")
        logWarning(s"Wrote data for ${splits.mkString(", ")} to $p")
      }
    }
    if (useChunkedReader) {
      OrcTableReader(conf, chunkSizeByteLimit, maxChunkedReaderMemoryUsageSizeBytes,
        parseOpts, buffer, offset, bufferSize, metrics,  isSchemaCaseSensitive, readDataSchema,
        tableSchema, splits, debugDumpPrefix, debugDumpAlways)
    } else {
      val table = withResource(buffer) { _ =>
        try {
          RmmRapidsRetryIterator.withRetryNoSplit[Table] {
            NvtxIdWithMetrics(NvtxRegistry.ORC_DECODE, metrics(GPU_DECODE_TIME)) {
              Table.readORC(parseOpts, buffer, offset, bufferSize)
            }
          }
        } catch {
          case e: Exception =>
            val dumpMsg = debugDumpPrefix.map { prefix =>
              if (!debugDumpAlways) {
                val p = DumpUtils.dumpBuffer(conf, buffer, offset, bufferSize, prefix, ".orc")
                s", data dumped to $p"
              } else {
                ""
              }
            }.getOrElse("")
            throw new IOException(s"Error when processing ${splits.mkString("; ")}$dumpMsg", e)
        }
      }
      closeOnExcept(table) { _ =>
        if (readDataSchema.length < table.getNumberOfColumns) {
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read ${table.getNumberOfColumns} from ${splits.mkString("; ")}")
        }
      }
      metrics(NUM_OUTPUT_BATCHES) += 1
      val evolvedSchemaTable = SchemaUtils.evolveSchemaIfNeededAndClose(table, tableSchema,
        readDataSchema, isSchemaCaseSensitive, Some(GpuOrcScan.castColumnTo))
      val rebasedTimeZone = GpuOrcTimezoneUtils.rebaseTimeZone(evolvedSchemaTable)
      // Rebase the timestamp columns (if it has) to JVM default timezone as Spark does.
      new SingleGpuDataProducer(rebasedTimeZone)
    }
  }
}

case class OrcTableReader(
    conf: Configuration,
    chunkSizeByteLimit: Long,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    parseOpts: ORCOptions,
    buffer: HostMemoryBuffer,
    offset: Long,
    bufferSize: Long,
    metrics : Map[String, GpuMetric],
    isSchemaCaseSensitive: Boolean,
    readDataSchema: StructType,
    tableSchema: TypeDescription,
    splits: Array[PartitionedFile],
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean) extends GpuDataProducer[Table] with Logging {

  private[this] val reader = new ORCChunkedReader(chunkSizeByteLimit,
    maxChunkedReaderMemoryUsageSizeBytes, parseOpts, buffer, offset, bufferSize)

  private[this] lazy val splitsString = splits.mkString("; ")

  override def hasNext: Boolean = reader.hasNext

  override def next: Table = {
    val table = NvtxIdWithMetrics(NvtxRegistry.ORC_DECODE, metrics(GPU_DECODE_TIME)) {
      try {
        reader.readChunk()
      } catch {
        case e: Exception =>
          val dumpMsg = debugDumpPrefix.map { prefix =>
            if (!debugDumpAlways) {
              val p = DumpUtils.dumpBuffer(conf, buffer, offset, bufferSize, prefix, ".orc")
              s", data dumped to $p"
            } else {
              ""
            }
          }.getOrElse("")
          throw new IOException(s"Error when processing $splitsString$dumpMsg", e)
      }
    }

    closeOnExcept(table) { _ =>
      if (readDataSchema.length < table.getNumberOfColumns) {
        throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
          s"but read ${table.getNumberOfColumns} from $splitsString")
      }
    }
    metrics(NUM_OUTPUT_BATCHES) += 1
    val evolvedSchemaTable = SchemaUtils.evolveSchemaIfNeededAndClose(table, tableSchema,
      readDataSchema, isSchemaCaseSensitive, Some(GpuOrcScan.castColumnTo))
    // Rebase the timestamp columns (if it has) to JVM default timezone as Spark does.
    GpuOrcTimezoneUtils.rebaseTimeZone(evolvedSchemaTable)
  }

  override def close(): Unit = {
    reader.close()
    buffer.close()
  }
}
