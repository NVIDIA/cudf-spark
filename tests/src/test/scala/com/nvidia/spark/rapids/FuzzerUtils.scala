/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.Row
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Utilities for creating random inputs for unit tests.
 */
object FuzzerUtils {

  /**
   * Default options when generating random data.
   */
  private val DEFAULT_OPTIONS = FuzzerOptions()

  /**
   * Create a schema with the specified data types.
   */
  def createSchema(dataTypes: DataType*): StructType = {
    new StructType(dataTypes.zipWithIndex
      .map(pair => StructField(s"c${pair._2}", pair._1, true)).toArray)
  }

  /**
   * Create a schema with the specified data types.
   */
  def createSchema(dataTypes: Seq[DataType], nullable: Boolean = true): StructType = {
    new StructType(dataTypes.zipWithIndex
      .map(pair => StructField(s"c${pair._2}", pair._1, nullable)).toArray)
  }

  /**
   * Creates a ColumnarBatch with random data based on the given schema.
   */
  def createColumnarBatch(
      schema: StructType,
      rowCount: Int,
      options: FuzzerOptions = DEFAULT_OPTIONS,
      seed: Long = 0): ColumnarBatch = {
    
    // Check if schema contains nested types
    val hasNestedTypes = schema.fields.exists { field =>
      field.dataType match {
        case _: ArrayType | _: MapType | _: StructType => true
        case _ => false
      }
    }
    
    if (hasNestedTypes) {
      // Use Row-based approach for nested types
      createColumnarBatchFromRows(schema, rowCount, options, seed)
    } else {
      // Use efficient builder approach for primitive types
      createColumnarBatchWithBuilder(schema, rowCount, options, seed)
    }
  }
  
  /**
   * Creates a ColumnarBatch using GpuColumnarBatchBuilder (for primitive types only)
   */
  private def createColumnarBatchWithBuilder(
      schema: StructType,
      rowCount: Int,
      options: FuzzerOptions,
      seed: Long): ColumnarBatch = {
    val rand = new Random(seed)
    val r = new EnhancedRandom(rand, options)
    val builders = new GpuColumnarBatchBuilder(schema, rowCount)
    schema.fields.zipWithIndex.foreach {
      case (field, i) =>
        val builder = builders.builder(i)
        val rows = 0 until rowCount
        field.dataType match {
          case DataTypes.ByteType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextByte()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.ShortType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextShort) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.IntegerType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextInt()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.LongType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextLong()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.FloatType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextFloat()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.DoubleType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextDouble()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.StringType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextString()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case dt: DecimalType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextLong()) match {
                case Some(value) =>
                  // bounding unscaledValue with precision
                  val invScale = (dt.precision to ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION)
                    .foldLeft(10L)((x, _) => x * 10)
                  builder.append(BigDecimal(value / invScale, dt.scale).bigDecimal)
                case None => builder.appendNull()
              }
            })
          case DataTypes.BinaryType =>
            rows.foreach(_ => {
              maybeNull(rand, field.nullable, r.nextBytes()) match {
                case Some(value) => builder.appendByteList(value, 0, value.length)
                case None => builder.appendNull()
              }
            })
          // For nested types (array, map, struct), we fall back to Row-based approach
          // since GpuColumnarBatchBuilder doesn't support these directly
          case _: ArrayType | _: MapType | _: StructType =>
            throw new IllegalStateException(
              s"GpuColumnarBatchBuilder does not support nested type ${field.dataType}")
        }
    }
    builders.build(rowCount)
  }
  
  /**
   * Creates a ColumnarBatch from Rows (supports nested types)
   */
  private def createColumnarBatchFromRows(
      schema: StructType,
      rowCount: Int,
      options: FuzzerOptions,
      seed: Long): ColumnarBatch = {
    val rand = new Random(seed)
    val rows = (0 until rowCount).map { _ =>
      generateRow(schema.fields, rand, options)
    }
    
    // Convert rows to columnar batch using cuDF column builders    
    val columns = schema.fields.safeMap { field =>
      val columnData = rows.map(row => {
        val idx = schema.fieldIndex(field.name)
        if (row.isNullAt(idx)) null else row.get(idx)
      })
      buildCudfColumn(field.dataType, columnData, field.nullable)
    }

    closeOnExcept(columns.toArray) { rawColumns =>
      closeOnExcept(rawColumns.zip(schema.fields).safeMap { case (cudfCol, field) =>
        GpuColumnVector.from(cudfCol, field.dataType).asInstanceOf[ColumnVector]
      }.toArray) { gpuCols =>
        new org.apache.spark.sql.vectorized.ColumnarBatch(gpuCols, rowCount)
      }
    }
  }
  
  /**
   * Builds a cuDF column from Scala data
   */
  private def buildCudfColumn(
      dataType: DataType,
      data: Seq[Any],
      nullable: Boolean): ai.rapids.cudf.ColumnVector = {
    import ai.rapids.cudf.{ColumnVector => CudfColumnVector}
    
    dataType match {
      case LongType =>
        val values = data.map(v => if (v == null) null else Long.box(v.asInstanceOf[Long]))
        CudfColumnVector.fromBoxedLongs(values: _*)
      case IntegerType =>
        val values = data.map(v => if (v == null) null else Int.box(v.asInstanceOf[Int]))
        CudfColumnVector.fromBoxedInts(values: _*)
      case DoubleType =>
        val values = data.map(v => if (v == null) null else Double.box(v.asInstanceOf[Double]))
        CudfColumnVector.fromBoxedDoubles(values: _*)
      case FloatType =>
        val values = data.map(v => if (v == null) null else Float.box(v.asInstanceOf[Float]))
        CudfColumnVector.fromBoxedFloats(values: _*)
      case BooleanType =>
        val values = data.map(v => if (v == null) null else Boolean.box(v.asInstanceOf[Boolean]))
        CudfColumnVector.fromBoxedBooleans(values: _*)
      case StringType =>
        val values = data.map(v => if (v == null) null else v.asInstanceOf[String])
        CudfColumnVector.fromStrings(values: _*)
      case ArrayType(elementType, containsNull) =>
        val listType = createHostListType(elementType, nullable, containsNull)
        val javaLists = data.map { v =>
          if (v == null) null
          else v.asInstanceOf[Seq[_]].map(boxValue).asJava
        }
        CudfColumnVector.fromLists(listType, javaLists: _*)
      case structType: StructType =>
        val childColumns = structType.fields.safeMap { field =>
          val childData = data.map { v =>
            if (v == null) null
            else {
              val row = v.asInstanceOf[org.apache.spark.sql.Row]
              val idx = structType.fieldIndex(field.name)
              if (row.isNullAt(idx)) null else row.get(idx)
            }
          }
          buildCudfColumn(field.dataType, childData, field.nullable)
        }
        withResource(childColumns) { cols =>
          CudfColumnVector.makeStruct(data.length, cols: _*)
        }
      case MapType(keyType, valueType, valueContainsNull) =>
        // Map is represented as list<struct<key, value>> in cuDF
        // Build it using fromLists by converting each map to a list of struct data
        import ai.rapids.cudf.HostColumnVector
        
        val structType = new HostColumnVector.StructType(true, Seq(
          createHostColumnType(keyType, false),  // Keys are not nullable
          createHostColumnType(valueType, valueContainsNull)
        ).asJava)
        
        val listType = new HostColumnVector.ListType(nullable, structType)
        
        val javaLists = data.map { v =>
          if (v == null) null
          else {
            val map = v.asInstanceOf[Map[Any, Any]]
            map.map { case (k, v) =>
              new HostColumnVector.StructData(
                boxValue(k).asInstanceOf[Object],
                boxValue(v).asInstanceOf[Object])
            }.toList.asJava
          }
        }
        CudfColumnVector.fromLists(listType, javaLists: _*)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: $dataType")
    }
  }
  
  /**
   * Creates a HostColumnVector type descriptor for lists
   */
  private def createHostListType(
      elementType: DataType,
      listNullable: Boolean,
      elementNullable: Boolean): ai.rapids.cudf.HostColumnVector.DataType = {
    import ai.rapids.cudf.HostColumnVector

    new HostColumnVector.ListType(
      listNullable,
      createHostColumnType(elementType, elementNullable))
  }
  
  /**
   * Creates a HostColumnVector type descriptor for any data type
   */
  private def createHostColumnType(
      dataType: DataType,
      nullable: Boolean): ai.rapids.cudf.HostColumnVector.DataType = {
    import ai.rapids.cudf.{DType, HostColumnVector}
    
    dataType match {
      case LongType => new HostColumnVector.BasicType(nullable, DType.INT64)
      case IntegerType => new HostColumnVector.BasicType(nullable, DType.INT32)
      case DoubleType => new HostColumnVector.BasicType(nullable, DType.FLOAT64)
      case FloatType => new HostColumnVector.BasicType(nullable, DType.FLOAT32)
      case BooleanType => new HostColumnVector.BasicType(nullable, DType.BOOL8)
      case StringType => new HostColumnVector.BasicType(nullable, DType.STRING)
      case _ => throw new IllegalArgumentException(s"Unsupported type: $dataType")
    }
  }
  
  /**
   * Boxes primitive values for Java interop
   */
  private def boxValue(v: Any): Any = v match {
    case l: Long => Long.box(l)
    case i: Int => Int.box(i)
    case d: Double => Double.box(d)
    case f: Float => Float.box(f)
    case b: Boolean => Boolean.box(b)
    case other => other
  }

  /**
   * Creates a ColumnarBatch with provided data.
   */
  def createColumnarBatch(values: Seq[Option[Any]], dataType: DataType): ColumnarBatch = {
    val schema = createSchema(Seq(dataType))
    val rowCount = values.length
    val builders = new GpuColumnarBatchBuilder(schema, rowCount)
    schema.fields.zipWithIndex.foreach {
      case (field, i) =>
        val builder = builders.builder(i)
        field.dataType match {
          case DataTypes.DoubleType =>
            values.foreach {
              case Some(value) => builder.append(value.asInstanceOf[Double])
              case None => builder.appendNull()
            }
        }
    }
    builders.build(rowCount)
  }


  private def maybeNull[T](r: Random, nullable: Boolean, nonNullValue: => T): Option[T] = {
    if (nullable && r.nextFloat() < 0.2) {
      None
    }  else {
      Some(nonNullValue)
    }
  }

  /**
   * Creates a DataFrame with random data based on the given schema.
   */
  def generateDataFrame(
        spark: SparkSession,
        schema: StructType,
        rowCount: Int = 1024,
        options: FuzzerOptions = DEFAULT_OPTIONS,
        seed: Long = 0): DataFrame = {
    val r = new Random(seed)
    val rows: Seq[Row] = (0 until rowCount).map(_ => generateRow(schema.fields, r, options))
    spark.createDataFrame(rows.asJava, schema)
  }

  /**
   * Creates a Row with random data based on the given field definitions.
   */
  def generateRow(fields: Array[StructField], rand: Random, options: FuzzerOptions): Row = {
    val r = new EnhancedRandom(rand, options)

    def handleDataType(dataType: DataType): Any = {
      dataType match {
        case DataTypes.BooleanType => r.nextBoolean()
        case DataTypes.ByteType => r.nextByte()
        case DataTypes.ShortType => r.nextShort()
        case DataTypes.IntegerType => r.nextInt()
        case DataTypes.LongType => r.nextLong()
        case DataTypes.FloatType => r.nextFloat()
        case DataTypes.DoubleType => r.nextDouble()
        case DataTypes.StringType => r.nextString()
        case DataTypes.TimestampType => r.nextTimestamp()
        case DataTypes.DateType => r.nextDate()
        case DataTypes.CalendarIntervalType => r.nextInterval()
        case DataTypes.NullType => null
        case ArrayType(elementType, _) =>
          val list = new ListBuffer[Any]
          //only generating 5 items in the array this can later be made configurable
          scala.Range(0, 5).foreach { _ =>
            list.append(handleDataType(elementType))
          }
          list.toList
        case MapType(keyType, valueType, _) =>
          val keyList = new ListBuffer[Any]
          //only generating 5 items in the array this can later be made configurable
          scala.Range(0, 5).foreach { _ =>
            keyList.append(handleDataType(keyType))
          }
          val valueList = new ListBuffer[Any]
          //only generating 5 items in the array this can later be made configurable
          scala.Range(0, 5).foreach { _ =>
            valueList.append(handleDataType(valueType))
          }
          val map = new mutable.HashMap[Any, Any]()
          keyList.zip(valueList).map { values =>
            map.put(values._1, values._2)
          }
          map.toMap
        case s: StructType =>
          generateRow(s.fields, rand, options)
        case _ => throw new IllegalStateException(
          s"fuzzer does not support data type $dataType")
      }
    }

    Row.fromSeq(fields.map { field =>
      if (field.nullable && r.nextFloat() < 0.2) {
        null
      } else {
        handleDataType(field.dataType)
      }
    })
  }

  def createDataFrame(dt: DataType)(spark: SparkSession, rowCount: Int = 100) = {
    generateDataFrame(spark, createSchema(Seq(dt)), rowCount)
  }
}

/**
 * Wrapper around Random that generates more useful data for unit testing.
 */
class EnhancedRandom(protected val r: Random, protected val options: FuzzerOptions) {

  def nextInterval(): CalendarInterval = {
    new CalendarInterval(nextInt(), nextInt(), nextLong())
  }

  def nextBoolean(): Boolean = r.nextBoolean()

  def nextByte(): Byte = {
    r.nextInt(5) match {
      case 0 => Byte.MinValue
      case 1 => Byte.MaxValue
      case 2 => (r.nextDouble() * Byte.MinValue).toByte
      case 3 => (r.nextDouble() * Byte.MaxValue).toByte
      case 4 => 0
    }
  }

  def nextShort(): Short = {
    r.nextInt(5) match {
      case 0 => Short.MinValue
      case 1 => Short.MaxValue
      case 2 => (r.nextDouble() * Short.MinValue).toShort
      case 3 => (r.nextDouble() * Short.MaxValue).toShort
      case 4 => 0
    }
  }

  def nextInt(): Int = {
    r.nextInt(5) match {
      case 0 => options.intMin
      case 1 => options.intMax
      case 2 => (r.nextDouble() * options.intMin).toInt
      case 3 => (r.nextDouble() * options.intMax).toInt
      case 4 => 0
    }
  }

  def nextLong(): Long = {
    r.nextInt(5) match {
      case 0 => options.longMin
      case 1 => options.longMax
      case 2 => (r.nextDouble() * options.longMin).toLong
      case 3 => (r.nextDouble() * options.longMax).toLong
      case 4 => 0
    }
  }

  def nextFloat(): Float = {
    r.nextInt(11) match {
      case 0 => Float.NaN
      case 1 => Float.PositiveInfinity
      case 2 => Float.NegativeInfinity
      case 3 => Float.MinValue
      case 4 => Float.MaxValue
      case 5 => 0 - r.nextFloat()
      case 6 => r.nextFloat()
      case 7 => 0f
      case 8 => -0f
      case 9 => r.nextFloat() * Float.MinValue
      case 10 => r.nextFloat() * Float.MaxValue
    }
  }

  def nextDouble(): Double = {
    r.nextInt(11) match {
      case 0 => Double.NaN
      case 1 => Double.PositiveInfinity
      case 2 => Double.NegativeInfinity
      case 3 => Double.MaxValue
      case 4 => Double.MinValue
      case 5 => 0 - r.nextDouble()
      case 6 => r.nextDouble()
      case 7 => 0d
      case 8 => -0d
      case 9 => r.nextDouble() * Double.MinValue
      case 10 => r.nextDouble() * Double.MaxValue
    }
  }

  def nextDate(): Date = {
    val futureDate = 6321706291000L // Upper limit Sunday, April 29, 2170 9:31:31 PM
    new Date((futureDate * r.nextDouble()).toLong);
  }

  def nextTimestamp(): Timestamp = {
    val futureDate = 6321706291000L // Upper limit Sunday, April 29, 2170 9:31:31 PM
    new Timestamp((futureDate * r.nextDouble()).toLong)
  }

  def nextString(): String = {
    val length = r.nextInt(options.maxStringLen)
    options.validStringChars match {
      case Some(ch) => nextString(ch, length)
      case _ =>
        // delegate to Scala's Random.nextString
        r.nextString(length)
    }
  }

  def nextString(validStringChars: String, maxStringLen: Int): String = {
    val b = new StringBuilder(maxStringLen)
    for (_ <- 0 until maxStringLen) {
      b.append(validStringChars.charAt(r.nextInt(validStringChars.length)))
    }
    b.toString
  }

  def nextBytes(): Array[Byte] = {
    val length = r.nextInt(options.maxBytesLen)
    val bytes = new Array[Byte](length)
    r.nextBytes(bytes)
    bytes
  }

}

case class FuzzerOptions(
    validStringChars: Option[String] = None,
    maxStringLen: Int = 64,
    maxBytesLen: Int = 64,
    intMin: Int = Int.MinValue,
    intMax: Int = Int.MaxValue,
    longMin: Long = Long.MinValue,
    longMax: Long = Long.MaxValue)
