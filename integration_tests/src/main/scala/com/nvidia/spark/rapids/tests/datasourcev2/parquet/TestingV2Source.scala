/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
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
package com.nvidia.spark.rapids.tests.datasourcev2.parquet

import java.util

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.arrow.vector.util.Text;

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

// Contains a bunch of classes for testing DataSourceV2 inputs
object TestingV2Source {
  var schema = new StructType(Array(StructField("col1", IntegerType)))
  var dataTypesToUse: Seq[DataType] = Seq(IntegerType)
}

trait TestingV2Source extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    var colNum = 0
    Option(options.get("arrowTypes")).foreach { typesStr =>
      val types = typesStr.split(",").map(_.trim()).filter(_.nonEmpty)
      val fields = types.map { t =>
        colNum += 1
        val colStr = s"col$colNum"
        t match {
          case "bool" =>
            StructField(colStr, BooleanType)
          case "byte" =>
            StructField(colStr, ByteType)
          case "short" =>
            StructField(colStr, ShortType)
          case "int" =>
            StructField(colStr, IntegerType)
          case "long" =>
            StructField(colStr, LongType)
          case "float" =>
            StructField(colStr, FloatType)
          case "double" =>
            StructField(colStr, DoubleType)
          case "string" =>
            StructField(colStr, StringType)
          case "timestamp" =>
            StructField(colStr, TimestampType)
          case "date" =>
            StructField(colStr, DateType)
        }
      }
      TestingV2Source.dataTypesToUse = fields.map(_.dataType).toSeq
      TestingV2Source.schema = new StructType(fields)
    }
    TestingV2Source.schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties))
  }

  def getTable(options: CaseInsensitiveStringMap): Table
}

abstract class SimpleScanBuilder extends ScanBuilder
  with Batch with Scan {

  override def build(): Scan = this

  override def toBatch: Batch = this

  override def readSchema(): StructType = TestingV2Source.schema

  override def createReaderFactory(): PartitionReaderFactory
}

abstract class SimpleBatchTable extends Table with SupportsRead  {

  override def schema(): StructType = TestingV2Source.schema

  override def name(): String = this.getClass.toString

  override def capabilities(): util.Set[TableCapability] = Set(BATCH_READ).asJava
}

case class ArrowInputPartition(dt: Seq[DataType], numRows: Int, startNum: Int)
  extends InputPartition

// DatasourceV2 that generates ArrowColumnVectors
// Default is to generate 2 partitions with 100 rows each.
// user can specify the datatypes with the .option() argument to the DataFrameReader
// via key "arrowTypes"
class ArrowColumnarDataSourceV2 extends TestingV2Source {

  class MyScanBuilder(options: CaseInsensitiveStringMap) extends SimpleScanBuilder {

    override def planInputPartitions(): Array[InputPartition] = {
      Array(ArrowInputPartition(TestingV2Source.dataTypesToUse, 100, 1),
        ArrowInputPartition(TestingV2Source.dataTypesToUse, 100, 101))
    }

    override def createReaderFactory(): PartitionReaderFactory = ColumnarReaderFactory
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder(options)
    }
  }
}

object ColumnarReaderFactory extends PartitionReaderFactory {
  private final val BATCH_SIZE = 20

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val ArrowInputPartition(dataTypes, numRows, startNum) = partition
    new PartitionReader[ColumnarBatch] {
      private var batch: ColumnarBatch = _

      private var current = 0

      override def next(): Boolean = {
        val batchSize = if (current < numRows) {
          if (current + BATCH_SIZE > numRows) {
            numRows - current
          } else {
            BATCH_SIZE
          }
        } else {
          0
        }

        if (batchSize == 0) {
          false
        } else {
          var dtypeNum = 0
          val vecs = dataTypes.map { dtype =>
            val vector = setupArrowVector(s"v$current$dtypeNum", dtype)
            val startVal = current + startNum * (dtypeNum + 2)
            fillArrowVec(dtype, vector, startVal, numRows)
            dtypeNum += 1
            new ArrowColumnVector(vector)
          }
          batch = new ColumnarBatch(vecs.toArray)
          batch.setNumRows(batchSize)
          current += batchSize
          true
        }
      }

      override def get(): ColumnarBatch = batch

      override def close(): Unit = batch.close()
    }
  }

  // this was copied from Spark ArrowUtils
  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  private def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType => ArrowType.Utf8.INSTANCE
    case BinaryType => ArrowType.Binary.INSTANCE
    // case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType =>
      if (timeZoneId == null) {
        throw new UnsupportedOperationException(
          s"${TimestampType.catalogString} must supply timeZoneId parameter")
      } else {
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
      }
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
  }

  // this was copied from Spark ArrowUtils
  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  private def toArrowField(
      name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(name, fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(name, fieldType,
          fields.map { field =>
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(name, mapType,
          Seq(toArrowField(MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            nullable = false,
            timeZoneId)).asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  private def fillArrowVec(dt: DataType, vec: ValueVector, start: Int,
      numRows: Int): Unit = dt match {
    case BooleanType =>
      val vector = vec.asInstanceOf[BitVector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, start + i & 1)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    case ByteType =>
      val vector = vec.asInstanceOf[TinyIntVector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, start + i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    case ShortType =>
      val vector = vec.asInstanceOf[SmallIntVector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, start + i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    case IntegerType =>
      val vector = vec.asInstanceOf[IntVector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, start + i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    case LongType => {
      val vector = vec.asInstanceOf[BigIntVector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, start + i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    }
    case StringType => {
      val vector = vec.asInstanceOf[VarCharVector]
      (0 until numRows).foreach { i =>
        val num = start + i
        val toAdd = s"${num}testString"
        vector.setSafe(i, new Text(toAdd))
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    }
    case FloatType =>
      val vector = vec.asInstanceOf[Float4Vector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, start + i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    case DoubleType =>
      val vector = vec.asInstanceOf[Float8Vector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, start + i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    case DateType =>
      val vector = vec.asInstanceOf[DateDayVector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, start + i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    case TimestampType =>
      val vector = vec.asInstanceOf[TimeStampMicroTZVector]
      val startms = 20145678912L;
      (0 until numRows).foreach { i =>
        vector.setSafe(i, startms + start + i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
  }

  private def setupArrowVector(name: String, dataType: DataType): ValueVector = {
    val rootAllocator = new RootAllocator(Long.MaxValue)
    val allocator = rootAllocator.newChildAllocator(s"$name", 0, Long.MaxValue)
    val vector = toArrowField(s"field$name", dataType, nullable = true, "Utc")
      .createVector(allocator)
    vector
  }
}

