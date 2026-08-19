/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import ai.rapids.cudf.{DType, HostColumnVector, Table}
import ai.rapids.cudf.DType.DTypeEnum
import ai.rapids.cudf.HostColumnVector.{BasicType, ListType}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.iceberg.ColumnDataGen.newRandom
import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.spark.GpuTypeToSparkType.toSparkType
import org.apache.iceberg.types.{Type, Types}

import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

trait ColumnDataGen[A <: AnyRef] {
  def generate(size: Int): Array[A]

  def toHostColumnVector(size: Int): HostColumnVector

  def poolValue(size: Int, addNull: Boolean = true): Array[A]

  def toPoolHostColumnVector(size: Int, addNull: Boolean = true): HostColumnVector
}


class RandomPooledDataGen[A <: AnyRef : ClassTag](private val random: Random,
    private val poolSize: Int,
    private val genNext: Random => A,
    private val toHostColumnVector: Array[A] => HostColumnVector) extends ColumnDataGen[A] {
  private val pool: Array[A] = fillPool()

  def generate(size: Int): Array[A] = {
    (0 until size)
      .map(_ => pool(random.nextInt(poolSize)))
      .toArray[A]
  }

  def poolValue(size: Int, addNull: Boolean = true): Array[A] = {
    require(size <= poolSize, s"Size $size is greater than pool size $poolSize")
    val values = (0 until size)
      .map(pool(_))
      .toArray[A]

    if (addNull) {
      values :+ null.asInstanceOf[A]
    } else {
      values
    }
  }

  def toPoolHostColumnVector(size: Int, addNull: Boolean = true): HostColumnVector = {
    toHostColumnVector(poolValue(size, addNull))
  }

  private final def fillPool(): Array[A] = {
    val ret = new Array[A](poolSize)
    for (i <- 0 until poolSize) {
      val isNull = random.nextBoolean()
      if (isNull) {
        ret(i) = null.asInstanceOf[A]
      } else {
        ret(i) = genNext(random)
      }
    }
    ret
  }

  override def toHostColumnVector(size: Int): HostColumnVector = {
    val data = generate(size)
    toHostColumnVector(data)
  }
}

class FilePathDataGen(var curFilePath: String) extends ColumnDataGen[String] {
  require(curFilePath != null, "File path cannot be null")

  def setCurrentFilePath(filePath: String): Unit = {
    require(filePath != null, "File path cannot be null")
    curFilePath = filePath
  }

  def generate(size: Int): Array[String] = {
    (0 until size)
      .map(_ => curFilePath)
      .toArray[String]
  }

  def toHostColumnVector(size: Int): HostColumnVector = {
    val data = generate(size)
    HostColumnVector.fromStrings(data: _*)
  }

  def poolValue(size: Int, addNull: Boolean = true): Array[String] = {
    throw new UnsupportedOperationException("Pooled value not supported by FilePathDataGen")
  }


  def toPoolHostColumnVector(size: Int, addNull: Boolean = true): HostColumnVector = {
    throw new UnsupportedOperationException("Pooled vector not supported by FilePathDataGen")
  }
}

class RowIndexDataGen(var rowIdx: Int = 0) extends ColumnDataGen[JLong] {
  def reset(): Unit = {
    rowIdx = 0
  }

  override def generate(size: Int): Array[JLong] = {
    val data = (rowIdx until rowIdx + size)
      .map(r => JLong.valueOf(r))
      .toArray
    rowIdx += size
    data
  }

  override def toHostColumnVector(size: Int): HostColumnVector = {
    val data = generate(size)
    HostColumnVector.fromBoxedLongs(data: _*)
  }

  override def poolValue(size: Int, addNull: Boolean): Array[JLong] = {
    throw new UnsupportedOperationException("Pooled value not supported by RowIndexDataGen")
  }

  override def toPoolHostColumnVector(size: Int, addNull: Boolean): HostColumnVector = {
    throw new UnsupportedOperationException("Pooled vector not supported by RowIndexDataGen")
  }
}

class ConstantDataGen[A <: AnyRef : ClassTag](val value: A, toHostVec: Array[A] => HostColumnVector)
  extends ColumnDataGen[A] {
  override def generate(size: Int): Array[A] = {
    Array.fill(size)(value)
  }

  override def toHostColumnVector(size: Int): HostColumnVector = {
    val data = generate(size)
    toHostVec(data)
  }

  override def poolValue(size: Int, addNull: Boolean): Array[A] = {
    if (addNull) {
      generate(size) :+ null.asInstanceOf[A]
    } else {
      generate(size)
    }
  }

  override def toPoolHostColumnVector(size: Int, addNull: Boolean): HostColumnVector = {
    toHostVec(poolValue(size, addNull))
  }
}

object ColumnDataGen {
  private val SEED = 1742356520L
  val POOL_SIZE = 300

  private[iceberg] def newRandom: Random = {
    new Random(SEED)
  }

  def boolean(random: Random, poolSize: Int = POOL_SIZE): ColumnDataGen[JBoolean] =
    new RandomPooledDataGen[JBoolean](random, poolSize,
      _.nextBoolean(),
      HostColumnVector.fromBoxedBooleans(_: _*))

  def int(random: Random, poolSize: Int = POOL_SIZE): ColumnDataGen[JInt] =
    new RandomPooledDataGen[JInt](random, poolSize,
      _.nextInt(),
      HostColumnVector.fromBoxedInts(_: _*))

  def long(random: Random, poolSize: Int = POOL_SIZE): ColumnDataGen[JLong] =
    new RandomPooledDataGen[JLong](random, poolSize,
      _.nextLong(),
      HostColumnVector.fromBoxedLongs(_: _*))


  def float(random: Random, poolSize: Int = POOL_SIZE): ColumnDataGen[JFloat] =
    new RandomPooledDataGen[JFloat](random, poolSize,
      _.nextFloat(),
      HostColumnVector.fromBoxedFloats(_: _*))

  def double(random: Random, poolSize: Int = POOL_SIZE): ColumnDataGen[JDouble] =
    new RandomPooledDataGen[JDouble](random, poolSize,
      _.nextDouble(),
      HostColumnVector.fromBoxedDoubles(_: _*))

  def string(random: Random, poolSize: Int = POOL_SIZE): ColumnDataGen[String] =
    new RandomPooledDataGen[String](random, poolSize,
      _.nextString(10),
      HostColumnVector.fromStrings(_: _*))

  def date(random: Random, poolSize: Int = POOL_SIZE): ColumnDataGen[JInt] =
    new RandomPooledDataGen[JInt](random, poolSize,
      _.nextInt(1000000),
      HostColumnVector.timestampDaysFromBoxedInts(_: _*))

  def timestamp(random: Random, poolSize: Int = POOL_SIZE): ColumnDataGen[JLong] =
    new RandomPooledDataGen[JLong](random, poolSize,
      r => Math.abs(r.nextLong()),
      HostColumnVector.timestampMicroSecondsFromBoxedLongs(_: _*))

  def binary(random: Random, poolSize: Int = POOL_SIZE, typeLen: Option[Int] = None)
  : ColumnDataGen[JList[Byte]] =
    new RandomPooledDataGen[JList[Byte]](random, poolSize,
      r => {
        val len = typeLen.getOrElse(r.nextInt(100) + 1)
        val bytes = new Array[Byte](len)
        r.nextBytes(bytes)
        bytes.toList.asJava
      },
      HostColumnVector.fromLists(new ListType(true, new BasicType(false, DType.UINT8)),
        _: _*))

  def decimal(random: Random, poolSize: Int = POOL_SIZE, precision: Int, scale: Int)
  : ColumnDataGen[JBigDecimal] =
    new RandomPooledDataGen[JBigDecimal](random, poolSize,
      r => {
        val beforePoint = (0 until (precision - scale))
          .map(_ => r.nextInt(8) + 1)
          .mkString("")
        val afterPoint = (0 until scale)
          .map(_ => r.nextInt(8) + 1)
          .mkString("")

        BigDecimal(s"$beforePoint.$afterPoint")
          .bigDecimal
      },
      values => {
        val dType = if (precision <= DType.DECIMAL32_MAX_PRECISION) {
          DType.create(DTypeEnum.DECIMAL32, -scale)
        } else if (precision <= DType.DECIMAL64_MAX_PRECISION) {
          DType.create(DTypeEnum.DECIMAL64, -scale)
        } else if (precision <= DType.DECIMAL128_MAX_PRECISION) {
          DType.create(DTypeEnum.DECIMAL128, -scale)
        } else {
          throw new UnsupportedOperationException(
            s"Unsupported precision $precision, scale $scale for decimal type")
        }
        HostColumnVector.build(dType, values.length, b => {
          values.foreach { value =>
            Option(value) match {
              case Some(v) =>
                b.append(v)
              case None =>
                b.appendNull()
            }
          }
        })
      })
}

class PooledTableGen(val schema: Schema,
    private val random: Random = newRandom,
    private val poolSize: Int = ColumnDataGen.POOL_SIZE) {

  private val filePathDataGen = new FilePathDataGen(PooledTableGen.PooledFilePaths.head)
  private val rowIndexDataGen = new RowIndexDataGen()
  private val isDeletedDataGen = new ConstantDataGen[JBoolean](false,
    arr => HostColumnVector.fromBoxedBooleans(arr: _*))


  private val sparkType = toSparkType(schema)
  private val columnDataGens = schema
    .columns()
    .asScala
    .toArray.map(f => {
      if (f.fieldId() == MetadataColumns.FILE_PATH.fieldId()) {
        filePathDataGen
      } else if (f.fieldId() == MetadataColumns.ROW_POSITION.fieldId()) {
        rowIndexDataGen
      } else if (f.fieldId() == MetadataColumns.IS_DELETED.fieldId()) {
        isDeletedDataGen
      } else {
        columnDataGen(f.`type`())
      }
    })

  def resetFilePath(newFilePath: String): Unit = {
    filePathDataGen.setCurrentFilePath(newFilePath)
    rowIndexDataGen.reset()
  }

  def generate(size: Int): Table = {
    withResource(columnDataGens.map(_.toHostColumnVector(size))) { hostVectors =>
      withResource(hostVectors.map(_.copyToDevice())) { gpuVectors =>
        new Table(gpuVectors: _*)
      }
    }
  }

  def toColumnarBatch(size: Int): ColumnarBatch = {
    withResource(generate(size)) { table =>
      GpuColumnVector.from(table, sparkType.fields.map(_.dataType))
    }
  }

  def pooledValues(fieldIds: Seq[Int], size: Int, addNull: Boolean = true): Seq[Array[AnyRef]] = {
    require(fieldIds.nonEmpty, "Field ids cannot be empty")
    val fieldIndices = fieldIds.map(fieldIdIndex)

    val columns = fieldIndices.map { idx =>
      columnDataGens(idx)
        .asInstanceOf[RandomPooledDataGen[_ <: AnyRef]]
        .poolValue(size, addNull)
    }.toArray

    require(columns.map(_.length).distinct.length == 1, "All columns must have the same length")

    columns
      .head
      .indices
      .map { i =>
        columns.map(_(i))
      }
  }

  def pooledHostVector(fieldIds: Seq[Int], size: Int, addNull: Boolean = true): ColumnarBatch = {
    require(fieldIds.nonEmpty, "Field ids cannot be empty")
    val fieldIndices = fieldIds.map(fieldIdIndex)
    val vectors = fieldIndices
      .safeMap { idx =>
        columnDataGens(idx)
          .asInstanceOf[RandomPooledDataGen[_]]
          .toPoolHostColumnVector(size, addNull)
      }

    withResource(vectors) { hostVecs =>
      val rowCount = vectors.head.getRowCount
      require(!vectors.exists(_.getRowCount != rowCount),
        "All columns must have the same length")

      val gpuVecs = hostVecs.zip(fieldIndices)
        .safeMap {
          case (hostVec, fieldIdx) => GpuColumnVector.from(hostVec.copyToDevice(),
            sparkType.fields(fieldIdx).dataType)
        }.toArray[ColumnVector]
      new ColumnarBatch(gpuVecs, size)
    }
  }

  private def fieldIdIndex(fieldId: Int): Int = {
    val idx = schema
      .columns()
      .asScala
      .indexWhere(_.fieldId() == fieldId)

    if (idx == -1) {
      throw new IllegalArgumentException(s"Cannot find field id $fieldId in table schema")
    }

    idx
  }


  private def columnDataGen(fieldType: Type): ColumnDataGen[_] = {
    fieldType.typeId match {
      case Type.TypeID.BOOLEAN => ColumnDataGen.boolean(random, poolSize)
      case Type.TypeID.INTEGER => ColumnDataGen.int(random, poolSize)
      case Type.TypeID.LONG => ColumnDataGen.long(random, poolSize)
      case Type.TypeID.FLOAT => ColumnDataGen.float(random, poolSize)
      case Type.TypeID.DOUBLE => ColumnDataGen.double(random, poolSize)
      case Type.TypeID.STRING => ColumnDataGen.string(random, poolSize)
      case Type.TypeID.DATE => ColumnDataGen.date(random, poolSize)
      case Type.TypeID.TIMESTAMP => ColumnDataGen.timestamp(random, poolSize)
      case Type.TypeID.BINARY => ColumnDataGen.binary(random, poolSize)
      case Type.TypeID.FIXED => ColumnDataGen.binary(random, poolSize,
        Some(fieldType.asInstanceOf[Types.FixedType].length()))
      case Type.TypeID.DECIMAL =>
        val decimalType = fieldType.asInstanceOf[Types.DecimalType]
        ColumnDataGen.decimal(random, poolSize, decimalType.precision(), decimalType.scale())
      case _ => throw new IllegalArgumentException(s"Unsupported type $fieldType")
    }
  }
}

object PooledTableGen {
  val PooledFilePaths: Seq[String] = Seq("/tmp/data1.parquet", "/tmp/data2.parquet")
}
