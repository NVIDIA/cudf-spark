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
package com.nvidia.spark.rapids.iceberg.data

import scala.collection.JavaConverters._
import scala.language.reflectiveCalls

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore}
import com.nvidia.spark.rapids.{GpuColumnVector, LazySpillableColumnarBatch, NoopMetric, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric.{JOIN_TIME, OP_TIME_LEGACY}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.{fieldIndex, PooledTableGen}
import com.nvidia.spark.rapids.iceberg.data.TestGpuDeleteLoader._
import com.nvidia.spark.rapids.iceberg.parquet.{GpuIcebergParquetReaderConf, SingleFile}
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{DeleteFile, FileContent, FileFormat, FileMetadata, MetadataColumns, PartitionSpec, Schema}
import org.apache.iceberg.MetadataColumns.isMetadataColumn
import org.apache.iceberg.common.DynMethods
import org.apache.iceberg.hadoop.HadoopFileIO
import org.apache.iceberg.spark.GpuTypeToSparkType.toSparkType
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.Tables.Table

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuDeleteFilterSuite extends AnyFunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    SpillFramework.initialize(new RapidsConf(new SparkConf))
  }

  override def afterAll(): Unit = {
    SpillFramework.shutdown()
  }

  private def fixture(deleteFiles: Seq[DeleteFile], _tableSchema: Schema = TABLE_SCHEMA) = new {
    val tableSchema = _tableSchema
    val deletedRows: Map[String, Seq[Long]] = Map(
      PooledTableGen.PooledFilePaths(0) -> Seq(1L, 3L, 5L),
      PooledTableGen.PooledFilePaths(1) -> Seq(2L, 4L, 7L),
    )
    val tableGen = new PooledTableGen(tableGenSchema(tableSchema, deleteFiles))
    val deleteLoader = new TestGpuDeleteLoader(tableGen, 57, deletedRows)
    val deleteFilter = gpuDeleteFilterOf(tableSchema, deleteFiles, Some(deleteLoader))
    val generatedInputData = {
      val batch1 = tableGen.toColumnarBatch(NUM_ROWS)
      val batch2 = tableGen.toColumnarBatch(NUM_ROWS)
      val batch3 = {
        tableGen.resetFilePath(PooledTableGen.PooledFilePaths(1))
        tableGen.toColumnarBatch(NUM_ROWS)
      }
      Seq(batch1, batch2, batch3)
        .iterator
    }
    val eqFieldIdSets = deleteFiles
      .filter(_.content() == FileContent.EQUALITY_DELETES)
      .map(_.equalityFieldIds().asScala.map(_.toInt).toSeq)
      .distinct

    val eqDelColIndices = eqFieldIdSets.map(_.map(fieldIndex(deleteFilter.requiredSchema, _)))
    val eqDelValueSets = eqFieldIdSets.map(deleteLoader.loadEqDeletes)
    val posDelColIndices = if (deleteFiles.exists(_.content() == FileContent.POSITION_DELETES)) {
      Seq(fieldIndex(deleteFilter.requiredSchema, MetadataColumns.FILE_PATH.fieldId()),
        fieldIndex(deleteFilter.requiredSchema, MetadataColumns.ROW_POSITION.fieldId()))
    } else {
      Seq.empty
    }
  }


  test("Filter with equality deletes only") {
    val EQ_DELETE_FIELD_COUNT = 3
    val testColumns = Table(("eqFieldId", "eqFieldName"),
      TABLE_SCHEMA.columns()
        .asScala
        .filter(canBeEqDeleteKey)
        .filterNot(f => isMetadataColumn(f.fieldId()))
        .combinations(EQ_DELETE_FIELD_COUNT)
        .map { fields =>
          (fields.map(_.fieldId()).toSeq, fields.map(_.name()).toSeq)
        }.toSeq: _*)

    forAll(testColumns) { (eqFieldIds: Seq[Int], _: Seq[String]) =>
      val deleteFile = eqDeleteFile(eqFieldIds)
      val f = fixture(Seq(deleteFile))


      val isDeletedColIdx = f.deleteFilter.requiredSchema.columns().size()

      val inputData = f.generatedInputData

      var rowIdx = 0
      for (resultBatch <- f.deleteFilter.filter(inputData)) {
        withResource(resultBatch) { _ =>
          val cuVecs = GpuColumnVector.extractBases(resultBatch)
          withResource(cuVecs.safeMap(_.copyToHost())) { hostVecs =>
            for (i <- 0 until resultBatch.numRows) {
              val data: Array[AnyRef] = f.eqDelColIndices
                .head
                .map(hostVecs(_))
                .map(valueOf(_, Integer.valueOf(i)))
                .toArray[AnyRef]
              val inEqDeleteSet = f.eqDelValueSets.head.exists(_.sameElements(data))
              val isDeleted = hostVecs(isDeletedColIdx).getBoolean(i)
              if (inEqDeleteSet) {
                assert(isDeleted, s"Row $rowIdx should be deleted, data: ${data.mkString(",")}")
              } else {
                assert(!isDeleted,
                  s"Row $rowIdx should not be deleted, data: ${data.mkString(",")}")
              }
              rowIdx += 1
            }
          }
        }
      }
    }
  }

  test("Filter with position deletes only") {
    val deleteFile = posDeleteFile()
    val f = fixture(Seq(deleteFile))

    val inputData = f.generatedInputData

    val filePathColIdx = f.posDelColIndices(0)
    val rowIdxColIdx = f.posDelColIndices(1)
    val isDeletedColIdx = f.deleteFilter.requiredSchema.columns().size()

    for (resultBatch <- f.deleteFilter.filter(inputData)) {
      withResource(resultBatch) { _ =>
        val baseCuVecs = GpuColumnVector.extractBases(resultBatch)
        withResource(baseCuVecs.safeMap(_.copyToHost())) { hostVecs =>
          for (i <- 0 until resultBatch.numRows) {
            val filePath = hostVecs(filePathColIdx).getJavaString(i)
            val curRowIdx = hostVecs(rowIdxColIdx).getLong(i)

            val isDeleted = hostVecs(isDeletedColIdx).getBoolean(i)

            val expectedDeleted = f.deletedRows
              .get(filePath)
              .exists(_.contains(curRowIdx))

            if (expectedDeleted) {
              assert(isDeleted, s"Filepath $filePath Row $curRowIdx should be deleted")
            } else {
              assert(!isDeleted, s"Filepath $filePath Row $curRowIdx should not be deleted")
            }
          }
        }
      }
    }
  }

  test("Filter with eq deletes and position deletes") {
    val eqFieldIdSets = Seq(Seq(1, 3), Seq(2, 6))
    val f = fixture(eqFieldIdSets.map(eqDeleteFile) :+ posDeleteFile())

    val inputData = f.generatedInputData

    val joinResultColIdx = f.deleteFilter.requiredSchema.columns().size()

    var idx = 0
    for (resultBatch <- f.deleteFilter.filter(inputData)) {
      withResource(resultBatch) { _ =>
        val baseGpuVecs = GpuColumnVector.extractBases(resultBatch)
        withResource(baseGpuVecs.safeMap(_.copyToHost())) { hostVecs =>
          for (i <- 0 until resultBatch.numRows) {
            val (filePath, rowIdx) = (hostVecs(f.posDelColIndices(0)).getJavaString(i),
              hostVecs(f.posDelColIndices(1)).getLong(i))
            val isDeleted = hostVecs(joinResultColIdx).getBoolean(i)

            val eqDeleteDeleted = f.eqDelColIndices.zip(f.eqDelValueSets).exists {
              case (colIndices, valueSet) =>
                val data = colIndices.map { idx =>
                  valueOf(hostVecs(idx), Integer.valueOf(i))
                }
                valueSet.exists(_.sameElements(data))
            }

            val expectedDeleted = eqDeleteDeleted || f.deletedRows
              .get(filePath)
              .exists(_.contains(rowIdx))

            if (expectedDeleted) {
              assert(isDeleted, s"Row $idx should be deleted")
            } else {
              assert(!isDeleted, s"Row $idx should not be deleted")
            }

            idx += 1
          }
        }
      }
    }
  }

  test("Filter with IS_DELETED metadata column") {
    val eqFieldIdSets = Seq(Seq(1, 3), Seq(2, 6))
    val f = fixture(eqFieldIdSets.map(eqDeleteFile) :+ posDeleteFile(),
      tableSchemaWithIsDeletedColumn())

    val inputData = f.generatedInputData

    val isDeletedColIdx = fieldIndex(f.tableSchema, MetadataColumns.IS_DELETED.fieldId())

    var idx = 0
    for (resultBatch <- f.deleteFilter.filter(inputData)) {
      withResource(resultBatch) { _ =>
        val baseGpuVecs = GpuColumnVector.extractBases(resultBatch)
        withResource(baseGpuVecs.safeMap(_.copyToHost())) { hostVecs =>
          for (i <- 0 until resultBatch.numRows) {
            val (filePath, rowIdx) = (hostVecs(f.posDelColIndices(0)).getJavaString(i),
              hostVecs(f.posDelColIndices(1)).getLong(i))
            val isDeleted = hostVecs(isDeletedColIdx).getBoolean(i)

            val eqDeleteDeleted = f.eqDelColIndices.zip(f.eqDelValueSets).exists {
              case (colIndices, valueSet) =>
                val data = colIndices.map { idx =>
                  valueOf(hostVecs(idx), Integer.valueOf(i))
                }
                valueSet.exists(_.sameElements(data))
            }

            val expectedDeleted = eqDeleteDeleted || f.deletedRows
              .get(filePath)
              .exists(_.contains(rowIdx))

            if (expectedDeleted) {
              assert(isDeleted, s"Row $idx should be deleted")
            } else {
              assert(!isDeleted, s"Row $idx should not be deleted")
            }

            idx += 1
          }
        }
      }
    }
  }

  test("Delete with eq deletes and position deletes") {
    val eqFieldIdSets = Seq(Seq(1, 3), Seq(2, 6))
    val f = fixture(eqFieldIdSets.map(eqDeleteFile) :+ posDeleteFile())

    val inputData = f.generatedInputData

    for (resultBatch <- f.deleteFilter.filterAndDelete(inputData)) {
      withResource(resultBatch) { _ =>
        val baseGpuVecs = (0 until resultBatch.numCols)
          .map(resultBatch.column)
          .map(_.asInstanceOf[GpuColumnVector])
          .map(_.getBase)
        withResource(baseGpuVecs.safeMap(_.copyToHost())) { hostVecs =>
          for (i <- 0 until resultBatch.numRows) {
            for ((colIndices, valueSet) <- f.eqDelColIndices.zip(f.eqDelValueSets)) {
              val data = colIndices.map { idx =>
                valueOf(hostVecs(idx), Integer.valueOf(i))
              }
              assert(!valueSet.exists(_.sameElements(data)),
                s"Row ${data.mkString(",", "[", "]")} should be deleted by eq deletes")
            }
          }
        }
      }
    }
  }

  test("Delete with IS_DELETED metadata column") {
    val eqFieldIdSets = Seq(Seq(1, 3), Seq(2, 6))
    val f = fixture(eqFieldIdSets.map(eqDeleteFile) :+ posDeleteFile(),
      tableSchemaWithIsDeletedColumn())

    val inputData = f.generatedInputData
    val isDeletedColIdx = fieldIndex(f.tableSchema, MetadataColumns.IS_DELETED.fieldId())

    for (resultBatch <- f.deleteFilter.filterAndDelete(inputData)) {
      withResource(resultBatch) { _ =>
        val baseGpuVecs = (0 until resultBatch.numCols)
          .map(resultBatch.column)
          .map(_.asInstanceOf[GpuColumnVector])
          .map(_.getBase)
        withResource(baseGpuVecs.safeMap(_.copyToHost())) { hostVecs =>
          for (i <- 0 until resultBatch.numRows) {
            for ((colIndices, valueSet) <- f.eqDelColIndices.zip(f.eqDelValueSets)) {
              val data = colIndices.map { idx =>
                valueOf(hostVecs(idx), Integer.valueOf(i))
              }
              assert(!valueSet.exists(_.sameElements(data)),
                s"Row ${data.mkString(",", "[", "]")} should be deleted by eq deletes")
            }

            assert(!hostVecs(isDeletedColIdx).getBoolean(i),
              s"Row $i should be deleted by IS_DELETED column")
          }
        }
      }
    }
  }
}

class TestGpuDeleteLoader(private val tableGen: PooledTableGen,
    private val rows: Int,
    private val posDeletes: Map[String, Seq[Long]] = Map.empty
) extends GpuDeleteLoader {

  override def loadDeletes(deletes: Seq[DeleteFile],
      schema: Schema,
      sparkTypes: Array[DataType]): LazySpillableColumnarBatch = {
    require(deletes.nonEmpty, "No deletes to load")
    require(deletes.map(_.content()).distinct.length == 1,
      "Only one kind of delete content is supported")

    deletes.head.content() match {
      case FileContent.EQUALITY_DELETES =>
        val fieldIds = schema
          .columns()
          .asScala
          .map(_.fieldId())
          .toSeq
        withResource(tableGen.pooledHostVector(fieldIds, rows)) { batch =>
          LazySpillableColumnarBatch(batch, "Eq deletes build")
        }
      case FileContent.POSITION_DELETES =>
        val pairs = posDeletes
          .keys
          .flatMap(k => posDeletes(k).map(v => (k, v)))
          .toSeq

        val filePaths = pairs.map(_._1)
        val rowPoses = pairs.map(p => java.lang.Long.valueOf(p._2))

        val hostVectors = Seq(
          HostColumnVector.fromStrings(filePaths: _*),
          HostColumnVector.fromBoxedLongs(rowPoses: _*)
        )

        val sparkTypes = Seq(StringType, LongType)

        withResource(hostVectors) { _ =>
          val columns = hostVectors.safeMap(_.copyToDevice())
            .zip(sparkTypes)
            .safeMap(p => GpuColumnVector.from(p._1, p._2))
            .toArray[ColumnVector]

          withResource(new ColumnarBatch(columns, pairs.length)) { batch =>
            LazySpillableColumnarBatch(batch, "Pos deletes build")
          }
        }

      case c => throw new UnsupportedOperationException(s"Unsupported delete content: $c")
    }
  }

  def loadEqDeletes(eqFieldIds: Seq[Int]): Seq[Array[AnyRef]] = {
    tableGen.pooledValues(eqFieldIds, rows)
  }
}


private object TestGpuDeleteLoader {
  val NUM_ROWS: Int = 1000
  val TABLE_SCHEMA: Schema = tableSchema()
  val TABLE_SPARK_TYPE: StructType = toSparkType(TABLE_SCHEMA)

  def valueOf(col: HostColumnVectorCore, rowIdx: Int): AnyRef = {
    if (col.isNull(rowIdx)) {
      return null
    }
    col.getType.getTypeId match {
      case DType.DTypeEnum.DECIMAL128 => col.getBigDecimal(rowIdx).asInstanceOf[AnyRef]
      case DType.DTypeEnum.STRING => col.getJavaString(rowIdx)
      case _ => HOST_COL_GET_ELEMENT.invoke(col, Integer.valueOf(rowIdx)).asInstanceOf[AnyRef]
    }
  }

  private val HOST_COL_GET_ELEMENT: DynMethods.UnboundMethod = DynMethods
    .builder("getElement")
    .hiddenImpl(classOf[HostColumnVectorCore], classOf[Int])
    .buildChecked()

  def tableSchema(): Schema = {
    new Schema(
      NestedField.optional(1, "offline", Types.BooleanType.get()),
      NestedField.optional(2, "count", Types.IntegerType.get()),
      NestedField.optional(3, "id", Types.LongType.get()),
      NestedField.optional(4, "height", Types.FloatType.get()),
      NestedField.optional(5, "weight", Types.DoubleType.get()),
      NestedField.optional(6, "name", Types.StringType.get()),
      NestedField.optional(7, "birthday", Types.DateType.get()),
      // Spark doesn't support Time
      //      NestedField.optional(8, "time", Types.TimeType.get()),
      NestedField.optional(9, "ts", Types.TimestampType.withZone()),
      NestedField.optional(10, "binary", Types.BinaryType.get()),
      NestedField.optional(11, "fixed", Types.FixedType.ofLength(10)),
      NestedField.optional(12, "decimal32", Types.DecimalType.of(7, 3)),
      NestedField.optional(13, "decimal64", Types.DecimalType.of(12, 3)),
      NestedField.optional(14, "decimal128", Types.DecimalType.of(38, 3)),
    )
  }

  def tableSchemaWithIsDeletedColumn(): Schema = {
    val allFields = (TABLE_SCHEMA.columns().asScala :+ MetadataColumns.IS_DELETED).toSeq
    new Schema(allFields: _*)
  }

  def eqDeleteFile(eqFieldIds: Seq[Int]): DeleteFile = {
    FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
      .ofEqualityDeletes(eqFieldIds: _*)
      .withPath("/tmp/eqdele.parquet")
      .withFormat(FileFormat.PARQUET)
      .withRecordCount(5)
      .withFileSizeInBytes(1024)
      .build()
  }

  def posDeleteFile(): DeleteFile = {
    FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
      .ofPositionDeletes()
      .withPath("/tmp/pos_deletes.parquet")
      .withFormat(FileFormat.PARQUET)
      .withRecordCount(5)
      .withFileSizeInBytes(1024)
      .build()
  }

  def gpuDeleteFilterOf(
      tableSchema: Schema,
      deleteFiles: Seq[DeleteFile],
      deleteLoader: Option[GpuDeleteLoader]): GpuDeleteFilter = {
    new GpuDeleteFilter(
      new IcebergFileIO(new HadoopFileIO(new Configuration())),
      tableSchema,
      Map.empty,
      GpuIcebergParquetReaderConf(
        caseSensitive = false,
        new Configuration(),
        100,
        100000,
        10000000,
        10000000,
        useChunkedReader = false,
        10000000,
        skipReadEstimate = true,
        None,
        parquetDebugDumpAlways = false,
        Map(OP_TIME_LEGACY -> NoopMetric, JOIN_TIME -> NoopMetric),
        SingleFile,
        tableSchema,
        None),
      deleteFiles,
      deleteLoader)
  }

  def tableGenSchema(tableSchema: Schema, deleteFiles: Seq[DeleteFile]): Schema = {
    gpuDeleteFilterOf(tableSchema, deleteFiles, None).requiredSchema
  }

  def canBeEqDeleteKey(field: NestedField): Boolean = {
    field.`type`().typeId() match {
      case TypeID.FLOAT | TypeID.DOUBLE | TypeID.BINARY => false
      case _ => true
    }
  }
}
