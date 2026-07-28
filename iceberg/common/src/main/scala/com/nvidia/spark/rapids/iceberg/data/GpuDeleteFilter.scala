/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.data

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric.{JOIN_TIME, OP_TIME_LEGACY}
import com.nvidia.spark.rapids.fileio.iceberg.{IcebergFileIO, IcebergInputFile}
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter2._
import com.nvidia.spark.rapids.iceberg.fieldIndex
import com.nvidia.spark.rapids.iceberg.parquet.GpuIcebergParquetReaderConf
import org.apache.iceberg.{DeleteFile, FileContent, MetadataColumns, Schema}
import org.apache.iceberg.spark.GpuTypeToSparkType.toSparkType
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.types.TypeUtil.getProjectedIds

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.rapids.execution.HashedExistenceJoinIterator
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuDeleteFilter(
    private val rapidsFileIO: IcebergFileIO,
    private val tableSchema: Schema,
    val inputFiles: Map[String, IcebergInputFile],
    val parquetConf: GpuIcebergParquetReaderConf,
    private val deletes: Seq[DeleteFile],
    deleteLoaderProvider: => Option[GpuDeleteLoader] = None) extends Logging with AutoCloseable {
  private lazy val readSchema = parquetConf.expectedSchema

  private lazy val deleteLoader = deleteLoaderProvider.getOrElse(
    new DefaultDeleteLoader(rapidsFileIO, inputFiles, parquetConf))

  private lazy val (eqDeleteFiles, posDeleteFiles) = {
    deletes.find(d => d.content() != FileContent.EQUALITY_DELETES &&
        d.content() != FileContent.POSITION_DELETES)
      .foreach(d => {
        throw new UnsupportedOperationException(s"Unsupported delete content: ${d.content()}")
      })
    deletes.partition(_.content() == FileContent.EQUALITY_DELETES)
  }

  /**
   * The schema required by the filter, which should be the schema of [[filter]]'s input batches.
   *
   * This schema is computed with the following rules:
   * 1. Add all the fields in the [[GpuIcebergParquetReaderConf.expectedSchema]].
   * 2. Add all missing fields which are required by the equality delete files, but not in the
   * [[GpuIcebergParquetReaderConf.expectedSchema]], if any.
   * 3. Add [[MetadataColumns.ROW_POSITION]] and [[MetadataColumns.FILE_PATH]] if there are
   * position delete files, and they are not in the schema.
   */
  lazy val requiredSchema: Schema = computeRequiredSchema()

  private lazy val isDeleteColIdx: Option[Int] = {
    val idx = requiredSchema
      .columns()
      .asScala
      .indexWhere(_.fieldId() == MetadataColumns.IS_DELETED.fieldId())
    if (idx == -1) {
      // Not found
      None
    } else {
      Some(idx)
    }
  }

  private lazy val filterOutputSparkDataTypes: Array[DataType] = isDeleteColIdx match {
    case Some(_) =>
      toSparkType(requiredSchema).fields.map(_.dataType)
    case None =>
      val originalSparkTypes = toSparkType(requiredSchema)
      val ret = new Array[DataType](originalSparkTypes.fields.length + 1)
      for (i <- originalSparkTypes.fields.indices) {
        ret(i) = originalSparkTypes.fields(i).dataType
      }
      // IS_DELETED column
      ret(originalSparkTypes.fields.length) = BooleanType
      ret
  }

  private lazy val posDeleteContext = loadPosDeletesContext()
  private lazy val eqDeleteContexts = loadEqDeleteContexts()


  /**
   * Filter the input batches based on the deletes and delete the rows.
   *
   * There are two differences with [[filter]] method:
   *
   * 1. If [[MetadataColumns.IS_DELETED]] column is not in [[requiredSchema]], this method will
   * <b>not</b> add an extra column. That's to say, the output schema will be exactly same as the
   * input schema.
   * 2. This method will delete the rows that are marked as deleted.
   *
   * Note: This method drops the extra columns that were added for delete matching
   * (FILE_PATH and ROW_POSITION columns for position deletes, and equality delete fields),
   * so the output schema matches the original expectedSchema.
   *
   * @param input Input column batches, which will be closed after this method returns.
   * @return Output column batches with rows deleted based on the filter result.
   */
  def filterAndDelete(input: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    // Compute which columns to keep (only those in the original expectedSchema)
    val expectedFieldIds = parquetConf.expectedSchema.columns().asScala.map(_.fieldId()).toSet

    val dropMask = Array.tabulate[Boolean](filterOutputSparkDataTypes.length) { i =>
      if (i < requiredSchema.columns().size()) {
        val fieldId = requiredSchema.columns().get(i).fieldId()
        !expectedFieldIds.contains(fieldId)  // Drop if not in expected schema
      } else {
        true  // Drop IS_DELETED column (if appended)
      }
    }

    filter(input).map(cb => {
      isDeleteColIdx match {
        case Some(idx) =>
          filterAndDrop(cb, idx, filterOutputSparkDataTypes)
        case None =>
          filterAndDrop(cb, cb.numCols() - 1, filterOutputSparkDataTypes, dropMask)
      }
    })
  }

  /**
   * Filter the input batches based on the deletes.
   *
   * The schema of input batches should be the same as the schema of [[requiredSchema]]. If not,
   * the behavior is undetermined.
   * If the input schema contains the [[MetadataColumns.IS_DELETED]] column, the output schema
   * will be exactly same input schema, and the column data will be updated based on the deletes,
   * by executing logic and operation with existing values in [[MetadataColumns.IS_DELETED]]
   * column.
   * Otherwise, the output schema will be [[requiredSchema]] with an extra
   * [[MetadataColumns.IS_DELETED]] column, the value of which will be updated based on the deletes.
   *
   * @param input Input column batches, which will be closed by after this method returns.
   * @return Input column batches with [[MetadataColumns.IS_DELETED]] column updated based on the
   *         filter result.
   */
  def filter(input: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    val mergeFunc = (delCol1: GpuColumnVector, delCol2: GpuColumnVector) => {
      GpuColumnVector.from(delCol1.getBase.or(delCol2.getBase), BooleanType)
    }
    (eqDeleteContexts ++ posDeleteContext)
      .zipWithIndex
      .foldLeft(input) {
        case (inputBatches, (ctx, idx)) =>
          val outputBatches = ctx.filter(inputBatches)
          isDeleteColIdx match {
            case Some(idx) =>
              outputBatches.map { cb =>
                mergeColumn(cb, cb.numCols() - 1, idx)(mergeFunc)
              }
            case None => if (idx == 0) {
              outputBatches
            } else {
              outputBatches.map { cb =>
                mergeColumn(cb, cb.numCols() - 1, cb.numCols() - 2)(mergeFunc)
              }
            }
          }
      }
  }

  private def computeRequiredSchema(): Schema = {
    if (deletes.isEmpty) {
      return readSchema
    }

    val requiredFieldIds = {
      val eqDeleteIds = eqDeleteFiles
        .flatMap(_.equalityFieldIds().asScala)
        .map(_.toInt)
        .toSet

      if (posDeleteFiles.nonEmpty) {
        eqDeleteIds ++ DELETE_EXTRA_METADATA_COLUMN_IDS
      } else {
        eqDeleteIds
      }
    }


    val readSchemaFieldIds = getProjectedIds(readSchema)
      .asScala
      .map(_.toInt)

    val missingFieldIds = requiredFieldIds -- readSchemaFieldIds

    if (missingFieldIds.isEmpty) {
      return readSchema
    }

    val cols = new ArrayBuffer[Types.NestedField](
      missingFieldIds.size + readSchema.columns().size())
    cols ++= readSchema.columns().asScala

    cols ++= missingFieldIds.diff(DELETE_EXTRA_METADATA_COLUMN_IDS)
      .map(id => Option(tableSchema.asStruct().field(id)).getOrElse {
        throw new IllegalArgumentException(s"Cannot find field id $id in table schema")
      })

    for (field <- DELETE_EXTRA_METADATA_COLUMNS) {
      if (missingFieldIds.contains(field.fieldId())) {
        cols += field
      }
    }

    new Schema(cols.asJava)
  }

  private def loadPosDeletesContext(): Option[DeleteFilterContext] = {
    if (posDeleteFiles.isEmpty) {
      return None
    }

    val posDeleteSparkType = toSparkType(POS_DELETE_SCHEMA)
    val posDeletes = deleteLoader.loadDeletes(posDeleteFiles,
      POS_DELETE_SCHEMA,
      posDeleteSparkType.fields.map(_.dataType))
    val buildKeys = POS_DELETE_SCHEMA
      .columns()
      .asScala
      .indices
      .map(idx => {
        val field = posDeleteSparkType.fields(idx)
        GpuBoundReference(idx, field.dataType, field.nullable)(ExprId(0), field.name)
      })

    val probeKeys = DELETE_EXTRA_METADATA_COLUMNS
      .zipWithIndex
      .map {
        case (field, idx) =>
          val sparkField = posDeleteSparkType.fields(idx)
          val inputIdx = fieldIndex(requiredSchema, field.fieldId())
          GpuBoundReference(inputIdx,
            sparkField.dataType,
            sparkField.nullable)(ExprId(0), sparkField.name)
      }

    Some(DeleteFilterContext(posDeletes,
      buildKeys,
      probeKeys,
      parquetConf.metrics(OP_TIME_LEGACY),
      parquetConf.metrics(JOIN_TIME)))
  }

  private def loadEqDeleteContexts(): Seq[DeleteFilterContext] = {
    if (eqDeleteFiles.isEmpty) {
      return Seq.empty
    }

    eqDeleteFiles
      .map(eqDelete => (eqDelete.equalityFieldIds().asScala, eqDelete))
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .map({
        case (eqIds, eqDeletes) => loadEqDeleteContext(eqDeletes, eqIds.map(_.toInt).toSeq)
      })
      .toSeq
  }

  private def loadEqDeleteContext(eqDeletes: Seq[DeleteFile],
      eqIds: Seq[Int]): DeleteFilterContext = {
    val schema = eqDeleteSchema(eqIds)
    val sparkType = toSparkType(schema)
    val sparkTypes = sparkType.fields.map(_.dataType)
    val buildSide = deleteLoader.loadDeletes(eqDeletes, schema, sparkTypes)
    val buildKeys = eqIds
      .indices
      .map(idx => {
        val field = sparkType.fields(idx)
        GpuBoundReference(idx, field.dataType, field.nullable)(ExprId(eqIds(idx)), field.name)
      })

    val probeKeys = eqIds.zipWithIndex
      .map {
        case (id, index) =>
          val field = sparkType.fields(index)
          val inputIdx = requiredSchema.columns()
            .asScala
            .indexWhere(_.fieldId() == id)
          GpuBoundReference(inputIdx, field.dataType, field.nullable)(ExprId(id), field.name)
      }


    DeleteFilterContext(buildSide,
      buildKeys,
      probeKeys,
      parquetConf.metrics(OP_TIME_LEGACY),
      parquetConf.metrics(JOIN_TIME))
  }

  private def eqDeleteSchema(eqIds: Seq[Int]): Schema = {
    val fields = eqIds
      .map(id => Option(tableSchema.asStruct().field(id))
        .getOrElse(throw new IllegalArgumentException(s"Cannot find field id $id in table schema")))
      .toArray

    new Schema(fields: _*)
  }

  override def close(): Unit = {
  }
}

object GpuDeleteFilter2 {
  private[iceberg] val DELETE_EXTRA_METADATA_COLUMNS: Seq[NestedField] = Seq(
    MetadataColumns.FILE_PATH,
    MetadataColumns.ROW_POSITION)

  private[iceberg] val DELETE_EXTRA_METADATA_COLUMN_IDS: Set[Int] =
    DELETE_EXTRA_METADATA_COLUMNS
      .map(_.fieldId())
      .toSet

  private[iceberg] val POS_DELETE_SCHEMA: Schema = new Schema(
    MetadataColumns.DELETE_FILE_PATH,
    MetadataColumns.DELETE_FILE_POS)


  private[iceberg] def mergeColumn(
      batch: ColumnarBatch, srcColIdx: Int, destColIdx: Int)
    (mergeOp: (GpuColumnVector, GpuColumnVector) => GpuColumnVector): ColumnarBatch = {
    require(srcColIdx >= 0 && srcColIdx < batch.numCols(),
      s"Invalid src column index: $srcColIdx, numCols: ${batch.numCols()}")
    require(destColIdx >= 0 && destColIdx < batch.numCols(),
      s"Invalid dest column index: $destColIdx, numCols: ${batch.numCols()}")
    require(srcColIdx != destColIdx, "srcColIdx and destColIdx should be different")

    val srcVec = batch.column(srcColIdx).asInstanceOf[GpuColumnVector]
    val destVec = batch.column(destColIdx).asInstanceOf[GpuColumnVector]

    withResource(batch) { _ =>
      closeOnExcept(mergeOp(srcVec, destVec)) { mergeVec =>
        val newColumns = new Array[ColumnVector](batch.numCols() - 1)
        for (i <- 0 until batch.numCols() - 1) {
          if (i == destColIdx) {
            newColumns(i) = mergeVec
          } else {
            newColumns(i) = batch.column(i).asInstanceOf[GpuColumnVector].incRefCount()
          }
        }
        new ColumnarBatch(newColumns, batch.numRows())
      }
    }
  }

  private[iceberg] def filterAndDrop(batch: ColumnarBatch,
      isDeletedColIdx: Int,
      outputDataType: Array[DataType],
      dropMask: Array[Boolean] = Array.empty): ColumnarBatch = {
    withResource(batch) { _ =>
      withResource(GpuColumnVector.from(batch)) { table =>
        withResource(table.getColumn(isDeletedColIdx).not()) { maskCv =>
          withResource(table.filter(maskCv)) { newTable =>
            if (dropMask.nonEmpty) {
              withResource(GpuColumnVector.from(newTable, outputDataType)) { newBatch =>
                GpuColumnVector.dropColumns(newBatch, dropMask)
              }
            } else {
              GpuColumnVector.from(newTable, outputDataType)
            }
          }
        }
      }
    }
  }
}

private case class DeleteFilterContext(
    buildBatch: LazySpillableColumnarBatch,
    buildKeys: Seq[GpuExpression],
    probeKeys: Seq[GpuExpression],
    opTime: GpuMetric,
    joinTime: GpuMetric) {
  def filter(input: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    val probeSide = input.map { cb =>
      withResource(cb) {
        LazySpillableColumnarBatch(_, "Deletes probe")
      }
    }

    // No condition, so buildSide doesn't affect anything, but GpuBuildRight matches
    // the join convention used in HashedExistenceJoinIterator (leftSemiHashJoinBuildRight)
    new HashedExistenceJoinIterator(buildBatch,
      buildKeys,
      probeSide,
      probeKeys,
      None,
      GpuBuildRight,
      true,
      opTime,
      joinTime)
  }
}

