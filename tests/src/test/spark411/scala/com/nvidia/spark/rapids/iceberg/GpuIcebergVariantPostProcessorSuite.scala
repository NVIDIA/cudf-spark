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
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.util.{HashMap => JHashMap}

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.{FuzzerUtils, GpuColumnVector, RapidsConf, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.iceberg.parquet.GpuParquetReaderPostProcessor
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.{Type, Types}
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType, VariantType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuIcebergVariantPostProcessorSuite extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    SpillFramework.initialize(new RapidsConf(new SparkConf()))
  }

  override def afterAll(): Unit = {
    try {
      SpillFramework.shutdown()
    } finally {
      super.afterAll()
    }
  }

  private def createParquetInfo(
      shadedSchema: MessageType): ParquetFileInfoWithBlockMeta = {
    val block = new BlockMetaData()
    block.setRowCount(10)
    ParquetFileInfoWithBlockMeta(
      filePath = new Path("/test/file.parquet"),
      blocks = Seq(block),
      partValues = InternalRow.empty,
      schema = ShimUtils.getParquetUnshader().unshade(shadedSchema),
      readSchema = StructType(Seq.empty),
      dateRebaseMode = null,
      timestampRebaseMode = null,
      hasInt96Timestamps = false,
      blocksFirstRowIndices = Seq(0L))
  }

  private def createProcessor(expectedSchema: Schema, fileSchema: Schema) = {
    val shadedSchema = ParquetSchemaUtil.convert(fileSchema, "test")
    new GpuParquetReaderPostProcessor(
      createParquetInfo(shadedSchema),
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)
  }

  test("Variant fields pass through the Iceberg post-processor") {
    val schema = new Schema(
      Types.NestedField.optional(1, "variant_value", Types.VariantType.get()),
      Types.NestedField.optional(2, "payload", Types.StructType.of(
        Types.NestedField.optional(3, "nested_variant", Types.VariantType.get()))),
      Types.NestedField.optional(4, "variant_list",
        Types.ListType.ofOptional(5, Types.VariantType.get())),
      Types.NestedField.optional(6, "variant_map",
        Types.MapType.ofOptional(7, 8, Types.StringType.get(), Types.VariantType.get())))

    assert(createProcessor(schema, schema).displayActionPlan() == "PassThrough")
  }

  test("missing optional Variant field is filled with nulls") {
    val fileSchema = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()))
    val expectedSchema = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "variant_value", Types.VariantType.get()))

    assert(createProcessor(expectedSchema, fileSchema).displayActionPlan() ==
      """ProcessStruct
        |  id (input[0]):
        |    PassThrough
        |  variant_value (generated):
        |    FillNull(variant)""".stripMargin)

    val rowCount = 3
    val inputSchema = StructType(Array(StructField("id", LongType, nullable = true)))
    val inputBatch = FuzzerUtils.createColumnarBatch(inputSchema, rowCount, seed = 42)
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, ACTIVE_ON_DECK_PRIORITY)
    }

    withResource(spillable) { _ =>
      withResource(createProcessor(expectedSchema, fileSchema)
          .process(spillable.getColumnarBatch())) { outputBatch =>
        assert(outputBatch.numRows() == rowCount)
        assert(outputBatch.numCols() == 2)
        assert(outputBatch.column(1).dataType() == VariantType)
        val variantColumn = outputBatch.column(1).asInstanceOf[GpuColumnVector].getBase
        assert(variantColumn.getNullCount == rowCount)
      }
    }
  }

  // Required descendants are visited before their missing optional container. They must not
  // fail independently: the container's action fills the whole column with nulls.
  Seq[(String, Type)](
    "Variant" -> Types.VariantType.get(),
    "struct with required Variant" -> Types.StructType.of(
      Types.NestedField.required(4, "variant_value", Types.VariantType.get())),
    "list with required Variant" -> Types.ListType.ofRequired(4, Types.VariantType.get()),
    "map with required Variant" -> Types.MapType.ofRequired(
      4, 5, Types.StringType.get(), Types.VariantType.get())
  ).foreach { case (typeName, addedType) =>
    test(s"missing nested optional $typeName preserves existing values and parent nulls") {
      val fileSchema = new Schema(
        Types.NestedField.optional(1, "payload", Types.StructType.of(
          Types.NestedField.optional(2, "score", Types.LongType.get()))))
      val expectedSchema = new Schema(
        Types.NestedField.optional(1, "payload", Types.StructType.of(
          Types.NestedField.optional(2, "score", Types.LongType.get()),
          Types.NestedField.optional(3, "added", addedType))))

      val inputType = StructType(Seq(StructField("score", LongType, nullable = true)))
      val hostType = new HostColumnVector.StructType(
        true, new HostColumnVector.BasicType(true, DType.INT64))
      val inputColumn = closeOnExcept(CudfColumnVector.fromStructs(
        hostType,
        new HostColumnVector.StructData(Long.box(10L)),
        null,
        new HostColumnVector.StructData(Long.box(30L)))) { column =>
        GpuColumnVector.from(column, inputType)
      }
      val inputBatch = closeOnExcept(inputColumn) { column =>
        new ColumnarBatch(Array(column), 3)
      }
      val spillable = closeOnExcept(inputBatch) { batch =>
        SpillableColumnarBatch(batch, ACTIVE_ON_DECK_PRIORITY)
      }

      withResource(spillable) { _ =>
        withResource(createProcessor(expectedSchema, fileSchema)
            .process(spillable.getColumnarBatch())) { outputBatch =>
          assert(outputBatch.numRows() == 3)
          assert(outputBatch.numCols() == 1)
          val payloadColumn = outputBatch.column(0).asInstanceOf[GpuColumnVector]
          val payloadType = payloadColumn.dataType().asInstanceOf[StructType]
          assert(payloadType("added").dataType == SparkSchemaUtil.convert(addedType))
          withResource(payloadColumn.copyToHost()) { hostColumn =>
            val payload = hostColumn.getBase
            assert(!payload.isNull(0))
            assert(payload.isNull(1))
            assert(!payload.isNull(2))
            assert(payload.getNumChildren == 2)
            val scores = payload.getChildColumnView(0)
            assert(!scores.isNull(0) && scores.getLong(0) == 10L)
            assert(!scores.isNull(2) && scores.getLong(2) == 30L)
            assert(payload.getChildColumnView(1).getNullCount == 3)
          }
        }
      }
    }
  }
}
