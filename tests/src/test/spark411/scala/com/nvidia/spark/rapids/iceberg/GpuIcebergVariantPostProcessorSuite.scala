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

import java.util.{Arrays, HashMap => JHashMap, List => JList}

import ai.rapids.cudf.{
  ColumnVector => CudfColumnVector,
  DType,
  HostColumnVector,
  HostColumnVectorCore
}
import ai.rapids.cudf.HostColumnVector.StructData
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
import org.apache.spark.sql.types.{
  ArrayType,
  LongType,
  MapType,
  StringType,
  StructField,
  StructType,
  VariantType
}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.types.variant.VariantBuilder

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

  private def score(value: java.lang.Long): StructData = new StructData(value)

  private def assertScoreAndNullVariant(
      column: HostColumnVectorCore,
      row: Int,
      expected: StructData): Unit = {
    assert(column.getNumChildren == 2)
    assert(column.isNull(row) == (expected == null))
    if (expected != null) {
      val expectedScore = expected.getField(0).asInstanceOf[java.lang.Long]
      val scores = column.getChildColumnView(0)
      assert(scores.isNull(row) == (expectedScore == null))
      if (expectedScore != null) {
        assert(scores.getLong(row) == expectedScore.longValue())
      }
    }
    assert(column.getChildColumnView(1).isNull(row))
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

  test("existing Variant values survive null filling and projection reordering") {
    val fileSchema = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "variant_value", Types.VariantType.get()))
    val expectedSchema = new Schema(
      Types.NestedField.optional(2, "variant_value", Types.VariantType.get()),
      Types.NestedField.optional(3, "added", Types.LongType.get()),
      Types.NestedField.optional(1, "id", Types.LongType.get()))
    val processor = createProcessor(expectedSchema, fileSchema)
    assert(processor.displayActionPlan() ==
      """ProcessStruct
        |  variant_value (input[1]):
        |    PassThrough
        |  added (generated):
        |    FillNull(bigint)
        |  id (input[0]):
        |    PassThrough""".stripMargin)

    // Use Spark's encoder as the byte oracle, independently of the GPU post-processor.
    // None is SQL null; parseJson("null") is a non-null Variant containing JSON null.
    val variants = Seq(
      Some(VariantBuilder.parseJson("""{"a": 1}""", false)),
      None,
      Some(VariantBuilder.parseJson("null", false)),
      Some(VariantBuilder.parseJson("""{"name": "spark", "values": [2, 3]}""", false)))
    val ids = Array(10L, 20L, 30L, 40L)
    val rowCount = variants.size
    val byteListType = new HostColumnVector.ListType(
      true, new HostColumnVector.BasicType(false, DType.UINT8))
    val hostType = new HostColumnVector.StructType(true, byteListType, byteListType)
    val variantRows = variants.map(_.map { variant =>
      new HostColumnVector.StructData(
        Arrays.asList(variant.getValue.map(Byte.box): _*),
        Arrays.asList(variant.getMetadata.map(Byte.box): _*))
    }.orNull)
    val idColumn = closeOnExcept(CudfColumnVector.fromLongs(ids: _*)) { column =>
      GpuColumnVector.from(column, LongType)
    }
    val inputBatch = closeOnExcept(idColumn) { id =>
      val variantColumn = closeOnExcept(CudfColumnVector.fromStructs(
          hostType, variantRows: _*)) { column =>
        GpuColumnVector.from(column, VariantType)
      }
      closeOnExcept(variantColumn) { variant =>
        new ColumnarBatch(Array(id, variant), rowCount)
      }
    }
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, ACTIVE_ON_DECK_PRIORITY)
    }

    withResource(spillable) { _ =>
      withResource(processor.process(spillable.getColumnarBatch())) { outputBatch =>
        assert(outputBatch.numRows() == rowCount)
        assert(outputBatch.numCols() == 3)
        assert(outputBatch.column(0).dataType() == VariantType)
        assert(outputBatch.column(1).dataType() == LongType)
        assert(outputBatch.column(2).dataType() == LongType)
        withResource(outputBatch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
          val variantColumn = host.getBase
          assert(variantColumn.getNumChildren == 2)
          assert(variantColumn.getNullCount == 1)
          val values = variantColumn.getChildColumnView(0)
          val metadata = variantColumn.getChildColumnView(1)
          variants.zipWithIndex.foreach { case (expected, row) =>
            assert(variantColumn.isNull(row) == expected.isEmpty)
            expected.foreach { variant =>
              assert(!values.isNull(row))
              assert(!metadata.isNull(row))
              assert(values.getBytesFromList(row).toSeq == variant.getValue.toSeq)
              assert(metadata.getBytesFromList(row).toSeq == variant.getMetadata.toSeq)
            }
          }
        }
        withResource(outputBatch.column(1).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
          assert(host.getBase.getNullCount == rowCount)
        }
        withResource(outputBatch.column(2).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
          ids.indices.foreach { row =>
            assert(!host.getBase.isNull(row))
            assert(host.getBase.getLong(row) == ids(row))
          }
        }
      }
    }
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

  test("missing optional Variant in existing list elements preserves structure and values") {
    val fileElementType = Types.StructType.of(
        Types.NestedField.optional(3, "score", Types.LongType.get()))
    val expectedElementType = Types.StructType.of(
        Types.NestedField.optional(3, "score", Types.LongType.get()),
        Types.NestedField.optional(4, "v", Types.VariantType.get()))
    val fileSchema = new Schema(
        Types.NestedField.optional(1, "items", Types.ListType.ofOptional(2, fileElementType)))
    val expectedSchema = new Schema(
        Types.NestedField.optional(1, "items", Types.ListType.ofOptional(2, expectedElementType)))
    val processor = createProcessor(expectedSchema, fileSchema)
    assert(processor.displayActionPlan().contains("ProcessList"))

    val elementType = StructType(Seq(StructField("score", LongType, nullable = true)))
    val inputType = ArrayType(elementType, containsNull = true)
    val expectedType = ArrayType(
        elementType.add("v", VariantType, nullable = true), containsNull = true)
    val hostType = new HostColumnVector.ListType(
        true, new HostColumnVector.StructType(
            true, new HostColumnVector.BasicType(true, DType.INT64)))
    val rows = Seq[JList[StructData]](
        null,
        Arrays.asList[StructData](),
        Arrays.asList[StructData](null),
        Arrays.asList(score(10L), null, score(null), score(20L)),
        Arrays.asList(score(30L), score(40L)))
    // Seven child rows in five batch rows catches using the parent count for FillNull.
    val offsets = Array(0, 0, 0, 1, 5, 7)
    val inputColumn = closeOnExcept(CudfColumnVector.fromLists(hostType, rows: _*)) { column =>
      GpuColumnVector.from(column, inputType)
    }
    val inputBatch = closeOnExcept(inputColumn) { column =>
      new ColumnarBatch(Array(column), rows.size)
    }
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, ACTIVE_ON_DECK_PRIORITY)
    }

    withResource(spillable) { _ =>
      withResource(processor.process(spillable.getColumnarBatch())) { outputBatch =>
        assert(outputBatch.numRows() == rows.size)
        assert(outputBatch.numCols() == 1)
        assert(outputBatch.column(0).dataType() == expectedType)
        withResource(outputBatch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
          val lists = host.getBase
          assert(lists.getNumChildren == 1)
          val elements = lists.getChildColumnView(0)
          assert(elements.getRowCount == offsets.last)
          assert(elements.getChildColumnView(1).getRowCount == offsets.last)
          assert(elements.getChildColumnView(1).getNullCount == offsets.last)
          rows.zipWithIndex.foreach { case (expected, row) =>
            assert(lists.isNull(row) == (expected == null))
            assert(lists.getStartListOffset(row) == offsets(row))
            assert(lists.getEndListOffset(row) == offsets(row + 1))
            if (expected != null) {
              (0 until expected.size()).foreach { index =>
                assertScoreAndNullVariant(elements, offsets(row) + index, expected.get(index))
              }
            }
          }
        }
      }
    }
  }

  test("missing optional Variant in existing map values preserves keys and structure") {
    val fileValueType = Types.StructType.of(
        Types.NestedField.optional(4, "score", Types.LongType.get()))
    val expectedValueType = Types.StructType.of(
        Types.NestedField.optional(4, "score", Types.LongType.get()),
        Types.NestedField.optional(5, "v", Types.VariantType.get()))
    val fileSchema = new Schema(
        Types.NestedField.optional(1, "entries", Types.MapType.ofOptional(
            2, 3, Types.StringType.get(), fileValueType)))
    val expectedSchema = new Schema(
        Types.NestedField.optional(1, "entries", Types.MapType.ofOptional(
            2, 3, Types.StringType.get(), expectedValueType)))
    val processor = createProcessor(expectedSchema, fileSchema)
    assert(processor.displayActionPlan().contains("ProcessMap"))

    val valueType = StructType(Seq(StructField("score", LongType, nullable = true)))
    val inputType = MapType(StringType, valueType, valueContainsNull = true)
    val expectedType = MapType(
        StringType, valueType.add("v", VariantType, nullable = true), valueContainsNull = true)
    val hostValueType = new HostColumnVector.StructType(
        true, new HostColumnVector.BasicType(true, DType.INT64))
    val hostType = new HostColumnVector.ListType(
        true, new HostColumnVector.StructType(
            false, new HostColumnVector.BasicType(false, DType.STRING), hostValueType))
    def entry(key: String, value: StructData): StructData = new StructData(key, value)
    val rows = Seq[JList[StructData]](
        null,
        Arrays.asList[StructData](),
        Arrays.asList(entry("only-null", null)),
        Arrays.asList(
            entry("first", score(10L)), entry("null-value", null),
            entry("null-score", score(null)), entry("last", score(20L))),
        Arrays.asList(entry("third", score(30L)), entry("fourth", score(40L))))
    val offsets = Array(0, 0, 0, 1, 5, 7)
    val inputColumn = closeOnExcept(CudfColumnVector.fromLists(hostType, rows: _*)) { column =>
      GpuColumnVector.from(column, inputType)
    }
    val inputBatch = closeOnExcept(inputColumn) { column =>
      new ColumnarBatch(Array(column), rows.size)
    }
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, ACTIVE_ON_DECK_PRIORITY)
    }

    withResource(spillable) { _ =>
      withResource(processor.process(spillable.getColumnarBatch())) { outputBatch =>
        assert(outputBatch.numRows() == rows.size)
        assert(outputBatch.numCols() == 1)
        assert(outputBatch.column(0).dataType() == expectedType)
        withResource(outputBatch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { host =>
          val maps = host.getBase
          assert(maps.getNumChildren == 1)
          val entries = maps.getChildColumnView(0)
          assert(entries.getNumChildren == 2)
          assert(entries.getRowCount == offsets.last)
          assert(entries.getNullCount == 0)
          val keys = entries.getChildColumnView(0)
          val values = entries.getChildColumnView(1)
          assert(keys.getRowCount == offsets.last)
          assert(values.getRowCount == offsets.last)
          assert(values.getChildColumnView(1).getRowCount == offsets.last)
          assert(values.getChildColumnView(1).getNullCount == offsets.last)
          rows.zipWithIndex.foreach { case (expected, row) =>
            assert(maps.isNull(row) == (expected == null))
            assert(maps.getStartListOffset(row) == offsets(row))
            assert(maps.getEndListOffset(row) == offsets(row + 1))
            if (expected != null) {
              (0 until expected.size()).foreach { index =>
                val childRow = offsets(row) + index
                val expectedEntry = expected.get(index)
                assert(!keys.isNull(childRow))
                assert(keys.getJavaString(childRow) == expectedEntry.getField(0))
                assertScoreAndNullVariant(
                    values, childRow, expectedEntry.getField(1).asInstanceOf[StructData])
              }
            }
          }
        }
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
