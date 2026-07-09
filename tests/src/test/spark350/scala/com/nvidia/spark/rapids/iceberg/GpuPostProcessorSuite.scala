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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.iceberg.parquet._
import com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded.unshade
import com.nvidia.spark.rapids.parquet.ParquetFileInfoWithBlockMeta
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{
  MessageType => ShadedMessageType,
  Type => ShadedType,
  Types => ShadedTypes
}
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.{
  PrimitiveTypeName => ShadedPrimitiveTypeName
}
import org.apache.iceberg.shaded.org.apache.parquet.schema.Type.{Repetition => ShadedRepetition}
import org.apache.iceberg.types.Types
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Unit tests for GpuParquetReaderPostProcessor to verify that the correct
 * ColumnAction tree is built based on schema comparison.
 */
class GpuPostProcessorSuite extends AnyFunSuite with BeforeAndAfterAll {

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

  private def createBlockMetaData(rowCount: Long): BlockMetaData = {
    val block = new BlockMetaData()
    block.setRowCount(rowCount)
    block
  }

  private def createParquetInfo(
      shadedSchema: ShadedMessageType,
      rowCount: Long = 10): (ParquetFileInfoWithBlockMeta, ShadedMessageType) = {
    val block = createBlockMetaData(rowCount)
    val info = ParquetFileInfoWithBlockMeta(
      filePath = new Path("/test/file.parquet"),
      blocks = Seq(block),
      partValues = InternalRow.empty,
      schema = unshade(shadedSchema),
      readSchema = StructType(Seq.empty),
      dateRebaseMode = null,
      timestampRebaseMode = null,
      hasInt96Timestamps = false,
      blocksFirstRowIndices = Seq(0L)
    )
    (info, shadedSchema)
  }

  /**
   * Build a `ParquetFileInfoWithBlockMeta` that simulates a split scan task whose blocks
   * begin somewhere in the middle of the file (i.e. `blocksFirstRowIndices` does not start
   * at 0). `firstFileGlobalRowIndex` is the file-global first-row index of the first block
   * in `blockRowCounts`; subsequent blocks are placed contiguously after it.
   */
  private def createMultiBlockParquetInfo(
      shadedSchema: ShadedMessageType,
      blockRowCounts: Seq[Long],
      firstFileGlobalRowIndex: Long): (ParquetFileInfoWithBlockMeta, ShadedMessageType) = {
    val blocks = blockRowCounts.map(createBlockMetaData)
    val firstRowIndices = blockRowCounts
      .scanLeft(firstFileGlobalRowIndex)(_ + _)
      .dropRight(1)
    val info = ParquetFileInfoWithBlockMeta(
      filePath = new Path("/test/file.parquet"),
      blocks = blocks,
      partValues = InternalRow.empty,
      schema = unshade(shadedSchema),
      readSchema = StructType(Seq.empty),
      dateRebaseMode = null,
      timestampRebaseMode = null,
      hasInt96Timestamps = false,
      blocksFirstRowIndices = firstRowIndices
    )
    (info, shadedSchema)
  }

  /**
   * Test 1: When file schema exactly matches expected schema, the entire batch passes through.
   * Tests primitive types, arrays, maps, and structs with exact type matching.
   */
  test("PassThrough when schemas match exactly") {
    // Use types that definitely map 1:1 between Parquet and Iceberg
    val longFieldId = 1
    val doubleFieldId = 2
    val boolFieldId = 3
    // Array type
    val arrayFieldId = 10
    val arrayElementId = 11
    // Map type  
    val mapFieldId = 20
    val mapKeyId = 21
    val mapValueId = 22
    // Struct type
    val structFieldId = 30
    val structField1Id = 31
    val structField2Id = 32

    // Build shaded parquet schema - use INT64, DOUBLE, BOOLEAN which map directly
    val longType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(longFieldId).named("long_col")
    val doubleType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(doubleFieldId).named("double_col")
    val boolType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.BOOLEAN, ShadedRepetition.OPTIONAL)
      .id(boolFieldId).named("bool_col")

    val arrayElementType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(arrayElementId).named("element")
    val arrayType = ShadedTypes.optionalList().element(arrayElementType)
      .id(arrayFieldId).named("array_col")

    val mapKeyType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.REQUIRED)
      .id(mapKeyId).named("key")
    val mapValueType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(mapValueId).named("value")
    val mapType = ShadedTypes.optionalMap().key(mapKeyType).value(mapValueType)
      .id(mapFieldId).named("map_col")

    val structField1 =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(structField1Id).named("x")
    val structField2 =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(structField2Id).named("y")
    val structType = ShadedTypes.optionalGroup().addField(structField1).addField(structField2)
      .id(structFieldId).named("struct_col")

    val parquetSchema = new ShadedMessageType("test",
      Seq(longType, doubleType, boolType, arrayType, mapType, structType).asJava)

    // Build expected Iceberg schema with exact same types
    val expectedSchema = new Schema(
      Types.NestedField.optional(longFieldId, "long_col", Types.LongType.get()),
      Types.NestedField.optional(doubleFieldId, "double_col", Types.DoubleType.get()),
      Types.NestedField.optional(boolFieldId, "bool_col", Types.BooleanType.get()),
      Types.NestedField.optional(arrayFieldId, "array_col",
        Types.ListType.ofOptional(arrayElementId, Types.LongType.get())),
      Types.NestedField.optional(mapFieldId, "map_col",
      Types.MapType.ofOptional(
        mapKeyId,
        mapValueId,
        Types.LongType.get(),
        Types.DoubleType.get())),
      Types.NestedField.optional(structFieldId, "struct_col",
        Types.StructType.of(
          Types.NestedField.optional(structField1Id, "x", Types.LongType.get()),
          Types.NestedField.optional(structField2Id, "y", Types.DoubleType.get())
        ))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    // When all types match exactly, the entire batch passes through
    assert(processor.displayActionPlan() == "PassThrough")
  }

  /**
   * Test 2: Comprehensive test with all kinds of operations:
   * - PassThrough columns for primitives AND complex types (struct, list, map)
   * - Type promotion (INT -> LONG, FLOAT -> DOUBLE, BINARY -> STRING)
   * - FetchConstant for partition columns
   * - FillNull for missing optional columns
   * - FetchFilePath for _file metadata column
   * - FetchRowPosition for _pos metadata column
   * - Nested type transformations with deep nesting:
   *   - array<struct> with field promotion
   *   - struct containing array with element promotion
   *   - map with key/value promotion
   *   - struct with mixed passthrough and promoted fields
   */
  test("All operations: passthrough, promotion, constants, metadata, deeply nested types") {
    // === Passthrough primitive columns (exact match, no transformation needed) ===
    val ptLongId = 1      // INT64 -> LONG (passthrough)
    val ptDoubleId = 2    // DOUBLE -> DOUBLE (passthrough)

    // === Passthrough complex columns (entire complex type matches exactly) ===
    // Passthrough list: array<INT64> -> array<LONG>
    val ptListId = 50
    val ptListElementId = 51
    // Passthrough map: map<INT64, DOUBLE> -> map<LONG, DOUBLE>
    val ptMapId = 60
    val ptMapKeyId = 61
    val ptMapValueId = 62
    // Passthrough struct: struct<x: INT64, y: DOUBLE> -> struct<x: LONG, y: DOUBLE>
    val ptStructId = 70
    val ptStructFieldXId = 71
    val ptStructFieldYId = 72

    // === Type promotion columns ===
    val promoIntId = 3    // INT32 -> LONG (promotion)
    val promoFloatId = 4  // FLOAT -> DOUBLE (promotion)
    val promoBinaryId = 5 // BINARY -> STRING (promotion)

    // === Generated columns ===
    val constColId = 6    // Missing, has constant value
    val missingColId = 7  // Missing, optional -> FillNull
    val filePathId = MetadataColumns.FILE_PATH.fieldId()
    val rowPosId = MetadataColumns.ROW_POSITION.fieldId()

    // === Deeply nested: array<struct<a: INT32->LONG, b: INT64 (passthrough)>> ===
    val arrayOfStructId = 100
    val arrayOfStructElementId = 101
    val aosFieldAId = 102  // INT32 -> LONG (promotion)
    val aosFieldBId = 103  // INT64 -> LONG (passthrough)

    // === Deeply nested: struct<x: array<INT32->LONG>, y: DOUBLE (passthrough)> ===
    val structWithArrayId = 200
    val swaFieldXId = 201
    val swaArrayElementId = 202  // INT32 -> LONG (promotion)
    val swaFieldYId = 203        // DOUBLE -> DOUBLE (passthrough)

    // === Map with key promotion, value passthrough ===
    val mapId = 300
    val mapKeyId = 301    // BINARY -> STRING (promotion)
    val mapValueId = 302  // DOUBLE -> DOUBLE (passthrough)

    // === Struct with mixed: one field passthrough, one field promotion ===
    val mixedStructId = 400
    val msFieldAId = 401  // INT64 -> LONG (passthrough)
    val msFieldBId = 402  // INT32 -> LONG (promotion)

    // Build shaded parquet schema
    val ptLongType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(ptLongId).named("pt_long")
    val ptDoubleType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(ptDoubleId).named("pt_double")

    // Passthrough list: array<INT64>
    val ptListElement =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(ptListElementId).named("element")
    val ptListType = ShadedTypes.optionalList().element(ptListElement)
      .id(ptListId).named("pt_list")

    // Passthrough map: map<INT64, DOUBLE>
    val ptMapKey =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.REQUIRED)
      .id(ptMapKeyId).named("key")
    val ptMapValue =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(ptMapValueId).named("value")
    val ptMapType = ShadedTypes.optionalMap().key(ptMapKey).value(ptMapValue)
      .id(ptMapId).named("pt_map")

    // Passthrough struct: struct<x: INT64, y: DOUBLE>
    val ptStructFieldX =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(ptStructFieldXId).named("x")
    val ptStructFieldY =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(ptStructFieldYId).named("y")
    val ptStructType = ShadedTypes.optionalGroup().addField(ptStructFieldX).addField(ptStructFieldY)
      .id(ptStructId).named("pt_struct")

    val promoIntType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT32, ShadedRepetition.OPTIONAL)
      .id(promoIntId).named("promo_int")
    val promoFloatType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.FLOAT, ShadedRepetition.OPTIONAL)
      .id(promoFloatId).named("promo_float")
    val promoBinaryType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.BINARY, ShadedRepetition.OPTIONAL)
      .id(promoBinaryId).named("promo_binary")

    // array<struct<a: INT32, b: INT64>>
    val aosFieldA = ShadedTypes.primitive(ShadedPrimitiveTypeName.INT32, ShadedRepetition.OPTIONAL)
      .id(aosFieldAId).named("a")
    val aosFieldB = ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(aosFieldBId).named("b")
    val aosStructElement = ShadedTypes.optionalGroup().addField(aosFieldA).addField(aosFieldB)
      .id(arrayOfStructElementId).named("element")
    val arrayOfStructType = ShadedTypes.optionalList().element(aosStructElement)
      .id(arrayOfStructId).named("array_of_struct")

    // struct<x: array<INT32>, y: DOUBLE>
    val swaArrayElement =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT32, ShadedRepetition.OPTIONAL)
      .id(swaArrayElementId).named("element")
    val swaFieldX = ShadedTypes.optionalList().element(swaArrayElement)
      .id(swaFieldXId).named("x")
    val swaFieldY = ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(swaFieldYId).named("y")
    val structWithArrayType = ShadedTypes.optionalGroup().addField(swaFieldX).addField(swaFieldY)
      .id(structWithArrayId).named("struct_with_array")

    // map<BINARY, DOUBLE>
    val mapKeyType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.BINARY, ShadedRepetition.REQUIRED)
      .id(mapKeyId).named("key")
    val mapValueType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(mapValueId).named("value")
    val mapType = ShadedTypes.optionalMap().key(mapKeyType).value(mapValueType)
      .id(mapId).named("map_col")

    // struct<a: INT64, b: INT32>
    val msFieldA = ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(msFieldAId).named("a")
    val msFieldB = ShadedTypes.primitive(ShadedPrimitiveTypeName.INT32, ShadedRepetition.OPTIONAL)
      .id(msFieldBId).named("b")
    val mixedStructType = ShadedTypes.optionalGroup().addField(msFieldA).addField(msFieldB)
      .id(mixedStructId).named("mixed_struct")

    val parquetSchema = new ShadedMessageType("test", Seq(
      ptLongType, ptDoubleType, ptListType, ptMapType, ptStructType,
      promoIntType, promoFloatType, promoBinaryType,
      arrayOfStructType, structWithArrayType, mapType, mixedStructType).asJava)

    // Build expected Iceberg schema
    val expectedSchema = new Schema(
      // Passthrough primitive columns
      Types.NestedField.optional(ptLongId, "pt_long", Types.LongType.get()),
      Types.NestedField.optional(ptDoubleId, "pt_double", Types.DoubleType.get()),
      // Passthrough complex columns (entire type matches exactly)
      Types.NestedField.optional(ptListId, "pt_list",
        Types.ListType.ofOptional(ptListElementId, Types.LongType.get())),
      Types.NestedField.optional(ptMapId, "pt_map",
        Types.MapType.ofOptional(ptMapKeyId, ptMapValueId,
          Types.LongType.get(), Types.DoubleType.get())),
      Types.NestedField.optional(ptStructId, "pt_struct",
        Types.StructType.of(
          Types.NestedField.optional(ptStructFieldXId, "x", Types.LongType.get()),
          Types.NestedField.optional(ptStructFieldYId, "y", Types.DoubleType.get())
        )),
      // Promotion columns
      Types.NestedField.optional(promoIntId, "promo_int", Types.LongType.get()),
      Types.NestedField.optional(promoFloatId, "promo_float", Types.DoubleType.get()),
      Types.NestedField.optional(promoBinaryId, "promo_binary", Types.StringType.get()),
      // Generated columns
      Types.NestedField.optional(constColId, "partition_col", Types.LongType.get()),
      Types.NestedField.optional(missingColId, "missing_col", Types.StringType.get()),
      Types.NestedField.optional(filePathId, "_file", Types.StringType.get()),
      Types.NestedField.optional(rowPosId, "_pos", Types.LongType.get()),
      // array<struct<a: LONG, b: LONG>> - element struct has mixed passthrough/promotion
      Types.NestedField.optional(arrayOfStructId, "array_of_struct",
        Types.ListType.ofOptional(arrayOfStructElementId,
          Types.StructType.of(
            Types.NestedField.optional(aosFieldAId, "a", Types.LongType.get()),
            Types.NestedField.optional(aosFieldBId, "b", Types.LongType.get())
          ))),
      // struct<x: array<LONG>, y: DOUBLE> - has nested list with promotion
      Types.NestedField.optional(structWithArrayId, "struct_with_array",
        Types.StructType.of(
          Types.NestedField.optional(swaFieldXId, "x",
            Types.ListType.ofOptional(swaArrayElementId, Types.LongType.get())),
          Types.NestedField.optional(swaFieldYId, "y", Types.DoubleType.get())
        )),
      // map<STRING, DOUBLE> - key promoted, value passthrough
      Types.NestedField.optional(mapId, "map_col",
        Types.MapType.ofOptional(mapKeyId, mapValueId,
          Types.StringType.get(), Types.DoubleType.get())),
      // struct<a: LONG (passthrough), b: LONG (promotion)>
      Types.NestedField.optional(mixedStructId, "mixed_struct",
        Types.StructType.of(
          Types.NestedField.optional(msFieldAId, "a", Types.LongType.get()),
          Types.NestedField.optional(msFieldBId, "b", Types.LongType.get())
        ))
    )

    val idToConstant = new JHashMap[Integer, Any]()
    idToConstant.put(constColId, 42L)

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      idToConstant,
      expectedSchema,
      shadedSchema,
      Map.empty)

    val expected =
      """ProcessStruct
        |  pt_long (input[0]):
        |    PassThrough
        |  pt_double (input[1]):
        |    PassThrough
        |  pt_list (input[2]):
        |    PassThrough
        |  pt_map (input[3]):
        |    PassThrough
        |  pt_struct (input[4]):
        |    PassThrough
        |  promo_int (input[5]):
        |    UpCast(int -> bigint)
        |  promo_float (input[6]):
        |    UpCast(float -> double)
        |  promo_binary (input[7]):
        |    UpCast(binary -> string)
        |  partition_col (generated):
        |    FetchConstant(fieldId=6, bigint)
        |  missing_col (generated):
        |    FillNull(string)
        |  _file (generated):
        |    FetchFilePath
        |  _pos (generated):
        |    FetchRowPosition
        |  array_of_struct (input[8]):
        |    ProcessList
        |      element:
        |        ProcessStruct
        |          field[0] (input[0]):
        |            UpCast(int -> bigint)
        |          field[1] (input[1]):
        |            PassThrough
        |  struct_with_array (input[9]):
        |    ProcessStruct
        |      field[0] (input[0]):
        |        ProcessList
        |          element:
        |            UpCast(int -> bigint)
        |      field[1] (input[1]):
        |        PassThrough
        |  map_col (input[10]):
        |    ProcessMap
        |      key:
        |        UpCast(binary -> string)
        |      value:
        |        PassThrough
        |  mixed_struct (input[11]):
        |    ProcessStruct
        |      field[0] (input[0]):
        |        PassThrough
        |      field[1] (input[1]):
        |        UpCast(int -> bigint)""".stripMargin

    assert(processor.displayActionPlan() == expected)
  }

  /**
   * Test 3: Error when required field is missing from file schema.
   */
  test("Throws exception for missing required field") {
    val col1Id = 1
    val col2Id = 2  // Required but missing from file

    val col1Type: ShadedType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(col1Id).named("col1")
    val parquetSchema = new ShadedMessageType("test", Seq(col1Type).asJava)

    val expectedSchema = new Schema(
      Types.NestedField.optional(col1Id, "col1", Types.LongType.get()),
      Types.NestedField.required(col2Id, "required_col", Types.StringType.get())
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)
    val ex = intercept[IllegalArgumentException] {
      // The action tree is built lazily, so missing required fields surface when it is accessed.
      processor.displayActionPlan()
    }
    assert(ex.getMessage.contains("Missing required field"))
  }

  test("Throws exception for missing required list field") {
    val listFieldId = 1
    val listElementId = 2

    val parquetSchema = new ShadedMessageType("test", Seq.empty[ShadedType].asJava)
    val expectedSchema = new Schema(
      Types.NestedField.required(listFieldId, "required_list",
        Types.ListType.ofOptional(listElementId, Types.LongType.get()))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)
    val ex = intercept[IllegalArgumentException] {
      processor.displayActionPlan()
    }
    assert(ex.getMessage.contains("Missing required field"))
  }

  test("Throws exception for missing required map field") {
    val mapFieldId = 1
    val mapKeyId = 2
    val mapValueId = 3

    val parquetSchema = new ShadedMessageType("test", Seq.empty[ShadedType].asJava)
    val expectedSchema = new Schema(
      Types.NestedField.required(mapFieldId, "required_map",
        Types.MapType.ofOptional(
          mapKeyId,
          mapValueId,
          Types.LongType.get(),
          Types.StringType.get()))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)
    val ex = intercept[IllegalArgumentException] {
      processor.displayActionPlan()
    }
    assert(ex.getMessage.contains("Missing required field"))
  }

  /**
   * Test 4: Process a columnar batch with array type (passthrough, no promotion needed)
   */
  test("Process columnar batch with array<long> passthrough") {
    import com.nvidia.spark.rapids.SpillableColumnarBatch
    import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
    import com.nvidia.spark.rapids.SpillPriorities
    import org.apache.spark.sql.types._

    val arrayFieldId = 1
    val arrayElementId = 2

    // Build shaded parquet schema: array<INT64>
    val arrayElementType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(arrayElementId).named("element")
    val arrayType = ShadedTypes.optionalList().element(arrayElementType)
      .id(arrayFieldId).named("array_col")

    val parquetSchema = new ShadedMessageType("test", Seq[ShadedType](arrayType).asJava)

    // Expected schema: array<LONG> (exact match, passthrough)
    val expectedSchema = new Schema(
      Types.NestedField.optional(arrayFieldId, "array_col",
        Types.ListType.ofOptional(arrayElementId, Types.LongType.get()))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema, rowCount = 10)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    // Verify action plan is PassThrough (no transformation needed)
    assert(processor.displayActionPlan() == "PassThrough")

    // Create a columnar batch with array<long> data using FuzzerUtils
    import com.nvidia.spark.rapids.FuzzerUtils
    val schema =
      StructType(Array(StructField("array_col", ArrayType(LongType, true), true)))
    val inputBatch = FuzzerUtils.createColumnarBatch(schema, rowCount = 3, seed = 42)
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }

    // Process the batch - it should pass through without errors
    withResource(spillable) { _ =>
      withResource(processor.process(spillable.getColumnarBatch())) { outputBatch =>
        // Verify basic properties
        assert(outputBatch.numRows() == 3)
        assert(outputBatch.numCols() == 1)
      }
    }
  }

  /**
   * Test 5: Process a columnar batch with struct type (passthrough)
   */
  test("Process columnar batch with struct<long, double> passthrough") {
    import com.nvidia.spark.rapids.SpillableColumnarBatch
    import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
    import com.nvidia.spark.rapids.SpillPriorities
    import org.apache.spark.sql.types._

    val structFieldId = 1
    val fieldAId = 2
    val fieldBId = 3

    // Build shaded parquet schema: struct<a: INT64, b: DOUBLE>
    val fieldA =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(fieldAId).named("a")
    val fieldB =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(fieldBId).named("b")
    val structType =
      ShadedTypes.optionalGroup().addField(fieldA).addField(fieldB)
      .id(structFieldId).named("struct_col")

    val parquetSchema = new ShadedMessageType("test", Seq[ShadedType](structType).asJava)

    // Expected schema: struct<a: LONG, b: DOUBLE> (exact match, passthrough)
    val expectedSchema = new Schema(
      Types.NestedField.optional(structFieldId, "struct_col",
        Types.StructType.of(
          Types.NestedField.optional(fieldAId, "a", Types.LongType.get()),
          Types.NestedField.optional(fieldBId, "b", Types.DoubleType.get())
        ))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema, rowCount = 10)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    // Verify action plan is PassThrough
    assert(processor.displayActionPlan() == "PassThrough")

    // Create a columnar batch with struct<long, double> data using FuzzerUtils
    import com.nvidia.spark.rapids.FuzzerUtils
    val schema = StructType(Array(StructField(
      "struct_col",
      StructType(Seq(
        StructField("a", LongType, true),
        StructField("b", DoubleType, true)
      )),
      true)))
    val inputBatch = FuzzerUtils.createColumnarBatch(schema, rowCount = 2, seed = 42)
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }

    // Process the batch - it should pass through without errors
    withResource(spillable) { _ =>
      withResource(processor.process(spillable.getColumnarBatch())) { outputBatch =>
        // Verify basic properties
        assert(outputBatch.numRows() == 2)
        assert(outputBatch.numCols() == 1)
      }
    }
  }

  /**
   * Test 6: Process a columnar batch with map type (passthrough)
   */
  test("Process columnar batch with map<long, double> passthrough") {
    import com.nvidia.spark.rapids.SpillableColumnarBatch
    import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
    import com.nvidia.spark.rapids.SpillPriorities
    import org.apache.spark.sql.types._

    val mapFieldId = 1
    val mapKeyId = 2
    val mapValueId = 3

    // Build shaded parquet schema: map<INT64, DOUBLE>
    val mapKeyType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.REQUIRED)
      .id(mapKeyId).named("key")
    val mapValueType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(mapValueId).named("value")
    val mapType = ShadedTypes.optionalMap().key(mapKeyType).value(mapValueType)
      .id(mapFieldId).named("map_col")

    val parquetSchema = new ShadedMessageType("test", Seq[ShadedType](mapType).asJava)

    // Expected schema: map<LONG, DOUBLE> (exact match, passthrough)
    val expectedSchema = new Schema(
      Types.NestedField.optional(mapFieldId, "map_col",
        Types.MapType.ofOptional(mapKeyId, mapValueId,
          Types.LongType.get(), Types.DoubleType.get()))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema, rowCount = 10)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    // Verify action plan is PassThrough
    assert(processor.displayActionPlan() == "PassThrough")

    // Create a columnar batch with map<long, double> data using FuzzerUtils
    import com.nvidia.spark.rapids.FuzzerUtils
    val schema =
      StructType(Array(StructField("map_col", MapType(LongType, DoubleType, true), true)))
    val inputBatch = FuzzerUtils.createColumnarBatch(schema, rowCount = 2, seed = 42)
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }

    // Process the batch - it should pass through without errors
    withResource(spillable) { _ =>
      withResource(processor.process(spillable.getColumnarBatch())) { outputBatch =>
        // Verify basic properties
        assert(outputBatch.numRows() == 2)
        assert(outputBatch.numCols() == 1)
      }
    }
  }

  /**
   * Test 7: Process struct with type promotion (INT32 -> INT64 on a field)
   * This exercises the ProcessStruct code path with non-passthrough transformations
   */
  test("Process columnar batch with struct<int, double> promoted to struct<long, double>") {
    import com.nvidia.spark.rapids.SpillableColumnarBatch
    import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
    import com.nvidia.spark.rapids.GpuColumnVector
    import com.nvidia.spark.rapids.SpillPriorities
    import ai.rapids.cudf.{ColumnVector => CudfColumnVector, DType, HostColumnVector}
    import org.apache.spark.sql.types._

    val structFieldId = 1
    val fieldAId = 2
    val fieldBId = 3

    // Build shaded parquet schema: struct<a: INT32, b: DOUBLE>
    val fieldA = ShadedTypes.primitive(ShadedPrimitiveTypeName.INT32, ShadedRepetition.OPTIONAL)
      .id(fieldAId).named("a")
    val fieldB = ShadedTypes.primitive(ShadedPrimitiveTypeName.DOUBLE, ShadedRepetition.OPTIONAL)
      .id(fieldBId).named("b")
    val structType = ShadedTypes.optionalGroup().addField(fieldA).addField(fieldB)
      .id(structFieldId).named("struct_col")

    val parquetSchema = new ShadedMessageType("test", Seq[ShadedType](structType).asJava)

    // Expected schema: struct<a: LONG, b: DOUBLE> (promotion needed for field a)
    val expectedSchema = new Schema(
      Types.NestedField.optional(structFieldId, "struct_col",
        Types.StructType.of(
          Types.NestedField.optional(fieldAId, "a", Types.LongType.get()),
          Types.NestedField.optional(fieldBId, "b", Types.DoubleType.get())
        ))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema, rowCount = 10)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    // Verify action plan involves ProcessStruct with UpCast for field a
    val actionPlan = processor.displayActionPlan()
    assert(actionPlan.contains("ProcessStruct"))
    assert(actionPlan.contains("UpCast"))

    val sparkStructType = StructType(Seq(
      StructField("a", IntegerType, true),
      StructField("b", DoubleType, true)
    ))
    def struct(vals: Object*): HostColumnVector.StructData =
      new HostColumnVector.StructData(vals: _*)
    val hostStructType = new HostColumnVector.StructType(
      true,
      new HostColumnVector.BasicType(true, DType.INT32),
      new HostColumnVector.BasicType(true, DType.FLOAT64))
    val inputCol = closeOnExcept(CudfColumnVector.fromStructs(
      hostStructType,
      struct(Integer.valueOf(1), Double.box(1.25)),
      null,
      struct(Integer.valueOf(3), Double.box(3.5)))) { cudfCol =>
      GpuColumnVector.from(cudfCol, sparkStructType)
    }
    val inputBatch = new org.apache.spark.sql.vectorized.ColumnarBatch(Array(inputCol), 3)
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }

    // Process the batch - this should exercise ProcessStruct with transformation
    withResource(spillable) { _ =>
      withResource(processor.process(spillable.getColumnarBatch())) { outputBatch =>
        // Verify basic properties
        assert(outputBatch.numRows() == 3)
        assert(outputBatch.numCols() == 1)

        val gpuCol = outputBatch.column(0).asInstanceOf[GpuColumnVector]
        withResource(gpuCol.copyToHost()) { hostCol =>
          assert(!hostCol.getBase.isNull(0))
          assert(hostCol.getBase.isNull(1))
          assert(!hostCol.getBase.isNull(2))
        }

        val cudfCol = gpuCol.getBase
        assert(cudfCol.getNumChildren == 2)
        withResource(cudfCol.getChildColumnView(0)) { child =>
          assert(child.getType.equals(DType.INT64))
        }
        withResource(cudfCol.getChildColumnView(1)) { child =>
          assert(child.getType.equals(DType.FLOAT64))
        }
      }
    }
  }

  test("Missing optional struct is filled with null parent values") {
    import com.nvidia.spark.rapids.Arm.withResource
    import com.nvidia.spark.rapids.GpuColumnVector

    val structFieldId = 1
    val fieldAId = 2
    val fieldBId = 3

    val parquetSchema = new ShadedMessageType("test", Seq.empty[ShadedType].asJava)
    val expectedSchema = new Schema(
      Types.NestedField.optional(structFieldId, "struct_col",
        Types.StructType.of(
          Types.NestedField.optional(fieldAId, "a", Types.LongType.get()),
          Types.NestedField.optional(fieldBId, "b", Types.StringType.get())
        ))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    assert(processor.displayActionPlan().contains("FillNull(struct<"))

    withResource(processor.process(
      new org.apache.spark.sql.vectorized.ColumnarBatch(
        Array.empty[org.apache.spark.sql.vectorized.ColumnVector],
        3))) { outputBatch =>
      assert(outputBatch.numRows() == 3)
      assert(outputBatch.numCols() == 1)

      withResource(outputBatch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
        (0 until outputBatch.numRows()).foreach { i =>
          assert(hostCol.getBase.isNull(i))
        }
      }
    }
  }

  test("Whole-struct constants build a FetchConstant action") {
    val structFieldId = 1
    val fieldAId = 2
    val fieldBId = 3

    val parquetSchema = new ShadedMessageType("test", Seq.empty[ShadedType].asJava)
    val expectedSchema = new Schema(
      Types.NestedField.optional(structFieldId, "struct_col",
        Types.StructType.of(
          Types.NestedField.optional(fieldAId, "a", Types.LongType.get()),
          Types.NestedField.optional(fieldBId, "b", Types.StringType.get())
        ))
    )

    val idToConstant = new JHashMap[Integer, Any]()
    idToConstant.put(structFieldId, null)

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      idToConstant,
      expectedSchema,
      shadedSchema,
      Map.empty)

    assert(processor.displayActionPlan().contains(
      s"FetchConstant(fieldId=$structFieldId, struct<"))
  }

  test("FetchRowPosition emits file-global _pos across multiple blocks") {
    import com.nvidia.spark.rapids.Arm.withResource
    import com.nvidia.spark.rapids.GpuColumnVector
    import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkColumnVector}

    val rowPosId = MetadataColumns.ROW_POSITION.fieldId()

    // Empty parquet schema: _pos is a generated metadata column, no input column is read
    // from parquet for it.
    val parquetSchema = new ShadedMessageType("test", Seq.empty[ShadedType].asJava)

    val expectedSchema = new Schema(
      Types.NestedField.optional(rowPosId, "_pos", Types.LongType.get())
    )

    // Simulate a split task whose first block starts at file-global row 500 (this is the
    // scenario the reader.scala fix exists for; before that fix the task-local first-row
    // index would be 0). Two blocks of 500 + 400 rows: file-global rows [500, 999] and
    // [1000, 1399].
    val (parquetInfo, shadedSchema) =
      createMultiBlockParquetInfo(parquetSchema, Seq(500L, 400L), firstFileGlobalRowIndex = 500L)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    def emptyBatch(rows: Int): ColumnarBatch =
      new ColumnarBatch(Array.empty[SparkColumnVector], rows)

    // Check every row, not just first/last — a middle-row off-by-one would otherwise slip
    // past, and the cross-block batch below relies on knowing the exact transition row.
    def assertPosRange(batch: ColumnarBatch, expectedStart: Long, expectedEnd: Long): Unit = {
      assert(batch.numRows() == (expectedEnd - expectedStart + 1).toInt)
      assert(batch.numCols() == 1)
      withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
        val base = hostCol.getBase
        var i = 0
        while (i < batch.numRows()) {
          val actual = base.getLong(i)
          val expected = expectedStart + i
          assert(actual == expected,
            s"_pos[$i] expected $expected, got $actual")
          i += 1
        }
      }
    }

    // Batch 1: 300 rows entirely inside block 0 → file-global _pos = 500..799.
    withResource(processor.process(emptyBatch(300))) { batch =>
      assertPosRange(batch, 500L, 799L)
    }

    // Batch 2: 100 rows still inside block 0 (100 rows of block 0 remain after this batch).
    // → file-global _pos = 800..899.
    withResource(processor.process(emptyBatch(100))) { batch =>
      assertPosRange(batch, 800L, 899L)
    }

    // Batch 3: 350 rows straddle the block 0 → block 1 boundary (100 from block 0 + 250 from
    // block 1). This exercises the `if (curRowPos >= curBlockRowEnd)` branch inside the
    // per-row loop in FetchRowPosition.execute — a single process() call that crosses
    // blocks. → file-global _pos = 900..1249.
    withResource(processor.process(emptyBatch(350))) { batch =>
      assertPosRange(batch, 900L, 1249L)
    }

    // Batch 4: 150 rows entirely inside block 1 → file-global _pos = 1250..1399.
    withResource(processor.process(emptyBatch(150))) { batch =>
      assertPosRange(batch, 1250L, 1399L)
    }
  }

  test("Constant struct with required children does not throw") {
    val structFieldId = 1
    val fieldAId = 2
    val fieldBId = 3

    val parquetSchema = new ShadedMessageType("test",
      Seq.empty[ShadedType].asJava)
    val expectedSchema = new Schema(
      Types.NestedField.optional(structFieldId, "partition_struct",
        Types.StructType.of(
          Types.NestedField.required(fieldAId, "a",
            Types.IntegerType.get()),
          Types.NestedField.required(fieldBId, "b",
            Types.StringType.get())
        ))
    )

    val idToConstant = new JHashMap[Integer, Any]()
    idToConstant.put(structFieldId, null)

    val (parquetInfo, shadedSchema) =
      createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      idToConstant,
      expectedSchema,
      shadedSchema,
      Map.empty)

    val plan = processor.displayActionPlan()
    assert(plan.contains(
      s"FetchConstant(fieldId=$structFieldId, struct<"))
  }

  // Regression: ALTER TABLE ADD COLUMN of an optional nested MAP must not
  // throw when reading old data files that pre-date the ADD. Iceberg map
  // keys are intrinsically required, so the post-order visitor used to call
  // MissingFieldActionBuilder for the key with isOptional=false and throw
  // "Missing required field" before the missing-container's own map() handler
  // could emit FillNull.
  test("Missing nested optional map with required key does not throw") {
    val infoStructId = 1
    val scoreFieldId = 2
    val propsMapId = 3  // newly-added MAP, missing from file
    val propsKeyId = 4
    val propsValueId = 5

    // Parquet file: info STRUCT<score: BIGINT> — no props
    val scoreType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(scoreFieldId).named("score")
    val infoStruct = ShadedTypes.optionalGroup().addField(scoreType)
      .id(infoStructId).named("info")
    val parquetSchema = new ShadedMessageType("test",
      Seq[ShadedType](infoStruct).asJava)

    // Expected: info STRUCT<score, props: MAP<STRING, BIGINT>>
    val expectedSchema = new Schema(
      Types.NestedField.optional(infoStructId, "info",
        Types.StructType.of(
          Types.NestedField.optional(scoreFieldId, "score", Types.LongType.get()),
          Types.NestedField.optional(propsMapId, "props",
            Types.MapType.ofOptional(propsKeyId, propsValueId,
              Types.StringType.get(), Types.LongType.get()))
        ))
    )

    val rowCount = 3
    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema, rowCount.toLong)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    val plan = processor.displayActionPlan()
    // The missing map itself must emit FillNull (it's the only valid behavior
    // for an optional missing field). Children are discarded by the parent.
    assert(plan.contains("FillNull(map<"),
      s"expected FillNull for missing map in plan:\n$plan")

    // Exercise process() too: action-tree construction succeeding is not the
    // same guarantee as execute() succeeding for FillNull(map<...>).
    import com.nvidia.spark.rapids.{FuzzerUtils, GpuColumnVector, SpillableColumnarBatch}
    import com.nvidia.spark.rapids.SpillPriorities
    import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
    import org.apache.spark.sql.types.{LongType, StructField, StructType => SparkStructType}

    val inputSparkSchema = SparkStructType(Array(StructField(
      "info",
      SparkStructType(Seq(StructField("score", LongType, true))),
      true)))
    val inputBatch = FuzzerUtils.createColumnarBatch(inputSparkSchema, rowCount, seed = 42)
    val spillable = closeOnExcept(inputBatch) { batch =>
      SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }
    withResource(spillable) { _ =>
      withResource(processor.process(spillable.getColumnarBatch())) { outputBatch =>
        assert(outputBatch.numRows() == rowCount)
        assert(outputBatch.numCols() == 1)
        val infoCol = outputBatch.column(0).asInstanceOf[GpuColumnVector].getBase
        // info struct gains props as a second child.
        assert(infoCol.getNumChildren == 2,
          s"expected info struct to have 2 children, got ${infoCol.getNumChildren}")
        withResource(infoCol.getChildColumnView(1)) { propsCol =>
          assert(propsCol.getNullCount == rowCount,
            s"props should be all-null; nullCount=${propsCol.getNullCount} rowCount=$rowCount")
        }
      }
    }
  }

  // Same defect, with LIST<required element> as the missing container.
  test("Missing nested optional list with required element does not throw") {
    val outerStructId = 1
    val keepFieldId = 2
    val newListId = 3   // newly-added list, missing from file
    val newListElementId = 4

    val keepType =
      ShadedTypes.primitive(ShadedPrimitiveTypeName.INT64, ShadedRepetition.OPTIONAL)
      .id(keepFieldId).named("keep")
    val outerStruct = ShadedTypes.optionalGroup().addField(keepType)
      .id(outerStructId).named("outer")
    val parquetSchema = new ShadedMessageType("test",
      Seq[ShadedType](outerStruct).asJava)

    val expectedSchema = new Schema(
      Types.NestedField.optional(outerStructId, "outer",
        Types.StructType.of(
          Types.NestedField.optional(keepFieldId, "keep", Types.LongType.get()),
          // List with a REQUIRED element — the bug also fires here because the
          // required element primitive is visited before the parent list().
          Types.NestedField.optional(newListId, "new_list",
            Types.ListType.ofRequired(newListElementId, Types.LongType.get()))
        ))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    val plan = processor.displayActionPlan()
    assert(plan.contains("FillNull(array<"),
      s"expected FillNull for missing list in plan:\n$plan")
  }

  // Doubly-nested: a missing struct that itself contains a missing map with
  // a required key. Exercises strict-ancestor isInsideMissingContainer when
  // multiple levels above also have partner == null.
  test("Doubly nested missing container with required descendant does not throw") {
    val metaStructId = 1   // missing struct
    val propsMapId = 2     // missing map (inside metaStruct)
    val propsKeyId = 3
    val propsValueId = 4

    val parquetSchema = new ShadedMessageType("test",
      Seq.empty[ShadedType].asJava)
    val expectedSchema = new Schema(
      Types.NestedField.optional(metaStructId, "meta",
        Types.StructType.of(
          Types.NestedField.optional(propsMapId, "props",
            Types.MapType.ofOptional(propsKeyId, propsValueId,
              Types.StringType.get(), Types.LongType.get()))
        ))
    )

    val (parquetInfo, shadedSchema) = createParquetInfo(parquetSchema)
    val processor = new GpuParquetReaderPostProcessor(
      parquetInfo,
      new JHashMap[Integer, Any](),
      expectedSchema,
      shadedSchema,
      Map.empty)

    val plan = processor.displayActionPlan()
    // Outer struct is missing; its child map is also missing — only the
    // outer FillNull(struct<...>) is composed into the plan.
    assert(plan.contains("FillNull(struct<"),
      s"expected FillNull for outer missing struct in plan:\n$plan")
  }

}
