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
{"spark":["350"],"carryForward":"all"}
{"spark":["400","411"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark.source

import com.nvidia.spark.rapids.iceberg.IcebergDefaultTestUtils.fieldWithWriteDefault
import org.apache.iceberg.{Schema, TableProperties}
import org.apache.iceberg.types.Types
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types.StructType

class GpuSparkWriteSuite extends AnyFunSuite {

  for (value <- Seq(9, 23)) {
    test(s"append and row-level writer schemas preserve write default $value and field IDs") {
      val field = fieldWithWriteDefault(2, "value", Types.IntegerType.get(), Int.box(value),
        initialDefault = Some(Int.box(2)))
      val tableSchema = new Schema(Types.NestedField.optional(
        1, "payload", Types.StructType.of(field)))
      // Iceberg derives this from Spark's query schema and does not retain field defaults.
      val schema = new Schema(Types.NestedField.optional(
        1, "payload", Types.StructType.of(
          Types.NestedField.optional(2, "value", Types.IntegerType.get()))))
      val context = mock(classOf[GpuWriteContext])
      when(context.dataSchema).thenReturn(schema)

      val schemas = Seq(GpuSparkWrite.dataWriterSparkTypeFor(schema, tableSchema)) ++
        Seq(Command.UPDATE, Command.MERGE).map { command =>
          GpuSparkPositionDeltaWrite.dataWriterSparkTypeFor(command, context, tableSchema)
        }
      schemas.foreach { sparkSchema =>
        assert(sparkSchema("payload").metadata.getLong(FIELD_ID_METADATA_KEY) == 1L)
        val child = sparkSchema("payload").dataType.asInstanceOf[StructType]("value")
        assert(child.metadata.getLong(FIELD_ID_METADATA_KEY) == 2L)
        assert(child.getCurrentDefaultValue().contains(value.toString))
      }
      val deleteSchema = GpuSparkPositionDeltaWrite.dataWriterSparkTypeFor(
        Command.DELETE, context, tableSchema)
      assert(deleteSchema == GpuWriteContext.positionDeleteSparkType)
      assert(deleteSchema.fields.forall(_.getCurrentDefaultValue().isEmpty))
    }
  }

  test("translateIcebergWriteProperties: adds 'compression' when only Iceberg key is present") {
    val writeProps = Map(TableProperties.PARQUET_COMPRESSION -> "zstd")
    val result = GpuSparkWrite.translateIcebergWriteProperties(writeProps)
    assert(result("compression") === "zstd")
    assert(result(TableProperties.PARQUET_COMPRESSION) === "zstd")
  }

  test("translateIcebergWriteProperties: explicit 'compression' is preserved") {
    val writeProps = Map(
      TableProperties.PARQUET_COMPRESSION -> "zstd",
      "compression" -> "snappy")
    val result = GpuSparkWrite.translateIcebergWriteProperties(writeProps)
    assert(result("compression") === "snappy",
      "explicit 'compression' must not be overwritten by the Iceberg codec key")
  }

  test("translateIcebergWriteProperties: missing Iceberg key returns map unchanged") {
    val writeProps = Map("write.parquet.row-group-size-bytes" -> "134217728")
    val result = GpuSparkWrite.translateIcebergWriteProperties(writeProps)
    assert(result === writeProps)
    assert(!result.contains("compression"))
  }
}
