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

import java.nio.ByteBuffer
import java.time.LocalDate

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.SparkQueryCompareTestSuite
import com.nvidia.spark.rapids.iceberg.IcebergDefaultTestUtils.fieldWithWriteDefault
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{Schema, TableProperties}
import org.apache.iceberg.types.{Type, Types}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.mockito.Mockito.{mock, when}

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.types.{
  BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType,
  IntegerType, LongType, StructField, StructType, TimestampType}

class GpuSparkWriteSuite extends SparkQueryCompareTestSuite {

  for (value <- Seq(None, Some(9), Some(23))) {
    test(s"data writers preserve the write schema's default $value and nested field IDs") {
      val intType = Types.IntegerType.get()
      val field = value.map(v => fieldWithWriteDefault(2, "value", intType, Int.box(v),
        initialDefault = Some(Int.box(2)))).getOrElse(
        Types.NestedField.optional(2, "value", intType))
      val schema = new Schema(Types.NestedField.optional(1, "payload", Types.StructType.of(field)))
      val context = mock(classOf[GpuWriteContext])
      when(context.dataSchema).thenReturn(schema)

      val schemas = Seq(GpuSparkWrite.dataWriterSparkTypeFor(schema)) ++
        Seq(Command.UPDATE, Command.MERGE).map { command =>
          GpuSparkPositionDeltaWrite.dataWriterSparkTypeFor(command, context)
        }
      schemas.foreach { sparkSchema =>
        assert(sparkSchema("payload").metadata.getLong(FIELD_ID_METADATA_KEY) == 1L)
        val child = sparkSchema("payload").dataType.asInstanceOf[StructType]("value")
        assert(child.metadata.getLong(FIELD_ID_METADATA_KEY) == 2L)
        assert(child.getCurrentDefaultValue() == value.map(_.toString))
        assert(child.metadata.contains("EXISTS_DEFAULT") == value.isDefined)
        value.foreach(_ => assert(child.metadata.getString("EXISTS_DEFAULT") == "2"))
      }
    }
  }

  test("the GPU Parquet writer persists supported defaults and nested field IDs") {
    case class DefaultCase(
        name: String,
        icebergType: Type,
        sparkType: DataType,
        initial: AnyRef,
        current: AnyRef,
        valueSql: String,
        initialSql: String,
        currentSql: String)

    val cases = Seq(
      DefaultCase("int_value", Types.IntegerType.get(), IntegerType, Int.box(2), Int.box(23),
        "17", "2", "23"),
      DefaultCase("long_value", Types.LongType.get(), LongType,
        Long.box(5000000000L), Long.box(6000000000L),
        "7000000000L", "5000000000L", "6000000000L"),
      DefaultCase("decimal_value", Types.DecimalType.of(9, 2), DecimalType(9, 2),
        new java.math.BigDecimal("-12.34"), new java.math.BigDecimal("23.45"),
        "CAST(34.56 AS DECIMAL(9,2))", "CAST(-12.34 AS DECIMAL(9,2))",
        "CAST(23.45 AS DECIMAL(9,2))"),
      DefaultCase("date_value", Types.DateType.get(), DateType,
        Int.box(LocalDate.of(2024, 1, 2).toEpochDay.toInt),
        Int.box(LocalDate.of(2024, 2, 3).toEpochDay.toInt),
        "DATE '2024-03-04'", "DATE '2024-01-02'", "DATE '2024-02-03'"),
      DefaultCase("timestamp_value", Types.TimestampType.withZone(), TimestampType,
        "2024-11-03T01:30:00.123456-04:00", "2024-11-03T01:30:00.123456-05:00",
        "TIMESTAMP '2024-11-04T06:30:00.123456Z'",
        "TIMESTAMP '2024-11-03T01:30:00.123456-04:00'",
        "TIMESTAMP '2024-11-03T01:30:00.123456-05:00'"),
      DefaultCase("boolean_value", Types.BooleanType.get(), BooleanType,
        Boolean.box(true), Boolean.box(false), "TRUE", "TRUE", "FALSE"),
      DefaultCase("float_value", Types.FloatType.get(), FloatType,
        Float.box(1.25f), Float.box(2.5f), "CAST(3.75 AS FLOAT)",
        "CAST(1.25 AS FLOAT)", "CAST(2.5 AS FLOAT)"),
      DefaultCase("double_value", Types.DoubleType.get(), DoubleType,
        Double.box(-2.5), Double.box(5.25), "CAST(7.5 AS DOUBLE)",
        "CAST(-2.5 AS DOUBLE)", "CAST(5.25 AS DOUBLE)"),
      DefaultCase("binary_value", Types.BinaryType.get(), BinaryType,
        ByteBuffer.wrap(Array[Byte](1, 2)), ByteBuffer.wrap(Array[Byte](3, 4)),
        "X'0506'", "X'0102'", "X'0304'"))
    val nested = DefaultCase("nested_value", Types.IntegerType.get(), IntegerType,
      Int.box(11), Int.box(29), "41", "11", "29")
    val fields = cases.zipWithIndex.map { case (fixture, index) =>
      fieldWithWriteDefault(index + 1, fixture.name, fixture.icebergType, fixture.current,
        initialDefault = Some(fixture.initial))
    }
    val child = fieldWithWriteDefault(31, nested.name, nested.icebergType, nested.current,
      initialDefault = Some(nested.initial))
    val parent = Types.NestedField.optional(30, "nested", Types.StructType.of(child))
    val writeSchema = GpuSparkWrite.dataWriterSparkTypeFor(new Schema((fields :+ parent): _*))
    // Independent SQL fixtures describe the data and expected defaults, not the converter's output.
    val projection = cases.map(_.valueSql) :+
      s"named_struct('${nested.name}', ${nested.valueSql})"
    val expected = withCpuSparkSession { spark =>
      spark.sql(s"SELECT ${projection.mkString(", ")}").collect().toSeq :+
        Row.fromSeq(Seq.fill(cases.length + 1)(null))
    }

    withTempPath { output =>
      withGpuSparkSession { spark =>
        val data = spark.createDataFrame(expected.asJava, writeSchema)
        ExecutionPlanCaptureCallback.startCaptureWithWritePlans()
        try {
          data.coalesce(1).write.parquet(output.getAbsolutePath)
          val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
          assert(plans.exists(plan =>
            ExecutionPlanCaptureCallback.contains(plan, "GpuDataWritingCommandExec")),
            plans.mkString("\n"))
        } finally {
          ExecutionPlanCaptureCallback.endCapture()
        }
      }
      withCpuSparkSession { spark =>
        def checkField(field: StructField, id: Int, fixture: DefaultCase): Unit = {
          assert(field.dataType == fixture.sparkType)
          assert(field.nullable)
          assert(field.metadata.getLong(FIELD_ID_METADATA_KEY) == id.toLong)
          val initial = field.metadata.getString("EXISTS_DEFAULT")
          val current = field.getCurrentDefaultValue().get
          val actualDefaults = spark.sql(s"SELECT $initial, $current").head()
          val expectedDefaults = spark.sql(
            s"SELECT ${fixture.initialSql}, ${fixture.currentSql}").head()
          assert(actualDefaults == expectedDefaults, fixture.name)
        }

        val files = output.listFiles().filter(_.getName.endsWith(".parquet"))
        assert(files.nonEmpty)
        files.foreach { file =>
          withResource(ParquetFileReader.open(HadoopInputFile.fromPath(
              new Path(file.getAbsolutePath), spark.sparkContext.hadoopConfiguration))) { reader =>
            val metadata = reader.getFooter.getFileMetaData
            val persisted = DataType.fromJson(metadata.getKeyValueMetaData.get(
                "org.apache.spark.sql.parquet.row.metadata")).asInstanceOf[StructType]
            assert(persisted.fieldNames.toSeq == (cases.map(_.name) :+ "nested"))
            cases.zipWithIndex.foreach { case (fixture, index) =>
              assert(metadata.getSchema.getFields.get(index).getId.intValue() == index + 1)
              checkField(persisted(fixture.name), index + 1, fixture)
            }
            val struct = persisted("nested")
            assert(struct.metadata.getLong(FIELD_ID_METADATA_KEY) == 30L)
            assert(struct.nullable)
            val parquetStruct = metadata.getSchema.getFields.get(cases.length)
            assert(parquetStruct.getId.intValue() == 30)
            assert(parquetStruct.asGroupType().getFields.get(0).getId.intValue() == 31)
            checkField(struct.dataType.asInstanceOf[StructType](nested.name), 31, nested)
          }
        }
        val actual = spark.read.parquet(output.getAbsolutePath).collect().toSeq
        assert(actual.length == expected.length)
        expected.foreach(row => assert(actual.contains(row)))
      }
    }
  }

  test("a position-only DELETE uses the position-delete schema") {
    val context = mock(classOf[GpuWriteContext])
    assert(GpuSparkPositionDeltaWrite.dataWriterSparkTypeFor(Command.DELETE, context) ==
      GpuWriteContext.positionDeleteSparkType)
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
