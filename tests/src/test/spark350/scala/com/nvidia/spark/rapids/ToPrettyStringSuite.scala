/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.shims.GpuToPrettyString

import org.apache.spark.sql.catalyst.expressions.{BoundReference, NamedExpression, ToPrettyString}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class ToPrettyStringSuite extends GpuUnitTests {

  private def testDataType(dataType: DataType): Unit = {
    val schema = (new StructType)
      .add(StructField("a", dataType, true))
    val numRows = 100
    val inputRows = GpuBatchUtilsSuite.createRows(schema, numRows)
    val cpuOutput: Array[String] = inputRows.map {
      input => 
        ToPrettyString(BoundReference(0, dataType, true), Some("UTC"))
        .eval(input).asInstanceOf[UTF8String].toString()
    }
    val child = GpuBoundReference(0, dataType, true)(NamedExpression.newExprId, "arg")
    val gpuToPrettyStr = GpuToPrettyString(child, Some("UTC"))

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val r2cConverter = new GpuRowToColumnConverter(schema)
      inputRows.foreach(r2cConverter.convert(_, batchBuilder))
      withResource(batchBuilder.build(numRows)) { columnarBatch =>
        withResource(GpuColumnVector.from(ColumnVector.fromStrings(cpuOutput: _*),
          DataTypes.StringType)) { expected =>
          checkEvaluation(gpuToPrettyStr, expected, columnarBatch)
        }
      }
    }
  }

  test("test show() on booleans") {
    testDataType(DataTypes.BooleanType)
  }

  test("test show() on bytes") {
    testDataType(DataTypes.ByteType)
  }

  test("test show() on shorts") {
    testDataType(DataTypes.ShortType)
  }

  test("test show() on ints") {
    testDataType(DataTypes.IntegerType)
  }

  test("test show() on longs") {
    testDataType(DataTypes.LongType)
  }

  test("test show() on floats") {
    testDataType(DataTypes.FloatType)
  }

  test("test show() on doubles") {
    testDataType(DataTypes.DoubleType)
  }

  test("test show() on strings") {
    testDataType(DataTypes.StringType)
  }

  test("test show() on decimals") {
    testDataType(DecimalType(8,2))
  }

  test("test show() on binary") {
    testDataType(DataTypes.BinaryType)
  }

  test("test show() on array") {
    testDataType(ArrayType(DataTypes.IntegerType))
  }

  test("test show() on map") {
    testDataType(MapType(DataTypes.IntegerType, DataTypes.IntegerType))
  }

  test("test show() on struct") {
    testDataType(StructType(Seq(StructField("a", DataTypes.IntegerType),
      StructField("b", DataTypes.IntegerType),
      StructField("c", DataTypes.IntegerType))))
  }
}
