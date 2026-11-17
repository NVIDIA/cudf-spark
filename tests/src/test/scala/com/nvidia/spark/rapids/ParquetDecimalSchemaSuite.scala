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

package com.nvidia.spark.rapids

import java.math.BigInteger

import ai.rapids.cudf.{ColumnVector, ColumnView, Scalar, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.parquet.ParquetSchemaUtils
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.sql.types.{DecimalType, StructField, StructType}

class ParquetDecimalSchemaSuite extends RmmSparkRetrySuiteBase {
  private def evolve(
      input: ColumnVector, from: DecimalType, to: DecimalType, nested: Boolean = false): Table = {
    val physicalType = if (from.precision <= 9) {
      "int32"
    } else if (from.precision <= 18) {
      "int64"
    } else {
      "fixed_len_byte_array(16)"
    }
    val field = s"optional $physicalType d (DECIMAL(${from.precision},${from.scale}));"
    val fileSchema = MessageTypeParser.parseMessageType(
      s"message spark { ${if (nested) s"optional group s { $field }" else field} }")
    val fields = StructType(Seq(StructField("d", to)))
    val readSchema = if (nested) StructType(Seq(StructField("s", fields))) else fields
    // Exercise the real Parquet cast predicate through its caller, without injecting OOMs.
    ParquetSchemaUtils.evolveSchemaIfNeededAndClose(new Table(input), fileSchema, readSchema,
      caseSensitive = true, useFieldId = false)
  }

  Seq((9, 7, false), (18, 10, false), (34, 20, false), (34, 20, true),
      (20, 10, false), (7, 9, false), (10, 18, false), (9, 9, false), (20, 20, false)).foreach {
    case (fromPrecision, toPrecision, nested) =>
      test(s"Parquet decimal($fromPrecision,2) to decimal($toPrecision,2), nested=$nested") {
        val from = DecimalType(fromPrecision, 2)
        val to = DecimalType(toPrecision, 2)
        val narrows = fromPrecision > toPrecision
        val bound = BigInteger.TEN.pow(toPrecision)
        val values = if (narrows) {
          Seq(bound.subtract(BigInteger.ONE), bound, bound.negate(), BigInteger.valueOf(42))
        } else {
          Seq(BigInteger.valueOf(42), BigInteger.valueOf(-42))
        }
        withResource(ColumnVector.decimalFromBigInt(-2, values: _*)) { wide =>
          withResource(wide.castTo(DecimalUtil.createCudfDecimal(from))) { input =>
            val originalRefCount = input.getRefCount
            def check(result: Table): Unit = {
              def checkColumn(actual: ColumnView): Unit = {
                assert(actual.getType == DecimalUtil.createCudfDecimal(to))
                // Check the device null mask: host decimal conversion can hide overflow.
                assert(actual.getNullCount == (if (narrows) 2 else 0))
                withResource(actual.copyToHost()) { host =>
                  values.zipWithIndex.foreach { case (value, index) =>
                    if (narrows && (index == 1 || index == 2)) {
                      assert(host.isNull(index))
                    } else {
                      assert(!host.isNull(index))
                      assert(host.getBigDecimal(index) == new java.math.BigDecimal(value, 2))
                    }
                  }
                }
              }
              if (nested) {
                withResource(result.getColumn(0).getChildColumnView(0))(checkColumn)
              } else {
                checkColumn(result.getColumn(0))
              }
            }
            if (nested) {
              withResource(ColumnVector.makeStruct(input)) { struct =>
                withResource(evolve(struct, from, to, nested = true))(check)
              }
            } else {
              withResource(evolve(input, from, to))(check)
            }
            assert(input.getRefCount == originalRefCount)
          }
        }
      }
  }

  test("Parquet all-null decimal narrowing preserves row count and nulls") {
    val from = DecimalType(20, 2)
    val to = DecimalType(10, 2)
    withResource(Scalar.fromNull(DecimalUtil.createCudfDecimal(from))) { nullValue =>
      withResource(ColumnVector.fromScalar(nullValue, 3)) { input =>
        withResource(evolve(input, from, to)) { result =>
          assert(result.getRowCount == 3)
          assert(result.getColumn(0).getNullCount == 3)
          assert(result.getColumn(0).getType == DecimalUtil.createCudfDecimal(to))
        }
        assert(input.getRefCount == 1)
      }
    }
  }
}
