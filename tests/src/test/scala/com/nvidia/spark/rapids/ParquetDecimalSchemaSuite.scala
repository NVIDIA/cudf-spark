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

import ai.rapids.cudf.{ColumnVector, Scalar, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.parquet.ParquetSchemaUtils
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.sql.types.{DecimalType, StructField, StructType}

class ParquetDecimalSchemaSuite extends RmmSparkRetrySuiteBase {
  private def evolve(input: ColumnVector, from: DecimalType, to: DecimalType): Table = {
    val physicalType = if (from.precision <= 9) {
      "int32"
    } else if (from.precision <= 18) {
      "int64"
    } else {
      "fixed_len_byte_array(16)"
    }
    val fileSchema = MessageTypeParser.parseMessageType(
      s"message spark { optional $physicalType d (DECIMAL(${from.precision},${from.scale})); }")
    val readSchema = StructType(Seq(StructField("d", to)))
    // Exercise the real Parquet cast predicate through its caller, without injecting OOMs.
    ParquetSchemaUtils.evolveSchemaIfNeededAndClose(new Table(input), fileSchema, readSchema,
      caseSensitive = true, useFieldId = false)
  }

  Seq((9, 7), (20, 10), (7, 9), (10, 18), (9, 9), (20, 20)).foreach {
    case (fromPrecision, toPrecision) =>
      test(s"Parquet decimal($fromPrecision,2) to decimal($toPrecision,2) without retry") {
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
            withResource(evolve(input, from, to)) { result =>
              assert(result.getColumn(0).getType == DecimalUtil.createCudfDecimal(to))
              if (narrows) {
                withResource(ColumnVector.decimalFromBoxedLongs(-2,
                    Long.box(bound.longValueExact() - 1), null, null, Long.box(42))) { expected =>
                  withResource(expected.castTo(DecimalUtil.createCudfDecimal(to))) { typed =>
                    CudfTestHelper.assertColumnsAreEqual(typed, result.getColumn(0))
                  }
                }
              } else {
                CudfTestHelper.assertColumnsAreEqual(input, result.getColumn(0))
              }
            }
            assert(input.getRefCount == 1)
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
