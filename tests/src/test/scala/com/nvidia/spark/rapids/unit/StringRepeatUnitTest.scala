/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.unit

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuColumnVector, GpuScalar, GpuUnitTests}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.rapids.GpuStringRepeat
import org.apache.spark.sql.types.DataTypes

// This class just covers some test cases that are not possible to call in PySpark tests.
// The remaining tests are in `integration_test/src/main/python/string_test.py`.
class StringRepeatUnitTest extends GpuUnitTests {
  test("Test StringRepeat with scalar string and scalar repeatTimes") {
    // Test repeat(NULL|str, NULL).
    withResource(GpuScalar(null, DataTypes.IntegerType)) { nullInt =>
      val doTest = (strScalar: GpuScalar) => {
        withResource(GpuStringRepeat(null, null).doColumnar(1, strScalar, nullInt)) { result =>
          assertResult(1)(result.getRowCount)
          assertResult(result.getNullCount)(result.getRowCount)
        }
      }

      withResource(GpuScalar(null, DataTypes.StringType)) { nullStr => doTest(nullStr) }
      withResource(GpuScalar("abc123", DataTypes.StringType)) { str => doTest(str) }
      withResource(GpuScalar("á é í", DataTypes.StringType)) { str => doTest(str) }
    }

    // Test repeat(NULL, intVal).
    withResource(GpuScalar(null, DataTypes.StringType)) { nullStr =>
      val doTest = (intVal: Int) =>
        withResource(GpuScalar(intVal, DataTypes.IntegerType)) { intScalar =>
          withResource(GpuStringRepeat(null, null).doColumnar(1, nullStr, intScalar)) { result =>
            assertResult(1)(result.getRowCount)
            assertResult(result.getNullCount)(result.getRowCount)
          }
        }

      doTest(-1)
      doTest(0)
      doTest(1)
    }

    // Test repeat(str, intVal).
    withResource(GpuScalar("abc123", DataTypes.StringType)) { strScalar =>
      val doTest = (intVal: Int, expectedStr: String) =>
        withResource(GpuScalar(intVal, DataTypes.IntegerType)) { intScalar =>
          withResource(GpuStringRepeat(null, null).doColumnar(1, strScalar, intScalar)) {
            result =>
              withResource(result.copyToHost()) { hostResult =>
                assertResult(1)(hostResult.getRowCount)
                assertResult(0)(hostResult.getNullCount)
                assertResult(expectedStr)(hostResult.getJavaString(0))
              }
          }
        }

      doTest(-1, "")
      doTest(0, "")
      doTest(1, "abc123")
      doTest(2, "abc123abc123")
      doTest(3, "abc123abc123abc123")
    }
  }

  test("Test StringRepeat with NULL scalar string and column repeatTimes") {
    val intCol = ColumnVector.fromBoxedInts(-3, null, -1, 0, 1, 2, null)

    withResource(GpuColumnVector.from(intCol, DataTypes.IntegerType)) { intGpuColumn =>
      // Test repeat(NULL, intVal).
      withResource(GpuScalar(null, DataTypes.StringType)) { nullStr =>
        withResource(GpuStringRepeat(null, null).doColumnar(nullStr, intGpuColumn)) { result =>
          assertResult(intGpuColumn.getRowCount)(result.getRowCount)
          assertResult(result.getRowCount)(result.getNullCount)
        }
      }
    }
  }

  test("Test StringRepeat with column strings and NULL scalar repeatTimes") {
    val strsCol = ColumnVector.fromStrings(null, "a", "", "123", "éá")

    withResource(GpuColumnVector.from(strsCol, DataTypes.StringType)) { strGpuColumn =>
      // Test repeat(strs, NULL).
      withResource(GpuScalar(null, DataTypes.IntegerType)) { nullInt =>
        withResource(GpuStringRepeat(null, null).doColumnar(strGpuColumn, nullInt)) { result =>
          assertResult(strGpuColumn.getRowCount)(result.getRowCount)
          assertResult(result.getRowCount)(result.getNullCount)
        }
      }
    }
  }

}
