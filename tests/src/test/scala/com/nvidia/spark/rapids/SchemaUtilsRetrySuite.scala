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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ColumnView, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.jni.GpuRetryOOM

import org.apache.spark.sql.types.{DataType, DecimalType, StructField, StructType}

class SchemaUtilsRetrySuite extends RmmSparkRetrySuiteBase {
  test("nested decimal narrowing closes sibling results once and preserves retry OOM") {
    def schema(decimal: DecimalType): StructType = StructType(Seq(
      StructField("s", StructType(Seq(
        StructField("a", decimal), StructField("b", decimal))))))

    val sourceSchema = schema(DecimalType(18, 2))
    val readSchema = schema(DecimalType(10, 2))
    val oom = new GpuRetryOOM("second nested decimal cast")
    var calls = 0
    // Observe the returned vectors without retaining them: schema evolution owns their cleanup.
    val castResults = new ArrayBuffer[ColumnVector]
    val cast = (column: ColumnView, target: DataType, source: DataType) => {
      calls += 1
      if (calls == 2) {
        throw oom
      }
      val result = GpuCast.castDecimalToDecimal(column, source.asInstanceOf[DecimalType],
        target.asInstanceOf[DecimalType], ansiMode = false)
      castResults += result
      result
    }

    withResource(ColumnVector.decimalFromLongs(-2, 42L, 10000000001L)) { decimal =>
      withResource(ColumnVector.makeStruct(decimal, decimal)) { input =>
        // This method consumes the table, including when the second child's callback fails.
        def evolve(): Table = SchemaUtils.evolveSchemaIfNeededAndClose(
          new Table(input), sourceSchema, readSchema, isCaseSensitive = true,
          castFunc = Some(cast))

        val thrown = intercept[GpuRetryOOM] {
          withResource(evolve()) { _ => fail("second child must fail") }
        }
        assert(thrown eq oom)
        assert(calls == 2)
        assert(castResults.size == 1)
        assert(castResults.head.getRefCount == 0)
        assert(input.getRefCount == 1)

        calls = 0
        castResults.clear()
        var attempts = 0
        withResource(withRetryNoSplit[Table] {
          attempts += 1
          evolve()
        }) { output =>
          assert(attempts == 2)
          assert(calls == 4)
          assert(castResults.size == 3)
          assert(castResults.forall(_.getRefCount == 0))
          withResource(ColumnVector.decimalFromBoxedLongs(-2, Long.box(42L), null)) { expected =>
            (0 until 2).foreach { index =>
              withResource(output.getColumn(0).getChildColumnView(index)) { actual =>
                CudfTestHelper.assertColumnsAreEqual(expected, actual)
              }
            }
          }
        }
        assert(input.getRefCount == 1)
      }
    }
  }
}
