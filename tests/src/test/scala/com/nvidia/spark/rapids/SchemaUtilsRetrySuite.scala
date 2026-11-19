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

import org.apache.spark.sql.types._

class SchemaUtilsRetrySuite extends RmmSparkRetrySuiteBase {
  Seq("struct", "array", "map value").foreach { shape =>
    test(s"nested decimal narrowing cleans up $shape siblings and preserves retry OOM") {
      def nestedType(first: DecimalType, second: DecimalType): DataType = {
        val fields = StructType(Seq(StructField("a", first), StructField("b", second)))
        shape match {
          case "struct" => fields
          case "array" => ArrayType(fields)
          case "map value" => MapType(IntegerType, fields)
        }
      }
      def schema(first: DecimalType, second: DecimalType): StructType =
        StructType(Seq(StructField("s", nestedType(first, second))))

      // Keep the same two sibling casts inside each supported container shape.
      def wrap(fields: ColumnVector): ColumnVector = shape match {
        case "struct" => fields.incRefCount()
        case "array" => ColumnVector.makeList(fields)
        case "map value" =>
          withResource(ColumnVector.fromInts(1, 2)) { keys =>
            withResource(ColumnVector.makeStruct(keys, fields)) { entries =>
              ColumnVector.makeList(entries)
            }
          }
      }

      val sourceSchema = schema(DecimalType(18, 2), DecimalType(18, 2))
      val firstTarget = DecimalType(10, 2)
      val secondTarget = DecimalType(11, 2)
      val readSchema = schema(firstTarget, secondTarget)
      val oom = new GpuRetryOOM("second nested decimal cast")
      var failSecondField = true
      // Observe returned vectors without retaining them: evolution owns their cleanup.
      val castResults = new ArrayBuffer[(DataType, ColumnVector)]
      val cast = (column: ColumnView, target: DataType, source: DataType) => {
        // Identify the failing sibling by its target type, not invocation counts.
        if (failSecondField && target == secondTarget) {
          failSecondField = false
          throw oom
        }
        val result = GpuCast.castDecimalToDecimal(column, source.asInstanceOf[DecimalType],
          target.asInstanceOf[DecimalType], ansiMode = false)
        castResults += ((target, result))
        result
      }
      def assertResultsClosed(expectedTargets: Set[DataType]): Unit = {
        assert(castResults.map(_._1).toSet == expectedTargets)
        assert(castResults.forall(_._2.getRefCount == 0))
      }

      withResource(ColumnVector.decimalFromLongs(-2, 42L, 100000000001L)) { decimal =>
        withResource(ColumnVector.makeStruct(decimal, decimal)) { fields =>
          withResource(wrap(fields)) { input =>
            val originalRefCount = input.getRefCount
            // Evolution consumes the table, including when the second child fails.
            def evolve(): Table = SchemaUtils.evolveSchemaIfNeededAndClose(
              new Table(input), sourceSchema, readSchema, isCaseSensitive = true,
              castFunc = Some(cast))

            val thrown = intercept[GpuRetryOOM] {
              withResource(evolve()) { _ => fail("second child must fail") }
            }
            assert(thrown eq oom)
            assertResultsClosed(Set(firstTarget))
            assert(input.getRefCount == originalRefCount)

            failSecondField = true
            castResults.clear()
            var attempts = 0
            withResource(withRetryNoSplit[Table] {
              attempts += 1
              evolve()
            }) { output =>
              assert(attempts == 2)
              assertResultsClosed(Set(firstTarget, secondTarget))
              def assertFields(actual: ColumnView): Unit = {
                assert(actual.getRowCount == 2)
                assert(actual.getNullCount == 0)
                withResource(ColumnVector.decimalFromBoxedLongs(-2, Long.box(42L), null)) {
                  expected =>
                    (0 until 2).foreach { index =>
                      withResource(actual.getChildColumnView(index)) { child =>
                        CudfTestHelper.assertColumnsAreEqual(expected, child)
                      }
                    }
                }
              }
              def assertList(actual: ColumnView)(checkChild: ColumnView => Unit): Unit = {
                assert(actual.getRowCount == 2)
                assert(actual.getNullCount == 0)
                withResource(actual.copyToHost()) { host =>
                  (0 until 2).foreach(index => assert(host.getList(index).size() == 1))
                }
                withResource(actual.getChildColumnView(0))(checkChild)
              }
              val actual = output.getColumn(0)
              shape match {
                case "struct" => assertFields(actual)
                case "array" => assertList(actual)(assertFields)
                case "map value" =>
                  assertList(actual) { entries =>
                    assert(entries.getNullCount == 0)
                    withResource(ColumnVector.fromInts(1, 2)) { expectedKeys =>
                      withResource(entries.getChildColumnView(0)) { keys =>
                        CudfTestHelper.assertColumnsAreEqual(expectedKeys, keys)
                      }
                    }
                    withResource(entries.getChildColumnView(1))(assertFields)
                  }
              }
            }
            assert(input.getRefCount == originalRefCount)
          }
        }
      }
    }
  }
}
