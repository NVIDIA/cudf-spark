/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.math.RoundingMode

import scala.util.Random

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.{GpuAlias, GpuColumnVector, GpuIsNotNull, GpuIsNull, GpuLiteral, GpuOverrides, GpuScalar, GpuUnitTests, HostColumnarToGpu, RapidsConf, RapidsHostColumnBuilder}
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class DecimalUnitTest extends GpuUnitTests {
  Random.setSeed(1234L)

  private val dec32Data = Array.fill[Decimal](10)(
    Decimal.fromDecimal(BigDecimal(Random.nextInt() / 1000, 3 + Random.nextInt(3))))
  private val dec64Data = Array.fill[Decimal](10)(
    Decimal.fromDecimal(BigDecimal(Random.nextLong() / 1000, 7 + Random.nextInt(3))))

  test("test decimal as scalar") {
    Array(dec32Data, dec64Data).flatten.foreach { dec =>
      // test GpuScalar.from(v: Any, t: DataType)
      val dt = DecimalType(DType.DECIMAL64_MAX_PRECISION, dec.scale)
      withResource(GpuScalar.from(dec.toDouble, dt)) { s =>
        assertResult(-dt.scale)(s.getType.getScale)
        assertResult(dec.toDouble)(GpuScalar.extract(s).asInstanceOf[Decimal].toDouble)
      }
      withResource(GpuScalar.from(dec.toString(), dt)) { s =>
        assertResult(-dt.scale)(s.getType.getScale)
        assertResult(dec.toString)(GpuScalar.extract(s).asInstanceOf[Decimal].toString)
      }
      val long = dec.toLong
      withResource(GpuScalar.from(long, DecimalType(dec.precision, 0))) { s =>
        assertResult(0)(s.getType.getScale)
        assertResult(long)(GpuScalar.extract(s).asInstanceOf[Decimal].toLong)
      }
    }
    // test exception throwing due to unsupported type
    assertThrows[IllegalArgumentException] {
      withResource(GpuScalar.from(true, DecimalType(10, 1))) { _ => }
    }
    // test exception throwing due to exceeded precision
    assertThrows[IllegalArgumentException] {
      val bigDec = Decimal(BigDecimal(Long.MaxValue / 100, 0))
      withResource(GpuScalar.from(bigDec, DecimalType(15, 1))) { _ => }
    }
  }

  test("test decimal as column vector") {
    val dt32 = DecimalType(DType.DECIMAL64_MAX_PRECISION, 5)
    val dt64 = DecimalType(DType.DECIMAL64_MAX_PRECISION, 9)
    val cudfCV = ColumnVector.decimalFromDoubles(GpuColumnVector.getNonNestedRapidsType(dt32),
      RoundingMode.UNNECESSARY, dec32Data.map(_.toDouble): _*)
    withResource(GpuColumnVector.from(cudfCV, dt32)) { cv: GpuColumnVector =>
      assertResult(dec32Data.length)(cv.getRowCount)
      val (precision, scale) = cv.dataType() match {
        case dt: DecimalType => (dt.precision, dt.scale)
      }
      withResource(cv.copyToHost()) { hostCv =>
        dec32Data.zipWithIndex.foreach { case (dec, i) =>
          val rescaled = dec.toJavaBigDecimal.setScale(scale)
          assertResult(rescaled.unscaledValue().longValueExact())(hostCv.getLong(i))
          assertResult(Decimal(rescaled))(hostCv.getDecimal(i, precision, scale))
        }
      }
    }
    val dec64WithNull = Array(null) ++ dec64Data.map(_.toJavaBigDecimal) ++ Array(null, null)
    withResource(GpuColumnVector.from(ColumnVector.fromDecimals(dec64WithNull: _*), dt64)) { cv =>
      assertResult(dec64WithNull.length)(cv.getRowCount)
      assertResult(true)(cv.hasNull)
      assertResult(3)(cv.numNulls)
      val (precision, scale) = cv.dataType() match {
        case dt: DecimalType => (dt.precision, dt.scale)
      }
      withResource(cv.copyToHost()) { hostCv =>
        dec64WithNull.zipWithIndex.foreach {
          case (dec, i) if dec == null =>
            assertResult(true)(hostCv.getBase.isNull(i))
          case (dec, i) =>
            val rescaled = dec.setScale(scale)
            assertResult(rescaled.unscaledValue().longValueExact())(hostCv.getLong(i))
            assertResult(Decimal(rescaled))(hostCv.getDecimal(i, precision, scale))
        }
      }
    }
    withResource(ColumnVector.decimalFromInts(0, 1)) { dcv =>
      GpuColumnVector.from(dcv, DecimalType(1, 0))
    }
    withResource(GpuScalar.from(dec64Data(0), dt64)) { scalar =>
      withResource(GpuColumnVector.from(scalar, 10, dt64)) { cv =>
        assertResult(10)(cv.getRowCount)
        withResource(cv.copyToHost()) { hcv =>
          (0 until 10).foreach { i =>
            assertResult(scalar.getLong)(hcv.getLong(i))
            assertResult(scalar.getBigDecimal)(
              hcv.getDecimal(i, dt64.precision, dt64.scale).toJavaBigDecimal)
          }
        }
      }
    }
  }

  private val rapidsConf = new RapidsConf(Map[String, String]())

  private val lit = Literal(dec32Data(0), DecimalType(dec32Data(0).precision, dec32Data(0).scale))

  test("test Literal with decimal") {
    val wrapperLit = GpuOverrides.wrapExpr(lit, rapidsConf, None)
    wrapperLit.tagForGpu()
    assertResult(true)(wrapperLit.canExprTreeBeReplaced)
    val gpuLit = wrapperLit.convertToGpu().asInstanceOf[GpuLiteral]
    withResourceIfAllowed(gpuLit.columnarEvalAny(null)) { s =>
      assertResult(lit.eval(null))(s.asInstanceOf[GpuScalar].getValue)
    }
    assertResult(lit.sql)(gpuLit.sql)

    // inconvertible because of precision overflow
    val wrp = GpuOverrides.wrapExpr(Literal(Decimal(12345L), DecimalType(38, 10)), rapidsConf, None)
    wrp.tagForGpu()
    assertResult(true)(wrp.canExprTreeBeReplaced)
  }

  test("test Alias with decimal") {
    val cpuAlias = Alias(lit, "A")()
    val wrapperAlias = GpuOverrides.wrapExpr(cpuAlias, rapidsConf, None)
    wrapperAlias.tagForGpu()
    assertResult(true)(wrapperAlias.canExprTreeBeReplaced)
    val gpuAlias = wrapperAlias.convertToGpu().asInstanceOf[GpuAlias]

    assertResult(cpuAlias.dataType)(gpuAlias.dataType)
    assertResult(cpuAlias.sql)(gpuAlias.sql)
    val batch = new ColumnarBatch(Array.empty, 1)
    withResourceIfAllowed(gpuAlias.columnarEvalAny(batch)) { s =>
      assertResult(cpuAlias.eval(null))(s.asInstanceOf[GpuScalar].getValue)
    }
  }

  test("test AttributeReference with decimal") {
    val cpuAttrRef = AttributeReference("test123", lit.dataType)()
    val wrapperAttrRef = GpuOverrides.wrapExpr(cpuAttrRef, rapidsConf, None)
    wrapperAttrRef.tagForGpu()
    assertResult(true)(wrapperAttrRef.canExprTreeBeReplaced)
    val gpuAttrRef = wrapperAttrRef.convertToGpu().asInstanceOf[AttributeReference]
    assertResult(cpuAttrRef.sql)(gpuAttrRef.sql)
    assertResult(true)(gpuAttrRef.sameRef(cpuAttrRef))
  }

  test("test IsNull/IsNotNull with decimal") {
    val decArray = Array(BigDecimal(1234567890L).bigDecimal, null,
      BigDecimal(-123L).bigDecimal, null, null)
    withResource(GpuColumnVector.from(ColumnVector.fromDecimals(decArray: _*), DecimalType(10, 0))
    ) { cv =>
      withResource(GpuIsNull(null).doColumnar(cv)) { isNullResult =>
        withResource(isNullResult.copyToHost()) { ret =>
          assertResult(false)(ret.getBoolean(0))
          assertResult(true)(ret.getBoolean(1))
          assertResult(false)(ret.getBoolean(2))
          assertResult(true)(ret.getBoolean(3))
          assertResult(true)(ret.getBoolean(4))
        }
      }
      withResource(GpuIsNotNull(null).doColumnar(cv)) { isNotNullResult =>
        withResource(isNotNullResult.copyToHost()) { ret =>
          assertResult(true)(ret.getBoolean(0))
          assertResult(false)(ret.getBoolean(1))
          assertResult(true)(ret.getBoolean(2))
          assertResult(false)(ret.getBoolean(3))
          assertResult(false)(ret.getBoolean(4))
        }
      }
    }
  }

  test("test HostColumnarToGpu.columnarCopy") {
    withResource(
      GpuColumnVector.from(ColumnVector.fromDecimals(dec64Data.map(_.toJavaBigDecimal): _*),
        DecimalType(DType.DECIMAL64_MAX_PRECISION, 9))) { cv =>
      val dt = new HostColumnVector.BasicType(false,
        GpuColumnVector.getNonNestedRapidsType(cv.dataType()))
      withResource(new RapidsHostColumnBuilder(dt, cv.getRowCount)) { builder =>
        withResource(cv.copyToHost()) { hostCV =>
          HostColumnarToGpu.columnarCopy(hostCV, builder, cv.dataType(), cv.getRowCount.toInt)
          withResource(builder.build()) { actual =>
            val expected = hostCV.getBase
            assertResult(expected.getType)(actual.getType)
            assertResult(expected.getRowCount)(actual.getRowCount)
            (0 until actual.getRowCount.toInt).foreach { i =>
              assertResult(expected.getBigDecimal(i))(actual.getBigDecimal(i))
            }
          }
        }
      }
    }
    val dec64WithNull = dec64Data.splitAt(5) match {
      case (left, right) =>
        Array(null) ++ left.map(_.toJavaBigDecimal) ++ Array(null) ++
          right.map(_.toJavaBigDecimal) ++ Array(null)
    }
    withResource(
      GpuColumnVector.from(ColumnVector.fromDecimals(dec64WithNull: _*),
        DecimalType(DType.DECIMAL64_MAX_PRECISION, 9))) { cv =>
      val dt = new HostColumnVector.BasicType(true,
        GpuColumnVector.getNonNestedRapidsType(cv.dataType()))
      withResource(new RapidsHostColumnBuilder(dt, cv.getRowCount)) { builder =>
        withResource(cv.copyToHost()) { hostCV =>
          HostColumnarToGpu.columnarCopy(hostCV, builder, cv.dataType(), cv.getRowCount.toInt)
          withResource(builder.build()) { actual =>
            val expected = hostCV.getBase
            assertResult(DType.create(DType.DTypeEnum.DECIMAL64, expected.getType.getScale)
            )(actual.getType)
            assertResult(expected.getRowCount)(actual.getRowCount)
            (0 until actual.getRowCount.toInt).foreach { i =>
              assertResult(expected.isNull(i))(actual.isNull(i))
              if (!actual.isNull(i)) {
                assertResult(expected.getBigDecimal(i))(actual.getBigDecimal(i))
              }
            }
          }
        }
      }
    }
  }

  test("test type checking of Scans") {
    val conf = new SparkConf()
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "BatchScanExec,ColumnarToRowExec,FileSourceScanExec")
    val decimalCsvStruct = StructType(Array(
      StructField("c_0", DecimalType(18, 0), true),
      StructField("c_1", DecimalType(7, 3), true),
      StructField("c_2", DecimalType(10, 10), true),
      StructField("c_3", DecimalType(15, 12), true),
      StructField("c_4", LongType, true),
      StructField("c_5", IntegerType, true)))

    withGpuSparkSession((ss: SparkSession) => {
      var rootPlan = frameFromOrc("decimal-test.orc")(ss).queryExecution.executedPlan
      assert(rootPlan.map(p => p).exists(_.isInstanceOf[GpuFileSourceScanExec]))
      rootPlan = fromCsvDf("decimal-test.csv", decimalCsvStruct)(ss).queryExecution.executedPlan
      assert(rootPlan.map(p => p).exists(_.isInstanceOf[FileSourceScanExec]))
      rootPlan = frameFromParquet("decimal-test.parquet")(ss).queryExecution.executedPlan
      assert(rootPlan.map(p => p).exists(_.isInstanceOf[GpuFileSourceScanExec]))
    }, conf)

    withGpuSparkSession((ss: SparkSession) => {
      var rootPlan = frameFromOrc("decimal-test.orc")(ss).queryExecution.executedPlan
      assert(rootPlan.map(p => p).exists(_.isInstanceOf[GpuBatchScanExec]))
      rootPlan = fromCsvDf("decimal-test.csv", decimalCsvStruct)(ss).queryExecution.executedPlan
      assert(rootPlan.map(p => p).exists(_.isInstanceOf[BatchScanExec]))
      rootPlan = frameFromParquet("decimal-test.parquet")(ss).queryExecution.executedPlan
      assert(rootPlan.map(p => p).exists(_.isInstanceOf[GpuBatchScanExec]))
    }, conf.set(SQLConf.USE_V1_SOURCE_LIST.key, ""))
  }
}
