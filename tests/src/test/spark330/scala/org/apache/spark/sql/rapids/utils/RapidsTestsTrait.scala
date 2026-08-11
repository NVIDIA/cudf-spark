/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import java.io.File
import java.util.TimeZone

import com.nvidia.spark.rapids.{ ExprChecksImpl, GpuOverrides, GpuProjectExec, ProjectExprContext, TestStats, TypeEnum, TypeSig}
import org.apache.commons.io.{FileUtils => fu}
import org.apache.commons.math3.util.Precision
import org.scalactic.TripleEqualsSupport.Spread
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsQueryTestUtil.isNaNOrInf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait RapidsTestsTrait extends RapidsTestsCommonTrait {

  val originalTimeZone = TimeZone.getDefault

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      fu.forceDelete(basePathDir)
    }
    fu.forceMkdir(basePathDir)
    fu.forceMkdir(new File(warehouse))
    fu.forceMkdir(new File(metaStorePathAbsolute))
    super.beforeAll()
    initializeSession()
    _spark.sparkContext.setLogLevel("WARN")
    // Spark use "America/Los_Angeles" as default timezone in tests
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  }

  override def afterAll(): Unit = {
    TimeZone.setDefault(originalTimeZone)

    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
    logInfo(
      "Test suite: " + this.getClass.getSimpleName +
        "; Suite test number: " + TestStats.suiteTestNumber +
        "; OffloadRapids number: " + TestStats.offloadRapidsTestNumber + "\n")
    TestStats.printMarkdown(this.getClass.getSimpleName)
    TestStats.reset()
  }

  protected def initializeSession(): Unit = {
    if (_spark == null) {
      val sparkBuilder = SparkSession
        .builder()
        .master(s"local[2]")
        // Avoid static evaluation for literal input by spark catalyst.
        .config(
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
          ConvertToLocalRelation.ruleName +
            "," + ConstantFolding.ruleName)
        .config("spark.rapids.sql.enabled", "true")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.sql.queryExecutionListeners",
          "org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.rapids.sql.explain", "ALL")
        .config("spark.rapids.sql.test.isFoldableNonLitAllowed", "true")
        // uncomment below config to run `strict mode`, where fallback to CPU is treated as fail
        // .config("spark.rapids.sql.test.enabled", "true")
        // .config("spark.rapids.sql.test.allowedNonGpu",
        // "SerializeFromObjectExec,DeserializeToObjectExec,ExternalRDDScanExec")
        .config("spark.rapids.sql.csv.read.decimal.enabled", "true")
        .config("spark.rapids.sql.format.avro.enabled", "true")
        .config("spark.rapids.sql.format.avro.read.enabled", "true")
        .config("spark.rapids.sql.format.hive.text.write.enabled", "true")
        .config("spark.rapids.sql.format.json.enabled", "true")
        .config("spark.rapids.sql.format.json.read.enabled", "true")
        .config("spark.rapids.sql.incompatibleDateFormats.enabled", "true")
        .config("spark.rapids.sql.python.gpu.enabled", "true")
        .config("spark.rapids.sql.rowBasedUDF.enabled", "true")
        .config("spark.rapids.sql.window.collectList.enabled", "true")
        .config("spark.rapids.sql.window.collectSet.enabled", "true")
        .config("spark.rapids.sql.window.range.byte.enabled", "true")
        .config("spark.rapids.sql.window.range.short.enabled", "true")
        .config("spark.rapids.sql.expression.Ascii", "true")
        .config("spark.rapids.sql.expression.Conv", "true")
        .config("spark.rapids.sql.expression.GetJsonObject", "true")
        .config("spark.rapids.sql.expression.JsonToStructs", "true")
        .config("spark.rapids.sql.expression.StructsToJson", "true")
        .config("spark.rapids.sql.exec.CollectLimitExec", "true")
        .config("spark.rapids.sql.exec.FlatMapCoGroupsInPandasExec", "true")
        .config("spark.rapids.sql.exec.WindowInPandasExec", "true")
        .config("spark.rapids.sql.hasExtendedYearValues", "false")
        .config("spark.rapids.memory.gpu.allocFraction",
          System.getProperty(
            "rapids.test.gpu.allocFraction", "1"))
        .config("spark.rapids.memory.gpu.maxAllocFraction",
          System.getProperty(
            "rapids.test.gpu.maxAllocFraction", "1"))
        .config("spark.rapids.memory.gpu.minAllocFraction",
          System.getProperty(
            "rapids.test.gpu.minAllocFraction", "0.25"))
        .appName("rapids spark plugin running Vanilla Spark UT")

      _spark = sparkBuilder
        .config("spark.unsafe.exceptionOnMemoryLeak", "true")
        .getOrCreate()
    }
  }

  protected var _spark: SparkSession = null

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(expression)
    assert(expr.resolved)

    rapidsCheckExpression(expr, expected, inputRow)
  }

  /**
   * Sort map data by key and return the sorted key array and value array.
   *
   * @param input
   * : input map data.
   * @param kt
   * : key type.
   * @param vt
   * : value type.
   * @return
   * the sorted key array and value array.
   */
  private def getSortedArrays(
      input: MapData,
      kt: DataType,
      vt: DataType): (ArrayData, ArrayData) = {
    val keyArray = input.keyArray().toArray[Any](kt)
    val valueArray = input.valueArray().toArray[Any](vt)
    val newMap = (keyArray.zip(valueArray)).toMap
    val sortedMap = mutable.SortedMap(newMap.toSeq: _*)(TypeUtils.getInterpretedOrdering(kt))
    (new GenericArrayData(sortedMap.keys.toArray), new GenericArrayData(sortedMap.values.toArray))
  }

  override protected def checkResult(
      result: Any,
      expected: Any,
      exprDataType: DataType,
      exprNullable: Boolean): Boolean = {
    val dataType = UserDefinedType.sqlType(exprDataType)

    // The result is null for a non-nullable expression
    assert(result != null || exprNullable, "exprNullable should be true if result is null")
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Spread[Double @unchecked]) =>
        expected.asInstanceOf[Spread[Double]].isWithin(result)
      case (result: InternalRow, expected: InternalRow) =>
        val st = dataType.asInstanceOf[StructType]
        assert(result.numFields == st.length && expected.numFields == st.length)
        st.zipWithIndex.forall {
          case (f, i) =>
            checkResult(
              result.get(i, f.dataType),
              expected.get(i, f.dataType),
              f.dataType,
              f.nullable)
        }
      case (result: ArrayData, expected: ArrayData) =>
        result.numElements == expected.numElements && {
          val ArrayType(et, cn) = dataType.asInstanceOf[ArrayType]
          var isSame = true
          var i = 0
          while (isSame && i < result.numElements) {
            isSame = checkResult(result.get(i, et), expected.get(i, et), et, cn)
            i += 1
          }
          isSame
        }
      case (result: MapData, expected: MapData) =>
        val MapType(kt, vt, vcn) = dataType.asInstanceOf[MapType]
        checkResult(
          getSortedArrays(result, kt, vt)._1,
          getSortedArrays(expected, kt, vt)._1,
          ArrayType(kt, containsNull = false),
          exprNullable = false) && checkResult(
          getSortedArrays(result, kt, vt)._2,
          getSortedArrays(expected, kt, vt)._2,
          ArrayType(vt, vcn),
          exprNullable = false)
      case (result: Double, expected: Double) =>
        if (result.isNaN && expected.isNaN) {
          true
        } else if (
          (isNaNOrInf(result) || isNaNOrInf(expected))
            || (result == -0.0) || (expected == -0.0)
        ) {
          java.lang.Double.doubleToRawLongBits(result) ==
            java.lang.Double.doubleToRawLongBits(expected)
        } else {
          Precision.equalsWithRelativeTolerance(result, expected, 0.00001d)
        }
      case (result: Float, expected: Float) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Row, expected: InternalRow) => result.toSeq == expected.toSeq(result.schema)
      case _ =>
        result == expected
    }
  }

  def checkDataTypeSupported(expr: Expression): Boolean = {
    RapidsTestConstants.SUPPORTED_DATA_TYPE.acceptsType(expr.dataType)
  }

  /**
   * Many of the expressions in RAPIDS do not support vectorized parameters(e.g. regexp_replace)
   * So need to check whether the expression being evaluated is qualified for vectorized parameters
   *
   * If Yes, we'll use pass the parameters of the expression as vectors (Vectorized Parameter).
   *
   * If No, we'll replace all the parameters with literals (Scalar Parameter) and evaluate
   * the expression. We're actually evaluating a constant expression tree in this case,
   * but it's fine for testing purposes. Notice that we'll need to make sure Constant Folding is
   * disabled.
   *
   * We always prefer Vectorized Parameters to evaluate expressions. Because Scalar Parameter
   * may hide some bugs. For example, an expression `some_expr(NULL)` may correctly return NULL
   * only because NullPropagation is working. But if we evaluate the expression with a vector
   * containing NUll, it might fail.
   *
   * @param e the expression being evaluated
   * @return true if the expression is qualified for vectorized parameters
   */
  def isQualifiedForVectorizedParams(e: Expression): Boolean = {
    val map = GpuOverrides.expressions
    e.foreachUp(expr => {
      logDebug(s"Checking expr $expr :\n")
      if (!map.contains(expr.getClass)) {
        logDebug(s"Check failed because ${expr.getClass} not found in GpuOverrides.expressions\n")
        return false
      }
      map(expr.getClass).getChecks.foreach(check => {
        if (check.isInstanceOf[ExprChecksImpl]) {
          val exprChecksImpl = check.asInstanceOf[ExprChecksImpl]
          if (!exprChecksImpl.contexts.contains(ProjectExprContext)) {
            logDebug(s"Check failed because $exprChecksImpl does not contain ProjectExprContext\n")
            return false
          }
          val context = exprChecksImpl.contexts(ProjectExprContext)
          (context.paramCheck.map(_.cudf) ++ context.repeatingParamCheck.map(_.cudf))
            .foreach(sig => {
              // use reflection to get the private field litOnlyTypes
              import scala.reflect.runtime.universe._
              val mirror = runtimeMirror(sig.getClass.getClassLoader)
              val privateFieldSymbol = typeOf[TypeSig].decl(TermName("litOnlyTypes")).asTerm
              val privateFieldMirror =
                mirror.reflect(sig).reflectField(privateFieldSymbol)
              val litOnlyTypes = privateFieldMirror.get.asInstanceOf[TypeEnum.ValueSet]
              if (litOnlyTypes.nonEmpty) {
                logDebug(s"Check failed because non empty litOnlyTypes: $litOnlyTypes \n")
                return false
              }
            })
        } else {
          logDebug(s"Check continues by skipping ${check.getClass}")
        }
      })
    })
    logDebug(s"Check succeed")
    true
  }

  def rapidsCheckExpression(origExpr: Expression, expected: Any, inputRow: InternalRow): Unit = {
    var result : Array[Row] = null
    var resultDF : DataFrame = null
    var expression = origExpr
    var isComparedByString = false

    if(!isQualifiedForVectorizedParams(origExpr)) {
      logInfo(s"$origExpr is being evaluated with Scalar Parameter")
      println(s"$origExpr is being evaluated with Scalar Parameter")
      expression = origExpr.transformUp {
        case BoundReference(ordinal, dataType, _) =>
          Literal(inputRow.asInstanceOf[GenericInternalRow].get(ordinal, dataType), dataType)
      }
      resultDF = _spark.range(0, 1).select(Column(expression))
      try {
        result = resultDF.collect()
      } catch {
        case e: Exception if hasArithmeticCause(e) =>
          logWarning(
            s"Exception during resultDF.collect() for $expression: ${e.getMessage}", e)
          isComparedByString = true
          result = resultDF.select(Column(resultDF.columns(0)).cast("string")).collect()
      }
    } else {
      logInfo(s"$expression is being evaluated with Vectorized Parameter")
      println(s"$expression is being evaluated with Vectorized Parameter")
      val typeHintForOrdinal : Map[Int, DataType] = expression.collect {
        // In spark UT testing expressions, they typically use `val s = 's.string.at(0)`
        // to define a bound reference with type string.
        case b: BoundReference => b.ordinal -> b.dataType
      }.toMap
      val df = if (inputRow != EmptyRow && inputRow != InternalRow.empty) {
        convertInternalRowToDataFrame(inputRow, typeHintForOrdinal)
      } else {
        // create a fake useless DF
        val schema = StructType(StructField("a", IntegerType, nullable = true) :: Nil)
        val empData = Seq(Row(1))
        _spark.createDataFrame(_spark.sparkContext.parallelize(empData), schema)
      }
      resultDF = df.select(Column(expression))
      try {
        result = resultDF.collect()
      } catch {
        case e: Exception if hasArithmeticCause(e) =>
          logWarning(
            s"Exception during resultDF.collect() for $expression: ${e.getMessage}", e)
          isComparedByString = true
          result = resultDF.select(Column(resultDF.columns(0)).cast("string")).collect()
      }
    }

    TestStats.testUnitNumber = TestStats.testUnitNumber + 1
    if (
      checkDataTypeSupported(expression) &&
        expression.children.forall(checkDataTypeSupported)
    ) {
      val projectTransformer = resultDF.queryExecution.executedPlan.collect {
        case p: GpuProjectExec => p
      }
      if (projectTransformer.size == 1) {
        TestStats.offloadRapidsUnitNumber += 1
        logInfo("Offload to native backend in the test.\n")
      } else {
        logInfo("Not supported in native backend, fall back to vanilla spark in the test.\n")
        shouldNotFallback()
      }
    } else {
      logInfo("Has unsupported data type, fall back to vanilla spark.\n")
      shouldNotFallback()
    }
    if (isComparedByString) {
      val actualStr = result.head.get(0)
      val rawStr = if (expected == null) null else expected.toString
      val catalystStr = try {
        if (expected == null) {
          null
        } else {
          val tz = Option(SQLConf.get.sessionLocalTimeZone)
          val castExpr = Cast(
            Literal.create(expected, expression.dataType), StringType, tz)
          val evaluated = castExpr.eval(InternalRow.empty)
          if (evaluated == null) null else evaluated.toString
        }
      } catch {
        case _: Exception => null
      }
      val matched =
        checkResult(actualStr, rawStr, StringType, expression.nullable) ||
        checkResult(actualStr, catalystStr, StringType, expression.nullable)
      if (!matched) {
        fail(
          s"Incorrect evaluation: $expression, " +
            s"actual: $actualStr, " +
            s"catalystExpected: $catalystStr, " +
            s"rawExpected: $rawStr")
      }
    } else if (
      !(checkResult(result.head.get(0), expected, expression.dataType, expression.nullable)
        || checkResult(
        CatalystTypeConverters.createToCatalystConverter(expression.dataType)(
          result.head.get(0)
        ), // decimal precision is wrong from value
        CatalystTypeConverters.convertToCatalyst(expected),
        expression.dataType,
        expression.nullable
      ))
    ) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(
        s"Incorrect evaluation: $expression, " +
          s"actual: ${result.head.get(0)}, " +
          s"expected: $expected$input")
    }
  }

  private def hasArithmeticCause(e: Throwable): Boolean = {
    var cur: Throwable = e
    while (cur != null) {
      if (cur.isInstanceOf[ArithmeticException]) return true
      cur = cur.getCause
    }
    false
  }

  def shouldNotFallback(): Unit = {
    TestStats.offloadRapids = false
  }

  def canConvertToDataFrame(inputRow: InternalRow): Boolean = {
    if (inputRow == EmptyRow || inputRow == InternalRow.empty) {
      return true
    }
    if (!inputRow.isInstanceOf[GenericInternalRow]) {
      return false
    }
    val values = inputRow.asInstanceOf[GenericInternalRow].values
    for (value <- values) {
      value match {
        case _: MapData => return false
        case _: ArrayData => return false
        case _: InternalRow => return false
        case _ =>
      }
    }
    true
  }

  def convertInternalRowToDataFrame(
      inputRow: InternalRow, typeHintForOrdinal: Map[Int, DataType]) : DataFrame = {
    val structFileSeq = new ArrayBuffer[StructField]()
    val values = inputRow match {
      case genericInternalRow: GenericInternalRow =>
        genericInternalRow.values
      case _ => throw new UnsupportedOperationException("Unsupported InternalRow.")
    }
    values.zipWithIndex.foreach { pair => {
      if (typeHintForOrdinal.contains(pair._2)) {
        structFileSeq.append(
          StructField(s"col${pair._2}", typeHintForOrdinal(pair._2), pair._1 == null))
      } else {
        pair._1 match {
          case boolean: java.lang.Boolean =>
            structFileSeq.append(StructField(s"col${pair._2}", BooleanType, boolean == null))
          case short: java.lang.Short =>
            structFileSeq.append(StructField(s"col${pair._2}", ShortType, short == null))
          case byte: java.lang.Byte =>
            structFileSeq.append(StructField(s"col${pair._2}", ByteType, byte == null))
          case integer: java.lang.Integer =>
            structFileSeq.append(StructField(s"col${pair._2}", IntegerType, integer == null))
          case long: java.lang.Long =>
            structFileSeq.append(StructField(s"col${pair._2}", LongType, long == null))
          case float: java.lang.Float =>
            structFileSeq.append(StructField(s"col${pair._2}", FloatType, float == null))
          case double: java.lang.Double =>
            structFileSeq.append(StructField(s"col${pair._2}", DoubleType, double == null))
          case utf8String: UTF8String =>
            structFileSeq.append(StructField(s"col${pair._2}", StringType, utf8String == null))
          case byteArr: Array[Byte] =>
            structFileSeq.append(StructField(s"col${pair._2}", BinaryType, byteArr == null))
          case decimal: Decimal =>
            structFileSeq.append(
              StructField(s"col${pair._2}", DecimalType(decimal.precision, decimal.scale),
                decimal == null))
          case null =>
            structFileSeq.append(StructField(s"col${pair._2}", NullType, nullable = true))
          case unsupported@_ =>
            throw new UnsupportedOperationException(s"Unsupported type: ${unsupported.getClass}")
        }
      }
    }
    }
    val fields = structFileSeq.toSeq
    _spark.internalCreateDataFrame(
      _spark.sparkContext.parallelize(Seq(inputRow)),
      StructType(fields))
  }
}
