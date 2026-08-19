/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.lang.invoke.SerializedLambda

import com.nvidia.spark.RapidsUDF
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, ScalaUDF, SpecializedGetters}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, DataType, MapType, StructType}

case class GpuScalaUDF(
    function: RapidsUDF,
    dataType: DataType,
    children: Seq[Expression],
    udfName: Option[String],
    nullable: Boolean,
    udfDeterministic: Boolean) extends GpuUserDefinedFunction {
  override def toString: String = s"${name}(${children.mkString(", ")})"

  /** name of the UDF function */
  override val name: String = udfName.map(cpuName => s"gpu_$cpuName")
    .getOrElse(this.getClass.getSimpleName)
}

object GpuScalaUDFMeta {
  def exprMeta: ExprRule[ScalaUDF] = GpuOverrides.expr[ScalaUDF](
    "User Defined Function, the UDF can choose to implement a RAPIDS accelerated interface " +
        "to get better performance.",
    ExprChecks.projectOnly(
      GpuUserDefinedFunction.udfTypeSig,
      TypeSig.all,
      repeatingParamCheck =
        Some(RepeatingParamCheck("param", GpuUserDefinedFunction.udfTypeSig, TypeSig.all))),
    (expr, conf, p, r) => new ExprMeta(expr, conf, p, r) {
      lazy val opRapidsFunc = GpuScalaUDF.getRapidsUDFInstance(expr.function)

      override def tagExprForGpu(): Unit = {
        if (opRapidsFunc.isEmpty && !this.conf.isCpuBasedUDFEnabled) {
          val udfName = expr.udfName.getOrElse("UDF")
          val udfClass = expr.function.getClass
          willNotWorkOnGpu(s"neither $udfName implemented by $udfClass provides " +
              s"a GPU implementation, nor the conf `${RapidsConf.ENABLE_CPU_BASED_UDF.key}` " +
              s"is enabled")
        }
      }

      override def convertToGpuImpl(): GpuExpression = {
        // It can come here only when at least one option as below is true.
        //   1. UDF implements a RAPIDS accelerated interface.
        //   2. The conf "spark.rapids.sql.rowBasedUDF.enabled" is enabled.
        opRapidsFunc.map { rapidsFunc =>
          GpuScalaUDF(
            rapidsFunc,
            expr.dataType,
            childExprs.map(_.convertToGpu()),
            expr.udfName,
            expr.nullable,
            expr.udfDeterministic)
        }.getOrElse {
          // This `require` is just for double check.
          require(this.conf.isCpuBasedUDFEnabled)
          GpuRowBasedScalaUDF(
            expr.function,
            expr.dataType,
            childExprs.map(_.convertToGpu()),
            expr.inputEncoders,
            expr.outputEncoder,
            expr.udfName,
            expr.nullable,
            expr.udfDeterministic)
        }
      }
    })
}

case class GpuRowBasedScalaUDF(
    sparkFunc: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]],
    outputEncoder: Option[ExpressionEncoder[_]],
    udfName: Option[String],
    nullable: Boolean,
    udfDeterministic: Boolean) extends GpuRowBasedUserDefinedFunction {

  /**
   * Create the converter which converts the catalyst data type to the scala data type.
   * This converter will be used for the UDF input type conversion.
   * We use `CatalystTypeConverters` to create the converter for:
   *   - UDF which doesn't provide inputEncoders, e.g., untyped Scala UDF and Java UDF
   *   - type which isn't supported by `ExpressionEncoder`, e.g., Any
   *   - primitive types, in order to use `identity` for better performance
   * For other cases like case class, Option[T], we use `ExpressionEncoder` instead since
   * `CatalystTypeConverters` doesn't support these data types.
   *
   * @param i the index of the child
   * @param dataType the output data type of the i-th child
   * @return the converter
   */
  def createInputConverter(i: Int, dataType: DataType): Any => Any = {
    val useEncoder =
      !(inputEncoders.isEmpty || // for untyped Scala UDF and Java UDF
          inputEncoders(i).isEmpty || // for types aren't supported by encoder, e.g. Any
          inputPrimitives(i)) // for primitive types

    if (useEncoder) {
      val enc = inputEncoders(i).get
      val fromRow = enc.createDeserializer()
      if (enc.isSerializedAsStructForTopLevel) {
        row: Any => fromRow(row.asInstanceOf[InternalRow])
      } else {
        val inputRow = new GenericInternalRow(1)
        value: Any => inputRow.update(0, value); fromRow(inputRow)
      }
    } else { // use CatalystTypeConverters
      CatalystTypeConverters.createToScalaConverter(dataType)
    }
  }

  /**
   *  Need nulls check when there are array types with nulls in the input.
   *  This is for `https://github.com/NVIDIA/spark-rapids/issues/3942`.
   */
  override val checkNull: Boolean = children.exists(child => hasArrayWithNulls(child.dataType))

  private def hasArrayWithNulls(dt: DataType): Boolean = dt match {
    case ArrayType(et, hasNull) => hasNull || hasArrayWithNulls(et)
    case MapType(kt, vt, _) => hasArrayWithNulls(kt) || hasArrayWithNulls(vt)
    case StructType(fields) => fields.exists(f => hasArrayWithNulls(f.dataType))
    case _ => false
  }

  override def toString: String = s"$name(${children.mkString(", ")})"

  /** name of the UDF function */
  override val name: String = udfName.map(cpuName => s"gpu_row_based_$cpuName")
    .getOrElse(this.getClass.getSimpleName)

  /** The input `row` consists of only child columns. */
  override final protected def evaluateRow(childrenRow: InternalRow): Any =
    catalystConverter(wrappedFunc(childrenRow))

  /**
   * The analyzer should be aware of Scala primitive types so as to make the
   * UDF return null if there is any null input value of these types. On the
   * other hand, Java UDFs can only have boxed types, thus this will return
   * Nil(has same effect with all false) and analyzer will skip null-handling
   * on them.
   */
  lazy val inputPrimitives: Seq[Boolean] = {
    inputEncoders.map { encoderOpt =>
      // It's possible that some of the inputs don't have a specific encoder(e.g. `Any`)
      if (encoderOpt.isDefined) {
        val encoder = encoderOpt.get
        if (encoder.isSerializedAsStruct) {
          // struct type is not primitive
          false
        } else {
          // `nullable` is false iff the type is primitive
          !encoder.schema.head.nullable
        }
      } else {
        // Any type is not primitive
        false
      }
    }
  }

  /**
   * The expected input types of this UDF, used to perform type coercion. If we do
   * not want to perform coercion, simply use "Nil". Note that it would've been
   * better to use Option of Seq[DataType] so we can use "None" as the case for no
   * type coercion. However, that would require more refactoring of the codebase.
   */
  def inputTypes: Seq[AbstractDataType] = {
    inputEncoders.map { encoderOpt =>
      if (encoderOpt.isDefined) {
        val encoder = encoderOpt.get
        if (encoder.isSerializedAsStruct) {
          encoder.schema
        } else {
          encoder.schema.head.dataType
        }
      } else {
        AnyDataType
      }
    }
  }

  /**
   * Create the converter which converts the scala data type to the catalyst data type for
   * the return data type of udf function. We'd use `ExpressionEncoder` to create the
   * converter for typed ScalaUDF only, since its the only case where we know the type tag
   * of the return data type of udf function.
   */
  private[this] lazy val catalystConverter: Any => Any = outputEncoder.map { enc =>
    val toRow = enc.createSerializer().asInstanceOf[Any => Any]
    if (enc.isSerializedAsStructForTopLevel) {
      value: Any =>
        if (value == null) null else toRow(value).asInstanceOf[InternalRow]
    } else {
      value: Any =>
        if (value == null) null else toRow(value).asInstanceOf[InternalRow].get(0, dataType)
    }
  }.getOrElse(CatalystTypeConverters.createToCatalystConverter(dataType))

  /** A little refactor to simplify the Spark code of executing the function */

  /** Build an accessor for each child to read the data from a row and do type conversion */
  private[this] lazy val childAccessors: Array[SpecializedGetters => Any] =
    children.zipWithIndex.map { case (child, i) =>
      val accessor = InternalRow.getAccessor(child.dataType, child.nullable)
      val converter = createInputConverter(i, child.dataType)
      row: SpecializedGetters => converter(accessor(row, i))
    }.toArray

  // scalastyle:off line.size.limit
  /**
   * Spark Scala UDF supports at most 22 parameters.
   */
  private lazy val wrappedFunc: InternalRow => Any = {
    children.size match {
      case 0 =>
        val f = sparkFunc.asInstanceOf[() => Any]
        (row: InternalRow) => f()
      case 1 =>
        val f = sparkFunc.asInstanceOf[(Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          f(a0)
        }
      case 2 =>
        val f = sparkFunc.asInstanceOf[(Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          f(a0, a1)
        }
      case 3 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          f(a0, a1, a2)
        }
      case 4 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          f(a0, a1, a2, a3)
        }
      case 5 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          f(a0, a1, a2, a3, a4)
        }
      case 6 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          f(a0, a1, a2, a3, a4, a5)
        }
      case 7 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          f(a0, a1, a2, a3, a4, a5, a6)
        }
      case 8 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7)
        }
      case 9 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8)
        }
      case 10 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9)
        }
      case 11 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
        }
      case 12 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)
        }
      case 13 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12)
        }
      case 14 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13)
        }
      case 15 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          val a14 = childAccessors(14)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14)
        }
      case 16 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          val a14 = childAccessors(14)(row)
          val a15 = childAccessors(15)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15)
        }
      case 17 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          val a14 = childAccessors(14)(row)
          val a15 = childAccessors(15)(row)
          val a16 = childAccessors(16)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16)
        }
      case 18 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          val a14 = childAccessors(14)(row)
          val a15 = childAccessors(15)(row)
          val a16 = childAccessors(16)(row)
          val a17 = childAccessors(17)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17)
        }
      case 19 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          val a14 = childAccessors(14)(row)
          val a15 = childAccessors(15)(row)
          val a16 = childAccessors(16)(row)
          val a17 = childAccessors(17)(row)
          val a18 = childAccessors(18)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18)
        }
      case 20 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          val a14 = childAccessors(14)(row)
          val a15 = childAccessors(15)(row)
          val a16 = childAccessors(16)(row)
          val a17 = childAccessors(17)(row)
          val a18 = childAccessors(18)(row)
          val a19 = childAccessors(19)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19)
        }
      case 21 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          val a14 = childAccessors(14)(row)
          val a15 = childAccessors(15)(row)
          val a16 = childAccessors(16)(row)
          val a17 = childAccessors(17)(row)
          val a18 = childAccessors(18)(row)
          val a19 = childAccessors(19)(row)
          val a20 = childAccessors(20)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20)
        }
      case 22 =>
        val f = sparkFunc.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (row: InternalRow) => {
          val a0 = childAccessors(0)(row)
          val a1 = childAccessors(1)(row)
          val a2 = childAccessors(2)(row)
          val a3 = childAccessors(3)(row)
          val a4 = childAccessors(4)(row)
          val a5 = childAccessors(5)(row)
          val a6 = childAccessors(6)(row)
          val a7 = childAccessors(7)(row)
          val a8 = childAccessors(8)(row)
          val a9 = childAccessors(9)(row)
          val a10 = childAccessors(10)(row)
          val a11 = childAccessors(11)(row)
          val a12 = childAccessors(12)(row)
          val a13 = childAccessors(13)(row)
          val a14 = childAccessors(14)(row)
          val a15 = childAccessors(15)(row)
          val a16 = childAccessors(16)(row)
          val a17 = childAccessors(17)(row)
          val a18 = childAccessors(18)(row)
          val a19 = childAccessors(19)(row)
          val a20 = childAccessors(20)(row)
          val a21 = childAccessors(21)(row)
          f(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21)
        }
    }
  } // end of wrappedFunc
  // scalastyle:on line.size.limit

}

object GpuScalaUDF {
  /**
   * Determine if the UDF function implements the [[com.nvidia.spark.RapidsUDF]] interface,
   * returning the instance if it does. The lambda wrapper that Spark applies to Java UDFs will be
   * inspected if necessary to locate the user's UDF instance.
   */
  def getRapidsUDFInstance(function: AnyRef): Option[RapidsUDF] = {
    function match {
      case f: RapidsUDF => Some(f)
      case f =>
        try {
          // This may be a lambda that Spark's UDFRegistration wrapped around a Java UDF instance.
          val clazz = f.getClass
          if (TrampolineUtil.getSimpleName(clazz).toLowerCase().contains("lambda")) {
            // Try to find a `writeReplace` method, further indicating it is likely a lambda
            // instance, and invoke it to serialize the lambda. Once serialized, captured arguments
            // can be examine to locate the Java UDF instance.
            // Note this relies on implementation details of Spark's UDFRegistration class.
            val writeReplace = clazz.getDeclaredMethod("writeReplace")
            writeReplace.setAccessible(true)
            val serializedLambda = writeReplace.invoke(f).asInstanceOf[SerializedLambda]
            if (serializedLambda.getCapturedArgCount == 1) {
              serializedLambda.getCapturedArg(0) match {
                case c: RapidsUDF => Some(c)
                case _ => None
              }
            } else {
              None
            }
          } else {
            None
          }
        } catch {
          case _: ClassCastException | _: NoSuchMethodException | _: SecurityException => None
        }
    }
  }
}
