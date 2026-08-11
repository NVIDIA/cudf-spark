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

package org.apache.spark.sql.hive.rapids

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{GpuExpression, GpuLiteral, GpuRowBasedUserDefinedFunction, GpuScalar}
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry, UDF}
import org.apache.hadoop.hive.ql.udf.{UDFType => HiveUDFType}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.serde2.objectinspector.{ConstantObjectInspector, ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SpecializedGetters}
import org.apache.spark.sql.hive.DeferredObjectAdapter
import org.apache.spark.sql.hive.HiveShim.HiveFunctionWrapper
import org.apache.spark.sql.hive.rapids.shims.{GpuRowBasedHiveGenericUDFShim, HiveInspectorsShim}
import org.apache.spark.sql.types.DataType

/** Common implementation across row-based Hive UDFs */
trait GpuRowBasedHiveUDFBase extends GpuRowBasedUserDefinedFunction with HiveInspectorsShim {
  val funcWrapper: HiveFunctionWrapper

  @transient
  val function: AnyRef

  override val udfDeterministic: Boolean = {
    val udfType = function.getClass.getAnnotation(classOf[HiveUDFType])
    udfType != null && udfType.deterministic() && !udfType.stateful()
  }

  override final val checkNull: Boolean = false

  override def nullable: Boolean = true

  override def toString: String = {
    s"$nodeName#${funcWrapper.functionClassName}(${children.mkString(",")})"
  }

  override def prettyName: String = name

  /** Create an object inspector from a GPU expression. */
  protected def gpuToInspector(expr: Expression): ObjectInspector = expr match {
    case GpuLiteral(value, dataType) =>
      // Convert GpuLiterals to CPU Literals as possible as we can for primitive types, to
      // leverage the Spark `toInspector(Expression)` method as much as possible.
      // Because the `toInspector(Expression)` method will take care of the CPU Literal
      // especially, converting it to a ConstantObjectInspector when it is primitive type. A
      // `ConstantObjectInspector` can accelerate the row data reading by caching the actual
      // value and skipping the null check which becomes unnecessary.
      value match {
        case scalar: ai.rapids.cudf.Scalar =>
          if (scalar.getType.isNestedType) {
            // Nested type, so create an inspector from the data type.
            toInspector(dataType)
          } else {
            try {
              toInspector(Literal.create(GpuScalar.extract(scalar), dataType))
            } catch {
              // Unsupported type for extraction, so use the data type way instead.
              case _: UnsupportedOperationException => toInspector(dataType)
            }
          }
        case _ => toInspector(Literal.create(value, dataType))
      }
    case ge: GpuExpression if ge.foldable =>
      // Create an inspector from the data type instead, to avoid evaluation on the driver side,
      // which will be triggered inside the `toInspector(Expression)` method for a foldable
      // expression. Because GPU expressions should not be evaluated on the driver side.
      toInspector(ge.dataType)
    case _ =>
      // For other expressions, it is safe to call `toInspector(Expression)`, which will call into
      // `toInspector(DataType)` directly for now.
      toInspector(expr)
  }

  @transient
  protected lazy val childRowAccessors: Array[SpecializedGetters => Any] =
    children.zipWithIndex.map { case (child, i) =>
      val accessor = InternalRow.getAccessor(child.dataType, child.nullable)
      row: SpecializedGetters => accessor(row, i)
    }.toArray

  @transient
  protected lazy val argumentInspectors = children.map(gpuToInspector)
}

/** Row-based version of Spark's `HiveSimpleUDF` running in a GPU operation */
case class GpuRowBasedHiveSimpleUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression]) extends GpuRowBasedHiveUDFBase {

  @scala.annotation.nowarn("msg=class UDF in package exec is deprecated")
  @transient
  override lazy val function: UDF = funcWrapper.createFunction[UDF]()

  @transient
  private lazy val wrappers = children.map(x => wrapperFor(gpuToInspector(x), x.dataType)).toArray

  @transient
  private lazy val cached: Array[AnyRef] = new Array[AnyRef](children.length)

  @transient
  private lazy val inputDataTypes: Array[DataType] = children.map(_.dataType).toArray

  @transient
  private lazy val method =
    function.getResolver.getEvalMethod(children.map(_.dataType.toTypeInfo).asJava)

  // Create parameter converters
  @transient
  private lazy val conversionHelper = new ConversionHelper(method, argumentInspectors.toArray)

  @transient
  private lazy val unwrapper = unwrapperFor(
    ObjectInspectorFactory.getReflectionObjectInspector(
      method.getGenericReturnType, ObjectInspectorOptions.JAVA))

  override protected def evaluateRow(childrenRow: InternalRow): Any = {
    val inputs = wrapRow(childRowAccessors.map(_(childrenRow)), wrappers, cached, inputDataTypes)
    val ret = FunctionRegistry.invoke(
      method,
      function,
      conversionHelper.convertIfNecessary(inputs : _*): _*)
    unwrapper(ret)
  }

  override lazy val dataType: DataType = javaTypeToDataType(method.getGenericReturnType)

  override def foldable: Boolean = udfDeterministic && children.forall(_.foldable)

  override def sql: String = s"$name(${children.map(_.sql).mkString(", ")})"
}

/** Row-based version of Spark's `HiveGenericUDF` running in a GPU operation */
case class GpuRowBasedHiveGenericUDF(
    name: String,
    funcWrapper: HiveFunctionWrapper,
    children: Seq[Expression]) extends GpuRowBasedHiveUDFBase {

  @transient
  override lazy val function: GenericUDF = funcWrapper.createFunction[GenericUDF]()

  @transient
  private lazy val returnInspector =
    function.initializeAndFoldConstants(argumentInspectors.toArray)

  @transient
  private lazy val deferredObjects = argumentInspectors.zip(children).map {
    case (inspect, child) => new DeferredObjectAdapter(inspect, child.dataType)
  }.toArray

  @transient
  private lazy val unwrapper = unwrapperFor(returnInspector)

  override protected def evaluateRow(childrenRow: InternalRow): Any = {
    returnInspector // Make sure initialized.

    var i = 0
    val length = children.length
    while (i < length) {
      val idx = i
      GpuRowBasedHiveGenericUDFShim.setDeferredObject(
        deferredObjects(i), childRowAccessors, idx, childrenRow)
      i += 1
    }
    unwrapper(function.evaluate(deferredObjects.asInstanceOf[Array[DeferredObject]]))
  }

  override lazy val dataType: DataType = inspectorToDataType(returnInspector)

  override def foldable: Boolean =
    udfDeterministic && returnInspector.isInstanceOf[ConstantObjectInspector]
}
