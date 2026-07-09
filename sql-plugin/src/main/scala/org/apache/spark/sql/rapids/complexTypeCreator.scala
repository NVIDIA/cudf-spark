/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import java.util.{List => JList}

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, HostColumnVector}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuExpressionsUtils, GpuMapUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.{AutoCloseableProducingSeq, ReallyAGpuExpression}
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FUNC_ALIAS
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, Metadata, NullType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuCreateArray(children: Seq[Expression], useStringTypeWhenEmpty: Boolean)
    extends GpuExpression with ShimExpression {

  def this(children: Seq[Expression]) = {
    this(children, SQLConf.get.getConf(SQLConf.LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE))
  }

  override def foldable: Boolean = children.forall(_.foldable)

  override def stringArgs: Iterator[Any] = super.stringArgs.take(1)

  override def checkInputDataTypes(): TypeCheckResult = {
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), s"function $prettyName")
  }

  private val defaultElementType: DataType = {
    if (useStringTypeWhenEmpty) {
      StringType
    } else {
      NullType
    }
  }

  override def dataType: ArrayType = {
    ArrayType(
      TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(children.map(_.dataType))
          .getOrElse(defaultElementType),
      containsNull = children.exists(_.nullable))
  }

  override def nullable: Boolean = false

  override def prettyName: String = "array"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(new Array[ColumnVector](children.size)) { columns =>
      val numRows = batch.numRows()
      children.indices.foreach { index =>
        columns(index) =
          children(index).columnarEval(batch).getBase
      }

      val elementDType =  GpuColumnVector.getRapidsType(dataType.elementType)
      // calling makeList with a nested DType and no columns is an error, but we will never
      // hit this case, because in Spark the type of `array()` is either `ArrayType(NullType)`
      // or `ArrayType(StringType)`.
      GpuColumnVector.from(ColumnVector.makeList(numRows, elementDType, columns: _*), dataType)
    }
  }
}

case class GpuCreateMap(
      children: Seq[Expression],
      useStringTypeWhenEmpty: Boolean)
    extends GpuExpression with ShimExpression {

  private val valueIndices: Seq[Int] = children.indices.filter(_ % 2 != 0)
  private val keyIndices: Seq[Int] = children.indices.filter(_ % 2 == 0)

  lazy val keys: Seq[Expression] = keyIndices.map(children)
  lazy val values: Seq[Expression] = valueIndices.map(children)

  private val defaultElementType: DataType = {
    if (useStringTypeWhenEmpty) {
      StringType
    } else {
      NullType
    }
  }

  override lazy val hasSideEffects: Boolean =
    GpuCreateMap.exceptionOnDupKeys || super.hasSideEffects

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    if (children.isEmpty) {
      // cudf's ColumnVector.makeList cannot construct a 0-element list column
      // when the element DType is nested (STRUCT here). For map(), fall back
      // to fromLists with an explicit schema, which mirrors how literal empty
      // maps are built in literals.scala.
      val colType = GpuColumnVector.convertFrom(dataType, nullable)
      val emptyRow: JList[HostColumnVector.StructData] = java.util.Collections.emptyList()
      val rows = Seq.fill(numRows)(emptyRow)
      return GpuColumnVector.from(ColumnVector.fromLists(colType, rows: _*), dataType)
    }
    withResource(new Array[ColumnVector](children.size)) { columns =>
      children.indices.foreach { index =>
        columns(index) = children(index).columnarEval(batch).getBase
      }

      withResource(Range(0, columns.length, 2)
        .safeMap(i => ColumnView.makeStructView(columns(i), columns(i + 1)))) { structsCols =>
        withResource(ColumnVector.makeList(numRows, DType.STRUCT, structsCols: _*)) {
          listsOfStructs => GpuCreateMap.createMapFromKeysValuesAsStructs(dataType, listsOfStructs)
        }
      }
    }
  }

  override def nullable: Boolean = false

  override def foldable: Boolean = children.forall(_.foldable)

  override lazy val dataType: MapType = {
    MapType(
      keyType = TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(keys.map(_.dataType))
        .getOrElse(defaultElementType),
      valueType = TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(values.map(_.dataType))
        .getOrElse(defaultElementType),
      valueContainsNull = values.exists(_.nullable))
  }
}

object GpuCreateMap {
  def apply(children: Seq[Expression]): GpuCreateMap = {
    new GpuCreateMap(children,
      SQLConf.get.getConf(SQLConf.LEGACY_CREATE_EMPTY_COLLECTION_USING_STRING_TYPE))
  }

  // Spark 4.1+ returns an enum value instead of String, so use toString first
  def exceptionOnDupKeys: Boolean =
    SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString.toUpperCase == "EXCEPTION"

  def createMapFromKeysValuesAsStructs(dataType: MapType,
                                       listsOfKeyValueStructs : ColumnView): GpuColumnVector = {
    GpuMapUtils.assertNoNullKeys(listsOfKeyValueStructs)
    withResource(listsOfKeyValueStructs.dropListDuplicatesWithKeysValues()) { deduped =>
      if (exceptionOnDupKeys) {
        // Compare child data row count before and after removing duplicates to determine
        // if there were duplicates.
        withResource(deduped.getChildColumnView(0)) { a =>
          withResource(listsOfKeyValueStructs.getChildColumnView(0)) { b =>
            if (a.getRowCount != b.getRowCount) {
              throw GpuMapUtils.duplicateMapKeyFoundError
            }
          }
        }
      }
      GpuColumnVector.from(deduped.incRefCount(), dataType)
    }
  }
}

case class GpuCreateNamedStruct(children: Seq[Expression]) extends GpuExpression
  with ShimExpression {
  lazy val (nameExprs, valExprs) = children.grouped(2).map {
    case Seq(name, value) => (name, value)
  }.toList.unzip

  // Names will be serialized before Spark scheduling, and the returned type GpuScalar
  // from GpuLiteral.columnarEval(null) can't be serializable, which causes
  // `org.apache.spark.SparkException: Task not serializable` issue.
  //
  // And on the other hand, the calling for columnarEval(null) in the driver side is
  // dangerous for GpuExpressions, we'll have to pull it apart manually.
  private lazy val names = nameExprs.map {
    case ge: GpuExpression =>
      GpuExpressionsUtils.extractGpuLit(ge).map(_.value)
        .getOrElse(throw new IllegalStateException(s"Unexpected GPU expression $ge"))
    case e => e.eval(EmptyRow)
  }

  override def nullable: Boolean = false

  override def foldable: Boolean = valExprs.forall(_.foldable)

  override lazy val dataType: StructType = {
    val fields = names.zip(valExprs).map {
      case (name, expr) =>
        val metadata = expr match {
          case ne: NamedExpression => ne.metadata
          case _ => Metadata.empty
        }
        StructField(name.toString, expr.dataType, expr.nullable, metadata)
    }
    StructType(fields)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName expects an even number of arguments.")
    } else {
      val invalidNames = nameExprs.filterNot(e => e.foldable && e.dataType == StringType)
      if (invalidNames.nonEmpty) {
        TypeCheckResult.TypeCheckFailure(
          s"Only foldable ${StringType.catalogString} expressions are allowed to appear at odd" +
              s" position, got: ${invalidNames.mkString(",")}")
      } else if (!names.contains(null)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure("Field name should not be null")
      }
    }
  }

  // There is an alias set at `CreateStruct.create`. If there is an alias,
  // this is the struct function explicitly called by a user and we should
  // respect it in the SQL string as `struct(...)`.
  override def prettyName: String = getTagValue(FUNC_ALIAS).getOrElse("named_struct")

  override def sql: String = getTagValue(FUNC_ALIAS).map { alias =>
    val childrenSQL = children.indices.filter(_ % 2 == 1).map(children(_).sql).mkString(", ")
    s"$alias($childrenSQL)"
  }.getOrElse(super.sql)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    // The names are only used for the type. Here we really just care about the data
    withResource(new Array[ColumnVector](valExprs.size)) { columns =>
      val numRows = batch.numRows()
      valExprs.indices.foreach { index =>
        columns(index) = valExprs(index).columnarEval(batch).getBase
      }
      GpuColumnVector.from(ColumnVector.makeStruct(numRows, columns: _*), dataType)
    }
  }

}
