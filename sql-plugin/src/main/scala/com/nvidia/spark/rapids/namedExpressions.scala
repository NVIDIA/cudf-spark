/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import java.util.Objects

import ai.rapids.cudf.ColumnVector
import ai.rapids.cudf.ast
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExprId, Generator, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuAlias(child: Expression, name: String)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Seq[String] = Seq.empty,
    val explicitMetadata: Option[Metadata] = None)
  extends GpuUnaryExpression with NamedExpression {

  // Alias(Generator, xx) need to be transformed into Generate(generator, ...)
  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && !child.isInstanceOf[Generator]

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def metadata: Metadata = {
    explicitMetadata.getOrElse {
      child match {
        case named: NamedExpression => named.metadata
        case _ => Metadata.empty
      }
    }
  }

  def newInstance(): NamedExpression =
    GpuAlias(child, name)(qualifier = qualifier, explicitMetadata = explicitMetadata)

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable, metadata)(exprId, qualifier)
    } else {
      UnresolvedAttribute(name)
    }
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix$delaySuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] =
    exprId :: qualifier :: explicitMetadata :: Nil

  override def hashCode(): Int = {
    val state = Seq(name, exprId, child, qualifier, explicitMetadata)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(other: Any): Boolean = other match {
    case a: GpuAlias =>
      name == a.name && exprId == a.exprId && child == a.child && qualifier == a.qualifier &&
        explicitMetadata == a.explicitMetadata
    case _ => false
  }

  override def sql: String = {
    if (SparkShimImpl.hasAliasQuoteFix) {
      val qualifierPrefix =
        if (qualifier.nonEmpty) qualifier.map(quoteIfNeeded).mkString(".") + "." else ""
      s"${child.sql} AS $qualifierPrefix${quoteIfNeeded(name)}"
    } else {
      val qualifierPrefix = if (qualifier.nonEmpty) qualifier.mkString(".") + "." else ""
      s"${child.sql} AS $qualifierPrefix${quoteIdentifier(name)}"
    }
  }

  private def quoteIfNeeded(part: String): String = {
    if (part.contains(".") || part.contains("`")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }

  // pass through any calls to columarEval to child
  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    child.columnarEval(batch)

  // pass through any calls to columarEvalAny to child
  override def columnarEvalAny(batch: ColumnarBatch): Any =
    child.columnarEvalAny(batch)

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("GpuAlias should never have doColumnar called")

  override def convertToAst(numLeftTableColumns: Int): ast.AstExpression = child match {
    case e: GpuExpression => e.convertToAst(numLeftTableColumns)
    case e => throw new IllegalStateException(s"Attempt to convert $e to AST")
  }
}
