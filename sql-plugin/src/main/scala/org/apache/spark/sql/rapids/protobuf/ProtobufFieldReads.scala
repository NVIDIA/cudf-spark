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

package org.apache.spark.sql.rapids.protobuf

import scala.annotation.tailrec

import com.nvidia.spark.rapids.{RapidsMeta, SparkPlanMeta}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference,
  ElementAt, Expression, ExprId, GetArrayItem, GetArrayStructFields, GetStructField,
  NamedExpression}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.types.{ArrayType, StructType}

/** The fields of a decoded protobuf struct that a query reads. */
sealed trait FieldReads {
  def merge(other: FieldReads): FieldReads

  /** Reads of the named child field, or None when the query does not read it. */
  def child(name: String): Option[FieldReads]
}

object FieldReads {
  /** The value is read as a whole, so every field below it is read. */
  case object All extends FieldReads {
    override def merge(other: FieldReads): FieldReads = All
    override def child(name: String): Option[FieldReads] = Some(All)
  }

  case class Fields(children: Map[String, FieldReads]) extends FieldReads {
    override def merge(other: FieldReads): FieldReads = other match {
      case All => All
      case Fields(others) => Fields((children.keySet ++ others.keySet).map { name =>
        name -> (children.get(name) ++ others.get(name)).reduce(_ merge _)
      }.toMap)
    }
    override def child(name: String): Option[FieldReads] = children.get(name)
  }

  val Unread: FieldReads = Fields(Map.empty)

  def path(names: Seq[String]): FieldReads =
    names.foldRight[FieldReads](All)((name, reads) => Fields(Map(name -> reads)))

  /**
   * The fields read from the struct that `isDecode` matches, by the plan that evaluates the
   * expression of `meta` and by the plans above it that receive the struct.
   *
   * Reads are traced through struct field and array element accesses in projections, filters,
   * sorts and aggregates. Any other use of the struct, a plan of another kind receiving it, or
   * the struct reaching the query output reads it as a whole.
   */
  def analyze(meta: RapidsMeta[_, _, _], isDecode: Expression => Boolean): FieldReads =
    planMetaOf(meta).map(trace(_, isDecode, Unread)).getOrElse(All)

  @tailrec
  private def trace(
      planMeta: SparkPlanMeta[_],
      isSource: Expression => Boolean,
      reads: FieldReads): FieldReads = {
    val plan = planMeta.wrapped.asInstanceOf[SparkPlan]
    expressionsOf(plan) match {
      case None => All
      case Some(exprs) =>
        // Project aliases and pass-through references carry the struct to the next plan.
        val (carriers, uses) = plan match {
          case _: ProjectExec => exprs.partition {
            case Alias(child, _) => isSource(child)
            case attr: Attribute => isSource(attr)
            case _ => false
          }
          case _ => (Seq.empty, exprs)
        }
        val allReads = uses.foldLeft(reads)((r, expr) => r.merge(readsIn(expr, isSource)))
        val carried: Set[ExprId] = plan match {
          case _: ProjectExec => carriers.collect { case n: NamedExpression => n.exprId }.toSet
          case _ => plan.output.filter(isSource).map(_.exprId).toSet
        }
        if (allReads == All || carried.isEmpty) {
          allReads
        } else {
          planMeta.parent match {
            case Some(next: SparkPlanMeta[_]) =>
              val isCarried: Expression => Boolean = {
                case attr: AttributeReference => carried.contains(attr.exprId)
                case _ => false
              }
              trace(next, isCarried, allReads)
            // The struct is part of the query output.
            case _ => All
          }
        }
    }
  }

  private def planMetaOf(meta: RapidsMeta[_, _, _]): Option[SparkPlanMeta[_]] =
    meta.parent match {
      case Some(p: SparkPlanMeta[_]) => Some(p)
      case Some(p) => planMetaOf(p)
      case None => None
    }

  private def expressionsOf(plan: SparkPlan): Option[Seq[Expression]] = plan match {
    case p: ProjectExec => Some(p.projectList)
    case f: FilterExec => Some(Seq(f.condition))
    case s: SortExec => Some(s.sortOrder)
    case a: BaseAggregateExec =>
      Some(a.groupingExpressions ++ a.aggregateExpressions ++ a.resultExpressions)
    case _ => None
  }

  private def readsIn(expr: Expression, isSource: Expression => Boolean): FieldReads =
    accessPath(expr, isSource) match {
      case Some((names, indices)) =>
        indices.foldLeft(path(names))((r, index) => r.merge(readsIn(index, isSource)))
      case None =>
        expr.children.foldLeft(Unread)((r, child) => r.merge(readsIn(child, isSource)))
    }

  /**
   * The field names that `expr` reads from a source through field and element accesses, with
   * the index expressions of those accesses, or None when `expr` is not such an access.
   */
  private def accessPath(
      expr: Expression,
      isSource: Expression => Boolean): Option[(Seq[String], Seq[Expression])] = expr match {
    case _ if isSource(expr) => Some((Seq.empty, Seq.empty))
    case g: GetStructField =>
      accessPath(g.child, isSource).map { case (names, indices) =>
        (names :+ g.child.dataType.asInstanceOf[StructType](g.ordinal).name, indices)
      }
    case g: GetArrayStructFields =>
      accessPath(g.child, isSource).map { case (names, indices) =>
        (names :+ g.field.name, indices)
      }
    // An array element has the fields of the array's element type.
    case g: GetArrayItem =>
      accessPath(g.child, isSource).map { case (names, indices) =>
        (names, indices :+ g.ordinal)
      }
    case e: ElementAt if e.left.dataType.isInstanceOf[ArrayType] =>
      accessPath(e.left, isSource).map { case (names, indices) =>
        (names, indices :+ e.right)
      }
    case _ => None
  }
}
