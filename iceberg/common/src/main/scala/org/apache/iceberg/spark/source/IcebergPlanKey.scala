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

package org.apache.iceberg.spark.source

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.perf.PlanKey
import org.apache.iceberg.expressions.{And, BoundPredicate, BoundReference, Expression,
  NamedReference, Not, Or, UnboundPredicate}

import org.apache.spark.sql.catalyst.expressions.XXH64
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

/**
 * History key of an Iceberg scan. The plan hash covers the table, the columns read and the
 * filters' operators and columns; the literals hash covers the filter values. Conjuncts and IN
 * values may come in any order.
 */
object IcebergPlanKey {

  def of(
      table: String,
      readSchema: StructType,
      filters: Seq[Expression],
      runtimeFilters: Seq[Expression]): Option[PlanKey] = {
    val pushed = filters.flatMap(conjuncts)
    val runtime = runtimeFilters.flatMap(conjuncts)
    val columns =
      unordered(readSchema.fields.map(f => hash(s"${f.name}:${f.dataType.catalogString}")))
    val plan = Seq(hash(table), columns, unordered(pushed.map(c => hash(shape(c)))),
      unordered(runtime.map(c => hash(shape(c))))).reduce(mix)
    val withValues = (pushed ++ runtime).exists(c => values(c).nonEmpty)
    val literals = if (withValues) {
      def keyed(cs: Seq[Expression]) =
        unordered(cs.map(c => hash(shape(c) + "=" + values(c).mkString(","))))
      mix(keyed(pushed), keyed(runtime))
    } else {
      PlanKey.NONE
    }
    Some(PlanKey(plan, literals))
  }

  private def conjuncts(e: Expression): Seq[Expression] = e match {
    case and: And => conjuncts(and.left()) ++ conjuncts(and.right())
    case other => Seq(other)
  }

  /** Operators and columns, without values. */
  private def shape(e: Expression): String = e match {
    case and: And => s"and(${shape(and.left())},${shape(and.right())})"
    case or: Or => s"or(${shape(or.left())},${shape(or.right())})"
    case not: Not => s"not(${shape(not.child())})"
    case p: UnboundPredicate[_] => s"${p.op()}(${column(p.term())})"
    case p: BoundPredicate[_] => s"${p.op()}(${column(p.term())})"
    case other => other.op().toString
  }

  /** The values, in a fixed order; empty for a filter without values. */
  private def values(e: Expression): Seq[String] = e match {
    case and: And => values(and.left()) ++ values(and.right())
    case or: Or => values(or.left()) ++ values(or.right())
    case not: Not => values(not.child())
    case p: UnboundPredicate[_] =>
      Option(p.literals()).map(_.asScala.map(l => text(l.value())).sorted.toSeq)
        .getOrElse(Seq.empty)
    case p: BoundPredicate[_] if p.isLiteralPredicate =>
      Seq(text(p.asLiteralPredicate().literal().value()))
    case p: BoundPredicate[_] if p.isSetPredicate =>
      p.asSetPredicate().literalSet().asScala.map(text).toSeq.sorted
    case _ => Seq.empty
  }

  private def column(term: AnyRef): String = term match {
    case ref: NamedReference[_] => ref.name()
    case ref: BoundReference[_] => ref.name()
    case other => String.valueOf(other)
  }

  private def text(v: Any): String = v match {
    case buffer: ByteBuffer =>
      val bytes = new Array[Byte](buffer.remaining())
      buffer.duplicate().get(bytes)
      bytes.map(b => f"$b%02x").mkString
    case other => String.valueOf(other)
  }

  private def hash(s: String): Long = {
    val bytes = s.getBytes(UTF_8)
    XXH64.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, 0L)
  }

  private def mix(h: Long, v: Long): Long = XXH64.hashLong(v, h)

  /** Combines hashes regardless of their order. */
  private def unordered(hashes: Seq[Long]): Long = hashes.sum
}
