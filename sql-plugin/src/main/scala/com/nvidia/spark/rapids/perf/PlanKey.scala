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

package com.nvidia.spark.rapids.perf

import java.net.URI

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, In, InSet,
  Literal, PredicateHelper, XXH64}
import org.apache.spark.sql.execution.{ExecSubqueryExpression, InSubqueryExec}
import org.apache.spark.sql.types.{DataType, Decimal, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

/**
 * What a plan node reads, as two 64-bit hashes that identify it in history.
 *
 * Equal if the hashes are equal. `source` and `describe` only serve the logs.
 *
 * @param plan the table or path, the columns read and the shape of the filters, without any
 *             literal
 * @param literals the literals that `plan` leaves out, each paired with its filter, and the
 *                 resolved key set of each dynamic partition pruning filter; `PlanKey.NONE` if
 *                 there are none
 * @param source the table name or the last segment of the path, for the logs
 * @param describe the readable text behind the hashes: its literals (true: all of it); not built
 *                 until asked for
 */
final class PlanKey(
    val plan: Long,
    val literals: Long,
    val source: String = "",
    describe: Boolean => String = _ => "") {

  /** The literals as text, capped; built on first use only. */
  lazy val literalsText: String = describe(false)

  /** The source, columns, filter shapes and literals as text, each part capped. */
  def text: String = describe(true)

  /** The hashes as 16 hex digits each, as logged. */
  def planHex: String = f"$plan%016x"

  def literalsHex: String = f"$literals%016x"

  override def equals(other: Any): Boolean = other match {
    case k: PlanKey => k.plan == plan && k.literals == literals
    case _ => false
  }

  override def hashCode: Int = java.lang.Long.hashCode(plan * 31 + literals)

  override def toString: String = s"PlanKey($planHex,$literalsHex)"
}

/**
 * Derives the `PlanKey` of a file scan.
 *
 * The history store compares keys as exact values, so one query must give the same key in every
 * run and every session. The plan is walked once, mixing what it reads into a running 64-bit hash
 * (Spark's `XXH64` over every character, number and node), without building a string.
 * Nothing that Spark assigns per run reaches it (expression ids, generated names) and nothing
 * depends on an iteration order: columns, conjuncts and the members of a set are combined with a
 * sum of their own well-mixed hashes.
 *
 * `plan` stays the same when only a literal changes, which is how a query that filters on a new
 * date each day finds the scans of the earlier days.
 */
object PlanKey extends PredicateHelper {

  def apply(plan: Long, literals: Long): PlanKey = new PlanKey(plan, literals)

  /** The literals hash of a plan without literals. */
  val NONE: Long = 0L

  /** Longest text of a part of `PlanKey.text`, and of `PlanKey.literalsText`. */
  private val MaxPartChars = 300
  private val MaxLiteralChars = 100

  /**
   * The key of a file scan.
   *
   * @param table the catalog name; the scan is identified by its root paths if there is none
   * @param roots root paths of the scan
   * @param readSchema the data columns read; partition columns are not in it
   * @param partitionFilters filters on partition columns, including dynamic pruning filters
   * @param dataFilters filters on data columns
   */
  def forFileScan(
      table: Option[String],
      roots: => Seq[Path],
      readSchema: StructType,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Option[PlanKey] = {
    sourceHash(table, roots).map { case (id, source) =>
      val columns = hashIgnoringOrder(
        readSchema.fields.iterator.map(f => columnHash(f.name, f.dataType)))
      val partition = hashFilters(partitionFilters)
      val data = hashFilters(dataFilters)
      val literals = if (partition.hasLiterals || data.hasLiterals) {
        addToHash(partition.literals, data.literals)
      } else {
        NONE
      }
      val plan = addToHash(addToHash(addToHash(id, columns), partition.shapes), data.shapes)
      new PlanKey(plan, literals, source,
        readableText(source, readSchema.fields.map(f => s"${f.name}:${f.dataType.catalogString}")
          .distinct.sorted, partitionFilters ++ dataFilters))
    }
  }

  /**
   * The hash of the catalog name, else of "path:" and the distinct roots, and the source for the
   * logs. None when there are no roots.
   */
  private def sourceHash(table: Option[String], roots: => Seq[Path]): Option[(Long, String)] =
    table.map(t => (addStringToHash(EmptyHash, t), t)).orElse {
      val paths = roots
      if (paths.isEmpty) {
        None
      } else {
        val rootsHash = hashIgnoringOrder(
          paths.iterator.map(p => addStringToHash(EmptyHash, normalizedPath(p))))
        Some((addToHash(addStringToHash(EmptyHash, "path:"), rootsHash), paths.head.getName))
      }
    }

  /** URI without user info or a trailing "/". */
  private def normalizedPath(path: Path): String = {
    val u = path.toUri
    val clean = if (u.getRawUserInfo == null) {
      u
    } else {
      new URI(u.getScheme, null, u.getHost, u.getPort, u.getPath, u.getQuery, u.getFragment)
    }
    clean.toString.stripSuffix("/")
  }

  /** The hashes of some filters, each over its distinct conjuncts. */
  private final case class FilterHashes(shapes: Long, literals: Long, hasLiterals: Boolean)

  /**
   * One conjunct at a time, so `a = 1 AND b = 2` and `b = 2 AND a = 1` agree. A literal stays with
   * its own conjunct: the literals hash sums the hash of each conjunct's shape and literals
   * together.
   */
  private def hashFilters(expressions: Seq[Expression]): FilterHashes = {
    val seen = new mutable.HashSet[Long]
    var shapes = 0L
    var literals = 0L
    var hasLiterals = false
    expressions.flatMap(splitConjunctivePredicates).foreach { e =>
      val hasher = new FilterHasher
      hasher.add(e)
      val both = addToHash(hasher.shape, hasher.literals)
      if (seen.add(both)) {
        shapes += hasher.shape
        if (hasher.hasLiterals) {
          literals += both
          hasLiterals = true
        }
      }
    }
    FilterHashes(shapes, literals, hasLiterals)
  }

  private def columnHash(name: String, dataType: DataType): Long =
    addTypeToHash(addStringToHash(EmptyHash, name), dataType)

  /** One expression: its shape, and its literals in the order of the tree. */
  private final class FilterHasher {
    var shape: Long = EmptyHash
    var literals: Long = EmptyHash
    var hasLiterals = false

    private def addLiteral(hash: Long): Unit = {
      literals = addToHash(literals, hash)
      hasLiterals = true
    }

    /** An IN list, a set and a pruning filter are alike in shape, their values are literals. */
    private def addInList(value: Expression): Unit = {
      shape = addToHash(shape, InTag)
      add(value)
    }

    def add(e: Expression): Unit = e match {
      case l: Literal =>
        shape = addToHash(shape, LiteralTag)
        addLiteral(hashOfValue(l.value))
      case s: InSubqueryExec =>
        // The key set the broadcast produced; known once the subquery has run.
        addInList(s.child)
        addLiteral(s.values().fold(Unresolved)(v => hashIgnoringOrder(v.iterator.map(hashOfValue))))
      case s: ExecSubqueryExpression =>
        shape = addToHash(shape, SubqueryTag)
        addLiteral(Try(hashOfValue(s.eval(null))).getOrElse(Unresolved))
      case i: In if i.list.forall(_.isInstanceOf[Literal]) =>
        addInList(i.value)
        addLiteral(hashIgnoringOrder(
          i.list.iterator.map(l => hashOfValue(l.asInstanceOf[Literal].value))))
      case i: InSet =>
        addInList(i.child)
        addLiteral(hashIgnoringOrder(i.hset.iterator.map(hashOfValue)))
      case a: AttributeReference =>
        shape = addStringToHash(addToHash(shape, AttributeTag), a.name)
      case _ =>
        shape = addStringToHash(addToHash(shape, e.children.length), e.getClass.getName)
        e.productIterator.foreach { // what the node holds besides its children
          case t: DataType => shape = addTypeToHash(shape, t)
          case s: String => shape = addStringToHash(shape, s)
          case v @ (_: Boolean | _: Char | _: Byte | _: Short | _: Int | _: Long) =>
            shape = addToHash(shape, hashOfValue(v))
          case _ =>
        }
        e.children.foreach(add)
    }
  }

  // Node kinds, and what stands for a set that has not been resolved.
  private val LiteralTag = 1L
  private val InTag = 2L
  private val SubqueryTag = 3L
  private val AttributeTag = 4L
  private val Unresolved = 5L
  private val NullValue = 6L

  /** The first of every hash. */
  private val EmptyHash = 0L

  /** Adds the number `v` to the running hash `h` (XXH64 of `v` seeded with `h`); order matters. */
  private def addToHash(h: Long, v: Long): Long = XXH64.hashLong(v, h)

  /** Adds the string `s` to the running hash `h`, one character at a time, then its length. */
  private def addStringToHash(h: Long, s: String): Long = {
    var x = h
    var i = 0
    while (i < s.length) {
      x = XXH64.hashInt(s.charAt(i), x)
      i += 1
    }
    addToHash(x, s.length)
  }

  /** Adds a column type to the running hash, as its full text, for example `decimal(11,2)`. */
  private def addTypeToHash(h: Long, t: DataType): Long = addStringToHash(h, t.catalogString)

  /**
   * A value on its own. Numbers, strings, small decimals and bytes are hashed directly; any other
   * value by its exact text. All integral types of the same number are alike.
   */
  private def hashOfValue(v: Any): Long = v match {
    case null => NullValue
    case s: UTF8String => XXH64.hashUTF8String(s, EmptyHash)
    case n @ (_: Byte | _: Short | _: Int | _: Long) =>
      addToHash(EmptyHash, n.asInstanceOf[Number].longValue)
    case d: Double => addToHash(EmptyHash, java.lang.Double.doubleToLongBits(d))
    case f: Float => addToHash(EmptyHash, java.lang.Float.floatToIntBits(f))
    case d: Decimal if d.precision <= Decimal.MAX_LONG_DIGITS =>
      addToHash(addToHash(EmptyHash, d.scale), d.toUnscaledLong)
    case bytes: Array[Byte] =>
      XXH64.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, EmptyHash)
    case other => addStringToHash(EmptyHash, String.valueOf(other))
  }

  /** Distinct hashes in any order: their sum. */
  private def hashIgnoringOrder(hashes: Iterator[Long]): Long = hashes.toSet.foldLeft(0L)(_ + _)

  /** The readable text of a key: only built when asked for, each part capped. */
  private def readableText(
      source: String,
      columns: => Seq[String],
      filters: Seq[Expression]): Boolean => String = all => {
    val conjuncts = filters.flatMap(splitConjunctivePredicates)
    val literals = joinCapped(conjuncts.iterator.flatMap(literalStrings),
      if (all) MaxPartChars else MaxLiteralChars)
    if (all) {
      s"$source columns=[${joinCapped(columns.iterator, MaxPartChars)}] " +
        s"filters=[${joinCapped(conjuncts.iterator.map(filterShapeText), MaxPartChars)}] " +
        s"literals=[$literals]"
    } else {
      literals
    }
  }

  /** `parts` joined by commas, up to `max` characters, then "..."; "-" if there are none. */
  private def joinCapped(parts: Iterator[String], max: Int): String = {
    val sb = new StringBuilder
    while (parts.hasNext && sb.length <= max) {
      if (sb.nonEmpty) {
        sb.append(',')
      }
      sb.append(parts.next())
    }
    if (sb.length > max) {
      sb.setLength(max)
      sb.append("...")
    }
    if (sb.isEmpty) "-" else sb.toString
  }

  private def literalStrings(e: Expression): Iterator[String] = e match {
    case l: Literal => Iterator.single(l.toString)
    case s: InSubqueryExec =>
      s.values().fold(Iterator.single("unresolved"))(_.iterator.map(String.valueOf))
    case s: ExecSubqueryExpression =>
      Iterator.single(Try(String.valueOf(s.eval(null))).getOrElse("unresolved"))
    case i: InSet => i.hset.iterator.map(String.valueOf)
    case _ => e.children.iterator.flatMap(literalStrings)
  }

  /** The `#123` or `#123L` that Spark prints after an attribute's name, which differs per run. */
  private val ExprIds = "#\\d+L?".r

  /** `e` as text without its literals: each value, and each set of values, is a `?`. */
  private def filterShapeText(e: Expression): String = ExprIds.replaceAllIn(e.transformDown {
    case _: Literal | _: ExecSubqueryExpression => Literal("?")
    case i: In if i.list.forall(_.isInstanceOf[Literal]) => In(i.value, Nil)
    case i: InSet => In(i.child, Nil)
  }.toString, "")
}
