/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable.HashMap

import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.internal.SQLConf

object ConfHelper {
  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  def toInteger(s: String, key: String): Integer = {
    try {
      s.trim.toInt
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be integer, but was $s")
    }
  }

  def toLong(s: String, key: String): Long = {
    try {
      s.trim.toLong
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be long, but was $s")
    }
  }

  def toDouble(s: String, key: String): Double = {
    try {
      s.trim.toDouble
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be double, but was $s")
    }
  }

  def stringToSeq(str: String): Seq[String] = {
    // Here 'split' returns a mutable array, 'toList' will convert it into a immutable list
    str.split(",").map(_.trim()).filter(_.nonEmpty).toList
  }

  def stringToSeq[T](str: String, converter: String => T): Seq[T] = {
    stringToSeq(str).map(converter)
  }

  def seqToString[T](v: Seq[T], stringConverter: T => String): String = {
    v.map(stringConverter).mkString(",")
  }

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.nonEmpty && str.head == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def makeConfAnchor(key: String, text: String = null): String = {
    val t = if (text != null) text else key
    // The anchor cannot be too long, so for now
    val a = key.replaceFirst("spark.rapids.", "")
    "<a name=\"" + s"$a" + "\"></a>" + t
  }

  def getSqlFunctionsForClass[T](exprClass: Class[T]): Option[Seq[String]] = {
    sqlFunctionsByClass.get(exprClass.getCanonicalName)
  }

  lazy val sqlFunctionsByClass: Map[String, Seq[String]] = {
    val functionsByClass = new HashMap[String, Seq[String]]
    FunctionRegistry.expressions.foreach { case (sqlFn, (expressionInfo, _)) =>
      val className = expressionInfo.getClassName
      val fnSeq = functionsByClass.getOrElse(className, Seq[String]())
      val fnCleaned = if (sqlFn != "|") {
        sqlFn
      } else {
        "\\|"
      }
      functionsByClass.update(className, fnSeq :+ s"`$fnCleaned`")
    }
    functionsByClass.mapValues(_.sorted).toMap
  }
}

abstract class ConfEntry[T](val key: String, val converter: String => T, val doc: String,
    val isInternal: Boolean, val isStartUpOnly: Boolean, val isCommonlyUsed: Boolean,
    val versionInfo: ConfVersionInfo) {

  def get(conf: Map[String, String]): T
  def get(conf: SQLConf): T
  def help(asTable: Boolean = false): Unit

  protected def effectiveVersionInfo: ConfVersionInfo = {
    if (versionInfo.sinceVersion == ConfVersionInfo.UNKNOWN_VERSION) {
      ConfVersionInfo.forKey(key)
    } else {
      versionInfo
    }
  }

  override def toString: String = key
}

class ConfEntryWithDefault[T](key: String, converter: String => T, doc: String,
    isInternal: Boolean, isStartupOnly: Boolean, isCommonlyUsed: Boolean = false,
    val defaultValue: T, versionInfo: ConfVersionInfo = ConfVersionInfo.UNKNOWN)
  extends ConfEntry[T](key, converter, doc, isInternal, isStartupOnly, isCommonlyUsed,
    versionInfo) {

  override def get(conf: Map[String, String]): T = {
    conf.get(key).map(converter).getOrElse(defaultValue)
  }

  override def get(conf: SQLConf): T = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      defaultValue
    } else {
      converter(tmp)
    }
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      val startupOnlyStr = if (isStartupOnly) "Startup" else "Runtime"
      if (asTable) {
        import ConfHelper.makeConfAnchor
        ConsoleOutput.writeLine(s"${makeConfAnchor(key)}|$doc|$defaultValue|$startupOnlyStr|" +
          s"${effectiveVersionInfo.sinceVersion}")
      } else {
        ConsoleOutput.writeLine(s"$key:")
        ConsoleOutput.writeLine(s"\t$doc")
        ConsoleOutput.writeLine(s"\tdefault $defaultValue")
        ConsoleOutput.writeLine(s"\ttype $startupOnlyStr")
        ConsoleOutput.writeLine()
      }
    }
  }
}

class OptionalConfEntry[T](key: String, val rawConverter: String => T, doc: String,
    isInternal: Boolean, isStartupOnly: Boolean, isCommonlyUsed: Boolean = false,
    versionInfo: ConfVersionInfo = ConfVersionInfo.UNKNOWN)
  extends ConfEntry[Option[T]](key, s => Some(rawConverter(s)), doc, isInternal,
  isStartupOnly, isCommonlyUsed, versionInfo) {

  override def get(conf: Map[String, String]): Option[T] = {
    conf.get(key).map(rawConverter)
  }

  override def get(conf: SQLConf): Option[T] = {
    val tmp = conf.getConfString(key, null)
    if (tmp == null) {
      None
    } else {
      Some(rawConverter(tmp))
    }
  }

  override def help(asTable: Boolean = false): Unit = {
    if (!isInternal) {
      val startupOnlyStr = if (isStartupOnly) "Startup" else "Runtime"
      if (asTable) {
        import ConfHelper.makeConfAnchor
        ConsoleOutput.writeLine(s"${makeConfAnchor(key)}|$doc|None|$startupOnlyStr|" +
          s"${effectiveVersionInfo.sinceVersion}")
      } else {
        ConsoleOutput.writeLine(s"$key:")
        ConsoleOutput.writeLine(s"\t$doc")
        ConsoleOutput.writeLine("\tNone")
        ConsoleOutput.writeLine(s"\ttype $startupOnlyStr")
        ConsoleOutput.writeLine()
      }
    }
  }
}

class TypedConfBuilder[T](
    val parent: ConfBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

  def this(parent: ConfBuilder, converter: String => T) = {
    this(parent, converter, Option(_).map(_.toString).orNull)
  }

  /** Apply a transformation to the user-provided values of the config entry. */
  def transform(fn: T => T): TypedConfBuilder[T] = {
    new TypedConfBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  /** Checks if the user-provided value for the config matches the validator. */
  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfBuilder[T] = {
    transform { v =>
      if (!validator(v)) {
        throw new IllegalArgumentException(errorMsg)
      }
      v
    }
  }

  /** Check that user-provided values for the config match a pre-defined set. */
  def checkValues(validValues: Set[T]): TypedConfBuilder[T] = {
    transform { v =>
      if (!validValues.contains(v)) {
        throw new IllegalArgumentException(
          s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, but was $v")
      }
      v
    }
  }

  def createWithDefault(value: T): ConfEntryWithDefault[T] = {
    // 'converter' will check the validity of default 'value', if it's not valid,
    // then 'converter' will throw an exception
    val transformedValue = converter(stringConverter(value))
    val ret = new ConfEntryWithDefault[T](parent.key, converter,
      parent.doc, parent.isInternal, parent.isStartupOnly, parent.isCommonlyUsed, transformedValue,
      parent.versionInfo)
    parent.register(ret)
    ret
  }

  /** Turns the config entry into a sequence of values of the underlying type. */
  def toSequence: TypedConfBuilder[Seq[T]] = {
    new TypedConfBuilder(parent, ConfHelper.stringToSeq(_, converter),
      ConfHelper.seqToString(_, stringConverter))
  }

  def createOptional: OptionalConfEntry[T] = {
    val ret = new OptionalConfEntry[T](parent.key, converter,
      parent.doc, parent.isInternal, parent.isStartupOnly, parent.isCommonlyUsed,
      parent.versionInfo)
    parent.register(ret)
    ret
  }
}

class ConfBuilder(val key: String, val register: ConfEntry[_] => Unit) {

  import ConfHelper._

  var doc: String = null
  var isInternal: Boolean = false
  var isStartupOnly: Boolean = false
  var isCommonlyUsed: Boolean = false
  var versionInfo: ConfVersionInfo = ConfVersionInfo.forKey(key)

  def doc(data: String): ConfBuilder = {
    this.doc = data
    this
  }

  def internal(): ConfBuilder = {
    this.isInternal = true
    this
  }

  def startupOnly(): ConfBuilder = {
    this.isStartupOnly = true
    this
  }

  def commonlyUsed(): ConfBuilder = {
    this.isCommonlyUsed = true
    this
  }

  def sinceVersion(version: String): ConfBuilder = {
    this.versionInfo = ConfVersionInfo(version)
    this
  }

  def booleanConf: TypedConfBuilder[Boolean] = {
    new TypedConfBuilder[Boolean](this, toBoolean(_, key))
  }

  def bytesConf(unit: ByteUnit): TypedConfBuilder[Long] = {
    new TypedConfBuilder[Long](this, byteFromString(_, unit))
  }

  def integerConf: TypedConfBuilder[Integer] = {
    new TypedConfBuilder[Integer](this, toInteger(_, key))
  }

  def longConf: TypedConfBuilder[Long] = {
    new TypedConfBuilder[Long](this, toLong(_, key))
  }

  def doubleConf: TypedConfBuilder[Double] = {
    new TypedConfBuilder(this, toDouble(_, key))
  }

  def stringConf: TypedConfBuilder[String] = {
    new TypedConfBuilder[String](this, identity[String])
  }
}
