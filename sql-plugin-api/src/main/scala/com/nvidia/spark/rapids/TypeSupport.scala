/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

/**
 * The level of support that the plugin has for a given type.  Used for documentation generation.
 */
sealed abstract class SupportLevel {
  def htmlTag: String
  def text: String
}

/**
 * N/A neither spark nor the plugin supports this.
 */
object NotApplicable extends SupportLevel {
  override def htmlTag: String = "<td> </td>"
  override def text: String = "NA"
}

/**
 * Spark supports this but the plugin does not.
 */
object NotSupported extends SupportLevel {
  override def htmlTag: String = s"<td><b>$text</b></td>"
  override def text: String = "NS"
}

/**
 * Both Spark and the plugin support this.
 */
class Supported() extends SupportLevel {
  override def htmlTag: String = s"<td>$text</td>"
  override def text: String = "S"
}

/**
 * The plugin partially supports this type.
 * @param missingChildTypes child types that are not supported
 * @param needsLitWarning true if we need to warn that we only support a literal value when Spark
 *                        does not.
 * @param note any other notes we want to include about not complete support.
 */
class PartiallySupported(
    val missingChildTypes: TypeEnum.ValueSet = TypeEnum.ValueSet(),
    val needsLitWarning: Boolean = false,
    val note: Option[String] = None) extends SupportLevel {
  override def htmlTag: String = {
    val typeStr = if (missingChildTypes.isEmpty) {
      None
    } else {
      Some("unsupported child types " + missingChildTypes.mkString(", "))
    }
    val litOnly = if (needsLitWarning) {
      Some("Literal value only")
    } else {
      None
    }
    val extraInfo = (note.toSeq ++ litOnly.toSeq ++ typeStr.toSeq).mkString(";<br/>")
    val allText = s"$text<br/>$extraInfo"
    s"<td><em>$allText</em></td>"
  }

  // don't include the extra info in the supported text field for now
  // as the qualification tool doesn't use it
  override def text: String = "PS"
}

/**
 * The Supported Types. The TypeSig API should be preferred for this, except in a few cases when
 * TypeSig asks for a TypeEnum.
 */
object TypeEnum extends Enumeration {
  type TypeEnum = Value

  val BOOLEAN: Value = Value
  val BYTE: Value = Value
  val SHORT: Value = Value
  val INT: Value = Value
  val LONG: Value = Value
  val FLOAT: Value = Value
  val DOUBLE: Value = Value
  val DATE: Value = Value
  val TIMESTAMP: Value = Value
  val STRING: Value = Value
  val DECIMAL: Value = Value
  val NULL: Value = Value
  val BINARY: Value = Value
  val CALENDAR: Value = Value
  val ARRAY: Value = Value
  val MAP: Value = Value
  val STRUCT: Value = Value
  val UDT: Value = Value
  val DAYTIME: Value = Value
  val YEARMONTH: Value = Value
  val VARIANT: Value = Value
}
