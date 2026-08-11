/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.SparkDateTimeException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType}
import org.apache.spark.unsafe.types.UTF8String

trait RapidsErrorUtils330To334Base extends RapidsErrorUtilsFor330plus with RapidsQueryErrorUtils {

  def mapKeyNotExistError(
      key: String,
      keyType: DataType,
      origin: Origin): NoSuchElementException = {
    QueryExecutionErrors.mapKeyNotExistError(key, keyType, origin.context)
  }

  def invalidArrayIndexError(index: Int, numElements: Int,
      isElementAtF: Boolean = false): ArrayIndexOutOfBoundsException = {
    if (isElementAtF) {
      QueryExecutionErrors.invalidElementAtIndexError(index, numElements)
    } else {
      QueryExecutionErrors.invalidArrayIndexError(index, numElements)
    }
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      errorContext: String = ""): ArithmeticException = {
    QueryExecutionErrors.arithmeticOverflowError(message, hint, errorContext)
  }

  def cannotChangeDecimalPrecisionError(
      value: Decimal,
      toType: DecimalType,
      context: String = ""): ArithmeticException = {
    QueryExecutionErrors.cannotChangeDecimalPrecisionError(
      value, toType.precision, toType.scale, context
    )
  }

  def overflowInIntegralDivideError(context: String = ""): ArithmeticException = {
    QueryExecutionErrors.arithmeticOverflowError(
      "Overflow in integral divide", "try_divide", context
    )
  }

  def sparkDateTimeException(infOrNan: String): SparkDateTimeException = {
    // These are the arguments required by SparkDateTimeException class to create error message.
    val errorClass = "CAST_INVALID_INPUT"
    val messageParameters = Array("DOUBLE", "TIMESTAMP", SQLConf.ANSI_ENABLED.key)
    new SparkDateTimeException(errorClass, Array(infOrNan) ++ messageParameters)
  }

  def sqlArrayIndexNotStartAtOneError(): RuntimeException = {
    new ArrayIndexOutOfBoundsException("SQL array indices start at 1")
  }

  def invalidInputSyntaxForBooleanError(s: UTF8String): RuntimeException = {
    QueryExecutionErrors.invalidInputSyntaxForBooleanError(s, "")
  }

  def invalidInputSyntaxForBooleanError(
      s: UTF8String,
      errorContext: String): RuntimeException = {
    QueryExecutionErrors.invalidInputSyntaxForBooleanError(s, errorContext)
  }

  def invalidInputInCastToDatetimeError(
      value: Any,
      from: DataType,
      to: DataType,
      errorContext: String): Throwable = {
    QueryExecutionErrors.invalidInputInCastToDatetimeError(value, from, to, errorContext)
  }
}
