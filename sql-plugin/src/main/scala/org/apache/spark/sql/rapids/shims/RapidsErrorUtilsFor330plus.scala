/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.shims

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}

trait RapidsErrorUtilsFor330plus {

  def divByZeroError(origin: Origin): ArithmeticException = {
    QueryExecutionErrors.divideByZeroError(origin.context)
  }

  def intervalDivByZeroError(origin: Origin): ArithmeticException = {
    divByZeroError(origin)
  }

  def divOverflowError(origin: Origin): ArithmeticException = {
    QueryExecutionErrors.overflowInIntegralDivideError(origin.context)
  }

  def arithmeticOverflowError(
      message: String,
      origin: Origin): ArithmeticException = {
    QueryExecutionErrors.arithmeticOverflowError(message, "", origin.context)
  }

  def foundDuplicateFieldInCaseInsensitiveModeError(
      requiredFieldName: String, matchedFields: String): Throwable = {
    QueryExecutionErrors.foundDuplicateFieldInCaseInsensitiveModeError(
      requiredFieldName, matchedFields)
  }

  def tableIdentifierExistsError(tableIdentifier: TableIdentifier): Throwable = {
    QueryCompilationErrors.tableIdentifierExistsError(tableIdentifier)
  }
}
