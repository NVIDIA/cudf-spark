/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.avro.NameValidator
import org.apache.avro.Schema

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
object TrampolineConnectShims {

  type SparkSession = org.apache.spark.sql.classic.SparkSession
  type DataFrame = org.apache.spark.sql.classic.DataFrame
  type Dataset = org.apache.spark.sql.classic.Dataset[org.apache.spark.sql.Row]

  def cleanupAnyExistingSession(): Unit = {
    org.apache.spark.sql.classic.SparkSession.cleanupAnyExistingSession()
  }

  def createDataFrame(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    org.apache.spark.sql.classic.Dataset.ofRows(spark, plan)
  }

  def getBuilder(): org.apache.spark.sql.classic.SparkSession.Builder = {
    org.apache.spark.sql.classic.SparkSession.builder()
  }

  def hasActiveSession: Boolean = {
    org.apache.spark.sql.classic.SparkSession.getActiveSession.isDefined
  }

  def getActiveSession: SparkSession = {
    org.apache.spark.sql.classic.SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("No active SparkSession found")
    )
  }

  def createSchemaParser(): Schema.Parser = {
    // Spark-4.0+ depends on Avro-1.12.0 where validate() is removed and we need to use
    // NameValidator interface instead of validate() method.
    new Schema.Parser(NameValidator.NO_VALIDATION).setValidateDefaults(false)
  }
}
