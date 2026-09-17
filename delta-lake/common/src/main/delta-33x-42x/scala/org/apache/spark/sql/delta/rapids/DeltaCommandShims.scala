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

package org.apache.spark.sql.delta.rapids

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Trait to abstract version-specific Spark API differences between Delta 3.3.x and Spark 4.x
 * Delta shims.
 *
 * Key API differences handled:
 * 1. SparkSession types: Spark 4.x split SparkSession to support both classic and connect
 *    execution modes. It introduced:
 *    - org.apache.spark.sql.SparkSession (SqlSparkSession): unified API interface
 *    - org.apache.spark.sql.classic.SparkSession (ClassicSparkSession): classic execution mode
 *    - org.apache.spark.sql.connect.SparkSession: Spark Connect remote execution
 *    Spark 3.x uses a single SparkSession type for all purposes.
 * 2. DataFrame creation: Spark 3.x uses Dataset.ofRows(), Spark 4.x uses
 *    TrampolineConnectShims.createDataFrame().
 * 3. Column creation: Spark 3.x uses new Column(expr), Spark 4.x uses DFUDFShims.exprToColumn().
 */
trait DeltaCommandShims {

  /**
   * Type alias for the version-specific SparkSession type used in the shimming layer.
   * This type is used when:
   * 1. Casting the SparkSession parameter inside run() method body
   * 2. As parameter type for shim methods (toOperationSparkSession, recacheByPlan)
   *
   */
  type ShimSparkSession <: SparkSession

  /**
   * Type alias for the version-specific SparkSession type used for internal operations.
   * This is the SparkSession type used within command execution for operations like:
   * - Creating DataFrames from LogicalPlans (via createDataFrame method)
   * - Utility operations (splitMetadataAndDataPredicates, createSetTransaction)
   *
   * Version mapping:
   * - Delta 3.3.x (Spark 3.x): org.apache.spark.sql.SparkSession
   * - Delta 4.x (Spark 4.0+): org.apache.spark.sql.classic.SparkSession (ClassicSparkSession)
   */
  type OperationSparkSession <: SparkSession

  /**
   * Convert ShimSparkSession to OperationSparkSession.
   * In Spark 4.x, this converts SqlSparkSession to ClassicSparkSession.
   * In Spark 3.x, Both types are SparkSession.
   */
  def toOperationSparkSession(spark: ShimSparkSession): OperationSparkSession

  /**
   * Get the active OperationSparkSession.
   */
  def getActiveOperationSparkSession: OperationSparkSession

  /**
   * Create a DataFrame from a LogicalPlan using version-specific API.
   * - Spark 3.x: Dataset.ofRows(spark, logicalPlan)
   * - Spark 4.x: TrampolineConnectShims.createDataFrame(spark, logicalPlan)
   */
  def createDataFrame(spark: OperationSparkSession, logicalPlan: LogicalPlan): DataFrame

  /**
   * Convert an Expression to a Column (version-specific API).
   */
  def exprToColumn(expr: Expression): Column

  /**
   * Recache by plan with the correct SparkSession type.
   */
  def recacheByPlan(spark: ShimSparkSession, plan: LogicalPlan): Unit
}
