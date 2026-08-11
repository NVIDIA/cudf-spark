/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, StringType}

object RapidsHiveErrors {
  // Lifted from org.apache.spark.sql.errors.QueryErrorsBase.
  // Converts an error class parameter to its SQL representation
  def toSQLValue(v: Any, t: DataType): String = Literal.create(v, t) match {
    case Literal(null, _) => "NULL"
    case Literal(v: Float, FloatType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else v.toString
    case l @ Literal(v: Double, DoubleType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else l.sql
    case l => l.sql
  }

  def requestedPartitionsMismatchTablePartitionsError(
      table: CatalogTable, partition: Map[String, Option[String]]): Throwable = {
    new SparkException(
      s"""
         |Requested partitioning does not match the ${table.identifier.table} table:
         |Requested partitions: ${partition.keys.mkString(",")}
         |Table partitions: ${table.partitionColumnNames.mkString(",")}
       """.stripMargin)
  }

  def cannotResolveAttributeError(name: String, outputStr: String): Throwable = {
    throw RapidsErrorUtils.cannotResolveAttributeError(name, outputStr)
  }

  def writePartitionExceedConfigSizeWhenDynamicPartitionError(
      numWrittenParts: Int,
      maxDynamicPartitions: Int,
      maxDynamicPartitionsKey: String): Throwable = {
    new SparkException(
      s"Number of dynamic partitions created is $numWrittenParts" +
        s", which is more than $maxDynamicPartitions" +
        s". To solve this try to set $maxDynamicPartitionsKey" +
        s" to at least $numWrittenParts.")
  }

  // Lifted from QueryExecutionErrors.
  def dynamicPartitionKeyNotAmongWrittenPartitionPathsError(key: String): Throwable = {
    new SparkException(
      s"Dynamic partition key ${toSQLValue(key, StringType)} is not among written partition paths.")
  }

  // Lifted from QueryExecutionErrors.
  def cannotRemovePartitionDirError(partitionPath: Path): Throwable = {
    new RuntimeException(s"Cannot remove partition directory '$partitionPath'")
  }
}