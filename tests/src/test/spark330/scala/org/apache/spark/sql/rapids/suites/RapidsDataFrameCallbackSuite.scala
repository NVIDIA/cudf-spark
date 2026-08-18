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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait
import org.apache.spark.sql.util.{DataFrameCallbackSuite, QueryExecutionListener}

class RapidsDataFrameCallbackSuite extends DataFrameCallbackSuite with RapidsSQLTestsTrait {
  import testImplicits._
  import org.apache.spark.sql.functions._

  testRapids("get numRows metrics by callback") {
    val metrics = ArrayBuffer.empty[Long]
    val listener = new QueryExecutionListener {
      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        val plan = stripAQEPlan(qe.executedPlan)
        val outputRows = plan.collectFirst {
          case node if node.metrics.contains("numOutputRows") =>
            node.metrics("numOutputRows").value
        }.getOrElse(fail(s"${plan.nodeName} has no numOutputRows metric"))
        metrics += outputRows
      }
    }
    spark.listenerManager.register(listener)

    try {
      val df = Seq(1 -> "a").toDF("i", "j").groupBy("i").count()

      df.collect()
      sparkContext.listenerBus.waitUntilEmpty()
      df.collect()
      Seq(1 -> "a", 1 -> "b").toDF("i", "j").groupBy("i").count().collect()

      sparkContext.listenerBus.waitUntilEmpty()
      assert(metrics === Seq(1L, 1L, 1L))
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  testRapids("execute callback functions for DataFrameWriter") {
    val commands = ArrayBuffer.empty[(String, LogicalPlan)]
    val exceptions = ArrayBuffer.empty[(String, Exception)]
    val listener = new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        exceptions += funcName -> exception
      }

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        commands += funcName -> qe.logical
      }
    }
    spark.listenerManager.register(listener)

    try {
      withTempPath { path =>
        val commandStart = commands.length
        spark.range(10).write.format("json").save(path.getCanonicalPath)
        sparkContext.listenerBus.waitUntilEmpty()
        val operationCommands = commands.drop(commandStart)
        assert(operationCommands.count {
          case ("command", cmd: InsertIntoHadoopFsRelationCommand) =>
            cmd.fileFormat.isInstanceOf[JsonFileFormat]
          case _ => false
        } === 1)
      }

      withTable("tab") {
        val commandStart = commands.length
        sql("CREATE TABLE tab(i long) using parquet")
        spark.range(10).write.insertInto("tab")
        sparkContext.listenerBus.waitUntilEmpty()
        val operationCommands = commands.drop(commandStart)
        assert(operationCommands.count {
          case ("command", cmd: InsertIntoHadoopFsRelationCommand) =>
            cmd.catalogTable.exists(_.identifier.identifier == "tab")
          case _ => false
        } === 1)
      }

      sparkContext.listenerBus.waitUntilEmpty()
      withTable("tab") {
        val commandStart = commands.length
        spark.range(10).select($"id", $"id" % 5 as "p")
          .write.partitionBy("p").saveAsTable("tab")
        sparkContext.listenerBus.waitUntilEmpty()
        val operationCommands = commands.drop(commandStart)
        assert(operationCommands.count {
          case ("command", cmd: CreateDataSourceTableAsSelectCommand) =>
            cmd.table.partitionColumnNames == Seq("p")
          case _ => false
        } === 1)
      }

      withTable("tab") {
        sql("CREATE TABLE tab(i long) using parquet")
        spark.udf.register("illegalUdf", udf((value: Long) => value / 0))
        val error = intercept[SparkException] {
          spark.range(10).selectExpr("illegalUdf(id)").write.insertInto("tab")
        }
        sparkContext.listenerBus.waitUntilEmpty()
        assert(exceptions === Seq("command" -> error))
      }
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
