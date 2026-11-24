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
{"spark":["330"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import java.nio.file.Files

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.test.{TestHive, TestHiveSingleton}
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.utils.{RapidsSQLTestsBaseTrait, RapidsTestsBaseTrait}

// TestHiveSingleton initializes its context in the superclass constructor, before
// beforeAll. Scope Spark system properties to that initialization and restore them
// immediately; the inherited Hive suites share the resulting TestHive context.
private[suites] object RapidsHiveTestSession {
  private val derbyLogProperty = "derby.stream.error.file"

  private lazy val hiveContext: SparkContext = {
    TrampolineUtil.cleanupAnyExistingSession()
    // The session cleanup does not stop a context that has no associated session.
    SparkContext.getActive.foreach(_.stop())

    // TestHive creates and manages its own warehouse directory.
    val conf = RapidsSQLTestsBaseTrait.nativeSparkConf(new SparkConf(false), "")
      .remove("spark.sql.warehouse.dir")
    val properties = conf.getAll
    val saved = properties.map { case (key, _) => key -> sys.props.get(key) }
    try {
      properties.foreach { case (key, value) => System.setProperty(key, value) }
      TestHive.sparkContext
    } finally {
      saved.foreach {
        case (key, Some(value)) => System.setProperty(key, value)
        case (key, None) => System.clearProperty(key)
      }
    }
  }

  def initialize(): SparkContext = {
    val context = hiveContext
    require(!context.isStopped, "The shared TestHive SparkContext has already stopped")
    context
  }

  def withContext[T](run: => T): T = synchronized {
    val testing = sys.props.get(IS_TESTING.key)
    val derbyLog = sys.props.get(derbyLogProperty)
    try {
      if (derbyLog.isEmpty) {
        // Derby resolves this property at startup and otherwise writes in the module root.
        val log = Files.createTempDirectory("rapids-hive-derby-").resolve("derby.log")
        System.setProperty(derbyLogProperty, log.toAbsolutePath.toString)
      }
      val context = initialize()
      try {
        run
      } finally {
        try {
          context.stop()
        } finally {
          SparkSession.clearActiveSession()
          SparkSession.clearDefaultSession()
        }
      }
    } finally {
      if (derbyLog.isEmpty) {
        System.clearProperty(derbyLogProperty)
      }
      testing match {
        case Some(value) => System.setProperty(IS_TESTING.key, value)
        case None => System.clearProperty(IS_TESTING.key)
      }
    }
  }

  def verify(spark: SparkSession): Unit = {
    val plugins = spark.sparkContext.getConf.get("spark.plugins", "").split(",").map(_.trim)
    require(plugins.contains("com.nvidia.spark.SQLPlugin"),
      "TestHive must initialize with the RAPIDS plugin")
    require(spark.conf.get("spark.rapids.sql.enabled").toBoolean,
      "The shared TestHive SparkSession must enable RAPIDS SQL")
    val listeners = spark.conf.get("spark.sql.queryExecutionListeners").split(",").map(_.trim)
    require(listeners.contains("org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback"),
      "The shared TestHive SparkSession must configure the plan capture listener")
  }
}

private[suites] trait RapidsHiveQuerySuite extends TestHiveSingleton with RapidsTestsBaseTrait {
  override def beforeAll(): Unit = {
    SparkSession.setActiveSession(spark)
    RapidsHiveTestSession.verify(spark)
    super.beforeAll()
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*) {
        val expectedAggregate = testName match {
          case "test count" => Some("GpuHashAggregateExec")
          case "udaf" => Some("HashAggregateExec")
          case "aggregators" => Some("ObjectHashAggregateExec")
          case _ => None
        }
        expectedAggregate match {
          case Some(operator) =>
            // Preserve Spark's expected rows on both backends for these representative queries.
            val enabled = spark.conf.get("spark.rapids.sql.enabled")
            try {
              spark.conf.set("spark.rapids.sql.enabled", false)
              testFun
              spark.conf.set("spark.rapids.sql.enabled", true)
              ExecutionPlanCaptureCallback.startCapture()
              try {
                testFun
                val plans = ExecutionPlanCaptureCallback.getResultsWithTimeout()
                assert(plans.exists(ExecutionPlanCaptureCallback.contains(_, operator)),
                  s"Expected $operator in executed Hive plans:\n${plans.mkString("\n")}")
                info(s"Executed Hive aggregation verified for $testName: $operator")
              } finally {
                ExecutionPlanCaptureCallback.endCapture()
              }
            } finally {
              spark.conf.set("spark.rapids.sql.enabled", enabled)
            }
          case None => testFun
        }
      }
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }
}
