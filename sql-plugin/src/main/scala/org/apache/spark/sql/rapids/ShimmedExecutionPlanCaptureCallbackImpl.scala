/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.{ArrayBuffer, Map => MutableMap}
import scala.util.matching.Regex

import com.nvidia.spark.rapids.{GpuCpuBridgeExpression, PlanShims, PlanUtils, ShimLoaderTemp}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{ExecSubqueryExpression, QueryExecution, ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec


/**
 * Note that the name is prefixed with "Shimmed" such that wildcard rules
 * under unshimmed-common-from-single-shim.txt don't get confused and pick this class to be
 * un-shimmed.
 */
class ShimmedExecutionPlanCaptureCallbackImpl extends ExecutionPlanCaptureCallbackBase {
  private[this] var shouldCapture: Boolean = false
  private[this] val execPlans: ArrayBuffer[SparkPlan] = ArrayBuffer.empty

  override def captureIfNeeded(qe: QueryExecution): Unit = synchronized {
    if (shouldCapture) {
      execPlans.append(qe.executedPlan)
    }
  }

  // Avoiding the use of default arguments since this is called from Python
  override def startCapture(): Unit = startCapture(10000)

  override def startCapture(timeoutMillis: Long): Unit = {
    SparkSession.getActiveSession.foreach { spark =>
      spark.sparkContext.listenerBus.waitUntilEmpty(timeoutMillis)
    }
    synchronized {
      execPlans.clear()
      shouldCapture = true
    }
  }

  override def endCapture(): Unit = endCapture(10000)

  override def endCapture(timeoutMillis: Long): Unit = synchronized {
    if (shouldCapture) {
      shouldCapture = false
      execPlans.clear()
    }
  }

  override def getResultsWithTimeout(timeoutMs: Long = 10000): Array[SparkPlan] = {
    try {
      val spark = SparkSession.active
      spark.sparkContext.listenerBus.waitUntilEmpty(timeoutMs)
      synchronized {
        execPlans.toArray
      }
    } finally {
      synchronized {
        shouldCapture = false
        execPlans.clear()
      }
    }
  }

  override def extractExecutedPlan(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: AdaptiveSparkPlanExec => p.executedPlan
      case p => PlanShims.extractExecutedPlan(p)
    }
  }

  override def assertCapturedAndGpuFellBack(
      // used by python code, should not be Array[String]
      fallbackCpuClassList: java.util.ArrayList[String],
      timeoutMs: Long): Unit = {
    val gpuPlans = getResultsWithTimeout(timeoutMs = timeoutMs)
    assert(gpuPlans.nonEmpty, "Did not capture a plan")
    fallbackCpuClassList.foreach(fallbackCpuClass => assertDidFallBack(gpuPlans, fallbackCpuClass))
  }

  /**
   * This method is used by the Python integration tests.
   * The method checks the schemata used in the GPU and CPU executed plans and compares it to the
   * expected schemata to make sure we are not reading more data than needed
   */
  override def assertSchemataMatch(
      cpuDf: DataFrame, gpuDf: DataFrame, expectedSchema: String): Unit = {
    import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
    import org.apache.spark.sql.execution.FileSourceScanExec
    import org.apache.spark.sql.types.StructType

    val adaptiveSparkPlanHelper = ShimLoaderTemp.newAdaptiveSparkPlanHelperShim()
    val cpuFileSourceScanSchemata =
      adaptiveSparkPlanHelper.collect(cpuDf.queryExecution.executedPlan) {
      case scan: FileSourceScanExec => scan.requiredSchema
    }
    val gpuFileSourceScanSchemata =
      adaptiveSparkPlanHelper.collect(gpuDf.queryExecution.executedPlan) {
      case scan: GpuFileSourceScanExec => scan.requiredSchema
    }
    assert(cpuFileSourceScanSchemata.size == gpuFileSourceScanSchemata.size,
      s"Found ${cpuFileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected ${gpuFileSourceScanSchemata.size}")

    cpuFileSourceScanSchemata.zip(gpuFileSourceScanSchemata).foreach {
      case (cpuScanSchema, gpuScanSchema) =>
         cpuScanSchema match {
           case otherType: StructType =>
             assert(gpuScanSchema.sameType(otherType))
             val expectedStructType = CatalystSqlParser.parseDataType(expectedSchema)
             assert(gpuScanSchema.sameType(expectedStructType),
               s"Type GPU schema ${gpuScanSchema.toDDL} doesn't match $expectedSchema")
             assert(cpuScanSchema.sameType(expectedStructType),
               s"Type CPU schema ${cpuScanSchema.toDDL} doesn't match $expectedSchema")
           case otherType => assert(false, s"The expected type $cpuScanSchema" +
             s" doesn't match the actual type $otherType")
         }
    }
  }

  override def assertCapturedAndGpuFellBack(
      fallbackCpuClass: String, timeoutMs: Long = 2000): Unit = {
    val gpuPlans = getResultsWithTimeout(timeoutMs = timeoutMs)
    assert(gpuPlans.nonEmpty, "Did not capture a plan")
    assertDidFallBack(gpuPlans, fallbackCpuClass)
  }

  override def assertDidFallBack(gpuPlans: Array[SparkPlan], fallbackCpuClass: String): Unit = {
    // Verify at least one of the plans has the fallback class
    assert(gpuPlans.exists(didFallBack(_, fallbackCpuClass)),
      s"Could not find $fallbackCpuClass in the GPU plans:\n" +
          gpuPlans.map(extractExecutedPlan).mkString("\n"))
  }

  override def assertDidFallBack(gpuPlan: SparkPlan, fallbackCpuClass: String): Unit = {
    assert(didFallBack(gpuPlan, fallbackCpuClass),
      s"Could not find $fallbackCpuClass in the GPU plan\n${extractExecutedPlan(gpuPlan)}")
  }

  override def assertDidFallBack(df: DataFrame, fallbackCpuClass: String): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    assertDidFallBack(Array(executedPlan), fallbackCpuClass)
  }

  override def assertContains(gpuPlan: SparkPlan, className: String): Unit = {
    assert(containsPlan(gpuPlan, className),
      s"Could not find $className in the Spark plan\n$gpuPlan")
  }

  override def assertContains(df: DataFrame, gpuClass: String): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    assertContains(executedPlan, gpuClass)
  }

  override def assertNotContain(gpuPlan: SparkPlan, className: String): Unit = {
    assert(!containsPlan(gpuPlan, className),
      s"We found $className in the Spark plan\n$gpuPlan")
  }

  override def assertNotContain(df: DataFrame, gpuClass: String): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    assertNotContain(executedPlan, gpuClass)
  }

  override def assertContainsAnsiCast(df: DataFrame): Unit = {
    val executedPlan = extractExecutedPlan(df.queryExecution.executedPlan)
    assert(containsPlanMatching(executedPlan,
      _.expressions.exists(PlanShims.isAnsiCastOptionallyAliased)),
        "Plan does not contain an ansi cast")
  }

  private def didFallBack(exp: Expression, fallbackCpuClass: String): Boolean = {
    exp match {
      case bridge: GpuCpuBridgeExpression =>
        // Check if the CPU expression inside the bridge matches the fallback class
        didFallBackInCpuExpression(bridge.cpuExpression, fallbackCpuClass)
      case _ =>
        !exp.getClass.getCanonicalName.equals("com.nvidia.spark.rapids.GpuExpression") &&
            PlanUtils.getBaseNameFromClass(exp.getClass.getName) == fallbackCpuClass ||
            exp.children.exists(didFallBack(_, fallbackCpuClass))
    }
  }

  private def didFallBackInCpuExpression(exp: Expression, fallbackCpuClass: String): Boolean = {
    PlanUtils.getBaseNameFromClass(exp.getClass.getName) == fallbackCpuClass ||
        exp.children.exists(didFallBackInCpuExpression(_, fallbackCpuClass))
  }

  override def didFallBack(plan: SparkPlan, fallbackCpuClass: String): Boolean = {
    // Recurse through AQE wrappers explicitly. extractExecutedPlan only unwraps a single
    // node, and AdaptiveSparkPlanExec/QueryStageExec are leaves for tree traversal, so a
    // plain SparkPlan.find cannot descend into the plan they wrap (e.g. a Databricks Delta
    // write command nested inside a ResultQueryStageExec).
    extractExecutedPlan(plan) match {
      case p: AdaptiveSparkPlanExec => didFallBack(p.executedPlan, fallbackCpuClass)
      case p: QueryStageExec => didFallBack(p.plan, fallbackCpuClass)
      case node =>
        (!node.getClass.getCanonicalName.equals("com.nvidia.spark.rapids.GpuExec") &&
            PlanUtils.sameClass(node, fallbackCpuClass)) ||
            node.expressions.exists(didFallBack(_, fallbackCpuClass)) ||
            node.children.exists(didFallBack(_, fallbackCpuClass))
    }
  }

  override def contains(gpuPlan: SparkPlan, className: String): Boolean = {
    containsPlan(gpuPlan, className)
  }

  private def containsExpression(exp: Expression, className: String,
      regexMap: MutableMap[String, Regex] // regex memoization
  ): Boolean = exp.find {
    case e if PlanUtils.getBaseNameFromClass(e.getClass.getName) == className => true
    case bridge: GpuCpuBridgeExpression => 
      containsCpuExpression(bridge.cpuExpression, className)
    case e: ExecSubqueryExpression => containsPlan(e.plan, className, regexMap)
    case _ => false
  }.nonEmpty

  private def containsCpuExpression(exp: Expression, className: String): Boolean = {
    exp.find { e =>
      PlanUtils.getBaseNameFromClass(e.getClass.getName) == className
    }.nonEmpty
  }

  private def containsPlan(plan: SparkPlan, className: String,
      regexMap: MutableMap[String, Regex] = MutableMap.empty // regex memoization
  ): Boolean = plan.find {
    case p if PlanUtils.sameClass(p, className) =>
      true
    case p: AdaptiveSparkPlanExec =>
      containsPlan(p.executedPlan, className, regexMap)
    case p: QueryStageExec =>
      containsPlan(p.plan, className, regexMap)
    case p: ReusedSubqueryExec =>
      containsPlan(p.child, className, regexMap)
    case p: ReusedExchangeExec =>
      containsPlan(p.child, className, regexMap)
    case p if p.expressions.exists(containsExpression(_, className, regexMap)) =>
      true
    case p: SparkPlan =>
      val sparkPlanStringForRegex = p.verboseStringWithSuffix(1000)
      regexMap.getOrElseUpdate(className, className.r)
          .findFirstIn(sparkPlanStringForRegex)
          .nonEmpty
  }.nonEmpty

  private def containsPlanMatching(plan: SparkPlan, f: SparkPlan => Boolean): Boolean = plan.find {
    case p if f(p) =>
      true
    case p: AdaptiveSparkPlanExec =>
      containsPlanMatching(p.executedPlan, f)
    case p: QueryStageExec =>
      containsPlanMatching(p.plan, f)
    case p: ReusedSubqueryExec =>
      containsPlanMatching(p.child, f)
    case p: ReusedExchangeExec =>
      containsPlanMatching(p.child, f)
    case p =>
      p.children.exists(plan => containsPlanMatching(plan, f))
  }.nonEmpty
}

