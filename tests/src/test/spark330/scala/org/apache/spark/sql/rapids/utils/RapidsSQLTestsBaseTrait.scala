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
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import java.util.{Locale, TimeZone}

import com.nvidia.spark.rapids.TestUtils
import org.apache.hadoop.fs.FileUtil
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.utils.RapidsTestConstants.RAPIDS_TEST
import org.apache.spark.sql.test.SharedSparkSession


/** Basic trait for Rapids SQL test cases. */
trait RapidsSQLTestsBaseTrait extends SharedSparkSession with RapidsTestsBaseTrait {
  protected override def beforeAll(): Unit = {
    TestUtils.clearCachedBatchSerializer()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = try {
    // SparkFunSuite will set this to true, and forget to reset to false
    System.clearProperty(IS_TESTING.key)
    super.afterAll()
  } finally {
    TestUtils.clearCachedBatchSerializer()
  }

  override protected def testFile(fileName: String): String = {
    import RapidsSQLTestsBaseTrait.sparkTestResourcesDir

    java.nio.file.Paths.get(sparkTestResourcesDir(getClass).toString, fileName)
      .toString
  }

  protected def testRapids(testName: String, testTag: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    test(RAPIDS_TEST + testName, testTag: _*)(testFun)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }

  override def sparkConf: SparkConf = {
    RapidsSQLTestsBaseTrait.nativeSparkConf(super.sparkConf, warehouse)
  }

  /**
   * Get all the children plan of plans.
   *
   * @param plans
   * : the input plans.
   * @return
   */
  private def getChildrenPlan(plans: Seq[SparkPlan]): Seq[SparkPlan] = {
    if (plans.isEmpty) {
      return Seq()
    }

    val inputPlans: Seq[SparkPlan] = plans.map {
      case stage: ShuffleQueryStageExec => stage.plan
      case plan => plan
    }

    var newChildren: Seq[SparkPlan] = Seq()
    inputPlans.foreach {
      plan =>
        newChildren = newChildren ++ getChildrenPlan(plan.children)
        // To avoid duplication of WholeStageCodegenXXX and its children.
        if (!plan.nodeName.startsWith("WholeStageCodegen")) {
          newChildren = newChildren :+ plan
        }
    }
    newChildren
  }

  /**
   * Get the executed plan of a data frame.
   *
   * @param df
   * : dataframe.
   * @return
   * A sequence of executed plans.
   */
  def getExecutedPlan(df: DataFrame): Seq[SparkPlan] = {
    df.queryExecution.executedPlan match {
      case exec: AdaptiveSparkPlanExec =>
        getChildrenPlan(Seq(exec.executedPlan))
      case plan =>
        getChildrenPlan(Seq(plan))
    }
  }
}

object RapidsSQLTestsBaseTrait extends Logging {
  private val resourceMap = scala.collection.mutable.Map.empty[String, java.nio.file.Path]
  private val testJarUrlRegex = raw"jar:file:(/.*-tests.jar)!.*".r
  TrampolineUtil.addShutdownHook(10000, () => {
    resourceMap.valuesIterator.foreach { dirPath =>
      logWarning(s"Deleting expanded test jar dir $dirPath")
      FileUtil.fullyDelete(dirPath.toFile)
    }
  })

  private def expandJar(jarPath: String): java.nio.file.Path = {
    val jarFile = new java.io.File(jarPath)
    val destDir = java.nio.file.Files.createTempDirectory(jarFile.getName + ".expanded")
    logWarning(s"Registering $destDir for deletion on exit")
    FileUtil.unZip(jarFile, destDir.toFile)
    destDir
  }

  def sparkTestResourcesDir(testClass: Class[_]): java.nio.file.Path = {
    var sparkTestClass = testClass
    while (sparkTestClass.getName.contains("rapids")) {
      sparkTestClass = sparkTestClass.getSuperclass
    }
    val sparkTestClassResource = "/" + sparkTestClass.getName.replace(".", "/") + ".class"
    val resourceURL = sparkTestClass.getResource(sparkTestClassResource).toString
    val resourceJar = resourceURL match {
      case testJarUrlRegex(testJarPath) => testJarPath
      case _ => sys.error(s"Could not extract tests jar path from $resourceURL")
    }
    this.synchronized {
      resourceMap.getOrElseUpdate(resourceJar, expandJar(resourceJar))
    }
  }

  def nativeSparkConf(origin: SparkConf, warehouse: String): SparkConf = {
    // Add Locale setting
    Locale.setDefault(Locale.US)
    // Spark use "America/Los_Angeles" as default timezone in tests
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))

    val conf = origin
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.sql.queryExecutionListeners",
        "org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.sql.cache.serializer", "com.nvidia.spark.ParquetCachedBatchSerializer")
      .set("spark.rapids.sql.explain", "ALL")
      // uncomment below config to run `strict mode`, where fallback to CPU is treated as fail
      // .set("spark.rapids.sql.test.enabled", "true")
      // .set("spark.rapids.sql.test.allowedNonGpu",
      // "SerializeFromObjectExec,DeserializeToObjectExec,ExternalRDDScanExec")
      .set("spark.rapids.sql.csv.read.decimal.enabled", "true")
      .set("spark.rapids.sql.format.avro.enabled", "true")
      .set("spark.rapids.sql.format.avro.read.enabled", "true")
      .set("spark.rapids.sql.format.hive.text.write.enabled", "true")
      .set("spark.rapids.sql.format.json.enabled", "true")
      .set("spark.rapids.sql.format.json.read.enabled", "true")
      .set("spark.rapids.sql.incompatibleDateFormats.enabled", "true")
      .set("spark.rapids.sql.python.gpu.enabled", "true")
      .set("spark.rapids.sql.rowBasedUDF.enabled", "true")
      .set("spark.rapids.sql.window.collectList.enabled", "true")
      .set("spark.rapids.sql.window.collectSet.enabled", "true")
      .set("spark.rapids.sql.window.range.byte.enabled", "true")
      .set("spark.rapids.sql.window.range.short.enabled", "true")
      .set("spark.rapids.sql.expression.Ascii", "true")
      .set("spark.rapids.sql.expression.Conv", "true")
      .set("spark.rapids.sql.expression.GetJsonObject", "true")
      .set("spark.rapids.sql.expression.JsonToStructs", "true")
      .set("spark.rapids.sql.expression.StructsToJson", "true")
      .set("spark.rapids.sql.exec.CollectLimitExec", "true")
      .set("spark.rapids.sql.exec.FlatMapCoGroupsInPandasExec", "true")
      .set("spark.rapids.sql.exec.WindowInPandasExec", "true")
      .set("spark.rapids.sql.hasExtendedYearValues", "false")
      .setAppName("rapids spark plugin running Vanilla Spark UT")

    conf
  }
}
