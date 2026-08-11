/*
 * Copyright (c) 2019-2025 NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.TestUtils.{findOperator, getFinalPlan}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.rapids.execution.GpuBroadcastHashJoinExec

/** Test plan modifications to add optimizing sorts after hash joins in the plan */
class HashSortOptimizeSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir {
  private def buildDataFrame1(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("a", "b", "c")
  }

  private def buildDataFrame2(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      (1, 12),
      (5, 14),
      (7, 17)
    ).toDF("x", "y")
  }

  private val sparkConf = new SparkConf()
      .set(RapidsConf.SQL_ENABLED.key, "true")
      .set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "true")

  /**
   * Find the first GPU optimize sort in the plan and verify it has been inserted after the
   * specified join node.
   **/
  private def validateOptimizeSort(queryPlan: SparkPlan, joinNode: SparkPlan): Unit = {
    val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(queryPlan)
    val sortNode = findOperator(executedPlan, _.isInstanceOf[GpuSortExec])
    assert(sortNode.isDefined, "No sort node found")
    val gse = sortNode.get.asInstanceOf[GpuSortExec]
    assert(gse.children.length == 1)
    assert(!gse.global)
    assert(gse.sortType == SortEachBatch)
    val sortChild = gse.children.head
    assertResult(joinNode) { sortChild }
  }

  test("should not insert sort after broadcast hash join") {
    SparkSessionHolder.withSparkSession(sparkConf, { spark =>
      val df1 = buildDataFrame1(spark)
      val df2 = buildDataFrame2(spark)
      val rdf = df1.join(df2, df1("a") === df2("x"))
      val plan = rdf.queryExecution.executedPlan
      // execute the plan so that the final adaptive plan is available when AQE is on
      rdf.collect()
      val joinNode = findOperator(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
      assert(joinNode.isDefined, "No broadcast join node found")
      // should not have sort, because of not have GpuDataWritingCommandExec
      val sortNode = findOperator(plan, _.isInstanceOf[GpuSortExec])
      assert(sortNode.isEmpty)
    })
  }

  test("should not insert sort after shuffled hash join") {
    val conf = sparkConf.clone().set("spark.sql.autoBroadcastJoinThreshold", "0")
    SparkSessionHolder.withSparkSession(conf, { spark =>
      val df1 = buildDataFrame1(spark)
      val df2 = buildDataFrame2(spark)
      val rdf = df1.join(df2, df1("a") === df2("x"))
      val plan = rdf.queryExecution.executedPlan
      // execute the plan so that the final adaptive plan is available when AQE is on
      rdf.collect()
      val joinNode = findOperator(plan, _.isInstanceOf[GpuShuffledSymmetricHashJoinExec])
      assert(joinNode.isDefined, "No shuffled hash join node found")
      // should not have sort, because of not have GpuDataWritingCommandExec
      val sortNode = findOperator(plan, _.isInstanceOf[GpuSortExec])
      assert(sortNode.isEmpty)
    })
  }

  test("config to disable") {
    val conf = sparkConf.clone().set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "false")
    SparkSessionHolder.withSparkSession(conf, { spark =>
      val df1 = buildDataFrame1(spark)
      val df2 = buildDataFrame2(spark)
      val rdf = df1.join(df2, df1("a") === df2("x"))
      val plan = rdf.queryExecution.executedPlan
      val sortNode = findOperator(plan, _.isInstanceOf[GpuSortExec])
      assert(sortNode.isEmpty)
    })
  }

  test("sort not inserted if there is already ordering") {
    SparkSessionHolder.withSparkSession(sparkConf, { spark =>
      val df1 = buildDataFrame1(spark)
      val df2 = buildDataFrame2(spark)
      val rdf = df1.join(df2, df1("a") === df2("x")).orderBy(df1("a"))
      val plan = rdf.queryExecution.executedPlan
      // Get the final executed plan when AQE is either enabled or disabled.
      val finalPlan = getFinalPlan(plan)

      val numSorts = finalPlan.map {
        case _: SortExec | _: GpuSortExec => 1
        case _ => 0
      }.sum
      assertResult(1) {
        numSorts
      }
      val sort = findOperator(plan, _.isInstanceOf[GpuSortExec])
      if (sort.isDefined) {
        assertResult(true) {
          sort.get.asInstanceOf[GpuSortExec].global
        }
      }
    })
  }

  test("should insert sort for GpuDataWritingCommandExec") {
    val conf = sparkConf.clone()

    withGpuSparkSession(spark => {
      buildDataFrame1(spark).createOrReplaceTempView("t1")
      val parquetTableName = "t3"
      createParquetTable(spark, parquetTableName)

      val df = spark.sql(s"INSERT INTO TABLE $parquetTableName SELECT a FROM t1 group by a")
      df.collect()

      // write should be on GPU
      val sortExec = TestUtils.findOperator(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuSortExec])
      assert(sortExec.isDefined)
    }, conf)
  }

  test("should not insert sort because of missing GpuDataWritingCommandExec") {
    val conf = sparkConf.clone()
    withGpuSparkSession(spark => {
      buildDataFrame1(spark).createOrReplaceTempView("t1")
      val df = spark.sql("select a+1, count(*) from " +
        "(SELECT a FROM t1 group by a) t " +
        "group by a+1")
      df.collect()

      val sortExec = findOperator(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuSortExec])
      assert(sortExec.isEmpty)
    }, conf)
  }

  test("should insert sort node for GpuDataWritingCommandExec") {
    val conf = sparkConf.clone()

    withGpuSparkSession(spark => {
      buildDataFrame1(spark).createOrReplaceTempView("t1")
      buildDataFrame2(spark).createOrReplaceTempView("t2")

      val parquetTableName = "t3"
      createParquetTable(spark, parquetTableName)

      val df = spark.sql(s"INSERT INTO TABLE $parquetTableName " +
        s"SELECT a FROM t1 join t2 on t1.a = t2.x")
      df.collect()

      val plan = df.queryExecution.executedPlan
      val sortExec = findOperator(plan, _.isInstanceOf[GpuSortExec])
      assert(sortExec.isDefined)

      val joinNode = findOperator(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
      validateOptimizeSort(plan, joinNode.get)
    }, conf)
  }

  def createParquetTable(spark: SparkSession, parquetTableName:String): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $parquetTableName (a INT) USING parquet")
      .collect()
  }
}
