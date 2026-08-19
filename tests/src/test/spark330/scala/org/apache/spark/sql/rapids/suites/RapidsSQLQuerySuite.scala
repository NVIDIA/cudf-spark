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

import java.io.File

import com.nvidia.spark.rapids.GpuDataWritingCommandExec
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLQuerySuite}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuInsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

/**
 * RAPIDS GPU tests for SQL query operations.
 *
 * This test suite validates SQL query execution on GPU.
 * It extends the original Spark SQLQuerySuite to ensure GPU implementation
 * produces the same results as CPU.
 *
 * Original Spark test:
 *  sql/core/src/test/scala/org/apache/spark/sql/SQLQuerySuite.scala
 * Test count: 228 tests
 *
 * Migration notes:
 * - SQLQuerySuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper,
 *   so we use RapidsSQLTestsTrait
 * - This is a comprehensive SQL query test suite covering:
 *   - Basic SQL operations (SELECT, WHERE, GROUP BY, ORDER BY, etc.)
 *   - Aggregations and window functions
 *   - Joins (inner, outer, cross, etc.)
 *   - Subqueries and CTEs
 *   - Type casting and conversions
 *   - NULL handling
 *   - ANSI SQL compliance
 *   - Various SQL functions and expressions
 *   - Query optimization and execution
 */
class RapidsSQLQuerySuite extends SQLQuerySuite with RapidsSQLTestsTrait {
  // All 228 tests from SQLQuerySuite will be inherited and run on GPU
  // The checkAnswer method is overridden in RapidsSQLTestsTrait to execute on GPU
  // GPU-specific SQL configuration is handled by RapidsSQLTestsTrait

  // GPU-specific test for "run sql directly on files"
  // Original test: SQLQuerySuite.scala lines 1666-1714
  testRapids("run sql directly on files") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.json(f.getCanonicalPath)
      checkAnswer(sql(s"select id from json.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select id from `org.apache.spark.sql.json`.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select a.id from json.`${f.getCanonicalPath}` as a"),
        df)
    })

    // Test invalid table name
    var e = intercept[AnalysisException] {
      sql("select * from in_valid_table")
    }
    assert(e.message.contains("Table or view not found"))

    e = intercept[AnalysisException] {
      sql("select * from no_db.no_table").show()
    }
    assert(e.message.contains("Table or view not found"))

    e = intercept[AnalysisException] {
      sql("select * from json.invalid_file")
    }
    assert(e.message.contains("Path does not exist"))

    // GPU-specific adjustment: in our UT running, there'll be a spark-hive jar in the CLASSPATH
    // So when DataSource.lookupDataSource is called, it'll return the OrcFileFormat class
    // successfully, then never report the exception with "Hive built-in ORC data source must
    // be used with Hive support". Modify the assert to expect "Path does not exist"
    e = intercept[AnalysisException] {
      sql(s"select id from `org.apache.spark.sql.hive.orc`.`file_path`")
    }
    assert(e.message.contains("Path does not exist"))

    e = intercept[AnalysisException] {
      sql(s"select id from `org.apache.spark.sql.sources.HadoopFsRelationProvider`.`file_path`")
    }
    assert(e.message.contains("Table or view not found: " +
      "`org.apache.spark.sql.sources.HadoopFsRelationProvider`.file_path"))

    e = intercept[AnalysisException] {
      sql(s"select id from `Jdbc`.`file_path`")
    }
    assert(e.message.contains("Unsupported data source type for direct query on files: Jdbc"))

    e = intercept[AnalysisException] {
      sql(s"select id from `org.apache.spark.sql.execution.datasources.jdbc`.`file_path`")
    }
    assert(e.message.contains("Unsupported data source type for direct query on files: " +
      "org.apache.spark.sql.execution.datasources.jdbc"))
  }

  // GPU-specific test for "SPARK-31594: Do not display the seed of rand/randn
  // with no argument in output schema"
  // Original test: SQLQuerySuite.scala lines 3484-3505
  // adjust the regex expression to match the projectExplainOutput
  // like "gpurand(-8183248517984607901, false)"
  testRapids("SPARK-31594: Do not display the seed of rand/randn with no argument in" +
    " output schema") {
    def checkIfSeedExistsInExplain(df: DataFrame): Unit = {
      val output = new java.io.ByteArrayOutputStream()
      Console.withOut(output) {
        df.explain()
      }
      val projectExplainOutput = output.toString.split("\n").find(_.contains("Project")).get
      assert(projectExplainOutput.matches(""".*randn?\(-?[0-9]+,\s*(?:true|false)\).*"""))
    }
    val df1 = sql("SELECT rand()")
    assert(df1.schema.head.name === "rand()")
    checkIfSeedExistsInExplain(df1)
    val df2 = sql("SELECT rand(1L)")
    assert(df2.schema.head.name === "rand(1)")
    checkIfSeedExistsInExplain(df2)
    // GPU doesn't support randn now. See https://github.com/NVIDIA/spark-rapids/issues/11613 
    // val df3 = sql("SELECT randn()")
    // assert(df3.schema.head.name === "randn()")
    // checkIfSeedExistsInExplain(df3)
    // val df4 = sql("SELECT randn(1L)")
    // assert(df4.schema.head.name === "randn(1)")
    // checkIfSeedExistsInExplain(df4)
  }

  // GPU-specific test for "SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class"
  // Original test: SQLQuerySuite.scala lines 3844-3874
  // Fix: Use testFile() to access Spark test resources instead of getContextClassLoader
  testRapids("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class") {
    val sumFuncClass = "org.apache.spark.examples.sql.Spark33084"
    val functionName = "test_udf"
    withTempDir { dir =>
      System.setProperty("ivy.home", dir.getAbsolutePath)
      // Use testFile() to access Spark test resource
      val sourceJar = new File(testFile("SPARK-33084.jar"))
      val targetCacheJarDir = new File(dir.getAbsolutePath +
        "/local/org.apache.spark/SPARK-33084/1.0/jars/")
      if (!targetCacheJarDir.exists()) {
        assert(targetCacheJarDir.mkdirs(),
          s"Failed to create directory: ${targetCacheJarDir.getAbsolutePath}")
      }
      // copy jar to local cache
      FileUtils.copyFileToDirectory(sourceJar, targetCacheJarDir)
      withTempView("v1") {
        withUserDefinedFunction(
          s"default.$functionName" -> false,
          functionName -> true) {
          // create temporary function without class
          val e = intercept[AnalysisException] {
            sql(s"CREATE TEMPORARY FUNCTION $functionName AS '$sumFuncClass'")
          }.getMessage
          assert(e.contains("Can not load class 'org.apache.spark.examples.sql.Spark33084"))
          sql("ADD JAR ivy://org.apache.spark:SPARK-33084:1.0")
          sql(s"CREATE TEMPORARY FUNCTION $functionName AS '$sumFuncClass'")
          // create a view using a function in 'default' database
          sql(s"CREATE TEMPORARY VIEW v1 AS SELECT $functionName(col1) FROM VALUES (1), (2), (3)")
          // view v1 should still using function defined in `default` database
          checkAnswer(sql("SELECT * FROM v1"), Seq(Row(2.0)))
        }
      }
    }
  }

  // GPU-specific test for "SPARK-36093: RemoveRedundantAliases should not change expression's name"
  // Original test: SQLQuerySuite.scala lines 4233-4264
  testRapids("SPARK-36093: RemoveRedundantAliases should not change expression's name") {
    withTable("t1", "t2") {
      withView("t1_v") {
        sql("CREATE TABLE t1(cal_dt DATE) USING PARQUET")
        sql(
          """
            |INSERT INTO t1 VALUES
            |(date'2021-06-27'),
            |(date'2021-06-28'),
            |(date'2021-06-29'),
            |(date'2021-06-30')""".stripMargin)
        sql("CREATE VIEW t1_v AS SELECT * FROM t1")
        sql(
          """
            |CREATE TABLE t2(FLAG INT, CAL_DT DATE)
            |USING PARQUET
            |PARTITIONED BY (CAL_DT)""".stripMargin)
        val insert = sql(
          """
            |INSERT INTO t2 SELECT 2 AS FLAG,CAL_DT FROM t1_v
            |WHERE CAL_DT BETWEEN '2021-06-29' AND '2021-06-30'""".stripMargin)

        // GPU-specific: Access CommandResultExec's innerChildren to get the actual command plan
        import org.apache.spark.sql.execution.CommandResultExec
        import scala.collection.mutable.ArrayBuffer
        
        val insertCmds = new ArrayBuffer[GpuInsertIntoHadoopFsRelationCommand]()
        
        def findInsertCmd(plan: Any): Unit = plan match {
          case CommandResultExec(_, commandPlan, _) =>
            // CommandResultExec stores the actual command in commandPhysicalPlan (innerChildren)
            findInsertCmd(commandPlan)
          case exec: GpuDataWritingCommandExec =>
            exec.cmd match {
              case cmd: GpuInsertIntoHadoopFsRelationCommand =>
                insertCmds += cmd
              case _ =>
            }
            findInsertCmd(exec.child)
          case node: org.apache.spark.sql.execution.SparkPlan =>
            // Also check innerChildren for nodes like CommandResultExec
            node.innerChildren.foreach {
              case plan: org.apache.spark.sql.execution.SparkPlan => findInsertCmd(plan)
              case _ =>
            }
            node.children.foreach(findInsertCmd)
          case _ =>
        }
        
        findInsertCmd(insert.queryExecution.executedPlan)
        
        if (insertCmds.nonEmpty) {
          insertCmds.head.partitionColumns.map(_.name).foreach(name => assert(name == "CAL_DT"))
        } else {
          fail("Could not find GpuInsertIntoHadoopFsRelationCommand in GPU plan; " +
            "this indicates an unexpected change in the GPU execution plan structure")
        }
        checkAnswer(sql("SELECT FLAG, CAST(CAL_DT as STRING) FROM t2 "),
            Row(2, "2021-06-29") :: Row(2, "2021-06-30") :: Nil)
        checkAnswer(sql("SHOW PARTITIONS t2"),
            Row("CAL_DT=2021-06-29") :: Row("CAL_DT=2021-06-30") :: Nil)
      }
    }
  }

  // Original: SQLQuerySuite.scala lines 4103-4124
  // Uses V1 parquet (default) instead of V2 to avoid DSv2
  // computeStats-before-pushdown guard (see #14422).
  // Disables AQE and broadcast so the static ReuseExchange
  // rule fires and we can assert ReusedExchangeExec.
  testRapids(
    "SPARK-33482: Fix FileScan canonicalization") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTempPath { path =>
        spark.range(5).toDF().write
          .mode("overwrite").parquet(path.toString)
        withTempView("t") {
          spark.read.parquet(path.toString)
            .createOrReplaceTempView("t")
          val df = sql(
            """SELECT *
              |FROM t AS t1
              |JOIN t AS t2 ON t2.id = t1.id
              |JOIN t AS t3 ON t3.id = t2.id
            """.stripMargin)
          df.collect()
          val reused = collect(
            df.queryExecution.executedPlan) {
            case r: ReusedExchangeExec => r
          }
          assert(reused.nonEmpty,
            "Expected ReusedExchangeExec for " +
              "3 identical scan canonicalization, " +
              "but found none.\nPlan:\n" +
              df.queryExecution.executedPlan)
        }
      }
    }
  }

}
