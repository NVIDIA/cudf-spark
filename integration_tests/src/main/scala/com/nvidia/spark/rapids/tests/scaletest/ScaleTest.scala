/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests.scaletest

import java.util.concurrent._

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.NANOSECONDS

import com.nvidia.spark.rapids.tests.scaletest.Utils.stackTraceAsString
import scopt.OptionParser

import org.apache.spark.sql.SparkSession

/**
 * Entry point for Scale Test
 */
object ScaleTest {

  val STATUS_COMPLETED = "Completed"
  val STATUS_COMPLETED_WITH_TASK_FAILURES = "CompletedWithTaskFailures"
  val STATUS_TIMEOUT = "Timeout"
  val STATUS_FAILED = "Failed"

  // case class represents all input commandline arguments
  case class Config(scaleFactor: Int = 1,
      complexity: Int = 1,
      inputDir: String = "",
      outputDir: String = "",
      reportPath: String = "",
      format: String = "parquet",
      version: String = "1.0.0",
      seed: Int = 41,
      iterations: Int = 1,
      queries: Seq[String] = Seq(),
      overwrite: Boolean = false,
      timeout: Long = 600000L,
      dry: Boolean = false)

  /**
   *
   * @param query the test query
   * @param iterations number of iterations for the query to run in a line
   * @param baseOutputPath base path for the output, the final output will append a postfix for
   *                       iterations
   * @param overwrite overwrite the output or not
   * @param format parquet or orc are possible to pass in now
   * @param spark SparkSession instance
   * @return a Sequence of elapses for all iterations
   */
  private def runOneQueryForIterations(query: TestQuery,
      baseOutputPath: String,
      overwrite: Boolean,
      format: String,
      spark: SparkSession,
      idleSessionListener: IdleSessionListener): QueryMeta
  = {
    val mode = if (overwrite == true) "overwrite" else "error"
    val executionTimes = ListBuffer[Long]()
    val exceptions = new ConcurrentLinkedQueue[String]()
    val status = ListBuffer[String]()

    (1 to query.iterations).foreach(i => {
      while (idleSessionListener.isBusy()){
        // Scala Test aims for stability not performance. And the sleep time will not be calculated
        // into execution time.
        val sleepTime = 1
        println(s"There are still jobs running, waiting for $sleepTime seconds.")
        Thread.sleep(sleepTime * 1000)
      }
      val taskFailureListener = new TaskFailureListener
      try {
        val future = scala.concurrent.Future {
          spark.sparkContext.setJobGroup(query.name, s"query=${query.name},iteration=$i")
          println(s"Iteration: $i")

          spark.conf.set("spark.sql.shuffle.partitions", query.shufflePartitions)
          spark.sparkContext.addSparkListener(taskFailureListener)

          val start = System.nanoTime()
          spark.sql(query.content).write.mode(mode).format(format).save(s"${baseOutputPath}_$i")
          val end = System.nanoTime()
          val elapsed = NANOSECONDS.toMillis(end - start)
          executionTimes += elapsed

          val failureOpt = taskFailureListener.taskFailures.headOption
          val statusForIter = failureOpt.map(_ => STATUS_COMPLETED_WITH_TASK_FAILURES)
            .getOrElse(STATUS_COMPLETED)
          failureOpt.foreach(failure => exceptions.add(failure.toString))
          status.append(statusForIter)
        }
        scala.concurrent.Await.result(future,
          scala.concurrent.duration.Duration(query.timeout, TimeUnit.MILLISECONDS))
      } catch {
        case e: java.util.concurrent.TimeoutException =>
          println(s"Timeout at iteration $i")
          // use "-1" to mark timeout execution
          executionTimes += -1
          spark.sparkContext.cancelAllJobs()
          status.append(STATUS_TIMEOUT)
          exceptions.add(e.getMessage)
        case e: Exception =>
          // We don't want a query failure to fail over the whole test.
          println(s"Query failed: $query - ${e.getMessage}")
          executionTimes += -1
          status.append(STATUS_FAILED)
          exceptions.add(stackTraceAsString(e))
      } finally {
        spark.sparkContext.removeSparkListener(taskFailureListener)
      }
    })
    QueryMeta(query.name, query.content, status.toSeq,
        exceptions.asScala.toSeq, executionTimes.toSeq)
  }

  /**
   * print generated queries and its physical plan, most for debug purpose
   * @param spark spark session
   * @param queryMap query map
   */
  private def printQueries(spark: SparkSession, queryMap: Map[String, TestQuery]): Unit
  = {
    for ((queryName, query) <- queryMap) {
      println("*"*80)
      println(queryName)
      println(query.content)
      spark.sql(query.content).explain()
    }
  }

  private def runScaleTest(config: Config): Unit = {
    // Init SparkSession
    val spark = SparkSession.builder()
      .appName("Scale Test")
      .getOrCreate()
    val idleSessionListener = new IdleSessionListener()
    spark.sparkContext.addSparkListener(idleSessionListener)
    val querySpecs = new QuerySpecs(config, spark)
    querySpecs.initViews()
    val queryMap = querySpecs.getCandidateQueries
    if (config.dry) {
      printQueries(spark, queryMap)
      sys.exit(1)
    }
    var results = Seq[QueryMeta]()
    for ((queryName, query) <- queryMap) {
      val outputPath = s"${config.outputDir}/$queryName"
      println(s"Running Query: $queryName for ${query.iterations} iterations")
      println(s"${query.content}")
      // run one query for several iterations in a row
      val queryMeta = runOneQueryForIterations(query, outputPath, config.overwrite, config.format,
        spark, idleSessionListener)
      results  = results :+ queryMeta
    }
    val report = new TestReport(config, results)
    report.save()
  }

  private def initArgParser(): OptionParser[Config] = {
    val supportFormats = List("parquet", "orc")
    new OptionParser[Config]("ScaleTest") {
      head("Scale Test", "1.0.0")
      arg[Int]("<scale factor>")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("scale factor for data size")
      arg[Int]("<complexity>")
        .required()
        .action((x, c) => c.copy(complexity = x))
        .text("complexity level for processing")
      arg[String]("<format>")
        .validate(x =>
          if (supportFormats.contains(x.toLowerCase)) success
          else failure(s"Format must be one of ${supportFormats.mkString(",")}")
        )
        .action((x, c) => c.copy(format = x.toLowerCase))
        .text("output format for the data")
      arg[String]("<input directory>")
        .required()
        .action((x, c) => c.copy(inputDir = x))
        .text("input directory for table data")
      arg[String]("<output directory>")
        .required()
        .action((x, c) => c.copy(outputDir = x))
        .text("directory for query output")
      arg[String]("<path to save report file>")
        .required()
        .action((x, c) => c.copy(reportPath = x))
        .text("path to save the JSON report file that contains test results")
      opt[Int]('d', "seed")
        .optional()
        .action((x, c) => c.copy(seed = x))
        .text("seed used to generate random data columns. default is 41 if not specified")
      opt[Unit]("overwrite")
        .optional()
        .action((_, c) => c.copy(overwrite = true))
        .text("Flag argument. Whether to overwrite the existing data in the path.")
      opt[Int]("iterations")
        .optional()
        .action((x, c) => c.copy(iterations = x))
        .text("iterations to run for each query. default: 1")
      opt[String]("queries")
        .optional()
        .action((x, c) => c.copy(queries = x.split(",").map(t => t.toLowerCase())))
        .text("Specify queries to run specifically. the format must be " +
          "query names with comma separated. e.g. --tables " +
          "q1,q2,q3. If not specified, all queries will be run for `--iterations` rounds")
      opt[Long]("timeout")
        .optional()
        .action((x, c) => c.copy(timeout = x))
        .text("timeout for each query in milliseconds, default is 10 minutes(600000)")
      opt[Unit]("dry")
      .optional()
        .action((_, c) => c.copy(dry = true))
        .text("Flag argument. Only print the queries but not execute them.")
    }
  }

  def main(args: Array[String]): Unit = {
    val OParser = initArgParser()
    OParser.parse(args, Config()) match {
      case Some(config) =>
        runScaleTest(config)
      case _ =>
        sys.exit(1)
    }
  }
}
