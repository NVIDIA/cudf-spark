/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.mortgage

import org.apache.spark.sql.SparkSession

case class ETLArgs(perfPath: String, acqPath: String, output: String)

object Benchmark {
  def etlArgs(input: Array[String]): ETLArgs =
    ETLArgs(input(0), input(1), input(2))

  def session: SparkSession = {
    val builder = SparkSession.builder.appName("MortgageJob")

    val master = System.getenv("SPARK_MASTER")
    if (master != null) {
      builder.master(master)
    }

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    spark.sqlContext.clearCache()

    spark
  }
}

object ETL {
  def main(args: Array[String]): Unit = {
    val jobArgs = Benchmark.etlArgs(args)

    val session = Benchmark.session

    Run.csv(session, jobArgs.perfPath, jobArgs.acqPath)
      .write.mode("overwrite").csv(jobArgs.output)
  }
}

object CsvToParquet {
  def main(args: Array[String]): Unit = {
    val jobArgs = Benchmark.etlArgs(args)

    val session = Benchmark.session

    ReadPerformanceCsv(session, jobArgs.perfPath)
      .write.mode("overwrite").parquet(jobArgs.output + "/perf")

    ReadAcquisitionCsv(session, jobArgs.acqPath)
      .write.mode("overwrite").parquet(jobArgs.output + "/acq")
  }
}

object AggregatesCsv {
  def main(args: Array[String]): Unit = {
    val jobArgs = Benchmark.etlArgs(args)

    val session = Benchmark.session

    SimpleAggregates.csv(session, jobArgs.perfPath, jobArgs.acqPath)
      .write.mode("overwrite").parquet(jobArgs.output)
  }
}

object AggregatesWithPercentilesCsv {
  def main(args: Array[String]): Unit = {
    val jobArgs = Benchmark.etlArgs(args)

    val session = Benchmark.session

    AggregatesWithPercentiles.csv(session, jobArgs.perfPath)
      .write.mode("overwrite").parquet(jobArgs.output)
  }
}

object AggregatesWithJoinCsv {
  def main(args: Array[String]): Unit = {
    val jobArgs = Benchmark.etlArgs(args)

    val session = Benchmark.session

    AggregatesWithJoin.csv(session, jobArgs.perfPath, jobArgs.acqPath)
      .write.mode("overwrite").parquet(jobArgs.output)
  }
}
