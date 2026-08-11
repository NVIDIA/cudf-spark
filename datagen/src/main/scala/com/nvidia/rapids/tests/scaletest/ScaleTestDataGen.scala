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

package com.nvidia.rapids.tests.scaletest

import scopt.OptionParser

import org.apache.spark.sql.SparkSession

object ScaleTestDataGen{

  // case class represents all input commandline arguments
  private case class Config(scaleFactor: Int = 1,
    complexity: Int = 1,
    outputDir: String = "",
    tables: Array[String] = Array("a_facts",
      "b_data",
      "c_data",
      "d_data",
      "e_data",
      "f_facts",
      "g_data"),
    format: String = "parquet",
    version: String = "1.0.0",
    seed: Int = 41,
    overwrite: Boolean = false)

  private def runDataGen(config: Config): Unit ={
    // Init SparkSession
    val spark = SparkSession.builder()
      .appName("Scale Test Data Generation")
      .getOrCreate()

    val tableGenerator = new TableGenerator(config.scaleFactor, config.complexity, config.seed,
      spark)
    val tableMap = tableGenerator.genTables(config.tables)
    val baseOutputPath = s"${config.outputDir}/" +
      s"SCALE_" +
      s"${config.scaleFactor}_" +
      s"${config.complexity}_" +
      s"${config.format}_" +
      s"${config.version}_" +
      s"${config.seed}"
    // 512 Mb block size requirement
    // 128 Mb is the default size in Spark
    val defaultGroupSizeInSpark = 128*1024*1024
    val customGroupSize = 512*1024*1024
    val specialRowGroupSizeMap = Map("b_data" -> customGroupSize,
      "g_data" -> customGroupSize)

    for ((tableName, df) <- tableMap) {
      var writer = df.write
      if (config.overwrite) {
        // default mode is "errorifexists" by Spark if not specified.
        writer = writer.mode("overwrite")
      }
      config.format match {
        case "parquet" => writer
          .option("parquet.block.size", specialRowGroupSizeMap.getOrElse(tableName,
            defaultGroupSizeInSpark))
          .parquet(s"$baseOutputPath/$tableName")
        case "orc" => writer
          .option("orc.block.size", specialRowGroupSizeMap.getOrElse(tableName,
            defaultGroupSizeInSpark))
          .orc(s"$baseOutputPath/$tableName")
        case unsupported => throw new IllegalArgumentException(s"Unknown format: $unsupported")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val tableList = List("a_facts", "b_data", "c_data", "d_data", "e_data", "f_facts", "g_data")
    val supportFormats = List("parquet","orc")
    val OParser = new OptionParser[Config]("DataGenEntry"){
      head("Scale Test Data Generation Application", "1.0.0")
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
      arg[String]("<output directory>")
        .required()
        .action((x, c) => c.copy(outputDir = x))
        .text("output directory for data generated")
      opt[String]('t', "tables")
        .optional()
        .action((x, c) => c.copy(tables = x.split(",").map(_.toLowerCase)))
        .validate(x =>
          if (x.split(",").forall(t => tableList.contains(t))) success
          else failure(s"Invalid table name. Must be one of ${tableList.mkString(",")}")
        )
        .text("tables to generate. If not specified, all tables will be generated")
      opt[Int]('d', "seed")
        .optional()
        .action((x, c) => c.copy(seed = x))
        .text("seed used to generate random data columns. default is 41 if not specified")
      opt[Unit]("overwrite")
        .optional()
        .action((_, c) => c.copy(overwrite = true))
        .text("Flag argument. Whether to overwrite the existing data in the path.")
    }

    OParser.parse(args, Config()) match {
      case Some(config) =>
        runDataGen(config)
      case _ =>
        sys.exit(1)
    }
  }
}
