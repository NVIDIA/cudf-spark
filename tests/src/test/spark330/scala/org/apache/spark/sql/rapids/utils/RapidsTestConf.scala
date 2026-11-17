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
{"spark":["330","420"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object RapidsTestConf extends Logging {
  // Match the comma-separated key=value options consumed by SparkSessionHolder.
  private def commandLineOptions: Seq[(String, String)] = {
    Option(System.getenv("SPARK_CONF")).toSeq.flatMap { options =>
      options.split(",").map(_.trim).filter(_.nonEmpty).map { option =>
        val keyValue = option.split("=", 2)
        require(keyValue.length == 2 && keyValue(0).trim.nonEmpty,
          "SPARK_CONF entries must have a nonempty key followed by '='")
        keyValue(0).trim -> keyValue(1)
      }
    }
  }

  def applyCommandLineConf(conf: SparkConf): SparkConf = {
    conf.setAll(commandLineOptions)
  }

  def verifyAnsiMode(spark: SparkSession): Unit = {
    val key = SQLConf.ANSI_ENABLED.key
    val actual = spark.conf.get(key).toBoolean
    commandLineOptions.reverse.find(_._1 == key).foreach { case (_, expected) =>
      require(actual == expected.toBoolean,
        s"Migrated UT session has $key=$actual instead of the requested ANSI mode")
    }
    logInfo(s"Migrated UT session has $key=$actual")
  }
}
