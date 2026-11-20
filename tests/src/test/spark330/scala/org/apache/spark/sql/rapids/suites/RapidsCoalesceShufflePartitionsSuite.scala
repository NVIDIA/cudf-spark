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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.IO_ENCRYPTION_ENABLED
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.CoalesceShufflePartitionsSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.{RapidsSQLTestsBaseTrait, RapidsTestsBaseTrait}

class RapidsCoalesceShufflePartitionsSuite
  extends CoalesceShufflePartitionsSuite with RapidsTestsBaseTrait {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }

  override def withSparkSession(
      f: SparkSession => Unit,
      targetPostShuffleInputSize: Int,
      minNumPostShufflePartitions: Option[Int],
      enableIOEncryption: Boolean = false): Unit = {
    // The upstream suite creates a fresh session for each set of AQE parameters.
    val sparkConf =
      RapidsSQLTestsBaseTrait.nativeSparkConf(new SparkConf(false), warehouse)
        .setMaster("local[*]")
        .setAppName("test")
        .set(UI_ENABLED, false)
        .set(IO_ENCRYPTION_ENABLED, enableIOEncryption)
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
        .set(SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key, "5")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.FETCH_SHUFFLE_BLOCKS_IN_BATCH.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(
          SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key,
          targetPostShuffleInputSize.toString)
    minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, numPartitions.toString)
      case None =>
        sparkConf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key, "1")
    }

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    try f(spark) finally spark.stop()
  }
}
