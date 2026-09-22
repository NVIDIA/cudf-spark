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

import test.org.apache.spark.sql.connector.JavaPartitionAwareDataSource

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.{DataSourceV2Suite, PartitionAwareDataSource}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsDataSourceV2Suite extends DataSourceV2Suite with RapidsSQLTestsTrait {

  testRapids("partitioning reporting") {
    withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "true") {
      Seq(classOf[PartitionAwareDataSource], classOf[JavaPartitionAwareDataSource]).foreach { cls =>
        withClue(cls.getName) {
          val df = spark.read.format(cls.getName).load()
          checkAnswer(df, Seq(Row(1, 4), Row(1, 4), Row(3, 6), Row(2, 6), Row(4, 2), Row(4, 2)))

          val groupByColA = df.groupBy("i").agg(sum("j"))
          checkAnswer(groupByColA, Seq(Row(1, 8), Row(2, 6), Row(3, 6), Row(4, 4)))
          assert(collectFirst(groupByColA.queryExecution.executedPlan) {
            case e: ShuffleExchangeLike => e
          }.isEmpty)

          val groupByColAB = df.groupBy("i", "j").agg(count("*"))
          checkAnswer(groupByColAB, Seq(Row(1, 4, 2), Row(2, 6, 1), Row(3, 6, 1), Row(4, 2, 2)))
          assert(collectFirst(groupByColAB.queryExecution.executedPlan) {
            case e: ShuffleExchangeLike => e
          }.isEmpty)

          val groupByColB = df.groupBy("j").agg(sum("i"))
          checkAnswer(groupByColB, Seq(Row(2, 8), Row(4, 2), Row(6, 5)))
          assert(collectFirst(groupByColB.queryExecution.executedPlan) {
            case e: ShuffleExchangeLike => e
          }.isDefined)

          val groupByAPlusB = df.groupBy(col("i") + col("j")).agg(count("*"))
          checkAnswer(groupByAPlusB, Seq(Row(5, 2), Row(6, 2), Row(8, 1), Row(9, 1)))
          assert(collectFirst(groupByAPlusB.queryExecution.executedPlan) {
            case e: ShuffleExchangeLike => e
          }.isDefined)
        }
      }
    }
  }
}
