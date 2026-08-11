/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.{ScalarSubquery, SparkPlan}

class ScalarSubquerySuite extends SparkQueryCompareTestSuite {

  private def checkExecPlan(plan: SparkPlan): Unit = {
    if (!plan.conf.getAllConfs(RapidsConf.SQL_ENABLED.key).toBoolean) return
    plan.find(_.expressions.exists(e => e.find(_.isInstanceOf[ScalarSubquery]).nonEmpty)) match {
      case Some(plan) =>
        throw new AssertionError(s"Assume no (cpu)ScalarSubquery, but found in $plan")
      case None =>
    }
  }

  testSparkResultsAreEqual("Uncorrelated Scalar Subquery", longsFromCSVDf,
    conf = enableCsvConf(),
    repart = 0,
    // In Spark 3.2.0+ canonicalization for ScalarSubquery was fixed. Because it is a bug fix
    // we have fixed it on our end for all versions of Spark, but the canonicalization check
    // only works if both have the fix or both don't have it.
    skipCanonicalizationCheck = !VersionUtils.isSpark320OrLater) {
    frame => {
      frame.createOrReplaceTempView("table")
      val ret = frame.sparkSession.sql(
        "SELECT longs, (SELECT max(more_longs) FROM table) FROM table")
      checkExecPlan(ret.queryExecution.executedPlan)
      ret
    }
  }
}
