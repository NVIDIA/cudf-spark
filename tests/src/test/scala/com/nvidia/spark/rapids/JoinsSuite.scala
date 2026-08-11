/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, HintInfo, Join, JoinHint}
import org.apache.spark.sql.rapids.TestTrampolineUtil
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._
import org.apache.spark.sql.types.BooleanType

class JoinsSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual2("Test broadcast hash join with ops", longsDf, nonZeroLongsDf,
    conf=new SparkConf()
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB"),
    assumeCondition = ignoreAnsi("https://github.com/NVIDIA/spark-rapids/issues/12700")) {
    (A, B) => A.join(B, (A("longs") - A("more_longs")) === (B("longs") - B("more_longs")))
  }

  // For spark to insert a shuffled hash join it has to be enabled with
  // "spark.sql.join.preferSortMergeJoin" = "false" and both sides have to
  // be larger than a broadcast hash join would want
  // "spark.sql.autoBroadcastJoinThreshold", but one side has to be smaller
  // than the number of splits * broadcast threshold and also be at least
  // 3 times smaller than the other side.  So it is not likely to happen
  // unless we can give it some help.
  lazy val shuffledJoinConf = new SparkConf()
      .set("spark.sql.autoBroadcastJoinThreshold", "160")
      .set("spark.sql.join.preferSortMergeJoin", "false")
      .set("spark.sql.shuffle.partitions", "2") // hack to try and work around bug in cudf

  IGNORE_ORDER_testSparkResultsAreEqual2("Test hash join", longsDf, biggerLongsDf,
    conf = shuffledJoinConf) {
    (A, B) => A.join(B, A("longs") === B("longs"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual2("Test hash semi join", longsDf, biggerLongsDf,
    conf = shuffledJoinConf) {
    (A, B) => A.join(B, A("longs") === B("longs"), "LeftSemi")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2("Test hash anti join", longsDf, biggerLongsDf,
    conf = shuffledJoinConf) {
    (A, B) => A.join(B, A("longs") === B("longs"), "LeftAnti")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2("Test hash right join", longsDf, biggerLongsDf,
    conf = shuffledJoinConf) {
    (A, B) => A.join(B, A("longs") === B("longs"), "Right")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2("Test hash full join", longsDf, biggerLongsDf,
    conf = shuffledJoinConf) {
    (A, B) => A.join(B, A("longs") === B("longs"), "FullOuter")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2("Test cross join", longsDf, biggerLongsDf,
    conf = shuffledJoinConf) {
    (A, B) => A.join(B.hint("broadcast"), A("longs") < B("longs"), "Cross")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2("Test cross join 2", longsDf, biggerLongsDf,
    conf = shuffledJoinConf) {
    (A, B) => A.join(B, A("longs") < B("longs"), "Cross")
  }

  // test replacement of sort merge join with hash join
  // make sure broadcast size small enough it doesn't get used
  testSparkResultsAreEqual2("Test replace sort merge join with hash join",
    longsDf, biggerLongsDf,
    conf = new SparkConf()
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.join.preferSortMergeJoin", "true")
      .set("spark.sql.shuffle.partitions", "2"),
    incompat = true,
    sort = true) {
    (A, B) => A.join(B, A("longs") === B("longs"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual2("Test left semi self join with nulls sort part",
    mixedDfWithNulls, mixedDfWithNulls, sortBeforeRepart = true) {
    (A, B) => A.join(B, A("longs") === B("longs"), "LeftSemi")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2("Test left anti self join with nulls with partition sort",
    mixedDfWithNulls, mixedDfWithNulls, sortBeforeRepart = true) {
    (A, B) => A.join(B, A("longs") === B("longs"), "LeftAnti")
  }

  for (buildRight <- Seq(false, true)) {
    for (leftEmpty <- Seq(false, true)) {
      for (rightEmpty <- Seq(false, true)) {
        def generateLeftTable(spark: SparkSession): DataFrame = {
          if (leftEmpty) {
            // Use a filter on a non-existent value to try to avoid Spark's query optimization
            // from potentially optimizing out the nested loop join by realizing at query
            // planning time that one of the dataframes is empty.
            longsDf(spark).filter("longs = 132435465768")
          } else {
            longsDf(spark)
          }
        }

        def generateRightTable(spark: SparkSession): DataFrame = {
          if (rightEmpty) {
            // Use a filter on a non-existent value to try to avoid Spark's query optimization
            // from potentially optimizing out the nested loop join by realizing at query
            // planning time that one of the dataframes is empty.
            biggerLongsDf(spark).filter("longs = 132435465768")
          } else {
            biggerLongsDf(spark)
          }
        }

        IGNORE_ORDER_testSparkResultsAreEqual2(
          "Test unconditional nested loop existence join " +
            s"buildRight=$buildRight leftEmpty=$leftEmpty rightEmpty=$rightEmpty",
          generateLeftTable,
          generateRightTable) {
          (df1, df2) => {
            val joinHint = if (buildRight) {
              JoinHint(None, Some(HintInfo(Some(BROADCAST))))
            } else {
              JoinHint(Some(HintInfo(Some(BROADCAST))), None)
            }
            val cpuPlan = Join(
              TestTrampolineUtil.toLogicalPlan(df1),
              TestTrampolineUtil.toLogicalPlan(df2),
              ExistenceJoin(AttributeReference("exists", BooleanType, false)()),
              None,
              joinHint)
            TestTrampolineUtil.toDataFrame(df1.sparkSession, cpuPlan)
          }
        }
      }
    }
  }
}
