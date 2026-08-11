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

import org.apache.spark.sql.functions.{col, lit}

class UnaryOperatorsSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual("Test literal string values in select", mixedFloatDf) {
    frame => frame.select(col("floats"), lit("test"))
  }

  testSparkResultsAreEqual("Test EulerNumber", singularDoubleDf) {
    frame => frame.selectExpr("e()")
  }

  testSparkResultsAreEqual("Test pi", singularDoubleDf) {
    frame => frame.selectExpr("pi()")
  }

  testSparkResultsAreEqual("Test md5", mixedDfWithNulls) {
    frame => frame.selectExpr("md5(strings)", "md5(cast(ints as string))",
      "md5(cast(strings as binary))")
  }

  testSparkResultsAreEqual("Test md5 - longs as binary", mixedDfWithNulls,
    assumeCondition = ignoreAnsi("cast long to binary is not supported in ANSI mode")) {
    frame => frame.selectExpr("md5(cast(longs as binary))")
  }

  testSparkResultsAreEqual("Test murmur3", mixedDfWithNulls) {
    frame => frame.selectExpr("hash(longs, 1, null, 'stock string', ints, strings)")
  }
}
