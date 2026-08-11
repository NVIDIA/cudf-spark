/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tests.udf.scala.{AlwaysTrueUDF, URLDecode, URLEncode}

import org.apache.spark.sql.functions.col

class ScalaUDFSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual("Scala urldecode", nullableStringsFromCsv) { frame =>
    // This is a basic smoke-test of the Scala UDF framework, not an
    // exhaustive test of the specific UDF implementation itself.
    val urldecode = frame.sparkSession.udf.register("urldecode", new URLDecode())
    frame.select(urldecode(col("strings")))
  }

  testSparkResultsAreEqual("Scala urlencode", nullableStringsFromCsv) { frame =>
    // This is a basic smoke-test of the Scala UDF framework, not an
    // exhaustive test of the specific UDF implementation itself.
    val urlencode = frame.sparkSession.udf.register("urlencode", new URLEncode())
    frame.select(urlencode(col("strings")))
  }

  testSparkResultsAreEqual("Scala always true", nullableStringsFromCsv) { frame =>
    // This is a basic smoke-test of the Scala UDF framework, not an
    // exhaustive test of the specific UDF implementation itself.
    val udf = frame.sparkSession.udf.register("alwaystrue", new AlwaysTrueUDF())
    frame.withColumn("alwaystrue", udf())
  }
}
