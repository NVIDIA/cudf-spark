/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import java.nio.charset.Charset

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

class RegularExpressionSuite extends SparkQueryCompareTestSuite {

  private val conf = new SparkConf()
    .set(RapidsConf.ENABLE_REGEXP.key, "true")

  test("Plan toString should not leak internal details of ternary expressions") {
    assume(isUnicodeEnabled())
    // see https://github.com/NVIDIA/spark-rapids/issues/7924 for background, but our ternary
    // operators, such as GpuRegexpExtract have additional attributes (cuDF patterns) that we
    // do not want displayed when printing a plan
    val conf = new SparkConf()
      .set(RapidsConf.ENABLE_REGEXP.key, "true")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
        "FileSourceScanExec,CollectLimitExec,DeserializeToObjectExec")
    withGpuSparkSession(spark => {
      spark.read.csv(getClass.getClassLoader.getResource("strings.csv").getPath).createTempView("t")
      val df = spark.sql("SELECT t._c0, regexp_extract(t._c0, '(.*) (.*) (.*)', 2) FROM t")
      df.collect()
      val planString = df.queryExecution.executedPlan.toString()
      val planStringWithoutAttrRefs = planString.replaceAll("#[0-9]+", "")
      assert(planStringWithoutAttrRefs.contains(
        "regexp_extract(_c0, (.*) (.*) (.*), 2) AS regexp_extract(_c0, (.*) (.*) (.*), 2)"))
    }, conf)
  }

  testGpuFallback(
    "String regexp_replace replace str columnar fall back",
    "RegExpReplace",
    nullableStringsFromCsv,
    execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference"), conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'a',strings)")
  }

  testGpuFallback("String regexp_replace null cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference", "Literal"), conf = conf) {
    frame => {
      // this test is only valid in Spark 3.0.x because the expression is NullIntolerant
      // since Spark 3.1.0 and gets replaced with a null literal instead
      val isValidTestForSparkVersion = ShimLoader.getShimVersion match {
        case SparkShimVersion(major, minor, _) => major == 3 && minor == 0
        case DatabricksShimVersion(major, minor, _, _) => major == 3 && minor == 0
        case ClouderaShimVersion(major, minor, _, _) => major == 3 && minor == 0
        case _ => true
      }
      assume(isValidTestForSparkVersion)
      frame.selectExpr("regexp_replace(strings,null,'D')")
    }
  }

  testGpuFallback("String regexp_replace input empty cpu fall back",
    "RegExpReplace",
    nullableStringsFromCsv, execsAllowedNonGpu = Seq("ProjectExec", "Alias",
      "RegExpReplace", "AttributeReference"), conf = conf) {
    frame => frame.selectExpr("regexp_replace(strings,'','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 1",
    nullableStringsFromCsv, conf = conf) { frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_replace(strings,'.*','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 2",
    nullableStringsFromCsv, conf = conf) { frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_replace(strings,'[a-z]+','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 3",
    nullableStringsFromCsv,  conf = conf) { frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_replace(strings,'foo$','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 4",
    nullableStringsFromCsv, conf = conf) { frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_replace(strings,'^foo','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 5",
    nullableStringsFromCsv, conf = conf) { frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_replace(strings,'(foo)','D')")
  }

  testSparkResultsAreEqual("String regexp_replace regex 6",
    nullableStringsFromCsv, conf = conf) { frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_replace(strings,'\\(foo\\)','D')")
  }

  // #14856 / revans2: anchored alternation with a backref. With the old (\r\n)? workaround the
  // synthetic group shifted (E) to group 2, so [$1] referenced the wrong group; once the
  // workaround is dropped (cudf #22763) (E) stays group 1 and GPU must match CPU here.
  testSparkResultsAreEqual("regexp_replace anchored alternation backref T$|(E)",
    spark => {
      import spark.implicits._
      Seq("T", "E", "TE", "ET", "xExE", "T\r\nE", "E\r\n").toDF("strings")
    }, conf = conf) { frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_replace(strings, 'T$|(E)', '[$1]')")
  }

  testSparkResultsAreEqual("String regexp_extract regex 1", extractStrings, conf = conf) {
    frame => 
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_extract(strings, '^([a-z]*)([0-9]*)([a-z]*)$', 1)")
  }

  testSparkResultsAreEqual("String regexp_extract regex 2", extractStrings, conf = conf) {
    frame => 
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_extract(strings, '^([a-z]*)([0-9]*)([a-z]*)$', 2)")
  }

  // note that regexp_extract with a literal string gets replaced with the literal result of
  // the regexp_extract call on CPU
  testSparkResultsAreEqual("String regexp_extract literal input",
    extractStrings, conf = conf) { frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_extract('abc123def', '^([a-z]*)([0-9]*)([a-z]*)$', 2)")
  }

  private def extractStrings(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq[(String)](
      (""),
      (null),
      ("abc123def"),
      ("abc\r\n12\r3\ndef"),
      ("123abc456")
    ).toDF("strings")
  }

  private def isUnicodeEnabled(): Boolean = {
    Charset.defaultCharset().name() == "UTF-8"
  }

  private def extractStringsLineEnd(session: SparkSession): DataFrame = {
    import session.implicits._
    Seq[(String)](
      ("123\n"),
      ("123\n\u0085"),
      ("123\r\n"),
      ("123\n\n"),
      ("123\u2029\n"),
      ("456\n"),
      ("456\r"),
      ("456\r123"),
      ("456\n\r"),
      ("456\n\n"),
      ("456\u2028\n"),
      ("456\u2028")
    ).toDF("strings")
  }

  testSparkResultsAreEqual("String regexp_extract end-of-line 1", extractStringsLineEnd,
    conf = conf) {
    frame =>
      assume(isUnicodeEnabled())
      frame.selectExpr("regexp_extract(strings, '(456(\r|\u2028)$|123\n$)', 0)")
  }

}
