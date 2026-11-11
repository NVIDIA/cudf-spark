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

package com.nvidia.spark.rapids

import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.{DataFrame, SparkSession}

class TimestampConditionalSuite extends SparkQueryCompareTestSuite {
  private def conditional(function: String, kind: String): String = kind match {
    case "IF" => s"IF(p, $function(a), $function(b))"
    case "CASE WHEN" =>
      s"CASE WHEN p THEN $function(a) WHEN NOT p THEN $function(b) ELSE $function(c) END"
  }

  private def longInputs(spark: SparkSession, multiplier: Long): DataFrame = {
    import spark.implicits._
    val max = Long.MaxValue / multiplier
    val min = Long.MinValue / multiplier
    val positiveOverflow = Some(max + 1)
    val negativeOverflow = Some(min - 1)
    Seq[(Option[Boolean], Option[Long], Option[Long], Option[Long])](
      (Some(true), Some(1L), positiveOverflow, negativeOverflow),
      (Some(true), Some(-1L), negativeOverflow, positiveOverflow),
      (Some(true), Some(max), positiveOverflow, negativeOverflow),
      (Some(true), Some(min), negativeOverflow, positiveOverflow),
      (Some(true), None, positiveOverflow, negativeOverflow),
      (Some(false), positiveOverflow, Some(2L), negativeOverflow),
      (Some(false), negativeOverflow, None, positiveOverflow),
      (None, positiveOverflow, Some(3L), Some(4L)),
      (None, negativeOverflow, None, None)).toDF("p", "a", "b", "c")
  }

  private def decimalInputs(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq[(Option[Boolean], Option[String], Option[String], Option[String])](
      (Some(true), Some("1.0000000"), Some("0.0000001"), Some("9223372036855.0000000")),
      (Some(true), None, Some("-9223372036855.0000000"), Some("-0.0000001")),
      (Some(false), Some("0.0000001"), Some("-1.0000000"), Some("9223372036855.0000000")),
      (Some(false), Some("9223372036855.0000000"), None, Some("0.0000001")),
      (None, Some("-9223372036855.0000000"), Some("2.0000000"), Some("3.0000000")),
      (None, Some("0.0000001"), None, None)).toDF("p", "a", "b", "c")
      .selectExpr("p", "cast(a as decimal(26, 7)) as a",
        "cast(b as decimal(26, 7)) as b", "cast(c as decimal(26, 7)) as c")
  }

  for {
    ansi <- Seq(false, true)
    kind <- Seq("IF", "CASE WHEN")
  } {
    val conf = new SparkConf()
      .set("spark.sql.ansi.enabled", ansi.toString)
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.sql.datetime.java8API.enabled", "true")

    for ((function, multiplier) <- Seq(
        ("timestamp_seconds", 1000000L), ("timestamp_millis", 1000L))) {
      testSparkResultsAreEqual(
          s"$function skips unselected Long overflow in $kind, ANSI=$ansi",
          spark => longInputs(spark, multiplier), conf = conf) { frame =>
        // Instant preserves boundary timestamps without legacy JVM timestamp decoding.
        frame.selectExpr(conditional(function, kind))
      }

      for (sign <- Seq(-1L, 1L)) {
        val overflow = if (sign > 0) {
          Long.MaxValue / multiplier + 1
        } else {
          Long.MinValue / multiplier - 1
        }
        testBothCpuGpuExpectedException[Exception](
            s"$function rejects selected Long overflow in $kind, sign=$sign, ANSI=$ansi",
            e => Iterator.iterate(e: Throwable)(_.getCause).takeWhile(_ != null).exists {
              cause => cause.isInstanceOf[ArithmeticException] && {
                if (cmpSparkVersion(5, 0, 0) >= 0) {
                  cause.getMessage.toLowerCase(Locale.ROOT).contains("overflow")
                } else {
                  cause.getMessage.contains("long overflow")
                }
              }
            },
            spark => {
              import spark.implicits._
              // Only the selected value in the first row overflows.
              Seq((true, overflow, 1L, 1L), (false, 1L, 1L, 1L))
                .toDF("p", "a", "b", "c")
            }, conf = conf) { frame =>
          frame.selectExpr(conditional(function, kind))
        }
      }
    }

    testSparkResultsAreEqual(
        s"timestamp_seconds skips unselected decimal errors in $kind, ANSI=$ansi",
        decimalInputs, conf = conf) { frame =>
      frame.selectExpr(conditional("timestamp_seconds", kind))
    }

    // Exercise each throwing condition independently: scale > 6 and integer digits > 12.
    for ((decimalType, error, invalid) <- Seq(
        ("decimal(18, 7)", "rounding", "0.0000001"),
        ("decimal(19, 6)", "overflow", "9223372036855.000000"))) {
      testSparkResultsAreEqual(
          s"timestamp_seconds skips unselected decimal $error in $kind, ANSI=$ansi",
          spark => {
            import spark.implicits._
            Seq[(Option[Boolean], Option[String], Option[String], Option[String])](
              (Some(true), Some("1.0000000"), Some(invalid), Some(s"-$invalid")),
              (Some(false), Some(s"-$invalid"), Some("-1.0000000"), Some(invalid)),
              (None, Some(invalid), None, Some("2.0000000")))
              .toDF("p", "a", "b", "c")
              .selectExpr("p", s"cast(a as $decimalType) as a",
                s"cast(b as $decimalType) as b", s"cast(c as $decimalType) as c")
          }, conf = conf) { frame =>
        frame.selectExpr(conditional("timestamp_seconds", kind))
      }
    }
  }

  for (ansi <- Seq(false, true)) {
    testSparkResultsAreEqual(
        s"timestamp_seconds keeps safe decimals on GPU in dynamic IN, ANSI=$ansi",
        spark => {
          import spark.implicits._
          Seq[(Option[BigDecimal], Long, Long)](
            (Some(BigDecimal("1.000001")), 1000001L, 2000000L),
            (Some(BigDecimal("-1.000001")), -1000001L, 0L),
            (Some(BigDecimal("999999999999.999999")), 999999999999999999L, 0L),
            (Some(BigDecimal("-999999999999.999999")), -999999999999999999L, 0L),
            (Some(BigDecimal(0)), 1L, 2L),
            (Some(BigDecimal(1)), 2000000L, 2000000L),
            (None, 1000000L, 2000000L)).toDF("seconds", "micros", "other_micros")
            .selectExpr("cast(seconds as decimal(18, 6)) as seconds", "micros", "other_micros")
        },
        conf = new SparkConf()
          .set("spark.sql.ansi.enabled", ansi.toString)
          .set("spark.sql.session.timeZone", "UTC"),
        existClasses = "GpuIn") { frame =>
      // A side-effecting list item makes dynamic IN fall back; require the GPU expression.
      frame.selectExpr("timestamp_micros(micros) IN " +
        "(timestamp_micros(other_micros), timestamp_seconds(seconds))")
    }
  }
}
