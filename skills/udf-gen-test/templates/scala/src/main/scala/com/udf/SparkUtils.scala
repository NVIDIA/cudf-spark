/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf

import com.nvidia.spark.rapids.ExplainPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark utility methods.
 */
object SparkUtils {

  /**
   * Apply key=value Spark configs to a builder.
   *
   * @param builder     the SparkSession builder to configure
   * @param sparkConfs  "spark.key=value" config strings
   * @return the same builder, for chaining
   */
  def applySparkConfs(
      builder: SparkSession.Builder,
      sparkConfs: Seq[String]
  ): SparkSession.Builder = {
    for (conf <- sparkConfs) {
      val kv = conf.split("=", 2)
      if (kv.length == 2) builder.config(kv(0), kv(1))
    }
    builder
  }

  /** 
   * Ops that cause fallback but can be ignored, since they are strictly used for testing:
   * - RDDScanExec/LocalTableScanExec: surfaces due to spark.createDataFrame()
   * - CollectLimitExec: surfaces during dataframe collection (e.g. df.show())
   * - ToPrettyString: surfaces due to df.show()
   */
  private val IgnoreOperations = Set(
    "RDDScanExec", "LocalTableScanExec", "CollectLimitExec", "ToPrettyString"
  )

  /**
   * Assert that the DataFrame's plan can run on GPU.
   * NOTE: This is only reliable in explainOnly mode, with AQE disabled.
   *
   * @param df              the DataFrame to check
   * @param returnFullPlan  if true, include the full plan in the error message
   * @throws RuntimeException if any operations cannot run on GPU
   */
  def assertPlanRunsOnGpu(df: DataFrame, returnFullPlan: Boolean = false): Unit = {
    val plan = getGpuPlan(df)
    val unsupportedOps = getUnsupportedOps(plan)
    if (unsupportedOps.nonEmpty) {
      val opsList = unsupportedOps.map(op => s"- $op").mkString("\n")
      var errorMsg = s"Some operations cannot run on GPU.\nFound the following unsupported ops:\n$opsList"
      if (returnFullPlan) {
        errorMsg += s"\n\nFull physical plan:\n$plan"
      }
      throw new RuntimeException(errorMsg)
    }
  }

  /** Get the potential GPU plan using the RAPIDS ExplainPlan API. */
  private def getGpuPlan(df: DataFrame): String = {
    ExplainPlan.explainPotentialGpuPlan(df, "NOT_ON_GPU")
  }

  /** Parse the plan for unsupported operations (lines starting with '!'). */
  private def getUnsupportedOps(plan: String): Seq[String] = {
    plan.split("\n").filter(_.trim.startsWith("!")).flatMap { line =>
      // Each unsupported line looks like: ![Exec] <OPERATION> cannot run on GPU
      val start = line.indexOf('<')
      val end = line.indexOf('>')
      if (start >= 0 && end > start) {
        val op = line.substring(start + 1, end)
        if (!IgnoreOperations.contains(op)) Some(line.trim) else None
      } else {
        None
      }
    }.toSeq
  }
}
