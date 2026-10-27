/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.jni.{SparkPlatformType => PlatformForJni, Version => VersionForJni}

import org.apache.spark.internal.Logging

object VersionUtils extends Logging {

  lazy val isSpark320OrLater: Boolean = ShimVersionUtils.isSpark320OrLater

  lazy val isSpark400OrLater: Boolean = ShimVersionUtils.isSpark400OrLater

  lazy val isSpark: Boolean = ShimVersionUtils.isSpark

  lazy val isDataBricks: Boolean = ShimVersionUtils.isDataBricks

  lazy val isCloudera: Boolean = ShimVersionUtils.isCloudera

  def cmpSparkVersion(major: Int, minor: Int, bugfix: Int): Int =
    ShimVersionUtils.cmpSparkVersion(major, minor, bugfix)

  /**
   * Get the version used by JNI interface
   * Must use `com.nvidia.spark.rapids.jni.Version` in the JNI interface
   */
  def getVersionForJni: VersionForJni = {
    val sparkShimVersion = ShimLoader.getShimVersion
    sparkShimVersion match {
      case SparkShimVersion(a, b, c) =>
        new VersionForJni(PlatformForJni.VANILLA_SPARK, a, b, c)
      case DatabricksShimVersion(_, _, _, dbVer) =>
        val major = dbVer.split("\\.")(0).toInt
        val minor = dbVer.split("\\.")(1).toInt
        new VersionForJni(PlatformForJni.DATABRICKS, major, minor, 0)
      case ClouderaShimVersion(a, b, c, _) =>
        new VersionForJni(PlatformForJni.CLOUDERA, a, b, c)
      case unknown =>
        // Unknown platform, customer specific platform.
        // All the platforms in this code base is listed above: Spark, Databricks and Cloudera
        // Please append the related operators into the below warning.
        logWarning(s"Unknown Spark platform type: ${unknown.getClass.getName}.  Some GPU " +
          s"operators like cast string to timestamp need to known the Spark " +
          s"platform/version, if the Spark platform is unknown, the GPU operator will use " +
          s"the default behavior, please make sure the default behavior is expected. Currently " +
          s"the related operator is: \n" +
          "    1. Cast string to timestamp \n")

        // Return unknown type
        new VersionForJni(PlatformForJni.UNKNOWN, 0, 0, 0)
    }
  }
}
