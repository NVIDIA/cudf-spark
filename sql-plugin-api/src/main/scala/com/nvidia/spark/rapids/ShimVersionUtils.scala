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

private[rapids] object ShimVersionUtils {
  lazy val isSpark320OrLater: Boolean = cmpSparkVersion(3, 2, 0) >= 0

  lazy val isSpark400OrLater: Boolean = cmpSparkVersion(4, 0, 0) >= 0

  lazy val isSpark: Boolean = ShimLoader.getShimVersion.isInstanceOf[SparkShimVersion]

  lazy val isDataBricks: Boolean = ShimLoader.getShimVersion.isInstanceOf[DatabricksShimVersion]

  lazy val isCloudera: Boolean = ShimLoader.getShimVersion.isInstanceOf[ClouderaShimVersion]

  def cmpSparkVersion(major: Int, minor: Int, bugfix: Int): Int = {
    val (sparkMajor, sparkMinor, sparkBugfix) = ShimLoader.getShimVersion match {
      case SparkShimVersion(a, b, c) => (a, b, c)
      case DatabricksShimVersion(a, b, c, _) => (a, b, c)
      case ClouderaShimVersion(a, b, c, _) => (a, b, c)
    }
    val fullVersion = ((major.toLong * 1000) + minor) * 1000 + bugfix
    val sparkFullVersion = ((sparkMajor.toLong * 1000) + sparkMinor) * 1000 + sparkBugfix
    sparkFullVersion.compareTo(fullVersion)
  }
}
