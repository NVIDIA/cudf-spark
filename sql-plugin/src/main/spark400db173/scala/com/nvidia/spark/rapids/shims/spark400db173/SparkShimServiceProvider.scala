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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.spark400db173

import com.nvidia.spark.rapids._

import org.apache.spark.SparkEnv

object SparkShimServiceProvider {
  // DB version should conform to "major.minor" and has no patch version.
  // Refer to VersionUtils.getVersionForJni
  val VERSION = DatabricksShimVersion(4, 0, 0, "17.3")
}

class SparkShimServiceProvider extends com.nvidia.spark.rapids.SparkShimServiceProvider {

  override def getShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  def matchesVersion(version: String): Boolean = {
    val shimEnabledProp = "spark.rapids.shims.spark400db173" + ".enabled"
    val shimEnabled = Option(SparkEnv.get)
      .flatMap(_.conf.getOption(shimEnabledProp).map(_.toBoolean))
      .getOrElse(true)

    DatabricksShimServiceProvider.matchesVersion(
      dbrVersion = "17.3.x",
      shimMatchEnabled = shimEnabled,
      disclaimer = "Development of support for Databricks 17.3.x is still in progress: " +
        "https://github.com/NVIDIA/spark-rapids/issues/14015"
    )
  }
}
