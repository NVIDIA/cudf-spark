/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

object DatabricksShimServiceProvider {
  val log = org.slf4j.LoggerFactory.getLogger(getClass().getName().stripSuffix("$"))

  def matchesVersion(dbrVersion: String,
    shimMatchEnabled: Boolean = true,
    disclaimer: String = ""
  ): Boolean = {
    var ignoreExceptions = true
    try {
      val sparkBuildInfo = org.apache.spark.BuildInfo
      val databricksBuildInfo = com.databricks.BuildInfo
      val matchRes = sparkBuildInfo.dbrVersion.startsWith(dbrVersion)
      val matchStatus = if (matchRes) "SUCCESS" else "FAILURE"
      val logMessage =
        s"""Databricks Runtime Build Info match: $matchStatus
           |\tDBR_VERSION: ${sparkBuildInfo.dbrVersion}
           |\tspark.BuildInfo.gitHash: ${sparkBuildInfo.gitHash}
           |\tdatabricks.BuildInfo.gitHash: ${databricksBuildInfo.gitHash}
           |\tdatabricks.BuildInfo.gitTimestamp: ${databricksBuildInfo.gitTimestamp}"""
           .stripMargin
      if (matchRes) {
        log.warn(logMessage)
        if (shimMatchEnabled) {
          if (disclaimer.nonEmpty) {
            log.warn(disclaimer)
          }
        } else {
          ignoreExceptions = false
          sys.error(disclaimer)
        }
      } else {
        log.debug(logMessage)
      }
      matchRes
    } catch {
      case x: Throwable if ignoreExceptions =>
        log.debug("Databricks detection failed: " + x, x)
        false
    }
  }
}
