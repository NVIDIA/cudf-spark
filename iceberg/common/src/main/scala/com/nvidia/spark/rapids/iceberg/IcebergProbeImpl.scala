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

package com.nvidia.spark.rapids.iceberg

import com.nvidia.spark.rapids.{ShimLoader, ShimReflectionUtils, SparkShimVersion, VersionUtils}
import org.apache.iceberg.IcebergBuild

import org.apache.spark.internal.Logging

class IcebergProbeImpl extends IcebergProbe with Logging {

  override def isSupportedSparkVersion(): Boolean = {
    ShimLoader.getShimVersion match {
      case _: SparkShimVersion =>
        VersionUtils.cmpSparkVersion(3, 5, 0) >= 0 &&
        VersionUtils.cmpSparkVersion(4, 2, 0) < 0
      case _ => false
    }
  }

  // Git commit ID -> version for all supported Iceberg releases.
  // Commit IDs are from iceberg-build.properties embedded in each release jar.
  // https://github.com/apache/iceberg/releases
  private val commitToVersion: Map[String, String] = Map(
    "229d8f6fcd109e6c8943ea7cbb41dab746c6d0ed" -> "1.6.0",
    "8e9d59d299be42b0bca9461457cd1e95dbaad086" -> "1.6.1",
    "7dbafb438ee1e68d0047bebcb587265d7d87d8a1" -> "1.9.0", // version() returns "unspecified"
    "f40208ae6fb2f33e578c2637d3dea1db18739f31" -> "1.9.1",
    "071d5606bc6199a0be9b3f274ec7fbf111d88821" -> "1.9.2",
    "2114bf631e49af532d66e2ce148ee49dd1dd1f1f" -> "1.10.0",
    "ccb8bc435062171e64bc8b7e5f56e6aed9c5b934" -> "1.10.1",
    "6976e020b894f6a6777704df2b8c4458cb291ae9" -> "1.11.0"
  )

  // e.g. iceberg-spark-runtime-3.5_2.12-1.10.0-*.jar -> "1.10.0"
  private lazy val jarVersionPattern = (
    """iceberg-spark-runtime-""" +
    """\d+\.\d+_""" +             // Spark feature.major
    """\d+\.\d+-""" +             // Scala major.minor
    """(\d+\.\d+\.\d+)"""         // Iceberg major.minor.patch
  ).r.unanchored

  private def extractVersionFromJarPath(commitId: String): String = {
    Option(classOf[IcebergBuild].getProtectionDomain)
      .flatMap(pd => Option(pd.getCodeSource))
      .flatMap(cs => Option(cs.getLocation))
      .map(_.getPath)
      .collectFirst {
        case path@jarVersionPattern(v) =>
          logWarning(s"Commit $commitId not in known map, " +
            s"extracted version $v from jar path: $path")
          v
      }
      .getOrElse(commitId)
  }

  override def getDetectedVersion: String = {
    val commitId = IcebergBuild.gitCommitId()
    commitToVersion.getOrElse(commitId, extractVersionFromJarPath(commitId))
  }

  // Iceberg major.minor -> shim sub-package
  private val icebergVersionToShim: Map[String, String] = Map(
    "1.6" -> "iceberg16x",
    "1.9" -> "iceberg19x",
    "1.10" -> "iceberg110x",
    "1.11" -> "iceberg111x"
  )

  override def shimPackage: String = {
    val version = getDetectedVersion
    val icebergParts = version.split("\\.")
    if (icebergParts.length < 2) {
      throw new UnsupportedOperationException(
        s"Unrecognized Iceberg version: $version")
    }
    val icebergMajorMinor = s"${icebergParts(0)}.${icebergParts(1)}"
    val subpackage = icebergVersionToShim.getOrElse(icebergMajorMinor,
      throw new UnsupportedOperationException(
        s"Unsupported Iceberg version: $version"))
    s"com.nvidia.spark.rapids.iceberg.$subpackage"
  }

  override def getProvider: IcebergProvider = {
    ShimReflectionUtils.newInstanceOf[IcebergProvider](
      s"${shimPackage}.IcebergProviderImpl")
  }
}
