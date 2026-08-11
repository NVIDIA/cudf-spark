/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

sealed abstract class ShimVersion

case class SparkShimVersion(major: Int, minor: Int, patch: Int) extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch"
}

case class ClouderaShimVersion(major: Int, minor: Int, patch: Int, clouderaVersion: String)
  extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch-cloudera-$clouderaVersion"
}

case class DatabricksShimVersion(
    major: Int,
    minor: Int,
    patch: Int,
    dbver: String = "") extends ShimVersion {
  override def toString(): String = s"$major.$minor.$patch-databricks$dbver"
}
