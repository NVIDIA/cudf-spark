/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.RapidsMeta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.internal.SQLConf

trait DeltaConfigChecker {
  /**
   * Check delta configurations if they are compatible with the Rapids Accelerator.
   * The `meta` will be marked if incompatible configurations are found.
   */
  def checkIncompatibleConfs(
      meta: RapidsMeta[_, _, _],
      deltaLog: Option[DeltaLog],
      sqlConf: SQLConf,
      options: Map[String, String]
  ): Unit

  /**
   * Check the properties of the existing table being written, using the session that planned the
   * write. The properties are only read when a version needs to check them. Accepts everything
   * by default.
   */
  def checkTableProperties(
      meta: RapidsMeta[_, _, _],
      spark: SparkSession,
      options: Map[String, String],
      tableProperties: => Map[String, String]): Unit = {}
}

object AcceptAllConfigChecker extends DeltaConfigChecker {
  override def checkIncompatibleConfs(
      meta: RapidsMeta[_, _, _],
      deltaLog: Option[DeltaLog],
      sqlConf: SQLConf,
      options: Map[String, String]): Unit = {
    // No-op, accepts all configurations
  }
}
