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

import com.databricks.sql.transaction.tahoe.{DeltaConfigs, DeltaOptions}
import com.databricks.sql.transaction.tahoe.actions.Metadata
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf

import org.apache.spark.sql.internal.SQLConf

object DeltaWriteUtils {
  // scalastyle:off line.size.limit
  /**
   * Optimized writes can be enabled/disabled through the following order:
   *  - Through DataFrameWriter options
   *  - Through SQL configuration
   *  - Through the table parameter
   *
   * This logic is copied from
   * https://github.com/delta-io/delta/blob/1b35c5dc5e041c192863ef1493e0b0262ef2e822/spark/src/main/scala/org/apache/spark/sql/delta/files/TransactionalWrite.scala#L547-L560
   * and slightly modified.
   */
  // scalastyle:on line.size.limit
  def shouldOptimizeWrite(writeOptions: Option[DeltaOptions], sessionConf: SQLConf,
                          metadata: Metadata): Boolean = {
    writeOptions.flatMap(_.optimizeWrite)
      .orElse(sessionConf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED))
      .orElse(DeltaConfigs.OPTIMIZE_WRITE.fromMetaData(metadata))
      .getOrElse(false)
  }
}
