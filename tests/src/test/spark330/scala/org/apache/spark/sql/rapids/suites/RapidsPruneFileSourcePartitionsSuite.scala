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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.PruneFileSourcePartitionsSuite
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsPruneFileSourcePartitionsSuite
  extends PruneFileSourcePartitionsSuite
  with RapidsSQLTestsTrait {

  override protected def collectPartitionFiltersFn()
      : PartialFunction[SparkPlan, Seq[Expression]] = {
    super.collectPartitionFiltersFn().orElse {
      case scan: GpuFileSourceScanExec =>
        scan.partitionFilters
      case scan: GpuBatchScanExec if scan.scan.isInstanceOf[FileScan] =>
        scan.scan.asInstanceOf[FileScan].partitionFilters
    }
  }

  override def getScanExecPartitionSize(plan: SparkPlan): Long = {
    plan.collectFirst {
      case scan: GpuFileSourceScanExec => scan.selectedPartitions.length.toLong
      case scan: GpuBatchScanExec if scan.scan.isInstanceOf[FileScan] =>
        scan.partitions.size.toLong
    }.getOrElse(super.getScanExecPartitionSize(plan))
  }
}
