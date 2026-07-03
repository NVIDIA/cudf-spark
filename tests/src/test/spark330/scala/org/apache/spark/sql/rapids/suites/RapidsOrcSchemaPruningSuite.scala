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

import com.nvidia.spark.rapids.GpuOrcScan
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.orc.{OrcV1SchemaPruningSuite, OrcV2SchemaPruningSuite}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.utils.{RapidsSchemaPruningTestUtils, RapidsSQLTestsBaseTrait}

class RapidsOrcV1SchemaPruningSuite
  extends OrcV1SchemaPruningSuite
  with RapidsSQLTestsBaseTrait
  with RapidsSchemaPruningTestUtils {

  override protected def checkScan(
      df: DataFrame,
      expectedSchemaCatalogStrings: String*): Unit = {
    checkScanSchema(df, expectedSchemaCatalogStrings: _*) {
      case scan: FileSourceScanExec => scan.requiredSchema
      case scan: GpuFileSourceScanExec => scan.requiredSchema
    }
    df.collect()
  }
}

class RapidsOrcV2SchemaPruningSuite
  extends OrcV2SchemaPruningSuite
  with RapidsSQLTestsBaseTrait
  with RapidsSchemaPruningTestUtils {

  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")

  override protected def checkScan(
      df: DataFrame,
      expectedSchemaCatalogStrings: String*): Unit = {
    checkScanSchema(df, expectedSchemaCatalogStrings: _*) {
      case scan: GpuBatchScanExec if scan.scan.isInstanceOf[GpuOrcScan] =>
        scan.scan.asInstanceOf[GpuOrcScan].readDataSchema
    }
    df.collect()
  }
}
