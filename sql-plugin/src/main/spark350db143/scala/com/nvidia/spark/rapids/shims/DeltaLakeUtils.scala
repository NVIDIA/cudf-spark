/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.execution.FileSourceScanExec

object DeltaLakeUtils {
  /* Allow skip_row on Databricks but block all other columns starting with _databricks_internal
  to avoid any unforeseen circumstances*/
  def isDatabricksDeltaLakeScan(f: FileSourceScanExec): Boolean = {
    f.requiredSchema.fields.exists(f => f.name.startsWith("_databricks_internal") &&
      !f.name.startsWith("_databricks_internal_edge_computed_column_skip_row"))
  }
}
