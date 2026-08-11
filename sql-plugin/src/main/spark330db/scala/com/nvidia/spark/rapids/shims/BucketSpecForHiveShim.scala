/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}

object BucketSpecForHiveShim {
  // Only for GpuInsertIntoHiveTable. On DB 330, the "InsertIntoHiveTable" does not
  // execute the bucketed write even given a bucket specification.
  def getBucketSpec(table: CatalogTable, forceHiveHash: Boolean): Option[BucketSpec] = {
    None
  }
}
