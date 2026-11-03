/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.execution.datasources.rapids

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument

import org.apache.spark.sql.execution.datasources.orc.OrcFilters
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

// Wrapper for Spark OrcFilters which is in private package
object OrcFiltersWrapper {
  def createFilter(schema: StructType, filters: Seq[Filter]): Option[SearchArgument] = {
    OrcFilters.createFilter(schema, filters)
  }
}
