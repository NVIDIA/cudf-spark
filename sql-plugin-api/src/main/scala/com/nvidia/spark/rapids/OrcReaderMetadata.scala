/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import java.time.ZoneId

import org.apache.orc.TypeDescription

// Orc schema wrapper
private case class OrcSchemaWrapper(schema: TypeDescription) extends SchemaBase {
  override def isEmpty: Boolean = schema.getFieldNames.isEmpty
}

/** Orc extra information containing the requested column ids for the current coalescing stripes */
case class OrcExtraInfo(
    requestedMapping: Option[Array[Int]],
    writerTimezone: ZoneId,
    writerUsedProlepticGregorian: Boolean) extends ExtraInfo
