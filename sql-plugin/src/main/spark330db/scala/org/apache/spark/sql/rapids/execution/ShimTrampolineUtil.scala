/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "332db"}
{"spark": "350db143"}
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, IdentityBroadcastMode}
import org.apache.spark.sql.execution.joins.{ExecutorBroadcastMode, HashedRelationBroadcastMode}
import org.apache.spark.sql.types.{DataType, StructType}

object ShimTrampolineUtil {

  def unionLikeMerge(left: DataType, right: DataType): DataType =
    StructType.unionLikeMerge(left, right)

  def isSupportedRelation(mode: BroadcastMode): Boolean = mode match {
    case _ : HashedRelationBroadcastMode => true
    case IdentityBroadcastMode | ExecutorBroadcastMode => true
    case _ => false
  }
}
