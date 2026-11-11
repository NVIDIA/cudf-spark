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
{"spark":["405","414"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import org.apache.spark.sql.execution.datasources.orc.OrcUtils.CATALYST_TYPE_ATTRIBUTE_NAME
import org.apache.spark.sql.types.{DataType, TimestampType}

object OrcSchemaTypeShims {
  def catalystTypeOverride(orcType: TypeDescription): Option[DataType] = {
    orcType.getCategory match {
      case Category.TIMESTAMP
          if Option(orcType.getAttributeValue(CATALYST_TYPE_ATTRIBUTE_NAME))
            .forall(_ == TimestampType.typeName) =>
        Some(TimestampType)
      case _ => None
    }
  }
}
