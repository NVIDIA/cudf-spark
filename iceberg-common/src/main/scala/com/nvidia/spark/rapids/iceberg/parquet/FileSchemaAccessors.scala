/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.parquet

import org.apache.iceberg.schema.SchemaWithPartnerVisitor
import org.apache.iceberg.types.Type

/**
 * Partner accessors to navigate file schema alongside expected schema.
 */
private class FileSchemaAccessors
    extends SchemaWithPartnerVisitor.PartnerAccessors[Type] {

  override def fieldPartner(partnerStruct: Type, fieldId: Int, name: String): Type = {
    if (partnerStruct == null) return null
    val structType = partnerStruct.asStructType()
    val field = structType.field(fieldId)
    if (field == null) null else field.`type`()
  }

  override def listElementPartner(partnerList: Type): Type = {
    if (partnerList == null) return null
    partnerList.asListType().elementType()
  }

  override def mapKeyPartner(partnerMap: Type): Type = {
    if (partnerMap == null) return null
    partnerMap.asMapType().keyType()
  }

  override def mapValuePartner(partnerMap: Type): Type = {
    if (partnerMap == null) return null
    partnerMap.asMapType().valueType()
  }
}
