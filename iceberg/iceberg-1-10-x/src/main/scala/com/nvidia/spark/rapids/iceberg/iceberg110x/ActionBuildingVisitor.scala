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

package com.nvidia.spark.rapids.iceberg.iceberg110x

import java.util.{Map => JMap}

import com.nvidia.spark.rapids.iceberg.parquet.{BaseActionBuildingVisitor, ColumnAction}
import org.apache.iceberg.types.{Type, Types}

/**
 * Iceberg visits Variant separately from primitive types, and its default callback throws.
 * Route Variant through the shared leaf action builder to apply the same missing-field rules.
 */
private[iceberg] class ActionBuildingVisitor(idToConstant: JMap[Integer, _])
    extends BaseActionBuildingVisitor(idToConstant) {

  override def variant(variant: Types.VariantType, partner: Type): ColumnAction = {
    buildLeafAction(variant, partner)
  }
}
