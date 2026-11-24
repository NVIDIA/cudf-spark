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

package org.apache.spark.sql.delta.rapids

import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, ColumnMappingTableFeature,
  DeletionVectorsTableFeature, TableFeature, TimestampNTZTableFeature,
  TypeWideningPreviewTableFeature, TypeWideningTableFeature, V2CheckpointTableFeature,
  VacuumProtocolCheckTableFeature, VariantTypePreviewTableFeature, VariantTypeTableFeature}
import org.apache.spark.sql.delta.actions.Protocol

/** Protocol checks shared by the supported Delta 4.x runtimes. */
trait DeltaVariantShreddingShim extends DeltaRuntimeShim {
  protected def variantShreddingFeatures: Set[TableFeature]

  // This is deliberately a reviewed set, not Delta's dynamic feature registry. A future
  // reader feature may change the VARIANT file layout and must fall back until reviewed.
  private lazy val knownReaderFeatureNames = (Set[TableFeature](
    CatalogOwnedTableFeature,
    ColumnMappingTableFeature,
    DeletionVectorsTableFeature,
    TimestampNTZTableFeature,
    TypeWideningPreviewTableFeature,
    TypeWideningTableFeature,
    V2CheckpointTableFeature,
    VacuumProtocolCheckTableFeature,
    VariantTypePreviewTableFeature,
    VariantTypeTableFeature) ++ variantShreddingFeatures).map(_.name)

  override final def mayContainShreddedVariant(protocol: Protocol): Boolean = {
    // The feature survives disabling shredded writes because old shredded files can remain.
    protocol.readerFeatures.exists(features => !features.subsetOf(knownReaderFeatureNames)) ||
      variantShreddingFeatures.exists(protocol.isFeatureSupported)
  }
}
