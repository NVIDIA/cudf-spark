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
{"spark":["401","411"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.delta.rapids

import org.mockito.Mockito.{mock, CALLS_REAL_METHODS}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.delta.{ColumnMappingTableFeature, DeletionVectorsTableFeature,
  TableFeature, VariantShreddingPreviewTableFeature, VariantTypeTableFeature}
import org.apache.spark.sql.delta.actions.Protocol

class DeltaVariantShreddingSuite extends AnyFunSuite {
  private def protocol(features: Set[String]): Protocol =
    Protocol(3, 7, Some(features), Some(features))

  test("an unclassified runtime defaults to CPU for VARIANT") {
    val runtime = mock(classOf[DeltaRuntimeShim], CALLS_REAL_METHODS)
    assert(runtime.mayContainShreddedVariant(protocol(Set.empty)))
  }

  test("a protocol with no shredding feature is safe for VARIANT reads") {
    assert(!DeltaRuntimeShim.mayContainShreddedVariant(protocol(Set.empty)))
    assert(!DeltaRuntimeShim.mayContainShreddedVariant(
      protocol(Set(VariantTypeTableFeature.name))))
  }

  test("known unrelated reader features retain VARIANT GPU eligibility") {
    val features = Set(VariantTypeTableFeature.name, ColumnMappingTableFeature.name,
      DeletionVectorsTableFeature.name)
    assert(!DeltaRuntimeShim.mayContainShreddedVariant(protocol(features)))
  }

  test("preview shredding requires CPU regardless of write properties") {
    val features = Set(VariantTypeTableFeature.name, VariantShreddingPreviewTableFeature.name)
    assert(DeltaRuntimeShim.mayContainShreddedVariant(protocol(features)))
  }

  test("stable shredding requires CPU on runtimes supporting the stable feature") {
    val stable = TableFeature.featureNameToFeature("variantShredding")
    if (!io.delta.VERSION.startsWith("4.0.")) {
      assert(stable.isDefined)
    }
    stable.foreach { feature =>
      assert(DeltaRuntimeShim.mayContainShreddedVariant(
        protocol(Set(VariantTypeTableFeature.name, feature.name))))
    }
  }

  test("an unknown reader feature cannot silently enable VARIANT GPU reads") {
    val features = Set(VariantTypeTableFeature.name, "futureVariantLayout")
    assert(DeltaRuntimeShim.mayContainShreddedVariant(protocol(features)))
  }

  test("unrelated unknown reader features also fail closed for VARIANT") {
    assert(DeltaRuntimeShim.mayContainShreddedVariant(protocol(Set("futureReaderFeature"))))
  }

  test("unknown writer-only features do not restrict VARIANT readers") {
    val p = Protocol(3, 7, Some(Set(VariantTypeTableFeature.name)),
      Some(Set(VariantTypeTableFeature.name, "futureWriterFeature")))
    assert(!DeltaRuntimeShim.mayContainShreddedVariant(p))
  }
}
