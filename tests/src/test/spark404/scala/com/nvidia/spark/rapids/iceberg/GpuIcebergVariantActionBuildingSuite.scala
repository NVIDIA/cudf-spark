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
{"spark": "404"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.util.{HashMap => JHashMap}

import com.nvidia.spark.rapids.iceberg.parquet.{ColumnAction, FillNull, PassThrough}
import org.apache.iceberg.types.{Type, Types}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.VariantType

class GpuIcebergVariantActionBuildingSuite extends AnyFunSuite {
  Seq[(String, Type, ColumnAction)](
    ("present", Types.VariantType.get(), PassThrough),
    ("missing", null, FillNull(VariantType))
  ).foreach { case (name, partner, expected) =>
    test(s"selected Iceberg visitor handles $name optional Variant") {
      val visitor = ShimUtils.newActionBuildingVisitor(new JHashMap[Integer, Any]())
      val variantType = Types.VariantType.get()
      val field = Types.NestedField.optional(1, "v", variantType)
      visitor.beforeField(field, partner)
      try {
        assert(visitor.variant(variantType, partner) == expected)
      } finally {
        visitor.afterField(field, partner)
      }
    }
  }
}
