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
{"spark":["400","420"],"carryForward":"all"}
{"spark":["400db173","410db183"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.parquet

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.sources.{IsNotNull, IsNull}
import org.apache.spark.sql.types.{StructField, StructType, VariantType}

class FilterToAstConverterVariantSuite extends AnyFunSuite {
  test("does not null check a variant column") {
    val schema = StructType(Seq(StructField("v", VariantType)))
    Seq(false, true).foreach { useColumnNames =>
      val converter = FilterToAstConverter(schema, isCaseSensitive = true,
        useColumnNames = useColumnNames)
      assert(converter.convertFilter(IsNull("v")).isEmpty)
      assert(converter.convertFilter(IsNotNull("v")).isEmpty)
    }
  }
}
