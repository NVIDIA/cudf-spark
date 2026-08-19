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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.delta.{DeltaProvider, NoDeltaProvider}

class DeltaLakeQuerySuiteSpark411 extends SparkQueryCompareTestSuite {
  test("delta provider resolves to a real implementation on spark 411") {
    val provider = DeltaProvider()
    assert(provider ne NoDeltaProvider)
    assert(provider.getClass.getName.contains("Delta41xProvider"))
  }
}
