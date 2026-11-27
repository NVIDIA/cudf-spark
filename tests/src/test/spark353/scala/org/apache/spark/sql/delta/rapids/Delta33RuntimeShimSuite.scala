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
{"spark":["353"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.delta.rapids

import org.scalatest.funsuite.AnyFunSuite

class Delta33RuntimeShimSuite extends AnyFunSuite {
  test("Spark 3.5.3+ getShimClassName dispatches only Delta 3.3.x") {
    Seq("3.3.0", "3.3.3").foreach { deltaVersion =>
      assert(DeltaRuntimeShim.getShimClassName(deltaVersion).exists(_.contains("delta33x")),
        deltaVersion)
    }
    Seq("3.2.1", "3.1.0", "4.0.0", "4.2.0", "4.3.0", "4.4.0").foreach { deltaVersion =>
      assert(DeltaRuntimeShim.getShimClassName(deltaVersion).isEmpty, deltaVersion)
    }
  }
}
