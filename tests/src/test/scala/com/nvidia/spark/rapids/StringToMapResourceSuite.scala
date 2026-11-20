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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, Rmm}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuStringToMap
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch

class StringToMapResourceSuite extends RmmSparkRetrySuiteBase {
  Seq(false, true).foreach { regex =>
    test(s"str_to_map duplicate-key failure releases device allocations: regex=$regex") {
      val conf = new SQLConf
      conf.setConfString("spark.sql.mapKeyDedupPolicy", "EXCEPTION")
      SQLConf.withExistingConf(conf) {
        val pairDelimiter = if (regex) "[,]" else ","
        val valueDelimiter = if (regex) "[:]" else ":"
        val expression = GpuStringToMap(
          GpuBoundReference(0, StringType, nullable = true)(ExprId(0), "input"),
          GpuLiteral.create(pairDelimiter, StringType),
          GpuLiteral.create(valueDelimiter, StringType),
          pairDelimiter, regex, valueDelimiter, regex)
        withResource(new ColumnarBatch(Array(GpuColumnVector.from(
            ColumnVector.fromStrings("a:1,a:2", "b:3", null), StringType)), 3)) { batch =>
          val before = Rmm.getTotalBytesAllocated
          val error = intercept[RuntimeException] {
            withResource(expression.columnarEval(batch)) { _ =>
              fail("duplicate keys should throw")
            }
          }
          assert(error.getMessage.contains("Duplicate map key"))
          assert(Rmm.getTotalBytesAllocated == before)
        }
      }
    }
  }
}
