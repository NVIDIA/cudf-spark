/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.GpuUnionExec

import org.apache.spark.sql.{DataFrameSetOperationsSuite, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuInMemoryTableScanExec
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsDataFrameSetOperationsSuite
    extends DataFrameSetOperationsSuite
    with RapidsSQLTestsBaseTrait {
  
  import testImplicits._

  testRapids(
      "SPARK-37371: UnionExec should support columnar if all children support columnar") {
    def checkGpuPlanExists(
        plan: SparkPlan,
        targetPlan: (SparkPlan) => Boolean): Unit = {
      val target = plan.collect {
        case p if targetPlan(p) => p
      }
      assert(target.nonEmpty, s"No matching GPU plan nodes found in: ${plan.treeString}")
      // GPU always supports columnar execution regardless of cache settings
      assert(target.forall(_.supportsColumnar),
        s"GPU plan nodes should always support columnar: " +
          s"${target.map(p => (p.getClass.getSimpleName, p.supportsColumnar))}")
    }

    Seq(true, false).foreach { supported =>
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> supported.toString) {
        val df1 = Seq(1, 2, 3).toDF("i").cache()
        val df2 = Seq(4, 5, 6).toDF("j").cache()

        // Materialize the cache
        df1.count()
        df2.count()

        val union = df1.union(df2)
        
        // Check GpuInMemoryTableScanExec exists and supports columnar
        // Note: Unlike CPU, GPU always supports columnar regardless of
        // CACHE_VECTORIZED_READER_ENABLED configuration
        checkGpuPlanExists(union.queryExecution.executedPlan,
          _.isInstanceOf[GpuInMemoryTableScanExec])
        
        // Check GpuUnionExec exists and supports columnar
        checkGpuPlanExists(union.queryExecution.executedPlan, 
          _.isInstanceOf[GpuUnionExec])
        
        // Verify query results
        checkAnswer(union, Row(1) :: Row(2) :: Row(3) :: Row(4) :: Row(5) :: Row(6) :: Nil)

        // Test union with cached and non-cached DataFrames
        val mixedUnion = df1.union(Seq(7, 8, 9).toDF("k"))
        
        // GpuUnionExec should exist and handle mixed scenarios
        val unionExec = mixedUnion.queryExecution.executedPlan.collect {
          case p: GpuUnionExec => p
        }
        assert(unionExec.nonEmpty, "GpuUnionExec should exist in the plan")
        assert(unionExec.forall(_.supportsColumnar), "GpuUnionExec should support columnar")
        
        // Verify query results
        checkAnswer(mixedUnion,
          Row(1) :: Row(2) :: Row(3) :: Row(7) :: Row(8) :: Row(9) :: Nil)
      }
    }
  }
}

