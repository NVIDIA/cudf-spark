/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.delta

import com.nvidia.spark.rapids.delta.GpuDeltaIdentityColumnStatsTracker

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.functions.{array, max, min, to_json}
import org.apache.spark.sql.nvidia.DFUDFShims
import org.apache.spark.sql.types.StructType

object GpuIdentityColumn {
  def createIdentityColumnStatsTracker(
      spark: SparkSession,
      dataCols: Seq[Attribute],
      schema: StructType,
      highWaterMarks: Set[String]): Option[GpuDeltaIdentityColumnStatsTracker] = {
    if (highWaterMarks.isEmpty) {
      return None
    }
    val identityInfo = schema.filter(f => highWaterMarks.contains(f.name)).map { f =>
      val useMax = if (f.metadata.getLong("delta.identity.step") <= 0) {
        false
      } else {
        true
      }
      f.name -> useMax
    }
    assert(identityInfo.size == highWaterMarks.size,
      s"expected $highWaterMarks found $identityInfo")

    val columns = identityInfo.map { case (name, useMax) =>
      // Build from a quoted unresolved attribute so dots in identity column names are treated
      // as literal name characters, not nested field traversal.
      val column = DFUDFShims.exprToColumn(UnresolvedAttribute.quoted(name))
      if (useMax) {
        max(column)
      } else {
        min(column)
      }
    }
    val identityExpr: Expression = {
      val dummyDF = Dataset.ofRows(spark, LocalRelation(dataCols))
      dummyDF.select(to_json(array(columns: _*)))
          .queryExecution.analyzed.expressions.head
    }
    Some(new GpuDeltaIdentityColumnStatsTracker(dataCols, identityExpr, identityInfo))
  }
}
