/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
{"spark": "341db"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.Scalar
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuToPrettyString(child: Expression, timeZoneId: Option[String] = None)
  extends ShimUnaryExpression with GpuExpression with TimeZoneAwareExpression {

  override lazy val resolved: Boolean = childrenResolved

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  override def withTimeZone(timeZoneId: String): GpuToPrettyString =
    copy(timeZoneId = Some(timeZoneId))

  override def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(child.columnarEval(batch)) { evaluatedCol =>
      withResource(GpuCast.doCast(
        evaluatedCol.getBase,
        evaluatedCol.dataType(),
        StringType,
        CastOptions.TO_PRETTY_STRING_OPTIONS)) { possibleStringResult =>
        if (possibleStringResult.hasNulls) {
          withResource(possibleStringResult.isNull) { isNull =>
            val stringColWithNulls = possibleStringResult
            withResource(Scalar.fromString(CastOptions.TO_PRETTY_STRING_OPTIONS.nullString)) {
              nullString =>
                GpuColumnVector.from(isNull.ifElse(nullString, stringColWithNulls), StringType)
            }
          }
        } else {
          GpuColumnVector.from(possibleStringResult.incRefCount(), StringType)
        }
      }
    }
  }
}