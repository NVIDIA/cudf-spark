/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuLiteral, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.window.{GpuSpecifiedWindowFrameMetaBase, GpuWindowExpressionMetaBase, ParsedBoundary, RangeBoundaryValue}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SpecifiedWindowFrame, WindowExpression}
import org.apache.spark.sql.rapids.shims.Spark32XShimsUtils
import org.apache.spark.sql.types.{DataType, DayTimeIntervalType}

class GpuSpecifiedWindowFrameMeta(
    windowFrame: SpecifiedWindowFrame,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends GpuSpecifiedWindowFrameMetaBase(windowFrame, conf, parent, rule) {

  override def getAndTagOtherTypesForRangeFrame(bounds: Expression, isLower: Boolean): Long = {
    bounds match {
      case Literal(value, _: DayTimeIntervalType) => value.asInstanceOf[Long]
      case _ =>super.getAndTagOtherTypesForRangeFrame(bounds, isLower)
    }
  }
}

class GpuWindowExpressionMeta(
    windowExpression: WindowExpression,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_,_,_]],
    rule: DataFromReplacementRule)
  extends GpuWindowExpressionMetaBase(windowExpression, conf, parent, rule) {

  override def tagOtherTypesForRangeFrame(bounds: Expression): Unit = {
    bounds match {
      case Literal(_, _: DayTimeIntervalType) => // Supported
      case _ => super.tagOtherTypesForRangeFrame(bounds)
    }
  }
}

object GpuWindowUtil {

  /**
   * Check if the type of RangeFrame is valid in GpuWindowSpecDefinition
   * @param orderSpecType the first order by data type
   * @param ft the first frame boundary data type
   * @return true to valid, false to invalid
   */
  def isValidRangeFrameType(orderSpecType: DataType, ft: DataType): Boolean = {
    Spark32XShimsUtils.isValidRangeFrameType(orderSpecType, ft)
  }

  def getRangeBoundaryValue(boundary: Expression): ParsedBoundary = boundary match {
    case GpuLiteral(value, _: DayTimeIntervalType) =>
      var x = value.asInstanceOf[Long]
      if (x == Long.MinValue) x = Long.MaxValue
      ParsedBoundary(isUnbounded = false, RangeBoundaryValue.long(Math.abs(x)))
    case anything => throw new UnsupportedOperationException("Unsupported window frame" +
      s" expression $anything")
  }
}
