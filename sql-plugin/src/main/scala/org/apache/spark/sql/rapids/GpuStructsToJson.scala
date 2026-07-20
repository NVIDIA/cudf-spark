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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{CastOptions, DataFromReplacementRule, GpuCast, GpuColumnVector, GpuExpression, GpuUnaryExpression, RapidsConf, RapidsMeta, UnaryExprMeta}
import com.nvidia.spark.rapids.GpuOverrides
import com.nvidia.spark.rapids.shims.LegacyBehaviorPolicyShim

import org.apache.spark.sql.catalyst.expressions.{Expression, StructsToJson}
import org.apache.spark.sql.catalyst.json.GpuJsonUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DataType, DateType, MapType, StringType, TimestampType}

class GpuStructsToJsonMeta(
    expr: StructsToJson,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule
  ) extends UnaryExprMeta[StructsToJson](expr, conf, parent, rule) {

  // Timezone only affects JSON output when the input contains timestamps.
  override def isTimeZoneSupported: Boolean = true

  override def tagExprForGpu(): Unit = {
    if (expr.options.get("pretty").exists(_.equalsIgnoreCase("true"))) {
      willNotWorkOnGpu("to_json option pretty=true is not supported")
    }
    val options = GpuJsonUtils.parseJSONOptions(expr.options)
    val hasDates = TrampolineUtil.dataTypeExistsRecursively(expr.child.dataType,
      _.isInstanceOf[DateType])
    if (hasDates) {
      GpuJsonUtils.dateFormatInWrite(options) match {
        case "yyyy-MM-dd" =>
        case dateFormat =>
          // we can likely support other formats but we would need to add tests
          // tracking issue is https://github.com/NVIDIA/spark-rapids/issues/9602
          willNotWorkOnGpu(s"Unsupported dateFormat '$dateFormat' in to_json")
      }
    }
    val hasTimestamps = TrampolineUtil.dataTypeExistsRecursively(expr.child.dataType,
      _.isInstanceOf[TimestampType])
    if (hasTimestamps) {
      GpuJsonUtils.timestampFormatInWrite(options) match {
        case "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]" =>
        case timestampFormat =>
          // we can likely support other formats but we would need to add tests
          // tracking issue is https://github.com/NVIDIA/spark-rapids/issues/9602
          willNotWorkOnGpu(s"Unsupported timestampFormat '$timestampFormat' in to_json")
      }
      if (expr.options.exists { case (key, _) => key.equalsIgnoreCase("timeZone") } &&
          options.zoneId.normalized() != GpuOverrides.UTC_TIMEZONE_ID) {
        willNotWorkOnGpu(s"Unsupported timeZone '${options.zoneId}' in to_json")
      }
      if (expr.zoneId.normalized() != GpuOverrides.UTC_TIMEZONE_ID) {
        // we hard-code the timezone `Z` in GpuCast.castTimestampToJson
        // so we need to fall back if expr different timeZone is specified
        willNotWorkOnGpu(s"Unsupported timeZone '${expr.zoneId}' in to_json")
      }
    }

    if ((hasDates || hasTimestamps) && LegacyBehaviorPolicyShim.isLegacyTimeParserPolicy) {
      willNotWorkOnGpu("LEGACY timeParserPolicy is not supported in GpuJsonToStructs")
    }

    val hasNonStringMapKey = TrampolineUtil.dataTypeExistsRecursively(expr.child.dataType, {
      case mt: MapType if !mt.keyType.isInstanceOf[StringType] => true
      case _ => false
    })
    if (hasNonStringMapKey) {
      willNotWorkOnGpu("Only strings are supported as keys for Maps")
    }
  }

  override def convertToGpu(child: Expression): GpuExpression =
    GpuStructsToJson(expr.options, child, expr.timeZoneId)
}

case class GpuStructsToJson(
  options: Map[String, String],
  child: Expression,
  timeZoneId: Option[String] = None) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val ignoreNullFields = options.getOrElse("ignoreNullFields", SQLConf.get.getConfString(
      SQLConf.JSON_GENERATOR_IGNORE_NULL_FIELDS.key)).toBoolean
    GpuCast.castToString(input.getBase, child.dataType,
      new CastOptions(legacyCastComplexTypesToString = false, ansiMode = false,
        stringToDateAnsiMode = false, castToJsonString = true,
        ignoreNullFieldsInStructs = ignoreNullFields))
  }

  override def dataType: DataType = StringType
}
