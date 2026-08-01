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

import java.util.Locale

import ai.rapids.cudf
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression, NvtxRegistry}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.JSONUtils
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

/**
 * GPU implementation of Spark's `from_json` (`JsonToStructs`).
 *
 * For a `MAP<STRING, STRING>` or `MAP<STRING, ARRAY<STRING>>` schema the map's keys, values and
 * array elements are rendered to match Spark CPU's Jackson output (see docs/compatibility.md):
 * string tokens are de-quoted and JSON-unescaped; numbers are re-rendered canonically (integer
 * leading zeros and `-0` stripped, floats in Java `Double.toString` form); the `NaN`/`Infinity`
 * spellings accepted with `allowNonNumericNumbers` render as their quoted canonical forms; nested
 * object/array values and elements are re-serialized compactly in document order. A map value that
 * is the JSON `null` literal keeps its pair and yields a SQL NULL value (for `ARRAY<STRING>`, a
 * null inner list), while for `ARRAY<STRING>` a value that is non-null but not a JSON array (a
 * scalar or object) nulls the whole row (PERMISSIVE bad-record). A number the JSON parser refuses
 * nulls the whole row the same way: the parser caps a number's digit count (signs, `.`, `e`/`E`,
 * and leading zeros excluded) and applies that cap to integers and floats alike, so a number past
 * it makes the record a Spark bad record whether it is a value, an array element, or buried inside
 * a nested value. Duplicate keys are kept in document order on every supported Spark version:
 * `JacksonParser.convertMap` builds `ArrayBasedMapData` directly, so `spark.sql.mapKeyDedupPolicy`
 * never applies to `from_json`.
 *
 * The rendering is the same on every Spark version, including 4.0.0+: `from_json` on a string
 * column parses via a Reader (Spark's `CreateJacksonParser.utf8String`), so Spark 4.0.0's
 * `spark.sql.json.enableExactStringParsing` (default `true`) does not apply. Its raw-source-byte
 * path (`JacksonParser`) fires only for `Array[Byte]` / file sources (e.g. `spark.read.json`),
 * never a Reader, so the CPU always re-serializes non-string tokens (via `copyCurrentStructure`)
 * and always unescapes string tokens (via `getText`) -- the same rendering produced here. Known
 * caveats (docs/compatibility.md): invalid UTF-8 is out of scope (Spark's `InputStreamReader`
 * replaces invalid byte sequences with U+FFFD while the GPU copies raw bytes through unchanged),
 * and float parse fidelity matches the `get_json_object` path (mantissas beyond 2^53 are not
 * correctly rounded, so an adversarial double can differ by 1 ULP and re-render to a different
 * shortest string).
 */
case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerantShim {
  import GpuJsonReadCommon._

  private lazy val parsedOptions = new JSONOptions(
    options,
    timeZoneId.get,
    SQLConf.get.columnNameOfCorruptRecord)

  private lazy val cudfOptions = GpuJsonReadCommon.cudfJsonOptions(parsedOptions)

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    NvtxRegistry.JSON_TO_STRUCTS {
      schema match {
        // The JNI name keeps its historical "Raw"; the output here is Spark-rendered, not raw.
        case MapType(StringType, ArrayType(StringType, _), _) =>
          JSONUtils.extractRawMapFromJsonString(input.getBase, cudfOptions,
            JSONUtils.MapValueType.ARRAY_OF_STRING)
        case MapType(StringType, StringType, _) =>
          JSONUtils.extractRawMapFromJsonString(input.getBase, cudfOptions,
            JSONUtils.MapValueType.STRING)
        // Defensive: GpuOverrides.tagExprForGpu gates the allowed map shapes, so any other map
        // value type is unreachable today. Fail loudly if that gating is ever widened without
        // teaching this dispatch the new MapValueType, instead of silently extracting as STRING.
        case MapType(_, valueType, _) =>
          throw new IllegalArgumentException(
            s"GpuJsonToStructs does not support map value type $valueType (schema $schema).")
        case struct: StructType =>
          val parsedStructs = JSONUtils.fromJSONToStructs(input.getBase, makeSchema(struct),
            cudfOptions, parsedOptions.locale == Locale.US)
          val hasDateTime = TrampolineUtil.dataTypeExistsRecursively(struct, t =>
            t.isInstanceOf[DateType] || t.isInstanceOf[TimestampType]
          )
          if (hasDateTime) {
            withResource(parsedStructs) { _ =>
              convertDateTimeType(parsedStructs, struct, parsedOptions)
            }
          } else {
            parsedStructs
          }
        case _ => throw new IllegalArgumentException(
          s"GpuJsonToStructs currently does not support schema of type $schema.")
      }
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = schema.asNullable

  override def nullable: Boolean = true
}
