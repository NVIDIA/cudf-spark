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
 * For a `MAP<STRING, ARRAY<STRING>>` (and `MAP<STRING, STRING>`) schema the map is extracted as a
 * "raw" map: keys, values and array elements are raw JSON byte ranges (surrounding quotes stripped
 * but NOT JSON-unescaped). Verified against Spark 3.5.5, the output diverges from Spark CPU on
 * three documented corner cases (see docs/compatibility.md):
 *  - escape sequences (e.g. `\"`, `\\`, `\\uXXXX`) are kept verbatim rather than
 *    unescaped/normalized;
 *  - for `ARRAY<STRING>`, object / nested-array elements are returned as their raw JSON substring
 *    rather than Spark's re-serialized form;
 *  - numeric elements keep their raw JSON token; Spark re-renders numbers, so non-canonical
 *    spellings differ (`007` -> `"7"` with `allowNumericLeadingZeros`, `1.00000` -> `"1.0"`,
 *    `1e2` -> `"100.0"`), and non-numeric numbers stay bare while Spark quotes them (`NaN` ->
 *    `"NaN"`, `Infinity` -> `"Infinity"`).
 * All of these differ from Spark on ALL versions, including 4.0.0+: `from_json` on a string column
 * parses via a Reader (Spark's `CreateJacksonParser.utf8String`), so Spark 4.0.0's
 * `spark.sql.json.enableExactStringParsing` (default `true`) does not apply. Its raw-source-byte
 * path (`JacksonParser`) fires only for `Array[Byte]` / file sources (e.g. `spark.read.json`),
 * never a Reader, so the CPU always re-serializes non-string tokens (cases 2/3, via
 * `copyCurrentStructure`) and always unescapes string tokens (case 1, via `getText`).
 * The following MATCH Spark and are NOT divergences: canonical elements whose raw text already
 * equals Spark's rendering (e.g. `1`, `1.5`, `true`); a map value that is not a JSON array and not
 * the JSON `null` literal nulls the whole row (PERMISSIVE bad-record); duplicate keys kept in
 * document order (matches Spark 3.5.x; later Spark may de-dup per `spark.sql.mapKeyDedupPolicy`).
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
        // Raw extraction (no unescaping, duplicate keys kept, non-string elements as raw text) --
        // see the class doc and docs/compatibility.md.
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
