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
{"spark":["330","350","400"],"carryForward":"all"}
{"spark":["330db","332db","341db","350db143","400db173"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims.parquet

import org.apache.parquet.schema.LogicalTypeAnnotation

/**
 * Spark versions before SPARK-54220 treat Parquet UNKNOWN annotations as absent and
 * fall back to the physical Parquet type.
 */
object ParquetUnknownTypeAnnotationShims {
  def effectiveLogicalTypeAnnotation(
      annotation: LogicalTypeAnnotation): LogicalTypeAnnotation = {
    if (isUnknownTypeAnnotation(annotation)) null else annotation
  }

  def mapsToNullType(annotation: LogicalTypeAnnotation): Boolean = false

  private def isUnknownTypeAnnotation(annotation: LogicalTypeAnnotation): Boolean = {
    annotation != null &&
      annotation.getClass.getName.endsWith("UnknownLogicalTypeAnnotation")
  }
}
