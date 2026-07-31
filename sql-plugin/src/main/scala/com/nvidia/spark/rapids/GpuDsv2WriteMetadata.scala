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

import org.apache.spark.sql.types.StructType

/**
 * Task-local metadata schema for DSv2 writers that receive metadata as a [[ColumnarBatch]].
 *
 * Spark's CPU path passes a [[org.apache.spark.sql.catalyst.ProjectingInternalRow]] which
 * carries its schema; GPU writers only see a [[org.apache.spark.sql.vectorized.ColumnarBatch]].
 * Writing tasks set this schema before calling `write(metadata, data)` / `reinsert(metadata, row)`.
 */
object GpuDsv2WriteMetadata {
  private val metadataSchemaLocal = new ThreadLocal[StructType]

  def withMetadataSchema[T](schema: StructType)(body: => T): T = {
    val prev = metadataSchemaLocal.get()
    metadataSchemaLocal.set(schema)
    try {
      body
    } finally {
      if (prev == null) {
        metadataSchemaLocal.remove()
      } else {
        metadataSchemaLocal.set(prev)
      }
    }
  }

  def metadataSchema: Option[StructType] = Option(metadataSchemaLocal.get())
}
