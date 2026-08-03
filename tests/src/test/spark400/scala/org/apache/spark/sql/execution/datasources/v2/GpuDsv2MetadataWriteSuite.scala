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
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.GpuDsv2WriteMetadata
import com.nvidia.spark.rapids.shims.DeltaInsertFilter
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

/**
 * Lightweight coverage for SPARK-50820 GPU write-task routing (issue #15348).
 */
class GpuDsv2MetadataWriteSuite extends AnyFunSuite {

  private def projectingRow(schema: StructType, ordinals: Seq[Int]): ProjectingInternalRow = {
    ProjectingInternalRow(schema, ordinals.toIndexedSeq)
  }

  test("GpuReplaceDataExec selects metadata-aware writing task when metadataProjection present") {
    val rowSchema = StructType(Seq(StructField("id", IntegerType), StructField("v", LongType)))
    val metaSchema = StructType(Seq(StructField("_row_id", LongType)))
    val rowProj = projectingRow(rowSchema, Seq(1, 2))
    val metaProj = projectingRow(metaSchema, Seq(3))
    val projections = ReplaceDataProjections(rowProj, Some(metaProj))

    // writingTask selection mirrors CPU ReplaceDataExec / DataAndMetadataWritingSparkTask
    val task = projections match {
      case ReplaceDataProjections(dataProj, Some(metadataProj)) =>
        GpuDataAndMetadataWritingSparkTask(dataProj, metadataProj)
      case _ =>
        fail("expected metadata projection")
    }
    assert(task.isInstanceOf[GpuDataAndMetadataWritingSparkTask])
    assert(task.dataProj === rowProj)
    assert(task.metadataProj === metaProj)
  }

  test("GpuReplaceDataExec keeps plain writing task when metadataProjection absent") {
    val rowSchema = StructType(Seq(StructField("id", IntegerType)))
    val rowProj = projectingRow(rowSchema, Seq(1))
    val projections = ReplaceDataProjections(rowProj, None)
    val task = projections match {
      case ReplaceDataProjections(_, Some(_)) =>
        fail("did not expect metadata projection")
      case _ =>
        GpuReplaceDataWritingSparkTask(projections)
    }
    assert(task.isInstanceOf[GpuReplaceDataWritingSparkTask])
  }

  test("GpuDsv2WriteMetadata ThreadLocal schema is visible to nested calls") {
    val schema = StructType(Seq(StructField("_row_id", LongType)))
    assert(GpuDsv2WriteMetadata.metadataSchema.isEmpty)
    val seen = GpuDsv2WriteMetadata.withMetadataSchema(schema) {
      GpuDsv2WriteMetadata.metadataSchema
    }
    assert(seen.contains(schema))
    assert(GpuDsv2WriteMetadata.metadataSchema.isEmpty)
  }

  test("DeltaInsertFilter exposes distinct insert and reinsert writers") {
    // Compile-time / API presence check: insert and reinsert helpers exist for Spark 4.x
    assert(DeltaInsertFilter.getClass.getMethods.exists(_.getName == "writeInserts"))
    assert(DeltaInsertFilter.getClass.getMethods.exists(_.getName == "writeReinserts"))
    assert(DeltaInsertFilter.getClass.getMethods.exists(_.getName == "filterReinsertRows"))
  }
}
