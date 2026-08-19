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

package com.nvidia.spark.rapids.iceberg.utils

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.GpuColumnVector
import org.apache.iceberg.types.Types.StructType

import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Gpu version of {@link org.apache.iceberg.util.StructProjection}
 */
class GpuStructProjection(val positionMap: Array[Int]) {
  def project(batch: ColumnarBatch): ColumnarBatch = {
    val cols = new Array[ColumnVector](positionMap.length)
    closeOnExcept(cols) { _ =>
      for (idx <- positionMap.indices) {
        cols(idx) = batch.column(positionMap(idx))
          .asInstanceOf[GpuColumnVector]
          .incRefCount()
      }

      new ColumnarBatch(cols, batch.numRows())
    }
  }
}

object GpuStructProjection {
  def apply(dataType: StructType, projectType: StructType): GpuStructProjection = {
    if (projectType.fields().asScala.exists(!_.`type`().isPrimitiveType)) {
      throw new IllegalArgumentException(
        "Gpu struct projection currently only supports primitive types")
    }
    val positionMap = new Array[Int](projectType.fields().size())

    for (idx <- positionMap.indices) {
      val projectedField = projectType.fields().get(idx)
      dataType.fields()
        .asScala
        .zipWithIndex
        .find(p => p._1.fieldId() == projectedField.fieldId())
        match {
         case Some(f) => positionMap(idx) = f._2
         case None => throw new IllegalArgumentException(
           s"Field ${projectedField.name()} not found in projection source")
       }
    }

    new GpuStructProjection(positionMap)
  }
}
