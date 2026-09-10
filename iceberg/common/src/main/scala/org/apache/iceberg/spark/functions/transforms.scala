/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.functions

import scala.util.Try

import com.nvidia.spark.rapids.iceberg.findFieldIndex
import org.apache.iceberg.Schema
import org.apache.iceberg.transforms.Transform

import org.apache.spark.sql.types.{DataType, StructType}


/**
 * Iceberg partition transform types.
 * <br/>
 * We need to build our own transform types because the ones in iceberg are
 */
sealed trait GpuTransform {
  def support(inputType: DataType, nullable: Boolean): Boolean
}

case class GpuBucket(bucket: Int) extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean): Boolean =
    GpuBucketExpression.isSupportedValueType(inputType)
}

case object GpuYears extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean) = true
}

case object GpuMonths extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean) = true
}

case object GpuDays extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean) = true
}

case object GpuHours extends GpuTransform {
  def support(inputType: DataType, nullable: Boolean) = true
}

case class GpuTruncate(width: Int) extends GpuTransform {
  override def support(inputType: DataType, nullable: Boolean): Boolean =
    GpuTruncateExpression.isSupportedValueType(inputType)
}

case object GpuIdentity extends GpuTransform {
  override def support(inputType: DataType, nullable: Boolean): Boolean = true
}

object GpuTransform {
  def apply(transform: String): GpuTransform = {
    if (transform.startsWith("bucket")) {
      val bucket = transform.substring("bucket[".length, transform.length - 1).toInt
      GpuBucket(bucket)
    } else if (transform.startsWith("year")) {
      GpuYears
    } else if (transform.startsWith("month")) {
      GpuMonths
    } else if (transform.startsWith("day")) {
      GpuDays
    } else if (transform.startsWith("hour")) {
      GpuHours
    } else if (transform.startsWith("truncate")) {
      val width = transform.substring("truncate[".length, transform.length - 1).toInt
      GpuTruncate(width)
    } else if (transform.startsWith("identity")) {
      GpuIdentity
    } else {
      throw new IllegalArgumentException(s"Unsupported transform: $transform")
    }
  }

  def apply(icebergTransform: Transform[_, _]): GpuTransform = {
    GpuTransform(icebergTransform.toString)
  }

  def tryFrom(icebergTransform: Transform[_, _]): Try[GpuTransform] = {
    Try {
      GpuTransform(icebergTransform)
    }
  }
}

case class GpuFieldTransform(sourceFieldId: Int, transform: GpuTransform) {
  def supports(inputType: StructType, inputSchema: Schema): Boolean = {
    // Iceberg allows partition source fields to reference nested-leaf field ids
    // (e.g. `bucket(4, contact.email)`). Those ids do not appear in
    // `schema.columns()` (top level only), so an Option-returning lookup lets
    // us refuse GPU and fall back to CPU instead of throwing.
    findFieldIndex(inputSchema, sourceFieldId) match {
      case Some(idx) =>
        val sparkField = inputType.fields(idx)
        transform.support(sparkField.dataType, sparkField.nullable)
      case None => false
    }
  }
}


