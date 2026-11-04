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
package org.apache.spark.sql.rapids.execution

import scala.concurrent.duration.{Duration, SECONDS}

import org.apache.spark.sql.execution.{ExternalRDDScanExec, RDDScanExec, SparkPlan}
import org.apache.spark.sql.types._

/** Admission uses completed shuffle metrics and a conservative bound on the assembled layout. */
private[execution] object DriverlessBroadcastBuildAdmission {
  def supportsPlan(plan: SparkPlan): Boolean = rejectionReason(plan).isEmpty

  def rejectionReason(plan: SparkPlan): Option[String] = {
    if (!supportsSchema(plan.schema)) {
      Some(s"unsupported build schema: ${plan.schema.simpleString}")
    } else if (plan.exists {
      case _: RDDScanExec | _: ExternalRDDScanExec[_] => true
      case _ => false
    }) {
      Some("opaque RDD build source")
    } else if (plan.exists(_.expressions.exists(!_.deterministic))) {
      Some("non-deterministic build expression")
    } else {
      None
    }
  }

  def waitTimeout(seconds: Long): Duration = {
    // SQLConf represents a disabled broadcast timeout as Long.MaxValue seconds.
    // Scala finite durations cannot represent more than Long.MaxValue nanoseconds.
    if (seconds < 0 || seconds > Long.MaxValue / 1000000000L) {
      Duration.Inf
    } else {
      Duration(seconds, SECONDS)
    }
  }

  private def supportsType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
         DateType | TimestampType | TimestampNTZType | NullType => true
    case _: DecimalType | _: StringType | _: YearMonthIntervalType | _: DayTimeIntervalType => true
    case _ => false
  }

  def supportsSchema(schema: StructType): Boolean =
    schema.fields.forall(f => supportsType(f.dataType))

  /**
   * Packed input already contains the value buffers. Concatenation can add validity masks
   * for rows whose input batch had no nulls, and can widen string offsets to 64 bits.
   * Charge a complete padded mask for every column, regardless of declared nullability,
   * plus complete 64-bit offsets for every string column. Existing masks and offsets are
   * not subtracted, so both a concatenated batch and an unchanged packed batch are covered.
   * Nested children require their own element counts and are outside this bound.
   */
  def upperBound(
      partitionBytes: Array[Long],
      uncompressedBytes: Long,
      rows: Long,
      schema: StructType): Option[BigInt] = {
    val reportedBytes = partitionBytes.foldLeft(BigInt(0))(_ + _)
    if (!supportsSchema(schema) || partitionBytes.exists(_ < 0) || uncompressedBytes < 0 ||
        rows < 0 || rows > Int.MaxValue ||
        (reportedBytes > 0 && rows == 0) ||
        (rows > 0 && schema.nonEmpty && uncompressedBytes == 0)) {
      None
    } else {
      // MapStatus sizes can saturate. The completed writer's dataSize metric supplies the
      // uncompressed payload total independently of Spark's compact block-size encoding.
      val payload = reportedBytes.max(BigInt(uncompressedBytes))
      val maskBytes = ((BigInt(rows) + 511) / 512) * 64
      val stringColumns = schema.fields.count(_.dataType.isInstanceOf[StringType])
      val offsets = (BigInt(rows) + 1) * 8 * stringColumns
      Some(payload + maskBytes * schema.length + offsets)
    }
  }

  def accepts(bound: Option[BigInt], maxBytes: Long): Boolean =
    maxBytes > 0 && bound.exists(_ < maxBytes)
}
