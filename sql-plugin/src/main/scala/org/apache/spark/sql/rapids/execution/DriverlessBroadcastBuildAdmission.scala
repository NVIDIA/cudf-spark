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

/** Planning eligibility and timeout conversion for the driverless build path. */
private[execution] object DriverlessBroadcastBuildAdmission {
  def requireSupportedRowCount(rows: Long): Unit = {
    require(rows >= 0 && rows <= Int.MaxValue,
      s"Driverless broadcast requires a single cuDF batch, but its build contains $rows rows; " +
        s"the row limit is ${Int.MaxValue}.")
  }

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

}
