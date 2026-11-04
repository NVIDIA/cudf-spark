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

import scala.concurrent.duration.Duration

import org.scalatest.funsuite.AnyFunSuite

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, LessThan, Literal, Rand}
import org.apache.spark.sql.execution.{FilterExec, LocalTableScanExec, ProjectExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class DriverlessBroadcastBuildAdmissionSuite extends AnyFunSuite {
  test("disabled broadcast timeout and huge positive timeouts remain infinite") {
    val conf = new SQLConf
    conf.setConfString("spark.sql.broadcastTimeout", "-1")
    assert(DriverlessBroadcastBuildAdmission.waitTimeout(conf.broadcastTimeout) == Duration.Inf)
    assert(DriverlessBroadcastBuildAdmission.waitTimeout(Long.MaxValue) == Duration.Inf)
    assert(DriverlessBroadcastBuildAdmission.waitTimeout(300) == Duration(300, "seconds"))
    assert(DriverlessBroadcastBuildAdmission.waitTimeout(0) == Duration.Zero)
  }

  test("non-determinism in a build projection or filter declines the driverless path") {
    val key = AttributeReference("key", IntegerType)()
    val scan = LocalTableScanExec(Seq(key), Seq(InternalRow(1)))
    assert(DriverlessBroadcastBuildAdmission.supportsPlan(scan))
    val randomProject = ProjectExec(Seq(Alias(Rand(Literal(42L)), "value")()), scan)
    assert(!DriverlessBroadcastBuildAdmission.supportsPlan(randomProject))
    assert(DriverlessBroadcastBuildAdmission.rejectionReason(randomProject)
      .exists(_.contains("non-deterministic")))
    val randomFilter = FilterExec(LessThan(Rand(Literal(42L)), Literal(0.5d)), scan)
    val deterministicProject = ProjectExec(Seq(key), randomFilter)
    assert(!DriverlessBroadcastBuildAdmission.supportsPlan(deterministicProject))
  }

  test("flat scalar schemas are eligible without relying on declared nullability") {
    val schema = StructType(Seq(BooleanType, ByteType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DateType, TimestampType, TimestampNTZType, NullType,
      DecimalType(38, 5), StringType, YearMonthIntervalType(), DayTimeIntervalType())
      .zipWithIndex.map { case (dataType, index) => StructField(s"c$index", dataType) })
    assert(DriverlessBroadcastBuildAdmission.supportsSchema(schema))
  }

  test("node admission counts hosts rather than executor instances") {
    val hosts = (1 to 20).map(i => s"host-$i").toSet
    assert(DriverlessBroadcastPolicy.nodeRejection(hosts, 20).isEmpty)
    assert(DriverlessBroadcastPolicy.nodeRejection(hosts + "host-21", 20).isDefined)
    assert(DriverlessBroadcastPolicy.nodeRejection(hosts + "host-1", 20).isEmpty)
    val conf = new RapidsConf(Map.empty[String, String])
    assert(conf.driverlessBroadcastMaxNodes == 20)
    intercept[IllegalArgumentException] {
      new RapidsConf(Map("spark.rapids.sql.driverlessBroadcast.maxNodes" -> "0"))
        .driverlessBroadcastMaxNodes
    }
  }

}
