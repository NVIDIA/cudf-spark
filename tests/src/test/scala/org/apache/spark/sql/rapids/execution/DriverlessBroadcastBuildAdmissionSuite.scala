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

import com.nvidia.spark.rapids.RapidsConf
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, LessThan, Literal, Rand}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.storage.BlockManagerId

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
    val scan = DriverlessBroadcastTestShim.scan(Seq(key), Seq(InternalRow(1)))
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

  test("executor admission counts replicas independently of host placement") {
    val colocated = Seq(BlockManagerId("driver", "same-host", 1000),
      BlockManagerId("executor-1", "same-host", 1001),
      BlockManagerId("executor-2", "same-host", 1002))
    val ids = DriverlessBroadcastPolicy.executorIds(colocated.iterator)
    assert(ids == Set("executor-1", "executor-2"))
    assert(DriverlessBroadcastPolicy.executorRejection(ids, 1).isDefined)
    val executors = (1 to 32).map(i => s"executor-$i").toSet
    assert(DriverlessBroadcastPolicy.executorRejection(executors, 32).isEmpty)
    assert(DriverlessBroadcastPolicy.executorRejection(executors + "executor-33", 32).isDefined)
    assert(DriverlessBroadcastPolicy.executorRejection(executors + "executor-1", 32).isEmpty)
    val conf = new RapidsConf(Map.empty[String, String])
    assert(conf.driverlessBroadcastMaxExecutors == 32)
    intercept[IllegalArgumentException] {
      new RapidsConf(Map("spark.rapids.sql.driverlessBroadcast.maxExecutors" -> "0"))
        .driverlessBroadcastMaxExecutors
    }
  }

}
