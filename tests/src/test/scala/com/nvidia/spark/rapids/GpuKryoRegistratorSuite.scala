/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class GpuKryoRegistratorSuite extends AnyFunSuite with BeforeAndAfter {

  before {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  after {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  test("GpuKryoRegistrator") {
    val conf =  new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.nvidia.spark.rapids.GpuKryoRegistrator")
      .set(RapidsConf.SQL_ENABLED.key, "true")
      .set(RapidsConf.TEST_CONF.key, "true")
      .set(RapidsConf.EXPLAIN.key, "ALL")

    TestUtils.withGpuSparkSession(conf) { spark =>
      import spark.implicits._
      val matched = SparkEnv.get.serializer match {
        case _: KryoSerializer => true
        case _ => false
      }
      assert(matched, "KryoSerializer is not found")
      val leftDf = Seq(
        (3, "history"),
        (2, "math"),
        (5, "history"),
        (4, "math")).toDF("std_id", "dept_name")

      val rightDf = Seq(
        ("piano", 3),
        ("math", 1),
        ("guitar", 3)).toDF("dept_name", "std_id")

      val df = leftDf.join(rightDf.hint("broadcast"),
        leftDf("dept_name").equalTo(rightDf("dept_name")))
      df.collect()
    }
  }

}
