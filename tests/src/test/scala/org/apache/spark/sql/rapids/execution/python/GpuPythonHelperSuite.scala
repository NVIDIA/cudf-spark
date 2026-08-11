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

package org.apache.spark.sql.rapids.execution.python

import com.nvidia.spark.rapids.{RapidsConf, TestUtils}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class GpuPythonHelperSuite extends AnyFunSuite with BeforeAndAfter {

  before {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  after {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  test("Python RMM pool disabled when 'NONE'") {
    val conf = new SparkConf().set(RapidsConf.RMM_POOL.key, "NONE")
    TestUtils.withGpuSparkSession(conf) { _ =>
      assertResult(false)(GpuPythonHelper.isPythonPooledMemEnabled)
    }
  }

}
