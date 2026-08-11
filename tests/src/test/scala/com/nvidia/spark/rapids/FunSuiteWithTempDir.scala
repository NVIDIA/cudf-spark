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

import java.io.File

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

// creates temp dir before test and deletes after test
trait FunSuiteWithTempDir extends AnyFunSuite with BeforeAndAfterEach {
  val TEST_FILES_ROOT: File = TestUtils.getTempDir(this.getClass.getSimpleName)

  protected override def beforeEach(): Unit = {
    TEST_FILES_ROOT.mkdirs()
  }

  protected override def afterEach(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(TEST_FILES_ROOT)
  }
}
