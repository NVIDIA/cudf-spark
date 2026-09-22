/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import java.io.File

trait RapidsTestsBaseTrait {

  protected val basePath: String =
    new File(System.getProperty("java.io.tmpdir"), "unit-tests-working-home").getAbsolutePath

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  def shouldRun(testName: String): Boolean = {
    BackendTestSettings.shouldRun(getClass.getCanonicalName, testName)
  }

  protected def getJavaMajorVersion(): Int = {
    val version = System.getProperty("java.version")
    // Allow these formats:
    // 1.8.0_72-ea
    // 9-ea
    // 9
    // 11.0.1
    val versionRegex = """(1\.)?(\d+)([._].+)?""".r
    version match {
      case versionRegex(_, major, _) => major.toInt
      case _ => throw new IllegalStateException(s"Cannot parse java version: $version")
    }
  }
}
