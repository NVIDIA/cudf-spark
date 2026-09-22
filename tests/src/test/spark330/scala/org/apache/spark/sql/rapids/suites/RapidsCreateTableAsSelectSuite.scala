/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
package org.apache.spark.sql.rapids.suites

import java.nio.file.Files

import org.scalatest.Ignore

import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait
import org.apache.spark.sql.sources.CreateTableAsSelectSuite
import org.apache.spark.util.Utils

class RapidsCreateTableAsSelectSuite
    extends CreateTableAsSelectSuite with RapidsSQLTestsTrait {

  private val writePermissionTest =
    "CREATE TABLE USING AS SELECT based on the file without write permission"

  private lazy val enforcesWritePermissions: Boolean = {
    val probeDir = Utils.createTempDir()
    try {
      probeDir.setWritable(false) && !Files.isWritable(probeDir.toPath)
    } finally {
      probeDir.setWritable(true)
      Utils.deleteRecursively(probeDir)
    }
  }

  override def tags: Map[String, Set[String]] = {
    if (enforcesWritePermissions) {
      super.tags
    } else {
      val inheritedTags = super.tags
      inheritedTags.updated(
        writePermissionTest,
        inheritedTags.getOrElse(writePermissionTest, Set.empty) + classOf[Ignore].getName)
    }
  }
}
