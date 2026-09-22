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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.{RapidsConf, RapidsReaderType}

import org.apache.spark.SparkException
import org.apache.spark.sql.{MetadataCacheSuite, MetadataCacheV1Suite, MetadataCacheV2Suite}
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

private[suites] trait RapidsMetadataCacheRecoveryHintTests {
  self: MetadataCacheSuite with RapidsSQLTestsTrait =>

  protected def expectRefreshHint: Boolean

  private def exceptionMessages(error: Throwable): String = {
    val messages = new StringBuilder
    var current = error
    while (current != null) {
      messages.append(current.toString)
      messages.append('\n')
      current = current.getCause
    }
    messages.toString()
  }

  Seq(RapidsReaderType.COALESCING, RapidsReaderType.MULTITHREADED).foreach { readerType =>
    testRapids(s"missing ORC file includes recovery guidance - $readerType") {
      withSQLConf(RapidsConf.ORC_READER_TYPE.key -> readerType.toString) {
        withTempPath { location =>
          spark.range(start = 0, end = 100, step = 1, numPartitions = 3)
            .write.orc(location.getAbsolutePath)

          val df = spark.read.orc(location.getAbsolutePath)
          assert(df.count() == 100)
          deleteOneFileInDirectory(location)

          val messages = exceptionMessages(intercept[SparkException](df.count()))
          assert(messages.contains("recreating the Dataset/DataFrame involved"))
          assert(messages.contains("REFRESH TABLE") === expectRefreshHint)
        }
      }
    }
  }
}

class RapidsMetadataCacheV1Suite extends MetadataCacheV1Suite with RapidsSQLTestsTrait
    with RapidsMetadataCacheRecoveryHintTests {
  override protected val expectRefreshHint: Boolean = true
}

class RapidsMetadataCacheV2Suite extends MetadataCacheV2Suite with RapidsSQLTestsTrait
    with RapidsMetadataCacheRecoveryHintTests {
  override protected val expectRefreshHint: Boolean = false
}
