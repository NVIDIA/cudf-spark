/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.hadoop.mapred.JobID

import org.apache.spark.internal.io.SparkHadoopWriterUtils

/**
 * This shim object uses the underlying fix from SparkHadoopWriterUtils in case this
 *  logic is updated in the future for other versions of Spark 3.3
 */
object RapidsHadoopWriterUtils {
  // SPARK-41448 create a jobID directly from the jobTrackerID
  def createJobID(jobTrackerID: String, id: Int): JobID = {
    SparkHadoopWriterUtils.createJobID(jobTrackerID, id)
  }
}

