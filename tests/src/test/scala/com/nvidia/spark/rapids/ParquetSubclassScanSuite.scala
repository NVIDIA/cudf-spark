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

package com.nvidia.spark.rapids

import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

/** Stands in for a ParquetFileFormat subclass, such as Delta's, that no provider claims. */
class UnclaimedParquetFileFormat extends ParquetFileFormat

class ParquetSubclassScanSuite extends SparkQueryCompareTestSuite {
  // Databricks shims apply their own scan rules for their ParquetFileFormat subclasses.
  if (!VersionUtils.isDataBricks) {
    testGpuFallback("unclaimed ParquetFileFormat subclass scan falls back to CPU",
      "FileSourceScanExec",
      spark => spark.read.format(classOf[UnclaimedParquetFileFormat].getName)
        .load(TestResourceFinder.getResourcePath("file-splits.parquet")),
      execsAllowedNonGpu = Seq("FileSourceScanExec")) {
      frame => frame
    }
  }
}
