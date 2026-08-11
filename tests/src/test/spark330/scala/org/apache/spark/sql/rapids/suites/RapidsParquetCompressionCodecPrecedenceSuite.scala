/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.parquet.ParquetCompressionCodecPrecedenceSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait
import org.apache.spark.util.VersionUtils

class RapidsParquetCompressionCodecPrecedenceSuite
  extends ParquetCompressionCodecPrecedenceSuite
  with RapidsSQLTestsBaseTrait {

  // Original: ParquetCompressionCodecPrecedenceSuite.scala
  //   lines 95-105 (Spark 3.3.0)
  // GPU's libcudf skips compression when compressed output >=
  // uncompressed (single-row table). Use 1000 rows so
  // compression actually produces smaller output.
  // https://github.com/NVIDIA/spark-rapids/issues/11416
  testRapids("Create parquet table with compression") {
    Seq(true, false).foreach { isPartitioned =>
      val codecs = Seq("UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD") ++
        (if (VersionUtils.isHadoop3) Seq("LZ4") else Seq())
      codecs.foreach { codec =>
        withTempDir { tmpDir =>
          val tbl = "TempParquetTable"
          withTable(tbl) {
            val base = tmpDir.toURI.toString.stripSuffix("/")
            val opts = s"OPTIONS('path'='$base/$tbl'," +
              s"'parquet.compression'='$codec')"
            val part = if (isPartitioned) "PARTITIONED BY (p)" else ""
            sql(s"CREATE TABLE $tbl USING Parquet $opts $part " +
              s"AS SELECT id AS col1, CAST(id % 2 AS INT) AS p " +
              s"FROM range(1000)")
            val dir = s"${tmpDir.getPath.stripSuffix("/")}/$tbl"
            val conf = spark.sessionState.newHadoopConf()
            val actual = for {
              f <- readAllFootersWithoutSummaryFiles(
                new Path(dir), conf)
              b <- f.getParquetMetadata.getBlocks.asScala
              c <- b.getColumns.asScala
            } yield c.getCodec.name()
            assert(actual.nonEmpty,
              s"No parquet column metadata found for codec $codec")
            assert(actual.distinct.forall(_ == codec),
              s"Expected $codec but got ${actual.distinct}")
          }
        }
      }
    }
  }
}
