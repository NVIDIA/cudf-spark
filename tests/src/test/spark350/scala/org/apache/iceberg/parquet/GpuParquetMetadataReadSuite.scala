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
{"spark":["350"],"carryForward":"sameMinor"}
{"spark":["411"],"carryForward":"sameMinor"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.parquet

import java.nio.file.Files

import com.nvidia.spark.rapids.shims.GpuBatchScanExec
import org.apache.hadoop.fs.FileUtil
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.getBuilder

class GpuParquetMetadataReadSuite extends AnyFunSuite with Matchers {
  private def withSession(body: SparkSession => Unit): Unit = {
    // File cache is a startup-only setting; changing a shared session's SQLConf is insufficient.
    TrampolineUtil.cleanupAnyExistingSession()
    try {
      val spark = getBuilder().master("local[1]").appName("Iceberg footer reads")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.rapids.filecache.enabled", "true")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
      body(spark)
    } finally {
      TrampolineUtil.cleanupAnyExistingSession()
    }
  }

  test("Iceberg metadata reads preserve CPU and GPU results with dictionary predicates") {
    val warehouse = Files.createTempDirectory("iceberg-footer-reads")
    try {
      withSession { spark =>
        spark.conf.set("spark.sql.catalog.footer_test", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set("spark.sql.catalog.footer_test.type", "hadoop")
        spark.conf.set("spark.sql.catalog.footer_test.warehouse", warehouse.toUri.toString)
        spark.conf.set("spark.rapids.sql.enabled", "false")
        spark.sql("CREATE TABLE footer_test.default.data USING ICEBERG " +
          "TBLPROPERTIES ('format-version'='2') AS " +
          "SELECT id, CAST(id % 4 AS STRING) AS payload FROM range(0, 2000, 1, 2)")
        // The absent value is within the column's min/max, so dictionary filtering can reject it.
        for (value <- Seq("2", "1.5")) {
          val query = s"SELECT id, payload FROM footer_test.default.data " +
            s"WHERE payload = '$value' AND id > 100"
          spark.conf.set("spark.rapids.sql.enabled", "false")
          val expected = spark.sql(query).collect().toSeq.sortBy(_.getLong(0))
          spark.conf.set("spark.rapids.sql.enabled", "true")
          for (reader <- Seq("COALESCING", "MULTITHREADED")) {
            spark.conf.set("spark.rapids.sql.format.parquet.reader.type", reader)
            (0 until 2).foreach { _ =>
              val df = spark.sql(query)
              df.collect().toSeq.sortBy(_.getLong(0)) shouldBe expected
              df.queryExecution.executedPlan.find(_.isInstanceOf[GpuBatchScanExec])
                .isDefined shouldBe true
            }
          }
        }
      }
    } finally {
      FileUtil.fullyDelete(warehouse.toFile)
    }
  }
}
