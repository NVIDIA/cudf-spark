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

try {
  val serializer = "com.nvidia.spark.ParquetCachedBatchSerializer"
  require(spark.conf.get("spark.sql.cache.serializer") == serializer)

  spark.range(2000).selectExpr("id", "CAST(id AS STRING) AS value")
    .createOrReplaceTempView("root_safety_source")
  spark.sql("CACHE TABLE root_safety_source")
  spark.sql("CREATE OR REPLACE TEMP VIEW root_safety_view AS " +
    "SELECT id + 1 AS id, value FROM root_safety_source")
  spark.sql("CACHE TABLE root_safety_view")

  // Building a second cache from the first exercises the CPU columnar cache read
  // that fails when the Parquet schema converter loses the Hadoop configuration.
  val derivedCache = spark.table("root_safety_source").selectExpr("value").cache()
  require(derivedCache.count() == 2000)

  val source = spark.sql("SELECT id, value FROM root_safety_source ORDER BY id")
  val view = spark.sql("SELECT id, value FROM root_safety_view ORDER BY id")
  val scan = source.queryExecution.executedPlan.collectFirst {
    case plan if plan.getClass.getName.contains("InMemoryTableScanExec") => plan
  }.getOrElse(throw new IllegalStateException("Missing in-memory table scan"))
  require(scan.supportsColumnar, "The cache scan must support columnar output")

  // Spark SQL may choose row output for collect(). Execute the real columnar cache scan to
  // exercise ShimCurrentBatchIterator and its access to Spark's ParquetColumnVector.
  val totals = scan.executeColumnar().map { batch =>
    var sum = 0L
    var row = 0
    while (row < batch.numRows()) {
      sum += batch.column(0).getLong(row)
      row += 1
    }
    (row, sum)
  }.collect()
  require(totals.map(_._1).sum == 2000)
  require(totals.map(_._2).sum == 1999000L)
  require(source.collect().length == 2000)
  require(view.collect().length == 2000)

  println("ROOT_SAFETY_PASS: cached table, view, and derived cache returned 2000 rows; " +
    "columnar reader returned 2000 rows with --jars")
  System.exit(0)
} catch {
  case error: Throwable =>
    error.printStackTrace()
    System.exit(1)
}
