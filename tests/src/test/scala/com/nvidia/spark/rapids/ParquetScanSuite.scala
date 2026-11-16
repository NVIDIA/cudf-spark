/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class ParquetScanSuite extends SparkQueryCompareTestSuite {
  private val fileSplitsParquet = frameFromParquet("file-splits.parquet")

  test("early close of a combining multithreaded Parquet reader releases deferred files") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      withCpuSparkSession { spark =>
        // Both files have physical data, but their different clipped schemas prevent combining.
        spark.range(0, 1000, 1, 1).toDF("a").write.parquet(s"$path/key=0")
        spark.range(0, 10, 1, 1).selectExpr("id AS a", "id AS b")
          .write.parquet(s"$path/key=1")
      }
      val conf = new SparkConf()
        .set(RapidsConf.PARQUET_READER_TYPE.key, "MULTITHREADED")
        .set(RapidsConf.READER_MULTITHREADED_COMBINE_THRESHOLD.key, "64m")
        .set(RapidsConf.READER_MULTITHREADED_READ_KEEP_ORDER.key, "true")
        .set("spark.sql.sources.useV1SourceList", "parquet")
        .set("spark.sql.files.maxPartitionBytes", "1g")
        .set("spark.sql.files.minPartitionNum", "1")
        .set("spark.rapids.sql.exec.CollectLimitExec", "false")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "CollectLimitExec")
      withGpuSparkSession({ spark =>
        val hostHandlesBefore = SpillFramework.stores.hostStore.numHandles
        val df = spark.read.option("mergeSchema", "true").parquet(path).limit(1)
        val scans = df.queryExecution.executedPlan.collect {
          case scan: GpuFileSourceScanExec => scan
        }
        assert(scans.size == 1, "Expected one GPU Parquet scan")
        assert(df.inputFiles.length == 2)
        assert(scans.head.inputRDD.getNumPartitions == 1)
        scans.head.readerFactory match {
          case factory: MultiFilePartitionReaderFactoryBase =>
            assert(factory.useMultiThread(Array(path)))
          case other => fail(s"Expected a multithreaded reader factory, got $other")
        }
        assert(df.collect().length == 1)
        // Ordered combining reads both files before decoding. LIMIT consumes only the first
        // group, so closing the reader must release the other file's deferred host buffer.
        assert(scans.head.metrics(GpuMetric.NUM_OUTPUT_BATCHES).value == 1)
        assert(SpillFramework.stores.hostStore.numHandles == hostHandlesBefore)
      }, conf)
    }
  }

  private val missingColsSchema = new StructType()
    .add("b", LongType).add("c", LongType).add("key", IntegerType)

  // None of the requested data columns exist in these files, so nothing is decoded and the
  // reader builds null batches, which must still be capped like decoded batches.
  private def withMissingColsData(f: String => Unit): Unit = withTempPath { dir =>
    val path = dir.getCanonicalPath
    withCpuSparkSession { spark =>
      (0 until 3).foreach { key =>
        spark.range(0, 3000, 1, 1).selectExpr("CAST(0 AS INT) AS a")
          .write.parquet(s"$path/key=$key")
      }
    }
    f(path)
  }

  private def missingColsConf(readerType: String, extra: (String, String)*): SparkConf = {
    val conf = new SparkConf()
      .set(RapidsConf.PARQUET_READER_TYPE.key, readerType)
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(RapidsConf.READER_MULTITHREADED_READ_KEEP_ORDER.key, "true")
      .set(RapidsConf.READER_MULTITHREADED_COMBINE_THRESHOLD.key, "64m")
      .set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "2000")
    extra.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  for (readerType <- Seq("PERFILE", "COALESCING", "MULTITHREADED");
      (name, extra) <- Seq(
        "byte target" -> Seq.empty[(String, String)],
        // A byte target large enough that the row limit, not the bytes, sets the cap
        "row limit" -> Seq(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key -> "700",
          RapidsConf.GPU_BATCH_SIZE_BYTES.key -> "1g"),
        "OOM injection" -> Seq(RapidsConf.TEST_RETRY_OOM_INJECTION_MODE.key -> "type=GPU"))) {
    test(s"metadata-only Parquet reads are capped, $readerType, $name") {
      withMissingColsData { path =>
        val conf = missingColsConf(readerType, extra: _*)
        val expected = withCpuSparkSession(
          _.read.schema(missingColsSchema).parquet(path).collect(), conf)
        withGpuSparkSession({ spark =>
          val df = spark.read.schema(missingColsSchema).parquet(path)
          val scan = df.queryExecution.executedPlan.collect {
            case scan: GpuFileSourceScanExec => scan
          }.head
          val rows = df.collect()
          assert(rows.length == 9000)
          assert(rows.forall(r => r.isNullAt(0) && r.isNullAt(1)))
          assert(rows.groupBy(_.getInt(2)).mapValues(_.length).toMap ==
            Map(0 -> 3000, 1 -> 3000, 2 -> 3000))
          assert(rows.map(_.toString).sorted.toSeq == expected.map(_.toString).sorted.toSeq)

          val rapidsConf = new RapidsConf(spark.sessionState.conf)
          val cap = GpuNullBatchIterator.maxRows(missingColsSchema,
            rapidsConf.maxReadBatchSizeRows, rapidsConf.gpuTargetBatchSizeBytes)
          assert(cap < 3000, s"the test needs a cap below the file size, got $cap")
          if (name == "row limit") {
            assert(cap == 700, s"the row limit should set the cap, got $cap")
          }
          // Before the fix this was one batch per file, or one for everything.
          val batchRows = scan.executeColumnar().mapPartitions(
            _.map { b => try b.numRows() finally b.close() }).collect()
          assert(batchRows.sum == 9000L)
          assert(batchRows.forall(_ <= cap), s"batches over the cap of $cap: ${batchRows.toSeq}")
        }, conf)
      }
    }
  }

  for (readerType <- Seq("PERFILE", "COALESCING", "MULTITHREADED")) {
    // No data column is read at all, so the batches only get partition columns on the GPU
    test(s"partition-only Parquet reads work without data columns, $readerType") {
      withMissingColsData { path =>
        val conf = missingColsConf(readerType)
        val run = (spark: org.apache.spark.sql.SparkSession) =>
          spark.read.schema(missingColsSchema).parquet(path).select("key")
            .groupBy("key").count().collect().map(_.toString).sorted.toSeq
        assert(withGpuSparkSession(run, conf) == withCpuSparkSession(run, conf))
      }
    }

    test(s"LIMIT on a metadata-only Parquet read stops early, $readerType") {
      withMissingColsData { path =>
        withGpuSparkSession({ spark =>
          val df = spark.read.schema(missingColsSchema).parquet(path).limit(1)
          val plan = df.queryExecution.executedPlan
          val scans = plan.collect { case scan: GpuFileSourceScanExec => scan }
          assert(scans.length == 1, plan.toString)
          assert(scans.head.inputRDD.getNumPartitions == 1, plan.toString)
          val rapidsConf = new RapidsConf(spark.sessionState.conf)
          val cap = GpuNullBatchIterator.maxRows(missingColsSchema,
            rapidsConf.maxReadBatchSizeRows, rapidsConf.gpuTargetBatchSizeBytes)
          assert(cap < 9000, s"the test needs a cap below the row count, got $cap")
          assert(df.collect().length == 1)
          // The scan has 9000 rows, but LIMIT 1 only needed the first capped batch
          val rows = scans.head.metrics(GpuMetric.NUM_OUTPUT_ROWS).value
          assert(rows >= 1 && rows <= cap, s"$rows rows produced for LIMIT 1 with a cap of $cap")
        }, missingColsConf(readerType)
          .set("spark.rapids.sql.exec.CollectLimitExec", "false")
          .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "CollectLimitExec"))
      }
    }
  }

  testSparkResultsAreEqual("Test Parquet with row chunks", fileSplitsParquet,
    conf = new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "100")) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test Parquet with byte chunks", fileSplitsParquet,
    conf = new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "100")) {
    frame => frame.select(col("*"))
  }

  // The read estimate is only consulted when there is no chunked reader, so the chunked reader
  // is turned off here to keep covering the byte limit path.
  testSparkResultsAreEqual("Test Parquet with byte chunks using the read estimate",
    fileSplitsParquet,
    conf = new SparkConf()
      .set(RapidsConf.CHUNKED_READER.key, "false")
      .set(RapidsConf.READER_USE_READ_ESTIMATE_FROM_SCHEMA.key, "true")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "100")) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test Parquet with byte chunks skipping the read estimate",
    fileSplitsParquet,
    conf = new SparkConf()
      .set(RapidsConf.CHUNKED_READER.key, "false")
      .set(RapidsConf.READER_USE_READ_ESTIMATE_FROM_SCHEMA.key, "false")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "100")) {
    frame => frame.select(col("*"))
  }

  // Eventually it would be nice to move this to the integration tests,
  // but the file it depends on is used in other tests too.
  testSparkResultsAreEqual("Test Parquet timestamps and dates",
    frameFromParquet("timestamp-date-test.parquet")) {
    frame => frame.select(col("*"))
  }

  // Column schema of decimal-test.parquet is: [c_0: decimal(18, 0), c_1: decimal(7, 3),
  // c_2: decimal(10, 10), c_3: decimal(15, 12), c_4: int64, c_5: float]
  testSparkResultsAreEqual("Test Parquet decimal stored as INT32/64",
    frameFromParquet("decimal-test.parquet")) {
    frame => frame.select(col("*"))
  }

  // Column schema of decimal-test-legacy.parquet is: [c_0: decimal(18, 0), c_1: decimal(7, 3),
  // c_2: decimal(10, 10), c_3: decimal(15, 12), c_4: int64, c_5: float]
  testSparkResultsAreEqual("Test Parquet decimal stored as FIXED_LEN_BYTE_ARRAY",
    frameFromParquet("decimal-test-legacy.parquet")) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("parquet dis-order read schema",
    frameFromParquetWithSchema("disorder-read-schema.parquet", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c3_long", LongType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("parquet dis-order read schema 1",
    frameFromParquetWithSchema("disorder-read-schema.parquet", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType),
      StructField("c3_long", LongType))))) { frame => frame }

  testSparkResultsAreEqual("parquet dis-order read schema 2",
    frameFromParquetWithSchema("disorder-read-schema.parquet", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("parquet dis-order read schema 3",
    frameFromParquetWithSchema("disorder-read-schema.parquet", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c2_string", StringType))))) { frame => frame }

  testSparkResultsAreEqual("parquet dis-order read schema 4",
    frameFromParquetWithSchema("disorder-read-schema.parquet", StructType(Seq(
      StructField("c2_string", StringType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  testSparkResultsAreEqual("parquet dis-order read schema 5",
    frameFromParquetWithSchema("disorder-read-schema.parquet", StructType(Seq(
      StructField("c3_long", LongType),
      StructField("c1_int", IntegerType))))) { frame => frame }

  /**
   * Schema of nested-unsigned.parquet is:
   *
   * message root {
   *   required int32 a (UINT_8);
   *   required int32 b (UINT_16);
   *   required int32 c (UINT_32);
   *   required group g {
   *     required int32 c1 (UINT_8);
   *     required int32 c2 (UINT_16);
   *     required int32 c3 (UINT_32);
   *   }
   *   required group m1 (MAP) {
   *     repeated group key_value {
   *       required int32 key (UINT_8);
   *       optional int32 value (UINT_8);
   *     }
   *   }
   *   required group m2 (MAP) {
   *     repeated group key_value {
   *       required int32 key (UINT_16);
   *       optional int32 value (UINT_16);
   *     }
   *   }
   *   required group m3 (MAP) {
   *     repeated group key_value {
   *       required int32 key (UINT_32);
   *       optional int32 value (UINT_32);
   *     }
   *   }
   *   optional group m4 (MAP) {
   *     repeated group key_value {
   *       required int32 key (UINT_32);
   *       required group value {
   *         required int32 c1 (UINT_8);
   *         required int32 c2 (UINT_16);
   *         required int32 c3 (UINT_32);
   *       }
   *     }
   *   }
   * }
   *
   * converted to Spark schema
   *
   * >>> df.printSchema()
   * root
   *  |-- a: short (nullable = true)
   *  |-- b: integer (nullable = true)
   *  |-- c: long (nullable = true)
   *  |-- g: struct (nullable = true)
   *  |    |-- c1: short (nullable = true)
   *  |    |-- c2: integer (nullable = true)
   *  |    |-- c3: long (nullable = true)
   *  |-- m1: map (nullable = true)
   *  |    |-- key: short
   *  |    |-- value: short (valueContainsNull = true)
   *  |-- m2: map (nullable = true)
   *  |    |-- key: integer
   *  |    |-- value: integer (valueContainsNull = true)
   *  |-- m3: map (nullable = true)
   *  |    |-- key: long
   *  |    |-- value: long (valueContainsNull = true)
   *  |-- m4: map (nullable = true)
   *  |    |-- key: long
   *  |    |-- value: struct (valueContainsNull = true)
   *  |    |    |-- c1: short (nullable = true)
   *  |    |    |-- c2: integer (nullable = true)
   *  |    |    |-- c3: long (nullable = true)
   *
   */
  testSparkResultsAreEqual("Test Parquet nested unsigned int: uint8, uint16, uint32",
    frameFromParquet("nested-unsigned.parquet"),
    // CPU version throws an exception when Spark < 3.2, so skip when Spark < 3.2.
    // The exception is like "Parquet type not supported: INT32 (UINT_8)"
    assumeCondition = (_ => (VersionUtils.isSpark320OrLater, "Spark version not 3.2.0+"))) {
    frame => frame.select(col("*"))
  }
  
  /**
   * A malformed version of nested-unsigned.parquet is, which should throw.
   */
  test("Test Parquet nested unsigned int malformed: uint8, uint16, uint32"){
    assumeSpark320orLater
    try {
      withGpuSparkSession(spark => {
        frameFromParquet("nested-unsigned-malformed.parquet")(spark).collect
      })
      fail("Did not receive an expected exception from cudf")
    } catch {
      case e: Exception =>
        if (!exceptionContains(e, "Encountered malformed")) {
          throw e
        }
    }
  }

  /**
   * Parquet file with 2 columns
   * <simple_uint64, UINT64>
   * <arr_uint64, array(UINT64)>
   */

  testSparkResultsAreEqual("Test Parquet unsigned int: uint64",
    frameFromParquet("test_unsigned64.parquet"),
    // CPU version throws an exception when Spark < 3.2, so skip when Spark < 3.2.
    // The exception is like "Parquet type not supported: INT32 (UINT_8)"
    assumeCondition = (_ => (VersionUtils.isSpark320OrLater, "Spark version not 3.2.0+"))) {
    frame => frame.select(col("*"))
  }

  test("gpuOutputBatchBytes metric is recorded for Parquet scan") {
    withGpuSparkSession({ spark =>
      val df = frameFromParquet("file-splits.parquet")(spark)
      val rows = df.collect()
      val scan = df.queryExecution.executedPlan
        .find(_.metrics.contains(GpuMetric.GPU_OUTPUT_BATCH_BYTES))
      assert(scan.isDefined)
      // Physical device width per row from cudf; variable-width types report 0.
      val bytesPerRow = df.schema.fields
        .map(f => GpuColumnVector.getRapidsType(f.dataType).getSizeInBytes).sum
      val minBytes = rows.length.toLong * bytesPerRow
      assert(scan.get.metrics(GpuMetric.GPU_OUTPUT_BATCH_BYTES).value >= minBytes)
    })
  }
}
