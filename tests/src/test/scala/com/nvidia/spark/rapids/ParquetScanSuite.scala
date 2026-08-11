/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class ParquetScanSuite extends SparkQueryCompareTestSuite {
  private val fileSplitsParquet = frameFromParquet("file-splits.parquet")

  testSparkResultsAreEqual("Test Parquet with row chunks", fileSplitsParquet,
    conf = new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "100")) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test Parquet with byte chunks", fileSplitsParquet,
    conf = new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "100")) {
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
}
