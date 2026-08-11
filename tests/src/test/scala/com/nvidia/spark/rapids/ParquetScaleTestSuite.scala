/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.PrimitiveType

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.tests.datagen.{DBGen, TableGen}
import org.apache.spark.sql.types._

class ParquetScaleTestSuite extends SparkQueryCompareTestSuite with Logging {
  private val sparkConf = new SparkConf()
      // for date time
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") // for Spark 31x
      .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") // for Spark 32x, 33x and ...
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") // for Spark 31x
      .set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") // for Spark 32x, 33x and ...

      // for int96
      .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") // for Spark 31x
      .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") // for Spark 32x, 33x and ...
      .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") // for Spark 31x
      .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") // for Spark 32x, 33x and ...
      .set("spark.rapids.sql.explain", "ALL")

  /**
   * By default cuDF splits row groups by 1,000,000 rows, we expect one row group.
   * Refer to cuDF parquet.hpp
   * default_row_group_size_bytes   = 128 * 1024 * 1024;  ///< 128MB per row group
   * default_row_group_size_rows = 1000000;     ///< 1 million rows per row group
   */
  private val rowsNum: Long = 1000L * 1000L

  case class ParquetStat(
      schema: Seq[String],
      rowGroupStats: Seq[RowGroupStat])

  case class RowGroupStat(rowCount: Long, columnStats: Seq[ColumnChunkStat])

  case class ColumnChunkStat(
      primitiveType: PrimitiveType,
      min: Comparable[_],
      max: Comparable[_],
      hasNonNullValue: Boolean,
      isNumNullsSet: Boolean,
      numNulls: Long
  )

  def writeScaleTestDataOnCpu(testDataPath: File, gen: SparkSession => DataFrame): Unit = {
    withCpuSparkSession(
      spark => {
        // define table
        val path = testDataPath.getAbsolutePath
        // write to a file on CPU
        gen(spark).coalesce(1).write.mode("overwrite").parquet(path)
      },
      sparkConf)
  }

  // write test data on CPU or GPU, then read the stats
  def getStats(filePath: File): SparkSession => ParquetStat = { spark =>
    withTempPath { writePath =>
      // Read from the testing Parquet file and then write to a Parquet file
      spark.read.parquet(filePath.getAbsolutePath).coalesce(1)
          .write.mode("overwrite").parquet(writePath.getAbsolutePath)

      // get Stats
      getStatsFromFile(writePath)
    }
  }

  /**
   * Find a parquet file in parquetDir and get the stats. It's similar to output of
   * `Parquet-cli meta file`. Parquet-cli:https://github
   * .com/apache/parquet-mr/tree/master/parquet-cli
   *
   * @param parquetDir parquet file directory
   * @return Parquet statistics
   */
  @scala.annotation.nowarn("msg=method readFooter in class ParquetFileReader is deprecated")
  private def getStatsFromFile(parquetDir: File): ParquetStat = {
    val parquetFile = parquetDir.listFiles(f => f.getName.endsWith(".parquet"))(0)
    val p = new Path(parquetFile.getCanonicalPath)
    val footer = ParquetFileReader.readFooter(new Configuration(), p,
      ParquetMetadataConverter.NO_FILTER)
    val columnTypes = footer.getFileMetaData.getSchema.getColumns.asScala.toArray
        .map(c => c.toString)
    val groupStats = footer.getBlocks.asScala.toArray.map { g =>
      val rowCount = g.getRowCount
      val columnChunkStats = g.getColumns.asScala.toArray.map { col =>
        ColumnChunkStat(
          col.getPrimitiveType,
          col.getStatistics.genericGetMin(),
          col.getStatistics.genericGetMax(),
          col.getStatistics.hasNonNullValue,
          col.getStatistics.isNumNullsSet,
          col.getStatistics.getNumNulls)
      }
      RowGroupStat(rowCount, columnChunkStats)
    }

    ParquetStat(columnTypes, groupStats)
  }

  private def checkStats(
      genDf: SparkSession => DataFrame,
      skipCheckSchema: Boolean = false): (ParquetStat, ParquetStat) = {
    withTempPath { testDataFile =>
      // Write test data to a file on CPU
      writeScaleTestDataOnCpu(testDataFile, genDf)

      // write data and get stats on CPU
      val cpuStats = withCpuSparkSession(getStats(testDataFile), sparkConf)
      val cpuFileSize = testDataFile.listFiles(f => f.getName.endsWith(".parquet"))(0).length()

      // write data and get stats on GPU
      val gpuStats = withGpuSparkSession(getStats(testDataFile), sparkConf)
      val gpuFileSize = testDataFile.listFiles(f => f.getName.endsWith(".parquet"))(0).length()

      // compare schema
      if (!skipCheckSchema) {
        assertResult(cpuStats.schema)(gpuStats.schema)
      }

      // compare stats
      assert(cpuStats.rowGroupStats.length == gpuStats.rowGroupStats.length)
      cpuStats.rowGroupStats.zip(gpuStats.rowGroupStats).foreach {
        case (cpuRowGroup, gpuRowGroup) => {
          assert(cpuRowGroup.rowCount == gpuRowGroup.rowCount)
          assert(cpuRowGroup.columnStats.length == gpuRowGroup.columnStats.length)
          cpuRowGroup.columnStats.zip(gpuRowGroup.columnStats).foreach {
            case (cpuColumnStat, gpuColumnStat) => {
              assert(cpuColumnStat.hasNonNullValue == gpuColumnStat.hasNonNullValue)
              if (cpuColumnStat.hasNonNullValue) {
                // compare all the attributes
                assertResult(cpuColumnStat)(gpuColumnStat)
              } else {
                // hasNonNullValue is false, which means stats are invalid, no need to compare
                // other attributes.
                /**
                 * hasNonNullValue means:
                 *
                 * Returns whether there have been non-null values added to this statistics
                 *
                 * @return true if the values contained at least one non-null value
                 *
                 * Refer to link: https://github.com/apache/parquet-mr/blob/apache-parquet-1.10.1
                 * /parquet-column/src/main/java/org/apache/parquet/column/statistics
                 * /Statistics.java#L504-L506
                 *
                 * e.g.: Spark 31x, for timestamp type
                 * CPU: hasNonNullValue: false, isNumNullsSet: false, getNumNulls: -1
                 * GPU: hasNonNullValue: false, isNumNullsSet: true, getNumNulls: 0
                 *
                 * Above are expected differences.
                 */
                assertResult(cpuColumnStat.primitiveType)(gpuColumnStat.primitiveType)
              }
            }
          }
        }
      }

      // Check the Gpu file size is not too large.
      assert(gpuFileSize < 2 * cpuFileSize)

      (cpuStats, gpuStats)
    }
  }

  private val basicTypes = Seq(
    // "float",  "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    // "double", "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    "boolean",
    "byte",
    "short",
    "int",
    "long",
    "decimal",
    "string",
    "date",
    "timestamp")

  test("Statistics tests for Parquet files written by GPU, float/double, with NaN") {
    assume(false, "Blocked by https://github.com/rapidsai/cudf/issues/13948")
    assume(false, "Move to scale test")

    val schema = StructType(Seq(
      StructField("c01", FloatType),
      StructField("c02", DoubleType)
    ))
    // 2 rows with NaN
    val data = Seq(Row(1.1f, Double.NaN), Row(Float.NaN, 2.2d))

    def genDf(schema: StructType, data: Seq[Row]): SparkSession => DataFrame = spark =>
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    checkStats(genDf(schema, data))

    // After https://github.com/rapidsai/cudf/issues/13948 is fixed
    // Change this test to the following code:
    //    val schemaStr =
    //      """
    //      struct<
    //        c01: float,
    //        c02: double
    //      >
    //      """
    //    val gen = DBGen()
    //    val tab = gen.addTable("tab", schemaStr, rowsNum)
    //    tab("c01").setNullProbability(0.5)
    //    checkStats(tab)
  }

  test("Statistics tests for Parquet files written by GPU, float/double, without NaN") {
    assume(true, "Move to scale test")
    val schemaStr =
      """
      struct<
        c01: float,
        c02: double
      >
      """
    val gen = DBGen()
    val tab = gen.addTable("tab", schemaStr, rowsNum)
    tab("c01").setValueGen(NonNaNFloatGenFunc())
    tab("c02").setValueGen(NonNaNDoubleGenFunc())

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    withTempPath { testDataFile =>
      // Write test data to a file on CPU
      writeScaleTestDataOnCpu(testDataFile, genDf(tab))

      // write data and get stats on CPU
      val cpuStats = withCpuSparkSession(getStats(testDataFile), sparkConf)
      val cpuFileSize = testDataFile.listFiles(f => f.getName.endsWith(".parquet"))(0).length()

      // write data and get stats on GPU
      val gpuStats = withGpuSparkSession(getStats(testDataFile), sparkConf)
      val gpuFileSize = testDataFile.listFiles(f => f.getName.endsWith(".parquet"))(0).length()

      // compare schema
      assertResult(cpuStats.schema)(gpuStats.schema)

      // Check the Gpu file size is not too large.
      assert(gpuFileSize < 2 * cpuFileSize)

      /**
       *
       * CPU stat:
       *
       * ParquetStat(WrappedArray([c01] optional float c01, [c02] optional double c02),
       * WrappedArray(RowGroupStat(1000000,
       * WrappedArray(
       * ColumnChunkStat(optional float c01,
       *   min = 0.0, max = 0.0,
       *   hasNonNullValue = false,
       *   isNumNullsSet = true,
       *   numNulls = 500532),
       * ColumnChunkStat(optional double c02,
       *   min = 0.0,
       *   max = 0.0,
       *   hasNonNullValue = false,
       *   isNumNullsSet = true,
       *   numNulls = 498986)))))
       *
       * GPU stat:
       *
       * ParquetStat(WrappedArray([c01] optional float c01, [c02] optional double c02),
       * WrappedArray(RowGroupStat(1000000,
       * WrappedArray(
       * ColumnChunkStat(optional float c01,
       *   min = -3.4026107E38,
       *   max = 3.4015179E38,
       *   hasNonNullValue = true,
       *   isNumNullsSet = true,
       *   numNulls = 500532),
       * ColumnChunkStat(optional double c02,
       *   min = -1.7xxxxxE308,
       *   max = 1.7xxxxE308,
       *   hasNonNullValue = true,
       *   isNumNullsSet = true,
       *   numNulls = 498986)))))
       *
       * There are differences between CPU and GPU:
       * CPU hasNonNullValue is false, CPU min/max is 0.0
       */
      assert(cpuStats.rowGroupStats.length == gpuStats.rowGroupStats.length)
      assert(cpuStats.rowGroupStats(0).columnStats(0).isNumNullsSet ==
          gpuStats.rowGroupStats(0).columnStats(0).isNumNullsSet)
      assert(cpuStats.rowGroupStats(0).columnStats(1).isNumNullsSet ==
          gpuStats.rowGroupStats(0).columnStats(1).isNumNullsSet)
      assert(cpuStats.rowGroupStats(0).columnStats(0).numNulls ==
          gpuStats.rowGroupStats(0).columnStats(0).numNulls)
      assert(cpuStats.rowGroupStats(0).columnStats(1).numNulls ==
          gpuStats.rowGroupStats(0).columnStats(1).numNulls)

      // write by GPU, read min/max by CPU
      val (floatMin, floatMax, doubleMin, doubleMax) = withTempPath { gpuFile =>
        withGpuSparkSession(spark => {
          // Read from the testing Parquet file and then write to a Parquet file
          spark.read.parquet(testDataFile.getAbsolutePath).coalesce(1)
              .write.mode("overwrite").parquet(gpuFile.getAbsolutePath)
        })

        val rowArray = withCpuSparkSession(spark => {
          // Read from the testing Parquet file and then write to a Parquet file
          spark.read.parquet(gpuFile.getAbsolutePath)
              .selectExpr("min(c01)", "max(c01)", "min(c02)", "max(c02)").collect()
        })

        (rowArray(0)(0), rowArray(0)(1), rowArray(0)(2), rowArray(0)(3))
      }

      assertResult(floatMin)(gpuStats.rowGroupStats(0).columnStats(0).min)
      assertResult(floatMax)(gpuStats.rowGroupStats(0).columnStats(0).max)
      assertResult(doubleMin)(gpuStats.rowGroupStats(0).columnStats(1).min)
      assertResult(doubleMax)(gpuStats.rowGroupStats(0).columnStats(1).max)
    }
  }

  test("Statistics tests for Parquet files written by GPU, basic types") {
    assume(false, "Move to scale test")
    val schema =
      """
      struct<
        c01: boolean,
        c02: byte,
        c03: short,
        c04: int,
        c05: long,
        c06: decimal,
        c07: string,
        c08: date,
        c09: timestamp
      >
      """
    // "float",  "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    // "double", "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    val gen = DBGen()
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c01").setNullProbability(0.5)
    tab("c06").setNullProbability(0.5)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, array") {
    assume(false, "Move to scale test")
    basicTypes.foreach { t =>
      val schema = s"struct<c01: array<$t>>"
      val nullProbabilities = Seq(0d, 0.5d)
      nullProbabilities.foreach { nullProbability =>
        try {
          val gen = DBGen()
          val tab = gen.addTable("tab", schema, rowsNum)
          tab("c01").setNullProbability(nullProbability)
          tab("c01").setLength(5) // avoid row group exceeds 128M, we expect one row group

          def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

          checkStats(genDf(tab))
        } catch {
          case e: Exception =>
            logError(s"check $schema, $nullProbability failed", e)
            throw e
        }
      }
    }
  }

  test("Statistics tests for Parquet files written by GPU, map") {
    assume(false, "Move to scale test")
    basicTypes.foreach { t =>
      val nullProbabilities = Seq(0d, 0.5d)
      nullProbabilities.foreach { nullProbability =>
        val schema = s"struct<c01: map<$t, $t>>"
        val gen = DBGen()
        val tab = gen.addTable("tab", schema, rowsNum)
        tab("c01").setNullProbability(nullProbability)
        tab("c01").setLength(3) // avoid row group exceeds 128M, we expect one row group

        def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

        checkStats(genDf(tab))
      }
    }
  }

  test("Statistics tests for Parquet files written by GPU, struct") {
    assume(false, "Move to scale test")
    val schema = basicTypes.zipWithIndex.map { case (t, index) =>
      s"c0$index: $t"
    }.mkString("struct<\nc1: struct<", ", \n", ">>")
    val gen = DBGen()
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.5)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, struct(array, map, struct)") {
    assume(false, "Move to scale test")
    val schema =
      """
      struct<
        c1: struct<
          c01: array<string>
        >,
        c2: struct<
          c01: map<string, int>
        >,
        c3: struct<
          c301: struct<
            c30101: int,
            c30102: byte
          >
        >
      >
      """
    val gen = DBGen()
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.9)
    tab("c2").setNullProbability(0.9)
    tab("c3").setNullProbability(0.9)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, array(struct, array)") {
    assume(false, "Move to scale test")
    val schema =
      """
      struct<
        c1: array<
          struct<
            c201: long,
            c202: int
          >
        >,
        c2: array<array<string>>
      >
      """
    val gen = DBGen()
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.8)
    tab("c2").setNullProbability(0.8)
    tab("c1").setLength(3)
    tab("c2").setLength(3)
    tab("c2")("child").setLength(3)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, map(array)") {
    assume(false, "Move to scale test")
    val schema =
      """
    struct<
      c1: map<
        array<long>,
        array<string>
      >
    >
    """
    val gen = DBGen()
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.8)
    tab("c1").setLength(3)
    tab("c1")("key").setLength(3)
    tab("c1")("value").setLength(3)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, map(struct)") {
    assume(false, "Move to scale test")
    val schema =
      """
      struct<
        c1: map<
          struct<
            c101: long,
            c102: int
          >,
          struct<
            c101: long,
            c102: string
          >
        >
      >
      """
    val gen = DBGen()
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.8)
    tab("c1").setLength(3)
    tab("c1")("key").setLength(3)
    tab("c1")("value").setLength(3)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, array(map)") {
    assume(false, "Move to scale test")
    val schema =
      """
      struct<
        c1: array<map<string, long>>
      >
      """
    val gen = DBGen()
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.8)
    tab("c1")("child").setLength(3)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)
    /**
     * Note: There are discrepancies between CPU and GPU file schemas,
     * but the Spark can read both them correctly, so it's not an issue.
     *
     * Details:
     *
     * CPU Parquet file schema is:
     * message spark_schema {
     *   optional group c1 (LIST) {
     *     repeated group list {
     *       optional group element (MAP) {
     *         repeated group key_value {
     *           required binary key (STRING);
     *           optional int64 value;
     *         }
     *       }
     *     }
     *   }
     * }
     *
     * GPU Parquet file schema is:
     * message schema {
     *   optional group c1 (LIST) {
     *     repeated group list {
     *       optional group c1 (MAP) {
     *         repeated group key_value {
     *           required binary key (STRING);
     *           optional int64 value;
     *         }
     *       }
     *     }
     *   }
     * }
     *
     * Spark reads both of them as:
     *
     * df.printSchema()
     * root
     *  |-- c1: array (nullable = true)
     *  |    |-- element: map (containsNull = true)
     *  |    |    |-- key: string
     *  |    |    |-- value: long (valueContainsNull = true)
     *
     */
    // skip check the schema
    val (cpuStat, gpuStat) = checkStats(genDf(tab), skipCheckSchema = true)

    val expectedCpuSchemaForSpark31x = Seq(
      "[c1, list, element, key_value, key] required binary key (UTF8)",
      "[c1, list, element, key_value, value] optional int64 value")

    val expectedCpuSchemaForSpark320Plus = Seq(
      "[c1, list, element, key_value, key] required binary key (STRING)",
      "[c1, list, element, key_value, value] optional int64 value")

    val expectedGpuSchemaForSpark31x = Seq(
      "[c1, list, c1, key_value, key] required binary key (UTF8)",
      "[c1, list, c1, key_value, value] optional int64 value")

    val expectedGpuSchemaForSpark320Plus = Seq(
      "[c1, list, c1, key_value, key] required binary key (STRING)",
      "[c1, list, c1, key_value, value] optional int64 value")

    assert(cpuStat.schema == expectedCpuSchemaForSpark31x ||
        cpuStat.schema == expectedCpuSchemaForSpark320Plus)

    assert(gpuStat.schema == expectedGpuSchemaForSpark31x ||
        gpuStat.schema == expectedGpuSchemaForSpark320Plus)
  }

  test("Statistics tests for Parquet files written by GPU, map(map)") {
    assume(false, "Move to scale test")
    val schema =
      """
      struct<
        c1: map<
          map<int, int>,
          map<int, int>
        >
      >
      """
    val gen = DBGen()
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.8)
    tab("c1").setLength(3)
    tab("c1")("key").setLength(3)
    tab("c1")("value").setLength(3)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }
}
