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

package com.nvidia.rapids.tests.scaletest

import scala.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.tests.datagen.{CorrelatedKeyGroup, DBGen, DistinctDistribution, ExponentialDistribution, FlatDistribution, MultiDistribution, RowNumPassThrough}
import org.apache.spark.sql.types.{DateType, TimestampType}


/**
 * pre-concepts:
 * - key groups:
 * 1. The primary key for table a. In table a each row will be unique
 * 2. Adjusted key group. The number of columns will correspond to the complexity. The types of the
 *    columns should be selected from the set {string, decimal(7, 2), decimal(15, 2), int, long,
 *    timestamp, date, struct<num: long, desc: string>}
 * 3. 3-column key group with the types string, date, long.
 * 4. 1 column that is an int with a unique count of 5
 */
class TableGenerator(scaleFactor: Int, complexity: Int, seed: Int, spark: SparkSession) {
  val dbgen: DBGen = DBGen()
  // diff days of(1970-01-01, 0001-01-01) and diff days of(1970-01-01, 9999-12-31)
  // set this to avoid out of range case for Date and Timestamp
  dbgen.setDefaultValueRange(DateType, -719162, 2932896)
  dbgen.setDefaultValueRange(TimestampType, -719162, 2932896)
  // define numRows in class as different table may depend on each other
  private val aNumRows = scaleFactor * 10000
  private val bNumRows = scaleFactor * 1000000
  private val cNumRows = scaleFactor * 100000
  private val dNumRows = scaleFactor * 100000
  private val eNumRows = scaleFactor * 1000000
  private val fNumRows = scaleFactor * 10000
  private val gNumRows = scaleFactor * 1000000
  private val random = new Random(seed)

  private def randomColumnType(): String = {
    // TODO: add more types when they are supported e.g. BinaryType CalendarintervalType etc.
    // See: https://github.com/NVIDIA/spark-rapids/issues/8717
    // ride-along/data columns will pick the column type from the candidates below.
    // required at https://github.com/NVIDIA/spark-rapids/issues/8813#issue-1822958165
    // TODO: maybe we can move this to an external text file so we can extend it easily in the
    //       future
    val candidates = Seq(
      "int",
      "long",
      "Decimal(7, 2)",
      "Decimal(19, 4)",
      "string",
      "map<string, struct<lat: Decimal(7,4), lon: Decimal(7, 4)>>",
      "timestamp",
      "date")
    candidates(random.nextInt(candidates.length))
  }

  private def randomNumericColumnType(): String = {
    val candidates = Seq(
      "int",
      "long",
      "Decimal(7, 2)",
      "Decimal(19, 4)"
    )
    candidates(random.nextInt(candidates.length))
  }

  private val keyGroup2ColumnTypeCandidate: Seq[String] = Seq(
    "string",
    "decimal(7, 2)",
    "decimal(15, 2)",
    "int",
    "long",
    "timestamp",
    "date",
    "struct<num: long, desc: string>"
  )

  // To get a constant column type list so it can be used for both c_data and d_data when comparing
  // key columns. Otherwise we will see column type mismatch error.
  private val randomColumnTypesKeyGroup2: Seq[String] = {
    (1 to complexity).map(_ => keyGroup2ColumnTypeCandidate(random.nextInt
    (keyGroup2ColumnTypeCandidate.length)))
  }

  private def expandColumnsForKeyGroup2(prefix: String): String = {
    (1 to complexity).map(i => s"${prefix}_$i").zip(randomColumnTypesKeyGroup2).map {
      case (colName, colType) => s"$colName $colType"
    }.mkString(", ")
  }

  /**
  a_facts: Each scale factor corresponds to 10,000 rows
    - primary_a the primary key from key group 1
    - key group 4
    - a_data_low_unique_1 data column that is a long with a unique count of 5
    - a_data_low_unique_len_1 data column that is a string with variable length ranging from 1 to 5
      inclusive (even distribution of lengths)
    - complexity data columns
   */
  private def genAFacts(): DataFrame = {
    val schema = "primary_a long," +
      "a_key4_1 int," +
      "a_data_low_unique_1 long," +
      "a_data_low_unique_len_1 string," +
      (1 to complexity).map(i => s"a_data_$i ${randomColumnType()}").mkString(",")
    val aFacts = dbgen.addTable("a_facts", schema, aNumRows)
    aFacts("primary_a").setSeedRange(1, aNumRows).setSeedMapping(DistinctDistribution())
    aFacts("a_key4_1").setSeedRange(1,5)
    aFacts("a_data_low_unique_1").setSeedRange(1, 5)
    aFacts("a_data_low_unique_len_1").setLength(1, 5)
    // ride-along columns will all use default data.
    aFacts.toDF(spark)
  }

  /**
  b_data: Each scale factor corresponds to 1,000,000 rows. (The row group size should be configured
  to be 512 MiB when writing parquet or equivalent when writing ORC)
    - b_foreign_a Should overlap with a_facts.primary_a about 99% of the time
    - key group 3 with 3 columns (The unique count should be about 99% of the total number of rows).
    - 10 data columns of various types
    - 10 decimal(10, 2) columns with a relatively small value range
   */
  private def genBData(): DataFrame = {
    val schema = "b_foreign_a long," +
      "b_key3_1 string," +
      "b_key3_2 date," +
      "b_key3_3 long," +
      (1 to 10).map(i => s"b_data_$i ${randomColumnType()}").mkString(", ") + ", " +
      (1 to 10).map(i => s"b_data_small_value_range_$i decimal(10, 2)").mkString(", ")
    val overlapRatio = 0.99
    val bData = dbgen.addTable("b_data", schema, bNumRows)
    // This column "Should overlap with a_facts.primary_a about 99% of the time"
    val overlapSeeds = Math.round(aNumRows * overlapRatio)
    val startSeed = aNumRows - overlapSeeds + 1
    val endSeed = startSeed + aNumRows
    bData("b_foreign_a").setSeedRange(startSeed, endSeed)
    bData.configureKeyGroup(
      (1 to 3).map(i => s"b_key3_$i"),
      CorrelatedKeyGroup(3, 1, Math.round(bNumRows * 0.99)),
      FlatDistribution())
    (1 to 10).map(i => bData(s"b_data_small_value_range_$i").setSeedRange(1, 10000))
    bData.toDF(spark)
  }

  /**
  c_data: Each scale factor corresponds to 100,000 rows.
    - c_foreign_a Should overlap with a_facts.primary_a about 50% of the time
    - key group 2 columns up to the complexity (each key should show up about 10 times)
    - c_data_row_num_1 1 data column that is essentially the row number for order by tests in window
    - 5 data columns
    - 5 numeric data columns
   */
  private def genCData(): DataFrame = {
    val schema = "c_foreign_a long," +
      expandColumnsForKeyGroup2("c_key2") + "," +
      "c_data_row_num_1 long," +
      (1 to 5).map(i => s"c_data_$i ${randomColumnType()}").mkString(",") + "," +
      (1 to 5).map(i => s"c_data_numeric_$i ${randomNumericColumnType()}").mkString(",")
    val overlapRatio = 0.50
    val cData = dbgen.addTable("c_data", schema, cNumRows)
    val overlapSeeds = Math.round(aNumRows * overlapRatio)
    val startSeed = aNumRows - overlapSeeds + 1
    val endSeed = startSeed + aNumRows
    cData("c_foreign_a").setSeedRange(startSeed, endSeed)
    cData.configureKeyGroup(
      (1 to complexity).map(i => s"c_key2_$i"),
      CorrelatedKeyGroup(2, 1, cNumRows/10),
      FlatDistribution())
    cData("c_data_row_num_1").setValueGen(RowNumPassThrough)
    cData.toDF(spark)
  }
  /**
  d_data: Each scale factor corresponds to 100,000 rows.
    - key group 2 complexity columns (each key should show up about 10 time, but the overlap with
      c_data for key group 2 should only be about 50%)
    - 10 data columns
   */
  private def genDData(): DataFrame = {
    val schema = expandColumnsForKeyGroup2("d_key2") +
      "," +
      (1 to 10).map(i => s"d_data_$i ${randomColumnType()}").mkString(",")
    val dData = dbgen.addTable("d_data", schema, dNumRows)
    // each key should show up about 10 time, but the overlap with c_data for key group 2 should
    // only be about 50%
    // For example if c_data.c_key2_* had 1000 rows the seed range would be (1, 100) so here
    // d_data.d_key2_* would use a seed range of (50, 150)
    // thus the overlap will be about ~50%
    val numSeedsInKeyTwo = cNumRows/10
    val keyTwoStartSeed = numSeedsInKeyTwo / 2
    val keyTwoEndSeed = keyTwoStartSeed + numSeedsInKeyTwo
    dData.configureKeyGroup(
      (1 to complexity).map(i => s"d_key2_$i"),
      CorrelatedKeyGroup(2, keyTwoStartSeed, keyTwoEndSeed),
      FlatDistribution()
    )
    dData.toDF(spark)
  }
  /**
  e_data: Each scale factor corresponds to 1,000,000 rows.
    - key group 3 with 3 columns (the unique count should be about 99% of the total number of rows
      and overlap with b_data key group 3 by about 90%).
    - 10 data columns
   */
  private def genEData(): DataFrame = {
    val schema = "e_key3_1 string," +
      "e_key3_2 date," +
      "e_key3_3 long," +
      (1 to 10).map(i => s"e_data_$i ${randomColumnType()}").mkString(",")
    val overlapRatio = 0.9
    val eData = dbgen.addTable("e_data", schema, eNumRows)
    val overlapBias = Math.round(bNumRows * (1 - overlapRatio))
    val startSeed = 1 + overlapBias
    val endSeed = Math.round(0.99 * eNumRows) + overlapBias
    eData.configureKeyGroup(
      (1 to 3).map(i => s"e_key3_$i"),
      CorrelatedKeyGroup(3, startSeed, endSeed),
      FlatDistribution()
    )
    eData.toDF(spark)

  }
  /**
  f_facts: Each scale factor corresponds to 10,000 rows
    - key group 4
    - f_data_low_unique_1 data column that is a long with a unique count of 5 but overlap with
      a_data_low_unique_1 is only 1 of the 5.
    - f_data_low_unique_len_1 data column that is a string with variable length ranging from 1 to 5
      inclusive (even distribution of lengths)
    - f_data_row_num_1 long column which is essentially the row number for window ordering
    - complexity/2 data columns
   */
  private def genFFacts(): DataFrame = {
    val schema = "f_key4_1 int, " +
      "f_data_low_unique_1 long, " +
      "f_data_low_unique_len_1 string, " +
      "f_data_row_num_1 long, " +
      (1 to Math.round(complexity/2.0).toInt).map(i => s"f_data_$i ${randomColumnType()}")
        .mkString(",")
    val fFact = dbgen.addTable("f_facts", schema, fNumRows)
    fFact("f_key4_1").setSeedRange(1, 5)
    // overlap only 1 of 5 => this (5, 9) vs. aFact (1, 5)
    fFact("f_data_low_unique_1").setSeedRange(5, 9)
    fFact("f_data_low_unique_len_1").setLength(1, 5)
    fFact("f_data_row_num_1").setValueGen(RowNumPassThrough)
    fFact.toDF(spark)
  }
  /**
  g_data: Each scale factor corresponds to 1,000,000 rows. (The row group size should be configured
          to be 512 MiB)
  key group 3 with 3 columns (should be skewed massively so there are a few keys with lots of values
  and a long tail with few).
  g_data_enum_1 1 string column with 5 unique values in it (an ENUM of sorts)
  g_data_row_num_1 1 long data column that is essentially the row number for range tests in window.
  20 byte data columns
  10 string data columns
   */
  private def genGData(): DataFrame = {
    val schema = "g_key3_1 string, " +
      "g_key3_2 date, " +
      "g_key3_3 long, " +
      "g_data_enum_1 string, " +
      "g_data_row_num_1 long, " +
      (1 to 20).map(i => s"g_data_byte_$i byte").mkString(",") + "," +
      (1 to 10).map(i => s"g_data_string_$i string").mkString(",")
    val gData = dbgen.addTable("g_data", schema, gNumRows)
    // use combination of (Exponential + Flat) Distribution to achieve
    // "a few keys with lots of values and a long tail with few"
    val expWeight = 10.0
    val flatWeight = 1.0
    gData.configureKeyGroup(
      (1 to 3).map(i => s"g_key3_$i"),
      CorrelatedKeyGroup(3, 1, 10000),
      MultiDistribution(Seq(
        (expWeight, ExponentialDistribution(50, 1.0)),
        (flatWeight, FlatDistribution())))
    )
    gData("g_data_enum_1").setSeedRange(1,5)
    gData("g_data_row_num_1").setValueGen(RowNumPassThrough)
    gData.toDF(spark)
  }

  private lazy val fullTableMap = Map[String, DataFrame](
    "a_facts" -> genAFacts(),
    "b_data" -> genBData(),
    "c_data" -> genCData(),
    "d_data" -> genDData(),
    "e_data" -> genEData(),
    "f_facts" -> genFFacts(),
    "g_data" -> genGData()
  )

  def genTables(targetTables: Seq[String]): Map[String, DataFrame] = {
    targetTables.map(key => (key, fullTableMap(key))).toMap
  }
}
