/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests.scaletest

import scala.collection.immutable.ListMap
import scala.util.Random

import com.nvidia.spark.rapids.tests.scaletest.ScaleTest.Config

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class TestQuery(name: String, content: String, iterations: Int, timeout: Long,
    description: String, shufflePartitions: Int = 200)


class QuerySpecs(config: Config, spark: SparkSession) {
  private val baseInputPath = s"${config.inputDir}/" +
    "SCALE_" +
    s"${config.scaleFactor}_" +
    s"${config.complexity}_" +
    s"${config.format}_" +
    s"${config.version}_" +
    s"${config.seed}"
  private val tables = Seq("a_facts",
    "b_data",
    "c_data",
    "d_data",
    "e_data",
    "f_facts",
    "g_data")

  private val random = new Random(config.seed)

  /**
   * To get numeric columns in a dataframe.
   * Currently numeric columns are limited to [byte, int, long, decimal]
   *
   * @param table table name
   * @return numeric type column names
   */
  private def getNumericColumns(table: String): Seq[String] = {
    assert(tables.contains(table), s"Invalid table: $table, candidate tables " +
        s"are ${tables.mkString(",")}")
    val df = spark.read.format(config.format).load(s"$baseInputPath/$table")
    df.schema.map { field =>
      (field.name, field.dataType)
    }.collect {
      case (columnName, ByteType | IntegerType | LongType | _: DecimalType) =>
        columnName
    }
  }

  /**
   * To get columns in a dataframe that work with min/max.
   * currently min/max columns are limited to [byte, int, long, decimal, string, timestamp, date]
   *
   * @param table table name
   * @return min/max type column names
   */
  private def getMinMaxColumns(table: String): Seq[String] = {
    assert(tables.contains(table), s"Invalid table: $table, candidate tables " +
        s"are ${tables.mkString(",")}")
    val df = spark.read.format(config.format).load(s"$baseInputPath/$table")

    df.schema.map { field =>
      (field.name, field.dataType)
    }.collect {
      case (columnName, ByteType | IntegerType | LongType | StringType |
                        TimestampType | DateType| _: DecimalType) =>
        columnName
    }
  }

  /**
   * To get numeric columns in a dataframe and also in candidate columns
   * @param table table name
   * @param columnCandidates candidate columns
   * @return numeric columns
   */
  private def getNumericColumns(table: String, columnCandidates: Seq[String]): Seq[String] = {
    val allNumeric = getNumericColumns(table)
    allNumeric.filter(i => columnCandidates.contains(i))
  }

  /**
   *
   * @param prefix     column name prefix e.g. "b_data"
   * @param startIndex start column index e.g. 1
   * @param endIndex   end column index e.g. 10
   * @return an expanded string for all columns with comma separated
   */
  private def expandColumnWithRange(prefix: String, startIndex: Int, endIndex: Int): String = {
    assert(startIndex <= endIndex, s"Invalid Data Column index range: $startIndex to $endIndex")
    (startIndex to endIndex).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * expand the key column names by complexity. e.g. c_key2_* when complexity is 5:
   * "c_key2_1, c_key2_2, c_key2_3, c_key2_4, c_key2_5"
   * This is for Key Group 2
   *
   * @param prefix     column name prefix
   * @param complexity indicates the number of columns
   * @return expanded columns names
   */
  private def expandKeyColumnByComplexity(prefix: String, complexity: Int): String = {
    (1 to complexity).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * expand the key column names to 3. e.g. b_key3_*:
   * "b_key3_1, b_key3_2, b_key3_3"
   * This is only for Key Group 3
   *
   * @param prefix prefix
   * @return expanded column names
   */
  private def expandKeyGroup3(prefix: String): String = {
    (1 to 3).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * expand the columns for aggregations functions by complexity. e.g. MIN(b_data_*) when
   * complexity is 3:
   * "MIN(b_data_1), MIN(b_data_2), MIN(b_data_3), MIN(b_data_4), MIN(b_data_5)"
   *
   * @param table      table name
   * @param prefix     column name prefix
   * @param aggFunc    aggregate function name
   * @param complexity indicates the number of columns
   * @return
   */
  private def expandAggColumnByComplexity(table: String, prefix: String, aggFunc: String,
  complexity: Int)
  : String
  = {
    // Current Agg functions are MIN MAX which doesn't apply to MapType, so filter first.
    getNumericColumns(table, (1 to complexity).map(i => s"${prefix}_$i")).map(
      i => s"${aggFunc}($i) as ${aggFunc}_$i"
    ).mkString(",")
  }

  /**
   * expand the where clause with AND condition after join by complexity. e.g. c_key2_* = d_key2_*
   * with complexity 3:
   * "c_key2_1 = d_key2_1 AND c_key2_2 = d_key2_2 AND c_key2_3 = d_key2_3"
   *
   * @param lhsPrefix  left hand side column name prefix
   * @param rhsPrefix  right hand side column name prefix
   * @param complexity indicates the number of columns
   * @return expanded WHERE clauses.
   */
  private def expandWhereClauseWithAndByComplexity(lhsPrefix: String, rhsPrefix: String,
      complexity: Int): String = {
    (1 to complexity).map(i => s"${lhsPrefix}_$i = ${rhsPrefix}_$i").mkString(" AND ")
  }

  /**
   * expand where clause with AND condition for Key Group 3 that only has 3 key columns.
   *
   * @param lhsPrefix left hand side column name prefix
   * @param rhsPrefix right hand side column name prefix
   * @return expanded WHERE clauses.
   */
  private def expandWhereClauseWithAndKeyGroup3(lhsPrefix: String, rhsPrefix: String): String = {
    expandWhereClauseWithAndByComplexity(lhsPrefix, rhsPrefix, 3)
  }

  /** expand data columns for A table */
  private def expandADataColumns(): String = {
    "a_data_low_unique_1, a_data_low_unique_len_1, " +
      (1 to config.complexity).map(i => s"a_data_$i").mkString(", ")
  }

  /**
   * expand columns names for B table.
   * B table contains 10 data columns for random types
   * and 10 data columns for decimal type.
   *
   * @return
   */
  private def expandBDataColumns(): String = {
    (1 to 10).map(i => s"b_data_$i").mkString(", ") + "," +
      (1 to 10).map(i => s"b_data_small_value_range_$i").mkString(", ")
  }

  /**
   * generate multiple SUM operations for numeric data columns
   * e.g. when table b contains numeric columns as ["b_1", "b_2", "b_3", "b_4"]
   * it will produce "SUM(b_1 * b_2), SUM(b_1 * b3), SUM(b_1 * b_4)....." according to complexity.
   * Note it's not only limited to 2 columns multiplied, when complexity goes large, it may contains
   * more columns. The returned value is of the form Seq[("SUM(b_1 * b2)", unique_column_name)].
   * so the data can be processed to name the resulting column appropriately.
   */
  private def expandSumForDataColumnsByComplexityInternal(table: String,
      complexity: Int): Seq[(String, String)] = {
    val numericColumns = getNumericColumns(table)
    val allSums = (2 until numericColumns.length).map(numericColumns.combinations)
        .reduce(_ ++ _).zipWithIndex
    allSums.take(complexity).map {
      case (cols, index) =>
        (cols.mkString("SUM(", " * ", ")"), s"EXPAND_SUM_$index")
    }.toSeq
  }

  private def expandMinMaxForDataColumnsByComplexityInternal(columns: Seq[String],
      complexity: Int): Seq[(String, String)] = {
    val allPossible = for {
      column <- columns
      min_or_max <- Seq("MIN", "MAX")
    } yield (s"$min_or_max($column)", s"${min_or_max}_$column")

    random.shuffle(allPossible).take(complexity)
  }

  private def mixedSumMinMaxColumnsByComplexityInternal(table: String,
      complexity: Int): Seq[(String, String)] = {
    val minMaxColumns = getMinMaxColumns(table)
    val maxPossibleMinMax = minMaxColumns.length * 2 // one for min and another for max
    val numMinMax = math.min(maxPossibleMinMax, complexity / 2)
    val numSums = complexity - numMinMax
    expandSumForDataColumnsByComplexityInternal(table, numSums) ++
      expandMinMaxForDataColumnsByComplexityInternal(minMaxColumns, numMinMax)
  }

  private def mixedSumMinMaxColumnsByComplexity(table: String, complexity: Int): String = {
    val initial = mixedSumMinMaxColumnsByComplexityInternal(table, complexity)
    initial.map {
      case (query, name) => s"$query AS $name"
    }.mkString(",")
  }

  private def windowMixedSumMinMaxColumnsByComplexity(table: String,
      complexity: Int, window: String): String = {
    val initial = mixedSumMinMaxColumnsByComplexityInternal(table, complexity)
    initial.map {
      case (query, name) => s"$query over $window AS $name"
    }.mkString(",")
  }

  /**
   * Read data and initialize temp views in Spark
   */
  def initViews(): Unit = {

    for (table <- tables) {
      spark.read.format(config.format).load(s"$baseInputPath/$table")
        .createOrReplaceTempView(table)
    }
  }

  def getCandidateQueries: Map[String, TestQuery] = {
    /*
    All queries are defined here
     */
    val allQueries = ListMap(Seq(
      TestQuery("q1",
        "SELECT a_facts.*, " +
            expandColumnWithRange("b_data", 1, 10) +
            " FROM b_data JOIN a_facts WHERE " +
            "primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Inner join with lots of ride along columns"),
      TestQuery("q2",
        "SELECT a_facts.*," +
            expandColumnWithRange("b_data", 1, 10) +
            " FROM b_data FULL OUTER JOIN a_facts WHERE primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Full outer join with lots of ride along columns"),
      TestQuery("q3",
        "SELECT a_facts.*," +
            expandColumnWithRange("b_data", 1, 10) +
            " FROM b_data LEFT OUTER JOIN a_facts WHERE primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Left outer join with lots of ride along columns"),
      TestQuery("q4",
        "SELECT c_data.* FROM c_data LEFT ANTI JOIN a_facts on primary_a = c_foreign_a",
        config.iterations,
        config.timeout,
        "Left anti-join lots of ride along columns."),
      TestQuery("q5",
        "SELECT c_data.* FROM c_data LEFT SEMI JOIN a_facts on primary_a = c_foreign_a",
        config.iterations,
        config.timeout,
        "Left semi-join lots of ride along columns."),
      TestQuery("q6",
        s"SELECT " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1) as rn, " +
            s"${expandAggColumnByComplexity(
              "c_data", "c_data", "MIN", 5)}," +
            s"${
              expandAggColumnByComplexity(
                "c_data", "c_data_numeric", "MIN", 5)
            }," +
            s"${expandAggColumnByComplexity("d_data", "d_data", "MAX", 10)} " +
            s"FROM c_data JOIN d_data WHERE " +
            s"${
              expandWhereClauseWithAndByComplexity(
                "c_key2", "d_key2", config.complexity)
            } GROUP BY " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding inner large key count equi-join followed by min/max agg."),
      TestQuery("q7",
        s"SELECT " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1) as rn, " +
            s"${
              expandAggColumnByComplexity(
                "c_data", "c_data", "MIN", 5)
            }," +
            s"${
              expandAggColumnByComplexity(
                "c_data", "c_data_numeric", "MIN", 5)
            }," +
            s"${expandAggColumnByComplexity(
              "d_data", "d_data", "MAX", 10)} " +
            s"FROM c_data FULL OUTER JOIN d_data WHERE " +
            s"${
              expandWhereClauseWithAndByComplexity(
                "c_key2", "d_key2", config.complexity)
            } GROUP BY " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding full outer large key count equi-join followed by min/max agg."),
      TestQuery("q8",
        s"SELECT " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1) as rn, " +
            s"${
              expandAggColumnByComplexity(
                "c_data", "c_data", "MIN", 5)
            }," +
            s"${
              expandAggColumnByComplexity(
                "c_data", "c_data_numeric", "MIN", 5)
            }," +
            s"${expandAggColumnByComplexity("d_data", "d_data", "MAX", 10)} " +
            s"FROM c_data LEFT OUTER JOIN d_data WHERE " +
            s"${
              expandWhereClauseWithAndByComplexity(
                "c_key2", "d_key2", config.complexity)
            } " +
            s"GROUP BY " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding left outer large key count equi-join followed by min/max agg."),
      TestQuery("q9",
        s"SELECT " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, " +
            s"COUNT(1) as rn, " +
            s"${
              expandAggColumnByComplexity(
                "c_data", "c_data", "MIN", 5)
            }," +
            s"${
              expandAggColumnByComplexity(
                "c_data", "c_data_numeric", "MIN", 5)
            } " +
            s"FROM c_data LEFT SEMI JOIN d_data on " +
            s"${
              expandWhereClauseWithAndByComplexity(
                "c_key2", "d_key2", config.complexity)
            }" +
            s" GROUP BY " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Left semi large key count equi-join followed by min/max agg."),
      TestQuery("q10",
        s"SELECT " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}," +
            s" COUNT(1) as rn," +
            s" ${expandAggColumnByComplexity(
              "c_data", "c_data", "MIN", config.complexity)}" +
            s" FROM c_data LEFT ANTI JOIN d_data on " +
            s"${
              expandWhereClauseWithAndByComplexity(
                "c_key2", "d_key2", config.complexity)
            } " +
            s"GROUP BY " +
            s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Left anti large key count equi-join followed by min/max agg."),
      TestQuery("q11",
        s"SELECT " +
            s"${expandKeyGroup3("b_key3")}, " +
            s"${expandColumnWithRange("e_data", 1, 10)}, " +
            s"${expandBDataColumns()} " +
            s"FROM b_data JOIN e_data on " +
            s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        "No obvious build side inner equi-join. (few shuffle partitions)",
        shufflePartitions = Math.ceil(config.scaleFactor/50.0).toInt),
      TestQuery("q12",
        s"SELECT " +
            s"${expandKeyGroup3("b_key3")}, " +
            s"${expandColumnWithRange("e_data", 1, 10)}, " +
            s"${expandBDataColumns()} " +
            s"FROM b_data FULL OUTER JOIN e_data on " +
            s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        " No obvious build side full outer equi-join. (few shuffle partitions)",
        shufflePartitions = Math.ceil(config.scaleFactor/50.0).toInt),
      TestQuery("q13",
        s"SELECT " +
            s"${expandKeyGroup3("b_key3")}, " +
            s"${expandColumnWithRange("e_data", 1, 10)}, " +
            s"${expandBDataColumns()} " +
            s"FROM b_data LEFT OUTER JOIN e_data WHERE " +
            s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        "No obvious build side left outer equi-join. (few shuffle partitions)",
        shufflePartitions = Math.ceil(config.scaleFactor/50.0).toInt),
      TestQuery("q14",
        s"SELECT " +
            s"${expandKeyGroup3("b_key3")}, " +
            s"${expandBDataColumns()} " +
            s"FROM " +
            s"b_data LEFT SEMI JOIN e_data on " +
            s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        "both sides large left semi equi-join. (few shuffle partitions)",
        shufflePartitions = Math.ceil(config.scaleFactor/50.0).toInt),
      TestQuery("q15",
        s"SELECT " +
            s"${expandKeyGroup3("b_key3")}, " +
            s"${expandBDataColumns()} " +
            s"FROM " +
            s"b_data LEFT ANTI JOIN e_data on " +
            s"${expandWhereClauseWithAndKeyGroup3("b_key3", "e_key3")}",
        config.iterations,
        config.timeout,
        "Both sides large left anti equi-join. (few shuffle partitions)",
        shufflePartitions = Math.ceil(config.scaleFactor/50.0).toInt),
      TestQuery("q16",
        s"SELECT a_key4_1, " +
            s"${
              expandColumnWithRange("a_data",
                1,
                Math.round(config.complexity / 2.0).toInt)
            }, " +
            s"${
              expandColumnWithRange("f_data",
                1,
                Math.round(config.complexity / 2.0).toInt)
            } " +
            s"FROM a_facts JOIN f_facts " +
            s"on a_key4_1 = f_key4_1 AND (a_data_low_unique_1 + f_data_low_unique_1) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST inner join."),
      TestQuery("q17",
        s"SELECT a_key4_1, " +
            s"${
              expandColumnWithRange("a_data",
                1,
                Math.round(config.complexity / 2.0).toInt)
            }, " +
            s"${
              expandColumnWithRange("f_data",
                1,
                Math.round(config.complexity / 2.0).toInt)
            } " +
            s"FROM a_facts FULL OUTER JOIN f_facts " +
            s"on a_key4_1 = f_key4_1 AND (a_data_low_unique_1 + f_data_low_unique_1) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST full outer join."),
      TestQuery("q18",
        s"SELECT a_key4_1, " +
            s"${
              expandColumnWithRange("a_data",
                1,
                Math.round(config.complexity / 2.0).toInt)
            }, " +
            s"${
              expandColumnWithRange("f_data",
                1,
                Math.round(config.complexity / 2.0).toInt)
            } " +
            s"FROM a_facts LEFT OUTER JOIN f_facts " +
            s"on a_key4_1 = f_key4_1 AND (a_data_low_unique_1 + f_data_low_unique_1) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST left anti join."),
      TestQuery("q19",
        s"SELECT " +
            s"a_key4_1, " +
            s"${expandADataColumns()} " +
            s"FROM a_facts " +
            s"LEFT ANTI JOIN f_facts on " +
            s"a_key4_1 = f_key4_1 AND (a_data_low_unique_1 + f_data_low_unique_1) != 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST left anti join."),
      TestQuery("q20",
        s"SELECT " +
            s"a_key4_1, " +
            s"${expandADataColumns()} " +
            s"FROM a_facts LEFT SEMI JOIN f_facts on " +
            s"a_key4_1 = f_key4_1 AND (a_data_low_unique_1 + f_data_low_unique_1) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional AST left semi join."),
      TestQuery("q21",
        s"SELECT " +
            s"a_key4_1, " +
            s"${
              expandColumnWithRange("a_data",
                1,
                Math.round(config.complexity / 2.0).toInt)
            }, " +
            s"${
              expandColumnWithRange("f_data",
                1,
                Math.round(config.complexity / 2.0).toInt)
            } " +
            s"FROM a_facts JOIN f_facts " +
            s"on a_key4_1 = f_key4_1 AND " +
            s"(length(concat(a_data_low_unique_len_1, f_data_low_unique_len_1))) = 2",
        config.iterations,
        config.timeout,
        "Extreme skew conditional NON-AST inner join."),
      TestQuery("q22",
        s"SELECT ${expandKeyGroup3("b_key3")}, " +
            s"${mixedSumMinMaxColumnsByComplexity("b_data", config.complexity)} " +
            s"FROM b_data GROUP BY " +
            s"${expandKeyGroup3("b_key3")}",
        config.iterations,
        config.timeout,
        "Group by aggregation, not a lot of combining, but lots of aggregations, " +
            "and CUDF does sort agg internally."),
      TestQuery("q23",
        s"SELECT ${mixedSumMinMaxColumnsByComplexity("b_data", config.complexity)} " +
            s"FROM b_data",
        config.iterations,
        config.timeout,
        "Reduction with lots of aggregations"),
      TestQuery("q24",
        s"SELECT ${expandKeyGroup3("g_key3")}, " +
            s"${mixedSumMinMaxColumnsByComplexity("g_data", config.complexity)} " +
            s"FROM g_data " +
            s"GROUP BY ${expandKeyGroup3("g_key3")}",
        config.iterations,
        config.timeout,
        "Group by aggregation with lots of combining, lots of aggs, and CUDF does hash agg" +
            " internally"),
      TestQuery("q25",
        s"select ${expandKeyGroup3("g_key3")}, " +
            s"collect_set(g_data_enum_1) as g_data_emnum_1_set FROM g_data GROUP BY " +
            s"${expandKeyGroup3("g_key3")}",
        config.iterations,
        config.timeout,
        "collect set group by agg"),
      TestQuery("q26",
        s"select b_foreign_a, collect_list(b_data_1) b_data_1_list " +
            s"FROM b_data GROUP BY b_foreign_a",
        config.iterations,
        config.timeout,
        "collect list group by agg with some hope of succeeding."),
      TestQuery("q27",
        s"select " +
            s"min(g_data_byte_1) over w as MIN_g_data_byte_1, " +
            s"max(g_data_byte_1) over w as MAX_g_data_byte_1, " +
            s"sum(g_data_byte_2) over w as SUM_g_data_byte_2, " +
            s"count(g_data_byte_3) over w as COUNT_g_data_byte_3, " +
            s"avg(g_data_byte_4) over w as AVG_g_data_byte_4, " +
            s"row_number() over w as rn " +
            s"from g_data " +
            s"WINDOW w AS (PARTITION BY ${expandKeyGroup3("g_key3")} ORDER BY " +
            s"g_data_row_num_1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        config.iterations,
        config.timeout,
        "Running Window with skewed partition by columns, and single order by column with small " +
            "number of basic window ops (min, max, sum, count, average, row_number)"),
      TestQuery("q28",
        s"select min(g_data_byte_1) over w as MIN_g_data_byte_1, " +
            s"max(g_data_byte_1) over w as MAX_g_data_byte_1, " +
            s"sum(g_data_byte_2) over w as SUM_g_data_byte_2, " +
            s"count(g_data_byte_3) over w as COUNT_g_data_byte_3, " +
            s"avg(g_data_byte_4) over w as AVG_g_data_byte_4 " +
            "from g_data " +
            s"WINDOW w AS (PARTITION BY ${expandKeyGroup3("g_key3")} " +
            s"ORDER BY g_data_row_num_1 RANGE BETWEEN ${1000 * config.scaleFactor}" +
            s" PRECEDING AND ${5000 * config.scaleFactor} " +
            s"FOLLOWING)",
        config.iterations,
        config.timeout,
        "Ranged Window with large range (lots of rows preceding and following) skewed partition" +
            " by columns and single order by column with small number of basic window ops (min, " +
            "max, sum, count, average)"),
      TestQuery("q29",
        s"select min(g_data_byte_1) over w as MIN_g_data_byte_1, " +
            s"max(g_data_byte_1) over w as MAX_g_data_byte_1, " +
            s"sum(g_data_byte_2) over w as SUM_g_data_byte_2, " +
            s"count(g_data_byte_3) over w as COUNT_g_data_byte_3, " +
            s"avg(g_data_byte_4) over w as AVG_g_data_byte_4 " +
            s"from g_data " +
            s"WINDOW w AS (PARTITION BY ${expandKeyGroup3("g_key3")} ORDER BY " +
            s"g_data_row_num_1 ROWS BETWEEN " +
            s"UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        config.iterations,
        config.timeout,
        "unbounded preceding and following window with skewed partition by columns and single " +
            "order by column with small number of basic window op (min, max, sum, count, average)"),
      TestQuery("q30",
        s"select min(g_data_byte_1) over w as MIN_g_data_byte_1, " +
            s"max(g_data_byte_1) over w as MAX_g_data_byte_1, " +
            s"sum(g_data_byte_2) over w as SUM_g_data_byte_2, " +
            s"count(g_data_byte_3) over w as COUNT_g_data_byte_3, " +
            s"avg(g_data_byte_4) over w as AVG_g_data_byte_4 " +
            s"from g_data " +
            s"WINDOW w AS (ORDER BY g_data_row_num_1 ROWS BETWEEN UNBOUNDED PRECEDING AND " +
            s"CURRENT ROW)",
        config.iterations,
        config.timeout,
        " running window with no partition by columns and single order by column with small " +
            "number of basic window ops (min, max, sum, count, average, row_number)"),
      TestQuery("q31",
        s"select min(g_data_byte_1) over w as MIN_g_data_byte_1, " +
            s"max(g_data_byte_1) over w as MAX_g_data_byte_1, " +
            s"sum(g_data_byte_2) over w as SUM_g_data_byte_2, " +
            s"count(g_data_byte_3) over w as COUNT_g_data_byte_3, " +
            s"avg(g_data_byte_4) over w as AVG_g_data_byte_4 " +
            s"from g_data " +
            s"WINDOW w AS (ORDER BY g_data_row_num_1 RANGE BETWEEN ${1000 * config.scaleFactor}" +
            s" PRECEDING AND ${5000 * config.scaleFactor} FOLLOWING)",
        config.iterations,
        config.timeout,
        " ranged window with large range (lots of rows preceding and following) no partition by " +
            "columns and single order by column with small number of basic window ops (min, max, " +
            "sum, count, average)"),
      TestQuery("q32",
        s"select min(g_data_byte_1) over w as MIN_g_data_byte_1, " +
            s"max(g_data_byte_1) over w as MAX_g_data_byte_1, " +
            s"sum(g_data_byte_2) over w as SUM_g_data_byte_2, " +
            s"count(g_data_byte_3) over w as COUNT_g_data_byte_3, " +
            s"avg(g_data_byte_4) over w as AVG_g_data_byte_4 " +
            s"from g_data " +
            s"WINDOW w AS (ORDER BY g_data_row_num_1 ROWS BETWEEN UNBOUNDED PRECEDING " +
            s"AND UNBOUNDED FOLLOWING)",
        config.iterations,
        config.timeout,
        "unbounded preceding and following window with no partition by columns and single order" +
            " by column with small number of basic window ops (min, max, sum, count, average)"),
      TestQuery("q33",
        s"select lag(g_data_byte_1, ${10 * config.scaleFactor}) OVER w as LAG_g_data_byte_1, " +
            s"lead(g_data_byte_2, ${10 * config.scaleFactor}) OVER w as LEAD_g_data_byte_2 " +
            s"from g_data " +
            s"WINDOW w as (PARTITION BY ${expandKeyGroup3("g_key3")} " +
            s"ORDER BY g_data_row_num_1)",
        config.iterations,
        config.timeout,
        "Lead/Lag window with skewed partition by columns and single order by column"),
      TestQuery("q34",
        s"select lag(g_data_byte_1, ${10 * config.scaleFactor}) OVER w as LAG_g_data_byte_1, " +
            s"lead(g_data_byte_2, ${10 * config.scaleFactor}) OVER w as LEAD_g_data_byte_2 " +
            s"from g_data " +
            s"WINDOW w as (ORDER BY g_data_row_num_1)",
        config.iterations,
        config.timeout,
        "Lead/Lag window with no partition by columns and single order by column."),
      TestQuery("q35",
        s"select min(c_data_numeric_1) over w as MIN_c_data_numeric_1, " +
            s"max(c_data_numeric_2) over w as MAX_c_data_numeric_2 " +
            s"from c_data " +
            s"WINDOW w AS " +
            s"( " +
            s"PARTITION BY ${
              expandColumnWithRange("c_key2",
                1, Math.round(config.complexity / 2.0).toInt)
            } " +
            s"ORDER BY ${
              expandColumnWithRange("c_key2",
                1, Math.round(config.complexity / 2.0).toInt)
            } " +
            s"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        config.iterations,
        config.timeout,
        "Running window with complexity/2 in partition by columns and complexity/2 in order" +
            " by columns."),
      TestQuery("q36",
        s"select min(c_data_numeric_1) over w as MIN_c_data_numeric_1, " +
            s"max(c_data_numeric_2) over w as MAX_c_data_numeric_2 " +
            s"from c_data " +
            s"WINDOW w AS " +
            s"(PARTITION BY ${
              expandColumnWithRange("c_key2",
                1, Math.round(config.complexity / 2.0).toInt)
            }" +
            s" ORDER BY ${
              expandColumnWithRange("c_key2",
                Math.round(config.complexity / 2.0).toInt, config.complexity)
            } " +
            s"ROWS BETWEEN UNBOUNDED PRECEDING " +
            s"AND UNBOUNDED FOLLOWING)",
        config.iterations,
        config.timeout,
        "unbounded to unbounded window with complexity/2 in partition by columns and complexity/2" +
            " in order by columns."),
      TestQuery("q37",
        s"select ${windowMixedSumMinMaxColumnsByComplexity("c_data", config.complexity, "w")} " +
            s" from c_data " +
            s"WINDOW w AS (PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 ROWS BETWEEN " +
            s"UNBOUNDED PRECEDING AND CURRENT ROW)",
        config.iterations,
        config.timeout,
        "Running window with simple partition by and order by columns, but complexity " +
            "window operations as combinations of a few input columns"),
      TestQuery("q38",
        s"select ${
          windowMixedSumMinMaxColumnsByComplexity("c_data", config.complexity, "w")} " +
            s"from c_data " +
            s"WINDOW w AS (PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 RANGE BETWEEN " +
            s"10 PRECEDING AND 10 FOLLOWING)",
        config.iterations,
        config.timeout,
        "Ranged window with simple partition by and order by columns, but complexity " +
            "window operations as combinations of a few input columns"),
      TestQuery("q39",
        s"select ${
          windowMixedSumMinMaxColumnsByComplexity("c_data", config.complexity, "w")} " +
            s"from c_data " +
            s"WINDOW w AS (PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 ROWS BETWEEN " +
            s"UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )",
        config.iterations,
        config.timeout,
        "unbounded window with simple partition by and order by columns, but complexity " +
            "window operations as combinations of a few input columns"),
      TestQuery("q40",
        s"select " +
            s"array_sort(collect_set(f_data_low_unique_1) " +
            s"OVER (PARTITION BY f_key4_1 order by f_data_row_num_1 ROWS BETWEEN UNBOUNDED " +
            s"PRECEDING AND CURRENT ROW)) as f_data_low_unique_1_set " +
            s"from f_facts",
        config.iterations,
        config.timeout,
        "COLLECT SET WINDOW (We may never really be able to do this well)"),
      TestQuery("q41",
        s"select " +
            s"collect_list(f_data_low_unique_1) " +
            s"OVER (PARTITION BY f_key4_1 order by f_data_row_num_1 " +
            s"ROWS BETWEEN ${config.complexity} PRECEDING and CURRENT ROW ) " +
            s"as f_data_low_unique_1_list " +
            s"from f_facts",
        config.iterations,
        config.timeout,
        "COLLECT LIST WINDOW (We may never really be able to do this well)"),
      TestQuery("q42",
        s"SELECT " +
            s"a_key4_1, " +
            s"a_data_low_unique_1, " +
            s"f_data_low_unique_1 " +
            s"FROM a_facts JOIN f_facts " +
            s"on a_key4_1 = f_key4_1 AND " +
            s"a_data_low_unique_1 < CAST(f_data_low_unique_1 AS BIGINT) AND " +
            s"CAST(f_data_row_num_1 AS BIGINT) > a_data_low_unique_1",
        config.iterations,
        config.timeout,
        "Conditional equi-join with CAST in non-equality condition (regression test for " +
            "#14283: non-AST CAST extraction into pre-join GpuProjectExec).")).map { tc =>
      (tc.name, tc)
    }: _*)
    if (config.queries.isEmpty) {
      allQueries
    } else {
      val selected = allQueries.filter {
        case (k, _) =>
          config.queries.contains(k)
      }
      selected
    }
  }
}
