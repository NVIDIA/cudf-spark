# Scale Test

## Build

The Scale Test suite is bundled into `integration_tests` module, it can be built by maven directly.

```bash
mvn package
```

You can choose profiles to build against specific Spark and JDK version. e.g.

```bash
mvn package -Dbuildver=332 -Pjdk17
```

## Query Sets

The queries are define in the source code. You can check the table below to see query content and the description:

| name | description | content |
|------|-------------|---------|
| q1   | Inner join with lots of ride along columns | SELECT a_facts.*, b_data_{1-10} FROM b_data JOIN a_facts WHERE primary_a = b_foreign_a |
| q2   | Full outer join with lots of ride along columns | SELECT a_facts.*, b_data_{1-10} FROM b_data FULL OUTER JOIN a_facts WHERE primary_a = b_foreign_a |
| q3   | Left outer join with lots of ride along columns | SELECT a_facts.*, b_data_{1-10} FROM b_data LEFT OUTER JOIN a_facts WHERE primary_a = b_foreign_a |
| q4         | Left anti-join lots of ride along columns.                                                            | SELECT c_data.* FROM c_data LEFT ANTI JOIN a_facts WHERE primary_a = c_foreign_a                                    |
| q5         | Left semi-join lots of ride along columns.                                                           | SELECT c_data.* FROM c_data LEFT SEMI JOIN a_facts WHERE primary_a = c_foreign_a                                    |
| q6         | Exploding inner large key count equi-join followed by min/max agg.                                    | SELECT c_key2_*, COUNT(1), MIN(c_data_*), MAX(d_data_*) FROM c_data JOIN d_data WHERE c_key2_* = d_key2_* GROUP BY c_key2_* |
| q7         | Exploding full outer large key count equi-join followed by min/max agg.                               | SELECT c_key2_*, COUNT(1), MIN(c_data_*), MAX(d_data_*) FROM c_data FULL OUTER JOIN d_data WHERE c_key2_* = d_key2_* GROUP BY c_key2_* |
| q8         | Exploding left outer large key count equi-join followed by min/max agg.                               | SELECT c_key2_*, COUNT(1), MIN(c_data_*), MAX(d_data_*) FROM c_data LEFT OUTER JOIN d_data WHERE c_key2_* = d_key2_* GROUP BY c_key2_* |
| q9         | Left semi large key count equi-join followed by min/max agg.                                           | SELECT c_key2_*, COUNT(1), MIN(c_data_*) FROM c_data LEFT SEMI JOIN d_data WHERE c_key2_* = d_key2_* GROUP BY c_key2_* |
| q10        | Left anti large key count equi-join followed by min/max agg.                                           | SELECT c_key2_*, COUNT(1), MIN(c_data_*) FROM c_data LEFT ANTI JOIN d_data WHERE c_key2_* = d_key2_* GROUP BY c_key2_* |
| q11        | No obvious build side inner equi-join. (Shuffle partitions should be set to 10)                     | SELECT b_key3_*, e_data_*, b_data_* FROM b_data JOIN e_data WHERE b_key3_* = e_key3_* |
| q12        | No obvious build side full outer equi-join. (Shuffle partitions should be set to 10)                | SELECT b_key3_*, e_data_*, b_data_* FROM b_data FULL OUTER JOIN e_data WHERE b_key3_* = e_key3_* |
| q13        | No obvious build side left outer equi-join. (Shuffle partitions should be set to 10)                | SELECT b_key3_*, e_data_*, b_data_* FROM b_data LEFT OUTER JOIN e_data WHERE b_key3_* = e_key3_* |
| q14        | Both sides large left semi equi-join. (Shuffle partitions should be set to 10)                      | SELECT b_key3_*, b_data_* FROM b_data LEFT SEMI JOIN e_data WHERE b_key3_* = e_key3_* |
| q15        | Both sides large left anti equi-join. (Shuffle partitions should be set to 10)                       | SELECT b_key3_*, b_data_* FROM b_data LEFT ANTI JOIN e_data WHERE b_key3_* = e_key3_* |
| q16        | Extreme skew conditional AST inner join.                                                            | SELECT a_key4_1, a_data_(1-complexit/2), f_data_(1-complexity/2) FROM a_facts JOIN f_facts WHERE a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) = 2 |
| q17        | Extreme skew conditional AST full outer join.                                                       | SELECT a_key4_1, a_data_(1-complexit/2), f_data_(1-complexity/2) FROM a_fact FULL OUTER JOIN f_fact WHERE a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) = 2 |
| q18        | Extreme skew conditional AST left outer join.                                                       | SELECT a_key4_1, a_data_(1-complexit/2), f_data_(1-complexity/2) FROM a_fact LEFT OUTER JOIN f_fact WHERE a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) = 2 |
| q19        | Extreme skew conditional AST left anti join.                                                        | SELECT a_key4_1, a_data_*, FROM a_fact LEFT ANTI JOIN f_fact WHERE a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) != 2 |
| q20        | Extreme skew conditional AST left semi join.                                                        | SELECT a_key4_1, a_data_* FROM a_fact LEFT SEMI JOIN f_fact WHERE a_key4_1 = f_key4_1 && (a_data_low_unique_1 + f_data_low_unique_1) = 2 |
| q21        | Extreme skew conditional NON-AST inner join.                                                        | SELECT a_key4_1, a_data_(1-complexity/2), f_data_(1-complexity/2) FROM a_fact JOIN f_fact WHERE a_key4_1 = f_key4_1 && (length(concat(a_data_low_unique_len_1, f_data_low_unique_len_1))) = 2 |
| q22        | Group by aggregation, not a lot of combining, but lots of aggregations, and CUDF does sort agg internally. | SELECT b_key3_*, complexity number of aggregations that are SUMs of 2 or more numeric data columns multiplied together or MIN/MAX of any data column FROM b_data GROUP BY b_key3_*. |
| q23        | Reduction with with lots of aggregations                                                               | SELECT complexity number of aggregations that are SUMs of 2 or more numeric data columns multiplied together or MIN/MAX of any data column FROM b_data. |
| q24        | Group by aggregation with lots of combining, lots of aggs, and CUDF does hash agg internally         | SELECT g_key3_*, complexity number of aggregations that are SUM/MIN/MAX/AVERAGE/COUNT of 2 or more byte columns cast to int and added, subtracted, multiplied together. FROM g_data GROUP BY g_key3_* |
| q25        | collect set group by agg                                                                              | select g_key3_*, collect_set(g_data_enum_1) FROM g_data GROUP BY g_key3_* |
| q26        | collect list group by agg with some hope of succeeding.                                              | select b_foreign_a, collect_list(b_data_1) FROM b_data GROUP BY b_foreign_a |
| q27        | Running Window with skewed partition by columns, and single order by column with small number of basic window ops (min, max, sum, count, average, row_number) | select {min(g_data_1), max(g_data_1), sum(g_data_2), count(g_data_3), average(g_data_4), row_number} over (UNBOUNDED PRECEDING TO CURRENT ROW PARTITION BY g_key3_* ORDER BY g_data_row_num_1) |
| q28        | Ranged Window with large range (lots of rows preceding and following) skewed partition by columns and single order by column with small number of basic window ops (min, max, sum, count, average) | select {min(g_data_1), max(g_data_1), sum(g_data_2), count(g_data_3), average(g_data_4)} over (RANGE BETWEEN 1000 * scale_factor PRECEDING AND 5000 * scale_factor FOLLOWING PARTITION BY g_key3_* ORDER BY g_data_row_num_1) |
| q29        | unbounded preceding and following window with skewed partition by columns, and single order by column with small number of basic window op (min, max, sum, count, average) | select {min(g_data_1), max(g_data_1), sum(g_data_2), count(g_data_3), average(g_data_4)} over (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING PARTITION BY g_key3_* ORDER BY g_data_row_num_1) |
| q30        | running window with no partition by columns and single order by column with small number of basic window ops (min, max, sum, count, average, row_number) | select {min(g_data_1), max(g_data_1), sum(g_data_2), count(g_data_3), average(g_data_4)} over (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ORDER BY g_data_row_num_1) |
| q31        | ranged window with large range (lots of rows preceding and following) no partition by columns and single order by column with small number of basic window ops (min, max, sum, count, average) | select {min(g_data_1), max(g_data_1), sum(g_data_2), count(g_data_3), average(g_data_4)} over (RANGE BETWEEN 1000 * scale_factor PRECEDING AND 5000 * scale_factor FOLLOWING ORDER BY g_data_row_num_1) |
| q32        | unbounded preceding and following window with no partition by columns and single order by column with small number of basic window ops (min, max, sum, count, average) | select  {min(g_data_1), max(g_data_1), sum(g_data_2), count(g_data_3), average(g_data_4)}  over (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ORDER BY g_row_num_1) |
| q33        | Lead/Lag window with skewed partition by columns and single order by column.                         | select {lag(g_data_1, 10 * scale_factor, lead(g_data_2, 10 * scale_factor)} OVER (PARTITION BY g_key3_* ORDER BY g_data_row_num_1) |
| q34        | Lead/Lag window with no partition by columns and single order by column.                               | select {lag(g_data_1, 10 * scale_factor, lead(g_data_2, 10 * scale_factor)} OVER (ORDER BY g_data_row_num_1) |
| q35        | Running window with complexity/2 in partition by columns and complexity/2 in order by columns.       | select {min(c_data_1), max(c_data_2)} over (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW PARTITION BY c_key2_(1 to complexity/2) ORDER BY c_key2_(complexity/2 to complexity) |
| q36        | unbounded to unbounded window with complexity/2 in partition by columns and complexity/2 in order by columns. | select {min(c_data_1), max(c_data_2)} over (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING PARTITION BY c_key2_(1 to complexity/2) ORDER BY c_key2_(complexity/2 to complexity) |
| q37        | Running window with simple partition by and order by columns, but complexity window operations as combinations of a few input columns | select {complexity aggregations mins/max of any column or SUM of two or more numeric columns multiplied together} over (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 |
| q38        | Ranged window with simple partition by and order by columns, but complexity window operations as combinations of a few input columns | select {complexity aggregations mins/max of any column or SUM of two or more numeric columns multiplied together} over (RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 |
| q39        | unbounded window with simple partition by and order by columns, but complexity window operations as combinations of a few input columns | select {complexity aggregations mins/max of any column or SUM of two or more numeric columns multiplied together} over (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING PARTITION BY c_foreign_a ORDER BY c_data_row_num_1 |
| q40        | COLLECT SET WINDOW (We may never really be able to do this well)                                       | select array_sort(collect_set(f_data_low_unique_1)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW PARTITION BY f_key4_* order by f_data_row_num_1) |
| q41        | COLLECT LIST WINDOW (We may never really be able to do this well)                                      | select collect_list(f_data_low_unique_1) OVER (ROWS BETWEEN complexity PRECEDING and CURRENT ROW PARTITION BY f_key4_* order by f_data_row_num_1) |
| q42        | Conditional equi-join with CAST in non-equality condition (regression test for #14283).                | SELECT a_key4_1, a_data_low_unique_1, f_data_low_unique_1 FROM a_facts JOIN f_facts on a_key4_1 = f_key4_1 AND a_data_low_unique_1 < CAST(f_data_low_unique_1 AS BIGINT) AND CAST(f_data_row_num_1 AS BIGINT) > a_data_low_unique_1 |


## Submit

The Scale Test can be submitted to Spark just as a normal Spark application.
The input arguments for the application are shown below:

```bash
Usage: ScaleTest [options] <scale factor> <complexity> <format> <input directory> <output directory> <path to save report file>

  <scale factor>           scale factor for data size
  <complexity>             complexity level for processing
  <format>                 output format for the data
  <input directory>        input directory for table data
  <output directory>       directory for query output
  <path to save report file>
                           path to save the report file that contains test results
  -d, --seed <value>       seed used to generate random data columns. default is 41 if not specified
  --overwrite              Flag argument. Whether to overwrite the existing data in the path.
  --iterations <value>     iterations to run for each query. default: 1
  --queries <value>        Specify queries to run specifically. the format must be query names with comma separated. e.g. --tables q1,q2,q3. If not specified, all queries will be run for `--iterations` rounds
  --timeout <value>        timeout for each query in milliseconds, default is 10 minutes(600000)
  --dry                    Flag argument. Only print the queries but not execute them
```

An example command to launch the Scale Test:

```bash
$SPARK_HOME/bin/spark-submit \
--master spark://<SPARK_MASTER>:7077 \
--conf spark.driver.memory=10G \
--conf spark.executor.memory=32G \
--conf spark.sql.parquet.int96RebaseModeInWrite=CORRECTED \
--conf spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED \
--jars $SPARK_HOME/examples/jars/scopt_2.12-3.7.1.jar \
--class com.nvidia.spark.rapids.tests.scaletest.ScaleTest \
./target/rapids-4-spark-integration-tests_2.12-26.06.1-SNAPSHOT-spark332.jar \
10 \
100 \
parquet \
<path-to-data-generated-by-data-gen-tool> \
./output \
./report.json \
--overwrite \
--queries q1,q2
```

## Test Report

Test results are recorded in the report file in JSON format. Currently only execution time is recorded. The record looks like below:

```json
[ {
  "name" : "q1",
  "executionTime" : [ 5175 ]
}, {
  "name" : "q2",
  "executionTime" : [ 2830 ]
} ]

```