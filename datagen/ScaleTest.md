# Scale Test

## Introduction of Scale Test Suite

The Scale Test suite is composed by 2 parts: data generation tool and test query
sets.

For the data generation tool, it leverages the big data generation library code
to produce large scale data. We defined several tables in the tool and put the schema
at the [table schema](#table-schema) section.

The tool is a part of the the datagen module and it will be compiled together with
the datagen library.

The entry class of the tool is `com.nvidia.rapids.tests.scaletest.ScaleTestDataGen`.
It will be submitted as the main class to Spark to run the data generation.

### User interface

The input arguments for this tool is described as below:

```bash
Usage: DataGenEntry [options] <scale factor> <complexity> <format> <output directory>

  <scale factor>        scale factor for data size
  <complexity>          complexity level for processing
  <format>              output format for the data
  <output directory>    output directory for data generated
  -t, --tables <value>  tables to generate. If not specified, all tables will be generated
  -d, --seed <value>    seed used to generate random data columns. default is 41 if not specified
  --overwrite           Flag argument. Whether to overwrite the existing data in the path.

```

The data generation tool can be used just like a normal Spark application, user
can submit it to Spark master with essential parameters:

```bash
$SPARK_HOME/bin/spark-submit \
--master spark://<SPARK_MASTER>:7077 \
--conf spark.driver.memory=10G \
--conf spark.executor.memory=32G \
--conf spark.sql.parquet.int96RebaseModeInWrite=CORRECTED \
--conf spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED \
--class com.nvidia.rapids.tests.scaletest.ScaleTestDataGen \ # the main class
--jars $SPARK_HOME/examples/jars/scopt_2.12-3.7.1.jar \ # one dependency jar just shipped with Spark under $SPARK_HOME
./target/datagen_2.12-26.08.0-SNAPSHOT-spark332.jar \
1 \
10 \
parquet \
<PATH_TO_SAVE_DATA>
```

Then a folder with name pattern: `SCALE_<scale factor>_<complexity>_<format>_<data gen tool version>_<seed>`
will be created under the `<PATH_TO_SAVE_DATA>` that user just provided.

## Test Query Sets
Please refer to [Query Sets](../integration_tests/ScaleTest.md#query-sets) in `integration_tests` module for more details.

## Table Metadata and Schema

This section was originally described in [issue-8813](https://github.com/NVIDIA/spark-rapids/issues/8813#issue-1822958165)
Put a copy of it here to make it more friendly to new users.

### Table naming:

Tables will be given a lower case letter prefix then an underscore followed by an arbitrary lower case suffix. The letter is there to limit us to 26 tables. If we need more than 26 we are doing something wrong.

### Key group naming:

Key groups don’t have names, but are all assigned numeric IDs starting with 1. The key group id may or may not show up in the names of columns, but it should remain consistent when generating the data.

### Column naming:

The names of key columns will be in a few forms. 
   * If the column is the primary key for a table, where it is a unique key per row, then it will be named `primary_${table_prefix}`.
   * If the column is intended to be joined with the primary key from another table it will be names `${table_prefix}_foreign_${foreign_table_prefix}`
   * If the key is a part of a keygroup that can be joined with other tables it will be named `${table_prefix}_key${key_group}_${column_number}` where `${column_number}` is the number of the column in the key group.
   * If a column is a data column, one that has processing done on it, or is just a ride-along column, then the name will be `${table_prefix}_data_${column_number}` where `${column_number}` is the number of the column in the set of data columns for the table.
   * If a data column has a very special purpose it can be named `${table_prefix}_data_${description}` instead to distinguish it.

It is assumed that if specific data types are needed for individual tests that the schema of the table will be used to find compatible columns.

### Key Groups:

There are several different key groups that we want to test. They are listed below. What is not listed below is the overlap in key groups. That will be handled in the table descriptions themselves.
   1. The primary key for table a. In table a each row will be unique
   2. Adjusted key group. The number of columns will correspond to the complexity. The types of the columns should be selected from the set {string, decimal(7, 2), decimal(15, 2), int, long, timestamp, date, struct<num: long, desc: string>}
   3. 3-column key group with the types string, date, long.
   4. 1 column that is an int with a unique count of 5

### Ride-along/Data Columns:

Unless specified columns are going to be generated with the default random value ranges. These data values should initially come from the set of types `{int, long, Decimal(7, 2), Decimal(19, 4), string, map<string, struct<lat: Decimal(7,4), lon: Deicmal(7, 4)>>, timestamp, date}` and should be selected in a deterministic way.

### Table schemas

  1. `a_facts`: Each scale factor corresponds to 10,000 rows
      * `primary_a` the primary key from key group 1
      * key group 4
      * `a_data_low_unique_1` data column that is a long with a unique count of 5
      * `a_data_low_unique_len_1` data column that is a string with variable length ranging from 1 to 5 inclusive (even distribution of lengths)
      * `complexity` data columns
   2. `b_data`: Each scale factor corresponds to 1,000,000 rows. (The row group size should be configured to be 512 MiB when writing parquet or equivalent when writing ORC)
      * `b_foreign_a` Should overlap with `a_facts.primary_a` about 99% of the time
      * key group 3 with 3 columns (The unique count should be about 99% of the total number of rows).
      * 10 data columns of various types
      * 10 decimal(10, 2) columns with a relatively small value range
  3. `c_data`: Each scale factor corresponds to 100,000 rows.
      * `c_foreign_a` Should overlap with `a_facts.primary_a` about 50% of the time
      * key group 2 columns up to the complexity (each key should show up about 10 times)
      * `c_data_row_num_1` 1 data column that is essentially the row number for order by tests in window
      * 5 data columns
      * 5 numeric data columns
   4. `d_data`: Each scale factor corresponds to 100,000 rows.
      * key group 2 complexity columns (each key should show up about 10 time, but the overlap with `c_data` for key group 2 should only be about 50%)
      * 10 data columns
   5. `e_data`: Each scale factor corresponds to 1,000,000 rows.
      * key group 3 with 3 columns (the unique count should be about 99% of the total number of rows and overlap with `b_data` key group 3 by about 90%).
      * 10 data columns
   6. `f_facts`: Each scale factor corresponds to 10,000 rows
      * key group 4
      * `f_data_low_unqie_1` data column that is a long with a unique count of 5 but overlap with `a_data_low_unique_1` is only 1 of the 5.
      * `f_data_low_unique_len_1` data column that is a string with variable length ranging from 1 to 5 inclusive (even distribution of lengths)
      * `f_data_row_num_1` long column which is essentially the row number for window ordering
      * complexity/2 data columns
   7. `g_data`: Each scale factor corresponds to 1,000,000 rows. (The row group size should be configured to be 512 MiB)
      * key group 3 with 3 columns (should be skewed massively so there are a few keys with lots of values and a long tail with few).
      * `g_data_enum_1` 1 string column with 5 unique values in it (an ENUM of sorts)
      * `g_data_row_num_1` 1 long data column that is essentially the row number for range tests in window.
      * 20 byte data columns
      * 10 string data columns
