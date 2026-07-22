# Copyright (c) 2023-2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from fastparquet_utils import get_fastparquet_result_canonicalizer
from spark_session import is_databricks_runtime, is_databricks133_or_later, spark_version, with_cpu_session, with_gpu_session


def fastparquet_unavailable():
    """
    Checks whether the `fastparquet` module is unavailable. Helps skip fastparquet tests.
    :return: True, if fastparquet is not available. Else, False.
    """
    try:
        import fastparquet
        return False
    except ImportError:
        return True


rebase_write_corrected_conf = {
    'spark.sql.parquet.datetimeRebaseModeInWrite': 'CORRECTED',
    'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED'
}

pandas_min_date = date(year=1677, month=9, day=22)   # Pandas.Timestamp.min, rounded up.
pandas_max_date = date(year=2262, month=4, day=11)   # Pandas.Timestamp.max, rounded down.
pandas_min_datetime = datetime(1677, 9, 21, 00, 12, 44, 0,
                               tzinfo=timezone.utc)  # Pandas.Timestamp.min, rounded up.
pandas_max_datetime = datetime(2262, 4, 11, 23, 47, 16, 0,
                               tzinfo=timezone.utc)  # Pandas.Timestamp.max, rounded down.


def copy_to_local(spark, hdfs_source, local_target):
    """
    Copies contents of hdfs_source to local_target.
    """
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    config = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
    fs.copyToLocalFile(Path(hdfs_source), Path(local_target))


def delete_local_directory(local_path):
    """
    Removes specified directory on local filesystem, along with all its contents.
    :param local_path: String path on local filesystem
    """
    print("Removing " + local_path)
    # Clean up local data path.
    if os.path.exists(path=local_path):
        try:
            import shutil
            shutil.rmtree(path=local_path)
        except OSError:
            print("Could not clean up local files in {}".format(local_path))


def read_parquet(data_path, local_data_path):
    """
    (Fetches a function that) Reads Parquet from the specified `data_path`.
    If the plugin is enabled, the read is done via Spark APIs, through the plugin.
    If the plugin is disabled, the data is copied to local_data_path, and read via `fastparquet`.
    :param data_path: Location of the (single) Parquet input file.
    :param local_data_path: Location of the Parquet input, on the local filesystem.
    :return: A function that reads Parquet, via the plugin or `fastparquet`.
    """

    def read_with_fastparquet_or_plugin(spark):
        import fastparquet
        plugin_enabled = spark.conf.get("spark.rapids.sql.enabled", "false") == "true"
        if plugin_enabled:
            return spark.read.parquet(data_path)
        else:
            copy_to_local(spark, data_path, local_data_path)
            df = fastparquet.ParquetFile(local_data_path).to_pandas()
            return spark.createDataFrame(df)

    return read_with_fastparquet_or_plugin


@pytest.mark.skipif(condition=is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Fastparquet is incompatible with Databricks versions < 13.3. "
                           "(https://github.com/NVIDIA/spark-rapids/issues/13197)")
@pytest.mark.skipif(condition=fastparquet_unavailable(),
                    reason="fastparquet is required for testing fastparquet compatibility")
@pytest.mark.skipif(condition=spark_version() < "3.4.0",
                    reason="spark.createDataFrame(pandasDF) fails with prior versions of Spark: "
                           "\"AttributeError: 'DataFrame' object has no attribute 'iteritems'. "
                           "Did you mean: 'isetitem'?\"")
@pytest.mark.parametrize('data_gen', [
    ByteGen(nullable=False),
    ShortGen(nullable=False),
    IntegerGen(nullable=False),
    pytest.param(IntegerGen(nullable=True),
                 marks=pytest.mark.xfail(reason="Nullables cause merge errors, when converting to Spark dataframe")),
    LongGen(nullable=False),
    pytest.param(FloatGen(nullable=False),
                 marks=pytest.mark.xfail(is_databricks_runtime(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/9778")),
    pytest.param(DoubleGen(nullable=False),
                 marks=pytest.mark.xfail(is_databricks_runtime(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/9778")),
    StringGen(nullable=False),
    pytest.param(DecimalGen(nullable=False),
                 marks=pytest.mark.xfail(reason="fastparquet reads Decimal columns as Float, as per "
                                                "https://fastparquet.readthedocs.io/en/latest/details.html#data-types")),
    pytest.param(DateGen(nullable=False,
                         start=pandas_min_date,
                         end=pandas_max_date),
                 marks=pytest.mark.xfail(reason="fastparquet reads dates as timestamps.")),
    pytest.param(DateGen(nullable=False),
                 marks=pytest.mark.xfail(reason="fastparquet reads far future dates (e.g. year=8705) incorrectly.")),
    pytest.param(TimestampGen(nullable=False,
                              start=pandas_min_datetime,
                              end=pandas_max_datetime),
                 marks=pytest.mark.skipif(condition=is_not_utc(),
                                          reason="fastparquet interprets timestamps in UTC timezone, regardless "
                                                 "of timezone settings")),  # Vanilla case.
    pytest.param(TimestampGen(nullable=False,
                              start=pandas_min_datetime,
                              end=pandas_max_datetime),
                 marks=pytest.mark.xfail(reason="fastparquet reads timestamps preceding 1900 incorrectly.")),
    pytest.param(
        ArrayGen(child_gen=IntegerGen(nullable=False), nullable=False),
        marks=pytest.mark.xfail(reason="Conversion from Pandas dataframe (read with fastparquet) to Spark dataframe "
                                       "fails: \"Unable to infer the type of the field a\".")),

    pytest.param(
        StructGen(children=[("first", IntegerGen(nullable=False)),
                            ("second", FloatGen(nullable=False))], nullable=False),
        marks=pytest.mark.xfail(is_databricks_runtime(),
                                reason="https://github.com/NVIDIA/spark-rapids/issues/9778")),
], ids=idfn)
def test_reading_file_written_by_spark_cpu(data_gen, spark_tmp_path):
    """
    This test writes data_gen output to Parquet via Apache Spark, then verifies that fastparquet and the RAPIDS
    plugin read the data identically.
    There are xfails here because of limitations in converting Spark dataframes to Pandas, if they contain nulls,
    as well as limitations in fastparquet's handling of Dates, Timestamps, Decimals, etc.
    """
    data_path = spark_tmp_path + "/FASTPARQUET_SINGLE_COLUMN_INPUT"
    local_base_path = (spark_tmp_path + "_local")
    local_data_path = local_base_path + "/FASTPARQUET_SINGLE_COLUMN_INPUT"
    gen = StructGen([('a', data_gen)], nullable=False)
    # Write data with CPU session.
    with_cpu_session(
        # Single output file, to avoid differences in order of file reads.
        lambda spark: gen_df(spark, gen, 2048).repartition(1).write.mode('overwrite').parquet(data_path),
        conf=rebase_write_corrected_conf
    )

    try:
        # Read Parquet with CPU (fastparquet) and GPU (plugin), and compare records.
        assert_gpu_and_cpu_are_equal_collect(read_parquet(data_path, local_data_path),
                                             result_canonicalize_func_before_compare=get_fastparquet_result_canonicalizer())
    finally:
        # Clean up local copy of data.
        delete_local_directory(local_base_path)


@pytest.mark.skipif(condition=is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Fastparquet is incompatible with Databricks versions < 13.3. "
                           "(https://github.com/NVIDIA/spark-rapids/issues/13197)")
@pytest.mark.skipif(condition=fastparquet_unavailable(),
                    reason="fastparquet is required for testing fastparquet compatibility")
@pytest.mark.skipif(condition=spark_version() < "3.4.0",
                    reason="spark.createDataFrame(pandasDF) fails with prior versions of Spark: "
                           "\"AttributeError: 'DataFrame' object has no attribute 'iteritems'. "
                           "Did you mean: 'isetitem'?\"")
@pytest.mark.parametrize('column_gen', [
    ByteGen(nullable=False),
    ShortGen(nullable=False),
    IntegerGen(nullable=False),
    pytest.param(IntegerGen(nullable=True),
                 marks=pytest.mark.xfail(reason="Nullables cause merge errors, when converting to Spark dataframe")),
    LongGen(nullable=False),
    pytest.param(LongGen(nullable=True),
                 marks=pytest.mark.xfail(reason="Nullables cause merge errors, when converting to Spark dataframe")),
    pytest.param(FloatGen(nullable=False),
                 marks=pytest.mark.xfail(is_databricks_runtime(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/9778")),
    pytest.param(DoubleGen(nullable=False),
                 marks=pytest.mark.xfail(is_databricks_runtime(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/9778")),
    StringGen(nullable=False),
    pytest.param(DecimalGen(nullable=False),
                 marks=pytest.mark.xfail(reason="fastparquet reads Decimal columns as Float, as per "
                                                "https://fastparquet.readthedocs.io/en/latest/details.html#data-types")),
    pytest.param(DateGen(nullable=False,
                         start=pandas_min_date,
                         end=pandas_max_date),
                 marks=pytest.mark.xfail(reason="fastparquet reads dates as timestamps.")),
    pytest.param(DateGen(nullable=False),
                 marks=pytest.mark.xfail(reason="fastparquet reads far future dates (e.g. year=8705) incorrectly.")),
    pytest.param(TimestampGen(nullable=False,
                              start=pandas_min_datetime,
                              end=pandas_max_datetime),
                 marks=pytest.mark.skipif(condition=is_not_utc(),
                                          reason="fastparquet interprets timestamps in UTC timezone, regardless "
                                                 "of timezone settings")),  # Vanilla case.
    pytest.param(TimestampGen(nullable=False,
                              start=datetime(1, 1, 1, tzinfo=timezone.utc),
                              end=pandas_min_datetime),
                 marks=pytest.mark.xfail(reason="fastparquet reads timestamps preceding 1900 incorrectly.")),
], ids=idfn)
def test_reading_file_written_with_gpu(spark_tmp_path, column_gen):
    """
    This test writes the data-gen output to file via the RAPIDS plugin, then checks that the data is read identically
    via fastparquet and Spark.
    There are xfails here because of fastparquet limitations in handling Decimal, Timestamps, Dates, etc.
    """
    data_path = spark_tmp_path + "/FASTPARQUET_TEST_GPU_WRITE_PATH"
    local_base_path = (spark_tmp_path + "_local")
    local_data_path = local_base_path + "/FASTPARQUET_SINGLE_COLUMN_INPUT"

    gen = StructGen([('a', column_gen),
                     ('part', IntegerGen(nullable=False))
                     ], nullable=False)

    # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
    conf = copy_and_update(rebase_write_corrected_conf, {'spark.sql.adaptive.enabled': 'false'})

    # Write data out with Spark RAPIDS plugin.
    with_gpu_session(
        lambda spark: gen_df(spark, gen, 2048).repartition(1).write.mode('overwrite').parquet(data_path),
        conf=conf
    )

    try:
        # For now, this compares the results of reading back the GPU-written data, via fastparquet and GPU.
        assert_gpu_and_cpu_are_equal_collect(read_parquet(data_path=data_path, local_data_path=local_data_path),
                                             conf=conf,
                                             result_canonicalize_func_before_compare=
                                             get_fastparquet_result_canonicalizer())
    finally:
        # Clean up local copy of data.
        delete_local_directory(local_base_path)


def copy_from_local(spark, local_source, hdfs_target):
    """
    Copies contents of local_source to hdfs_target.
    """
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    config = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(config)
    fs.copyFromLocalFile(Path(local_source), Path(hdfs_target))


@pytest.mark.skipif(condition=is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Fastparquet is incompatible with Databricks versions < 13.3. "
                           "(https://github.com/NVIDIA/spark-rapids/issues/13197)")
@pytest.mark.skipif(condition=fastparquet_unavailable(),
                    reason="fastparquet is required for testing fastparquet compatibility")
@pytest.mark.parametrize('column_gen', [
    ByteGen(nullable=False),
    ByteGen(nullable=True),
    ShortGen(nullable=False),
    ShortGen(nullable=True),
    IntegerGen(nullable=False),
    IntegerGen(nullable=True),
    LongGen(nullable=False),
    LongGen(nullable=True),
    FloatGen(nullable=False),
    FloatGen(nullable=True),
    DoubleGen(nullable=False),
    DoubleGen(nullable=True),
    DecimalGen(nullable=False),
    DecimalGen(nullable=True),
    pytest.param(
        StringGen(nullable=False),
        marks=pytest.mark.xfail(reason="String columns written with fastparquet are read differently between "
                                       "Apache Spark, and the Spark RAPIDS plugin. "
                                       "See https://github.com/NVIDIA/spark-rapids/issues/9387.")),
    pytest.param(
        StringGen(nullable=True),
        marks=pytest.mark.xfail(reason="String columns written with fastparquet are read differently between "
                                       "Apache Spark, and the Spark RAPIDS plugin. "
                                       "See https://github.com/NVIDIA/spark-rapids/issues/9387.")),
    pytest.param(
        DateGen(nullable=False,
                start=pandas_min_date,
                end=pandas_max_date),
        marks=pytest.mark.xfail(reason="spark_df.toPandas() problem: Dates generated in Spark can't be written "
                                       "with fastparquet, because the dtype/encoding cannot be deduced. "
                                       "This test has a workaround in test_reading_file_rewritten_with_fastparquet.")),
    pytest.param(
        TimestampGen(nullable=False),
        marks=pytest.mark.xfail(reason="Old timestamps are out of bounds for Pandas. E.g.:  "
                                       "\"pandas._libs.tslibs.np_datetime.OutOfBoundsDatetime: Out of bounds "
                                       "nanosecond timestamp: 740-07-19 18:09:56\"."
                                       "This test has a workaround in test_reading_file_rewritten_with_fastparquet.")),
    pytest.param(
        TimestampGen(nullable=False,
                     start=pandas_min_datetime,
                     end=pandas_max_datetime),
        marks=pytest.mark.xfail(reason="spark_df.toPandas() problem: Timestamps in Spark can't be "
                                       "converted to pandas, because of type errors. The error states: "
                                       "\"TypeError: Casting to unit-less dtype 'datetime64' is not supported. "
                                       "Pass e.g. 'datetime64[ns]' instead.\" This test has a workaround in "
                                       "test_reading_file_rewritten_with_fastparquet.")),
    pytest.param(
        ArrayGen(IntegerGen(nullable=False), nullable=False),
        marks=pytest.mark.xfail(reason="spark.toPandas() problem: toPandas() converts Array columns into String. "
                                       "The test then fails with the same problem as with String columns. "
                                       "See https://github.com/NVIDIA/spark-rapids/issues/9387."
                                       "This test has a workaround in test_reading_file_rewritten_with_fastparquet.")),
], ids=idfn)
def test_reading_file_written_with_fastparquet(column_gen, spark_tmp_path):
    """
    This test writes data-gen output with fastparquet, and checks that both Apache Spark and the RAPIDS plugin
    read the written data correctly.
    """
    suffix = "/FASTPARQUET_WRITE_PATH"
    data_path = spark_tmp_path + suffix
    local_base_path = (spark_tmp_path + "_local")
    local_data_path = local_base_path + suffix

    def write_with_fastparquet(spark, data_gen):
        import fastparquet
        #  TODO: (future) Compression settings?
        dataframe = gen_df(spark, data_gen, 2048)
        os.makedirs(name=local_base_path, exist_ok=True)
        fastparquet.write(local_data_path, dataframe.toPandas())
        try:
            # Copy out to cluster's filesystem (possibly HDFS).
            copy_from_local(spark=spark, local_source=local_data_path, hdfs_target=data_path)
        finally:
            # Remove local copy.
            delete_local_directory(local_path=local_base_path)


    gen = StructGen([('a', column_gen),
                     ('part', IntegerGen(nullable=False))
                     ], nullable=False)
    # Write data with CPU session.
    with_cpu_session(
        lambda spark: write_with_fastparquet(spark, gen)
    )
    # Read Parquet with CPU (Apache Spark) and GPU (plugin), and compare records.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(data_path),
        rebase_write_corrected_conf)


@pytest.mark.skipif(condition=is_databricks_runtime() and not is_databricks133_or_later(),
                    reason="Fastparquet is incompatible with Databricks versions < 13.3. "
                           "(https://github.com/NVIDIA/spark-rapids/issues/13197)")
@pytest.mark.skipif(condition=fastparquet_unavailable(),
                    reason="fastparquet is required for testing fastparquet compatibility")
@pytest.mark.parametrize('column_gen, time_format', [
    pytest.param(
        DateGen(nullable=False,
                 start=pandas_min_date,
                 end=pandas_max_date), 'int64',
        marks=pytest.mark.xfail(reason="Apache Spark and the plugin both have problems reading dates written via "
                                       "fastparquet, if written in int64: "
                                       "\"Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false)).\"")),
    pytest.param(
        DateGen(nullable=False), 'int96',
        marks=pytest.mark.xfail(reason="fastparquet does not support int96RebaseModeInWrite, for dates before "
                                       "1582-10-15 or timestamps before 1900-01-01T00:00:00Z. "
                                       "This messes up reads from Apache Spark and the plugin.")),
    (DateGen(nullable=False,
             start=date(year=2000, month=1, day=1),
             end=date(year=2020, month=12, day=31)), 'int96'),
    (DateGen(nullable=True,
            start=date(year=2000, month=1, day=1),
            end=date(year=2020, month=12, day=31)), 'int96'),
    pytest.param(
        TimestampGen(nullable=False,
            start=datetime(2000, 1, 1, tzinfo=timezone.utc),
            end=datetime(2200, 12, 31, tzinfo=timezone.utc)), 'int64',
        marks=pytest.mark.xfail(reason="Apache Spark and the plugin both have problems reading timestamps written via "
                                       "fastparquet, if written in int64: "
                                       "\"Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false)).\"")),
    (TimestampGen(nullable=False,
                 start=datetime(2000, 1, 1, tzinfo=timezone.utc),
                 end=datetime(2200, 12, 31, tzinfo=timezone.utc)), 'int96'),
    (TimestampGen(nullable=True,
                  start=datetime(2000, 1, 1, tzinfo=timezone.utc),
                  end=datetime(2200, 12, 31, tzinfo=timezone.utc)), 'int96'),
    pytest.param(
        TimestampGen(nullable=False), 'int96',
        marks=pytest.mark.xfail(reason="fastparquet does not support int96RebaseModeInWrite, for dates before "
                                       "1582-10-15 or timestamps before 1900-01-01T00:00:00Z. "
                                       "This messes up reads from Apache Spark and the plugin.")),
    pytest.param(
        ArrayGen(nullable=False, child_gen=IntegerGen(nullable=False)), 'int96',
        marks=pytest.mark.xfail(reason="fastparquet fails to serialize array elements with any available encoding. "
                                       "E.g. \"Error converting column 'a' to bytes using encoding JSON. "
                                       "Original error: Object of type int32 is not JSON serializable\".")),
    (StructGen(nullable=False, children=[('first', IntegerGen(nullable=False))]), 'int96'),
    pytest.param(
        StructGen(nullable=True, children=[('first', IntegerGen(nullable=False))]), 'int96',
        marks=pytest.mark.xfail(reason="fastparquet fails to read nullable Struct columns written from Apache Spark. "
                                       "It fails the rewrite to parquet, thereby failing the test.")),
], ids=idfn)
def test_reading_file_rewritten_with_fastparquet(column_gen, time_format, spark_tmp_path):
    """
    This test is a workaround to test data-types that have problems being converted
    from Spark dataframes to Pandas dataframes.
    For instance, sparkDF.toPandas() incorrectly converts ARRAY<INT> columns into
    STRING columns.
    This test writes the Spark dataframe into a temporary file, and then uses
    `fastparquet` to read and write the file again, to the final destination.
    The final file should be in the correct format, with the right datatypes.
    This is then checked for read-accuracy, via CPU and GPU.
    """
    suffix = "/FASTPARQUET_WRITE_PATH"
    hdfs_data_path = spark_tmp_path + suffix
    local_base_path = (spark_tmp_path + "_local")
    local_data_path = local_base_path + suffix

    def rewrite_with_fastparquet(spark, data_gen):
        """
        This helper function (eventually) writes data generated from `data_gen` to the local filesystem,
        via fastparquet.
        To preserve data types from data_gen, the writes are done first through Spark, thus:
          1. Write to HDFS with Spark. (This preserves data types.)
          2. Copy the data to local FS (so that fastparquet can read it).
          3. Read data with fastparquet, write it back out to local FS with fastparquet.
          4. Copy the fastparquet output back to HDFS, for reads with Spark.
        """
        import fastparquet
        hdfs_tmp_data_path = hdfs_data_path + "_tmp"
        spark_df = gen_df(spark, data_gen, 2048)
        # 1. Write to HDFS with Spark.
        spark_df.repartition(1).write.mode("overwrite").parquet(hdfs_tmp_data_path)
        # Make local base directory.
        os.makedirs(name=local_base_path, exist_ok=True)
        # 2. Copy Spark-written data to local filesystem, for read with Parquet.
        local_tmp_data_path = local_data_path + "_tmp"
        copy_to_local(spark=spark, hdfs_source=hdfs_tmp_data_path, local_target=local_tmp_data_path)
        # 3. Read local tmp data with fastparquet, rewrite to final local path, with fastparquet.
        pandas_df = fastparquet.ParquetFile(local_tmp_data_path).to_pandas()
        fastparquet.write(local_data_path, pandas_df, times=time_format)
        # 4. Copy fastparquet-written data back to HDFS, so that Spark can read it.
        copy_from_local(spark=spark, local_source=local_data_path, hdfs_target=hdfs_data_path)
        # Local data can now be cleaned up.
        delete_local_directory(local_base_path)

    gen = StructGen([('a', column_gen),
                     ('part', IntegerGen(nullable=False))], nullable=False)
    # Write data with CPU session.
    with_cpu_session(
        lambda spark: rewrite_with_fastparquet(spark, gen)
    )
    # Read Parquet with CPU (Apache Spark) and GPU (plugin), and compare records.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(hdfs_data_path),
        rebase_write_corrected_conf)
