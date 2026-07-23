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

from asserts import assert_gpu_and_cpu_writes_are_equal_collect, assert_gpu_fallback_write, assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from delta_lake_utils import *
from marks import *
import os
import glob
import pyarrow.parquet as pq
from spark_session import is_before_spark_320, is_databricks_runtime, supports_delta_lake_deletion_vectors, \
    with_cpu_session, with_gpu_session, is_before_spark_353, is_spark_353_or_later, \
    is_databricks173_or_later

delta_delete_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                            {"spark.rapids.sql.command.DeleteCommand": "true",
                                             "spark.rapids.sql.command.DeleteCommandEdge": "true"})


def _assert_db173_gpu_delta_scan_if_enabled(spark, df):
    if is_databricks173_or_later() and \
            str(spark.conf.get("spark.rapids.sql.enabled", "false")).lower() == "true":
        plan = df._jdf.queryExecution().executedPlan()
        callback = spark._sc._jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
        has_gpu_scan = any(
            callback.contains(plan, scan)
            for scan in ["GpuFileSourceScanExec", "GpuFileGpuScan"])
        assert has_gpu_scan, str(plan)
    return df


def _delta_sql_with_gpu_scan_assert(spark, sql):
    return _assert_db173_gpu_delta_scan_if_enabled(spark, spark.sql(sql))

def delta_sql_delete_test(spark_tmp_path, use_cdf, dest_table_func, delete_sql,
                          check_func, enable_deletion_vectors, partition_columns=None):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path, dest_table_func, use_cdf, enable_deletion_vectors, partition_columns)
    def do_delete(spark, path):
        return spark.sql(delete_sql.format(path=path))
    with_cpu_session(setup_tables)
    check_func(data_path, do_delete)

def assert_delta_sql_delete_collect(spark_tmp_path, use_cdf, dest_table_func, delete_sql,
                                    enable_deletion_vectors,
                                    partition_columns=None,
                                    conf=delta_delete_enabled_conf,
                                    skip_sql_result_check=False, expect_write=True,
                                    expected_num_affected_rows=None,
                                    assert_gpu_delete_command=False):
    def read_data(spark, path):
        read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
        df = read_func(spark, path)
        return df.sort(df.columns)

    def checker(data_path, do_delete):
        cpu_path = data_path + "/CPU"
        gpu_path = data_path + "/GPU"
        if not skip_sql_result_check:
            # compare resulting dataframe from the delete operation (some older Spark versions return empty here)
            cpu_result = with_cpu_session(lambda spark: do_delete(spark, cpu_path).collect(), conf=conf)
            if expect_write:
                gpu_result = assert_rapids_delta_write(lambda spark: do_delete(spark, gpu_path).collect(), conf=conf)
            elif assert_gpu_delete_command:
                gpu_result = assert_rapids_gpu_delete_ran(
                    lambda spark: do_delete(spark, gpu_path).collect(), conf=conf)
            else:
                gpu_result = with_gpu_session(lambda spark: do_delete(spark, gpu_path).collect(), conf=conf)
            assert_equal(cpu_result, gpu_result)
            if expected_num_affected_rows is not None:
                assert gpu_result[0][0] == expected_num_affected_rows
        # compare table data results, read both via CPU to make sure GPU write can be read by CPU
        cpu_result = with_cpu_session(lambda spark: read_data(spark, cpu_path).collect(), conf=conf)
        gpu_result = with_cpu_session(lambda spark: read_data(spark, gpu_path).collect(), conf=conf)
        assert_equal(cpu_result, gpu_result)
        # Using partition columns involves sorting, and there's no guarantees on the task
        # partitioning due to random sampling.
        if not partition_columns:
            with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
    delta_sql_delete_test(spark_tmp_path, use_cdf, dest_table_func, delete_sql, checker, enable_deletion_vectors,
                          partition_columns)

fallback_test_params = [{"spark.rapids.sql.format.delta.write.enabled": "false"},
                        {"spark.rapids.sql.format.parquet.enabled": "false"},
                        {"spark.rapids.sql.format.parquet.write.enabled": "false"},
                        {"spark.rapids.sql.command.DeleteCommand": "false"},
                        ]
if is_before_spark_353():
    # DeleteCommand is disabled by default before Spark 3.5.3
    fallback_test_params.append(delta_writes_enabled_conf)

@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("disable_conf", fallback_test_params, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
def test_delta_delete_disabled_fallback(spark_tmp_path, disable_conf, enable_deletion_vectors):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path,
                                dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                                use_cdf=False, enable_deletion_vectors=enable_deletion_vectors)
    def write_func(spark, path):
        delete_sql="DELETE FROM delta.`{}`".format(path)
        spark.sql(delete_sql)
    with_cpu_session(setup_tables)
    assert_gpu_fallback_write(write_func, read_delta_path, data_path,
                              "ExecutedCommandExec", disable_conf)

@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), \
    reason="Deletion vectors new in Delta Lake 2.4 / Apache Spark 3.4")
def test_delta_deletion_vector_fallback(spark_tmp_path, use_cdf):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_tables(spark, data_path,
                                dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                                use_cdf=use_cdf, enable_deletion_vectors=True)
    def write_func(spark, path):
        delete_sql="DELETE FROM delta.`{}`".format(path)
        spark.sql(delete_sql)
    with_cpu_session(setup_tables)
    disable_conf = copy_and_update(delta_delete_enabled_conf,
        {"spark.databricks.delta.delete.deletionVectors.persistent": "true"})

    assert_gpu_fallback_write(write_func, read_delta_path, data_path,
                              "ExecutedCommandExec", disable_conf)

@allow_non_gpu("SortExec, ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors(), \
    reason="Deletion vectors new in Delta Lake 2.4 / Apache Spark 3.4")
def test_delta_deletion_vector(spark_tmp_path):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_table(spark, data_path,
                                dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                                use_cdf=False, enable_deletion_vectors=True)
    def write_func(path):
        delete_sql="DELETE FROM delta.`{}` where a = 0".format(path)
        def delete_func(spark):
            spark.sql(delete_sql)
        return delete_func

    def read_parquet_sql(data_path):
        return lambda spark : _delta_sql_with_gpu_scan_assert(
            spark, 'select * from delta.`{}`'.format(data_path))

    with_cpu_session(setup_tables)
    with_cpu_session(write_func(data_path))

    assert_gpu_and_cpu_are_equal_collect(read_parquet_sql(data_path))

'''
This test is specifically designed to setup a parquet file with multiple row groups and we 
select a value from the last row group so the first ones get dropped to cause a misalignment 
between the deletion vectors and the parquet file when reading 

Example: only the first two values (-3268, -3267) are deleted 
    <-- Row group 1 -->|<-- Row group 2 -->|<-- Row group 3 -->
   |  -3268, -3267,...|.... 0  ,   1,......|...  3266, 3267  |
dv:|   true, true,....|...false, false,....|... false, false | 

After dropping Row group 1 and 2, we have a misalignment and now it shows 3266 and 3267 as deleted
    <-- Row group 3 -->
   |...  3266, 3267   |
dv:|   true, true,....|...false, false,....|... false, false | 
'''
@allow_non_gpu("SerializeFromObjectExec", "DeserializeToObjectExec",
               "FilterExec", "MapElementsExec", "ProjectExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors() or is_before_spark_353(), \
                    reason="Deletion vectors new in Delta Lake 2.4 / Apache Spark 3.4")
@pytest.mark.parametrize("reader_type", ["PERFILE", "COALESCING", "MULTITHREADED"])
def test_delta_deletion_vector_read_drop_row_group(spark_tmp_path, reader_type):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_table(spark, data_path,
                               # create records -3268 .... 3267 in sorted order so we can have predictable row groups
                               # coalesce the partitions to have a single file with multiple row groups
                               dest_table_func=lambda spark: unary_op_df(spark, IntegerGen(nullable=False, min_val=-3268, max_val=3267, special_cases=[]), seed=12345).sort("a").coalesce(1),
                               # limiting the block size to make sure we have more than one row group
                               use_cdf=False, enable_deletion_vectors=True, options={"parquet.block.size": "4096"})
    def write_func(path):
        # choose -3000 to make sure the deleted indices are towards the top of the file
        delete_sql=f"DELETE FROM delta.`{path}` where a < -3000"
        def delete_func(spark):
            count = spark.sql(delete_sql).collect()[0][0]
            assert(count > 0)
        return delete_func

    def read_parquet_sql(data_path, last_rg_value):
        # selecting a number from the last row group to make sure the first row groups are dropped
        return lambda spark : _delta_sql_with_gpu_scan_assert(
            spark, f"select * from delta.`{data_path}` where a = {last_rg_value}")

    enable_conf = copy_and_update(delta_delete_enabled_conf,
                                  {"spark.databricks.delta.delete.deletionVectors.persistent": "true"})

    with_cpu_session(setup_tables, conf=enable_conf)

    # Find all parquet files in the directory
    files = glob.glob(os.path.join(data_path, "*.parquet"))
    assert(len(files) == 1)
    parquet_file = pq.ParquetFile(files[0])
    # We should have more than one row group so we can drop the first ones
    assert(parquet_file.num_row_groups > 1)

    last_rg_index = parquet_file.num_row_groups - 1
    rg_meta = parquet_file.metadata.row_group(last_rg_index)
    stats = rg_meta.column(0).statistics
    lrg_min_value = stats.min

    with_cpu_session(write_func(data_path), conf=enable_conf)

    assert_gpu_and_cpu_are_equal_collect(
        read_parquet_sql(data_path, lrg_min_value),
        conf={"spark.rapids.sql.format.parquet.reader.type": reader_type,
              # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
              "spark.sql.adaptive.enabled": "false"})

@allow_non_gpu("SerializeFromObjectExec", "DeserializeToObjectExec",
               "FilterExec", "MapElementsExec", "ProjectExec")
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors() or is_before_spark_353(), \
                    reason="Deletion vectors new in Delta Lake 2.4 / Apache Spark 3.4")
@pytest.mark.parametrize("reader_type", ["PERFILE", "COALESCING", "MULTITHREADED"])
# a='' shouldn't match anything as a is an int
@pytest.mark.parametrize("condition", ["where a = 0", "", "where a = ''"])
@disable_ansi_mode
def test_delta_deletion_vector_read(spark_tmp_path, reader_type, condition):
    data_path = spark_tmp_path + "/DELTA_DATA"
    def setup_tables(spark):
        setup_delta_dest_table(spark, data_path,
                               dest_table_func=lambda spark: unary_op_df(spark, int_gen),
                               use_cdf=False, enable_deletion_vectors=True)
    def write_func(path):
        delete_sql=f"DELETE FROM delta.`{path}` {condition}"
        def delete_func(spark):
            count = spark.sql(delete_sql).collect()[0][0]
            if condition != "where a = ''":
                assert(count > 0)
            else:
                assert(count == 0)
        return delete_func

    def read_parquet_sql(data_path):
        return lambda spark : _delta_sql_with_gpu_scan_assert(
            spark, f"select * from delta.`{data_path}`")

    enable_conf = copy_and_update(delta_delete_enabled_conf,
                                  {"spark.databricks.delta.delete.deletionVectors.persistent": "true"})

    with_cpu_session(setup_tables, conf=enable_conf)
    with_cpu_session(write_func(data_path), conf=enable_conf)

    assert_gpu_and_cpu_are_equal_collect(
        read_parquet_sql(data_path),
        conf={"spark.rapids.sql.format.parquet.reader.type": reader_type,
              # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
              "spark.sql.adaptive.enabled": "false"})

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_delete_entire_table(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vectors):
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen)
    delete_sql = "DELETE FROM delta.`{path}`"
    # Databricks recently changed how the num_affected_rows is computed
    # on deletes of entire files, RAPIDS Accelerator has yet to be updated.
    # https://github.com/NVIDIA/spark-rapids/issues/8123
    skip_sql_result = is_databricks_runtime()
    assert_delta_sql_delete_collect(spark_tmp_path, use_cdf, generate_dest_data,
                                    delete_sql, enable_deletion_vectors, partition_columns,
                                    skip_sql_result_check=skip_sql_result, expect_write=False)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [["a"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values, ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
def test_delta_delete_partitions(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vectors):
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen)
    delete_sql = "DELETE FROM delta.`{path}` WHERE a = 3"
    # Databricks recently changed how the num_affected_rows is computed
    # on deletes of entire files, RAPIDS Accelerator has yet to be updated.
    # https://github.com/NVIDIA/spark-rapids/issues/8123
    skip_sql_result = is_databricks_runtime()
    assert_delta_sql_delete_collect(spark_tmp_path, use_cdf, generate_dest_data,
                                    delete_sql, enable_deletion_vectors, partition_columns,
                                    skip_sql_result_check=skip_sql_result, expect_write=False)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@datagen_overrides(seed=0, permanent=True, reason='https://github.com/NVIDIA/spark-rapids/issues/9884')
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_xfail_reasons(
                                        enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12041",
                                        disabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12047"), ids=idfn)
def test_delta_delete_rows(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vectors):
    # Databricks changes the number of files being written, so we cannot compare logs unless there's only one slice
    num_slices_to_test = 1 if is_databricks_runtime() else 10
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen, num_slices=num_slices_to_test)
    delete_sql = "DELETE FROM delta.`{path}` WHERE b < 'd'"
    assert_delta_sql_delete_collect(spark_tmp_path, use_cdf, generate_dest_data,
                                    delete_sql, enable_deletion_vectors, partition_columns)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_runtime(),
                    reason="Databricks whole-table DELETE row-count regression coverage")
def test_delta_delete_entire_table_reports_row_count(spark_tmp_path):
    def generate_dest_data(spark):
        return spark.createDataFrame(
            [(1, "a"), (1, "b"), (2, "c"), (3, "d"), (3, "e")],
            "a INT, b STRING")

    conf = copy_and_update(delta_delete_enabled_conf,
                           {"spark.databricks.delta.dmlMetricsFromMetadata.enabled": "true"})
    delete_sql = "DELETE FROM delta.`{path}`"
    # Whole-table deletes can remove files via Delta metadata only, so there may be
    # no RapidsDeltaWrite plan to capture; validate row count and final contents.
    assert_delta_sql_delete_collect(
        spark_tmp_path, use_cdf=False, dest_table_func=generate_dest_data, delete_sql=delete_sql,
        enable_deletion_vectors=False, conf=conf, expect_write=False,
        expected_num_affected_rows=5, assert_gpu_delete_command=True)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_runtime(),
                    reason="Databricks metadata-only DELETE row-count regression coverage")
def test_delta_delete_metadata_only_reports_row_count(spark_tmp_path):
    def generate_dest_data(spark):
        return spark.createDataFrame(
            [(1, "a"), (1, "b"), (2, "c"), (3, "d"), (3, "e")],
            "a INT, b STRING")

    conf = copy_and_update(delta_delete_enabled_conf,
                           {"spark.databricks.delta.dmlMetricsFromMetadata.enabled": "true"})
    delete_sql = "DELETE FROM delta.`{path}` WHERE a = 3"
    # Partition predicate deletes can also be metadata-only, so do not require a
    # captured RapidsDeltaWrite plan for this row-count regression check.
    assert_delta_sql_delete_collect(
        spark_tmp_path, use_cdf=False, dest_table_func=generate_dest_data, delete_sql=delete_sql,
        enable_deletion_vectors=False, partition_columns=["a"], conf=conf, expect_write=False,
        expected_num_affected_rows=2, assert_gpu_delete_command=True)

@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks173_or_later(),
                    reason="DBR 17.3 row tracking regression coverage")
def test_delta_delete_preserves_row_tracking_db173(spark_tmp_path):
    conf = copy_and_update(delta_delete_enabled_conf, delta_row_tracking_dml_conf)
    assert_delta_row_tracking_dml(
        spark_tmp_path, "DELETE FROM delta.`{path}` WHERE a IN (2, 3)", conf)

@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@pytest.mark.skipif(not supports_delta_lake_deletion_vectors() or is_before_spark_353(),
    reason="Deletion vectors new in Delta Lake 2.4 / Apache Spark 3.4")
def test_delta_delete_twice_with_dv(spark_tmp_path):
    """Regression test for https://github.com/NVIDIA/spark-rapids/issues/14442.
    The second DELETE on a DV-enabled table accesses _metadata.file_path and _metadata.row_index
    as nested fields. The plugin must not prune _metadata when its nested fields are still
    referenced."""
    data_path = spark_tmp_path + "/DELTA_DATA"
    def generate_dest_data(spark):
        return two_col_df(spark,
                          IntegerGen(special_cases=[100]),
                          IntegerGen(special_cases=[200]))
    conf = copy_and_update(delta_delete_enabled_conf,
        {"spark.databricks.delta.delete.deletionVectors.persistent": "true"})
    # Setup identical tables for CPU and GPU
    with_cpu_session(lambda spark: setup_delta_dest_tables(spark, data_path,
        generate_dest_data, use_cdf=False, enable_deletion_vectors=True))
    cpu_path = data_path + "/CPU"
    gpu_path = data_path + "/GPU"
    # First delete creates a deletion vector
    first_delete_sql = "DELETE FROM delta.`{path}` WHERE a = 100"
    with_cpu_session(lambda spark: spark.sql(first_delete_sql.format(path=cpu_path)).collect(), conf=conf)
    with_gpu_session(lambda spark: spark.sql(first_delete_sql.format(path=gpu_path)).collect(), conf=conf)
    # Second delete reads the table with existing DV, triggering _metadata nested field access
    second_delete_sql = "DELETE FROM delta.`{path}` WHERE b = 200"
    with_cpu_session(lambda spark: spark.sql(second_delete_sql.format(path=cpu_path)).collect(), conf=conf)
    with_gpu_session(lambda spark: spark.sql(second_delete_sql.format(path=gpu_path)).collect(), conf=conf)
    # Verify the final table state matches between CPU and GPU
    cpu_result = with_cpu_session(lambda spark:
        spark.sql("SELECT * FROM delta.`{}`".format(cpu_path)).sort("a", "b").collect(), conf=conf)
    gpu_result = with_cpu_session(lambda spark:
        spark.sql("SELECT * FROM delta.`{}`".format(gpu_path)).sort("a", "b").collect(), conf=conf)
    assert_equal(cpu_result, gpu_result)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"]], ids=idfn)
@pytest.mark.skipif(is_before_spark_320(), reason="Delta Lake writes are not supported before Spark 3.2.x")
@datagen_overrides(seed=0, permanent=True, reason='https://github.com/NVIDIA/spark-rapids/issues/9884')
@pytest.mark.parametrize("enable_deletion_vectors", deletion_vector_values_with_xfail_reasons(
                                        enabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12041",
                                        disabled_xfail_reason="https://github.com/NVIDIA/spark-rapids/issues/12047"), ids=idfn)
def test_delta_delete_dataframe_api(spark_tmp_path, use_cdf, partition_columns, enable_deletion_vectors):
    from delta.tables import DeltaTable
    data_path = spark_tmp_path + "/DELTA_DATA"
    # Databricks changes the number of files being written, so we cannot compare logs unless there's only one slice
    num_slices_to_test = 1 if is_databricks_runtime() else 10
    def generate_dest_data(spark):
        return three_col_df(spark,
                            SetValuesGen(IntegerType(), range(5)),
                            SetValuesGen(StringType(), "abcdefg"),
                            string_gen, num_slices=num_slices_to_test)
    with_cpu_session(lambda spark: setup_delta_dest_tables(spark, data_path, generate_dest_data, use_cdf, enable_deletion_vectors, partition_columns))
    def do_delete(spark, path):
        dest_table = DeltaTable.forPath(spark, path)
        dest_table.delete("b > 'c'")
    read_func = read_delta_path_with_cdf if use_cdf else read_delta_path
    assert_gpu_and_cpu_writes_are_equal_collect(do_delete, read_func, data_path,
                                                conf=delta_delete_enabled_conf)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path))
