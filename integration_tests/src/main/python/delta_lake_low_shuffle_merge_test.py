# Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

import pyspark.sql.functions as f
import pytest

from conftest import is_databricks_runtime
from delta_lake_merge_common import *
from marks import *
from pyspark.sql.types import *
from spark_session import is_databricks_version, spark_version

delta_merge_enabled_conf = copy_and_update(delta_writes_enabled_conf,
                                           {"spark.rapids.sql.command.MergeIntoCommand": "true",
                            "spark.rapids.sql.command.MergeIntoCommandEdge": "true",
                            "spark.rapids.sql.delta.lowShuffleMerge.enabled": "true",
                            "spark.rapids.sql.format.parquet.reader.type": "PERFILE",
                            "spark.databricks.delta.deletionVectors.useMetadataRowIndex": "true",
                            "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled":
                                "true"})

def supports_delta_low_shuffle_merge():
    return is_databricks_version(17, 3) or \
        (not is_databricks_runtime() and spark_version().startswith("3.4"))


def _assert_gpu_low_shuffle_merge(
        do_merge, data_path, conf, expect_write=True, expect_low_shuffle=True):
    assert expect_write
    cpu_result = with_cpu_session(lambda spark: do_merge(spark, data_path + "/CPU"), conf=conf)

    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        gpu_result = with_gpu_session(
            lambda spark: do_merge(spark, data_path + "/GPU"), conf=conf)
        captured_plans = callback.getResultsWithTimeout(10000)
    finally:
        callback.endCapture()

    assert_equal(cpu_result, gpu_result)
    if expect_low_shuffle:
        assert any(callback.contains(plan, "GpuUnionExec") for plan in captured_plans), \
            "GpuUnionExec was not found in the captured low-shuffle MERGE write plans"
        if is_databricks_version(17, 3):
            assert any(callback.contains(plan, "GpuFileSourceScanExec") and
                       "__metadata_row_index" in str(plan) for plan in captured_plans), \
                "GPU row-index discovery scan was not found in the captured MERGE plans"


@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_low_shuffle_merge_when_gpu_file_scan_override_failed(spark_tmp_path,
                                                                    spark_tmp_table_factory,
                                                                    use_cdf, num_slices):
    # Need to eliminate duplicate keys in the source table otherwise update semantics are ambiguous
    src_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, num_slices=num_slices).groupBy("a").agg(f.max("b").alias("b"))
    dest_table_func = lambda spark: two_col_df(spark, int_gen, string_gen, seed=1, num_slices=num_slices)
    merge_sql = "MERGE INTO {dest_table} USING {src_table} ON {dest_table}.a == {src_table}.a" \
                " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"

    conf = copy_and_update(delta_merge_enabled_conf,
                           {
                               "spark.rapids.sql.exec.FileSourceScanExec": "false",
                               # Disable auto broadcast join due to this issue:
                               # https://github.com/NVIDIA/spark-rapids/issues/10973
                               "spark.sql.autoBroadcastJoinThreshold": "-1"
                            })
    assert_delta_sql_merge_collect(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                                   src_table_func, dest_table_func, merge_sql, False, conf=conf)



@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("table_ranges", [(range(20), range(10)),  # partial insert of source
                                          (range(5), range(5)),  # no-op insert
                                          (range(10), range(20, 30))  # full insert of source
                                          ], ids=idfn)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_not_match_insert_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                           use_cdf, partition_columns, num_slices):
    do_test_delta_merge_not_match_insert_only(spark_tmp_path, spark_tmp_table_factory,
                                              table_ranges, use_cdf, False, partition_columns,
                                              num_slices, False, delta_merge_enabled_conf)

# DBR 17.3 AQE can replace a no-match join with its row-based EmptyRelationExec.
@allow_non_gpu("EmptyRelationExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("table_ranges", [(range(10), range(20)),  # partial delete of target
                                          (range(5), range(5)),  # full delete of target
                                          (range(10), range(20, 30))  # no-op delete
                                          ], ids=idfn)
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                       use_cdf, partition_columns, num_slices):
    do_test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                          use_cdf, False, partition_columns, num_slices, False,
                                          delta_merge_enabled_conf)

@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, num_slices):
    do_test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                                        num_slices, False, delta_merge_enabled_conf,
                                        assert_func=_assert_gpu_low_shuffle_merge)


@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 effective duplicate-match semantics")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
@pytest.mark.parametrize("src_rows", [
    pytest.param([(1, "chosen", True), (1, "ignored", False), (4, "inserted", True)],
                 id="one_effective"),
    pytest.param([(1, "ignored-1", False), (1, "ignored-2", False),
                  (4, "inserted", True)], id="none_effective")
])
def test_delta_low_shuffle_merge_accepts_non_effective_duplicate_matches(
        spark_tmp_path, spark_tmp_table_factory, use_cdf, src_rows):
    def src_table_func(spark):
        return spark.createDataFrame(src_rows, "k INT, v STRING, apply BOOLEAN")

    def dest_table_func(spark):
        return spark.createDataFrame([(1, "old"), (2, "keep")], "k INT, v STRING")

    merge_sql = ("MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
                 "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s.v "
                 "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)")
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        assert_func=_assert_gpu_low_shuffle_merge, conf=delta_merge_enabled_conf)


@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 effective duplicate-match semantics")
def test_delta_low_shuffle_merge_rejects_effective_duplicate_matches(
        spark_tmp_path, spark_tmp_table_factory):
    src_table = spark_tmp_table_factory.get()

    def do_merge(spark):
        gpu_enabled = \
            str(spark.conf.get("spark.rapids.sql.enabled", "false")).lower() == "true"
        target_path = spark_tmp_path + ("/GPU" if gpu_enabled else "/CPU")
        spark.createDataFrame([(1, "old")], "k INT, v STRING") \
            .write.format("delta") \
            .option("delta.enableDeletionVectors", "false") \
            .mode("overwrite") \
            .save(target_path)
        spark.createDataFrame(
            [(1, "first", True), (1, "second", True)],
            "k INT, v STRING, apply BOOLEAN").createOrReplaceTempView(src_table)
        return spark.sql(
            "MERGE INTO delta.`{}` t USING {} s ON t.k = s.k "
            "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s.v".format(
                target_path, src_table)).collect()

    assert_gpu_and_cpu_error(
        do_merge,
        conf=delta_merge_enabled_conf,
        error_message="DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE")


@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 low-shuffle helper-column regression")
def test_delta_low_shuffle_merge_internal_column_names(
        spark_tmp_path, spark_tmp_table_factory):
    def src_table_func(spark):
        return spark.createDataFrame(
            [(1, True, "chosen", 10, "source"),
             (1, False, "ignored", 11, "source-ignored"),
             (4, True, "inserted", 40, "source-inserted")],
            "k INT, apply BOOLEAN, _row_dropped_ STRING, _incr_metrics_ INT, "
            "_source_row_present_ STRING")

    def dest_table_func(spark):
        return spark.createDataFrame(
            [(1, "old", 100, "target"), (2, "keep", 200, "target-keep")],
            "k INT, _row_dropped_ STRING, _incr_metrics_ INT, "
            "_target_row_present_ STRING")

    merge_sql = ("MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
                 "WHEN MATCHED AND s.apply THEN UPDATE SET "
                 "t._row_dropped_ = s._row_dropped_, "
                 "t._incr_metrics_ = s._incr_metrics_, "
                 "t._target_row_present_ = s._source_row_present_ "
                 "WHEN NOT MATCHED THEN INSERT (k, _row_dropped_, _incr_metrics_, "
                 "_target_row_present_) VALUES (s.k, s._row_dropped_, s._incr_metrics_, "
                 "s._source_row_present_)")
    # DBR's CPU MERGE uses these same fixed helper names and fails during analysis, so there is no
    # valid CPU oracle for this regression. Run the GPU implementation and compare with the
    # explicit expected rows instead.
    data_path = spark_tmp_path + "/DELTA_DATA/GPU"
    src_table = spark_tmp_table_factory.get()
    dest_table = spark_tmp_table_factory.get()

    def setup_tables(spark):
        setup_delta_dest_table(
            spark, data_path, dest_table_func, use_cdf=False,
            enable_deletion_vectors=False)
        src_table_func(spark).createOrReplaceTempView(src_table)

    with_cpu_session(setup_tables, conf=delta_merge_enabled_conf)

    def do_merge(spark):
        read_delta_path(spark, data_path).createOrReplaceTempView(dest_table)
        return spark.sql(merge_sql.format(
            src_table=src_table, dest_table=dest_table)).collect()

    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        with_gpu_session(do_merge, conf=delta_merge_enabled_conf)
        captured_plans = callback.getResultsWithTimeout(10000)
    finally:
        callback.endCapture()

    actual = with_cpu_session(
        lambda spark: read_delta_path(spark, data_path).orderBy("k").collect(),
        conf=delta_merge_enabled_conf)
    assert [tuple(row) for row in actual] == [
        (1, "chosen", 10, "source"),
        (2, "keep", 200, "target-keep"),
        (4, "inserted", 40, "source-inserted")]
    assert any(callback.contains(plan, "GpuUnionExec") for plan in captured_plans), \
        "GpuUnionExec was not found in the captured low-shuffle MERGE write plans"
    assert any(callback.contains(plan, "GpuFileSourceScanExec") and
               "__metadata_row_index" in str(plan) for plan in captured_plans), \
        "GPU row-index discovery scan was not found in the captured MERGE plans"


@allow_non_gpu("ColumnarToRowExec", "FileSourceScanExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 low-shuffle row-tracking regression")
def test_delta_low_shuffle_merge_preserves_row_tracking(spark_tmp_path):
    conf = copy_and_update(delta_merge_enabled_conf, delta_row_tracking_dml_conf)
    data_path = spark_tmp_path + "/DELTA_DATA"
    with_cpu_session(lambda spark: setup_delta_row_tracking_dest_tables(
        spark, data_path, row_tracking_dml_test_df), conf=conf)
    merge_sql = ("MERGE INTO delta.`{path}` t "
                 "USING (SELECT * FROM VALUES (2, 'B', 'y'), (9, 'I', 'y') "
                 "AS s(a, b, c)) s ON t.a = s.a "
                 "WHEN MATCHED THEN UPDATE SET t.c = s.c "
                 "WHEN NOT MATCHED THEN INSERT *")

    def tracked_rows(spark, path):
        rows = spark.sql(
            "SELECT a, b, c, _metadata.row_id AS row_id, "
            "_metadata.row_commit_version AS row_commit_version "
            "FROM delta.`{}`".format(path)).collect()
        return {r["a"]: (r["b"], r["c"], r["row_id"], r["row_commit_version"])
                for r in rows}

    before = {
        run: with_cpu_session(
            lambda spark, path=data_path + "/" + run: tracked_rows(spark, path), conf=conf)
        for run in ["CPU", "GPU"]
    }

    def do_merge(spark, path):
        return spark.sql(merge_sql.format(path=path)).collect()

    # DBR 17.3 exposes nullable row-tracking scan fields that the GPU reader does not support, so
    # low-shuffle planning intentionally falls back to the classic GPU merge executor.
    _assert_gpu_low_shuffle_merge(do_merge, data_path, conf, expect_low_shuffle=False)

    for run in ["CPU", "GPU"]:
        after = with_cpu_session(
            lambda spark, path=data_path + "/" + run: tracked_rows(spark, path), conf=conf)
        assert sorted(after) == [1, 2, 3, 4, 9], "{}: {}".format(run, after)
        for key in [1, 2, 3, 4]:
            assert after[key][2] == before[run][key][2], \
                "{}: row id of a={} changed: {} -> {}".format(
                    run, key, before[run][key], after[key])
        for key in [1, 3, 4]:
            assert after[key][3] == before[run][key][3], \
                "{}: copied row commit version changed: {} -> {}".format(
                    run, before[run][key], after[key])
        assert after[2][3] > before[run][2][3], \
            "{}: updated row commit version did not advance".format(run)
        assert after[9][2] > max(value[2] for value in before[run].values()), \
            "{}: inserted row id is not fresh".format(run)


@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 large temporary deletion-vector regression")
def test_delta_low_shuffle_merge_large_temporary_deletion_vector(
        spark_tmp_path, spark_tmp_table_factory):
    num_rows = 400000
    data_path = spark_tmp_path + "/DELTA_DATA"
    src_table = spark_tmp_table_factory.get()

    def dest_table_func(spark):
        return spark.range(num_rows).selectExpr("id", "id AS value").coalesce(1)

    def setup_tables(spark):
        setup_delta_dest_tables(
            spark, data_path, dest_table_func,
            use_cdf=False, enable_deletion_vectors=False)
        spark.range(num_rows).where(
            f.pmod(f.xxhash64("id"), f.lit(10)) == 0).selectExpr(
                "id", "id + 1 AS value").createOrReplaceTempView(src_table)

    with_cpu_session(setup_tables, conf=delta_merge_enabled_conf)

    def do_merge(spark, path):
        dest_table = spark_tmp_table_factory.get()
        read_delta_path(spark, path).createOrReplaceTempView(dest_table)
        return spark.sql(
            "MERGE INTO {dest} t USING {src} s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET t.value = s.value".format(
                dest=dest_table, src=src_table)).collect()

    _assert_gpu_low_shuffle_merge(do_merge, data_path, delta_merge_enabled_conf)

    def table_stats(spark, path):
        return read_delta_path(spark, path).select(
            f.count("*").alias("row_count"),
            f.sum(f.when(f.col("value") == f.col("id") + 1, 1).otherwise(0))
                .alias("updated_count")).collect()

    cpu_stats = with_cpu_session(
        lambda spark: table_stats(spark, data_path + "/CPU"), conf=delta_merge_enabled_conf)
    gpu_stats = with_cpu_session(
        lambda spark: table_stats(spark, data_path + "/GPU"), conf=delta_merge_enabled_conf)
    assert_equal(cpu_stats, gpu_stats)
    assert cpu_stats[0]["row_count"] == num_rows
    assert cpu_stats[0]["updated_count"] > 0


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("merge_sql", [
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN MATCHED AND s.b > 'q' THEN UPDATE SET d.a = s.a / 2, d.b = s.b" \
    " WHEN NOT MATCHED THEN INSERT *",
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN NOT MATCHED AND s.b > 'q' THEN INSERT *",
    "MERGE INTO {dest_table} d USING {src_table} s ON d.a == s.a" \
    " WHEN MATCHED AND s.b > 'a' AND s.b < 'g' THEN UPDATE SET d.a = s.a / 2, d.b = s.b" \
    " WHEN MATCHED AND s.b > 'g' AND s.b < 'z' THEN UPDATE SET d.a = s.a / 4, d.b = concat('extra', s.b)" \
    " WHEN NOT MATCHED AND s.b > 'b' AND s.b < 'f' THEN INSERT *" \
    " WHEN NOT MATCHED AND s.b > 'f' AND s.b < 'z' THEN INSERT (b) VALUES ('not here')" ], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_upsert_with_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, merge_sql, num_slices):
    do_test_delta_merge_upsert_with_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, False, 
                                              merge_sql, num_slices, False, 
                                              delta_merge_enabled_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_upsert_with_unmatchable_match_condition(spark_tmp_path, spark_tmp_table_factory, use_cdf, num_slices):
    do_test_delta_merge_upsert_with_unmatchable_match_condition(spark_tmp_path,
                                                                spark_tmp_table_factory,
                                                                use_cdf,
                                                                False,
                                                                num_slices,
                                                                False,
                                                                delta_merge_enabled_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("use_cdf", [True, False], ids=idfn)
def test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf):
    do_test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                                                delta_merge_enabled_conf)
