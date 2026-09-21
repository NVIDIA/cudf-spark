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

from uuid import UUID

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
                            "spark.rapids.sql.test.delta.lowShuffleMerge.failOnFallback": "true",
                            "spark.rapids.sql.format.parquet.reader.type": "PERFILE",
                            "spark.databricks.delta.deletionVectors.useMetadataRowIndex": "true",
                            "spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled":
                                "true"})


def supports_delta_low_shuffle_merge():
    return is_databricks_version(17, 3) or \
        (not is_databricks_runtime() and spark_version().startswith("3.4"))


_INSERT_KEY_OFFSET = 1 << 40


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
                               "spark.rapids.sql.test.delta.lowShuffleMerge.failOnFallback": "false",
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
@pytest.mark.parametrize("use_cdf", [pytest.param(True, marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13552")), False], ids=idfn)
@pytest.mark.parametrize("partition_columns", [None, ["a"], ["b"], ["a", "b"]], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                       use_cdf, partition_columns, num_slices):
    do_test_delta_merge_match_delete_only(spark_tmp_path, spark_tmp_table_factory, table_ranges,
                                          use_cdf, False, partition_columns, num_slices, False,
                                          delta_merge_enabled_conf)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("use_cdf", [pytest.param(True, marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13552")), False], ids=idfn)
@pytest.mark.parametrize("num_slices", num_slices_to_test, ids=idfn)
def test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, num_slices):
    do_test_delta_merge_standard_upsert(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                                        num_slices, False, delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 low-shuffle NOT MATCHED BY SOURCE support")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_low_shuffle_merge_not_matched_by_source(
        spark_tmp_path, spark_tmp_table_factory, use_cdf):
    def dest_table_func(spark):
        return gen_df(
            spark,
            [("a", UniqueLongGen(nullable=False)),
             ("b", IntegerGen(
                 min_val=-1000000, max_val=1000000, nullable=False, special_cases=[]))])

    def src_table_func(spark):
        generated = dest_table_func(spark)
        matched = generated.where(f.pmod("a", f.lit(4)) == 2).selectExpr(
            "a", "-b AS b")
        inserted = generated.where(f.pmod("a", f.lit(4)) == 3).selectExpr(
            "a + {} AS a".format(_INSERT_KEY_OFFSET), "b")
        return matched.unionByName(inserted)

    merge_sql = ("MERGE INTO {dest_table} d USING {src_table} s ON d.a = s.a "
                 "WHEN MATCHED THEN UPDATE SET d.b = s.b "
                 "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.a, s.b) "
                 "WHEN NOT MATCHED BY SOURCE AND pmod(d.a, 4) = 0 THEN DELETE "
                 "WHEN NOT MATCHED BY SOURCE AND pmod(d.a, 4) = 1 "
                 "THEN UPDATE SET d.b = d.b + 1")
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        conf=delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 effective duplicate-match semantics")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
@pytest.mark.parametrize("has_effective_match", [True, False], ids=idfn)
def test_delta_low_shuffle_merge_accepts_non_effective_duplicate_matches(
        spark_tmp_path, spark_tmp_table_factory, use_cdf, has_effective_match):
    def dest_table_func(spark):
        return gen_df(
            spark,
            [("k", UniqueLongGen(nullable=False)),
             ("v", StringGen(pattern="[a-z]{1,20}", nullable=False))])

    def src_table_func(spark):
        generated = dest_table_func(spark)
        matched = generated.where(f.pmod("k", f.lit(4)) == 0)
        first_match = matched.select(
            "k", f.concat(f.lit("first-"), "v").alias("v"),
            f.lit(has_effective_match).alias("apply"))
        second_match = matched.select(
            "k", f.concat(f.lit("second-"), "v").alias("v"),
            f.lit(False).alias("apply"))
        inserted = generated.where(f.pmod("k", f.lit(4)) == 1).select(
            (f.col("k") + _INSERT_KEY_OFFSET).alias("k"), "v",
            f.lit(True).alias("apply"))
        return first_match.unionByName(second_match).unionByName(inserted)

    merge_sql = ("MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
                 "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s.v "
                 "WHEN NOT MATCHED THEN INSERT (k, v) VALUES (s.k, s.v)")
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        conf=delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
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
        target = gen_df(
            spark,
            [("k", UniqueLongGen(nullable=False)),
             ("v", StringGen(pattern="[a-z]{1,20}", nullable=False))])
        target.write.format("delta") \
            .option("delta.enableDeletionVectors", "false") \
            .mode("overwrite") \
            .save(target_path)
        matched = target.where(f.pmod("k", f.lit(4)) == 0)
        first_match = matched.select(
            "k", f.concat(f.lit("first-"), "v").alias("v"),
            f.lit(True).alias("apply"))
        second_match = matched.select(
            "k", f.concat(f.lit("second-"), "v").alias("v"),
            f.lit(True).alias("apply"))
        first_match.unionByName(second_match).createOrReplaceTempView(src_table)
        return spark.sql(
            "MERGE INTO delta.`{}` t USING {} s ON t.k = s.k "
            "WHEN MATCHED AND s.apply THEN UPDATE SET t.v = s.v".format(
                target_path, src_table)).collect()

    assert_gpu_and_cpu_error(
        do_merge,
        conf=delta_merge_enabled_conf,
        error_message="DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE")


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 low-shuffle helper-column regression")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_low_shuffle_merge_internal_column_names(
        spark_tmp_path, spark_tmp_table_factory, use_cdf):
    # Use low-shuffle-specific helper names so the CPU MERGE remains a valid oracle.
    # DBR's CPU command rejects _row_dropped_ and the row-presence flags as ambiguous.
    def dest_table_func(spark):
        return gen_df(
            spark,
            [("k", UniqueLongGen(nullable=False)),
             ("v", StringGen(pattern="[a-z]{1,20}", nullable=False)),
             ("_incr_metrics_", IntegerGen(
                 min_val=-1000000, max_val=1000000, nullable=False, special_cases=[])),
             ("_metadata_file_path", StringGen(
                 pattern="[a-z]{1,20}", nullable=False)),
             ("__metadata_row_index", LongGen(nullable=False))])

    def src_table_func(spark):
        generated = dest_table_func(spark)
        matched = generated.where(f.pmod("k", f.lit(4)) == 0)
        effective = matched.select(
            "k", f.lit(True).alias("apply"),
            f.concat(f.lit("updated-"), "v").alias("v"),
            (f.col("_incr_metrics_") + 1).alias("_incr_metrics_"),
            "_metadata_file_path", "__metadata_row_index")
        ignored = matched.select(
            "k", f.lit(False).alias("apply"),
            f.concat(f.lit("ignored-"), "v").alias("v"),
            (f.col("_incr_metrics_") + 2).alias("_incr_metrics_"),
            "_metadata_file_path", "__metadata_row_index")
        inserted = generated.where(f.pmod("k", f.lit(4)) == 1).select(
            (f.col("k") + _INSERT_KEY_OFFSET).alias("k"),
            f.lit(True).alias("apply"),
            "v", "_incr_metrics_",
            "_metadata_file_path", "__metadata_row_index")
        return effective.unionByName(ignored).unionByName(inserted)

    merge_sql = ("MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
                 "AND t._metadata_file_path = s._metadata_file_path "
                 "AND t.__metadata_row_index = s.__metadata_row_index "
                 "WHEN MATCHED AND s.apply THEN UPDATE SET "
                 "t.v = s.v, t._incr_metrics_ = s._incr_metrics_ "
                 "WHEN NOT MATCHED THEN INSERT (k, v, _incr_metrics_, "
                 "_metadata_file_path, __metadata_row_index) "
                 "VALUES (s.k, s.v, s._incr_metrics_, "
                 "s._metadata_file_path, s.__metadata_row_index)")
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=use_cdf, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        conf=delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 low-shuffle nondeterministic-action regression")
@pytest.mark.parametrize("use_cdf", [False, True], ids=idfn)
def test_delta_low_shuffle_merge_non_deterministic_action_values(
        spark_tmp_path, spark_tmp_table_factory, use_cdf):
    def dest_table_func(spark):
        return gen_df(spark, [("k", UniqueLongGen(nullable=False))], length=128) \
            .withColumn("v", f.lit(0.5)).withColumn("u", f.lit(0.5)) \
            .withColumn("token", f.lit("unchanged"))

    def src_table_func(spark):
        generated = dest_table_func(spark)
        matched = generated.where(f.pmod("k", f.lit(4)) == 0)
        effective = matched.selectExpr("k", "4 AS x", "2 AS d")
        # Under ANSI mode, eagerly evaluating the unused update would divide by zero.
        ignored = matched.selectExpr("k", "4 AS x", "0 AS d")
        inserted = generated.where(f.pmod("k", f.lit(4)) == 1).selectExpr(
            "k + {} AS k".format(_INSERT_KEY_OFFSET), "4 AS x", "2 AS d")
        return effective.unionByName(ignored).unionByName(inserted)

    merge_sql = (
        "MERGE INTO {dest_table} t USING {src_table} s ON t.k = s.k "
        "WHEN MATCHED AND s.d <> 0 THEN UPDATE SET "
        "t.v = s.x / s.d + rand(7), t.token = uuid() "
        "WHEN NOT MATCHED THEN INSERT (k, v, u, token) "
        "VALUES (s.k, rand(7), rand(11) + 40, uuid()) "
        "WHEN NOT MATCHED BY SOURCE AND pmod(t.k, 4) = 2 "
        "THEN UPDATE SET t.u = rand(13) + 10, t.token = uuid()")
    conf = copy_and_update(delta_merge_enabled_conf, {
        "spark.sql.ansi.enabled": "true",
        "spark.rapids.sql.expression.cpuBridge.enabled": "false"})
    original_keys = set(with_cpu_session(
        lambda spark: [r["k"] for r in dest_table_func(spark).select("k").collect()]))
    inserted_keys = {key + _INSERT_KEY_OFFSET for key in original_keys if key % 4 == 1}
    updated_keys = {key for key in original_keys if key % 4 in (0, 2)}

    def check_func(data_path, do_merge):
        assert_collect(do_merge, data_path, conf)
        # CPU/GPU random values need not agree. Check their ranges and clause routing instead,
        # then require exact equality between each engine's table and CDF values.
        for run in ["CPU", "GPU"]:
            path = data_path + "/" + run
            rows = with_cpu_session(
                lambda spark: read_delta_path(spark, path).collect(), conf=conf)
            table = {r["k"]: (r["v"], r["u"], r["token"]) for r in rows}
            assert len(rows) == len(table) == len(original_keys | inserted_keys)
            assert set(table) == original_keys | inserted_keys
            for key, (v, u, token) in table.items():
                if key in inserted_keys:
                    assert 0 <= v < 1 and 40 <= u < 41, (run, key, v, u)
                elif key % 4 == 0:
                    assert 2 <= v < 3 and u == 0.5, (run, key, v, u)
                elif key % 4 == 2:
                    assert v == 0.5 and 10 <= u < 11, (run, key, v, u)
                else:
                    assert (v, u, token) == (0.5, 0.5, "unchanged"), (run, key, v, u, token)
                if key in updated_keys | inserted_keys:
                    assert str(UUID(token)) == token, (run, key, token)
            if use_cdf:
                def merge_changes(spark):
                    version = spark.sql(f"DESCRIBE HISTORY delta.`{path}`") \
                        .where("operation = 'MERGE'").orderBy("version", ascending=False) \
                        .first()["version"]
                    return read_delta_path_with_cdf(spark, path) \
                        .where(f"_commit_version = {version}").collect()

                changes = with_cpu_session(merge_changes, conf=conf)
                expected_changes = {(key, kind) for key in updated_keys
                                    for kind in ["update_preimage", "update_postimage"]} | \
                    {(key, "insert") for key in inserted_keys}
                assert len(changes) == len(expected_changes)
                assert {(r["k"], r["_change_type"]) for r in changes} == expected_changes
                for row in changes:
                    if row["_change_type"] == "update_preimage":
                        expected = (0.5, 0.5, "unchanged")
                    else:
                        expected = table[row["k"]]
                    actual = (row["v"], row["u"], row["token"])
                    assert actual == expected, (run, row["k"], actual, expected)

    delta_sql_merge_test(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                         src_table_func, dest_table_func, merge_sql, check_func)


# DBR 17.3 exposes nullable row-tracking fields that make low-shuffle planning fall back to the
# classic GPU merge, which consumes its GPU Parquet scan through ColumnarToRowExec.
@allow_non_gpu("ColumnarToRowExec", *delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 row-tracking fallback regression")
def test_delta_low_shuffle_merge_row_tracking_falls_back_to_classic_merge(
        spark_tmp_path, spark_tmp_table_factory):
    # This verifies row tracking through classic GPU MERGE, not the low-shuffle path.
    # TODO: Add a strict failOnFallback=true regression once nullable row-tracking scans are
    # supported by low shuffle merge (https://github.com/NVIDIA/cudf-spark/issues/11079).
    conf = copy_and_update(delta_merge_enabled_conf, delta_row_tracking_dml_conf)
    conf["spark.rapids.sql.test.delta.lowShuffleMerge.failOnFallback"] = "false"
    data_path = spark_tmp_path + "/DELTA_DATA"

    def dest_table_func(spark):
        return gen_df(
            spark,
            [("a", UniqueLongGen(nullable=False)),
             ("b", StringGen(pattern="[a-z]{1,20}", nullable=False)),
             ("c", StringGen(pattern="[a-z]{1,20}", nullable=False))],
            num_slices=1)

    with_cpu_session(lambda spark: setup_delta_row_tracking_dest_tables(
        spark, data_path, dest_table_func), conf=conf)
    src_table = spark_tmp_table_factory.get()
    merge_sql = ("MERGE INTO delta.`{path}` t "
                 "USING {src_table} s ON t.a = s.a "
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
        generated = dest_table_func(spark)
        matched = generated.where(f.pmod("a", f.lit(4)) == 0).select(
            "a", "b", f.concat(f.lit("updated-"), "c").alias("c"))
        inserted = generated.where(f.pmod("a", f.lit(4)) == 1).select(
            (f.col("a") + _INSERT_KEY_OFFSET).alias("a"), "b",
            f.concat(f.lit("inserted-"), "c").alias("c"))
        matched.unionByName(inserted).createOrReplaceTempView(src_table)
        return spark.sql(merge_sql.format(path=path, src_table=src_table)).collect()

    # DBR 17.3 exposes nullable row-tracking scan fields that the GPU reader does not support, so
    # low-shuffle planning intentionally falls back to the classic GPU merge executor.
    assert_collect(do_merge, data_path, conf)

    for run in ["CPU", "GPU"]:
        after = with_cpu_session(
            lambda spark, path=data_path + "/" + run: tracked_rows(spark, path), conf=conf)
        original_keys = set(before[run])
        updated_keys = {key for key in original_keys if key % 4 == 0}
        inserted_keys = {key + _INSERT_KEY_OFFSET
                         for key in original_keys if key % 4 == 1}
        assert set(after) == original_keys | inserted_keys, "{}: {}".format(run, after)
        for key in original_keys:
            assert after[key][2] == before[run][key][2], \
                "{}: row id of a={} changed: {} -> {}".format(
                    run, key, before[run][key], after[key])
        for key in original_keys - updated_keys:
            assert after[key][3] == before[run][key][3], \
                "{}: copied row commit version changed: {} -> {}".format(
                    run, before[run][key], after[key])
        for key in updated_keys:
            assert after[key][3] > before[run][key][3], \
                "{}: updated row commit version did not advance for a={}".format(run, key)
        max_original_row_id = max(value[2] for value in before[run].values())
        for key in inserted_keys:
            assert after[key][2] > max_original_row_id, \
                "{}: inserted row id is not fresh for a={}".format(run, key)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not is_databricks_version(17, 3),
                    reason="DBR 17.3 temporary deletion-vector regression")
def test_delta_low_shuffle_merge_temporary_deletion_vector(
        spark_tmp_path, spark_tmp_table_factory):
    def dest_table_func(spark):
        return gen_df(
            spark,
            [("id", UniqueLongGen(nullable=False)),
             ("value", IntegerGen(
                 min_val=-1000000, max_val=1000000, nullable=False, special_cases=[]))],
            num_slices=1)

    def src_table_func(spark):
        return dest_table_func(spark).where(
            f.pmod(f.xxhash64("id"), f.lit(10)) == 0).selectExpr(
                "id", "value + 1 AS value")

    merge_sql = ("MERGE INTO {dest_table} t USING {src_table} s ON t.id = s.id "
                 "WHEN MATCHED THEN UPDATE SET t.value = s.value")
    assert_delta_sql_merge_collect(
        spark_tmp_path, spark_tmp_table_factory,
        use_cdf=False, enable_deletion_vectors=False,
        src_table_func=src_table_func, dest_table_func=dest_table_func,
        merge_sql=merge_sql, compare_logs=False,
        conf=delta_merge_enabled_conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order
@pytest.mark.skipif(not supports_delta_low_shuffle_merge(),
                    reason="Low Shuffle Merge requires Delta Lake 2.4 or DBR 17.3")
@pytest.mark.parametrize("use_cdf", [pytest.param(True, marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13552")), False], ids=idfn)
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
@pytest.mark.parametrize("use_cdf", [pytest.param(True, marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/13552")), False], ids=idfn)
def test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf):
    do_test_delta_merge_update_with_aggregation(spark_tmp_path, spark_tmp_table_factory, use_cdf, False,
                                                delta_merge_enabled_conf)
