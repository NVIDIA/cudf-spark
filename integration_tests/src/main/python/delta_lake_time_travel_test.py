# Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import datetime
from typing import Dict

from asserts import *
from data_gen import *
from delta_lake_utils import delta_meta_allow
from marks import *
from spark_session import with_gpu_session, is_spark_353_or_later, is_databricks122_or_later
from dataclasses import dataclass


in_commit_ts_param_id = lambda val: f"in_commit_ts={val}"

@delta_lake
def test_time_travel_on_non_existing_table():
    def time_travel_on_non_existing_table():
        with_gpu_session(lambda spark: spark.sql("SELECT * FROM not_existing VERSION AS OF 0"))

    err_msg = "not_existing" if is_databricks122_or_later() else "AnalysisException"
    assert_spark_exception(time_travel_on_non_existing_table, err_msg)

@dataclass
class SetupTableResult:
    table_path: str
    table_name: str
    # key is commit version, value is if this version has data
    commit_versions: Dict[int, bool]

    def check_version_count(self, df_func):
        def check(spark, _version, _has_data):
            df = df_func(spark, _version)
            if _has_data:
                assert(df.count() > 0)
            else:
                assert(df.count() == 0)

        for version, has_data in self.commit_versions.items():
            with_cpu_session(lambda spark: check(spark, version, has_data))


    def check_versions_are_same(self, df_func):
        for version in self.commit_versions.keys():
            assert_gpu_and_cpu_are_equal_collect(lambda spark: df_func(spark, version))


def do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory, in_commit_ts = True,
                                     times = 2):
    table = spark_tmp_table_factory.get()
    table_path = f"{spark_tmp_path}/{table}"

    def setup_delta_table(spark):
        spark.sql(f"CREATE TABLE {table}(a int, b string) USING delta LOCATION '{table_path}'")
        if in_commit_ts:
            spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true');")
        df = two_col_df(spark, int_gen, string_gen)
        df.write.mode("append").format("delta").save(table_path)


    def append_to_delta_table(spark):
        df = two_col_df(spark, int_gen, string_gen)
        df.write.mode("append").format("delta").save(table_path)

    with_cpu_session(setup_delta_table)

    if times > 1:
        for _ in range(times - 1):
            with_cpu_session(append_to_delta_table)

    if in_commit_ts:
        commit_versions = {**{0: False, 1: False},  **{v: True for v in range(2, 2 + times)}}
    else:
        commit_versions = {**{0: False},  **{v: True for v in range(1, 1 + times)}}


    return SetupTableResult(table_path, table, commit_versions)

def do_get_delta_table_timestamps(spark, table_name) -> Dict[int, datetime]:
    commit_history_rows = (spark.sql(f"DESCRIBE HISTORY {table_name}")
                           .select("version", "timestamp")
                           .collect())
    def convert_ts(ts):
        if isinstance(ts, (int, float)):
            # timestamp is in microseconds
            return datetime.fromtimestamp(ts / 1000_000.0)
        else:
            return ts
    commit_map = {row.version: convert_ts(row.timestamp) for row in commit_history_rows}
    return commit_map


def enable_in_commit_ts():
    # In commit timestamp is added in oss delta 3.3.0, e.g. spark 3.5.3 for spark-rapids
    if is_spark_353_or_later():
        return [True, False]
    else:
        return [False]


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("in_commit_ts", enable_in_commit_ts(), ids=in_commit_ts_param_id)
def test_time_travel_df_version(spark_tmp_path, spark_tmp_table_factory, in_commit_ts):
    result = do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory,
                                                  in_commit_ts,
                                                  times = 3)
    def df_of_version(spark, version):
        return spark.read.format("delta").option("versionAsOf", version).load(result.table_path)

    result.check_version_count(df_of_version)
    result.check_versions_are_same(df_of_version)

@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("in_commit_ts", enable_in_commit_ts(), ids=in_commit_ts_param_id)
def test_time_travel_sql_version(spark_tmp_path, spark_tmp_table_factory, in_commit_ts):
    result = do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory,
                                                  in_commit_ts,
                                                  times = 3)
    def df_of_version(spark, version):
        return spark.sql(f"SELECT * FROM {result.table_name} VERSION AS OF {version}")

    result.check_version_count(df_of_version)
    result.check_versions_are_same(df_of_version)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("in_commit_ts", enable_in_commit_ts(), ids=in_commit_ts_param_id)
def test_time_travel_df_timestamp(spark_tmp_path, spark_tmp_table_factory, in_commit_ts):
    result = do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory,
                                                  in_commit_ts,
                                                  times = 3)
    commit_map = with_cpu_session(lambda spark: do_get_delta_table_timestamps(spark,
                                                                              result.table_name))
    def df_of_version(spark, version):
        ts = commit_map[version].isoformat()
        return spark.read.format("delta").option("timestampAsOf", ts).load(result.table_path)

    result.check_version_count(df_of_version)
    result.check_versions_are_same(df_of_version)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@ignore_order(local=True)
@pytest.mark.parametrize("in_commit_ts", enable_in_commit_ts(), ids=in_commit_ts_param_id)
def test_time_travel_sql_timestamp(spark_tmp_path, spark_tmp_table_factory, in_commit_ts):
    result = do_set_up_tables_for_time_travel(spark_tmp_path, spark_tmp_table_factory,
                                                  in_commit_ts,
                                                  times = 3)
    commit_map = with_cpu_session(lambda spark: do_get_delta_table_timestamps(spark,
                                                                              result.table_name))
    def df_of_version(spark, version):
        ts = commit_map[version].isoformat()
        return spark.sql(f"SELECT * FROM {result.table_name} TIMESTAMP AS OF '{ts}'")


    result.check_version_count(df_of_version)
    result.check_versions_are_same(df_of_version)
