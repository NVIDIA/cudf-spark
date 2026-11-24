# Copyright (c) 2026, NVIDIA CORPORATION.
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

from spark_session import with_cpu_session, with_gpu_session, with_spark_session


_canonical_sql_enabled = 'spark.cudf.sql.enabled'
_legacy_sql_enabled = 'spark.rapids.sql.enabled'


def _executed_plan(spark):
    df = spark.range(4).selectExpr('id + 1 AS x')
    df.collect()
    return df._jdf.queryExecution().executedPlan().toString()


def _job_type(spark):
    return spark.sparkContext.getLocalProperty('spark.job.description').rsplit('[', 1)[1]


def test_conflicting_aliases_reach_the_plugin_and_canonical_wins():
    def check(spark):
        # The harness keeps both names so the plugin, not the harness, resolves the conflict.
        assert spark.conf.get(_legacy_sql_enabled) == 'false'
        assert spark.conf.get(_canonical_sql_enabled) == 'true'
        assert 'Gpu' in _executed_plan(spark)
    with_spark_session(check, {_legacy_sql_enabled: 'false', _canonical_sql_enabled: 'true'})


def test_canonical_test_conf_replaces_legacy_default():
    def check(spark):
        assert spark.conf.get('spark.cudf.sql.incompatibleOps.enabled') == 'true'
        assert spark.conf.get('spark.rapids.sql.incompatibleOps.enabled', None) is None
    with_spark_session(check, {'spark.cudf.sql.incompatibleOps.enabled': 'true'})


def test_cpu_session_overrides_canonical_test_conf():
    def check(spark):
        assert spark.conf.get(_legacy_sql_enabled) == 'false'
        assert spark.conf.get(_canonical_sql_enabled, None) is None
        assert _job_type(spark) == 'CPU]'
        assert 'Gpu' not in _executed_plan(spark)
    with_cpu_session(check, {_canonical_sql_enabled: 'true'})


def test_gpu_session_overrides_canonical_test_conf():
    def check(spark):
        assert spark.conf.get(_legacy_sql_enabled) == 'true'
        assert spark.conf.get(_canonical_sql_enabled, None) is None
        assert _job_type(spark) == 'GPU]'
        assert 'Gpu' in _executed_plan(spark)
    with_gpu_session(check, {_canonical_sql_enabled: 'false'})


def test_in_test_legacy_write_reaches_the_plugin():
    def check(spark):
        spark.conf.set(_legacy_sql_enabled, 'false')
        assert 'Gpu' not in _executed_plan(spark)
        spark.conf.set(_legacy_sql_enabled, 'true')
        assert 'Gpu' in _executed_plan(spark)
    with_gpu_session(check)
