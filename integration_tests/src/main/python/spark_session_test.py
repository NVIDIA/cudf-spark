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


def _assert_session_mode(spark, enabled, job_type):
    expected = str(enabled).lower()
    assert spark.conf.get(_canonical_sql_enabled) == expected
    assert spark.conf.get(_legacy_sql_enabled) == expected
    assert spark.sparkContext.getLocalProperty('spark.job.description').endswith(
        '[{}]'.format(job_type))


def test_session_conf_aliases_prefer_canonical_values():
    with_spark_session(
        lambda spark: _assert_session_mode(spark, True, 'GPU'),
        {_legacy_sql_enabled: 'false', _canonical_sql_enabled: 'true'})


def test_cpu_session_overrides_conflicting_aliases():
    def assert_cpu_and_defaults(spark):
        _assert_session_mode(spark, False, 'CPU')
        assert spark.conf.get('spark.cudf.sql.castDecimalToFloat.enabled') == 'false'
        assert spark.conf.get('spark.rapids.sql.castDecimalToFloat.enabled') == 'false'

    with_cpu_session(
        assert_cpu_and_defaults,
        {_legacy_sql_enabled: 'true', _canonical_sql_enabled: 'true'})


def test_gpu_session_overrides_conflicting_aliases():
    with_gpu_session(
        lambda spark: _assert_session_mode(spark, True, 'GPU'),
        {_legacy_sql_enabled: 'false', _canonical_sql_enabled: 'false'})
