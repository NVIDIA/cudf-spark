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

import re

import pytest
import pyspark.sql.functions as f

from asserts import (assert_cpu_and_gpu_are_equal_collect_with_capture,
                     assert_gpu_and_cpu_error, assert_gpu_fallback_collect)
from data_gen import non_utc_allow
from marks import allow_non_gpu, incompat
from spark_session import is_before_spark_350


_CONF = {
    'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
    'spark.sql.ansi.enabled': False,
    'spark.rapids.sql.incompatibleDateFormats.enabled': False,
    'spark.rapids.sql.expression.cpuBridge.enabled': True,
}
_LOCAL_FORMAT = 'yyyy-MM-dd HH:mm:ss.SSS'
_OFFSET_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
_TIMEZONES = ['UTC', 'Asia/Shanghai', 'America/New_York']


@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('separator', [' ', "'T'"])
@pytest.mark.parametrize('fraction', ['', '.SSS'])
@pytest.mark.parametrize('offset', ['', 'X', 'XX', 'XXX'])
@pytest.mark.parametrize('zone', _TIMEZONES)
def test_timestamp_format_corrected(separator, fraction, offset, zone):
    pattern = 'yyyy-MM-dd' + separator + 'HH:mm:ss' + fraction + offset
    sep = separator.replace("'", '')
    # Feed all offset spellings to every pattern, including invalid/truncated forms.
    # The Spark CPU result is the oracle for each X width and each fractional width.
    values = [None, '']
    for digits in ['', '.', '.1', '.12', '.123', '.1234', '.000', '.999']:
        for suffix in ['', 'Z', '+08', '+0800', '+0830', '+08:30', '-05:30',
                       '+00:00', '-00:00', '+18:00', '-18:00', '+18:01', '+19:00',
                       '+08:60', '+8:30', '+08:3', '+083', '+08:30:00', '+08:30:01',
                       '+083001', '+18:00:01', '+08:30:60', 'z']:
            values.append('1969-12-31' + sep + '23:59:59' + digits + suffix)
    tail = ('.123' if fraction else '') + ('Z' if offset else '')
    for date in ['2024-02-29', '2023-02-29', '2024-13-01', '2024-01-32']:
        values.append(date + sep + '12:34:56' + tail)
    for time in ['24:00:00', '23:60:00', '23:59:60', '1:00:00']:
        values.append('2024-01-01' + sep + time + tail)
    valid = '2024-01-01' + sep + '12:34:56' + tail
    values += [valid, valid.lower(), valid + ' ', ' ' + valid, valid + 'tail',
               valid.replace(sep, 'T' if sep == ' ' else ' ', 1)]

    def query(spark):
        df = spark.createDataFrame([(v,) for v in values], 'a string')
        return df.select('a', f.to_timestamp('a', pattern),
                         f.unix_timestamp('a', pattern), f.to_date('a', pattern))

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, exist_classes='GpuGetTimestamp,GpuUnixTimestamp',
        non_exist_classes='GpuCpuBridgeExpression',
        conf=dict(_CONF, **{'spark.sql.session.timeZone': zone}))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('zone', _TIMEZONES)
@pytest.mark.parametrize('policy', ['CORRECTED', 'EXCEPTION'])
@pytest.mark.parametrize('pattern', [_LOCAL_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSS",
                                   "yyyy-MM-dd'T'HH:mm:ss"])
def test_timestamp_millisecond_formatting(zone, policy, pattern):
    # Match Spark's fractional truncation, including negative epoch values.
    values = ['1969-12-31 23:59:59.999999', '1970-01-01 00:00:00.000001',
              '2024-03-10T06:59:59.123456Z', '2024-03-10T07:00:00.987654Z',
              '2024-11-03T05:59:59.123456Z', '2024-11-03T06:00:00.987654Z', None]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame([(v,) for v in values], 'a string').select(
            f.date_format(f.col('a').cast('timestamp'), pattern),
            f.from_unixtime(f.col('a').cast('timestamp').cast('long'), pattern)),
        exist_classes='GpuDateFormatClass,GpuFromUnixTime',
        non_exist_classes='GpuCpuBridgeExpression',
        conf=dict(_CONF, **{'spark.sql.session.timeZone': zone,
                           'spark.sql.legacy.timeParserPolicy': policy}))


# Floating-point ROUND has an existing compatibility opt-in. Date-format compatibility
# remains disabled so this test verifies native support for the new formats.
@incompat
@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('zone', _TIMEZONES)
def test_timestamp_format_rounding_projection(zone):
    values = ['2024-02-29 12:34:56.1', '2024-02-29 12:34:56.12',
              '2024-02-29T12:34:56.123+08:30', '2024-02-29T12:34:56.123Z',
              '1969-12-31 23:59:59.999', 'invalid', None]

    def query(spark):
        df = spark.createDataFrame([(v,) for v in values], 'a string')
        timestamp = f.coalesce(f.to_timestamp('a', _LOCAL_FORMAT),
                               f.to_timestamp('a', _OFFSET_FORMAT))
        rounded = (f.round(f.unix_timestamp(timestamp) / f.lit(600)) * 600).cast('timestamp')
        return df.select('a', f.when(f.col('a').isNotNull(),
                                    f.date_format(rounded, _LOCAL_FORMAT)).otherwise(f.lit(None)))

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        query, exist_classes='GpuGetTimestamp,GpuDateFormatClass',
        non_exist_classes='GpuCpuBridgeExpression',
        conf=dict(_CONF, **{'spark.sql.session.timeZone': zone}))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('pattern,value', [
    (_LOCAL_FORMAT, '2024-01-01 00:00:00.1'),
    (_OFFSET_FORMAT, '2024-01-01T00:00:00.12+08:30'),
])
@pytest.mark.parametrize('ansi', [False, True])
def test_timestamp_fraction_ansi_valid(pattern, value, ansi):
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame([(value,), (None,)], 'a string').select(
            f.to_timestamp('a', pattern)),
        exist_classes='GpuGetTimestamp', non_exist_classes='GpuCpuBridgeExpression',
        conf=dict(_CONF, **{'spark.sql.ansi.enabled': ansi}))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('conditional', ['if', 'case'])
def test_timestamp_fraction_ansi_conditional(conditional):
    values = [(0, False, 'invalid'), (1, True, '2024-01-01 00:00:00.123'),
              (2, None, 'invalid'), (3, True, None)]
    parsed = "to_timestamp(a, '{}')".format(_LOCAL_FORMAT)
    expression = ('if(ok, {}, cast(null as timestamp))'.format(parsed)
                  if conditional == 'if' else
                  'case when ok then {} else cast(null as timestamp) end'.format(parsed))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(values, 'id int, ok boolean, a string').selectExpr(
            'id', expression),
        exist_classes='GpuGetTimestamp', non_exist_classes='GpuCpuBridgeExpression',
        conf=dict(_CONF, **{'spark.sql.ansi.enabled': True}))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('zone,pattern,value', [
    ('UTC', _LOCAL_FORMAT, '+294247-01-10 04:00:54.775'),
    ('UTC', _LOCAL_FORMAT, '-290308-12-21 19:59:06.000'),
    ('+08:00', _LOCAL_FORMAT, '+294247-01-10 04:00:54.776'),
    ('-05:00', _LOCAL_FORMAT, '-290308-12-21 19:59:05.225'),
    ('America/New_York', _LOCAL_FORMAT, '-290308-12-21 19:59:05.225'),
    ('Asia/Shanghai', _LOCAL_FORMAT + 'XXX', '+294247-01-10 12:30:54.775+08:30'),
])
def test_timestamp_arithmetic_boundary_valid(zone, pattern, value):
    # Resolve the session/input zone before checking seconds * 1000000 + fraction.
    # Return epoch seconds so Python's datetime year limit is irrelevant.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame([(value,), (None,)], 'a string').select(
            f.to_timestamp('a', pattern).cast('long')),
        exist_classes='GpuGetTimestamp', non_exist_classes='GpuCpuBridgeExpression',
        conf=dict(_CONF, **{'spark.sql.session.timeZone': zone}))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('zone,pattern,value', [
    ('UTC', _LOCAL_FORMAT, '+294247-01-10 04:00:54.776'),
    ('UTC', _LOCAL_FORMAT, '-290308-12-21 19:59:05.225'),
    ('-05:00', _LOCAL_FORMAT, '+294247-01-10 04:00:54.775'),
    ('+08:00', _LOCAL_FORMAT, '-290308-12-21 19:59:06.000'),
    ('America/New_York', _LOCAL_FORMAT, '+294247-01-10 04:00:54.775'),
    ('America/New_York', _LOCAL_FORMAT + 'XXX', '+294247-01-10 12:30:54.776+08:30'),
    ('Asia/Shanghai', _LOCAL_FORMAT + 'XXX', '-290308-12-22 04:29:05.225+08:30'),
    ('UTC', _LOCAL_FORMAT, '+999999999-01-01 00:00:00.000'),
    ('UTC', _LOCAL_FORMAT, '-999999999-01-01 00:00:00.000'),
])
@pytest.mark.parametrize('ansi', [False, True])
def test_timestamp_arithmetic_overflow(zone, pattern, value, ansi):
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame([(value,)], 'a string').select(
            f.to_timestamp('a', pattern)).collect(),
        conf=dict(_CONF, **{'spark.sql.ansi.enabled': ansi,
                           'spark.sql.session.timeZone': zone}),
        error_message=re.compile('ArithmeticException: long overflow'))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('ansi', [False, True])
@pytest.mark.parametrize('conditional', ['if', 'case'])
def test_timestamp_arithmetic_overflow_conditional(ansi, conditional):
    values = [(0, False, '+294247-01-10 04:00:54.776'),
              (1, True, '2024-01-01 00:00:00.123'),
              (2, None, '-290308-12-21 19:59:05.225')]
    parsed = "to_timestamp(a, '{}')".format(_LOCAL_FORMAT)
    expression = ('if(ok, {}, cast(null as timestamp))'.format(parsed)
                  if conditional == 'if' else
                  'case when ok then {} else cast(null as timestamp) end'.format(parsed))
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(values, 'id int, ok boolean, a string').selectExpr(
            'id', expression),
        exist_classes='GpuGetTimestamp', non_exist_classes='GpuCpuBridgeExpression',
        conf=dict(_CONF, **{'spark.sql.ansi.enabled': ansi,
                           'spark.sql.session.timeZone': 'UTC'}))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.skipif(is_before_spark_350(), reason='try_to_timestamp requires Spark 3.5')
def test_try_timestamp_arithmetic_overflow():
    # try_to_timestamp suppresses parse errors, but Spark still propagates arithmetic overflow.
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame(
            [('+294247-01-10 04:00:54.776',)], 'a string').select(
                f.call_function('try_to_timestamp', f.col('a'), f.lit(_LOCAL_FORMAT))).collect(),
        conf=dict(_CONF, **{'spark.sql.ansi.enabled': True,
                           'spark.sql.session.timeZone': 'UTC'}),
        error_message=re.compile('ArithmeticException: long overflow'))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('value', ['2024-01-01 00:00:00.1234', '2024-02-30 00:00:00.123'])
def test_timestamp_fraction_ansi_invalid(value):
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame([(value,)], 'a string').select(
            f.to_timestamp('a', _LOCAL_FORMAT)).collect(),
        conf=dict(_CONF, **{'spark.sql.ansi.enabled': True}),
        error_message=re.compile(
            'CANNOT_PARSE_TIMESTAMP|DateTimeParseException|'
            'Exception occurred when parsing timestamp'))


@allow_non_gpu(*non_utc_allow)
@pytest.mark.skipif(is_before_spark_350(), reason='try_to_timestamp requires Spark 3.5')
def test_try_timestamp_fraction_ansi_invalid():
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(
            [('2024-01-01 00:00:00.1234',), ('2024-01-01 00:00:00.1',)], 'a string').select(
                f.call_function('try_to_timestamp', f.col('a'), f.lit(_LOCAL_FORMAT))),
        exist_classes='GpuGetTimestamp', non_exist_classes='GpuCpuBridgeExpression',
        conf=dict(_CONF, **{'spark.sql.ansi.enabled': True}))


@allow_non_gpu('ProjectExec', 'GetTimestamp', *non_utc_allow)
@pytest.mark.parametrize('policy', ['LEGACY', 'EXCEPTION'])
@pytest.mark.parametrize('incompat', [False, True])
@pytest.mark.parametrize('pattern,value', [
    (_LOCAL_FORMAT, '2024-01-01 00:00:00.123'),
    (_OFFSET_FORMAT, '2024-01-01T00:00:00.123+08:30'),
])
def test_timestamp_fraction_unsupported_policy_fallback(policy, incompat, pattern, value):
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame([(value,)], 'a string').select(
            f.to_timestamp('a', pattern)), 'GetTimestamp',
        conf=dict(_CONF, **{'spark.sql.legacy.timeParserPolicy': policy,
                           'spark.rapids.sql.incompatibleDateFormats.enabled': incompat,
                           'spark.rapids.sql.expression.cpuBridge.enabled': False}))
