# Copyright (c) 2020-2026, NVIDIA CORPORATION.
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
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, assert_gpu_and_cpu_error, assert_gpu_and_cpu_are_equal_sql
from conftest import is_utc, get_test_tz
from data_gen import *
from datetime import date, datetime, timezone
from dateutil import tz
from marks import allow_non_gpu, approximate_float, datagen_overrides, disable_ansi_mode, ignore_order, incompat, tz_sensitive_test
from pyspark.sql.types import *
from spark_session import with_cpu_session, is_before_spark_350, is_before_spark_400, is_databricks143_or_later
import pyspark.sql.functions as f
from timezones import all_timezones, fixed_offset_timezones, fixed_offset_timezones_iana, variable_offset_timezones, variable_offset_timezones_iana

# Some operations only work in UTC specifically
non_utc_tz_allow = ['TruncTimestamp', 'MonthsBetween'] if not is_utc() else []
# Others work in all supported time zones

# the last time that is configured to be supported by the GPU transition rules
last_supported_tz_time = datetime(2200, 12, 30, 23, 59, 59, 999999, tzinfo=timezone.utc)

tz_rules_date_gen = DateGen(end=date(2170,12,31))
tz_rules_date_n_time_gens = [tz_rules_date_gen, TimestampGen(end=last_supported_tz_time)]

# We only support literal intervals for TimeSub
vals = [(-584, 1563), (1943, 1101), (2693, 2167), (2729, 0), (44, 1534), (2635, 3319),
            (1885, -2828), (0, 2463), (932, 2286), (0, 0)]
@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timesub(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        # We are starting at year 0015 to make sure we don't go before year 0001 while doing TimeSub
        lambda spark: unary_op_df(spark, TimestampGen(start=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
            .selectExpr("a - (interval {} days {} seconds)".format(days, seconds)))

@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timeadd(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        # We are starting at year 0005 to make sure we don't go before year 0001
        # and beyond year 10000 while doing TimeAdd
        lambda spark: unary_op_df(spark, TimestampGen(start=datetime(5, 1, 1, tzinfo=timezone.utc), end=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
            .selectExpr("a + (interval {} days {} seconds)".format(days, seconds)))

@allow_non_gpu(*non_utc_allow)
def test_timeadd_daytime_column():
    gen_list = [
        # timestamp column max year is 1000
        ('t', TimestampGen(end=datetime(1000, 1, 1, tzinfo=timezone.utc))),
        # max days is 8000 year, so added result will not be out of range
        ('d', DayTimeIntervalGen(min_value=timedelta(days=0), max_value=timedelta(days=8000 * 365)))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen_list).selectExpr("t + d", "t + INTERVAL '1 02:03:04' DAY TO SECOND"))

@pytest.mark.skipif(is_before_spark_350(), reason='DayTimeInterval overflow check for seconds is not supported before Spark 3.5.0')
def test_interval_seconds_overflow_exception():
    err_message = 'outside range' if is_databricks143_or_later() else 'IllegalArgumentException'
    assert_gpu_and_cpu_error(
        lambda spark : spark.sql(""" select cast("interval '10 01:02:69' day to second" as interval day to second) """).collect(),
        conf={},
        error_message=err_message)

@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timeadd_from_subquery(data_gen):

    def fun(spark):
        df = unary_op_df(spark, TimestampGen(start=datetime(5, 1, 1, tzinfo=timezone.utc), end=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
        df.createOrReplaceTempView("testTime")
        spark.sql("select a, ((select max(a) from testTime) + interval 1 day) as datePlus from testTime").createOrReplaceTempView("testTime2")
        return spark.sql("select * from testTime2 where datePlus > current_timestamp")

    assert_gpu_and_cpu_are_equal_collect(fun)

@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timesub_from_subquery(data_gen):

    def fun(spark):
        df = unary_op_df(spark, TimestampGen(start=datetime(5, 1, 1, tzinfo=timezone.utc), end=datetime(15, 1, 1, tzinfo=timezone.utc)), seed=1)
        df.createOrReplaceTempView("testTime")
        spark.sql("select a, ((select min(a) from testTime) - interval 1 day) as dateMinus from testTime").createOrReplaceTempView("testTime2")
        return spark.sql("select * from testTime2 where dateMinus < current_timestamp")

    assert_gpu_and_cpu_are_equal_collect(fun)


@disable_ansi_mode  # ANSI mode tested separately.
# Should specify `spark.sql.legacy.interval.enabled` to test `DateAddInterval` after Spark 3.2.0,
# refer to https://issues.apache.org/jira/browse/SPARK-34896
# [SPARK-34896][SQL] Return day-time interval from dates subtraction
# 1. Add the SQL config `spark.sql.legacy.interval.enabled` which will control when Spark SQL should use `CalendarIntervalType` instead of ANSI intervals.
@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_dateaddinterval(data_gen):
    days, seconds = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), seed=1)
            .selectExpr('a + (interval {} days {} seconds)'.format(days, seconds),
            'a - (interval {} days {} seconds)'.format(days, seconds)),
        legacy_interval_enabled_conf)

# test add days(not specify hours, minutes, seconds, milliseconds, microseconds) in ANSI mode.
@pytest.mark.parametrize('data_gen', vals, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_dateaddinterval_ansi(data_gen):
    days, _ = data_gen
    # only specify the `days`
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), seed=1)
            .selectExpr('a + (interval {} days)'.format(days)),
        conf=copy_and_update(ansi_enabled_conf, legacy_interval_enabled_conf))

# Throws if add hours, minutes or seconds, milliseconds, microseconds to a date in ANSI mode
def test_dateaddinterval_ansi_exception():
    assert_gpu_and_cpu_error(
        # specify the `seconds`
        lambda spark : unary_op_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), seed=1)
            .selectExpr('a + (interval {} days {} seconds)'.format(1, 5)).collect(),
        conf=copy_and_update(ansi_enabled_conf, legacy_interval_enabled_conf),
        error_message="IllegalArgumentException")

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_datediff(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).selectExpr(
            'datediff(a, b)',
            'datediff(\'2016-03-02\', b)',
            'datediff(date(null), b)',
            'datediff(a, date(null))',
            'datediff(a, \'2016-03-02\')'))

@allow_non_gpu(*non_utc_tz_allow)
def test_months_between_runtime_fallback():
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    time_zone_gen = TimestampGen(tzinfo=tz.gettz(os.environ.get('TZ', 'UTC')))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, time_zone_gen).selectExpr('months_between(a, b, false)'))

@allow_non_gpu(*non_utc_tz_allow)
def test_months_between_tz_rules():
    time_zone_gen = TimestampGen(end=last_supported_tz_time, tzinfo=tz.gettz(os.environ.get('TZ', 'UTC')))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, time_zone_gen).selectExpr('months_between(a, b, false)'))

@allow_non_gpu(*non_utc_tz_allow)
def test_months_between_first_day():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('months_between(a, timestamp"2024-01-01", false)'))

@allow_non_gpu(*non_utc_tz_allow)
def test_months_between_last_day():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('months_between(a, timestamp"2023-12-31", false)'))

@allow_non_gpu(*non_utc_tz_allow)
@approximate_float()
def test_months_between_round_runtime_fallback():
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    time_zone_gen = TimestampGen(tzinfo=tz.gettz(os.environ.get('TZ', 'UTC')))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, time_zone_gen).selectExpr('months_between(a, b, true)'))

@allow_non_gpu(*non_utc_tz_allow)
@approximate_float()
def test_months_between_round_tz_rules():
    time_zone_gen = TimestampGen(end=last_supported_tz_time, tzinfo=tz.gettz(os.environ.get('TZ', 'UTC')))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, time_zone_gen).selectExpr('months_between(a, b, true)'))

@allow_non_gpu(*non_utc_tz_allow)
@approximate_float()
def test_months_between_first_day_round():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('months_between(a, timestamp"2024-01-01", true)'))

@allow_non_gpu(*non_utc_tz_allow)
@approximate_float()
def test_months_between_last_day_round():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, timestamp_gen).selectExpr('months_between(a, timestamp"2023-12-31", true)'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_hour_tz_rules(time_zone):
    tz_timestamp_gen = TimestampGen(end=last_supported_tz_time, tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr('hour(a)'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_hour_tz_rules_runtime_fallback(time_zone):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    tz_timestamp_gen = TimestampGen(tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr('hour(a)'))

@pytest.mark.parametrize('time_zone', fixed_offset_timezones_iana, ids=idfn)
def test_hour_fixed_offset(time_zone):
    tz_timestamp_gen = TimestampGen(tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr('hour(a)'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_minute_tz_rules(time_zone):
    tz_timestamp_gen = TimestampGen(end=last_supported_tz_time, tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, tz_timestamp_gen).selectExpr('minute(a)'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_minute_tz_rules_runtime_fallback(time_zone):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    tz_timestamp_gen = TimestampGen(tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, tz_timestamp_gen).selectExpr('minute(a)'))

@pytest.mark.parametrize('time_zone', fixed_offset_timezones_iana, ids=idfn)
def test_minute_fixed_offset(time_zone):
    tz_timestamp_gen = TimestampGen(tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, tz_timestamp_gen).selectExpr('minute(a)'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_second_tz_rules(time_zone):
    tz_timestamp_gen = TimestampGen(end=last_supported_tz_time, tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr('second(a)'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_second_tz_rules_runtime_fallback(time_zone):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    tz_timestamp_gen = TimestampGen(tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr('second(a)'))

@pytest.mark.parametrize('time_zone', fixed_offset_timezones_iana, ids=idfn)
def test_second_fixed_offset(time_zone):
    tz_timestamp_gen = TimestampGen(tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr('second(a)'))

def test_quarter():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen).selectExpr('quarter(a)'))

def test_weekday():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen).selectExpr('weekday(a)'))

def test_dayofweek():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen).selectExpr('dayofweek(a)'))

def test_last_day():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen).selectExpr('last_day(a)'))

# We have to set the upper/lower limit on IntegerGen so the date_add doesn't overflow
# Python uses proleptic gregorian date which extends Gregorian calendar as it always existed and
# always exist in future. When performing date_sub('0001-01-01', 1), it will blow up because python
# doesn't recognize dates before Jan 01, 0001. Samething with date_add('9999-01-01', 1), because
# python doesn't recognize dates after Dec 31, 9999. To get around this problem, we have limited the
# Integer value for days to stay within ~ 200 years or ~70000 days to stay within the legal limits
# python date
days_gen = [ByteGen(), ShortGen(), IntegerGen(min_val=-70000, max_val=70000, special_cases=[-70000, 7000,0,1,-1])]
@pytest.mark.parametrize('data_gen', days_gen, ids=idfn)
def test_dateadd(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), data_gen)
                .selectExpr('date_add(a, b)',
               'date_add(date(\'2016-03-02\'), b)',
               'date_add(date(null), b)',
               'date_add(a, cast(null as {}))'.format(string_type),
               'date_add(a, cast(24 as {}))'.format(string_type)))

@pytest.mark.parametrize('data_gen', days_gen, ids=idfn)
def test_datesub(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, DateGen(start=date(200, 1, 1), end=date(800, 1, 1)), data_gen)
                .selectExpr('date_sub(a, b)',
                'date_sub(date(\'2016-03-02\'), b)',
                'date_sub(date(null), b)',
                'date_sub(a, cast(null as {}))'.format(string_type),
                'date_sub(a, cast(24 as {}))'.format(string_type)))

# In order to get a bigger range of values tested for Integer days for date_sub and date_add
# we are casting the output to unix_timestamp. Even that overflows if the integer value is greater
# than 103819094 and less than -109684887 for date('9999-12-31') or greater than 107471152 and less
# than -106032829 for date('0001-01-01') so we have to cap the days values to the lower upper and
# lower ranges.
to_unix_timestamp_days_gen=[ByteGen(), ShortGen(), IntegerGen(min_val=-106032829, max_val=103819094, special_cases=[-106032829, 103819094,0,1,-1])]
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10027')
@pytest.mark.parametrize('data_gen', to_unix_timestamp_days_gen, ids=idfn)
@incompat
@allow_non_gpu(*non_utc_allow)
def test_dateadd_with_date_overflow(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, DateGen(),
           data_gen).selectExpr('unix_timestamp(date_add(a, b))',
           'unix_timestamp(date_add( date(\'2016-03-02\'), b))',
           'unix_timestamp(date_add(date(null), b))',
           'unix_timestamp(date_add(a, cast(null as {})))'.format(string_type),
           'unix_timestamp(date_add(a, cast(24 as {})))'.format(string_type)))

to_unix_timestamp_days_gen=[ByteGen(), ShortGen(), IntegerGen(max_val=106032829, min_val=-103819094, special_cases=[106032829, -103819094,0,1,-1])]
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10027')
@pytest.mark.parametrize('data_gen', to_unix_timestamp_days_gen, ids=idfn)
@incompat
@allow_non_gpu(*non_utc_allow)
def test_datesub_with_date_overflow(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, DateGen(),
           data_gen).selectExpr('unix_timestamp(date_sub(a, b))',
           'unix_timestamp(date_sub( date(\'2016-03-02\'), b))',
           'unix_timestamp(date_sub(date(null), b))',
           'unix_timestamp(date_sub(a, cast(null as {})))'.format(string_type),
           'unix_timestamp(date_sub(a, cast(24 as {})))'.format(string_type)))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_year(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.year(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_month(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.month(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_dayofmonth(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.dayofmonth(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_gens, ids=idfn)
def test_dayofyear(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.dayofyear(f.col('a'))))

@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@tz_sensitive_test
def test_unix_timestamp_runtime_fallback(data_gen):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col('a'))))

@pytest.mark.parametrize('data_gen', tz_rules_date_n_time_gens, ids=idfn)
@tz_sensitive_test
def test_unix_timestamp_tz_rules(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col('a'))))


@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_to_unix_timestamp_runtime_fallback(data_gen, ansi_enabled):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("to_unix_timestamp(a)"),
        conf = {'spark.sql.ansi.enabled': ansi_enabled})


@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen', tz_rules_date_n_time_gens, ids=idfn)
def test_to_unix_timestamp_tz_rules(data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("to_unix_timestamp(a)"),
        conf = {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_from_utc_timestamp_tz_rules(time_zone):
    tz_timestamp_gen = TimestampGen(end=last_supported_tz_time, tzinfo=timezone.utc)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr(f'from_utc_timestamp(a, "{time_zone}")'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_from_utc_timestamp_tz_rules_runtime_fallback(time_zone):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    tz_timestamp_gen = TimestampGen(tzinfo=timezone.utc)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr(f'from_utc_timestamp(a, "{time_zone}")'))

@pytest.mark.parametrize('time_zone', fixed_offset_timezones, ids=idfn)
def test_from_utc_timestamp_fixed_offset(time_zone):
    tz_timestamp_gen = TimestampGen(tzinfo=timezone.utc)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr(f'from_utc_timestamp(a, "{time_zone}")'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones, ids=idfn)
def test_from_utc_timestamp_dst_spring_forward(time_zone):
    # March 13, 2022 2:30 AM EDT doesn't exist due to DST spring forward
    # In Spark this will return the same result as 3:30 AM EDT
    # Test times around the spring forward time, including things like 2:30 AM
    start_time = datetime(2022, 3, 13, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2022, 3, 13, 4, 0, 0, 0, tzinfo=timezone.utc)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, TimestampGen(start=start_time,end=end_time))\
            .select(f.from_utc_timestamp(f.col('a'), time_zone)))

@pytest.mark.parametrize('time_zone', variable_offset_timezones, ids=idfn)
def test_from_utc_timestamp_dst_spring_backward(time_zone):
    # November 6, 2022 1:30 AM EDT exists twice due to DST fall back
    # Test times around the spring forward time, including things like 1:30 AM
    start_time = datetime(2022, 11, 6, 0, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2022, 11, 6, 3, 0, 0, 0, tzinfo=timezone.utc)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, TimestampGen(start=start_time,end=end_time))\
            .select(f.from_utc_timestamp(f.col('a'), time_zone)))

@pytest.mark.parametrize('time_zone', variable_offset_timezones, ids=idfn)
def test_from_utc_timestamp_dst_leap_year(time_zone):
    # Check that Feb 29th (leap year) is handled correctly
    start_time = datetime(2024, 2, 28, 0, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2024, 3, 2, 0, 0, 0, 0, tzinfo=timezone.utc)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, TimestampGen(start=start_time,end=end_time))\
            .select(f.from_utc_timestamp(f.col('a'), time_zone)))

@allow_non_gpu('FromUTCTimestamp')
def test_unsupported_fallback_from_utc_timestamp():
    time_zone_gen = StringGen(pattern="UTC")
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", timestamp_gen), ("tzone", time_zone_gen)]).selectExpr(
            "from_utc_timestamp(a, tzone)"),
        'FromUTCTimestamp')

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_to_utc_timestamp_tz_rules(time_zone):
    tz_timestamp_gen = TimestampGen(end=last_supported_tz_time, tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr(f'to_utc_timestamp(a, "{time_zone}")'))

@pytest.mark.parametrize('time_zone', variable_offset_timezones_iana, ids=idfn)
def test_to_utc_timestamp_tz_rules_runtime_fallback(time_zone):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    tz_timestamp_gen = TimestampGen(tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr(f'to_utc_timestamp(a, "{time_zone}")'))

@pytest.mark.parametrize('time_zone', fixed_offset_timezones_iana, ids=idfn)
def test_to_utc_timestamp_fixed_offset(time_zone):
    tz_timestamp_gen = TimestampGen(tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr(f'to_utc_timestamp(a, "{time_zone}")'))

# test from_utc_timestamp
# If `end_timestamp` is 2200 year, then generated timestamps are < 2200 year, will use GPU to compute both DST and non-DST timezones.
# If it has any generated timestamp is > 2200 year and timezone is DST, then `fallback` to CPU.
# The `fallback` means GPU operator invokes CPU to compute, not really fallback to CPU.
@pytest.mark.parametrize('time_zone', all_timezones, ids=idfn)
@pytest.mark.parametrize('end_timestamp', [last_supported_tz_time, None], ids=idfn)
def test_comprehensive_from_utc_timestamp(time_zone, end_timestamp):
    # if end = None, will use the default value
    tz_timestamp_gen = TimestampGen(end = end_timestamp, tzinfo=timezone.utc)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr(f'from_utc_timestamp(a, "{time_zone}")'))

# test to_utc_timestamp
# If `end_timestamp` is 2200 year, then generated timestamps are < 2200 year, will use GPU to compute both DST and non-DST timezones.
# If it has any generated timestamp is > 2200 year and timezone is DST, then `fallback` to CPU.
# The `fallback` means GPU operator invokes CPU to compute, not really fallback to CPU.
@pytest.mark.parametrize('time_zone', all_timezones, ids=idfn)
@pytest.mark.parametrize('end_timestamp', [last_supported_tz_time, None], ids=idfn)
def test_comprehensive_to_utc_timestamp(time_zone, end_timestamp):
    # if end = None, will use the default value
    tz_timestamp_gen = TimestampGen(end=end_timestamp, tzinfo=tz.gettz(time_zone))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, tz_timestamp_gen).selectExpr(f'to_utc_timestamp(a, "{time_zone}")'))

@allow_non_gpu('ToUTCTimestamp')
def test_unsupported_fallback_to_utc_timestamp():
    time_zone_gen = StringGen(pattern="UTC")
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", timestamp_gen), ("tzone", time_zone_gen)]).selectExpr(
            "to_utc_timestamp(a, tzone)"),
        'ToUTCTimestamp')

@allow_non_gpu('FromUnixTime')
@pytest.mark.parametrize('data_gen', [long_gen], ids=idfn)
def test_unsupported_fallback_from_unixtime(data_gen):
    fmt_gen = StringGen(pattern="[M]")
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", data_gen), ("fmt", fmt_gen)]).selectExpr(
            "from_unixtime(a, fmt)"),
        'FromUnixTime')


@pytest.mark.parametrize('invalid,fmt', [
    ('2021-01/01', 'yyyy-MM-dd'),
    ('2021/01-01', 'yyyy/MM/dd'),
    ('2021/01', 'yyyy-MM'),
    ('2021-01', 'yyyy/MM'),
    ('01/02/201', 'dd/MM/yyyy'),
    ('2021-01-01 00:00', 'yyyy-MM-dd HH:mm:ss'),
    ('19224303 22:82:35', 'yyyy-MM-dd HH:mm:ss'),
    ('01#01', 'MM-dd'),
    ('01T01', 'MM/dd'),
    ('29-02', 'dd-MM'),  # 1970-02-29 is invalid
    ('01-01', 'dd/MM'),
    ('2021-01', 'MM/yyyy'),
    ('2021-01', 'MM-yyyy'),
    ('01-02-2022', 'MM/dd/yyyy'),
    ('99-01-2022', 'MM-dd-yyyy'),
], ids=idfn)
@pytest.mark.parametrize('parser_policy', ["CORRECTED", "EXCEPTION"], ids=idfn)
@pytest.mark.parametrize('operator', ["to_unix_timestamp", "unix_timestamp", "to_timestamp", "to_date"], ids=idfn)
def test_string_to_timestamp_functions_ansi_invalid(invalid, fmt, parser_policy, operator):
    sql = "{operator}(a, '{fmt}')".format(fmt=fmt, operator=operator)
    parser_policy_dic = {"spark.sql.legacy.timeParserPolicy": "{}".format(parser_policy)}

    def fun(spark):
        df = spark.createDataFrame([(invalid,)], "a string")
        return df.selectExpr(sql).collect()

    assert_gpu_and_cpu_error(fun, conf=copy_and_update(parser_policy_dic, ansi_enabled_conf), error_message="Exception")


@pytest.mark.parametrize('parser_policy', ["CORRECTED", "EXCEPTION"], ids=idfn)
# first get expected string via `date_format`
def test_string_to_timestamp_functions_ansi_valid(parser_policy):
    expr_format = "{operator}(date_format(a, '{fmt}'), '{fmt}')"
    formats = ['yyyy-MM-dd', 'yyyy/MM/dd', 'yyyy-MM', 'yyyy/MM', 'dd/MM/yyyy', 'yyyy-MM-dd HH:mm:ss',
               'MM-dd', 'MM/dd', 'dd-MM', 'dd/MM', 'MM/yyyy', 'MM-yyyy', 'MM/dd/yyyy', 'MM-dd-yyyy']
    operators = ["to_unix_timestamp", "unix_timestamp", "to_timestamp", "to_date"]
    format_operator_pairs = [(fmt, operator) for fmt in formats for operator in operators]
    expr_list = [expr_format.format(operator=operator, fmt=fmt) for (fmt, operator) in format_operator_pairs]
    parser_policy_dic = {"spark.sql.legacy.timeParserPolicy": "{}".format(parser_policy)}

    def fun(spark):
        df = spark.createDataFrame([(datetime(1970, 8, 12, tzinfo=timezone.utc),)], "a timestamp")
        return df.selectExpr(expr_list)

    assert_gpu_and_cpu_are_equal_collect(fun, conf=copy_and_update(parser_policy_dic, ansi_enabled_conf))


@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@tz_sensitive_test
def test_unix_timestamp_runtime_fallback(data_gen, ansi_enabled):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col("a"))),
        {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen', tz_rules_date_n_time_gens, ids=idfn)
@tz_sensitive_test
def test_unix_timestamp_tz_rules(data_gen, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(f.unix_timestamp(f.col("a"))),
        {'spark.sql.ansi.enabled': ansi_enabled})


str_date_and_format_gen_all = [pytest.param(StringGen('[0-9]{4}/[01][0-9]'),'yyyy/MM', marks=pytest.mark.xfail(reason="cudf does no checks")),
        (StringGen('[0-9]{4}/[01][12]/[0-2][1-8]'),'yyyy/MM/dd'),
        (StringGen('[01][12]/[0-2][1-8]'), 'MM/dd'),
        (StringGen('[0-2][1-8]/[01][12]'), 'dd/MM'),
        (ConvertGen(DateGen(nullable=False), lambda d: d.strftime('%Y/%m').zfill(7), data_type=StringType()), 'yyyy/MM')]

str_date_and_format_gen_tz_max = [pytest.param(StringGen('[0-9]{4}/[01][0-9]'),'yyyy/MM', marks=pytest.mark.xfail(reason="cudf does no checks")),
        (StringGen('1[0-9]{3}/[01][12]/[0-2][1-8]'),'yyyy/MM/dd'),
        (StringGen('2[0-1][0-9]{2}/[01][12]/[0-2][1-8]'),'yyyy/MM/dd'),
        (StringGen('[01][12]/[0-2][1-8]'), 'MM/dd'),
        (StringGen('[0-2][1-8]/[01][12]'), 'dd/MM'),
        (ConvertGen(DateGen(nullable=False), lambda d: d.strftime('%Y/%m').zfill(7), data_type=StringType()), 'yyyy/MM')]

# get invalid date string df
def invalid_date_string_df(spark):
    return spark.createDataFrame([['invalid_date_string']], "a string")

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen_tz_max, ids=idfn)
def test_string_to_unix_timestamp_tz_rules(data_gen, date_form, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, seed=1).selectExpr("to_unix_timestamp(a, '{}')".format(date_form)),
        {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen_all, ids=idfn)
def test_string_to_unix_timestamp_runtime_fallback(data_gen, date_form, ansi_enabled):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, seed=1).selectExpr("to_unix_timestamp(a, '{}')".format(date_form)),
        {'spark.sql.ansi.enabled': ansi_enabled})

def test_string_to_unix_timestamp_ansi_exception():
    assert_gpu_and_cpu_error(
        lambda spark : invalid_date_string_df(spark).selectExpr("to_unix_timestamp(a, '{}')".format('yyyy/MM/dd')).collect(),
        error_message="Exception",
        conf=ansi_enabled_conf)

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen_tz_max, ids=idfn)
def test_string_unix_timestamp_tz_rules(data_gen, date_form, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, seed=1).select(f.unix_timestamp(f.col('a'), date_form)),
        {'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
@pytest.mark.parametrize('data_gen,date_form', str_date_and_format_gen_all, ids=idfn)
def test_string_unix_timestamp_runtime_fallback(data_gen, date_form, ansi_enabled):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, seed=1).select(f.unix_timestamp(f.col('a'), date_form)),
        {'spark.sql.ansi.enabled': ansi_enabled})

def test_string_unix_timestamp_ansi_exception():
    assert_gpu_and_cpu_error(
        lambda spark : invalid_date_string_df(spark).select(f.unix_timestamp(f.col('a'), 'yyyy/MM/dd')).collect(),
        error_message="Exception",
        conf=ansi_enabled_conf)


@disable_ansi_mode  # ANSI mode is tested separately.
@tz_sensitive_test
@pytest.mark.parametrize('parser_policy', ["CORRECTED", "EXCEPTION"], ids=idfn)
def test_to_timestamp_runtime_fallback(parser_policy):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    gen = StringGen("[0-9]{3}[1-9]-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]) ([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]")
    if get_test_tz() == "Asia/Shanghai":
        # ensure some times around transition are tested
        gen = gen.with_special_case("1991-04-14 02:00:00")\
        .with_special_case("1991-04-14 02:30:00")\
        .with_special_case("1991-04-14 03:00:00")\
        .with_special_case("1991-09-15 02:00:00")\
        .with_special_case("1991-09-15 02:30:00")\
        .with_special_case("1991-09-15 03:00:00")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, gen)
            .select(f.col("a"), f.to_timestamp(f.col("a"), "yyyy-MM-dd HH:mm:ss")),
        { "spark.sql.legacy.timeParserPolicy": parser_policy})

@disable_ansi_mode  # ANSI mode is tested separately.
@tz_sensitive_test
@pytest.mark.parametrize('parser_policy', ["CORRECTED", "EXCEPTION"], ids=idfn)
def test_to_timestamp_tz_rules(parser_policy):
    gen = StringGen("(19[0-9]{2}|20[0-9]{2}|21[0-9]{2})-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) ([0-1][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]")
    if get_test_tz() == "Asia/Shanghai":
        # ensure some times around transition are tested
        gen = gen.with_special_case("1991-04-14 02:00:00")\
        .with_special_case("1991-04-14 02:30:00")\
        .with_special_case("1991-04-14 03:00:00")\
        .with_special_case("1991-09-15 02:00:00")\
        .with_special_case("1991-09-15 02:30:00")\
        .with_special_case("1991-09-15 03:00:00")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, gen)
            .select(f.col("a"), f.to_timestamp(f.col("a"), "yyyy-MM-dd HH:mm:ss")),
        { "spark.sql.legacy.timeParserPolicy": parser_policy})

legacy_formats_for_legacy_mode = [
    pytest.param('yyyyMMdd', 'yyyyMMdd', id='yyyyMMdd'),
    pytest.param('yyyymmdd', 'yyyymmdd', id='yyyymmdd'),
    pytest.param('yyyy-mm-dd', 'yyyy-mm-dd', id='yyyy-mm-dd'),
    pytest.param('yyyyMMdd', 'yyyy-MM-dd HH:mm:ss.SSS', id='yyyyMMdd-to-millis')
]

# mm: minute; MM: month
@disable_ansi_mode
@pytest.mark.parametrize("input_format, output_format", legacy_formats_for_legacy_mode)
# Test years after 1900, refer to issues: https://github.com/NVIDIA/spark-rapids/issues/11543, https://github.com/NVIDIA/spark-rapids/issues/11539
@pytest.mark.skipif(get_test_tz() != "Asia/Shanghai" and get_test_tz() != "UTC", reason="https://github.com/NVIDIA/spark-rapids/issues/11562")
def test_formats_for_legacy_mode(input_format, output_format):
    gen = StringGen('(19[0-9]{2}|[2-9][0-9]{3})([0-9]{4})')
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, gen),
        "tab",
        '''select unix_timestamp(a, '{input_format}'),
                  from_unixtime(unix_timestamp(a, '{input_format}'), '{output_format}'),
                  date_format(to_timestamp(a, '{input_format}'), '{output_format}')
           from tab
        '''.format(input_format=input_format, output_format=output_format),
        {'spark.sql.legacy.timeParserPolicy': 'LEGACY',
         'spark.rapids.sql.incompatibleDateFormats.enabled': True})


@disable_ansi_mode
def test_date_format_timestamp_millis_legacy_prc():
    seconds_gen = LongGen(
        min_val=0,
        max_val=int(last_supported_tz_time.timestamp()),
        nullable=False,
        special_cases=[])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, seconds_gen).selectExpr(*[
            "date_format(timestamp_millis(a * 1000 + {}), "
            "'yyyy-MM-dd HH:mm:ss.SSS')".format(offset)
            for offset in [1, 123, 999]
        ]),
        {'spark.sql.legacy.timeParserPolicy': 'LEGACY',
         'spark.rapids.sql.incompatibleDateFormats.enabled': True,
         'spark.sql.session.timeZone': 'PRC'})


# mm: minute; MM: month
@disable_ansi_mode
@pytest.mark.skipif(get_test_tz() != "Asia/Shanghai" and get_test_tz() != "UTC", reason="https://github.com/NVIDIA/spark-rapids/issues/11562")
def test_formats_for_legacy_mode_other_formats_runtime_fallback():
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    format = "yyyyMMdd HH:mm:ss"
    # Test years after 1900,
    gen = StringGen('(19[0-9]{2}|[2-9][0-9]{3})([0-9]{4}) [0-9]{2}:[0-9]{2}:[0-9]{2}')
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, gen),
        "tab",
        '''select unix_timestamp(a, '{}'),
                  from_unixtime(unix_timestamp(a, '{}'), '{}'),
                  date_format(to_timestamp(a, '{}'), '{}')
           from tab
        '''.format(format, format, format, format, format),
        {'spark.sql.legacy.timeParserPolicy': 'LEGACY',
         'spark.rapids.sql.incompatibleDateFormats.enabled': True})

# mm: minute; MM: month
@disable_ansi_mode
@pytest.mark.skipif(get_test_tz() != "Asia/Shanghai" and get_test_tz() != "UTC", reason="https://github.com/NVIDIA/spark-rapids/issues/11562")
def test_formats_for_legacy_mode_other_formats_tz_rules():
    format = "yyyyMMdd HH:mm:ss"
    # Test years after 1900, before 2200 see https://github.com/NVIDIA/spark-rapids/issues/6840
    gen = StringGen('(19[0-9]{2}|2[0-1][0-9]{2})([0-9]{4}) [0-9]{2}:[0-9]{2}:[0-9]{2}')
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, gen),
        "tab",
        '''select unix_timestamp(a, '{}'),
                  from_unixtime(unix_timestamp(a, '{}'), '{}'),
                  date_format(to_timestamp(a, '{}'), '{}')
           from tab
        '''.format(format, format, format, format, format),
        {'spark.sql.legacy.timeParserPolicy': 'LEGACY',
         'spark.rapids.sql.incompatibleDateFormats.enabled': True})

@disable_ansi_mode
@allow_non_gpu('ProjectExec', 'GetTimestamp')
def test_to_timestamp_legacy_millisecond_format_fallback():
    conf = {
        'spark.sql.legacy.timeParserPolicy': 'LEGACY',
        'spark.rapids.sql.incompatibleDateFormats.enabled': True,
        'spark.rapids.sql.expression.cpuBridge.enabled': False
    }
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(
            [('2024-01-01 00:00:00.123',)], 'a string')
            .selectExpr("to_timestamp(a, 'yyyy-MM-dd HH:mm:ss.SSS')"),
        'GetTimestamp',
        conf)

@disable_ansi_mode
@tz_sensitive_test
@pytest.mark.parametrize("input_str", [
    "1999-12-31 11:59:59",
    "1999-12-31  11:59:59",
    "1999-12-31   11:59:59",
    "1999-12-31 \t 11:59:59",
    "1999-12-31 11: 59: 59",
], ids=idfn)
def test_to_timestamp_legacy_multi_whitespace(input_str):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame([(input_str,)], "a string")
            .select(f.to_timestamp(f.col("a"), "yyyy-MM-dd HH:mm:ss")),
        {'spark.sql.legacy.timeParserPolicy': 'LEGACY',
         'spark.rapids.sql.incompatibleDateFormats.enabled': True})

# LEGACY SimpleDateFormat skips only space and tab before fields, so a leading space/tab parses but
# any other leading control byte (\r \f \v \b ...) is NULL on CPU. The GPU outer trim must match
# and not strip arbitrary control characters.
@disable_ansi_mode
@tz_sensitive_test
@pytest.mark.parametrize("input_str", [
    " 2024-05-06",
    "\t2024-05-06",
    "\r2024-05-06",
    "\f2024-05-06",
    "\v2024-05-06",
    "\b2024-05-06",
], ids=idfn)
def test_to_timestamp_legacy_leading_control_chars(input_str):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame([(input_str,)], "a string")
            .select(f.to_timestamp(f.col("a"), "yyyy-MM-dd")),
        {'spark.sql.legacy.timeParserPolicy': 'LEGACY',
         'spark.rapids.sql.incompatibleDateFormats.enabled': True})

@disable_ansi_mode
@tz_sensitive_test
@pytest.mark.parametrize("parser_policy", ['LEGACY', 'CORRECTED'], ids=idfn)
def test_to_timestamp_rejects_t_separator(parser_policy):
    conf = {'spark.sql.legacy.timeParserPolicy': parser_policy}
    if parser_policy == 'LEGACY':
        conf['spark.rapids.sql.incompatibleDateFormats.enabled'] = True
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(
            [("1999-12-31 11:59:59",), ("1999-12-31T11:59:59",)], "a string")
            .select(f.to_timestamp(f.col("a"), "yyyy-MM-dd HH:mm:ss")),
        conf)

@disable_ansi_mode
@tz_sensitive_test
def test_to_timestamp_legacy_packed_rejects_t_separator():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(
            [("20241231 23:59:58",), ("20241231T23:59:58",)], "a string")
            .select(f.to_timestamp(f.col("a"), "yyyyMMdd HH:mm:ss")),
        {'spark.sql.legacy.timeParserPolicy': 'LEGACY',
         'spark.rapids.sql.incompatibleDateFormats.enabled': True})

# Spark 4.0+ defaults timeParserPolicy to CORRECTED, while the plugin's getTimeParserPolicy falls
# back to EXCEPTION. Both route through the non-legacy parser, so with the policy left UNSET the
# GPU and CPU must still agree: one ' ' separator only, no leading-whitespace trim ('T', double
# space, leading control char all NULL).
@disable_ansi_mode  # ANSI mode is tested separately.
@tz_sensitive_test
@pytest.mark.skipif(is_before_spark_400(),
                    reason="Spark 4.0+ defaults timeParserPolicy to CORRECTED")
def test_to_timestamp_unset_policy_corrected_default():
    data = [("2024-05-06 11:59:59",), ("2024-05-06  11:59:59",),
            ("2024-05-06T11:59:59",), ("\r2024-05-06 11:59:59",)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data, "a string")
            .select(f.to_timestamp(f.col("a"), "yyyy-MM-dd HH:mm:ss")))

@tz_sensitive_test
@pytest.mark.parametrize("ansi_enabled", [True, False], ids=['ANSI_ON', 'ANSI_OFF'])
def test_to_date(ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, date_gen)
            .select(f.to_date(f.col("a").cast('string'), "yyyy-MM-dd")),
        {'spark.sql.ansi.enabled': ansi_enabled})

# invalid values for operators: to_date, to_timestamp; Spark 320 and 320+ do not support special datetime: e.g.: now, today, ...
_invalid_values_for_to_datetime = ['now', 'today', '']

# test ANSI mode, invalid input
@pytest.mark.parametrize('invalid', _invalid_values_for_to_datetime)
def test_to_date_ansi_on_invalid_value(invalid):
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame([(invalid,)], "a string").select(f.to_date(f.col("a"), "yyyy-MM-dd")).collect(),
        conf = ansi_enabled_conf,
        error_message="Exception")

# test ANSI mode, invalid input
@pytest.mark.parametrize('invalid', _invalid_values_for_to_datetime)
def test_to_timestamp_ansi_on_invalid_value(invalid):
    assert_gpu_and_cpu_error(
        lambda spark: spark.createDataFrame([(invalid,)], "a string").select(f.to_timestamp(f.col("a"), "yyyy-MM-dd")).collect(),
        conf = ansi_enabled_conf,
        error_message="Exception")

@tz_sensitive_test
@pytest.mark.parametrize('data_gen', [StringGen('0[1-9][0-9]{4}')], ids=idfn)
def test_to_date_format_MMyyyy(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).select(f.to_date(f.col("a"), "MMyyyy")))

def test_to_date_ansi_exception():
    assert_gpu_and_cpu_error(
        lambda spark : invalid_date_string_df(spark).select(f.to_date(f.col("a"), "yyyy-MM-dd")).collect(),
        error_message="Exception",
        conf=ansi_enabled_conf)

supported_date_formats = ['yyyy-MM-dd', 'yyyy-MM', 'yyyy/MM/dd', 'yyyy/MM', 'dd/MM/yyyy',
                          'MM-dd', 'MM/dd', 'dd-MM', 'dd/MM']
@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', [date_gen], ids=idfn)
@allow_non_gpu('DateFormatClass', 'Cast')
def test_date_format_for_date_runtime_fallback(data_gen, date_format):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)))
    
@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', [tz_rules_date_gen], ids=idfn)
@allow_non_gpu('ProjectExec')
def test_date_format_for_date_tz_rules(data_gen, date_format):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)))

@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
# use 9999-12-30 instead of 9999-12-31 to avoid the issue: https://github.com/NVIDIA/spark-rapids/issues/10083
@pytest.mark.parametrize('data_gen', [TimestampGen(end=datetime(9999, 12, 30, 23, 59, 59, 999999, tzinfo=timezone.utc))], ids=idfn)
def test_date_format_for_time_runtime_fallback(data_gen, date_format):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)))
    
@pytest.mark.parametrize('date_format', supported_date_formats, ids=idfn)
# use 9999-12-30 instead of 9999-12-31 to avoid the issue: https://github.com/NVIDIA/spark-rapids/issues/10083
@pytest.mark.parametrize('data_gen', [TimestampGen(end=datetime(2200, 12, 30, 23, 59, 59, 999999, tzinfo=timezone.utc))], ids=idfn)
def test_date_format_for_time_tz_rules(data_gen, date_format):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)))

@pytest.mark.parametrize('date_format', supported_date_formats + ['yyyyMMdd'], ids=idfn)
# from 0001-02-01 to 9999-12-30 to avoid 'year 0 is out of range'
@pytest.mark.parametrize('data_gen', [LongGen(min_val=int(datetime(1, 2, 1).timestamp()), max_val=int(datetime(2200, 12, 30).timestamp()))], ids=idfn)
def test_from_unixtime(data_gen, date_format):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, length=5).selectExpr("from_unixtime(a, '{}')".format(date_format)))

@pytest.mark.parametrize('date_format', supported_date_formats + ['yyyyMMdd'], ids=idfn)
# from 0001-02-01 to 9999-12-30 to avoid 'year 0 is out of range'
@pytest.mark.parametrize('data_gen', [LongGen(min_val=int(datetime(1, 2, 1).timestamp()), max_val=int(datetime(9999, 12, 30).timestamp()))], ids=idfn)
def test_from_unixtime_runtime_fallback(data_gen, date_format):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, length=5).selectExpr("from_unixtime(a, '{}')".format(date_format)))

unsupported_date_formats = ['F']
@pytest.mark.parametrize('date_format', unsupported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@allow_non_gpu('DateFormatClass', 'Cast')
def test_date_format_f(data_gen, date_format):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)),
        'DateFormatClass')

@pytest.mark.parametrize('date_format', unsupported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@allow_non_gpu('DateFormatClass', 'Cast')
def test_date_format_f_incompat(data_gen, date_format):
    # note that we can't support it even with incompatibleDateFormats enabled
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)),
        'DateFormatClass', conf)

maybe_supported_date_formats = ['dd-MM-yyyy', 'yyyy-MM-dd HH:mm:ss.SSS', 'yyyy-MM-dd HH:mm:ss.SSSSSS']
@pytest.mark.parametrize('date_format', maybe_supported_date_formats, ids=idfn)
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
@allow_non_gpu('DateFormatClass', 'Cast')
def test_date_format_maybe(data_gen, date_format):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)),
        'DateFormatClass')

@pytest.mark.parametrize('date_format', maybe_supported_date_formats, ids=idfn)

@pytest.mark.parametrize('data_gen', [date_gen,
                                      # use 9999-12-30 instead of 9999-12-31 to avoid the issue: https://github.com/NVIDIA/spark-rapids/issues/10083
                                      TimestampGen(end=datetime(9999, 12, 30, 23, 59, 59, 999999, tzinfo=timezone.utc))
                                      ], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_date_format_maybe_incompat_runtime_fallback(data_gen, date_format):
    # We will do a CPU fallback during runtime for timezones with transitions during 
    # years > 2200 as described in https://github.com/NVIDIA/spark-rapids/issues/6840
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)), conf)

@pytest.mark.parametrize('date_format', maybe_supported_date_formats, ids=idfn)

@pytest.mark.parametrize('data_gen', [date_gen,
                                      # use 9999-12-30 instead of 9999-12-31 to avoid the issue: https://github.com/NVIDIA/spark-rapids/issues/10083
                                      TimestampGen(end=datetime(2200, 12, 30, 23, 59, 59, 999999, tzinfo=timezone.utc))
                                      ], ids=idfn)
def test_date_format_maybe_incompat_tz_rules(data_gen, date_format):
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("date_format(a, '{}')".format(date_format)), conf)


@disable_ansi_mode  # ANSI mode tested separately.
# Reproduce conditions for https://github.com/NVIDIA/spark-rapids/issues/5670
# where we had a failure due to GpuCast canonicalization with timezone.
# In this case it was doing filter after project, the way I get that to happen is by adding in the
# input_file_name(), otherwise filter happens before project.
@allow_non_gpu("CollectLimitExec", "FileSourceScanExec" ,"DeserializeToObjectExec", *non_utc_allow)
@ignore_order()
def test_date_format_mmyyyy_cast_canonicalization(spark_tmp_path):
    data_path = spark_tmp_path + '/CSV_DATA'
    gen = StringGen(pattern='[0][0-9][1][8-9][1-9][1-9]', nullable=False)
    schema = gen.data_type
    with_cpu_session(lambda spark : gen_df(spark, gen, length=100).write.csv(data_path))
    def do_join_cast(spark):
        left = spark.read.csv(data_path)\
            .selectExpr("date_format(to_date(_c0, 'MMyyyy'), 'MM/dd/yyyy') as monthly_reporting_period", "substring_index(substring_index(input_file_name(),'/',-1),'.',1) as filename")
        right = spark.read.csv(data_path).withColumnRenamed("_c0", "r_c0")\
            .selectExpr("date_format(to_date(r_c0, 'MMyyyy'), 'MM/dd/yyyy') as monthly_reporting_period", "substring_index(substring_index(input_file_name(),'/',-1),'.',1) as filename")\
            .withColumnRenamed("monthly_reporting_period", "r_monthly_reporting_period")\
            .withColumnRenamed("filename", "r_filename")
        return left.join(right, left.monthly_reporting_period == right.r_monthly_reporting_period, how='inner')
    assert_gpu_and_cpu_are_equal_collect(do_join_cast)


@allow_non_gpu('DateFormatClass', 'Cast')
@pytest.mark.parametrize('data_gen', date_n_time_gens, ids=idfn)
def test_unsupported_fallback_date_format(data_gen):
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_fallback_collect(
        lambda spark : gen_df(spark, [("a", data_gen)]).selectExpr(
            "date_format(a, a)"),
        "DateFormatClass",
        conf)


@disable_ansi_mode  # Failure cases for ANSI mode are tested separately.
@allow_non_gpu('GetTimestamp')
def test_unsupported_fallback_to_date():
    date_gen = StringGen(pattern="2023-08-01")
    pattern_gen = StringGen(pattern="[M]")
    conf = {"spark.rapids.sql.incompatibleDateFormats.enabled": "true"}
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, [("a", date_gen), ("b", pattern_gen)]).selectExpr(
            "to_date(a, b)"),
        'GetTimestamp',
        conf)


# (-62135510400, 253402214400) is the range of seconds that can be represented by timestamp_seconds
# considering the influence of time zone.
ts_float_gen = SetValuesGen(FloatType(), [0.0, -0.0, 1.0, -1.0, 1.234567, -1.234567, 16777215.0, float('inf'), float('-inf'), float('nan')])
seconds_gens = [LongGen(min_val=-62135510400, max_val=253402214400), IntegerGen(), ShortGen(), ByteGen(),
                DoubleGen(min_exp=0, max_exp=32), ts_float_gen, DecimalGen(16, 6), DecimalGen(13, 3), DecimalGen(10, 0), DecimalGen(7, -3), DecimalGen(6, 6)]
@pytest.mark.parametrize('data_gen', seconds_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_seconds(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_seconds(a)"))

@allow_non_gpu(*non_utc_allow)
def test_timestamp_seconds_long_overflow():
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, long_gen).selectExpr("timestamp_seconds(a)").collect(),
        conf={},
        error_message='long overflow')

# For Decimal(20, 7) case, the data is both 'Overflow' and 'Rounding necessary', this case is to verify
# that 'Rounding necessary' check is before 'Overflow' check. So we should make sure that every decimal
# value in test data is 'Rounding necessary' by setting full_precision=True to avoid leading and trailing zeros.
# Otherwise, the test data will bypass the 'Rounding necessary' check and throw an 'Overflow' error.
@pytest.mark.parametrize('data_gen', [DecimalGen(7, 7, full_precision=True), DecimalGen(20, 7, full_precision=True)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_seconds_rounding_necessary(data_gen):
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_seconds(a)").collect(),
        conf={},
        error_message='Rounding necessary')

@pytest.mark.parametrize('data_gen', [DecimalGen(19, 6), DecimalGen(20, 6)], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_seconds_decimal_overflow(data_gen):
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_seconds(a)").collect(),
        conf={},
        error_message='Overflow')

millis_gens = [LongGen(min_val=-62135410400000, max_val=253402214400000), IntegerGen(), ShortGen(), ByteGen()]
@pytest.mark.parametrize('data_gen', millis_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_millis(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_millis(a)"))

@allow_non_gpu(*non_utc_allow)
def test_timestamp_millis_long_overflow():
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, long_gen).selectExpr("timestamp_millis(a)").collect(),
        conf={},
        error_message='long overflow')

micros_gens = [LongGen(min_val=-62135510400000000, max_val=253402214400000000), IntegerGen(), ShortGen(), ByteGen()]
@pytest.mark.parametrize('data_gen', micros_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_timestamp_micros(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr("timestamp_micros(a)"))


@pytest.mark.parametrize('parser_policy', ['LEGACY', 'CORRECTED', 'EXCEPTION'], ids=idfn)
def test_date_to_timestamp(parser_policy):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, date_gen),
        "tab",
        "SELECT cast(a as timestamp) from tab",
        conf = {
            "spark.sql.legacy.timeParserPolicy": parser_policy,
            "spark.rapids.sql.incompatibleDateFormats.enabled": True})

# Generate format strings, which are case insensitive and have some garbage rows.
trunc_date_format_gen = StringGen('(?i:YEAR|YYYY|YY|QUARTER|MONTH|MM|MON|WEEK)') \
    .with_special_pattern('invalid', weight=50)
trunc_timestamp_format_gen = StringGen('(?i:YEAR|YYYY|YY|QUARTER|MONTH|MM|MON|WEEK|DAY|DD|HOUR|MINUTE|SECOND|MILLISECOND|MICROSECOND)') \
    .with_special_pattern('invalid', weight=50)

@pytest.mark.parametrize('data_gen', [date_gen], ids=idfn)
@pytest.mark.parametrize('format_gen', [trunc_date_format_gen], ids=idfn)
def test_trunc_date_full_input(data_gen, format_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, data_gen, format_gen).selectExpr('trunc(a, b)'))

@allow_non_gpu(*non_utc_tz_allow)
@pytest.mark.parametrize('format_gen', [trunc_timestamp_format_gen], ids=idfn)
@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
def test_trunc_timestamp_full_input(format_gen, data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, format_gen, data_gen).selectExpr('date_trunc(a, b)'))

@pytest.mark.parametrize('format_gen', [trunc_date_format_gen], ids=idfn)
def test_trunc_date_single_value(format_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, format_gen).selectExpr('trunc("1980-05-18", a)'))

@allow_non_gpu(*non_utc_tz_allow)
@pytest.mark.parametrize('format_gen', [trunc_timestamp_format_gen], ids=idfn)
def test_trunc_timestamp_single_value(format_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, format_gen).selectExpr(
            'date_trunc(a, "1980-05-18T09:32:05.359")'))

@pytest.mark.parametrize('data_gen', [date_gen], ids=idfn)
def test_trunc_date_single_format(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'trunc(a, "YEAR")',
            'trunc(a, "YYYY")',
            'trunc(a, "YY")',
            'trunc(a, "QUARTER")',
            'trunc(a, "MONTH")',
            'trunc(a, "MM")',
            'trunc(a, "MON")',
            'trunc(a, "WEEK")',
            'trunc(a, "invalid")'))

@allow_non_gpu(*non_utc_tz_allow)
@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
def test_trunc_timestamp_single_format(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'date_trunc("YEAR", a)',
            'date_trunc("YYYY", a)',
            'date_trunc("YY", a)',
            'date_trunc("QUARTER", a)',
            'date_trunc("MONTH", a)',
            'date_trunc("MM", a)',
            'date_trunc("MON", a)',
            'date_trunc("WEEK", a)',
            'date_trunc("DAY", a)',
            'date_trunc("DD", a)',
            'date_trunc("HOUR", a)',
            'date_trunc("MINUTE", a)',
            'date_trunc("SECOND", a)',
            'date_trunc("MILLISECOND", a)',
            'date_trunc("MICROSECOND", a)',
            'date_trunc("invalid", a)'))
