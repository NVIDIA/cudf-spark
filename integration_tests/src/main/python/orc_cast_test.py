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

from asserts import (assert_cpu_and_gpu_are_equal_collect_with_capture,
                     assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error)
from conftest import get_test_tz, is_databricks_runtime, is_not_utc
from data_gen import *
from marks import (allow_non_gpu, datagen_overrides, ignore_order, tz_sensitive_test,
                   validate_execs_in_gpu_plan)
from pyspark.sql.types import *
from spark_session import with_cpu_session
from orc_test import reader_opt_confs
from parquet_test import _scan_output_batches


def create_orc(data_gen_list, data_path):
    # generate ORC dataframe, and dump it to local file 'data_path'
    with_cpu_session(
        lambda spark: gen_df(spark, data_gen_list).write.orc(data_path)
    )


@pytest.mark.parametrize('reader_confs', reader_opt_confs, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "orc"])
@pytest.mark.parametrize('to_type', ['boolean', 'tinyint', 'smallint', 'int', 'bigint'])
def test_casting_among_integer_types(spark_tmp_path, reader_confs, v1_enabled_list, to_type):
    # cast integral types to another integral types
    int_gens = [boolean_gen] + integral_gens
    gen_list = [('c' + str(i), gen) for i, gen in enumerate(int_gens)]
    data_path = spark_tmp_path + '/ORC_DATA'
    create_orc(gen_list, data_path)

    # generate schema string like "c0 to_type, c1 to_type, ..., c4 to_type"
    schema_str = " {}, ".join([x[0] for x in gen_list]) + " {}"
    schema_str = schema_str.format(*([to_type] * len(gen_list)))
    all_confs = copy_and_update(reader_confs,
                                {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(data_path),
        conf=all_confs)

non_utc_allow_orc_scan=['ColumnarToRowExec', 'FileScan', 'FileSourceScanExec'] if is_not_utc() else []

@pytest.mark.parametrize('to_type', ['float', 'double', 'string', 'timestamp'])
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_casting_from_integer(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_cast_integer'
    # The Python 'datetime' module only supports a max-year of 10000, so we set the Long type max
    # to '1e11'. If the long-value is out of this range, pytest will throw an exception.
    data_gen = [('boolean_col', boolean_gen), ('tinyint_col', byte_gen),
                ('smallint_col', ShortGen(min_val=BYTE_MAX + 1)),
                ('int_col', IntegerGen(min_val=SHORT_MAX + 1)),
                ('bigint_col', LongGen(min_val=INT_MAX + 1, max_val=int(1e11))),
                ('negint_col', IntegerGen(max_val=-1))]
    create_orc(data_gen, orc_path)

    schema_str = "boolean_col {}, tinyint_col {}, smallint_col {}, int_col {}, bigint_col {}, negint_col {}"
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(
            schema_str.format(*([to_type] * len(data_gen)))).orc(orc_path)
    )

non_utc_allow_for_test_casting_from_overflow_long = ['FileSourceScanExec', 'ColumnarToRowExec', ] if is_not_utc() else []
@pytest.mark.parametrize('overflow_long_gen', [LongGen(min_val=int(1e16)),
                                               LongGen(max_val=int(-1e16))])
@pytest.mark.parametrize('to_type', ['timestamp'])
@allow_non_gpu(*non_utc_allow_for_test_casting_from_overflow_long)
def test_casting_from_overflow_long(spark_tmp_path, overflow_long_gen,to_type):
    # Timestamp(micro-seconds) is actually type of int64, when casting long(int64) to timestamp,
    # we need to multiply 1e6 (or 1e3), and it may cause overflow. This function aims to test
    # whether if 'ArithmeticException' is caught.
    orc_path = spark_tmp_path + '/orc_cast_overflow_long'
    create_orc([('long_column', overflow_long_gen)], orc_path)
    schema_str = "long_column {}".format(to_type)
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.read.schema(schema_str).orc(orc_path).collect(),
        conf={},
        error_message="ArithmeticException"
    )


# When casting float/double to double/float, we need to compare values of GPU with CPU
# in an approximate way.
@pytest.mark.approximate_float
@pytest.mark.parametrize('to_type', ['float', 'double', 'boolean', 'tinyint', 'smallint', 'int', 'bigint'])
def test_casting_from_float_and_double(spark_tmp_path, to_type):
    orc_path = spark_tmp_path + '/orc_casting_from_float_and_double'
    data_gen = [('float_column', float_gen), ('double_column', double_gen)]
    create_orc(data_gen, orc_path)
    schema_str = "float_column {}, double_column {}".format(to_type, to_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema_str).orc(orc_path)
    )

@pytest.mark.parametrize('data_gen', [DoubleGen(max_exp=32, special_cases=None),
                                      DoubleGen(max_exp=32, special_cases=[8.88e9, 9.99e10, 1.314e11])])
@allow_non_gpu(*non_utc_allow_orc_scan)
def test_casting_from_double_to_timestamp(spark_tmp_path, data_gen):
    # ORC will assume the original double value in seconds, we need to convert them to
    # timestamp(INT64 in micro-seconds).
    #
    # The 'datetime' module in python requires 0 <= year < 10000, and UTC timestamp is start from 1970/1/1.
    # That is, the minimum valid negative number is -1970 * 365 * 24 * 3600 = -62125920000 -> 6e10 -> 2^32.
    # So we set max_exp = 32 in DoubleGen.
    #
    # The maximum valid positive number is (10000 - 1970) * 365 * 24 * 3600 = 253234080000 -> 2e11 -> 2^37,
    # so we add some special cases from 2^33 - 2^37 (8e9 ~ 1e11).
    #
    # In DoubleGen, special_case=None will generate some NaN, INF corner cases.

    orc_path = spark_tmp_path + '/orc_casting_from_double_to_timestamp'
    with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).write.orc(orc_path)
    )
    # the name of unique column is 'a', cast it into timestamp type
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a timestamp").orc(orc_path)
    )


@tz_sensitive_test
@pytest.mark.skipif(not is_not_utc(), reason="non-UTC ORC timestamp regression test")
@allow_non_gpu(*non_utc_allow_orc_scan)
@validate_execs_in_gpu_plan('GpuFileSourceScanExec')
def test_non_utc_timestamp_regressions(spark_tmp_path):
    integer_path = spark_tmp_path + '/orc_integer_timestamp_regression'
    double_path = spark_tmp_path + '/orc_double_timestamp_regression'
    physical_path = spark_tmp_path + '/orc_physical_timestamp_regression'

    with_cpu_session(
        lambda spark: spark.createDataFrame([(514952012,)], "a long")
            .write.orc(integer_path)
    )
    with_cpu_session(
        lambda spark: spark.createDataFrame(
            [(0.0,),
             (float('nan'),),
             (float('inf'),),
             (float('-inf'),),
             (-8589934591.999999,),
             (-7953731124.723491,),
             (-0.0015,),
             (-0.0005,),
             (0.0005,),
             (0.0015,)],
            "a double")
            .write.orc(double_path)
    )
    with_cpu_session(
        lambda spark: spark.createDataFrame(
            [(-2957649381472612,), (-3649379812521628,)], "a long")
            .selectExpr("timestamp_micros(a) AS a")
            .write.orc(physical_path)
    )

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a timestamp").orc(integer_path)
    )
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a timestamp").orc(double_path)
    )
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.orc(physical_path)
    )


# Regression for ORC BIGINT-to-TIMESTAMP schema evolution at a historical overlap. The input is
# interpreted as local wall-clock time 1883-11-18 12:02:00. America/New_York moved backward from
# LMT (-04:56:02) to EST (-05:00)
# at 17:00:00Z that day, so local times from 12:00:00 through 12:03:57 are ambiguous.
#
# CPU ORC's convertFromUtc subtracts the raw -05:00 offset before looking up the applicable offset.
# It therefore probes 17:02:00Z, selects EST, and Spark preserves the original Calendar offset by
# choosing withLaterOffsetAtOverlap(), producing -2717650680000000 microseconds. The GPU historical
# path must preserve that ORC-selected instant through Spark's historical rebase.
@tz_sensitive_test
@pytest.mark.skipif(
    get_test_tz() != 'America/New_York',
    reason='requires the 1883 America/New_York LMT-to-EST overlap')
@allow_non_gpu(*non_utc_allow_orc_scan)
@validate_execs_in_gpu_plan('GpuFileSourceScanExec')
def test_casting_from_bigint_to_timestamp_in_new_york_lmt_overlap(spark_tmp_path):
    orc_path = spark_tmp_path + '/orc_bigint_timestamp_lmt_overlap'
    with_cpu_session(
        lambda spark: spark.createDataFrame([(-2717668680,)], 'a long')
            .write.orc(orc_path)
    )

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema('a timestamp').orc(orc_path)
    )


@allow_non_gpu(*non_utc_allow_for_test_casting_from_overflow_long)
# Keep both signs beyond the Long microsecond range after any timezone correction.
@pytest.mark.parametrize("overflow_value", [9223372123255.0, -9223372123255.0])
def test_casting_from_overflow_double_to_timestamp(spark_tmp_path, overflow_value):
    orc_path = spark_tmp_path + '/orc_casting_from_overflow_double_to_timestamp'
    with_cpu_session(
        lambda spark: spark.createDataFrame(
            [(overflow_value,)], "a double")
            .write.orc(orc_path)
    )
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.read.schema("a timestamp").orc(orc_path).collect(),
        conf={},
        error_message="ArithmeticException"
    )


@pytest.mark.parametrize('data_gen', [DecimalGen(precision=9, scale=2),
                                      DecimalGen(precision=18, scale=4),
                                      DecimalGen(precision=38, scale=6)])
@pytest.mark.parametrize('read_type', ["DECIMAL(9,2)",
                                       "DECIMAL(18,4)",
                                       "DECIMAL(38,6)"])
@pytest.mark.parametrize('ansi_mode', ["false", "true"])
def test_casting_decimal_to_decimal(spark_tmp_path, data_gen, read_type, ansi_mode):
    """
    Tests that ORC files with decimal columns written with one set of
    precision and scale are readable with different precision/scale.
    """
    orc_path = spark_tmp_path + '/orc_casting_from_decimal_to_decimal'
    with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).write.orc(orc_path)
    )
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema("a " + read_type).orc(orc_path)
            .selectExpr("*", "a is null as is_null"),
        conf={'spark.sql.ansi.enabled': ansi_mode}
    )


@ignore_order(local=True)
# Pair reader and API settings to cover V1/V2 without multiplying every boundary case.
@pytest.mark.parametrize('reader, v1_enabled_list', [
    ('PERFILE', 'orc'), ('MULTITHREADED', ''), ('COALESCING', 'orc'), ('COALESCING', '')])
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
@pytest.mark.parametrize('file_precision, file_scale, read_precision, read_scale', [
    (9, 2, 7, 2), (18, 2, 10, 2), (34, 2, 20, 2), (34, 2, 18, 2),
    # More precision can still leave fewer digits for the whole-number part.
    (9, 2, 10, 4),
    # Rounding can carry into a whole-number digit outside the requested precision.
    (9, 3, 8, 2)])
def test_orc_decimal_precision_narrowing(spark_tmp_path, reader, v1_enabled_list,
        ansi_enabled, file_precision, file_scale, read_precision, read_scale):
    data_path = spark_tmp_path + '/ORC_DECIMAL_NARROWING'
    decimal_type = DecimalType(file_precision, file_scale)
    # Include both signs of the exact read bound and the first value beyond it.
    max_value = '9' * (read_precision - read_scale) + '.' + '9' * min(file_scale, read_scale)
    overflow = '1' + '0' * (read_precision - read_scale) + '.00'
    inputs = [max_value, '-' + max_value, '0.00', '0.01']
    if file_precision - file_scale > read_precision - read_scale:
        inputs += [overflow, '-' + overflow]
    if file_scale > read_scale:
        inputs += [max_value + '4', max_value + '5', '-' + max_value + '4', '-' + max_value + '5']
    values = [Decimal(v) for v in inputs] + [None]
    with_cpu_session(lambda spark: gen_df(spark,
        [('id', RepeatSeqGen(list(range(len(values))), data_type=IntegerType())),
         ('d', RepeatSeqGen(values, data_type=decimal_type))], length=len(values))
        .selectExpr('id', 'd', 'named_struct("d", d) as s', 'array(d) as a', 'map(1, d) as m',
            f'cast(null as decimal({file_precision},{file_scale})) as n',
            f'map(cast(id + 1 as decimal({file_precision},{file_scale})), id) as k')
        .repartition(2).write.orc(data_path))

    target = f'decimal({read_precision},{read_scale})'
    schema = (f'id int, d {target}, s struct<d:{target}>, a array<{target}>, '
        f'm map<int,{target}>, n {target}, k map<{target},int>')
    conf = {
        'spark.rapids.sql.format.orc.reader.type': reader,
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        # Use CPU row-reader overflow semantics for both V1 and V2. This does not
        # change the GPU reader or select the data-source API.
        'spark.sql.orc.enableVectorizedReader': 'false',
        'spark.sql.ansi.enabled': ansi_enabled}

    # Decimal128 row conversion can hide overflow by turning it into null on the host.
    # Evaluate null checks on the GPU too, including every nested decimal leaf.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).orc(data_path).selectExpr('*',
            'd is null as d_is_null', 's.d is null as struct_is_null',
            'a[0] is null as array_is_null', 'm[1] is null as map_is_null',
            'n is null as all_null', 'map_keys(k)[0] + 1 as key_added', 'd + 1 as added'),
        conf=conf)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).orc(data_path)
            .filter(f'd > {max_value} OR d < -{max_value}').select('id'), conf=conf)


@ignore_order(local=True)
@pytest.mark.parametrize('v1_enabled_list', ['', 'orc'])
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_orc_decimal_narrowing_map_key_overflow(spark_tmp_path,
        v1_enabled_list, ansi_enabled):
    data_path = spark_tmp_path + '/ORC_DECIMAL_MAP_KEYS'
    values = [(Decimal(value),) for value in
        ['99999999.99', '-99999999.99', '100000000.00', '-100000000.00']]
    with_cpu_session(lambda spark: spark.createDataFrame(values, 'd decimal(18,2)')
        .selectExpr('map(d, 1) as k').coalesce(1).write.orc(data_path))
    conf = {
        'spark.rapids.sql.format.orc.reader.type': 'COALESCING',
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        # ORC's CPU row reader inserts evolved keys into a TreeMap, which rejects null.
        # Its vectorized reader exposes overflowing keys as null for comparison with GPU.
        'spark.sql.orc.enableVectorizedReader': 'true',
        'spark.sql.orc.enableNestedColumnVectorizedReader': 'true',
        'spark.sql.ansi.enabled': ansi_enabled}
    # Inspect the key on the GPU, before collecting an invalid null-key map to the host.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema('k map<decimal(10,2),int>').orc(data_path)
            .selectExpr('map_keys(k)[0] as key', 'map_keys(k)[0] is null as key_is_null'),
        conf=conf)


@ignore_order(local=True)
@pytest.mark.parametrize('v1_enabled_list', ['', 'orc'])
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
@pytest.mark.parametrize('file_precisions', [(10, 10), (10, 18)], ids=['same-schema', 'mixed-schema'])
def test_orc_decimal_narrowing_mixed_file_precisions(spark_tmp_path,
        v1_enabled_list, ansi_enabled, file_precisions):
    paths = [f'{spark_tmp_path}/ORC_DECIMAL_MIXED_{index}' for index in range(2)]
    for index, (precision, data_path) in enumerate(zip(file_precisions, paths)):
        inputs = ['99999999.99', '-99999999.99']
        if precision == 18:
            inputs += ['100000000.00', '-100000000.00']
        values = [(index * 10 + row, Decimal(value) if value is not None else None)
            for row, value in enumerate(inputs + [None])]
        with_cpu_session(lambda spark: spark.createDataFrame(values,
            f'id int, d decimal({precision},2)').coalesce(1).write.orc(data_path))

    conf = {
        'spark.rapids.sql.format.orc.reader.type': 'COALESCING',
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.orc.enableVectorizedReader': 'false',
        'spark.sql.ansi.enabled': ansi_enabled,
        'spark.sql.adaptive.enabled': 'false',
        'spark.sql.files.minPartitionNum': '1',
        'spark.sql.files.openCostInBytes': '0',
        'spark.rapids.sql.metrics.level': 'DEBUG'}

    def read(spark):
        df = spark.read.schema('id int, d decimal(10,2)').orc(paths)
        assert len(df.inputFiles()) == 2
        return df.selectExpr('id', 'd', 'd is null as is_null', 'd + 1 as added')

    def check_batches(cpu_plan, gpu_plan):
        # DBR can keep files in separate partitions despite these file settings.
        # Keep CPU/GPU result checks there; assert schema-dependent batching on Apache Spark.
        if not is_databricks_runtime():
            # Rule out separate Spark partitions as the reason for separate output batches.
            assert gpu_plan.execute().getNumPartitions() == 1
            # Both precisions decode to DECIMAL64, but their TypeDescriptions differ.
            # Split the mixed case so each cast receives its own tableSchema precision.
            expected_batches = 1 if file_precisions[0] == file_precisions[1] else 2
            assert _scan_output_batches(gpu_plan) == expected_batches

    assert_cpu_and_gpu_are_equal_collect_with_capture(read, conf=conf,
        gpu_plan_assertion=check_batches)
