# Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

from asserts import *
from data_gen import *
from conftest import is_not_utc
from datetime import timezone
from conftest import is_databricks_runtime
from marks import approximate_float, allow_non_gpu, ignore_order, datagen_overrides
from spark_session import *

# mark this test as ci_1 for mvn verify sanity check in pre-merge CI
pytestmark = [pytest.mark.premerge_ci_1]

TEXT_INPUT_EXEC='FileSourceScanExec'

# allow non gpu when time zone is non-UTC because of https://github.com/NVIDIA/spark-rapids/issues/9653'
non_utc_file_source_scan_allow = ['FileSourceScanExec'] if is_not_utc() else []

non_utc_project_allow = ['StructsToJson', 'JsonToStructs'] if is_not_utc() else []

# Spark 4.0+ lowers some to_json fallbacks through ProjectExec + evaluator invocation
# instead of exposing StructsToJson directly as the non-GPU plan node. Databricks
# keeps the Project on GPU and bridges only the StructsToJson expression to CPU.
structs_to_json_uses_project_fallback = is_spark_400_or_later() and not is_databricks_runtime()
structs_to_json_fallback_allow = ['StructsToJson'] + \
    (['ProjectExec'] if structs_to_json_uses_project_fallback else [])
structs_to_json_fallback_class = \
    'ProjectExec' if structs_to_json_uses_project_fallback else 'StructsToJson'


json_supported_gens = [
    # Spark does not escape '\r' or '\n' even though it uses it to mark end of record
    # This would require multiLine reads to work correctly, so we avoid these chars
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    StringGen('[nN][aA][nN]'),
    StringGen('[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen,
    pytest.param(double_gen),
    pytest.param(FloatGen(no_nans=False)),
    pytest.param(float_gen),
    DoubleGen(no_nans=False)
]

optional_whitespace_regex = '[ \t\xA0\u1680\u180e\u2000-\u200a\u202f\u205f\u3000]?'

_enable_all_types_conf = {
    'spark.rapids.sql.expression.JsonToStructs': 'true',
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true',
    'spark.rapids.sql.json.read.float.enabled': 'true',
    'spark.rapids.sql.json.read.double.enabled': 'true',
    'spark.rapids.sql.json.read.decimal.enabled': 'true'
}

_bool_schema = StructType([
    StructField('number', BooleanType())])

_byte_schema = StructType([
    StructField('number', ByteType())])

_short_schema = StructType([
    StructField('number', ShortType())])

_int_schema = StructType([
    StructField('number', IntegerType())])

_long_schema = StructType([
    StructField('number', LongType())])

_float_schema = StructType([
    StructField('number', FloatType())])

_double_schema = StructType([
    StructField('number', DoubleType())])

_decimal_10_2_schema = StructType([
    StructField('number', DecimalType(10, 2))])

_decimal_10_3_schema = StructType([
    StructField('number', DecimalType(10, 3))])

_date_schema = StructType([
    StructField('number', DateType())])

_timestamp_schema = StructType([
    StructField('number', TimestampType())])

_string_schema = StructType([
    StructField('a', StringType())])

json_supported_date_formats = [
    None, # represents not specifying a format (which is different from explicitly specifying the default format in some Spark versions)
    'yyyy-MM-dd', 'yyyy/MM/dd',
    'yyyy-MM', 'yyyy/MM',
    'MM-yyyy', 'MM/yyyy',
    'MM-dd-yyyy', 'MM/dd/yyyy',
    'dd-MM-yyyy', 'dd/MM/yyyy']

json_supported_ts_parts = [
    "'T'HH:mm:ss.SSSXXX",
    "'T'HH:mm:ss[.SSS][XXX]",
    "'T'HH:mm:ss.SSS",
    "'T'HH:mm:ss[.SSS]",
    "'T'HH:mm:ss",
    "'T'HH:mm[:ss]",
    "'T'HH:mm"]

json_supported_timestamp_formats = [
    None, # represents not specifying a format (which is different from explicitly specifying the default format in some Spark versions)
]
for date_part in json_supported_date_formats:
    if date_part:
        # use date format without time component
        json_supported_timestamp_formats.append(date_part)
        # use date format and each supported time format
        for ts_part in json_supported_ts_parts:
            json_supported_timestamp_formats.append(date_part + ts_part)


def read_json_df(data_path, schema, spark_tmp_table_factory_ignored, options = {}):
    def read_impl(spark):
        reader = spark.read
        if not schema is None:
            reader = reader.schema(schema)
        for key, value in options.items():
            reader = reader.option(key, value)
        return debug_df(reader.json(data_path))
    return read_impl

def read_json_sql(data_path, schema, spark_tmp_table_factory, options = {}):
    opts = options
    if not schema is None:
        opts = copy_and_update(options, {'schema': schema})
    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        return spark.catalog.createTable(tmp_name, source='json', path=data_path, **opts)
    return read_impl

@approximate_float
@pytest.mark.parametrize('data_gen', [
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen,], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
@allow_non_gpu('FileSourceScanExec')
def test_json_infer_schema_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.json(data_path),
            conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('data_gen', json_supported_gens, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(schema).json(data_path),
            conf=updated_conf)

@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_input_meta(spark_tmp_path, v1_enabled_list):
    gen = StructGen([('a', long_gen), ('b', long_gen), ('c', long_gen)], nullable=False)
    first_data_path = spark_tmp_path + '/JSON_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(first_data_path))
    second_data_path = spark_tmp_path + '/JSON_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(second_data_path))
    data_path = spark_tmp_path + '/JSON_DATA'
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(gen.data_type)
                    .json(data_path)
                    .filter(f.col('b') > 0)
                    .selectExpr('b',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=updated_conf)

allow_non_gpu_for_json_scan = ['FileSourceScanExec', 'BatchScanExec'] if is_not_utc() else []
@pytest.mark.parametrize('date_format', [None, 'yyyy-MM-dd'] if is_before_spark_320 else json_supported_date_formats, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
@allow_non_gpu(*allow_non_gpu_for_json_scan)
def test_json_date_formats_round_trip(spark_tmp_path, date_format, v1_enabled_list):
    gen = StructGen([('a', DateGen())], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf,
        {'spark.sql.sources.useV1SourceList': v1_enabled_list,
         'spark.rapids.sql.json.read.datetime.enabled': 'true'})

    def create_test_data(spark):
        write = gen_df(spark, gen).write
        if date_format:
            write = write.option('dateFormat', date_format)
        return write.json(data_path)

    with_cpu_session(lambda spark : create_test_data(spark))

    def do_read(spark):
        read = spark.read.schema(schema)
        if date_format:
            read = read.option('dateFormat', date_format)
        return read.json(data_path)

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: do_read(spark),
            conf=updated_conf)


not_utc_allow_for_test_json_scan = ['BatchScanExec', 'FileSourceScanExec'] if is_not_utc() else []
@allow_non_gpu(*not_utc_allow_for_test_json_scan)
@pytest.mark.parametrize('timestamp_format', json_supported_timestamp_formats)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_ts_formats_round_trip(spark_tmp_path, timestamp_format, v1_enabled_list):
    data_gen = TimestampGen()
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type

    def create_test_data(spark):
        write = gen_df(spark, gen).write
        if timestamp_format:
            write = write.option('timestampFormat', timestamp_format)
        write.json(data_path)

    with_cpu_session(lambda spark: create_test_data(spark))
    updated_conf = copy_and_update(_enable_all_types_conf,
        {'spark.sql.sources.useV1SourceList': v1_enabled_list,
         'spark.rapids.sql.json.read.datetime.enabled': 'true'})

    def do_read(spark):
        read = spark.read.schema(schema)
        if timestamp_format:
            read = read.option('timestampFormat', timestamp_format)
        return read.json(data_path)

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: do_read(spark),
            conf=updated_conf)

@allow_non_gpu('FileSourceScanExec', 'ProjectExec')
@pytest.mark.skipif(is_before_spark_341(), reason='`TIMESTAMP_NTZ` is only supported in PySpark 341+')
@pytest.mark.parametrize('timestamp_format', json_supported_timestamp_formats)
@pytest.mark.parametrize("timestamp_type", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
def test_json_ts_formats_round_trip_ntz_v1(spark_tmp_path, timestamp_format, timestamp_type):
    json_ts_formats_round_trip_ntz(spark_tmp_path, timestamp_format, timestamp_type, 'json', 'FileSourceScanExec')

@allow_non_gpu('BatchScanExec', 'ProjectExec')
@pytest.mark.skipif(is_before_spark_341(), reason='`TIMESTAMP_NTZ` is only supported in PySpark 341+')
@pytest.mark.parametrize('timestamp_format', json_supported_timestamp_formats)
@pytest.mark.parametrize("timestamp_type", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
def test_json_ts_formats_round_trip_ntz_v2(spark_tmp_path, timestamp_format, timestamp_type):
    json_ts_formats_round_trip_ntz(spark_tmp_path, timestamp_format, timestamp_type, '', 'BatchScanExec')

def json_ts_formats_round_trip_ntz(spark_tmp_path, timestamp_format, timestamp_type, v1_enabled_list, cpu_scan_class):
    data_gen = TimestampGen(tzinfo=None if timestamp_type == "TIMESTAMP_NTZ" else timezone.utc)
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type

    def create_test_data(spark):
        write = gen_df(spark, gen).write
        if timestamp_format:
            write = write.option('timestampFormat', timestamp_format)
        write.json(data_path)

    with_cpu_session(lambda spark: create_test_data(spark))
    updated_conf = copy_and_update(_enable_all_types_conf,
        {
            'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.timestampType': timestamp_type,
            'spark.rapids.sql.json.read.datetime.enabled': 'true'
        })

    def do_read(spark):
        read = spark.read.schema(schema)
        if timestamp_format:
            read = read.option('timestampFormat', timestamp_format)
        return read.json(data_path)


    if timestamp_type == "TIMESTAMP_LTZ":
        if is_not_utc():
            # non UTC is not support for json, skip capture check
            # Tracked in https://github.com/NVIDIA/spark-rapids/issues/9912
            assert_gpu_and_cpu_are_equal_collect(lambda spark: do_read(spark), conf = updated_conf)
        else:
            assert_cpu_and_gpu_are_equal_collect_with_capture(
                lambda spark : do_read(spark),
                exist_classes = 'Gpu' + cpu_scan_class,
                non_exist_classes = cpu_scan_class,
                conf=updated_conf)


    else:
        # we fall back to CPU due to "unsupported data types in output: TimestampNTZType"
        assert_gpu_fallback_collect(
            lambda spark : do_read(spark),
            cpu_fallback_class_name = cpu_scan_class,
            conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('filename', [
    'boolean.json',
    'boolean_invalid.json',
    'ints.json',
    pytest.param('ints_invalid.json', marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/4940')), # This fails for dates, as not all are invalid
    'nan_and_inf.json',
    pytest.param('nan_and_inf_strings.json', marks=pytest.mark.skipif(is_before_spark_330(), reason='https://issues.apache.org/jira/browse/SPARK-38060 fixed in Spark 3.3.0')),
    'nan_and_inf_invalid.json',
    'floats.json',
    'floats_leading_zeros.json',
    'floats_invalid.json',
    'floats_edge_cases.json',
    'decimals.json',
    'dates.json',
    'dates_invalid.json',
])
@pytest.mark.parametrize('schema', [_bool_schema, _byte_schema, _short_schema, _int_schema, _long_schema, \
                                    _float_schema, _double_schema, _decimal_10_2_schema, _decimal_10_3_schema, \
                                    _date_schema], ids=idfn)
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('allow_non_numeric_numbers', ['true', 'false'])
@pytest.mark.parametrize('allow_numeric_leading_zeros', [
    'true',
    'false'
])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@allow_non_gpu(*not_utc_allow_for_test_json_scan)
@pytest.mark.parametrize('date_format', [None, 'yyyy-MM-dd'])
def test_basic_json_read(std_input_path, filename, schema, read_func, allow_non_numeric_numbers, \
        allow_numeric_leading_zeros, ansi_enabled, spark_tmp_table_factory, date_format):
    updated_conf = copy_and_update(_enable_all_types_conf,
        {'spark.sql.ansi.enabled': ansi_enabled,
         'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
         'spark.rapids.sql.json.read.datetime.enabled': 'true'})
    options = {"allowNonNumericNumbers": allow_non_numeric_numbers,
           "allowNumericLeadingZeros": allow_numeric_leading_zeros,
           }

    if date_format:
        options['dateFormat'] = date_format

    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
                  schema,
                  spark_tmp_table_factory,
                  options),
        conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('filename', [
    'boolean.json',
    'boolean_invalid.json',
    'ints.json',
    pytest.param('ints_invalid.json', marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/4940')), # This fails for dates, as not all are invalid
    'nan_and_inf.json',
    pytest.param('nan_and_inf_strings.json', marks=pytest.mark.skipif(is_before_spark_330(), reason='https://issues.apache.org/jira/browse/SPARK-38060 fixed in Spark 3.3.0')),
    'nan_and_inf_invalid.json',
    'floats.json',
    'floats_leading_zeros.json',
    'floats_invalid.json',
    'floats_edge_cases.json',
    'decimals.json',
    'dates.json',
    'dates_invalid.json',
])
@pytest.mark.parametrize('schema', [_bool_schema, _byte_schema, _short_schema, _int_schema, _long_schema, \
                                    _float_schema, _double_schema, _decimal_10_2_schema, _decimal_10_3_schema, \
                                    _date_schema], ids=idfn)
@pytest.mark.parametrize('allow_non_numeric_numbers', ['true', 'false'])
@pytest.mark.parametrize('allow_numeric_leading_zeros', [
    'true',
    'false'
])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@allow_non_gpu(TEXT_INPUT_EXEC, *non_utc_project_allow)
@pytest.mark.parametrize('date_format', [None, 'yyyy-MM-dd'])
def test_basic_from_json(std_input_path, filename, schema, allow_non_numeric_numbers, \
        allow_numeric_leading_zeros, ansi_enabled, date_format):
    updated_conf = copy_and_update(_enable_all_types_conf,
        {'spark.sql.ansi.enabled': ansi_enabled,
         'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
         'spark.rapids.sql.json.read.datetime.enabled': 'true'})
    options = {"allowNonNumericNumbers": allow_non_numeric_numbers,
           "allowNumericLeadingZeros": allow_numeric_leading_zeros,
           }

    if date_format:
        options['dateFormat'] = date_format

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.text(std_input_path + '/' + filename).
          selectExpr("value as json").
          select(f.col("json"), f.from_json(f.col("json"), schema, options)),
        conf=updated_conf)


@ignore_order
@pytest.mark.parametrize('filename', [
    'malformed1.ndjson',
    'malformed2.ndjson',
    'malformed3.ndjson',
    'malformed4.ndjson'
])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('schema', [_int_schema])
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_read_invalid_json(spark_tmp_table_factory, std_input_path, read_func, filename, schema, v1_enabled_list):
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
                  schema,
                  spark_tmp_table_factory,
                  {}),
        conf=conf)

@pytest.mark.parametrize('filename', [
    'mixed-primitives.ndjson',
    'mixed-primitives-nested.ndjson',
    'simple-nested.ndjson',
    'mixed-nested.ndjson',
    'mixed-types-in-struct.ndjson',
    'mixed-primitive-arrays.ndjson',
])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('schema', [_int_schema], ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_read_valid_json(spark_tmp_table_factory, std_input_path, read_func, filename, schema, v1_enabled_list):
    conf = copy_and_update(_enable_all_types_conf,
        {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
                  schema,
                  spark_tmp_table_factory,
                  {}),
        conf=conf)

@pytest.mark.parametrize('filename', ['nested-structs.ndjson'])
@pytest.mark.parametrize('schema', [
    StructType([StructField('teacher', StringType())]),
    StructType([
        StructField('student', StructType([
            StructField('name', StringType()),
            StructField('age', IntegerType())
        ]))
    ]),
    StructType([
        StructField('teacher', StringType()),
        StructField('student', StructType([
            StructField('name', StringType()),
            StructField('age', IntegerType())
        ]))
    ]),
])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_read_nested_struct(spark_tmp_table_factory, std_input_path, read_func, filename, schema, v1_enabled_list):
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
                  schema,
                  spark_tmp_table_factory,
                  {}),
        conf=conf)

@pytest.mark.parametrize('filename', ['optional-fields.ndjson'])
@pytest.mark.parametrize('schema', [
    StructType([StructField('teacher', StringType())]),
    StructType([StructField('student', StringType())]),
    StructType([
        StructField('teacher', StringType()),
        StructField('student', StringType())
    ]),
])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_read_optional_fields(spark_tmp_table_factory, std_input_path, read_func, filename, schema, v1_enabled_list):
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
                  schema,
                  spark_tmp_table_factory,
                  {}),
        conf=conf)

# allow non gpu when time zone is non-UTC because of https://github.com/NVIDIA/spark-rapids/issues/9653'
not_utc_json_scan_allow=['FileSourceScanExec'] if is_not_utc() else []

@approximate_float
@pytest.mark.parametrize('filename', [
    'dates.json',
])
@pytest.mark.parametrize('schema', [_date_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('FileSourceScanExec')),
    pytest.param('CORRECTED', marks=pytest.mark.allow_non_gpu(*not_utc_json_scan_allow)),
    pytest.param('EXCEPTION', marks=pytest.mark.allow_non_gpu(*not_utc_json_scan_allow))
])
def test_json_read_valid_dates(std_input_path, filename, schema, read_func, ansi_enabled, time_parser_policy, spark_tmp_table_factory):
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy,
                                    'spark.rapids.sql.json.read.datetime.enabled': 'true'})
    f = read_func(std_input_path + '/' + filename, schema, spark_tmp_table_factory, {})
    if time_parser_policy == 'LEGACY' and ansi_enabled == 'true':
        assert_gpu_fallback_collect(
            f,
            'FileSourceScanExec',
            conf=updated_conf)
    else:
        assert_gpu_and_cpu_are_equal_collect(f, conf=updated_conf)

@pytest.mark.parametrize('date_gen_pattern', [
    '[0-9]{1,4}-[0-3]{1,2}-[0-3]{1,2}',
    '[0-9]{1,2}-[0-3]{1,2}-[0-9]{1,4}',
    '[1-9]{4}-[1-3]{2}-[1-3]{2}',
    '[1-9]{4}-[1-3]{1,2}-[1-3]{1,2}',
    '[1-3]{1,2}-[1-3]{1,2}-[1-9]{4}',
    '[1-3]{1,2}/[1-3]{1,2}/[1-9]{4}',
])
@pytest.mark.parametrize('schema', [StructType([StructField('value', DateType())])])
@pytest.mark.parametrize('date_format', [None, 'yyyy-MM-dd'] if is_before_spark_320 else json_supported_date_formats)
@pytest.mark.parametrize('ansi_enabled', [True, False])
@pytest.mark.parametrize('allow_numeric_leading_zeros', [True, False])
@allow_non_gpu(*allow_non_gpu_for_json_scan)
def test_json_read_generated_dates(spark_tmp_table_factory, spark_tmp_path, date_gen_pattern, schema, date_format, \
        ansi_enabled, allow_numeric_leading_zeros):
    # create test data with json strings where a subset are valid dates
    # example format: {"value":"3481-1-31"}
    path = spark_tmp_path + '/JSON_DATA'

    data_gen = StringGen(optional_whitespace_regex + date_gen_pattern + optional_whitespace_regex, nullable=False)

    with_cpu_session(lambda spark: gen_df(spark, data_gen).write.json(path))

    updated_conf = copy_and_update(_enable_all_types_conf, {
        'spark.sql.ansi.enabled': ansi_enabled,
        'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
        'spark.rapids.sql.json.read.datetime.enabled': 'true'})

    options = { 'allowNumericLeadingZeros': allow_numeric_leading_zeros }
    if date_format:
        options['dateFormat'] = date_format

    f = read_json_df(path, schema, spark_tmp_table_factory, options)
    assert_gpu_and_cpu_are_equal_collect(f, conf = updated_conf)

@approximate_float
@pytest.mark.parametrize('filename', [
    'dates_invalid.json',
])
@pytest.mark.parametrize('schema', [_date_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('date_format', [None, 'yyyy-MM-dd'] if is_before_spark_320 else json_supported_date_formats)
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('FileSourceScanExec')),
    pytest.param('CORRECTED', marks=pytest.mark.allow_non_gpu(*not_utc_json_scan_allow)),
    pytest.param('EXCEPTION', marks=pytest.mark.allow_non_gpu(*not_utc_json_scan_allow))
])
def test_json_read_invalid_dates(std_input_path, filename, schema, read_func, ansi_enabled, date_format, \
        time_parser_policy, spark_tmp_table_factory):
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy,
                                    'spark.rapids.sql.json.read.datetime.enabled': 'true'})
    options = { 'dateFormat': date_format } if date_format else {}
    f = read_func(std_input_path + '/' + filename, schema, spark_tmp_table_factory, options)
    if time_parser_policy == 'EXCEPTION':
        assert_gpu_and_cpu_error(
            df_fun=lambda spark: f(spark).collect(),
            conf=updated_conf,
            error_message='Exception')
    elif time_parser_policy == 'LEGACY' and ansi_enabled == 'true':
        assert_gpu_fallback_collect(
            f,
            'FileSourceScanExec',
            conf=updated_conf)
    else:
        assert_gpu_and_cpu_are_equal_collect(f, conf=updated_conf)

@approximate_float
@pytest.mark.parametrize('filename', [
    'timestamps.json',
])
@pytest.mark.parametrize('schema', [_timestamp_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('FileSourceScanExec')),
    # For non UTC cases, corrected and exception will have CPU fallback in lack of timezone support.
    pytest.param('CORRECTED', marks=pytest.mark.allow_non_gpu(*non_utc_file_source_scan_allow)),
    pytest.param('EXCEPTION', marks=pytest.mark.allow_non_gpu(*non_utc_file_source_scan_allow))
])
def test_json_read_valid_timestamps(std_input_path, filename, schema, read_func, ansi_enabled, time_parser_policy, \
        spark_tmp_table_factory):
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy,
                                    'spark.rapids.sql.json.read.datetime.enabled': 'true'})
    f = read_func(std_input_path + '/' + filename, schema, spark_tmp_table_factory, {})
    assert_gpu_and_cpu_are_equal_collect(f, conf=updated_conf)

@pytest.mark.parametrize('schema', [_string_schema])
@pytest.mark.parametrize('read_func', [read_json_df, read_json_sql])
@pytest.mark.parametrize('allow_unquoted_chars', ["true"])
@pytest.mark.parametrize('filename', ['unquotedChars.json'])
def test_json_unquotedCharacters(std_input_path, filename, schema, read_func, allow_unquoted_chars, spark_tmp_table_factory):
    assert_gpu_and_cpu_are_equal_collect(
        read_func(std_input_path + '/' + filename,
        schema,
        spark_tmp_table_factory,
        {"allowUnquotedControlChars": allow_unquoted_chars}),
        conf=_enable_all_types_conf)

@ignore_order
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
@pytest.mark.skipif(is_databricks_runtime(), reason="Databricks does not support ignoreCorruptFiles")
def test_json_read_with_corrupt_files(spark_tmp_path, v1_enabled_list):
    first_data_path = spark_tmp_path + '/JSON_DATA/first'
    with_cpu_session(lambda spark : spark.range(1).toDF("a").write.json(first_data_path))
    second_data_path = spark_tmp_path + '/JSON_DATA/second'
    with_cpu_session(lambda spark : spark.range(1, 2).toDF("a").write.orc(second_data_path))
    third_data_path = spark_tmp_path + '/JSON_DATA/third'
    with_cpu_session(lambda spark : spark.range(2, 3).toDF("a").write.json(third_data_path))

    all_confs = copy_and_update(_enable_all_types_conf,
                                {'spark.sql.files.ignoreCorruptFiles': "true",
                                 'spark.sql.sources.useV1SourceList': v1_enabled_list})
    schema = StructType([StructField("a", IntegerType())])

    # when ignoreCorruptFiles is enabled, gpu reading should not throw exception, while CPU can successfully
    # read the three files without ignore corrupt files. So we just check if GPU will throw exception.
    with_gpu_session(
            lambda spark : spark.read.schema(schema)
                .json([first_data_path, second_data_path, third_data_path])
                .collect(),
            conf=all_confs)

@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
def test_json_read_count(spark_tmp_path, v1_enabled_list):
    gen_list = [byte_gen, short_gen, int_gen, long_gen, boolean_gen]
    gen = StructGen([('_c' + str(i), gen) for i, gen in enumerate(gen_list)], nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.json(data_path))
    assert_gpu_and_cpu_row_counts_equal(
            lambda spark : spark.read.schema(schema).json(data_path),
            conf=updated_conf)

@allow_non_gpu(*non_utc_allow)
def test_from_json_map():
    # The test here is working around some inconsistencies in how the keys are parsed for maps
    # on the GPU the keys are dense, but on the CPU they are sparse
    json_string_gen = StringGen(r'{"a": "[0-9]{0,5}"(, "b": "[A-Z]{0,5}")?}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json(f.col('a'), 'MAP<STRING,STRING>')),
        conf=_enable_all_types_conf)

@allow_non_gpu(*non_utc_allow)
def test_from_json_map_with_invalid():
    # The test here is working around some inconsistencies in how the keys are parsed for maps
    # on the GPU the keys are dense, but on the CPU they are sparse
    json_string_gen = StringGen(r'{"a": "[0-9]{0,5}"(, "b": "[A-Z]{0,5}")?}') \
        .with_special_pattern('', weight=50) \
        .with_special_pattern('  ', weight=50) \
        .with_special_pattern('null', weight=50) \
        .with_special_pattern('invalid', weight=50) \
        .with_special_pattern(r'{"a": "[0-9]{0,5}"', weight=50) \
        .with_special_pattern(r'{"a": "[0-9]{0,5}', weight=50) \
        .with_special_pattern(r'{"a": "[0-9]{0,5}"}abc', weight=50) \
        .with_special_pattern(r'{"a": "[0-9]{0,5}"}{"b": "B"}', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json(f.col('a'), 'MAP<STRING,STRING>')),
        conf=_enable_all_types_conf)

@allow_non_gpu(*non_utc_allow)
@pytest.mark.parametrize('allow_single_quotes', ['true', 'false'])
@pytest.mark.parametrize('allow_non_numeric_numbers', ['true', 'false'])
@pytest.mark.parametrize('allow_unquoted_chars', ['true', 'false'])
def test_from_json_map_with_options(allow_single_quotes,  
                                    allow_non_numeric_numbers,
                                    allow_unquoted_chars):
    # Test the input with:
    #  - Double quotes
    #  - Single quotes
    #  - Numbers with leading zeros
    #  - Non-numeric numbers
    #  - Unquoted control characters in quoted strings
    json_string_gen = StringGen(r'{"a": "[0-9]{0,5}"}') \
        .with_special_pattern(r"""{'a': "[0-9]{0,5}"}""", weight=50) \
        .with_special_pattern(r'{"a": 0[0-9]{0,5}}', weight=50) \
        .with_special_pattern(r'{"a": [+-]?(INF|Infinity|NaN)}', weight=50) \
        .with_special_pattern(r'{"(a|a\r\n\tb)": "(xyz|01\r\n\t23)"}', weight=50)
    options = {"allowSingleQuotes": allow_single_quotes,
                # Cannot test `allowNumericLeadingZeros==true` because the GPU output always has
                # leading zeros while the CPU output does not, thus test will always fail.
               "allowNumericLeadingZeros": "false",
               "allowNonNumericNumbers": allow_non_numeric_numbers,
               "allowUnquotedControlChars": allow_unquoted_chars}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen, length=20) \
            .select(f.from_json(f.col('a'), 'MAP<STRING,STRING>', options)),
        conf=_enable_all_types_conf)

@allow_non_gpu('ProjectExec', 'JsonToStructs')
def test_from_json_map_fallback():
    # The test here is working around some inconsistencies in how the keys are parsed for maps
    # on the GPU the keys are dense, but on the CPU they are sparse
    json_string_gen = StringGen(r'{"a": \d\d}')
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json(f.col('a'), 'MAP<STRING,INT>')),
        'JsonToStructs',
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('schema', [
    'struct<a:string>',
    'struct<b:string>',
    'struct<c:string>',
    'struct<a:int>',
    'struct<a:long>',
    'struct<a:float>',
    'struct<a:double>',
    'struct<a:decimal>',
    'struct<d:string>',
    'struct<a:string,b:string>',
    'struct<c:int,a:string>',
    ])
@allow_non_gpu(*non_utc_allow)
def test_from_json_struct(schema):
    json_string_gen = StringGen(r'{\'a\': [0-9]{0,5}, "b": \'[A-Z]{0,5}\', "c": 1\d\d\d}') \
        .with_special_pattern('', weight=50) \
        .with_special_pattern('null', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', schema)),
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('schema', [
    'struct<a:string,a:string>',
    ])
@allow_non_gpu("JsonToStructs")
def test_from_json_struct_fallback_dupe_keys(schema):
    json_string_gen = StringGen(r'{\'a\': [0-9]{0,5}, "b": \'[A-Z]{0,5}\', "c": 1\d\d\d}') \
        .with_special_pattern('', weight=50) \
        .with_special_pattern('null', weight=50)
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', schema)),
        'JsonToStructs',
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('pattern', [
    r'{ "bool": (true|false|True|False|TRUE|FALSE) }',
    r'{ "bool": "(true|false)" }',
    r'{ "bool": "(True|False|TRUE|FALSE)" }',
    r'{ "bool": [0-9]{0,2}(\.[0-9]{1,2})? }',
    r'{ "bool": "[0-9]{0,2}(\.[0-9]{1,2})?" }',
    r'{ "bool": [0-9]{4}-[0-9]{2}-[0-9]{2} }',
    r'{ "bool": "[0-9]{4}-[0-9]{2}-[0-9]{2}" }'
])
@allow_non_gpu(*non_utc_allow)
def test_from_json_struct_boolean(pattern):
    json_string_gen = StringGen(pattern) \
        .with_special_case('', weight=50) \
        .with_special_case('null', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', 'struct<bool:boolean>')),
        conf=_enable_all_types_conf)

@allow_non_gpu(*non_utc_allow)
@datagen_overrides(seed=0, reason='https://github.com/NVIDIA/spark-rapids/issues/10898')
def test_from_json_struct_decimal():
    json_string_gen = StringGen(r'{ "a": "[+-]?([0-9]{0,5})?(\.[0-9]{0,2})?([eE][+-]?[0-9]{1,2})?" }') \
        .with_special_pattern('', weight=50) \
        .with_special_pattern('null', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', 'struct<a:decimal>')),
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('date_gen', [
    # "yyyy-MM-dd"
    "\"" + optional_whitespace_regex + "[1-8]{1}[0-9]{3}-[0-3]{1,2}-[0-3]{1,2}" + optional_whitespace_regex + "\"",
    # "yyyy-MM"
    "\"" + optional_whitespace_regex + "[1-8]{1}[0-9]{3}-[0-3]{1,2}" + optional_whitespace_regex + "\"",
    # "yyyy"
    "\"" + optional_whitespace_regex + "[0-9]{4}" + optional_whitespace_regex + "\"",
    # "dd/MM/yyyy"
    "\"" + optional_whitespace_regex + "[0-9]{2}/[0-9]{2}/[1-8]{1}[0-9]{3}" + optional_whitespace_regex + "\"",
    # special constant values
    "\"" + optional_whitespace_regex + "(now|today|tomorrow|epoch)" + optional_whitespace_regex + "\"",
    # "nnnnn" (number of days since epoch prior to Spark 3.4, throws exception from 3.4)
    pytest.param("\"[0-9]{5}\"", marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/9664")),
    # integral
    pytest.param("[0-9]{1,5}", marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/9664")),
    # floating-point
    "[0-9]{0,2}\\.[0-9]{1,2}"
    # boolean
    "(true|false)"
])
@pytest.mark.parametrize('date_format', [None, 'yyyy-MM-dd'] if is_before_spark_320 else json_supported_date_formats)
@allow_non_gpu(*non_utc_project_allow)
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10535')
def test_from_json_struct_date(date_gen, date_format):
    json_string_gen = StringGen(r'{ "a": ' + date_gen + ' }') \
        .with_special_case('{ "a": null }') \
        .with_special_case('null')
    options = { 'dateFormat': date_format } if date_format else { }
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.legacy.timeParserPolicy': 'CORRECTED'})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', 'struct<a:date>', options)),
        conf=conf)

@allow_non_gpu('JsonToStructs')
@pytest.mark.parametrize('date_gen', ["\"[1-8]{1}[0-9]{3}-[0-3]{1,2}-[0-3]{1,2}\""])
@pytest.mark.parametrize('date_format', [
    None,
    "yyyy-MM-dd",
])
def test_from_json_struct_date_fallback_legacy(date_gen, date_format):
    json_string_gen = StringGen(r'{ "a": ' + date_gen + ' }') \
        .with_special_case('{ "a": null }') \
        .with_special_case('null')
    options = { 'dateFormat': date_format } if date_format else { }
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.legacy.timeParserPolicy': 'LEGACY'})
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', 'struct<a:date>', options)),
        'JsonToStructs',
        conf=conf)

@pytest.mark.skipif(is_spark_320_or_later(), reason="We only fallback for non-default formats prior to 320")
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('date_gen', ["\"[1-8]{1}[0-9]{3}-[0-3]{1,2}-[0-3]{1,2}\""])
@pytest.mark.parametrize('date_format', [
    "dd/MM/yyyy",
    "yyyy/MM/dd",
])
def test_from_json_struct_date_fallback_non_default_format(date_gen, date_format):
    json_string_gen = StringGen(r'{ "a": ' + date_gen + ' }') \
        .with_special_case('{ "a": null }') \
        .with_special_case('null')
    options = { 'dateFormat': date_format }
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.legacy.timeParserPolicy': 'CORRECTED'})
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', 'struct<a:date>', options)),
        'ProjectExec',
        conf=conf)

@pytest.mark.parametrize('timestamp_gen', [
    # "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"
    "\"" + optional_whitespace_regex + "[1-8]{1}[0-9]{3}-[0-3]{1,2}-[0-3]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\\.[0-9]{1,6})?Z?" + optional_whitespace_regex + "\"",
    # "yyyy-MM-dd"
    "\"" + optional_whitespace_regex + "[1-8]{1}[0-9]{3}-[0-3]{1,2}-[0-3]{1,2}" + optional_whitespace_regex + "\"",
    # "yyyy-MM"
    "\"" + optional_whitespace_regex + "[1-8]{1}[0-9]{3}-[0-3]{1,2}" + optional_whitespace_regex + "\"",
    # "yyyy"
    "\"" + optional_whitespace_regex + yyyy_start_0001 + optional_whitespace_regex + "\"",
    # "dd/MM/yyyy"
    "\"" + optional_whitespace_regex + "[0-9]{2}/[0-9]{2}/[1-8]{1}[0-9]{3}" + optional_whitespace_regex + "\"",
    # special constant values
    pytest.param("\"" + optional_whitespace_regex + "(now|today|tomorrow|epoch)" + optional_whitespace_regex + "\"", marks=pytest.mark.xfail(condition=is_before_spark_320(), reason="https://github.com/NVIDIA/spark-rapids/issues/9724")),
    # "nnnnn" (number of days since epoch prior to Spark 3.4, throws exception from 3.4)
    pytest.param("\"" + optional_whitespace_regex + "[0-9]{5}" + optional_whitespace_regex + "\"", marks=pytest.mark.skip(reason="https://github.com/NVIDIA/spark-rapids/issues/9664")),
    # integral
    pytest.param("[0-9]{1,5}", marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/4940")),
    pytest.param("[1-9]{1,8}", marks=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/4940")),
    # floating-point
    r"[0-9]{0,2}\.[0-9]{1,2}"
    # boolean
    "(true|false)"
])
@pytest.mark.parametrize('timestamp_format', [
    # Even valid timestamp format, CPU fallback happens still since non UTC is not supported for json.
    pytest.param(None, marks=pytest.mark.allow_non_gpu(*non_utc_project_allow)),
    # https://github.com/NVIDIA/spark-rapids/issues/9723
    pytest.param("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]", marks=pytest.mark.allow_non_gpu('ProjectExec')),
    pytest.param("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", marks=pytest.mark.allow_non_gpu('ProjectExec')),
    pytest.param("dd/MM/yyyy'T'HH:mm:ss[.SSS][XXX]", marks=pytest.mark.allow_non_gpu('ProjectExec')),
])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param("LEGACY", marks=pytest.mark.allow_non_gpu('JsonToStructs')),
    pytest.param("CORRECTED", marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10535'))
])
@pytest.mark.parametrize('ansi_enabled', [ True, False ])
def test_from_json_struct_timestamp(timestamp_gen, timestamp_format, time_parser_policy, ansi_enabled):
    json_string_gen = StringGen(r'{ "a": ' + timestamp_gen + ' }') \
        .with_special_case('{ "a": null }') \
        .with_special_case('{ "a": "6395-12-21T56:86:40.205705Z" }') \
        .with_special_case('null')
    options = { 'timestampFormat': timestamp_format } if timestamp_format else { }
    conf = copy_and_update(_enable_all_types_conf, {
        'spark.sql.legacy.timeParserPolicy': time_parser_policy, 
        'spark.sql.ansi.enabled': ansi_enabled})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', 'struct<a:timestamp>', options)),
        conf=conf)

@allow_non_gpu('JsonToStructs')
@pytest.mark.parametrize('timestamp_gen', ["\"[1-8]{1}[0-9]{3}-[0-3]{1,2}-[0-3]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\\.[0-9]{1,6})?Z?\""])
@pytest.mark.parametrize('timestamp_format', [
    None,
    "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]",
])
def test_from_json_struct_timestamp_fallback_legacy(timestamp_gen, timestamp_format):
    json_string_gen = StringGen(r'{ "a": ' + timestamp_gen + ' }') \
        .with_special_case('{ "a": null }') \
        .with_special_case('null')
    options = { 'timestampFormat': timestamp_format } if timestamp_format else { }
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.legacy.timeParserPolicy': 'LEGACY'})
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', 'struct<a:timestamp>', options)),
        'JsonToStructs',
        conf=conf)

@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('timestamp_gen', ["\"[1-8]{1}[0-9]{3}-[0-3]{1,2}-[0-3]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\\.[0-9]{1,6})?Z?\""])
@pytest.mark.parametrize('timestamp_format', [
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "dd/MM/yyyy'T'HH:mm:ss[.SSS][XXX]",
])
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10535')
def test_from_json_struct_timestamp_fallback_non_default_format(timestamp_gen, timestamp_format):
    json_string_gen = StringGen(r'{ "a": ' + timestamp_gen + ' }') \
        .with_special_case('{ "a": null }') \
        .with_special_case('null')
    options = { 'timestampFormat': timestamp_format } if timestamp_format else { }
    conf = copy_and_update(_enable_all_types_conf, {'spark.sql.legacy.timeParserPolicy': 'CORRECTED'})
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.col('a'), f.from_json('a', 'struct<a:timestamp>', options)),
        'ProjectExec',
        conf=conf)

@pytest.mark.parametrize('schema', [
    'struct<teacher:string>',
    'struct<student:struct<name:string,age:int>>',
    'struct<teacher:string,student:struct<name:string,age:int>>'
])
@allow_non_gpu(*non_utc_allow)
def test_from_json_struct_of_struct(schema):
    json_string_gen = StringGen(r'{"teacher": "[A-Z]{1}[a-z]{2,5}",' \
                                r'"student": {"name": "[A-Z]{1}[a-z]{2,5}", "age": 1\d}}') \
        .with_special_pattern('', weight=50) \
        .with_special_pattern('null', weight=50) \
        .with_special_pattern('invalid_entry', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', schema)),
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('schema', ['struct<teacher:string>',
                                    'struct<student:array<struct<name:string,class:string>>>',
                                    'struct<teacher:string,student:array<struct<name:string,class:string>>>'])
@allow_non_gpu(*non_utc_allow)
def test_from_json_struct_of_list(schema):
    json_string_gen = StringGen(r'{"teacher": "[A-Z]{1}[a-z]{2,5}",' \
                                r'"student": \[{"name": "[A-Z]{1}[a-z]{2,5}", "class": "junior"},' \
                                r'{"name": "[A-Z]{1}[a-z]{2,5}", "class": "freshman"}\]}') \
        .with_special_pattern('', weight=50) \
        .with_special_pattern('null', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', schema)),
        conf=_enable_all_types_conf)

@allow_non_gpu(*non_utc_allow)
def test_from_json_struct_of_list_with_mismatched_schema():
    json_string_gen = StringGen(r'{"teacher": "[A-Z]{1}[a-z]{2,5}",' \
                                r'"student": \["[A-Z]{1}[a-z]{2,5}"\]}') \
        .with_special_pattern('', weight=50) \
        .with_special_pattern('null', weight=50)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', 'struct<teacher:string,student:array<struct<name:string,class:string>>>')),
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('schema', ['struct<teacher:string>',
                                    'struct<student:array<struct<name:string,class:string>>>',
                                    'struct<teacher:string,student:array<struct<name:string,class:string>>>'])
@allow_non_gpu(*non_utc_allow)
def test_from_json_struct_of_list_with_mixed_nested_types_input(schema):
    json_string_gen = StringGen(r'{"teacher": "[A-Z]{1}[a-z]{2,5}",' \
                                r'"student": \[{"name": "[A-Z]{1}[a-z]{2,5}", "class": "junior"},' \
                                r'{"name": "[A-Z]{1}[a-z]{2,5}", "class": "freshman"}\]}') \
        .with_special_pattern('', weight=50) \
        .with_special_pattern('null', weight=50) \
        .with_special_pattern('{"student": \["[A-Z]{1}[a-z]{2,5}"\]}', weight=100) \
        .with_special_pattern('{"student": \[[1-9]{1,5}\]}', weight=100) \
        .with_special_pattern('{"student": {"[A-Z]{1}[a-z]{2,5}": "[A-Z]{1}[a-z]{2,5}"}}', weight=100)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', schema)),
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('schema', [
    'struct<a:string>'
])
@allow_non_gpu(*non_utc_allow)
def test_from_json_mixed_types_list_struct(schema):
    json_string_gen = StringGen(r'{"a": (\[1,2,3\]|{"b":"[a-z]{2}"}) }')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select('a', f.from_json('a', schema)),
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('schema', ['struct<a:string>', 'struct<a:string,b:int>'])
@allow_non_gpu(*non_utc_allow)
def test_from_json_struct_all_empty_string_input(schema):
    json_string_gen = StringGen('')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', schema)),
        conf=_enable_all_types_conf)

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.skipif(is_before_spark_340(), reason='enableDateTimeParsingFallback is supported from Spark3.4.0')
@pytest.mark.parametrize('filename,schema', [("dates.json", _date_schema),("dates.json", _timestamp_schema),
                                             ("timestamps.json", _timestamp_schema)])
def test_json_datetime_parsing_fallback_cpu_fallback(std_input_path, filename, schema):
    data_path = std_input_path + "/" + filename
    assert_gpu_fallback_collect(
        lambda spark : spark.read.schema(schema).option('enableDateTimeParsingFallback', "true").json(data_path),
        'FileSourceScanExec',
        conf=_enable_all_types_conf)

@pytest.mark.skipif(is_before_spark_340(), reason='enableDateTimeParsingFallback is supported from Spark3.4.0')
@pytest.mark.parametrize('filename,schema', [("ints.json", _int_schema)])
def test_json_datetime_parsing_fallback_no_datetime(std_input_path, filename, schema):
    data_path = std_input_path + "/" + filename
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.schema(schema).option('enableDateTimeParsingFallback', "true").json(data_path),
        conf=_enable_all_types_conf)

@pytest.mark.skip(reason=str("https://github.com/NVIDIA/spark-rapids/issues/8403"))
@pytest.mark.parametrize('v1_enabled_list', ["", "json"])
@pytest.mark.parametrize('col_name', ['K0', 'k0', 'K3', 'k3', 'V0', 'v0'], ids=idfn)
@ignore_order
def test_read_case_col_name(spark_tmp_path, v1_enabled_list, col_name):
    all_confs = {'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.rapids.sql.format.json.read.enabled': True,
            'spark.rapids.sql.format.json.enabled': True}
    gen_list =[('k0', LongGen(nullable=False, min_val=0, max_val=0)), 
            ('k1', LongGen(nullable=False, min_val=1, max_val=1)),
            ('k2', LongGen(nullable=False, min_val=2, max_val=2)),
            ('k3', LongGen(nullable=False, min_val=3, max_val=3)),
            ('v0', LongGen()),
            ('v1', LongGen()),
            ('v2', LongGen()),
            ('v3', LongGen())]
 
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/JSON_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.partitionBy('k0', 'k1', 'k2', 'k3').json(data_path))

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(gen.data_type).json(data_path).selectExpr(col_name),
            conf=all_confs)


_to_json_datagens=[byte_gen,
    boolean_gen,
    short_gen,
    int_gen,
    long_gen,
    decimal_gen_32bit,
    decimal_gen_64bit,
    decimal_gen_128bit,
    pytest.param(float_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9350')),
    pytest.param(double_gen, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/9350')),
    date_gen,
    timestamp_gen,
    StringGen('[A-Za-z0-9\r\n\'"\\\\]{0,10}', nullable=True) \
        .with_special_case('\u1f600') \
        .with_special_case('"a"') \
        .with_special_case('\\"a\\"') \
        .with_special_case('\'a\'') \
        .with_special_case('\\\'a\\\''),
    pytest.param(StringGen('\u001a', nullable=True), marks=pytest.mark.xfail(
        reason='https://github.com/NVIDIA/spark-rapids/issues/9705'))
]

# Spark 400 changed the default timestamp format to "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXXXX]"
# We need to explicitly specify the format for Spark 400
_gpu_supported_timestamp_format_conf = {'timestampFormat': "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"}

@pytest.mark.parametrize('data_gen', [int_gen, date_gen], ids=idfn)
def test_structs_to_json_without_timestamp_non_utc_timezone(spark_tmp_path, data_gen):
    struct_gen = StructGen([
        ('a', data_gen),
        ("b", ArrayGen(data_gen, nullable=True)),
        ("c", MapGen(StringGen('[A-Z]{1,10}', nullable=False), data_gen, nullable=True)),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'timeZone': 'PRC' }

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.select(f.to_json("my_struct", options).alias("ms"))

    conf = copy_and_update(_enable_all_types_conf,
        { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : struct_to_json(spark),
        conf=conf)

@pytest.mark.parametrize('data_gen', _to_json_datagens, ids=idfn)
@pytest.mark.parametrize('ignore_null_fields', [True, False])
@pytest.mark.parametrize('timezone', [
    'UTC',
    'Etc/UTC'
])
@allow_non_gpu(*non_utc_project_allow)
def test_structs_to_json(data_gen, ignore_null_fields, timezone):
    struct_gen = StructGen([
        ('a', data_gen),
        ("b", StructGen([('child', data_gen)], nullable=True)),
        ("c", ArrayGen(StructGen([('child', data_gen)], nullable=True))),
        ("d", MapGen(StringGen('[A-Za-z0-9]{0,10}', nullable=False), data_gen)),
        ("e", ArrayGen(MapGen(StringGen('[A-Z]{5}', nullable=False), data_gen), nullable=True)),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'ignoreNullFields': ignore_null_fields,
                'timeZone': timezone}
    options.update(_gpu_supported_timestamp_format_conf)

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.select(
                f.to_json("my_struct", options).alias("ms"))

    conf = copy_and_update(_enable_all_types_conf,
        { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : struct_to_json(spark),
        conf=conf)

@pytest.mark.parametrize('data_gen', _to_json_datagens, ids=idfn)
@pytest.mark.parametrize('ignore_null_fields', [True, False])
@pytest.mark.parametrize('timezone', [
    'UTC',
    'Etc/UTC'
])
@allow_non_gpu(*non_utc_project_allow)
def test_arrays_to_json(data_gen, ignore_null_fields, timezone):
    array_gen = ArrayGen(data_gen, nullable=True)
    gen = StructGen([("my_array", array_gen)], nullable=False)

    options = { 'ignoreNullFields': ignore_null_fields,
                'timeZone': timezone}
    options.update(_gpu_supported_timestamp_format_conf)

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.select(
                f.to_json("my_array", options).alias("ma"))

    conf = copy_and_update(_enable_all_types_conf,
        { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : struct_to_json(spark),
        conf=conf)

@pytest.mark.parametrize('data_gen', _to_json_datagens, ids=idfn)
@pytest.mark.parametrize('ignore_null_fields', [True, False])
@pytest.mark.parametrize('timezone', [
    'UTC',
    'Etc/UTC'
])
@allow_non_gpu(*non_utc_project_allow)
def test_maps_to_json(data_gen, ignore_null_fields, timezone):
    map_gen = MapGen(StringGen('[A-Z]{1,10}', nullable=False), data_gen, nullable=True)
    gen = StructGen([("my_map", map_gen)], nullable=False)

    options = { 'ignoreNullFields': ignore_null_fields,
                'timeZone': timezone}
    options.update(_gpu_supported_timestamp_format_conf)

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.select(
                f.to_json("my_map", options).alias("mm"))

    conf = copy_and_update(_enable_all_types_conf,
        { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : struct_to_json(spark),
        conf=conf)

@pytest.mark.parametrize('data_type', [FloatType(), DoubleType()], ids=idfn)
def test_to_json_non_finite_floating_point(data_type):
    schema = StructType([StructField('value', data_type)])
    data = [(float('nan'),), (float('inf'),), (float('-inf'),), (1.5,), (None,)]

    def struct_to_json(spark):
        df = spark.createDataFrame(data, schema)
        return df.select(
            f.to_json(f.struct(f.col('value'))).alias('struct_json'),
            f.to_json(f.array(f.col('value'))).alias('array_json'),
            f.to_json(f.create_map(f.lit('value'), f.col('value'))).alias('map_json'))

    conf = copy_and_update(_enable_all_types_conf,
        { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : struct_to_json(spark),
        conf=conf)

@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
@pytest.mark.parametrize('timestamp_format', [
    'yyyy-MM-dd\'T\'HH:mm:ss[.SSS][XXX]'
])
@pytest.mark.parametrize('timezone', [
    'UTC',
    'Etc/UTC'
])
@allow_non_gpu(*non_utc_project_allow)
def test_structs_to_json_timestamp(data_gen, timestamp_format, timezone):
    struct_gen = StructGen([
        ("b", StructGen([('child', data_gen)], nullable=True)),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'timestampFormat': timestamp_format,
                'timeZone': timezone}

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.withColumn("my_json", f.to_json("my_struct", options))

    conf = copy_and_update(_enable_all_types_conf,
                           { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : struct_to_json(spark),
        conf=conf)

@allow_non_gpu(*structs_to_json_fallback_allow)
@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
@pytest.mark.parametrize('timezone', ['UTC+07:00'])
def test_structs_to_json_fallback_timezone(data_gen, timezone):
    struct_gen = StructGen([
        ('a', data_gen),
        ("b", StructGen([('child', data_gen)], nullable=True)),
        ("c", ArrayGen(StructGen([('child', data_gen)], nullable=True))),
        ("d", MapGen(LongGen(nullable=False), data_gen)),
        ("d", MapGen(StringGen('[A-Za-z0-9]{0,10}', nullable=False), data_gen)),
        ("e", ArrayGen(MapGen(LongGen(nullable=False), data_gen), nullable=True)),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'timeZone': timezone }

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.withColumn("my_json", f.to_json("my_struct", options)).drop("my_struct")

    conf = copy_and_update(_enable_all_types_conf,
                           { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_fallback_collect(
        lambda spark : struct_to_json(spark),
        structs_to_json_fallback_class,
        conf=conf)

@allow_non_gpu(*structs_to_json_fallback_allow)
@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
@pytest.mark.parametrize('timezone', ['UTC+07:00'])
def test_structs_to_json_fallback_explicit_timezone(data_gen, timezone):
    struct_gen = StructGen([
        ('a', data_gen),
        ("b", StructGen([('child', data_gen)], nullable=True)),
        ("c", ArrayGen(StructGen([('child', data_gen)], nullable=True))),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'timeZone': timezone }
    options.update(_gpu_supported_timestamp_format_conf)

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.withColumn("my_json", f.to_json("my_struct", options)).drop("my_struct")

    conf = copy_and_update(_enable_all_types_conf,
                           { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_fallback_collect(
        lambda spark : struct_to_json(spark),
        structs_to_json_fallback_class,
        conf=conf)

@allow_non_gpu(*structs_to_json_fallback_allow)
@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
def test_structs_to_json_fallback_analyzed_non_utc_timezone(data_gen):
    struct_gen = StructGen([
        ('a', data_gen),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    def struct_to_json(spark):
        spark.conf.set('spark.sql.session.timeZone', 'PRC')
        df = gen_df(spark, gen)
        ret = df.withColumn(
            "my_json", f.to_json("my_struct", _gpu_supported_timestamp_format_conf)
        ).drop("my_struct")
        ret._jdf.queryExecution().analyzed()
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        return ret

    conf = copy_and_update(_enable_all_types_conf,
                           { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_fallback_collect(
        lambda spark : struct_to_json(spark),
        structs_to_json_fallback_class,
        conf=conf)

@allow_non_gpu(*structs_to_json_fallback_allow)
@pytest.mark.parametrize('data_gen', [date_gen, timestamp_gen], ids=idfn)
def test_structs_to_json_fallback_legacy(data_gen):
    struct_gen = StructGen([
        ("a", StructGen([('child', data_gen)], nullable=True)),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.withColumn("my_json", f.to_json("my_struct")).drop("my_struct")

    conf = copy_and_update(_enable_all_types_conf,
        { 'spark.rapids.sql.expression.StructsToJson': True,
          'spark.sql.legacy.timeParserPolicy': 'LEGACY'})

    assert_gpu_fallback_collect(
        lambda spark : struct_to_json(spark),
        structs_to_json_fallback_class,
        conf=conf)

@allow_non_gpu(*structs_to_json_fallback_allow)
@pytest.mark.parametrize('data_gen', [date_gen], ids=idfn)
@pytest.mark.parametrize('timezone', ['UTC'])
@pytest.mark.parametrize('date_format', [
    'yyyy-dd-MM',
    'dd/MM/yyyy',
])
def test_structs_to_json_fallback_date_formats(data_gen, timezone, date_format):
    struct_gen = StructGen([
        ('a', data_gen),
        ("b", StructGen([('child', data_gen)], nullable=True)),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'timeZone': timezone,
                'dateFormat': date_format }

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.withColumn("my_json", f.to_json("my_struct", options)).drop("my_struct")

    conf = copy_and_update(_enable_all_types_conf,
                           { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_fallback_collect(
        lambda spark : struct_to_json(spark),
        structs_to_json_fallback_class,
        conf=conf)

@allow_non_gpu(*structs_to_json_fallback_allow)
@pytest.mark.parametrize('data_gen', [timestamp_gen], ids=idfn)
@pytest.mark.parametrize('timezone', ['UTC'])
@pytest.mark.parametrize('timestamp_format', [
    'yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX',
    'dd/MM/yyyy\'T\'HH:mm:ss[.SSS][XXX]',
])
def test_structs_to_json_fallback_timestamp_formats(data_gen, timezone, timestamp_format):
    struct_gen = StructGen([
        ('a', data_gen),
        ("b", StructGen([('child', data_gen)], nullable=True)),
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'timeZone': timezone,
                'timestampFormat': timestamp_format }

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.withColumn("my_json", f.to_json("my_struct", options)).drop("my_struct")

    conf = copy_and_update(_enable_all_types_conf,
                           { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_fallback_collect(
        lambda spark : struct_to_json(spark),
        structs_to_json_fallback_class,
        conf=conf)


@allow_non_gpu(*structs_to_json_fallback_allow)
def test_structs_to_json_fallback_pretty():
    struct_gen = StructGen([
        ('a', long_gen),
        ("b", byte_gen),
        ("c", ArrayGen(short_gen))
    ], nullable=False)
    gen = StructGen([('my_struct', struct_gen)], nullable=False)

    options = { 'pretty': True }

    def struct_to_json(spark):
        df = gen_df(spark, gen)
        return df.withColumn("my_json", f.to_json("my_struct", options)).drop("my_struct")

    conf = copy_and_update(_enable_all_types_conf,
                           { 'spark.rapids.sql.expression.StructsToJson': True })

    assert_gpu_fallback_collect(
        lambda spark : struct_to_json(spark),
        structs_to_json_fallback_class,
        conf=conf)

#####################################################
# Some from_json tests ported over from Apache Spark
#####################################################

# from_json escaping
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_escaping():
    schema = StructType([StructField("\"quote", IntegerType())])
    data = [[r'''{"\"quote":10}'''],
            [r"""{'"quote':20}"""]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

# from_json
# from_json null input column
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json():
    schema = StructType([StructField("a", IntegerType())])
    data = [[r'''{"a": 1}'''],
            [None]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

# from_json - input=empty array, schema=struct, output=single row with null
# from_json - input=empty object, schema=struct, output=single row with null
# SPARK-19543: from_json empty input column
@pytest.mark.parametrize('data', [
    [[r'''[]''']],
    [[r'''{ }''']],
    [[r''' ''']]], ids=idfn)
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_empty_table(data):
    schema = StructType([StructField("a", IntegerType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

# SPARK-20549: from_json bad UTF-8
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_bad_json():
    schema = StructType([StructField("a", IntegerType())])
    data = [["\u0000\u0000\u0000A\u0001AAA"]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

# from_json - invalid data
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_invalid():
    schema = StructType([StructField("a", IntegerType())])
    data = [[r'''{"a": 1}'''],
            [None]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

# This does not work the same way as the unit test. We fallback, and nulls are allowed as input, so we will just go with it for now
# If we ever do try to support FAILFAST this shold be updated so that there is an invalid JSON line and we produce the proper
# error
@allow_non_gpu('JsonToStructs')
def test_spark_from_json_invalid_failfast():
    schema = StructType([StructField("a", IntegerType())])
    data = [[r'''{"a": 1}'''],
            [None]]
    assert_gpu_fallback_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema, {'mode': 'FAILFAST'})),
        'JsonToStructs',
        conf =_enable_all_types_conf)

#from_json - input=array, schema=array, output=array
#from_json - input=object, schema=array, output=array of single row
#from_json - input=empty array, schema=array, output=empty array
#from_json - input=empty object, schema=array, output=array of single row with null
@allow_non_gpu('JsonToStructs')
def test_spark_from_json_array_schema():
    schema = ArrayType(StructType([StructField("a", IntegerType())]))
    data = [[r'''[{"a": 1}, {"a": 2}]'''],
            [r'''{"a": 1}'''],
            [r'''[ ]'''],
            [r'''{ }''']]
    assert_gpu_fallback_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        'JsonToStructs',
        conf =_enable_all_types_conf)

# from_json - input=array of single object, schema=struct, output=single row
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_single_item_array_to_struct():
    schema = StructType([StructField("a", IntegerType())])
    data = [[r'''[{"a": 1}]''']]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

#from_json - input=array, schema=struct, output=single row
@allow_non_gpu('JsonToStructs')
def test_spark_from_json_struct_with_corrupted_row():
    schema = StructType([StructField("a", IntegerType()), StructField("corrupted", StringType())])
    data = [[r'''[{"a": 1}, {"a": 2}]''']]
    assert_gpu_fallback_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema, {'columnNameOfCorruptRecord': 'corrupted'})),
        'JsonToStructs',
        conf =_enable_all_types_conf)

# The Spark test sets the time zone to several in a list, but we are just going to go off of our TZ testing for selecting time zones
# for this part of the test
# from_json with timestamp
@allow_non_gpu(*non_utc_allow)
def test_spark_from_json_timestamp_default_format():
    schema = StructType([StructField("a", IntegerType())])
    data = [[r'''{"t": "2016-01-01T00:00:00.123Z"}''']]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

# The spark test only sets the timezone as an ID one, but we really just care that we fallback when it is set to something we cannot support
@pytest.mark.parametrize('zone_id', [
    "UTC",
    "-08:00",
    "+01:00",
    "Africa/Dakar",
    "America/Los_Angeles",
    "Asia/Urumqi",
    "Asia/Hong_Kong",
    "Europe/Brussels"], ids=idfn)
@allow_non_gpu('JsonToStructs')
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10535')
# This is expected to fallback to the CPU because the timestampFormat is not supported, but really is, so we shold be better about this.
def test_spark_from_json_timestamp_format_option_zoneid(zone_id):
    schema = StructType([StructField("t", TimestampType())])
    data = [[r'''{"t": "2016-01-01T00:00:00"}''']]
    assert_gpu_fallback_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema, {'timestampFormat': "yyyy-MM-dd'T'HH:mm:ss",'timeZone': zone_id})),
        'JsonToStructs',
        conf =_enable_all_types_conf)

@pytest.mark.parametrize('zone_id', [
    "UTC",
    "-08:00",
    "+01:00",
    "Africa/Dakar",
    "America/Los_Angeles",
    "Asia/Urumqi",
    "Asia/Hong_Kong",
    "Europe/Brussels"], ids=idfn)
@allow_non_gpu(*non_utc_allow)
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485')
def test_spark_from_json_timestamp_format_option_zoneid_but_supported_format(zone_id):
    schema = StructType([StructField("t", TimestampType())])
    data = [[r'''{"t": "2016-01-01 00:00:00"}''']]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema, {'timestampFormat': "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]",'timeZone': zone_id})),
        conf =_enable_all_types_conf)

@pytest.mark.parametrize('zone_id', [
    "UTC",
    pytest.param("-08:00",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485')),
    pytest.param("+01:00",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485')),
    pytest.param("Africa/Dakar",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485')),
    pytest.param("America/Los_Angeles",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485')), # This works only some of the time??? https://github.com/NVIDIA/spark-rapids/issues/10488
    pytest.param("Asia/Urumqi",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485')),
    pytest.param("Asia/Hong_Kong",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485')),
    pytest.param("Europe/Brussels",marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485'))], ids=idfn)
@allow_non_gpu(*non_utc_allow)
def test_spark_from_json_timestamp_format_option_zoneid_but_default_format(zone_id):
    schema = StructType([StructField("t", TimestampType())])
    data = [[r'''{"t": "2016-01-01 00:00:00"}'''],
        [r'''{"t": "2023-07-27 12:21:05"}''']]
    conf = copy_and_update(_enable_all_types_conf, {"spark.rapids.sql.json.read.datetime.enabled": "true"})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema, {'timeZone': zone_id})),
        conf =conf)

# from_json with option (timestampFormat)
# no timestamp format appears to actually work
@allow_non_gpu('JsonToStructs')
def test_spark_from_json_timestamp_format():
    schema = StructType([StructField("time", TimestampType())])
    data = [[r'''{"time": "26/08/2015 18:00"}''']]
    assert_gpu_fallback_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema, {'timestampFormat': "dd/MM/yyyy HH:mm"})),
        'JsonToStructs',
        conf =_enable_all_types_conf)

# from_json missing fields
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_missing_fields_with_cr():
    schema = StructType([StructField("a", LongType(), False), StructField("b", StringType(), False), StructField("c", StringType(), False)])
    data = [["""{
        "a": 1,
        "c": "foo"
        }"""]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_missing_fields():
    schema = StructType([StructField("a", LongType(), False), StructField("b", StringType(), False), StructField("c", StringType(), False)])
    data = [["""{"a": 1,"c": "foo"}"""]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

# For now we are going to try and rely on the dateFormat to fallback, but we might want to
# fallback for unsupported locals too
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('data,locale', [
    ([["""{"d":"Nov 2018"}"""]], "en-US"),
    ([["""{"d":"ноя 2018"}"""]], "ru-RU"),
], ids=idfn)
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/10485')
def test_spark_from_json_date_with_locale(data, locale):
    schema = StructType([StructField("d", DateType())])
    assert_gpu_fallback_collect(
            lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema, {'dateFormat': 'MMM yyyy', 'locale': locale})),
        'ProjectExec',
        conf =_enable_all_types_conf)

@allow_non_gpu(*non_utc_allow)
@pytest.mark.skipif(is_before_spark_320(), reason="dd/MM/yyyy is supported in 3.2.0 and after")
def test_spark_from_json_date_with_format():
    data = [["""{"time": "26/08/2015"}"""],
            ["""{"time": "01/01/2024"}"""]]
    schema = StructType([StructField("d", DateType())])
    conf = copy_and_update(_enable_all_types_conf, {"spark.rapids.sql.json.read.datetime.enabled": "true"})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema, {'dateFormat': 'dd/MM/yyyy'})),
        conf =conf)

# TEST from_json missing columns
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_missing_columns():
    schema = StructType([StructField("b", IntegerType())])
    data = [['''{"a": 1}''']]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

# TEST from_json invalid json
@allow_non_gpu(*non_utc_allow) # https://github.com/NVIDIA/spark-rapids/issues/10453
def test_spark_from_json_invalid_json():
    schema = StructType([StructField("a", IntegerType())])
    data = [['''{"a" 1}''']]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame(data, 'json STRING').select(f.col('json'), f.from_json(f.col('json'), schema)),
        conf =_enable_all_types_conf)

@allow_non_gpu(*non_utc_allow)
def test_from_json_input_wrapped_in_whitespaces():
    json_string_gen = StringGen(r'[ \r\n\t]{0,5}({"key":( |\r|\n|\t|)"[A-Za-z]{0,5}"}|null|invalid|)[ \r\n\t]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, json_string_gen) \
            .select(f.from_json('a', 'struct<key:string>')),
        conf=_enable_all_types_conf)
