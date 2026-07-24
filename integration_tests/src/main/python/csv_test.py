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

from asserts import *
from conftest import get_non_gpu_allowed, is_not_utc, is_gbk_supported
from datetime import datetime, timezone
from data_gen import *
from marks import *
from pyspark.sql.types import *
from spark_session import *

_acq_schema = StructType([
    StructField('loan_id', LongType()),
    StructField('orig_channel', StringType()),
    StructField('seller_name', StringType()),
    StructField('orig_interest_rate', DoubleType()),
    StructField('orig_upb', IntegerType()),
    StructField('orig_loan_term', IntegerType()),
    StructField('orig_date', StringType()),
    StructField('first_pay_date', StringType()),
    StructField('orig_ltv', DoubleType()),
    StructField('orig_cltv', DoubleType()),
    StructField('num_borrowers', DoubleType()),
    StructField('dti', DoubleType()),
    StructField('borrower_credit_score', DoubleType()),
    StructField('first_home_buyer', StringType()),
    StructField('loan_purpose', StringType()),
    StructField('property_type', StringType()),
    StructField('num_units', IntegerType()),
    StructField('occupancy_status', StringType()),
    StructField('property_state', StringType()),
    StructField('zip', IntegerType()),
    StructField('mortgage_insurance_percent', DoubleType()),
    StructField('product_type', StringType()),
    StructField('coborrow_credit_score', DoubleType()),
    StructField('mortgage_insurance_type', DoubleType()),
    StructField('relocation_mortgage_indicator', StringType())])

_perf_schema = StructType([
    StructField('loan_id', LongType()),
    StructField('monthly_reporting_period', StringType()),
    StructField('servicer', StringType()),
    StructField('interest_rate', DoubleType()),
    StructField('current_actual_upb', DoubleType()),
    StructField('loan_age', DoubleType()),
    StructField('remaining_months_to_legal_maturity', DoubleType()),
    StructField('adj_remaining_months_to_maturity', DoubleType()),
    StructField('maturity_date', StringType()),
    StructField('msa', DoubleType()),
    StructField('current_loan_delinquency_status', IntegerType()),
    StructField('mod_flag', StringType()),
    StructField('zero_balance_code', StringType()),
    StructField('zero_balance_effective_date', StringType()),
    StructField('last_paid_installment_date', StringType()),
    StructField('foreclosed_after', StringType()),
    StructField('disposition_date', StringType()),
    StructField('foreclosure_costs', DoubleType()),
    StructField('prop_preservation_and_repair_costs', DoubleType()),
    StructField('asset_recovery_costs', DoubleType()),
    StructField('misc_holding_expenses', DoubleType()),
    StructField('holding_taxes', DoubleType()),
    StructField('net_sale_proceeds', DoubleType()),
    StructField('credit_enhancement_proceeds', DoubleType()),
    StructField('repurchase_make_whole_proceeds', StringType()),
    StructField('other_foreclosure_proceeds', DoubleType()),
    StructField('non_interest_bearing_upb', DoubleType()),
    StructField('principal_forgiveness_upb', StringType()),
    StructField('repurchase_make_whole_proceeds_flag', StringType()),
    StructField('foreclosure_principal_write_off_amount', StringType()),
    StructField('servicing_activity_indicator', StringType())])

_date_schema = StructType([
    StructField('date', DateType())])

_ts_schema = StructType([
    StructField('ts', TimestampType())])

_bad_str_schema = StructType([
    StructField('string', StringType())])

_good_str_schema = StructType([
    StructField('Something', StringType())])

_three_str_schema = StructType([
    StructField('a', StringType()),
    StructField('b', StringType()),
    StructField('c', StringType())])

_trucks_schema = StructType([
    StructField('make', StringType()),
    StructField('model', StringType()),
    StructField('year', IntegerType()),
    StructField('price', StringType()),
    StructField('comment', StringType())])

_bool_schema = StructType([
    StructField('boolean', BooleanType())])

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

_number_as_string_schema = StructType([
    StructField('number', StringType())])

_empty_byte_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', ByteType()),
    StructField('ignored_b', StringType())])

_empty_short_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', ShortType()),
    StructField('ignored_b', StringType())])

_empty_int_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', IntegerType()),
    StructField('ignored_b', StringType())])

_empty_long_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', LongType()),
    StructField('ignored_b', StringType())])

_empty_float_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', FloatType()),
    StructField('ignored_b', StringType())])

_empty_double_schema = StructType([
    StructField('ignored_a', StringType()),
    StructField('number', DoubleType()),
    StructField('ignored_b', StringType())])

_enable_all_types_conf = {'spark.rapids.sql.csv.read.float.enabled': 'true',
        'spark.rapids.sql.csv.read.double.enabled': 'true',
        'spark.rapids.sql.csv.read.decimal.enabled': 'true',
        'spark.sql.legacy.timeParserPolicy': 'CORRECTED'}

def read_csv_df(data_path, schema, spark_tmp_table_factory_ignored, options = {}):
    def read_impl(spark):
        reader = spark.read
        if not schema is None:
            reader = reader.schema(schema)
        for key, value in options.items():
            reader = reader.option(key, value)
        return debug_df(reader.csv(data_path))
    return read_impl

def read_csv_sql(data_path, schema, spark_tmp_table_factory, options = {}):
    opts = options
    if not schema is None:
        opts = copy_and_update(options, {'schema': schema})
    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        return spark.catalog.createTable(tmp_name, source='csv', path=data_path, **opts)
    return read_impl

@approximate_float
@pytest.mark.parametrize('name,schema,options', [
    ('Acquisition_2007Q3.txt', _acq_schema, {'sep': '|'}),
    ('Performance_2007Q3.txt_0', _perf_schema, {'sep': '|'}),
    ('ts.csv', _date_schema, {}),
    ('date.csv', _date_schema, {}),
    ('ts.csv', _ts_schema, {}),
    ('str.csv', _ts_schema, {}),
    ('str.csv', _bad_str_schema, {'header': 'true'}),
    ('str.csv', _good_str_schema, {'header': 'true'}),
    ('no-comments.csv', _three_str_schema, {}),
    ('empty.csv', _three_str_schema, {}),
    ('just_comments.csv', _three_str_schema, {'comment': '#'}),
    ('trucks.csv', _trucks_schema, {'header': 'true'}),
    ('trucks.tsv', _trucks_schema, {'sep': '\t', 'header': 'true'}),
    ('trucks-different.csv', _trucks_schema, {'sep': '|', 'header': 'true', 'quote': "'"}),
    ('trucks-blank-names.csv', _trucks_schema, {'header': 'true'}),
    ('trucks-windows.csv', _trucks_schema, {'header': 'true'}),
    ('trucks-empty-values.csv', _trucks_schema, {'header': 'true'}),
    ('trucks-extra-columns.csv', _trucks_schema, {'header': 'true'}),
    pytest.param('trucks-comments.csv', _trucks_schema, {'header': 'true', 'comment': '~'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/2066')),
    ('trucks-more-comments.csv', _trucks_schema,  {'header': 'true', 'comment': '#'}),
    pytest.param('trucks-missing-quotes.csv', _trucks_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/130')),
    pytest.param('trucks-null.csv', _trucks_schema, {'header': 'true', 'nullValue': 'null'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/2068')),
    pytest.param('trucks-null.csv', _trucks_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/1986')),
    pytest.param('simple_int_values.csv', _byte_schema, {'header': 'true'}),
    pytest.param('simple_int_values.csv', _short_schema, {'header': 'true'}),
    pytest.param('simple_int_values.csv', _int_schema, {'header': 'true'}),
    pytest.param('simple_int_values.csv', _long_schema, {'header': 'true'}),
    ('simple_int_values.csv', _float_schema, {'header': 'true'}),
    ('simple_int_values.csv', _double_schema, {'header': 'true'}),
    ('simple_int_values.csv', _decimal_10_2_schema, {'header': 'true'}),
    ('decimals.csv', _decimal_10_2_schema, {'header': 'true'}),
    ('decimals.csv', _decimal_10_3_schema, {'header': 'true'}),
    pytest.param('empty_int_values.csv', _empty_byte_schema, {'header': 'true'}),
    pytest.param('empty_int_values.csv', _empty_short_schema, {'header': 'true'}),
    pytest.param('empty_int_values.csv', _empty_int_schema, {'header': 'true'}),
    pytest.param('empty_int_values.csv', _empty_long_schema, {'header': 'true'}),
    pytest.param('empty_int_values.csv', _empty_float_schema, {'header': 'true'}),
    pytest.param('empty_int_values.csv', _empty_double_schema, {'header': 'true'}),
    pytest.param('nan_and_inf.csv', _float_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/125')),
    pytest.param('floats_invalid.csv', _float_schema, {'header': 'true'}),
    pytest.param('floats_invalid.csv', _double_schema, {'header': 'true'}),
    pytest.param('simple_float_values.csv', _byte_schema, {'header': 'true'}),
    pytest.param('simple_float_values.csv', _short_schema, {'header': 'true'}),
    pytest.param('simple_float_values.csv', _int_schema, {'header': 'true'}),
    pytest.param('simple_float_values.csv', _long_schema, {'header': 'true'}),
    pytest.param('simple_float_values.csv', _float_schema, {'header': 'true'}),
    pytest.param('simple_float_values.csv', _double_schema, {'header': 'true'}),
    pytest.param('simple_float_values.csv', _decimal_10_2_schema, {'header': 'true'}),
    pytest.param('simple_float_values.csv', _decimal_10_3_schema, {'header': 'true'}),
    pytest.param('simple_boolean_values.csv', _bool_schema, {'header': 'true'}),
    pytest.param('ints_with_whitespace.csv', _number_as_string_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/2069')),
    pytest.param('ints_with_whitespace.csv', _byte_schema, {'header': 'true'}, marks=pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/130'))
    ], ids=idfn)
@pytest.mark.parametrize('read_func', [read_csv_df, read_csv_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@tz_sensitive_test
@allow_non_gpu(*non_utc_allow)
def test_basic_csv_read(std_input_path, name, schema, options, read_func, v1_enabled_list, ansi_enabled, spark_tmp_table_factory):
    updated_conf=copy_and_update(_enable_all_types_conf, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.ansi.enabled': ansi_enabled
    })
    assert_gpu_and_cpu_are_equal_collect(read_func(std_input_path + '/' + name, schema, spark_tmp_table_factory, options),
            conf=updated_conf)

@pytest.mark.parametrize('name,schema,options', [
    pytest.param('small_float_values.csv', _float_schema, {'header': 'true'}),
    pytest.param('small_float_values.csv', _double_schema, {'header': 'true'}),
], ids=idfn)
@pytest.mark.parametrize('read_func', [read_csv_df, read_csv_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
def test_csv_read_small_floats(std_input_path, name, schema, options, read_func, v1_enabled_list, ansi_enabled, spark_tmp_table_factory):
    updated_conf=copy_and_update(_enable_all_types_conf, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.ansi.enabled': ansi_enabled
    })
    assert_gpu_and_cpu_are_equal_collect(read_func(std_input_path + '/' + name, schema, spark_tmp_table_factory, options),
                                         conf=updated_conf)

csv_supported_gens = [
        # Spark does not escape '\r' or '\n' even though it uses it to mark end of record
        # This would require multiLine reads to work correctly so we avoid these chars
        StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
        StringGen('[aAbB ]{0,10}'),
        StringGen('[nN][aA][nN]'),
        StringGen('[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?'),
        byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen,
        DoubleGen(no_nans=False),
        pytest.param(double_gen),
        pytest.param(FloatGen(no_nans=False)),
        pytest.param(float_gen),
        TimestampGen()]

@approximate_float
@pytest.mark.parametrize('data_gen', csv_supported_gens, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@allow_non_gpu(*non_utc_allow)
def test_round_trip(spark_tmp_path, data_gen, v1_enabled_list):
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(data_path))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(schema).csv(data_path),
            conf=updated_conf)

@disable_ansi_mode
@allow_non_gpu('org.apache.spark.sql.execution.LeafExecNode')
@pytest.mark.parametrize('read_func', [read_csv_df, read_csv_sql])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.csv.enabled', 'spark.rapids.sql.format.csv.read.enabled'])
def test_csv_fallback(spark_tmp_path, read_func, disable_conf, spark_tmp_table_factory):
    data_gens =[
        StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
        byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen]

    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {disable_conf: 'false'})

    reader = read_func(data_path, schema, spark_tmp_table_factory)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(data_path))
    assert_gpu_fallback_collect(
            lambda spark : reader(spark).select(f.col('*'), f.col('_c2') + f.col('_c3')),
            # TODO add support for lists
            cpu_fallback_class_name=get_non_gpu_allowed()[0],
            conf=updated_conf)

csv_supported_date_formats = ['yyyy-MM-dd', 'yyyy/MM/dd', 'yyyy-MM', 'yyyy/MM',
        'MM-yyyy', 'MM/yyyy', 'MM-dd-yyyy', 'MM/dd/yyyy', 'dd-MM-yyyy', 'dd/MM/yyyy']
@pytest.mark.parametrize('date_format', csv_supported_date_formats, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('BatchScanExec,FileSourceScanExec')),
    # Date is also time zone related for csv since rebase.
    pytest.param('CORRECTED', marks=pytest.mark.allow_non_gpu(*non_utc_allow)),
    pytest.param('EXCEPTION', marks=pytest.mark.allow_non_gpu(*non_utc_allow))
])
def test_date_formats_round_trip(spark_tmp_path, date_format, v1_enabled_list, ansi_enabled, time_parser_policy):
    gen = StructGen([('a', DateGen())], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf,
       {'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.ansi.enabled': ansi_enabled,
        'spark.sql.legacy.timeParserPolicy': time_parser_policy})
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write\
                    .option('dateFormat', date_format)\
                    .csv(data_path))
    if time_parser_policy == 'LEGACY':
        expected_class = 'FileSourceScanExec'
        if v1_enabled_list == '':
            expected_class = 'BatchScanExec'
        assert_gpu_fallback_collect(
            lambda spark : spark.read \
                .schema(schema) \
                .option('dateFormat', date_format) \
                .csv(data_path),
            expected_class,
            conf=updated_conf)
    else:
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('dateFormat', date_format)\
                    .csv(data_path),
            conf=updated_conf)

non_utc_allow_for_test_read_valid_and_invalid_dates=['BatchScanExec', 'FileSourceScanExec'] if is_not_utc() else []

@pytest.mark.parametrize('filename', ["date.csv"])
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('BatchScanExec,FileSourceScanExec')),
    # Date is also time zone related for csv since rebasing.
    pytest.param('CORRECTED', marks=pytest.mark.allow_non_gpu(*non_utc_allow_for_test_read_valid_and_invalid_dates)),
    pytest.param('EXCEPTION', marks=pytest.mark.allow_non_gpu(*non_utc_allow_for_test_read_valid_and_invalid_dates))
])
def test_read_valid_and_invalid_dates(std_input_path, filename, v1_enabled_list, ansi_enabled, time_parser_policy):
    data_path = std_input_path + '/' + filename
    updated_conf = copy_and_update(_enable_all_types_conf,
                                   {'spark.sql.sources.useV1SourceList': v1_enabled_list,
                                    'spark.sql.ansi.enabled': ansi_enabled,
                                    'spark.sql.legacy.timeParserPolicy': time_parser_policy})
    if time_parser_policy == 'EXCEPTION':
        assert_gpu_and_cpu_error(
            lambda spark : spark.read \
                .schema(_date_schema) \
                .csv(data_path)
                .collect(),
            conf=updated_conf,
            error_message='Exception')
    else:
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read \
                .schema(_date_schema) \
                .csv(data_path),
            conf=updated_conf)

csv_supported_ts_parts = ['', # Just the date
        "'T'HH:mm:ss.SSSXXX",
        "'T'HH:mm:ss[.SSS][XXX]",
        "'T'HH:mm:ss.SSS",
        "'T'HH:mm:ss[.SSS]",
        "'T'HH:mm:ss",
        "'T'HH:mm[:ss]",
        "'T'HH:mm"]

@pytest.mark.parametrize('ts_part', csv_supported_ts_parts)
@pytest.mark.parametrize('date_format', csv_supported_date_formats)
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@allow_non_gpu(*non_utc_allow)
def test_ts_formats_round_trip(spark_tmp_path, date_format, ts_part, v1_enabled_list):
    full_format = date_format + ts_part
    data_gen = TimestampGen()
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write\
                    .option('timestampFormat', full_format)\
                    .csv(data_path))
    updated_conf = copy_and_update(_enable_all_types_conf,
                   {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read\
                    .schema(schema)\
                    .option('timestampFormat', full_format)\
                    .csv(data_path),
            conf=updated_conf)

@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_input_meta(spark_tmp_path, v1_enabled_list):
    gen = StructGen([('a', long_gen), ('b', long_gen)], nullable=False)
    first_data_path = spark_tmp_path + '/CSV_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(first_data_path))
    second_data_path = spark_tmp_path + '/CSV_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(second_data_path))
    data_path = spark_tmp_path + '/CSV_DATA'
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.schema(gen.data_type)\
                    .csv(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            conf=updated_conf)

@allow_non_gpu('ProjectExec', 'Alias', 'InputFileName', 'InputFileBlockStart', 'InputFileBlockLength',
               'FilterExec', 'And', 'IsNotNull', 'GreaterThan', 'Literal',
               'FileSourceScanExec',
               'BatchScanExec', 'CsvScan')
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@pytest.mark.parametrize('disable_conf', ['spark.rapids.sql.format.csv.enabled', 'spark.rapids.sql.format.csv.read.enabled'])
def test_input_meta_fallback(spark_tmp_path, v1_enabled_list, disable_conf):
    gen = StructGen([('a', long_gen), ('b', long_gen)], nullable=False)
    first_data_path = spark_tmp_path + '/CSV_DATA/key=0'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(first_data_path))
    second_data_path = spark_tmp_path + '/CSV_DATA/key=1'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.csv(second_data_path))
    data_path = spark_tmp_path + '/CSV_DATA'
    updated_conf = copy_and_update(_enable_all_types_conf, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        disable_conf: 'false'})
    assert_gpu_fallback_collect(
            lambda spark : spark.read.schema(gen.data_type)\
                    .csv(data_path)\
                    .filter(f.col('a') > 0)\
                    .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
            cpu_fallback_class_name = 'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec',
            conf=updated_conf)

@allow_non_gpu('DataWritingCommandExec,ExecutedCommandExec,WriteFilesExec', *non_utc_allow)
def test_csv_save_as_table_fallback(spark_tmp_path, spark_tmp_table_factory):
    gen = TimestampGen()
    data_path = spark_tmp_path + '/CSV_DATA'
    assert_gpu_fallback_write(
            lambda spark, path: unary_op_df(spark, gen).coalesce(1).write.format("csv").mode('overwrite').option("path", path).saveAsTable(spark_tmp_table_factory.get()),
            lambda spark, path: spark.read.csv(path),
            data_path,
            'DataWritingCommandExec')

@allow_non_gpu(any = True)
@pytest.mark.parametrize('metadata_column', ["file_path", "file_name", "file_size", "file_modification_time"])
def test_csv_scan_with_hidden_metadata_fallback(spark_tmp_path, metadata_column):
    data_path = spark_tmp_path + "/hidden_metadata.csv"
    with_cpu_session(lambda spark : spark.range(10) \
                     .selectExpr("id") \
                     .write \
                     .mode("overwrite") \
                     .csv(data_path))

    def do_csv_scan(spark):
        df = spark.read.csv(data_path).selectExpr("_c0", "_metadata.{}".format(metadata_column))
        return df

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        do_csv_scan,
        exist_classes= "FileSourceScanExec",
        non_exist_classes= "GpuBatchScanExec")

@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_round_trip_for_interval(spark_tmp_path, v1_enabled_list):
    csv_interval_gens = [
        DayTimeIntervalGen(start_field="day", end_field="day"),
        DayTimeIntervalGen(start_field="day", end_field="hour"),
        DayTimeIntervalGen(start_field="day", end_field="minute"),
        DayTimeIntervalGen(start_field="day", end_field="second"),
        DayTimeIntervalGen(start_field="hour", end_field="hour"),
        DayTimeIntervalGen(start_field="hour", end_field="minute"),
        DayTimeIntervalGen(start_field="hour", end_field="second"),
        DayTimeIntervalGen(start_field="minute", end_field="minute"),
        DayTimeIntervalGen(start_field="minute", end_field="second"),
        DayTimeIntervalGen(start_field="second", end_field="second"),
    ]

    gen = StructGen([('_c' + str(i), csv_interval_gens[i]) for i in range(0, len(csv_interval_gens))], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = gen.data_type
    updated_conf = copy_and_update(_enable_all_types_conf, {'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_cpu_session(
        lambda spark: gen_df(spark, gen).write.csv(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).csv(data_path),
        conf=updated_conf)

@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec', 'DeserializeToObjectExec')
def test_csv_read_case_insensitivity(spark_tmp_path):
    gen_list = [('one', int_gen), ('tWo', byte_gen), ('THREE', boolean_gen)]
    data_path = spark_tmp_path + '/CSV_DATA'

    with_cpu_session(lambda spark: gen_df(spark, gen_list).write.option('header', True).csv(data_path))

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.option('header', True).csv(data_path).select('one', 'two', 'three'),
        exist_classes = 'GpuFileSourceScanExec',
        non_exist_classes = 'FileSourceScanExec',
        conf = {'spark.sql.caseSensitive': 'false'}
    )

@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec', 'DeserializeToObjectExec')
def test_csv_read_count(spark_tmp_path):
    data_gens = [byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    data_path = spark_tmp_path + '/CSV_DATA'

    with_cpu_session(lambda spark: gen_df(spark, gen_list).write.csv(data_path))

    # TODO This does not really test that the GPU count actually runs on the GPU
    # because this test has @allow_non_gpu for operators that fall back to CPU
    # when Spark performs an initial scan to infer the schema. To resolve this
    # we would need a new `assert_gpu_and_cpu_row_counts_equal_with_capture` function.
    # Tracking issue: https://github.com/NVIDIA/spark-rapids/issues/9199
    assert_gpu_and_cpu_row_counts_equal(lambda spark: spark.read.csv(data_path),
        conf = {'spark.rapids.sql.explain': 'ALL'})

@allow_non_gpu('FileSourceScanExec', 'ProjectExec', 'CollectLimitExec', 'DeserializeToObjectExec')
@pytest.mark.skipif(is_before_spark_341(), reason='`TIMESTAMP_NTZ` is only supported in PySpark 341+')
@pytest.mark.parametrize('date_format', csv_supported_date_formats)
@pytest.mark.parametrize('ts_part', csv_supported_ts_parts)
@pytest.mark.parametrize("timestamp_type", [
    pytest.param('TIMESTAMP_LTZ', marks=pytest.mark.xfail(is_spark_350_or_later(), reason="https://github.com/NVIDIA/spark-rapids/issues/9325")),
    "TIMESTAMP_NTZ"])
def test_csv_infer_schema_timestamp_ntz_v1(spark_tmp_path, date_format, ts_part, timestamp_type):
    csv_infer_schema_timestamp_ntz(spark_tmp_path, date_format, ts_part, timestamp_type, 'csv', 'FileSourceScanExec')

@allow_non_gpu('BatchScanExec', 'FileSourceScanExec', 'ProjectExec', 'CollectLimitExec', 'DeserializeToObjectExec')
@pytest.mark.skip(reason="https://github.com/NVIDIA/spark-rapids/issues/9325")
@pytest.mark.skipif(is_before_spark_341(), reason='`TIMESTAMP_NTZ` is only supported in PySpark 341+')
@pytest.mark.parametrize('date_format', csv_supported_date_formats)
@pytest.mark.parametrize('ts_part', csv_supported_ts_parts)
@pytest.mark.parametrize("timestamp_type", [
    pytest.param('TIMESTAMP_LTZ', marks=pytest.mark.xfail(is_spark_350_or_later(), reason="https://github.com/NVIDIA/spark-rapids/issues/9325")),
    "TIMESTAMP_NTZ"])
def test_csv_infer_schema_timestamp_ntz_v2(spark_tmp_path, date_format, ts_part, timestamp_type):
    csv_infer_schema_timestamp_ntz(spark_tmp_path, date_format, ts_part, timestamp_type, '', 'BatchScanExec')

def csv_infer_schema_timestamp_ntz(spark_tmp_path, date_format, ts_part, timestamp_type, v1_enabled_list, cpu_scan_class):
    full_format = date_format + ts_part
    # specify to use no timezone rather than defaulting to UTC
    data_gen = TimestampGen(tzinfo=None)
    gen = StructGen([('a', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    with_cpu_session(
        lambda spark : gen_df(spark, gen).write
            .option('timestampFormat', full_format)
            .csv(data_path))

    def do_read(spark):
        return spark.read.option("inferSchema", "true") \
            .option('timestampFormat', full_format) \
            .csv(data_path)

    conf = { 'spark.sql.timestampType': timestamp_type,
             'spark.sql.sources.useV1SourceList': v1_enabled_list }

    # determine whether Spark CPU infers TimestampType or TimestampNtzType
    inferred_type = with_cpu_session(
        lambda spark : do_read(spark).schema["_c0"].dataType.typeName(), conf=conf)

    if inferred_type == "timestamp_ntz":
        # we fall back to CPU due to "unsupported data types in output: TimestampNTZType"
        assert_gpu_fallback_collect(
            lambda spark: do_read(spark),
            cpu_fallback_class_name = cpu_scan_class,
            conf = conf)
    else:
        if is_not_utc():
            # non UTC is not support for csv, skip capture check
            # tracked in https://github.com/NVIDIA/spark-rapids/issues/9913
            assert_gpu_and_cpu_are_equal_collect(lambda spark: do_read(spark), conf = conf)
        else:
            assert_cpu_and_gpu_are_equal_collect_with_capture(
                lambda spark: do_read(spark),
                exist_classes = 'Gpu' + cpu_scan_class,
                non_exist_classes = cpu_scan_class,
                conf = conf)



@allow_non_gpu('FileSourceScanExec', 'CollectLimitExec', 'DeserializeToObjectExec')
@pytest.mark.skipif(is_before_spark_340(), reason='`preferDate` is only supported in Spark 340+')
def test_csv_prefer_date_with_infer_schema(spark_tmp_path):
    # start date ""0001-01-02" required due to: https://github.com/NVIDIA/spark-rapids/issues/5606
    data_gens = [byte_gen, short_gen, int_gen, long_gen, boolean_gen, timestamp_gen, DateGen(start=date(1, 1, 2))]
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    data_path = spark_tmp_path + '/CSV_DATA'

    with_cpu_session(lambda spark: gen_df(spark, gen_list).write.csv(data_path))

    if is_not_utc(): # non UTC is not support for csv
        exist_clazz = 'FileSourceScanExec'
        non_exist_clazz = 'GpuFileSourceScanExec'
    else:
        exist_clazz = 'GpuFileSourceScanExec'
        non_exist_clazz = 'FileSourceScanExec'

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.option("inferSchema", "true").csv(data_path),
        exist_classes = exist_clazz,
        non_exist_classes = non_exist_clazz)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.option("inferSchema", "true").option("preferDate", "false").csv(data_path),
        exist_classes = exist_clazz,
        non_exist_classes = non_exist_clazz)

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.skipif(is_before_spark_340(), reason='enableDateTimeParsingFallback is supported from Spark3.4.0')
@pytest.mark.parametrize('filename,schema',[("date.csv", _date_schema), ("date.csv", _ts_schema),
                                            ("ts.csv", _ts_schema)])
def test_csv_datetime_parsing_fallback_cpu_fallback(std_input_path, filename, schema):
    data_path = std_input_path + "/" + filename
    assert_gpu_fallback_collect(
        lambda spark : spark.read.schema(schema).option('enableDateTimeParsingFallback', "true").csv(data_path),
        'FileSourceScanExec',
        conf=_enable_all_types_conf)

@pytest.mark.skipif(is_before_spark_340(), reason='enableDateTimeParsingFallback is supported from Spark3.4.0')
@pytest.mark.parametrize('filename,schema', [("simple_int_values.csv", _int_schema), ("str.csv", _good_str_schema)])
def test_csv_datetime_parsing_fallback_no_datetime(std_input_path, filename, schema):
    data_path = std_input_path + "/" + filename
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.schema(schema).option('enableDateTimeParsingFallback', "true").csv(data_path),
        conf=_enable_all_types_conf)

@pytest.mark.parametrize('read_func', [read_csv_df, read_csv_sql])
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@pytest.mark.parametrize('col_name', ['K0', 'k0', 'K3', 'k3', 'V0', 'v0'], ids=idfn)
@ignore_order
def test_read_case_col_name(spark_tmp_path, spark_tmp_table_factory, read_func, v1_enabled_list, col_name):
    all_confs = {'spark.sql.sources.useV1SourceList': v1_enabled_list}
    gen_list =[('k0', LongGen(nullable=False, min_val=0, max_val=0)), 
            ('k1', LongGen(nullable=False, min_val=1, max_val=1)),
            ('k2', LongGen(nullable=False, min_val=2, max_val=2)),
            ('k3', LongGen(nullable=False, min_val=3, max_val=3)),
            ('v0', LongGen()),
            ('v1', LongGen()),
            ('v2', LongGen()),
            ('v3', LongGen())]
 
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/CSV_DATA'
    reader = read_func(data_path, gen.data_type, spark_tmp_table_factory)
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.partitionBy('k0', 'k1', 'k2', 'k3').csv(data_path))

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : reader(spark).selectExpr(col_name),
            conf=all_confs)

@ignore_order(local=True)
@allow_non_gpu('CollectLimitExec')
@pytest.mark.skipif(not is_gbk_supported(), reason="GBK is not supported")
def test_csv_read_gbk_encoded_data(std_input_path):
    # Conf does not work before 4.0.0, so verify even setting to false it should still work.
    legacy_charset = "false"
    if is_spark_400_or_later() or is_databricks_runtime():
        # true from Spark 4.0.0 or on supported Databricks runtimes to pass the test for GBK.
        # We can not test the "GBK-with-false" case because Spark will fail the
        # current app before running into the GPU world.
        legacy_charset = "true"
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read
            .option("charset", "GBK")
            .option("header", "true")
            .schema("name string, age int, city string, job string")
            .csv(std_input_path + "/test_gbk.csv"),
        conf={"spark.sql.legacy.javaCharsets": legacy_charset})

@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_csv_read_blank_lines_with_control_chars(std_input_path, v1_enabled_list):
    """Verify that lines consisting only of control characters (<= 0x20) are filtered out,
    matching Spark CPU behavior (CSVExprUtils.filterCommentAndEmpty uses String.trim)."""
    data_path = std_input_path + '/blank-control-chars.csv'
    schema = StructType([StructField('col1', StringType())])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).option('header', 'true').csv(data_path),
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list})
