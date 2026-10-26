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
    ('trucks-null.csv', _trucks_schema, {'header': 'true'}),
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
    read = read_func(std_input_path + '/' + name, schema, spark_tmp_table_factory, options)
    if name == 'trucks-null.csv' and 'nullValue' not in options:
        if v1_enabled_list == 'csv':
            gpu_scan = 'GpuFileSourceScanExec'
        elif read_func is read_csv_sql:
            gpu_scan = 'GpuFileSourceScanExec'
        else:
            gpu_scan = 'GpuBatchScanExec'
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            read, exist_classes=gpu_scan, conf=updated_conf)
    else:
        assert_gpu_and_cpu_are_equal_collect(read, conf=updated_conf)

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

# Keep this list in sync with GpuCSVScan.correctedDateFormats. None exercises
# the absent-format/default-formatter path as well as the derived parsing fallback policy.
csv_corrected_date_formats = [None, 'yyyy-MM-dd', 'yyyy/MM/dd', 'yyyy-MM', 'yyyy/MM',
        'dd/MM/yyyy', 'yyyy-MM-dd HH:mm:ss', 'MM-dd', 'MM/dd', 'dd-MM', 'dd/MM',
        'MM/yyyy', 'MM-yyyy', 'MM/dd/yyyy', 'MM-dd-yyyy', 'MMyyyy']

_csv_corrected_date_inputs = [
    None, '', '2020', '2020-05', '2020-5', '2020-05-01', '2020-5-1', '2020-05-1',
    '2020-5-01', '200-01-01', '0200-01-01', '0000-01-01', '-0000-01-01',
    '-0001-01-01', '+2020-05-01', '+02020-05-01', '02020-05-01', '+12345-01-01',
    '12345-01-01', '-12345-01-01', '+123456-01-01', '+1234567-01-01',
    '+5881580-07-11', '+5881580-07-12', '-5877641-06-23', '-5877641-06-22',
    '+5881580-07-11 23:59:59', '-5877641-06-23 00:00:00',
    '+999999999-01-01', ' 2020-05-01', '2020-05-01 ', '\t2020-05-01\t',
    '+0000002020-05-01', '-0000002020-05-01', '+0000000000000002020-05-01',
    '+00000000000000002020-05-01', '+9999999999999999999-01-01',
    '\x012020-05-01\x01', '\x1b2020-05-01\x1b', '\x1f2020-05-01\x1f',
    '\x7f2020-05-01\x7f', '2020-05-01T', '2020-05-01T10:11:12 PST',
    '2020-05-01 10:11:12', '2020-05-01 24:00:00', '2020-05-01 10:11:60',
    '2020-05-01 10:11:12 PST', '2020-05-01junk', '2020-05-01-02',
    '2020-02-29', '2019-02-29', '2020-02-30', '2020-00-01', '2020-13-01',
    '1582-10-04', '1582-10-10', '1582-10-15', '1900-02-29',
    '2021-03-14 02:30:00', '2021-11-07 01:30:00',
    '2020-01-00', '2020-01-32', '2020/05/01', '2020/5/1', '2020/05', '2020/5',
    '2020/02/29', '2020/02/30', '01/05/2020', '1/5/2020', '05/01/2020',
    '05-01-2020', '01-05-2020', '05-2020', '05/2020', '05-01', '05/01',
    '01-05', '01/05', '02-29', '29/02', '052020', '05200', '052020junk',
    '05-01-2020junk', '+12345/01/01', '-0001/01/01', '01/01/+12345',
    '05+12345', '05-0001', '050000', '05-+12345', '05/+12345', '01-01-+12345',
    '05+0000002020', '05/+0000002020',
    'GMT2020', '20GMT20-05-01', '2020-05-01GMT', 'invalid']



# Keep the grammar boundaries together; the format matrix below reuses the same rows.
_csv_corrected_date_corner_inputs = [
    # Sign, width, integer overflow, and truncated date components.
    '+', '-', '++2020', '--2020', '+-2020', '-+2020', '2020-', '2020--01',
    '2020-05-', '2020-005-01', '2020-05-001', '2020-01-010', '2020-123-01',
    '+2147483647-05-01', '+2147483648-05-01', '-2147483648-05-01',
    '+4294969316-05-01', '-4294969316-05-01', '0000000-01-01', '00000000-01-01',
    # Gregorian centuries, BCE leap years, missing-year defaults, and month lengths.
    '2000-02-29', '2100-02-29', '2400-02-29', '0000-02-29', '-0004-02-29',
    '-0001-02-29', '2020-04-30', '2020-04-31', '2020-12-31', '2020-12-32',
    '02-28', '02-30', '02-00', '00-01', '13-01', '04-31',
    # Every time field's upper bound and signed/truncated fields. CAST ignores a space tail.
    '2020-05-01 00:00:00', '2020-05-01 23:59:59', '2020-05-01 23:60:00',
    '2020-05-01 23:59:60', '2020-05-01 -1:00:00', '2020-05-01 00:-1:00',
    '2020-05-01 00:00:-1', '2020-05-01 10:', '2020-05-01 10:11:',
    '2020-05-01 10:11:12.0', '2020-05-01Tjunk', '2020-05-01 junk',
    '2020-05-01\tjunk', '2020 junk', '2020-05Tjunk',
    # GMT replacement removes original occurrences only and is case sensitive.
    'GMTGMT2020-05-01GMTGMT', '20GMT20-0GMT5-0GMT1', 'GMT', 'GMTGMT',
    'GM2020-05-01', '2020-05-01GM', '2020-05-01G', 'gmt2020-05-01',
    'GGMTMT2020-05-01', '20GGMTMT20-05-01', '2020-05-01GMT junk',
    '2020-05-01GMT\tjunk', '2020-05GMT', '2020GMT', '2020-02-30GMT',
    # UTF8String trimAll examines bytes rather than Unicode code points.
    '\u00a02020-05-01\u00a0', '\u20032020-05-01\u2003', '\u30002020-05-01\u3000',
    '２０２０-05-01', '2020-０５-01', '2020-05-０１']

_csv_corrected_date_format_examples = {
    'yyyy-MM-dd': '2020-05-01', 'yyyy/MM/dd': '2020/05/01',
    'yyyy-MM': '2020-05', 'yyyy/MM': '2020/05', 'dd/MM/yyyy': '01/05/2020',
    'yyyy-MM-dd HH:mm:ss': '2020-05-01 23:59:59',
    'MM-dd': '05-01', 'MM/dd': '05/01', 'dd-MM': '01-05', 'dd/MM': '01/05',
    'MM/yyyy': '05/2020', 'MM-yyyy': '05-2020', 'MM/dd/yyyy': '05/01/2020',
    'MM-dd-yyyy': '05-01-2020', 'MMyyyy': '052020'}


def write_corrected_date_csv(spark_tmp_path, extra_inputs=()):
    path = spark_tmp_path + '/corrected_dates'
    with_cpu_session(lambda spark: spark.createDataFrame(
        list(enumerate(_csv_corrected_date_inputs + _csv_corrected_date_corner_inputs +
            list(extra_inputs))), 'id int, value string').coalesce(1)
        .write.option('quoteAll', True).option('ignoreLeadingWhiteSpace', False)
        .option('ignoreTrailingWhiteSpace', False).csv(path))
    return path


def assert_corrected_date_csv(path, date_format, v1_enabled_list, conf, options):
    def read(spark):
        reader = spark.read.schema('id int, value date').options(**options)
        if date_format is not None:
            reader = reader.option('dateFormat', date_format)
        # Python datetime cannot represent signed/extended years. Compare epoch days instead.
        return reader.csv(path).selectExpr('id', "datediff(value, DATE '1970-01-01') as days")

    updated_conf = copy_and_update(conf, {
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
        'spark.rapids.sql.format.csv.read.enabled': 'true',
        'spark.rapids.sql.incompatibleDateFormats.enabled': 'false'})
    scan = 'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec'
    assert_cpu_and_gpu_are_equal_collect_with_capture(read, exist_classes='Gpu' + scan,
        non_exist_classes=scan, conf=updated_conf, require_non_empty=True)


@ignore_order(local=True)
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('date_format', csv_corrected_date_formats, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('session_tz', ['UTC', 'America/New_York'])
@pytest.mark.parametrize('parsing_fallback', ['false', 'true'])
def test_csv_corrected_date_parsing(spark_tmp_path, date_format, v1_enabled_list,
        session_tz, parsing_fallback):
    # Every proper prefix must be checked: a delimiter or field can end at the input boundary.
    example = _csv_corrected_date_format_examples.get(date_format, '')
    prefixes = [example[:end] for end in range(len(example))]
    path = write_corrected_date_csv(spark_tmp_path, prefixes)
    assert_corrected_date_csv(path, date_format, v1_enabled_list,
        {'spark.sql.session.timeZone': session_tz},
        {'enableDateTimeParsingFallback': parsing_fallback})


_csv_date_fallback_policies = [
    (None, None), (None, 'true'), (None, 'false'), ('true', None), ('false', None),
    ('false', 'true'), ('true', 'false'), ('true', 'true'), ('false', 'false')]


@ignore_order(local=True)
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('date_format,prefer_date', [
    (None, 'true'), (None, 'false'), ('yyyy/MM/dd', 'true'), ('yyyy-MM', 'true')], ids=idfn)
@pytest.mark.parametrize('policy_index,read_fallback,conf_fallback',
    [(i, *policy) for i, policy in enumerate(_csv_date_fallback_policies)])
def test_csv_corrected_date_fallback_precedence(spark_tmp_path, date_format, prefer_date,
        policy_index, read_fallback, conf_fallback):
    path = write_corrected_date_csv(spark_tmp_path)
    conf = {'spark.sql.session.timeZone': 'UTC' if policy_index % 2 else 'America/New_York'}
    options = {'preferDate': prefer_date}
    if read_fallback is not None:
        options['enableDateTimeParsingFallback'] = read_fallback
    if conf_fallback is not None:
        conf['spark.sql.legacy.csv.enableDateTimeParsingFallback'] = conf_fallback
    assert_corrected_date_csv(path, date_format, 'csv' if policy_index % 2 else '', conf, options)


@ignore_order(local=True)
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('prefer_date', ['false', 'true'])
@pytest.mark.parametrize('parsing_fallback', ['false', 'true'])
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_csv_corrected_date_default_formatter(spark_tmp_path, prefer_date,
        parsing_fallback, v1_enabled_list, ansi_enabled):
    path = write_corrected_date_csv(spark_tmp_path)
    assert_corrected_date_csv(path, None, v1_enabled_list,
        {'spark.sql.session.timeZone': 'America/New_York',
         'spark.sql.ansi.enabled': ansi_enabled},
        {'preferDate': prefer_date, 'enableDateTimeParsingFallback': parsing_fallback})


@ignore_order(local=True)
@allow_non_gpu('ProjectExec')
@pytest.mark.skipif(not (is_databricks_version(14, 3) or is_databricks_version(17, 3)),
        reason='Requires detected CSV date-format metadata')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('parsing_fallback', ['false', 'true'])
@pytest.mark.parametrize('date_format', [None, 'yyyy/MM/dd'], ids=idfn)
@pytest.mark.parametrize('columns', [('a', 'b', 'c'), ('c', 'b')], ids=idfn)
def test_csv_corrected_date_detected_formats(spark_tmp_path, v1_enabled_list,
        parsing_fallback, date_format, columns):
    path = spark_tmp_path + '/detected_date_formats'
    with_cpu_session(lambda spark: spark.createDataFrame([
        (0, '2020/05/01', '01/05/2020', '2020'),
        (1, '2020/02/29', '29/02/2020', '+0000002020-05-01'),
        (2, '2020-05-01', '2020-05-01', '2020-05-01'),
        (3, 'invalid', None, '2020-5-1')], 'id int, a string, b string, c string')
        .coalesce(1).write.csv(path))
    # Match the metadata emitted by the runtime's CSV schema inference.
    key = '__detected_date_formats'
    schema = StructType([
        StructField('id', IntegerType()),
        StructField('a', DateType(), metadata={key: 'yyyy/MM/dd'}),
        StructField('b', DateType(), metadata={key: 'dd/MM/yyyy'}),
        StructField('c', DateType())])

    def read(spark):
        reader = spark.read.schema(schema).option('preferDate', 'true') \
            .option('enableDateTimeParsingFallback', parsing_fallback)
        if date_format is not None:
            reader = reader.option('dateFormat', date_format)
        return reader.csv(path).selectExpr('id', *[
            "datediff({}, DATE '1970-01-01') as {}".format(name, name) for name in columns])

    scan = 'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec'
    assert_cpu_and_gpu_are_equal_collect_with_capture(read, exist_classes='Gpu' + scan,
        non_exist_classes=scan, require_non_empty=True,
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list,
              'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
              'spark.sql.session.timeZone': 'America/New_York',
              'spark.rapids.sql.format.csv.read.enabled': 'true',
              'spark.rapids.sql.incompatibleDateFormats.enabled': 'false'})


@ignore_order(local=True)
@allow_non_gpu('ProjectExec')
@pytest.mark.skipif(not (is_databricks_version(14, 3) or is_databricks_version(17, 3)),
        reason='Requires detected CSV date-format metadata')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('date_format,columns', [
    pytest.param(None, ('supported',), id='pruned-unsupported-column'),
    pytest.param('yyyy/MM/dd', ('unsupported', 'supported'), id='explicit-format-overrides-metadata')])
def test_csv_corrected_date_ignored_detected_format(spark_tmp_path, v1_enabled_list,
        date_format, columns):
    path = spark_tmp_path + '/ignored_detected_date_format'
    with_cpu_session(lambda spark: spark.createDataFrame([
        (0, '2020/05/01', '2020/05/01'),
        (1, '2020/02/29', '2020/02/29'),
        (2, None, None)], 'id int, supported string, unsupported string')
        .coalesce(1).write.csv(path))
    schema = StructType([
        StructField('id', IntegerType()),
        StructField('supported', DateType(), metadata={'__detected_date_formats': 'yyyy/MM/dd'}),
        StructField('unsupported', DateType(),
            metadata={'__detected_date_formats': 'dd-MMM-yyyy'})])

    def read(spark):
        reader = spark.read.schema(schema).option('preferDate', 'true') \
            .option('enableDateTimeParsingFallback', 'false')
        if date_format is not None:
            reader = reader.option('dateFormat', date_format)
        return reader.csv(path).selectExpr('id', *[
            "datediff({}, DATE '1970-01-01') as {}".format(name, name) for name in columns])

    # Unsupported metadata must not force CPU scans when its column is pruned or
    # an explicit dateFormat takes precedence over the detected format.
    scan = 'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec'
    assert_cpu_and_gpu_are_equal_collect_with_capture(read, exist_classes='Gpu' + scan,
        non_exist_classes=scan, require_non_empty=True,
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list,
              'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
              'spark.sql.session.timeZone': 'America/New_York',
              'spark.rapids.sql.format.csv.read.enabled': 'true',
              'spark.rapids.sql.incompatibleDateFormats.enabled': 'false'})


@ignore_order(local=True)
@allow_non_gpu('FileSourceScanExec', 'BatchScanExec')
@pytest.mark.skipif(not (is_databricks_version(14, 3) or is_databricks_version(17, 3)),
        reason='Requires detected CSV date-format metadata')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('date_format', ['dd-MMM-yyyy', 'dd-MM-yyyy'])
@pytest.mark.parametrize('count_only', [False, True])
def test_csv_corrected_date_unsupported_detected_format(spark_tmp_path, v1_enabled_list,
        date_format, count_only):
    path = spark_tmp_path + '/unsupported_detected_date_format'
    with_cpu_session(lambda spark: spark.createDataFrame([
        ('01-May-2020',), ('01-05-2020',), ('invalid',)], 'value string')
        .coalesce(1).write.csv(path))
    schema = StructType([
        StructField('value', DateType(), metadata={'__detected_date_formats': date_format})])

    def read(spark):
        df = spark.read.schema(schema).option('preferDate', 'true') \
            .option('enableDateTimeParsingFallback', 'false').csv(path)
        return df.selectExpr('count(*) as num_rows') if count_only else df

    assert_gpu_fallback_collect(read,
        'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec',
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list,
              'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
              'spark.rapids.sql.format.csv.read.enabled': 'true',
              'spark.rapids.sql.incompatibleDateFormats.enabled': 'false'})


@ignore_order(local=True)
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('session_tz', ['UTC', 'America/New_York'])
@pytest.mark.parametrize('parsing_fallback', ['false', 'true'])
def test_csv_corrected_date_unquoted_whitespace(spark_tmp_path, v1_enabled_list,
        session_tz, parsing_fallback):
    path = spark_tmp_path + '/date_unquoted_whitespace'
    values = ['2020-05-01', ' 2020-05-01', '2020-05-01 ', ' 2020-05-01 ',
              '\t2020-05-01', '2020-05-01\t', '\t2020-05-01\t', ' \t2020-05-01\t ']
    lines = []
    for index, value in enumerate(values):
        lines.extend([str(2 * index) + ',' + value, str(2 * index + 1) + ',"' + value + '"'])
    # Write raw CSV records so quoted and unquoted fields retain exactly the same whitespace.
    with_cpu_session(lambda spark: spark.createDataFrame([(line,) for line in lines], 'value string')
        .coalesce(1).write.text(path))
    assert_corrected_date_csv(path, 'yyyy-MM-dd', v1_enabled_list,
        {'spark.sql.session.timeZone': session_tz},
        {'ignoreLeadingWhiteSpace': 'false', 'ignoreTrailingWhiteSpace': 'false',
         'enableDateTimeParsingFallback': parsing_fallback})


@allow_non_gpu('FileSourceScanExec', 'BatchScanExec')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('leading,trailing', [('true', 'false'), ('false', 'true'), ('true', 'true')])
def test_csv_corrected_date_whitespace_options_fallback(spark_tmp_path, v1_enabled_list,
        leading, trailing):
    path = spark_tmp_path + '/date_whitespace'
    lines = ['0, 2020-05-01 ', '1," 2020-05-01 "', '2,\t2020-05-01\t',
             '3,"\t2020-05-01\t"']
    with_cpu_session(lambda spark: spark.createDataFrame([(line,) for line in lines], 'value string')
        .coalesce(1).write.text(path))
    assert_gpu_fallback_collect(lambda spark: spark.read.schema('id int, value date')
        .option('dateFormat', 'yyyy-MM-dd').option('enableDateTimeParsingFallback', 'false')
        .option('ignoreLeadingWhiteSpace', leading).option('ignoreTrailingWhiteSpace', trailing)
        .csv(path), 'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec',
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list,
              'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
              'spark.rapids.sql.format.csv.read.enabled': 'true',
              'spark.rapids.sql.incompatibleDateFormats.enabled': 'false'})


@ignore_order(local=True)
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('session_tz,read_tz,locale', [
    ('UTC', 'America/New_York', 'en-US'),
    ('America/New_York', 'UTC', 'ru-RU'),
    ('UTC', 'Asia/Kathmandu', 'zh-CN'),
    ('America/New_York', 'Pacific/Apia', 'en-US')])
def test_csv_corrected_date_timezone_and_locale(spark_tmp_path, v1_enabled_list,
        session_tz, read_tz, locale):
    # These include a DST gap/overlap and a skipped civil day. DateType keeps calendar days.
    path = write_corrected_date_csv(spark_tmp_path, [
        '2021-03-14 02:30:00', '2021-11-07 01:30:00', '2011-12-30 12:00:00'])
    assert_corrected_date_csv(path, 'yyyy-MM-dd HH:mm:ss', v1_enabled_list,
        {'spark.sql.session.timeZone': session_tz},
        {'timeZone': read_tz, 'locale': locale, 'enableDateTimeParsingFallback': 'false'})


@ignore_order(local=True)
@allow_non_gpu('FileSourceScanExec', 'BatchScanExec')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('mode', ['DROPMALFORMED', 'FAILFAST'])
def test_csv_corrected_date_record_mode_fallback(spark_tmp_path, v1_enabled_list, mode):
    path = spark_tmp_path + '/date_record_mode'
    # The second row requires parsing retry even in FAILFAST mode.
    rows = [(0, '2020/05/01'), (1, '2020-05-01')]
    if mode == 'DROPMALFORMED':
        rows.append((2, '2020-02-30'))
    with_cpu_session(lambda spark: spark.createDataFrame(rows, 'id int, value string')
        .coalesce(1).write.csv(path))
    assert_gpu_fallback_collect(lambda spark: spark.read.schema('id int, value date')
        .option('dateFormat', 'yyyy/MM/dd').option('enableDateTimeParsingFallback', 'true')
        .option('mode', mode).csv(path),
        'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec',
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list,
              'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
              'spark.rapids.sql.format.csv.read.enabled': 'true'})


@allow_non_gpu('FileSourceScanExec', 'BatchScanExec')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
def test_csv_corrected_date_failfast_invalid(spark_tmp_path, v1_enabled_list):
    path = spark_tmp_path + '/date_failfast'
    with_cpu_session(lambda spark: spark.createDataFrame([(0, '2020-02-30')],
        'id int, value string').write.csv(path))
    assert_gpu_and_cpu_error(lambda spark: spark.read.schema('id int, value date')
        .option('dateFormat', 'yyyy-MM-dd').option('enableDateTimeParsingFallback', 'true')
        .option('mode', 'FAILFAST').csv(path).collect(),
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list,
              'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
              'spark.rapids.sql.format.csv.read.enabled': 'true'},
        error_message='Malformed records are detected')


@ignore_order(local=True)
@allow_non_gpu('FileSourceScanExec', 'BatchScanExec')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('date_format', [
    'yyyyMMdd', 'yyyy-M-d', 'yyyy-MM-dd HH:ss', 'yyyy-MM-dd-MM'])
def test_csv_corrected_date_unsupported_format_fallback(spark_tmp_path, v1_enabled_list,
        date_format):
    path = spark_tmp_path + '/unsupported_date_format'
    # These patterns must stay on CPU independently of the expression format allowlist.
    with_cpu_session(lambda spark: spark.createDataFrame([
        (0, '20200501'), (1, '2020-5-1'), (2, 'invalid'),
        (3, '2020-05-01 99:99'), (4, '2020-05-01-06')], 'id int, value string')
        .coalesce(1).write.csv(path))
    assert_gpu_fallback_collect(lambda spark: spark.read.schema('id int, value date')
        .option('dateFormat', date_format).option('enableDateTimeParsingFallback', 'false')
        .csv(path), 'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec',
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list,
              'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
              'spark.rapids.sql.format.csv.read.enabled': 'true',
              'spark.rapids.sql.incompatibleDateFormats.enabled': 'false'})


@ignore_order(local=True)
@allow_non_gpu('FileSourceScanExec', 'BatchScanExec')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('policy', ['LEGACY', 'EXCEPTION'])
def test_csv_corrected_date_other_policy_fallback(spark_tmp_path, v1_enabled_list, policy):
    path = spark_tmp_path + '/other_date_policy'
    with_cpu_session(lambda spark: spark.createDataFrame([
        (0, '2020-05-01'), (1, '1500-01-01')], 'id int, value string')
        .coalesce(1).write.csv(path))
    assert_gpu_fallback_collect(lambda spark: spark.read.schema('id int, value date')
        .option('dateFormat', 'yyyy-MM-dd').csv(path),
        'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec',
        conf={'spark.sql.sources.useV1SourceList': v1_enabled_list,
              'spark.sql.legacy.timeParserPolicy': policy,
              'spark.sql.session.timeZone': 'America/New_York',
              'spark.rapids.sql.format.csv.read.enabled': 'true'})


@ignore_order(local=True)
@allow_non_gpu('ProjectExec', 'FileSourceScanExec', 'BatchScanExec')
@pytest.mark.skipif(is_before_spark_340(), reason='Parsing fallback option requires Spark 3.4+')
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('read_timestamp', [False, True])
def test_csv_corrected_date_timestamp_pruning(spark_tmp_path, v1_enabled_list, read_timestamp):
    path = spark_tmp_path + '/date_and_timestamp'
    with_cpu_session(lambda spark: spark.createDataFrame([
        (0, '2020/05/01', '2020-05-01 10:11:12'),
        (1, '2020-05-01', '2020-05-01 10:11:12')], 'id int, value string, ts string')
        .coalesce(1).write.csv(path))

    def read(spark):
        df = spark.read.schema('id int, value date, ts timestamp') \
            .option('dateFormat', 'yyyy/MM/dd').option('timestampFormat', 'yyyy-MM-dd HH:mm:ss') \
            .option('enableDateTimeParsingFallback', 'true').csv(path)
        return df if read_timestamp else df.select('id', 'value')

    conf = {'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
            'spark.sql.session.timeZone': 'UTC',
            'spark.rapids.sql.format.csv.read.enabled': 'true',
            'spark.rapids.sql.incompatibleDateFormats.enabled': 'false'}
    scan = 'FileSourceScanExec' if v1_enabled_list == 'csv' else 'BatchScanExec'
    if read_timestamp:
        assert_gpu_fallback_collect(read, scan, conf=conf)
    else:
        assert_cpu_and_gpu_are_equal_collect_with_capture(read, exist_classes='Gpu' + scan,
            non_exist_classes=scan, conf=conf, require_non_empty=True)


@pytest.mark.parametrize('date_format', csv_supported_date_formats, ids=idfn)
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('BatchScanExec,FileSourceScanExec')),
    pytest.param('CORRECTED'),
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

@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
def test_date_read_corrected_non_utc_timezone(spark_tmp_path, v1_enabled_list):
    data_path = spark_tmp_path + '/CSV_DATA'
    schema = StructType([
        StructField('id', IntegerType()),
        StructField('d', DateType())])
    updated_conf = copy_and_update(_enable_all_types_conf,
        {'spark.sql.sources.useV1SourceList': v1_enabled_list,
         'spark.sql.session.timeZone': 'America/New_York',
         'spark.sql.legacy.timeParserPolicy': 'CORRECTED',
         'spark.rapids.sql.incompatibleDateFormats.enabled': 'true'})
    with_cpu_session(
        lambda spark: spark.createDataFrame([
            (1, '2026-01-15'),
            (2, '2026-07-15'),
            (3, '1500-01-01')], 'id INT, d STRING') \
            .write.option('header', 'true').csv(data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema) \
            .option('header', 'true') \
            .option('dateFormat', 'yyyy-MM-dd') \
            .csv(data_path),
        conf=updated_conf)

non_utc_allow_for_test_read_valid_and_invalid_dates=['BatchScanExec', 'FileSourceScanExec'] if is_not_utc() else []

@pytest.mark.parametrize('filename', ["date.csv"])
@pytest.mark.parametrize('v1_enabled_list', ["", "csv"])
@pytest.mark.parametrize('ansi_enabled', ["true", "false"])
@pytest.mark.parametrize('time_parser_policy', [
    pytest.param('LEGACY', marks=pytest.mark.allow_non_gpu('BatchScanExec,FileSourceScanExec')),
    pytest.param('CORRECTED'),
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


@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
def test_csv_multiline_quoted_empty_lines(spark_tmp_path, v1_enabled_list):
    path = spark_tmp_path + '/csv_multiline'
    text = 'a,b\n"first\n\nquoted",1\nnormal,2\n'
    with_cpu_session(lambda spark: spark.createDataFrame([(text,)], 'value string')
                     .coalesce(1).write.text(path))

    def read(spark):
        return spark.read.schema('a string, b int').option('header', True) \
            .option('multiLine', True).csv(path)

    conf = {'spark.sql.sources.useV1SourceList': v1_enabled_list}
    rows = with_cpu_session(lambda spark: read(spark).collect(), conf=conf)
    assert [tuple(row) for row in rows] == [('first\n\nquoted', 1), ('normal', 2)]
    assert_gpu_and_cpu_are_equal_collect(read, conf=conf)


@pytest.mark.parametrize('compression', ['none', 'bzip2'])
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('projection', ['all', 'pruned', 'count',
    pytest.param('limit', marks=pytest.mark.allow_non_gpu('CollectLimitExec'))])
def test_csv_small_reader_budget(spark_tmp_path, compression, v1_enabled_list, projection):
    generators = [int_gen, StringGen('[a-zé,]{1,8}'), long_gen]
    gen = StructGen([(f'c{i}', generators[i % 3]) for i in range(46)], nullable=False)
    path = spark_tmp_path + '/csv_small_reader_budget'
    with_cpu_session(lambda spark: gen_df(spark, gen, length=64).coalesce(1).write
                     .option('header', True).option('compression', compression).csv(path))

    def read(spark):
        df = spark.read.schema(gen.data_type).option('header', True).csv(path)
        if projection == 'pruned':
            return df.select('c0', 'c1', 'c2')
        if projection == 'count':
            return df.selectExpr('count(*) as n')
        if projection == 'limit':
            return df.limit(1)
        return df

    assert_gpu_and_cpu_are_equal_collect(read, conf={
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.rapids.sql.reader.batchSizeBytes': '2048'})


@ignore_order(local=True)
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('header', [False, True])
@pytest.mark.parametrize('boundary', ['rows', 'bytes', 'partition'])
@pytest.mark.parametrize('projection', ['all', 'pruned', 'count'])
def test_csv_read_interior_bom(spark_tmp_path, v1_enabled_list, header, boundary, projection):
    path = spark_tmp_path + '/csv_interior_bom'
    prefix = '# x\n# y\na,b\n' if header else ''
    values = ['plain', '\ufefffirst', '\ufeff#value', '\ufeff', 'normal'] * 8
    text = '\ufeff' + prefix + '\n'.join(f'{value},{i}' for i, value in enumerate(values))
    with_cpu_session(lambda spark: spark.createDataFrame([(text,)], 'value string')
                     .coalesce(1).write.text(path))

    def read(spark):
        df = spark.read.schema('a string, b int').option('header', header).option('comment', '#').csv(path)
        if projection == 'pruned':
            return df.select('b')
        if projection == 'count':
            return df.selectExpr('count(*) as n')
        return df

    boundary_conf = {
        'rows': {'spark.rapids.sql.reader.batchSizeRows': '1'},
        'bytes': {'spark.rapids.sql.reader.batchSizeBytes': '1'},
        'partition': {'spark.sql.files.maxPartitionBytes': '32'}
    }
    assert_gpu_and_cpu_are_equal_collect(read, conf={
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        **boundary_conf[boundary]})


@ignore_order(local=True)
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('header', [False, True])
def test_csv_pruned_partition_bom_input_meta(spark_tmp_path, v1_enabled_list, header):
    path = spark_tmp_path + '/csv_partition_bom'
    prefix = '# x\n# y\na,b\n' if header else ''
    values = ['plain', '\ufefffirst', '\ufeff#value', '\ufeff', 'normal'] * 8
    text = '\ufeff' + prefix + '\n'.join(f'{value},{i}' for i, value in enumerate(values))
    with_cpu_session(lambda spark: spark.createDataFrame([(text,)], 'value string')
                     .coalesce(1).write.text(path + '/p=0'))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema('a string, b int').option('header', header)
            .option('comment', '#').csv(path)
            .selectExpr('b', 'input_file_block_start()', 'input_file_block_length()'),
        conf={
            'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.files.maxPartitionBytes': '32'})


@ignore_order(local=True)
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('prefix', ['', '\n \t\n', '# leading comment\n \t\n'])
@pytest.mark.parametrize('first_record,header,quote', [
    ('\ufefffirst,1', False, '"'), ('\ufeff#value,1', False, '"'), ('\ufeff', False, '"'),
    ('\ufeff \t', False, '"'), ('\ufeff\ufefffirst,1', False, '"'),
    ('\ufeff"quoted,value",1', False, '"'), ('\ufeffa,b', True, '"'),
    ('\ufeff#not_header,ignored', True, '"'), ('\ufeff"quoted,value",1', False, ''),
    ('\ufeff', False, ''), ('\ufeff#value,1', False, '')])
@pytest.mark.parametrize('schema', ['a string, b int', 'a string'])
def test_csv_first_record_bom(spark_tmp_path, v1_enabled_list, prefix, first_record, header,
                            schema, quote):
    path = spark_tmp_path + '/csv_first_record_bom'
    text = '\ufeff' + prefix + first_record + '\n\ufefflater,2\nnormal,3'
    with_cpu_session(lambda spark: spark.createDataFrame([(text,)], 'value string')
                     .coalesce(1).write.text(path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).option('comment', '#').option('quote', quote)
            .option('header', header).csv(path),
        conf={
            'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.files.maxPartitionBytes': '1m',
            'spark.rapids.sql.reader.batchSizeRows': '1'})


@ignore_order(local=True)
@pytest.mark.parametrize('v1_enabled_list', ['', 'csv'])
@pytest.mark.parametrize('null_value', ['', 'NULL'])
def test_csv_first_record_bom_without_quotes(spark_tmp_path, v1_enabled_list, null_value):
    path = spark_tmp_path + '/csv_first_record_bom_without_quotes'
    text = '\ufeff\ufeff#value\n"later,quoted"\n\ufeffend'
    with_cpu_session(lambda spark: spark.createDataFrame([(text,)], 'value string')
                     .coalesce(1).write.text(path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema('a string').option('comment', '#').option('quote', '')
            .option('nullValue', null_value).csv(path),
        conf={
            'spark.sql.sources.useV1SourceList': v1_enabled_list,
            'spark.sql.files.maxPartitionBytes': '1m'})


spark_350_timestamp_inference_xfail_formats = {
    ('yyyy-MM-dd', ''),
    ('yyyy-MM', ''),
    ('yyyy-MM-dd', "'T'HH:mm:ss"),
    ('yyyy-MM-dd', "'T'HH:mm"),
}

def run_csv_infer_schema_timestamp_ntz(
        spark_tmp_path, date_format, ts_part, timestamp_type, v1_enabled_list, cpu_scan_class):
    try:
        csv_infer_schema_timestamp_ntz(
            spark_tmp_path, date_format, ts_part, timestamp_type, v1_enabled_list,
            cpu_scan_class)
    except Exception:
        if (is_spark_350()
                and timestamp_type == 'TIMESTAMP_LTZ'
                and (date_format, ts_part) in spark_350_timestamp_inference_xfail_formats):
            pytest.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/9325")
        raise

@allow_non_gpu('FileSourceScanExec', 'ProjectExec', 'CollectLimitExec', 'DeserializeToObjectExec')
@pytest.mark.skipif(is_before_spark_341(), reason='`TIMESTAMP_NTZ` is only supported in PySpark 341+')
@pytest.mark.parametrize('date_format', csv_supported_date_formats)
@pytest.mark.parametrize('ts_part', csv_supported_ts_parts)
@pytest.mark.parametrize('timestamp_type', ['TIMESTAMP_LTZ', 'TIMESTAMP_NTZ'])
def test_csv_infer_schema_timestamp_ntz_v1(spark_tmp_path, date_format, ts_part, timestamp_type):
    run_csv_infer_schema_timestamp_ntz(
        spark_tmp_path, date_format, ts_part, timestamp_type, 'csv', 'FileSourceScanExec')

@allow_non_gpu('BatchScanExec', 'FileSourceScanExec', 'ProjectExec', 'CollectLimitExec', 'DeserializeToObjectExec')
@pytest.mark.skipif(is_databricks_runtime(),
    reason="https://github.com/NVIDIA/spark-rapids/issues/9325")
@pytest.mark.skipif(is_before_spark_341(), reason='`TIMESTAMP_NTZ` is only supported in PySpark 341+')
@pytest.mark.parametrize('date_format', csv_supported_date_formats)
@pytest.mark.parametrize('ts_part', csv_supported_ts_parts)
@pytest.mark.parametrize('timestamp_type', ['TIMESTAMP_LTZ', 'TIMESTAMP_NTZ'])
def test_csv_infer_schema_timestamp_ntz_v2(spark_tmp_path, date_format, ts_part, timestamp_type):
    run_csv_infer_schema_timestamp_ntz(
        spark_tmp_path, date_format, ts_part, timestamp_type, '', 'BatchScanExec')

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

    # Use the writer's date format to keep this test focused on preferDate inference.
    # GPU support for detected variable-width formats is tracked in
    # https://github.com/NVIDIA/cudf-spark/issues/16540.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.option("inferSchema", "true")
            .option("dateFormat", "yyyy-MM-dd").csv(data_path),
        exist_classes = exist_clazz,
        non_exist_classes = non_exist_clazz)
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.read.option("inferSchema", "true")
            .option("dateFormat", "yyyy-MM-dd").option("preferDate", "false").csv(data_path),
        exist_classes = exist_clazz,
        non_exist_classes = non_exist_clazz)

@allow_non_gpu('FileSourceScanExec')
@pytest.mark.skipif(is_before_spark_340(), reason='enableDateTimeParsingFallback is supported from Spark3.4.0')
@pytest.mark.parametrize('filename,schema',[("date.csv", _ts_schema),
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
