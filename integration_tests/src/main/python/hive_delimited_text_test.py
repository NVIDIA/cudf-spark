# Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_sql_writes_are_equal_collect, assert_gpu_fallback_collect
from conftest import get_non_gpu_allowed, is_not_utc
from data_gen import *
from enum import Enum
from marks import *
from pyspark.sql.types import *
from spark_session import is_spark_cdh, with_cpu_session

hive_text_enabled_conf = {"spark.rapids.sql.format.hive.text.enabled": True,
                          "spark.rapids.sql.format.hive.text.read.enabled": True}

hive_text_write_enabled_conf = {"spark.rapids.sql.format.hive.text.enabled": True,
                                "spark.rapids.sql.format.hive.text.write.enabled": True}

acq_schema = StructType([
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

perf_schema = StructType([
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

timestamp_schema = StructType([
    StructField('ts', TimestampType())])

date_schema = StructType([
    StructField('date', DateType())])

trucks_schema = StructType([
    StructField('make', StringType()),
    StructField('model', StringType()),
    StructField('year', IntegerType()),
    StructField('price', StringType()),
    StructField('comment', StringType())])


def make_schema(column_type):
    """
    Constructs a table schema with a single column of the specified type
    """
    return StructType([StructField('number', column_type)])


def read_hive_text_sql(data_path, schema, spark_tmp_table_factory, options=None):
    if options is None:
        options = {}
    opts = options
    if schema is not None:
        opts = copy_and_update(options, {'schema': schema})

    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        return spark.catalog.createTable(tmp_name, source='hive', path=data_path, **opts)

    return read_impl

def read_hive_text_sql_wrong_case(data_path, schema, spark_tmp_table_factory, options=None):
    if options is None:
        options = {}
    def mk_upper(f):
        return StructField(f.name.upper(), f.dataType)

    upper_s = StructType(list(map(mk_upper, schema.fields)))
    print("CONVERTED " +str(schema) + " TO " + str(upper_s))
    lower_fields = ','.join(map(lambda name: name.lower(), schema.fieldNames()))
    opts = copy_and_update(options, {'schema': upper_s})

    def read_impl(spark):
        tmp_name = spark_tmp_table_factory.get()
        spark.catalog.createTable(tmp_name, source='hive', path=data_path, **opts)
        return spark.sql("SELECT " + lower_fields + " FROM " + tmp_name)

    return read_impl


non_utc_allow_for_test_basic_hive_text_read=['HiveTableScanExec', 'DataWritingCommandExec', 'WriteFilesExec'] if is_not_utc() else []
@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@approximate_float
@pytest.mark.parametrize('name,schema,options', [

    # Numeric Reads.
    ('hive-delim-text/simple-boolean-values', make_schema(BooleanType()),        {}),
    ('hive-delim-text/simple-int-values',     make_schema(ByteType()),           {}),
    ('hive-delim-text/simple-int-values',     make_schema(ShortType()),          {}),
    ('hive-delim-text/simple-int-values',     make_schema(IntegerType()),        {}),
    ('hive-delim-text/simple-int-values',     make_schema(LongType()),           {}),
    ('hive-delim-text/simple-int-values',     make_schema(FloatType()),          {}),
    ('hive-delim-text/simple-int-values',     make_schema(DoubleType()),         {}),
    ('hive-delim-text/simple-int-values',     make_schema(StringType()),         {}),
    pytest.param('hive-delim-text/simple-int-values', make_schema(DecimalType(10, 2)), {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    pytest.param('hive-delim-text/simple-int-values', make_schema(DecimalType(10, 3)),   {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),

    # Floating Point.
    ('hive-delim-text/simple-float-values',   make_schema(FloatType()),          {}),
    ('hive-delim-text/simple-float-values',   make_schema(DoubleType()),         {}),
    pytest.param('hive-delim-text/simple-float-values', make_schema(DecimalType(10, 3)), {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    pytest.param('hive-delim-text/simple-float-values', make_schema(DecimalType(38, 10)), {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    ('hive-delim-text/simple-float-values',   make_schema(IntegerType()),        {}),
    ('hive-delim-text/extended-float-values', make_schema(IntegerType()),        {}),
    ('hive-delim-text/extended-float-values', make_schema(FloatType()),          {}),
    ('hive-delim-text/extended-float-values', make_schema(DoubleType()),         {}),
    pytest.param('hive-delim-text/extended-float-values',   make_schema(DecimalType(10, 3)),   {},
        marks=pytest.mark.xfail(reason="GPU supports more valid values than CPU. "
            "https://github.com/NVIDIA/spark-rapids/issues/7246")),
    pytest.param('hive-delim-text/extended-float-values',   make_schema(DecimalType(38, 10)),   {},
        marks=pytest.mark.xfail(reason="GPU supports more valid values than CPU. "
            "https://github.com/NVIDIA/spark-rapids/issues/7246")),

    # Custom datasets
    ('hive-delim-text/Acquisition_2007Q3', acq_schema, {}),
    ('hive-delim-text/Performance_2007Q3', perf_schema, {'serialization.null.format': ''}),
    ('hive-delim-text/Performance_2007Q3', perf_schema, {}),
    ('hive-delim-text/trucks-1', trucks_schema, {}),
    ('hive-delim-text/trucks-err', trucks_schema, {}),

    # Date/Time
    pytest.param('hive-delim-text/timestamp', timestamp_schema, {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    pytest.param('hive-delim-text/date', date_schema, {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),

    # Test that lines beginning with comments ('#') aren't skipped.
    ('hive-delim-text/comments', StructType([StructField("str", StringType()),
                                             StructField("num", IntegerType()),
                                             StructField("another_str", StringType())]), {}),

    # Test that carriage returns ('\r'/'^M') are treated similarly to newlines ('\n')
    ('hive-delim-text/carriage-return', StructType([StructField("str", StringType())]), {}),
    ('hive-delim-text/carriage-return-err', StructType([StructField("str", StringType())]), {}),
], ids=idfn)
@allow_non_gpu(*non_utc_allow_for_test_basic_hive_text_read)
def test_basic_hive_text_read(std_input_path, name, schema, spark_tmp_table_factory, options):
    assert_gpu_and_cpu_are_equal_collect(read_hive_text_sql(std_input_path + '/' + name,
                                                            schema, spark_tmp_table_factory, options),
                                         conf=hive_text_enabled_conf)


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@approximate_float
@pytest.mark.parametrize('name,schema,options', [
    ('hive-delim-text/simple-boolean-values', make_schema(BooleanType()), {})
], ids=idfn)
@allow_non_gpu(*non_utc_allow_for_test_basic_hive_text_read)
def test_case_insensitive_hive_text_read(std_input_path, name, schema, spark_tmp_table_factory, options):
    assert_gpu_and_cpu_are_equal_collect(read_hive_text_sql_wrong_case(std_input_path + '/' + name,
                                                            schema, spark_tmp_table_factory, options),
                                         conf=hive_text_enabled_conf)


hive_text_supported_gens = [
    StringGen('(\\w| |\t|\ud720){0,10}', nullable=False),
    StringGen('[aAbB ]{0,10}'),
    StringGen('[nN][aA][nN]'),
    StringGen('[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?'),
    byte_gen, short_gen, int_gen, long_gen, boolean_gen, date_gen,
    float_gen,
    FloatGen(no_nans=False),
    double_gen,
    DoubleGen(no_nans=False),
    TimestampGen(),
]


def create_hive_text_table(spark, column_gen, text_table_name, data_path, fields="my_field"):
    """
    Helper method to create a Hive Text table with contents from the specified
    column generator.
    :param spark: Spark context for the test
    :param column_gen: Data generator for the table's column
    :param text_table_name: (Temp) Name of the created Hive Text table
    :param data_path: Data location for the created Hive Text table
    :param fields: The fields composing the table to be created
    """
    gen_df(spark, column_gen).repartition(1).createOrReplaceTempView("input_view")
    spark.sql("DROP TABLE IF EXISTS " + text_table_name)
    spark.sql("CREATE TABLE " + text_table_name + " STORED AS TEXTFILE " +
              "LOCATION '" + data_path + "' " +
              "AS SELECT " + fields + " FROM input_view")


def read_hive_text_table(spark, text_table_name, fields="my_field"):
    """
    Helper method to read the contents of a Hive (Text) table.
    :param spark: Spark context for the test
    :param text_table_name: Name of the Hive (Text) table to be read
    :param fields: The fields to be read from the specified table
    """
    return spark.sql("SELECT " + fields + " FROM " + text_table_name)


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@approximate_float
@pytest.mark.parametrize('data_gen', hive_text_supported_gens, ids=idfn)
@allow_non_gpu(*non_utc_allow_for_test_basic_hive_text_read)
def test_hive_text_round_trip(spark_tmp_path, data_gen, spark_tmp_table_factory):
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table(spark, gen, table_name, data_path))

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: read_hive_text_table(spark, table_name),
            conf=hive_text_enabled_conf)


def create_hive_text_table_partitioned(spark, column_gen, text_table_name, data_path):
    gen_df(spark, column_gen).repartition(1).createOrReplaceTempView("input_view")
    spark.sql("DROP TABLE IF EXISTS " + text_table_name)
    column_type = column_gen.children[0][1].data_type.simpleString()  # Because StructGen([('my_field', gen)]).
    spark.sql("CREATE TABLE " + text_table_name +
              "( my_field " + column_type + ") "
              "PARTITIONED BY (dt STRING) "
              "STORED AS TEXTFILE "
              "LOCATION '" + data_path + "' ")
    spark.sql("INSERT OVERWRITE " + text_table_name + " PARTITION( dt='1' ) "
              "SELECT my_field FROM input_view")
    spark.sql("INSERT OVERWRITE " + text_table_name + " PARTITION( dt='2' ) "
              "SELECT my_field FROM input_view")


def read_hive_text_table_partitions(spark, text_table_name, partition):
    """
    Helper method to read the contents of a Hive (Text) table.
    :param spark: Spark context for the test
    :param text_table_name: Name of the Hive (Text) table to be read
    :param partition: Partition selection string (e.g. "dt=1")
    """
    return spark.sql("SELECT my_field FROM %s WHERE %s" % (text_table_name, partition))


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@approximate_float
@allow_non_gpu("EqualTo", "IsNotNull", *non_utc_allow_for_test_basic_hive_text_read)  # Accounts for partition predicate: `WHERE dt='1'`
@pytest.mark.parametrize('data_gen', hive_text_supported_gens, ids=idfn)
def test_hive_text_round_trip_partitioned(spark_tmp_path, data_gen, spark_tmp_table_factory):
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table_partitioned(spark, gen, table_name, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_hive_text_table_partitions(spark, table_name, "dt='1'"),
        conf=hive_text_enabled_conf)


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@approximate_float
@allow_non_gpu("EqualTo", "IsNotNull", *non_utc_allow_for_test_basic_hive_text_read)  # Accounts for partition predicate: `WHERE dt='1'`
@pytest.mark.parametrize('data_gen', [boolean_gen], ids=idfn)
def test_hive_text_round_trip_partitioned_case_insensitive(spark_tmp_path, data_gen, spark_tmp_table_factory):
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table_partitioned(spark, gen, table_name, data_path))

    # The 'DT' would need to be 'dt' for it to be case sensitive
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_hive_text_table_partitions(spark, table_name, "DT='1'"),
        conf=hive_text_enabled_conf)

@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@approximate_float
@allow_non_gpu("EqualTo", "IsNotNull", "Or", *non_utc_allow_for_test_basic_hive_text_read)  # Accounts for partition predicate
@pytest.mark.parametrize('data_gen', hive_text_supported_gens, ids=idfn)
def test_hive_text_round_trip_two_partitions(spark_tmp_path, data_gen, spark_tmp_table_factory):
    """
    Added to reproduce: https://github.com/NVIDIA/spark-rapids/issues/7383
    """
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table_partitioned(spark, gen, table_name, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_hive_text_table_partitions(spark, table_name, "dt='1' or dt='2'"),
        conf=hive_text_enabled_conf)


hive_text_unsupported_gens = [
    ArrayGen(string_gen),
    StructGen([('int_field', int_gen), ('string_field', string_gen)]),
    MapGen(StringGen(nullable=False), string_gen),
    binary_gen,
    StructGen([('b', byte_gen), ('i', int_gen), ('arr_of_i', ArrayGen(int_gen))]),
    ArrayGen(StructGen([('b', byte_gen), ('i', int_gen), ('arr_of_i', ArrayGen(int_gen))]))
]


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@allow_non_gpu("org.apache.spark.sql.hive.execution.HiveTableScanExec")
@pytest.mark.parametrize('unsupported_gen', hive_text_unsupported_gens, ids=idfn)
def test_hive_text_fallback_for_unsupported_types(spark_tmp_path, unsupported_gen, spark_tmp_table_factory):
    supported_gen = int_gen  # Generator for 1 supported data type. (IntegerGen chosen arbitrarily.)
    gen = StructGen([('my_supported_int_field', supported_gen),
                     ('my_unsupported_field', unsupported_gen), ], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table(spark,
                                                          gen,
                                                          table_name,
                                                          data_path,
                                                          "my_supported_int_field, my_unsupported_field"))

    assert_gpu_fallback_collect(
            lambda spark: read_hive_text_table(spark, table_name, "my_unsupported_field"),
            cpu_fallback_class_name=get_non_gpu_allowed()[0],
            conf=hive_text_enabled_conf)

    # GpuHiveTableScanExec cannot partially read only those columns that are of supported types.
    # Even if the output-projection uses only supported types, the read should fall back to CPU
    # if the table has even one column of an unsupported type.
    assert_gpu_fallback_collect(
        lambda spark: read_hive_text_table(spark, table_name, "my_supported_int_field"),
        cpu_fallback_class_name=get_non_gpu_allowed()[0],
        conf=hive_text_enabled_conf)


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@pytest.mark.parametrize('data_gen', [StringGen()], ids=idfn)
def test_hive_text_default_enabled(spark_tmp_path, data_gen, spark_tmp_table_factory):
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    with_cpu_session(lambda spark: create_hive_text_table(spark, gen, table_name, data_path))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: read_hive_text_table(spark, table_name),
        conf={})


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@allow_non_gpu("org.apache.spark.sql.hive.execution.HiveTableScanExec")
@pytest.mark.parametrize('data_gen', [TimestampGen()], ids=idfn)
def test_custom_timestamp_formats_disabled(spark_tmp_path, data_gen, spark_tmp_table_factory):
    """
    This is to test that the plugin falls back to CPU execution, in case a Hive delimited
    text table is set up with a custom timestamp format, via the "timestamp.formats"
    property.
    Note that this property could be specified in either table properties,
    or SerDe properties.
    """
    gen = StructGen([('my_field', data_gen)], nullable=False)
    data_path = spark_tmp_path + '/hive_text_table'
    table_name = spark_tmp_table_factory.get()

    from enum import Enum

    class PropertyLocation(Enum):
        TBLPROPERTIES = 1,
        SERDEPROPERTIES = 2

    def create_hive_table_with_custom_timestamp_format(spark, property_location):
        gen_df(spark, gen).repartition(1).createOrReplaceTempView("input_view")
        spark.sql("DROP TABLE IF EXISTS " + table_name)
        spark.sql("CREATE TABLE " + table_name + " (my_field TIMESTAMP) "
                  "STORED AS TEXTFILE " +
                  "LOCATION '" + data_path + "' ")
        spark.sql("ALTER TABLE " + table_name + " SET " +
                  ("TBLPROPERTIES" if property_location == PropertyLocation.TBLPROPERTIES else "SERDEPROPERTIES") +
                  "('timestamp.formats'='yyyy-MM-dd HH:mm:ss.SSS')")
        spark.sql("INSERT INTO TABLE " + table_name + " SELECT * FROM input_view")

    with_cpu_session(lambda spark:
                     create_hive_table_with_custom_timestamp_format(spark, PropertyLocation.TBLPROPERTIES))
    assert_gpu_fallback_collect(
        lambda spark: read_hive_text_table(spark, table_name),
        cpu_fallback_class_name=get_non_gpu_allowed()[0],
        conf=hive_text_enabled_conf)

    with_cpu_session(lambda spark:
                     create_hive_table_with_custom_timestamp_format(spark, PropertyLocation.SERDEPROPERTIES))
    assert_gpu_fallback_collect(
        lambda spark: read_hive_text_table(spark, table_name),
        cpu_fallback_class_name=get_non_gpu_allowed()[0],
        conf=hive_text_enabled_conf)


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text reads are disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@pytest.mark.parametrize('codec', ['BZip2Codec',    # BZ2 compression, i.e. Splittable.
                                   'DefaultCodec',  # DEFLATE, i.e. Gzip, without headers. Unsplittable.
                                   'GzipCodec'])    # Gzip proper. Unsplittable.
def test_read_compressed_hive_text(spark_tmp_table_factory, codec):
    """
    This tests whether compressed Hive Text tables are readable from spark-rapids.
    For GZIP/DEFLATE compressed tables, spark-rapids should not attempt to split the input files.
    Bzip2 compressed tables are splittable, and should remain readable.
    """

    table_name = spark_tmp_table_factory.get()

    def create_table_with_compressed_files(spark):
        spark.range(100000)\
            .selectExpr("id",
                        "cast(id as string) id_string")\
            .repartition(1).write.format("hive").saveAsTable(table_name)

    # Create Hive Text table with compression enabled.
    with_cpu_session(create_table_with_compressed_files,
                     {"hive.exec.compress.output": "true",
                      "mapreduce.output.fileoutputformat.compress.codec":
                          "org.apache.hadoop.io.compress.{}".format(codec)})

    # Attempt to read from table with very small (2KB) splits.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("SELECT COUNT(1) FROM {}".format(table_name)),
        conf={"spark.sql.files.maxPartitionBytes": "2048b"}
    )


# Hive Delimited Text writer tests follow.


TableWriteMode = Enum('TableWriteMode', ['CTAS', 'CreateThenWrite'])


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text is disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@approximate_float
@ignore_order(local=True)
@pytest.mark.parametrize('mode', [TableWriteMode.CTAS, TableWriteMode.CreateThenWrite])
@pytest.mark.parametrize('input_dir,schema,options', [
    ('hive-delim-text/simple-boolean-values', make_schema(BooleanType()),        {}),
    ('hive-delim-text/simple-int-values',     make_schema(ByteType()),           {}),
    ('hive-delim-text/simple-int-values',     make_schema(ShortType()),          {}),
    ('hive-delim-text/simple-int-values',     make_schema(IntegerType()),        {}),
    ('hive-delim-text/simple-int-values',     make_schema(LongType()),           {}),
    ('hive-delim-text/simple-int-values',     make_schema(FloatType()),          {}),
    ('hive-delim-text/simple-int-values',     make_schema(DoubleType()),         {}),
    ('hive-delim-text/simple-int-values',     make_schema(StringType()),         {}),
    pytest.param('hive-delim-text/simple-int-values', make_schema(DecimalType(10, 2)), {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    pytest.param('hive-delim-text/simple-int-values', make_schema(DecimalType(10, 3)),   {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    # Floating Point.
    ('hive-delim-text/simple-float-values',   make_schema(FloatType()),          {}),
    pytest.param('hive-delim-text/simple-float-values', make_schema(DecimalType(10, 3)), {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    pytest.param('hive-delim-text/simple-float-values', make_schema(DecimalType(38, 10)), {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    ('hive-delim-text/simple-float-values',   make_schema(IntegerType()),        {}),
    ('hive-delim-text/extended-float-values', make_schema(IntegerType()),        {}),
    ('hive-delim-text/extended-float-values', make_schema(FloatType()),          {}),
    ('hive-delim-text/extended-float-values', make_schema(DoubleType()),         {}),
    pytest.param('hive-delim-text/extended-float-values',   make_schema(DecimalType(10, 3)),   {},
                 marks=pytest.mark.xfail(reason="GPU supports more valid values than CPU. "
                                                "https://github.com/NVIDIA/spark-rapids/issues/7246")),
    pytest.param('hive-delim-text/extended-float-values',   make_schema(DecimalType(38, 10)),   {},
                 marks=pytest.mark.xfail(reason="GPU supports more valid values than CPU. "
                                                "https://github.com/NVIDIA/spark-rapids/issues/7246")),

    # Custom datasets
    ('hive-delim-text/Acquisition_2007Q3', acq_schema, {}),
    ('hive-delim-text/Performance_2007Q3', perf_schema, {}),
    ('hive-delim-text/trucks-1', trucks_schema, {}),
    ('hive-delim-text/trucks-err', trucks_schema, {}),

    # Date/Time
    pytest.param('hive-delim-text/timestamp', timestamp_schema, {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),
    pytest.param('hive-delim-text/date', date_schema, {},
                 marks=pytest.mark.xfail(condition=is_spark_cdh(),
                                         reason="https://github.com/NVIDIA/spark-rapids/issues/7423")),

    # Test that lines beginning with comments ('#') aren't skipped.
    ('hive-delim-text/comments', StructType([StructField("str", StringType()),
                                             StructField("num", IntegerType()),
                                             StructField("another_str", StringType())]), {}),

    # Test that carriage returns ('\r'/'^M') are treated similarly to newlines ('\n')
    ('hive-delim-text/carriage-return', StructType([StructField("str", StringType())]), {}),
    ('hive-delim-text/carriage-return-err', StructType([StructField("str", StringType())]), {}),
], ids=idfn)
@allow_non_gpu(*non_utc_allow_for_test_basic_hive_text_read)
def test_basic_hive_text_write(std_input_path, input_dir, schema, spark_tmp_table_factory, mode, options):
    basic_hive_text_write_test_impl(std_input_path, input_dir, schema, spark_tmp_table_factory, mode, options, hive_text_write_enabled_conf)


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text is disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@approximate_float
@ignore_order(local=True)
@pytest.mark.parametrize('mode', [TableWriteMode.CTAS, TableWriteMode.CreateThenWrite])
@pytest.mark.parametrize('input_dir,schema,options', [
    ('hive-delim-text/simple-int-values', make_schema(IntegerType()), {})
], ids=idfn)
@allow_non_gpu(*non_utc_allow_for_test_basic_hive_text_read)
def test_basic_hive_text_write_enabled_by_default(std_input_path, input_dir, schema, spark_tmp_table_factory, mode, options):
    basic_hive_text_write_test_impl(std_input_path, input_dir, schema, spark_tmp_table_factory, mode, options, conf={})


def basic_hive_text_write_test_impl(std_input_path, input_dir, schema, spark_tmp_table_factory, mode, options, conf):
    # Configure table options, including schema.
    if options is None:
        options = {}
    opts = options
    if schema is not None:
        opts = copy_and_update(options, {'schema': schema})

    # Initialize data path.
    data_path = std_input_path + "/" + input_dir

    def create_input_table(spark):
        input_table_name = spark_tmp_table_factory.get()
        spark.catalog.createExternalTable(input_table_name, source='hive', path=data_path, **opts)
        return input_table_name

    input_table = with_cpu_session(create_input_table)

    def write_table_sql(spark, table_name):
        if mode == TableWriteMode.CTAS:
            return [
                "CREATE TABLE {} SELECT * FROM {}".format(table_name, input_table)
            ]
        elif mode == TableWriteMode.CreateThenWrite:
            return [
                "CREATE TABLE {} LIKE {}".format(table_name, input_table),

                "INSERT OVERWRITE TABLE {} "
                " SELECT * FROM {} ".format(table_name, input_table)
            ]

    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory,
        write_table_sql,
        conf=conf)


PartitionWriteMode = Enum('PartitionWriteMode', ['Static', 'Dynamic'])


@pytest.mark.skipif(is_spark_cdh(),
                    reason="Hive text is disabled on CDH, as per "
                           "https://github.com/NVIDIA/spark-rapids/pull/7628")
@ignore_order(local=True)
@pytest.mark.parametrize('mode', [PartitionWriteMode.Static, PartitionWriteMode.Dynamic])
def test_partitioned_hive_text_write(mode, spark_tmp_table_factory):
    def create_input_table(spark):
        tmp_input = spark_tmp_table_factory.get()
        spark.sql("CREATE TABLE " + tmp_input +
                  " (make STRING, model STRING, year INT, type STRING, comment STRING)" +
                  " STORED AS TEXTFILE")
        spark.sql("INSERT INTO TABLE " + tmp_input + " VALUES " +
                  "('Ford',   'F-150',       2020, 'ICE',      'Popular' ),"
                  "('GMC',    'Sierra 1500', 1997, 'ICE',      'Older'),"
                  "('Chevy',  'D-Max',       2015, 'ICE',      'Isuzu?' ),"
                  "('Tesla',  'CyberTruck',  2025, 'Electric', 'BladeRunner'),"
                  "('Rivian', 'R1T',         2022, 'Electric', 'Heavy'),"
                  "('Jeep',   'Gladiator',   2024, 'Hybrid',   'Upcoming')")
        return tmp_input

    input_table = with_cpu_session(create_input_table)

    def write_partitions_sql(spark, output_table):
        if mode == PartitionWriteMode.Static:
            return [
                "CREATE TABLE {} "
                " (make STRING, model STRING, year INT, comment STRING)"
                " PARTITIONED BY (type STRING) STORED AS TEXTFILE".format(output_table),

                "INSERT INTO TABLE {} PARTITION (type='ICE')"
                " SELECT make, model, year, comment FROM {} "
                " WHERE type='ICE'".format(output_table, input_table),

                "INSERT OVERWRITE TABLE {} PARTITION (type='Electric')"
                " SELECT make, model, year, comment FROM {} "
                " WHERE type='Electric'".format(output_table, input_table),

                # Second (over)write to the same "Electric" partition.
                "INSERT OVERWRITE TABLE {} PARTITION (type='Electric')"
                " SELECT make, model, year, comment FROM {} "
                " WHERE type='Electric'".format(output_table, input_table),

                "INSERT INTO TABLE " + output_table + " PARTITION (type='Hybrid')" +
                " SELECT make, model, year, comment FROM " + input_table +
                " WHERE type='Hybrid'",
                ]
        elif mode == PartitionWriteMode.Dynamic:
            return [
                "CREATE TABLE " + output_table +
                " (make STRING, model STRING, year INT, comment STRING)"
                " PARTITIONED BY (type STRING) STORED AS TEXTFILE",

                "INSERT OVERWRITE TABLE " + output_table +
                " SELECT make, model, year, comment, type FROM " + input_table,

                # Second (over)write to only the "Electric" partition.
                "INSERT OVERWRITE TABLE " + output_table +
                " SELECT make, model, year, comment, type FROM " + input_table +
                " WHERE type = 'Electric'"
            ]
        else:
            raise Exception("Unsupported PartitionWriteMode {}".format(mode))

    assert_gpu_and_cpu_sql_writes_are_equal_collect(
        spark_tmp_table_factory,
        write_partitions_sql,
        conf={"hive.exec.dynamic.partition.mode": "nonstrict",
              "spark.rapids.sql.format.hive.text.enabled": True,
              "spark.rapids.sql.format.hive.text.write.enabled": True}
    )
