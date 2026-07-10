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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, \
    assert_gpu_sql_fallback_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import allow_non_gpu, allow_non_gpu_conditional
from spark_session import is_before_spark_400, is_databricks173_or_later


####################################################################################################
# Gpu only supports StringType, aka StringType(collate = UTF8_BINARY, constraint = NoConstraint),
# does not support: collate = non-UTF8_BINARY
# does not support: varchar(constraint = MaxLength), char(constraint = FixedLength)
# does not support: Collate expression
####################################################################################################

# some non-UTF8_BINARY collations
_non_utf8_binary_collations = ["UNICODE", "UTF8_LCASE", "UNICODE_CI"]


# test Collate, currently does not have GPU version for Collate
@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
@allow_non_gpu("ProjectExec", "Collate", "ResolvedCollation")
def test_collate_expr_fallback():
    data_gen = [("c1", string_gen)]
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr("concat(collate(c1, 'UTF8_BINARY'), 'a')"),
        cpu_fallback_class_name="Collate")


# Concat does not support StringType with non-UTF8_BINARY collation
# Fallback reason, i.e. !Expression <Concat> concat(c1#3, c2#4) cannot run on GPU because \
# input expression AttributeReference c2#4 (StringType(UNICODE) is not supported); \
# expression Concat concat(c1#3, c2#4) produces an unsupported type StringType(UNICODE); \
# input expression AttributeReference c1#3 (StringType(UNICODE) is not supported)
@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
@pytest.mark.parametrize('collate_type', _non_utf8_binary_collations)
@allow_non_gpu("ProjectExec")
def test_collate_column_fallback(collate_type):
    data_gen = [("c1", StringGen(collation=collate_type)), ("c2", StringGen(collation=collate_type))]
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr("concat(c1, c2)"),
        cpu_fallback_class_name="Concat")


# Concat supports StringType with `UTF8_BINARY` collation
@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
def test_collate_column():
    data_gen = data_gen = [("c1", StringGen(collation='UTF8_BINARY')), ("c2", StringGen(collation='UTF8_BINARY'))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr("Concat(c1, c2)"))


# Test non-UTF8_BINARY string literal, it falls back
@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
@pytest.mark.parametrize('collate_type', _non_utf8_binary_collations)
@allow_non_gpu("ProjectExec")
def test_collate_literal_fallback(collate_type):
    data_gen = [("c1", string_gen)]
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr(f"concat(c1, '_tail' COLLATE {collate_type})"),
        cpu_fallback_class_name="Literal")


# Test UTF8_BINARY string literal, it supports.
@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
def test_collate_literal():
    data_gen = [("c1", string_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).selectExpr("concat(c1, '_tail' COLLATE UTF8_BINARY)"))


@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
@allow_non_gpu("SortAggregateExec", "SortExec", "ShuffleExchangeExec",)
@pytest.mark.parametrize('collate_type', _non_utf8_binary_collations)
def test_collate_count_fallback(collate_type):
    data_gen = [("c1", StringGen(collation=collate_type))]
    assert_gpu_sql_fallback_collect(
        lambda spark: gen_df(spark, data_gen),
        cpu_fallback_class_name="SortOrder",
        table_name="tab",
        sql="select c1, count(*) from tab group by c1",
        # Disable AQE temporarily until https://github.com/NVIDIA/spark-rapids/issues/14319 is resolved.
        conf={'spark.sql.adaptive.enabled': False})


@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
@allow_non_gpu("SortAggregateExec", "ShuffleExchangeExec", "SortExec", "ColumnarToRowExec", "FileSourceScanExec")
def test_collate_using_table_fallback(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()

    def setup_table(spark):
        spark.sql(f"CREATE TABLE {table}(c STRING COLLATE UTF8_LCASE) USING PARQUET")
        spark.sql(f"INSERT INTO {table} VALUES ('aaa')")

    with_cpu_session(setup_table)

    assert_gpu_sql_fallback_collect(
        lambda spark: gen_df(spark, [('dummy_col', int_gen)]),
        cpu_fallback_class_name="AttributeReference",
        table_name="dummy_table",
        sql=f"SELECT COUNT(*), c FROM {table} GROUP BY c")


@pytest.mark.skipif(is_before_spark_400(), reason="Spark versions before 400 do not support collate")
def test_collate_using_table(spark_tmp_table_factory):
    table = spark_tmp_table_factory.get()

    def setup_table(spark):
        spark.sql(f"CREATE TABLE {table}(c STRING COLLATE UTF8_BINARY) USING PARQUET")
        spark.sql(f"INSERT INTO {table} VALUES ('aaa')")

    with_cpu_session(setup_table)

    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, [('dummy_col', int_gen)]),
        table_name="dummy_table",
        sql=f"SELECT COUNT(*), c FROM {table} GROUP BY c")


# Test `preserveCharVarcharTypeInfo` is true; char/varchar type
# char:
#   The CPU plan for char is: Contains(static_invoke(CharVarcharCodegenUtils.readSidePadding(char_col#8, 5)), a).
#   Both scan and project fall back to CPU, because scan and project input do not support CharType, \
#     and more Gpu does not support `static_invoke`.
# varchar:
#   The CPU plan for varchar is: Contains(char_col#13, a)
#   Scan and project does not support varchar type
@pytest.mark.skipif(is_before_spark_400(),
                    reason="Spark 32x, 33x do not support char/varchar type; Spark 34x, 35x throw exception")
@pytest.mark.parametrize('char_type', ["char(5)", "varchar(5)"])
@allow_non_gpu("ProjectExec", "ColumnarToRowExec", "FileSourceScanExec")
def test_constraint_char_varchar_preserve_enabled_fallback(spark_tmp_path, char_type):
    preserve_char_conf = {"spark.sql.preserveCharVarcharTypeInfo": True}
    file_path = spark_tmp_path + '/PARQUET_DATA'
    data = [("a",), ("ab",), ("abc",)]
    schema = f"char_col {char_type}"

    # Writing with `preserveCharVarcharTypeInfo` enabled, so reading back keeps char/varchar type.
    # And also, set this config to avoid error: Logical plan should not have output of char/varchar
    # type when spark.sql.preserveCharVarcharTypeInfo is false
    with_cpu_session(
        lambda spark: spark.createDataFrame(data, schema).write.parquet(file_path),
        conf=preserve_char_conf)

    assert_gpu_fallback_collect(
        lambda spark: spark.read.parquet(file_path).selectExpr("contains(char_col, 'a')"),
        cpu_fallback_class_name="Contains",
        conf=preserve_char_conf)


# Test `preserveCharVarcharTypeInfo` is false(default value); char type
# Spark scan treats char as StringType and injects read-side padding in ProjectExec.
# `static_invoke` (readSidePadding) has no GPU implementation, so ProjectExec falls back.
@pytest.mark.skipif(is_before_spark_400(),
                    reason="Spark 32x, 33x do not support char/varchar type; Spark 34x, 35x throw exception")
@allow_non_gpu_conditional(not is_databricks173_or_later(), "ProjectExec")
def test_constraint_char_preserve_disabled_fallback(spark_tmp_path):
    preserve_char_conf = {"spark.sql.preserveCharVarcharTypeInfo": True}
    file_path = spark_tmp_path + '/PARQUET_DATA'
    data = [("a",), ("ab",), ("abc",)]
    schema = f"char_col char(5)"

    # Writing with `preserveCharVarcharTypeInfo` enabled, so reading back keeps char/varchar type.
    # And also, set this config to avoid error: Logical plan should not have output of char/varchar
    # type when spark.sql.preserveCharVarcharTypeInfo is false
    with_cpu_session(
        lambda spark: spark.createDataFrame(data, schema).write.parquet(file_path),
        conf=preserve_char_conf)

    # when read from the Parquet file with `preserveCharVarcharTypeInfo,
    # the char_col is still char/varchar type.
    read_func = lambda spark: spark.read.parquet(file_path).selectExpr(
        "contains(char_col, 'a')")

    if is_databricks173_or_later():
        # DB 17.3+ no longer injects StaticInvoke for char padding
        assert_gpu_and_cpu_are_equal_collect(read_func)
    else:
        assert_gpu_fallback_collect(read_func,
            cpu_fallback_class_name="ProjectExec")


# Test `preserveCharVarcharTypeInfo` is false(default value); varchar type
# Spark treats varchar as StringType, it's transparent to GPU, so this case can run on GPU.
@pytest.mark.skipif(is_before_spark_400(),
                    reason="Spark 32x, 33x do not support char/varchar type; Spark 34x, 35x throw exception")
@pytest.mark.parametrize('char_type', ["varchar(5)"])
def test_constraint_varchar_preserve_disabled(spark_tmp_path, char_type):
    preserve_char_conf = {"spark.sql.preserveCharVarcharTypeInfo": True}
    file_path = spark_tmp_path + '/PARQUET_DATA'
    data = [("a",), ("ab",), ("abc",)]
    schema = f"char_col {char_type}"

    # Writing with `preserveCharVarcharTypeInfo` enabled, so reading back keeps char/varchar type.
    # And also, set this config to avoid error: Logical plan should not have output of char/varchar
    # type when spark.sql.preserveCharVarcharTypeInfo is false
    with_cpu_session(
        lambda spark: spark.createDataFrame(data, schema).write.parquet(file_path),
        conf=preserve_char_conf)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.parquet(file_path).selectExpr("contains(char_col, 'a')"))
