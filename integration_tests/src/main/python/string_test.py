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
import random

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, \
    assert_gpu_sql_fallback_collect, assert_gpu_fallback_collect, assert_gpu_and_cpu_error, \
    assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import is_databricks_runtime, get_datagen_seed
from data_gen import *
from marks import *
from pyspark.sql.types import *
import pyspark.sql.utils
import pyspark.sql.functions as f
from spark_session import with_cpu_session, with_gpu_session, is_databricks104_or_later, is_databricks_version_or_later, is_before_spark_320, is_before_spark_330, is_spark_400_or_later, is_before_spark_340

_regexp_conf = { 'spark.rapids.sql.regexp.enabled': 'true' }

_gbk_edge_cases = [
    bytearray(b''),                        # empty
    bytearray(b'\xc4\xe3\xba\xc3'),       # "你好" in GBK
    bytearray(b'\xff\xff'),                # invalid lead bytes
    bytearray(b'\x81'),                    # truncated lead byte (no second byte)
    bytearray(b'\x81\xff'),               # lead + 0xFF (consumed as pair, maps to FFFD)
    bytearray(b'\x81\x30'),               # lead + invalid second byte (< 0x40)
    bytearray(b'Hi\xc4\xe3\xba\xc3World'), # mixed ASCII and Chinese
]

@pytest.mark.parametrize('data_gen', [
    # Random CJK ideographs encoded as GBK — tests normal decode path
    BinaryGen(min_length=0, max_length=50, min_val=0x4E00, max_val=0x9FA5, encoding='gbk'),
    # Raw random bytes — tests error handling and edge cases
    BinaryGen(min_length=0, max_length=50),
], ids=['gbk_chinese', 'random_bytes'])
def test_string_decode_gbk(data_gen):
    gen = data_gen
    for sc in _gbk_edge_cases:
        gen = gen.with_special_case(sc)
    # javaCharsets=true lets the CPU accept GBK (it is not in the default allow-list);
    # codingErrorAction=true keeps the CPU in REPLACE mode so U+FFFD matches the GPU.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=2048)
            .selectExpr("decode(a, 'GBK')"),
        conf={
            'spark.sql.legacy.javaCharsets': 'true',
            'spark.sql.legacy.codingErrorAction': 'true',
        })


# codingErrorAction=false is Spark 4.0+: malformed bytes raise MALFORMED_CHARACTER_CODING
# on the CPU, and the GPU must raise the same error instead of falling back.
@pytest.mark.skipif(not is_spark_400_or_later(), reason="codingErrorAction introduced in Spark 4.0")
def test_string_decode_gbk_report_mode_clean():
    # Only valid GBK pairs -> both CPU and GPU must succeed.
    gen = BinaryGen(min_length=0, max_length=50, min_val=0x4E00, max_val=0x9FA5, encoding='gbk')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=2048)
            .selectExpr("decode(a, 'GBK')"),
        conf={
            'spark.sql.legacy.javaCharsets': 'true',
            'spark.sql.legacy.codingErrorAction': 'false',
        })


@pytest.mark.skipif(not is_spark_400_or_later(), reason="codingErrorAction introduced in Spark 4.0")
def test_string_decode_gbk_report_mode_malformed():
    # Input with malformed GBK bytes -> CPU and GPU must both raise MALFORMED_CHARACTER_CODING.
    gen = BinaryGen(min_length=0, max_length=50)
    for sc in _gbk_edge_cases:
        gen = gen.with_special_case(sc)
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, gen, length=2048)
            .selectExpr("decode(a, 'GBK')").collect(),
        conf={
            'spark.sql.legacy.javaCharsets': 'true',
            'spark.sql.legacy.codingErrorAction': 'false',
        },
        error_message='MALFORMED_CHARACTER_CODING')

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

@pytest.mark.parametrize('empty_type', all_empty_string_types)
@pytest.mark.parametrize('num_splits', ['-1', '0', '1', '2'])
def test_split_input_all_empty(empty_type, num_splits):
    data_gen = mk_empty_str_gen(empty_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'split(a, "AB", ' + num_splits + ')',
                'split(a, "C", ' + num_splits + ')',
                'split(a, ">>", ' + num_splits + ')',
                'split(a, "_", ' + num_splits + ')'),
                conf=_regexp_conf)

def test_split_no_limit():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'split(a, "AB")',
                'split(a, "C")',
                'split(a, "_")'),
                conf=_regexp_conf)

def test_split_negative_limit():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "AB", -1)',
            'split(a, "C", -2)',
            'split(a, "_", -999)'),
            conf=_regexp_conf)

def test_split_zero_limit():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "AB", 0)',
            'split(a, "C", 0)',
            'split(a, "_", 0)'),
        conf=_regexp_conf)

def test_split_one_limit():
    data_gen = mk_str_gen('([ABC]{0,3}_?){1,7}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "AB", 1)',
            'split(a, "C", 1)',
            'split(a, "_", 1)'),
        conf=_regexp_conf)

def test_split_positive_limit():
    data_gen = mk_str_gen('([ABC]{0,3}_?){0,7}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "AB", 2)',
            'split(a, "C", 3)',
            'split(a, "_", 999)'))


@pytest.mark.parametrize('data_gen,delim', [(mk_str_gen('([ABC]{0,3}_?){0,7}'), '_'),
    (mk_str_gen('([MNP_]{0,3}\\.?){0,5}'), '.'),
    (mk_str_gen('([123]{0,3}\\^?){0,5}'), '^'),
    (mk_str_gen('([XYZ]{0,3}XYZ?){0,5}'), 'XYZ'),
    (mk_str_gen('([DEF]{0,3}DELIM?){0,5}'), 'DELIM')], ids=idfn)
def test_substring_index(data_gen,delim):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.substring_index(f.lit('123'), delim, 1),
                f.substring_index(f.col('a'), delim, 1),
                f.substring_index(f.col('a'), delim, 3),
                f.substring_index(f.col('a'), delim, 0),
                f.substring_index(f.col('a'), delim, -1),
                f.substring_index(f.col('a'), delim, -4)))


@allow_non_gpu('SubstringIndex')
@pytest.mark.parametrize('data_gen', [mk_str_gen('([ABC]{0,3}_?){0,7}')], ids=idfn)
def test_unsupported_fallback_substring_index(data_gen):
    delim_gen = StringGen(pattern="_")
    num_gen = IntegerGen(min_val=0, max_val=10, special_cases=[])

    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [("a", data_gen),
                           ("delim", delim_gen),
                           ("num", num_gen)], length=10).selectExpr(sql_text),
            "SubstringIndex")

    assert_gpu_did_fallback("SUBSTRING_INDEX(a, '_', num)")
    assert_gpu_did_fallback("SUBSTRING_INDEX(a, delim, 0)")
    assert_gpu_did_fallback("SUBSTRING_INDEX(a, delim, num)")
    assert_gpu_did_fallback("SUBSTRING_INDEX('a_b', '_', num)")
    assert_gpu_did_fallback("SUBSTRING_INDEX('a_b', delim, 0)")
    assert_gpu_did_fallback("SUBSTRING_INDEX('a_b', delim, num)")


# ONLY LITERAL WIDTH AND PAD ARE SUPPORTED
def test_lpad():
    gen = mk_str_gen('.{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'LPAD("literal", 2, " ")',
                'LPAD(a, 2, " ")',
                'LPAD(a, NULL, " ")',
                'LPAD(a, 5, NULL)',
                'LPAD(a, 5, "G")',
                'LPAD(a, -1, "G")'))


@allow_non_gpu('StringLPad')
def test_unsupported_fallback_lpad():
    gen = mk_str_gen('.{0,5}')
    pad_gen = StringGen(pattern="G")
    num_gen = IntegerGen(min_val=0, max_val=10, special_cases=[])

    def assert_gpu_did_fallback(sql_string):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [("a", gen),
                           ("len", num_gen),
                           ("pad", pad_gen)], length=10).selectExpr(sql_string),
            "StringLPad")

    assert_gpu_did_fallback('LPAD(a, 2, pad)')
    assert_gpu_did_fallback('LPAD(a, len, " ")')
    assert_gpu_did_fallback('LPAD(a, len, pad)')
    assert_gpu_did_fallback('LPAD("foo", 2, pad)')
    assert_gpu_did_fallback('LPAD("foo", len, " ")')
    assert_gpu_did_fallback('LPAD("foo", len, pad)')


# ONLY LITERAL WIDTH AND PAD ARE SUPPORTED
def test_rpad():
    gen = mk_str_gen('.{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'RPAD(a, 2, " ")',
                'RPAD(a, NULL, " ")',
                'RPAD(a, 5, NULL)',
                'RPAD(a, 5, "G")',
                'RPAD(a, -1, "G")'))


@allow_non_gpu('StringRPad')
def test_unsupported_fallback_rpad():
    gen = mk_str_gen('.{0,5}')
    pad_gen = StringGen(pattern="G")
    num_gen = IntegerGen(min_val=0, max_val=10, special_cases=[])

    def assert_gpu_did_fallback(sql_string):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [("a", gen),
                           ("len", num_gen),
                           ("pad", pad_gen)], length=10).selectExpr(sql_string),
            "StringRPad")

    assert_gpu_did_fallback('RPAD(a, 2, pad)')
    assert_gpu_did_fallback('RPAD(a, len, " ")')
    assert_gpu_did_fallback('RPAD(a, len, pad)')
    assert_gpu_did_fallback('RPAD("foo", 2, pad)')
    assert_gpu_did_fallback('RPAD("foo", len, " ")')
    assert_gpu_did_fallback('RPAD("foo", len, pad)')


# ONLY LITERAL SEARCH PARAMS ARE SUPPORTED
def test_position():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'POSITION(NULL IN a)',
                'POSITION("Z_" IN a)',
                'POSITION("" IN a)',
                'POSITION("_" IN a)',
                'POSITION("A" IN a)'))

def test_locate():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'locate("Z", a, -1)',
                'locate("Z", a, 4)',
                'locate("abc", "1abcd", 0)',
                'locate("abc", "1abcd", 10)',
                'locate("A", a, 500)',
                'locate("_", a, NULL)'))


@allow_non_gpu('StringLocate')
def test_unsupported_fallback_locate():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    pos_gen = IntegerGen()

    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [("a", gen), ("pos", pos_gen)], length=10).selectExpr(sql_text),
            'StringLocate')

    assert_gpu_did_fallback('locate(a, a, -1)')
    assert_gpu_did_fallback('locate("a", a, pos)')
    assert_gpu_did_fallback('locate(a, a, pos)')
    assert_gpu_did_fallback('locate(a, "a", pos)')

# There is no contains function exposed in older versions of Spark. You can turn it into a
# LIKE %FOO% or we have seen some use instr > 0 to do the same thing.
# Spark optimizes LIKE to be a contains, we also optimize instr to do
# something similar.
def test_instr_as_contains():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'instr(a, "A") > 0',
                '0 < instr(a, "A")',
                '1 <= instr(a, "A")',
                'instr(a, "A") >= 1',
                'a LIKE "%A%"'))

def test_instr():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'instr("A", "A")',
                'instr("a", "A")',
                'instr(a, "Z")',
                'instr(a, "A")',
                'instr(a, "_")',
                'instr(a, NULL)',
                'instr(NULL, "A")',
                'instr(NULL, NULL)'))


@allow_non_gpu('StringInstr')
def test_unsupported_fallback_instr():
    gen = mk_str_gen('.{0,3}Z_Z.{0,3}A.{0,3}')

    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            unary_op_df(spark, gen, length=10).selectExpr(sql_text),
            'StringInstr')

    assert_gpu_did_fallback('instr(a, a)')
    assert_gpu_did_fallback('instr("a", a)')


def test_contains():
    gen = mk_str_gen('.{0,3}Z?_Z?.{0,3}A?.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.lit('Z').contains('Z'),
                f.lit('foo').contains('Z_'),
                f.col('a').contains('Z'),
                f.col('a').contains('Z_'),
                f.col('a').contains(''),
                f.col('a').contains(None)))

def test_contains_col_search():
    gen = mk_str_gen('.{0,3}Z?_Z?.{0,3}A?.{0,3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen, length=10).select(
                f.lit('Z').contains(f.col('a')),
                f.col('a').contains(f.col('a'))))

def test_contains_col_search_with_nulls():
    src_gen = StringGen(pattern='[a-z]{0,5}').with_special_case('').with_special_case(None)
    search_gen = StringGen(pattern='[a-c]{0,3}').with_special_case('').with_special_case(None)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark, [('src', src_gen), ('search', search_gen)], length=100)
                .selectExpr(
                    'contains(src, search)',
                    'contains(src, "")',
                    'contains("abc", search)',
                    'contains(CAST(NULL AS STRING), search)',
                    'contains(src, CAST(NULL AS STRING))'))


@pytest.mark.parametrize('data_gen', [mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}'), StringGen('')])
def test_trim(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'TRIM(a)',
                'TRIM("Ab" FROM a)',
                'TRIM("A\ud720" FROM a)',
                'TRIM(BOTH NULL FROM a)',
                'TRIM("" FROM a)'))

@pytest.mark.parametrize('data_gen', [mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}'), StringGen('')])
def test_ltrim(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'LTRIM(a)',
                'LTRIM("Ab", a)',
                'TRIM(LEADING "A\ud720" FROM a)',
                'TRIM(LEADING NULL FROM a)',
                'TRIM(LEADING "" FROM a)'))

@pytest.mark.parametrize('data_gen', [mk_str_gen('[Ab \ud720]{0,3}A.{0,3}Z[ Ab]{0,3}'), StringGen('')])
def test_rtrim(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'RTRIM(a)',
                'RTRIM("Ab", a)',
                'TRIM(TRAILING "A\ud720" FROM a)',
                'TRIM(TRAILING NULL FROM a)',
                'TRIM(TRAILING "" FROM a)'))

@pytest.mark.skipif(condition=is_spark_400_or_later() or is_databricks_version_or_later(14, 3),
                    reason="startsWith(None)/endswith(None) seems to cause an NPE in Column.fn() on Apache Spark 4.0, "
                           "and Databricks 14.3."
                           "See https://issues.apache.org/jira/browse/SPARK-48995.")
def test_startswith():
    gen = mk_str_gen('[Ab\ud720]{3}A.{0,3}Z[Ab\ud720]{3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.lit('foo').startswith('f'),
                f.lit('bar').startswith('1'),
                f.col('a').startswith('A'),
                f.col('a').startswith(''),
                f.col('a').startswith(None),
                f.col('a').startswith('A\ud720')))

@allow_non_gpu('StartsWith')
def test_unsupported_fallback_startswith():
    gen = StringGen(pattern='[a-z]')

    def assert_gpu_did_fallback(op):
        assert_gpu_fallback_collect(lambda spark:
            unary_op_df(spark, gen, length=10).select(op),
            'StartsWith')

    assert_gpu_did_fallback(f.lit("TEST").startswith(f.col("a")))
    assert_gpu_did_fallback(f.col("a").startswith(f.col("a")))


@pytest.mark.skipif(condition=is_spark_400_or_later() or is_databricks_version_or_later(14, 3),
                    reason="startsWith(None)/endswith(None) seems to cause an NPE in Column.fn() on Apache Spark 4.0, "
                           "and Databricks 14.3."
                           "See https://issues.apache.org/jira/browse/SPARK-48995.")
def test_endswith():
    gen = mk_str_gen('[Ab\ud720]{3}A.{0,3}Z[Ab\ud720]{3}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.lit('foo').startswith('f'),
                f.lit('bar').startswith('1'),
                f.col('a').endswith('A'),
                f.col('a').endswith(''),
                f.col('a').endswith(None),
                f.col('a').endswith('A\ud720')))


@allow_non_gpu('EndsWith')
def test_unsupported_fallback_endswith():
    gen = StringGen(pattern='[a-z]')

    def assert_gpu_did_fallback(op):
        assert_gpu_fallback_collect(lambda spark:
            unary_op_df(spark, gen, length=10).select(op),
            'EndsWith')

    assert_gpu_did_fallback(f.lit("TEST").endswith(f.col("a")))
    assert_gpu_did_fallback(f.col("a").endswith(f.col("a")))


def test_concat_ws_basic():
    gen = StringGen(nullable=True)
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(gen, 2, force_no_nulls=True))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: binary_op_df(spark, gen).select(
                f.concat_ws("-"),
                f.concat_ws("-", f.col('a')),
                f.concat_ws(None, f.col('a')),
                f.concat_ws("-", f.col('a'), f.col('b')),
                f.concat_ws("-", f.col('a'), f.lit('')),
                f.concat_ws("*", f.col('a'), f.col('b'), f.col('a')),
                f.concat_ws("*", s1, f.col('b')),
                f.concat_ws("+", f.col('a'), s2),
                f.concat_ws("-", f.lit(None), f.lit(None)),
                f.concat_ws("-", f.lit(None).cast('string'), f.col('b')),
                f.concat_ws("+", f.col('a'), f.lit(None).cast('string')),
                f.concat_ws(None, f.col('a'), f.col('b')),
                f.concat_ws("+", f.col('a'), f.lit(''))))

def test_concat_ws_arrays():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(gen, 2, force_no_nulls=True))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: binary_op_df(spark, gen).select(
                f.concat_ws("*", f.array(f.lit('2'), f.lit(''), f.lit('3'), f.lit('Z'))),
                f.concat_ws("*", s1, s2),
                f.concat_ws("-", f.array()),
                f.concat_ws("-", f.array(), f.lit('u')),
                f.concat_ws(None, f.lit('z'), s1, f.lit('b'), s2, f.array()),
                f.concat_ws("+", f.lit('z'), s1, f.lit('b'), s2, f.array()),
                f.concat_ws("*", f.col('b'), f.lit('z')),
                f.concat_ws("*", f.lit('z'), s1, f.lit('b'), s2, f.array(), f.col('b')),
                f.concat_ws("-", f.array(f.lit(None))),
                f.concat_ws("-", f.array(f.lit('')))))

def test_concat_ws_nulls_arrays():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: binary_op_df(spark, gen).select(
                f.concat_ws("*", f.lit('z'), f.array(f.lit('2'), f.lit(None), f.lit('Z'))),
                f.concat_ws("*", f.array(f.lit(None), f.lit(None))),
                f.concat_ws("*", f.array(f.lit(None), f.lit(None)), f.col('b'), f.lit('a'))))

def test_concat_ws_sql_basic():
    gen = StringGen(nullable=True)
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: binary_op_df(spark, gen),
            'concat_ws_table',
            'select ' +
                'concat_ws("-"), ' +
                'concat_ws("-", a), ' +
                'concat_ws(null, a), ' +
                'concat_ws("-", a, b), ' +
                'concat_ws("-", null, null), ' +
                'concat_ws("+", \'aaa\', \'bbb\', \'zzz\'), ' +
                'concat_ws(null, b, \'aaa\', \'bbb\', \'zzz\'), ' +
                'concat_ws("=", b, \'\', \'bbb\', \'zzz\'), ' +
                'concat_ws("*", b, a, cast(null as string)) from concat_ws_table')

def test_concat_ws_sql_col_sep():
    gen = StringGen(nullable=True)
    sep = StringGen('[-,*,+,!]', nullable=True)
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: three_col_df(spark, gen, gen, sep),
            'concat_ws_table',
            'select ' +
                'concat_ws(c, a), ' +
                'concat_ws(c, a, b), ' +
                'concat_ws(c, null, null), ' +
                'concat_ws(c, \'aaa\', \'bbb\', \'zzz\'), ' +
                'concat_ws(c, b, \'\', \'bbb\', \'zzz\'), ' +
                'concat_ws(c, b, a, cast(null as string)) from concat_ws_table')


@pytest.mark.skipif(is_databricks_runtime(),
    reason='Databricks optimizes out concat_ws call in this case')
@allow_non_gpu('ProjectExec', 'Alias', 'ConcatWs')
def test_concat_ws_sql_col_sep_only_sep_specified():
    gen = StringGen(nullable=True)
    sep = StringGen('[-,*,+,!]', nullable=True)
    assert_gpu_sql_fallback_collect(
            lambda spark: three_col_df(spark, gen, gen, sep),
            'ConcatWs',
            'concat_ws_table',
            'select ' +
                'concat_ws(c) from concat_ws_table')

def test_concat_ws_sql_arrays():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: three_col_df(spark, gen, gen, StringGen(nullable=True)),
            'concat_ws_table',
            'select ' +
                'concat_ws("-", array()), ' +
                'concat_ws(null, c, c, array(c)), ' +
                'concat_ws("-", array(), c), ' +
                'concat_ws("-", a, b), ' +
                'concat_ws("-", a, array(null, c), b, array()), ' +
                'concat_ws("-", array(null, null)), ' +
                'concat_ws("-", a, array(null), b, array()), ' +
                'concat_ws("*", array(\'2\', \'\', \'3\', \'Z\', c)) from concat_ws_table')

def test_concat_ws_sql_arrays_col_sep():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    sep = StringGen('[-,*,+,!]', nullable=True)
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: three_col_df(spark, gen, StringGen(nullable=True), sep),
            'concat_ws_table',
            'select ' +
                'concat_ws(c, array()) as emptyCon, ' +
                'concat_ws(c, b, b, array(b)), ' +
                'concat_ws(c, a, array(null, c), b, array()), ' +
                'concat_ws(c, array(null, null)), ' +
                'concat_ws(c, a, array(null), b, array()), ' +
                'concat_ws(c, array(\'2\', \'\', \'3\', \'Z\', b)) from concat_ws_table')

def test_concat_ws_sql_arrays_all_null_col_sep():
    gen = ArrayGen(StringGen(nullable=True), nullable=True)
    sep = NullGen()
    assert_gpu_and_cpu_are_equal_sql(
            lambda spark: three_col_df(spark, gen, StringGen(nullable=True), sep),
            'concat_ws_table',
            'select ' +
                'concat_ws(c, array(null, null)), ' +
                'concat_ws(c, a, array(null), b, array()), ' +
                'concat_ws(c, b, b, array(b)) from concat_ws_table')

def test_array_join():
    ar_gen = ArrayGen(StringGen("a{0,10}", nullable=True), max_length=3, nullable=True)
    sep_gen = StringGen("b{0,2}", nullable=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: two_col_df(spark, ar_gen, sep_gen).selectExpr("*",
                "array_join(a, '*')",
                "array_join(a, b)",
                "array_join(a, '**', 'WAS NULL')",
                "array_join(a, b, 'WAS NULL')",
                "array_join(a, null, 'WAS NULL')",
                "array_join(a, b, null)",
                "array_join(array('1', null, '3'), '*')",
                "array_join(array('1', null, '3'), b)",
                "array_join(array('1', null, '3'), '**', 'WAS NULL')",
                "array_join(array('1', null, '3'), b, 'WAS NULL')",
                "array_join(array('1', null, '3'), null, 'WAS NULL')",
                "array_join(array('1', null, '3'), b, null)",
                "array_join(null, '*')",
                "array_join(null, b)",
                "array_join(null, '**', 'WAS NULL')",
                "array_join(null, b, 'WAS NULL')",
                "array_join(null, null, 'WAS NULL')",
                "array_join(null, b, null)",
                "array_join(null, null, null)"))

def test_substring():
    gen = mk_str_gen('.{0,30}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'SUBSTRING(a, 1, 5)',
                'SUBSTRING(a, 5, 2147483647)',
                'SUBSTRING(a, 5, -2147483648)',
                'SUBSTRING(a, 1)',
                'SUBSTRING(a, -3)',
                'SUBSTRING(a, 3, -2)',
                'SUBSTRING(a, 100)',
                'SUBSTRING(a, -100)',
                'SUBSTRING(a, NULL)',
                'SUBSTRING(a, 1, NULL)',
                'SUBSTRING(a, -5, 0)',
                'SUBSTRING(a, -5, 4)',
                'SUBSTRING(a, 10, 0)',
                'SUBSTRING(a, -50, 10)',
                'SUBSTRING(a, -10, -1)',
                'SUBSTRING(a, 0, 10)',
                'SUBSTRING(a, 0, 0)'))

def test_substring_column():
    str_gen = mk_str_gen('.{0,30}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: three_col_df(spark, str_gen, int_gen, int_gen).selectExpr(
            'SUBSTRING(a, b, c)',
            'SUBSTRING(a, b, 0)',
            'SUBSTRING(a, b, 5)',
            'SUBSTRING(a, b, -5)',
            'SUBSTRING(a, b, 100)',
            'SUBSTRING(a, b, -100)',
            'SUBSTRING(a, b, NULL)',
            'SUBSTRING(a, 0, c)',
            'SUBSTRING(a, 5, c)',
            'SUBSTRING(a, -5, c)',
            'SUBSTRING(a, 100, c)',
            'SUBSTRING(a, -100, c)',
            'SUBSTRING(a, NULL, c)',
            'SUBSTRING(\'abc\', b, c)',
            'SUBSTRING(\'abc\', 1, c)',
            'SUBSTRING(\'abc\', 0, c)',
            'SUBSTRING(\'abc\', 5, c)',
            'SUBSTRING(\'abc\', -1, c)',
            'SUBSTRING(\'abc\', -5, c)',
            'SUBSTRING(\'abc\', NULL, c)',
            'SUBSTRING(\'abc\', b, 10)',
            'SUBSTRING(\'abc\', b, -10)',
            'SUBSTRING(\'abc\', b, 2)',
            'SUBSTRING(\'abc\', b, 0)',
            'SUBSTRING(\'abc\', b, NULL)',
            'SUBSTRING(\'abc\', b)',
            'SUBSTRING(a, b)'))

@pytest.mark.skipif(is_databricks_runtime() and not is_databricks104_or_later(),
                    reason="https://github.com/NVIDIA/spark-rapids/issues/7463")
def test_ephemeral_substring():
    str_gen = mk_str_gen('.{0,30}')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: three_col_df(spark, str_gen, int_gen, int_gen)\
            .filter("substr(a, 1, 3) > 'mmm'"))

def test_repeat_scalar_and_column():
    gen_s = StringGen(nullable=False)
    gen_r = IntegerGen(min_val=-100, max_val=100, special_cases=[0], nullable=True)
    (s,) = with_cpu_session(lambda spark: gen_scalars_for_sql(gen_s, 1))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen_r).selectExpr(
                'repeat({}, a)'.format(s),
                'repeat({}, null)'.format(s)))

def test_repeat_column_and_scalar():
    gen_s = StringGen(nullable=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen_s).selectExpr(
                'repeat(a, -10)',
                'repeat(a, 0)',
                'repeat(a, 10)',
                'repeat(a, null)'
            ))

def test_repeat_null_column_and_scalar():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.range(100).selectExpr('CAST(NULL as STRING) AS a').selectExpr(
                'repeat(a, -10)',
                'repeat(a, 0)',
                'repeat(a, 10)'
            ))

def test_repeat_column_and_column():
    gen_s = StringGen(nullable=True)
    gen_r = IntegerGen(min_val=-100, max_val=100, special_cases=[0], nullable=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: two_col_df(spark, gen_s, gen_r).selectExpr('repeat(a, b)'))

def test_replace():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'REPLACE("TEST", "TEST", "PROD")',
                'REPLACE("NO", "T\ud720", "PROD")',
                'REPLACE(a, "TEST", "PROD")',
                'REPLACE(a, "T\ud720", "PROD")',
                'REPLACE(a, "", "PROD")',
                'REPLACE(a, "T", NULL)',
                'REPLACE(a, NULL, "PROD")',
                'REPLACE(a, "T", "")'))


def test_replace_col_args_with_nulls():
    src_gen = StringGen(pattern='[a-z]{0,5}').with_special_case('')
    search_gen = StringGen(pattern='[a-c]{0,3}').with_special_case('')
    replace_gen = StringGen(pattern='[X-Z]{0,3}').with_special_case('')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: gen_df(spark,
                [('src', src_gen), ('search', search_gen), ('repl', replace_gen)], length=100)
                .selectExpr(
                    'REPLACE(src, search, repl)',
                    'REPLACE(src, search, "")',
                    'REPLACE(src, "", repl)',
                    'REPLACE("abcabc", search, repl)'))


@incompat
def test_translate():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'translate(a, "TEST", "PROD")',
                'translate(a, "TEST", "P")',
                'translate(a, "T\ud720", "PROD")',
                'translate(a, "", "PROD")',
                'translate(a, NULL, "PROD")',
                'translate(a, "TEST", NULL)',
                'translate("AaBbCc", "abc", "123")',
                'translate("AaBbCc", "abc", "1")'))

@incompat
@allow_non_gpu('StringTranslate')
def test_unsupported_fallback_translate():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            unary_op_df(spark, gen, length=10).selectExpr(sql_text),
            'StringTranslate')

    assert_gpu_did_fallback('TRANSLATE(a, "TEST", a)')
    assert_gpu_did_fallback('TRANSLATE(a, a, "TEST")')
    assert_gpu_did_fallback('TRANSLATE(a, a, a)')
    assert_gpu_did_fallback('TRANSLATE("TEST", "TEST", a)')
    assert_gpu_did_fallback('TRANSLATE("TEST", a, "TEST")')
    assert_gpu_did_fallback('TRANSLATE("TEST", a, a)')


@incompat
@pytest.mark.skipif(is_before_spark_320(), reason="Only in Spark 3.2+ does translate() support unicode \
    characters with code point >= U+10000. See https://issues.apache.org/jira/browse/SPARK-34094")
def test_translate_large_codepoints():
    gen = mk_str_gen('.{0,5}TEST[\ud720 \U0010FFFF A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'translate(a, "T\U0010FFFF", "PROD")'))

def test_length():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'LENGTH(a)',
                'CHAR_LENGTH(a)',
                'CHARACTER_LENGTH(a)'))

def test_byte_length():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'BIT_LENGTH(a)', 'OCTET_LENGTH(a)'))

def test_ascii():
    # StringGen will generate ascii and latin1 characters by default, which is the same as the supported
    # range of ascii in plugin. Characters outside of this range will return mismatched results.
    gen = mk_str_gen('.{0,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(f.ascii(f.col('a'))),
            conf={'spark.rapids.sql.expression.Ascii': True})

@incompat
def test_initcap():
    # Because we don't use the same unicode version we need to limit
    # the character set to something more reasonable
    # upper and lower should cover the corner cases, this is mostly to
    # see if there are issues with spaces
    gen = StringGen('([aAbB1357ȺéŸ_@%-]{0,15}[ \r\n\t]{1,2}){1,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.initcap(f.col('a'))))

@incompat
@pytest.mark.xfail(reason='Spark initcap will not convert ŉ to ʼN')
def test_initcap_special_chars():
    gen = mk_str_gen('ŉ([aAbB13ȺéŸ]{0,5}){1,5}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.initcap(f.col('a'))))

def test_like_null():
    gen = mk_str_gen('.{0,3}a[|b*.$\r\n]{0,2}c.{0,3}')\
            .with_special_pattern('.{0,3}oo.{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.col('a').like('_')))

def test_like():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).select(
                f.lit('_oo_').like('_oo_'),
                f.lit('_aa_').like('_oo_'),
                f.col('a').like('%o%'), # turned into contains
                f.col('a').like('%a%'), # turned into contains
                f.col('a').like(''), #turned into equals
                f.col('a').like('12345678'), #turned into equals
                f.col('a').like('\\%SystemDrive\\%\\\\Users%'),
                f.col('a').like('_'),
                f.col('a').like('_oo_'),
                f.col('a').like('_oo%'),
                f.col('a').like('%oo_'),
                f.col('a').like('_\u201c%'),
                f.col('a').like('_a[d]%'),
                f.col('a').like('_a(d)%'),
                f.col('a').like('_$'),
                f.col('a').like('_$%'),
                f.col('a').like('_._'),
                f.col('a').like('_?|}{_%'),
                f.col('a').like('%a{3}%')))

@allow_non_gpu('Like')
def test_unsupported_fallback_like():
    gen = StringGen('[a-z]')
    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            unary_op_df(spark, gen, length=10).selectExpr(sql_text),
            'Like')

    assert_gpu_did_fallback("'lit' like a")
    assert_gpu_did_fallback("a like a")


@allow_non_gpu('RLike')
def test_unsupported_fallback_rlike():
    gen = StringGen('\/lit\/')

    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            unary_op_df(spark, gen, length=10).selectExpr(sql_text),
            'RLike')

    assert_gpu_did_fallback("'lit' rlike a")
    assert_gpu_did_fallback("a rlike a")


def test_like_simple_escape():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a like "_a^d%" escape "c"',
                'a like "a_a" escape "c"',
                'a like "a%a" escape "c"',
                'a like "c_" escape "c"',
                'a like x "6162632325616263" escape "#"',
                'a like x "61626325616263" escape "#"'))

def test_like_complex_escape():
    gen = mk_str_gen('(\u20ac|\\w){0,3}a[|b*.$\r\n]{0,2}c\\w{0,3}')\
            .with_special_pattern('\\w{0,3}oo\\w{0,3}', weight=100.0)\
            .with_special_case('_')\
            .with_special_case('\r')\
            .with_special_case('\n')\
            .with_special_case('a{3}bar')\
            .with_special_case('12345678')\
            .with_special_case('12345678901234')\
            .with_special_case('%SystemDrive%\\Users\\John')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr(
                'a like x "256f6f5f"',
                'a like x "6162632325616263" escape "#"',
                'a like x "61626325616263" escape "#"',
                'a like ""',
                'a like "_oo_"',
                'a like "_oo%"',
                'a like "%oo_"',
                'a like "_\u20AC_"',
                'a like "\\%SystemDrive\\%\\\\\\\\Users%"',
                'a like "_oo"'),
            conf={'spark.sql.parser.escapedStringLiterals': 'true'})


format_number_gens = integral_gens + [DecimalGen(precision=7, scale=7), DecimalGen(precision=18, scale=0),
                                      DecimalGen(precision=18, scale=3), DecimalGen(precision=36, scale=5),
                                      DecimalGen(precision=36, scale=-5), DecimalGen(precision=38, scale=10),
                                      DecimalGen(precision=38, scale=-10),
                                      DecimalGen(precision=38, scale=30, special_cases=[Decimal('0.000125')]),
                                      DecimalGen(precision=38, scale=32, special_cases=[Decimal('0.000125')]),
                                      DecimalGen(precision=38, scale=37, special_cases=[Decimal('0.000125')])]

@pytest.mark.parametrize('data_gen', format_number_gens, ids=idfn)
def test_format_number_supported(data_gen):
    gen = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'format_number(a, -2)',
            'format_number(a, 0)',
            'format_number(a, 1)',
            'format_number(a, 5)',
            'format_number(a, 10)',
            'format_number(a, 13)',
            'format_number(a, 30)',
            'format_number(a, 100)')
    )

format_float_special_vals = [float('nan'), float('inf'), float('-inf'), 0.0, -0.0, 
                             1.1234543, 0.0000152, 0.0000252, 0.999999, 999990.0, 
                             0.001234, 0.00000078, 7654321.1234567]

@pytest.mark.parametrize('data_gen', [SetValuesGen(FloatType(),  format_float_special_vals), 
                                      SetValuesGen(DoubleType(), format_float_special_vals)], ids=idfn)
def test_format_number_float_special(data_gen):
    gen = data_gen
    cpu_results = with_cpu_session(lambda spark: unary_op_df(spark, gen).selectExpr(
            'format_number(a, 5)').collect())
    gpu_results = with_gpu_session(lambda spark: unary_op_df(spark, gen).selectExpr(
            'format_number(a, 5)').collect())
    for cpu, gpu in zip(cpu_results, gpu_results):
        assert cpu[0] == gpu[0]

def test_format_number_double_value():
    data_gen = DoubleGen(nullable=False, no_nans=True)
    cpu_results = list(map(lambda x: float(x[0].replace(",", "")), with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('format_number(a, 5)').collect())))
    gpu_results = list(map(lambda x: float(x[0].replace(",", "")), with_gpu_session(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('format_number(a, 5)').collect())))
    for cpu, gpu in zip(cpu_results, gpu_results):
        assert math.isclose(cpu, gpu, abs_tol=1.1e-5)

def test_format_number_float_value():
    data_gen = FloatGen(nullable=False, no_nans=True)
    cpu_results = list(map(lambda x: float(x[0].replace(",", "")), with_cpu_session(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('format_number(a, 5)').collect())))
    gpu_results = list(map(lambda x: float(x[0].replace(",", "")), with_gpu_session(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('format_number(a, 5)').collect())))
    for cpu, gpu in zip(cpu_results, gpu_results):
        assert math.isclose(cpu, gpu, rel_tol=1e-7) or math.isclose(cpu, gpu, abs_tol=1.1e-5)

# valid base range is [2, 36], to_base can be negative, out of range results nulls
# When base is 36, the valid alphabets are: [0-9], [a-z] and [A-Z]
@pytest.mark.parametrize('from_base,pattern',
                         [
                             pytest.param(10, r'-?[0-9]{1,18}',       id='from_10'),
                             pytest.param(16, r'-?[0-9a-fA-F]{1,15}', id='from_16'),
                             pytest.param(36, r'-?[0-9a-zA-Z]{1,11}', id='from_36')
                         ])
@pytest.mark.parametrize('to_base', [2, 10, 16, 21, 36, -2, -10, -16, -29, -33], ids=idfn)
def test_conv_with_more_valid_values(from_base, to_base, pattern):
    gen = [("str_col", mk_str_gen(pattern))]
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, gen),
        "tab",
        f"select " +
        f"conv('', 3, 35), " + # corner case: empty string
        f"conv(str_col, {from_base}, {to_base}), " +
        f"conv(null, {from_base}, {to_base}), " +
        f"conv(str_col, null,  {to_base}), " +
        f"conv(str_col, {from_base}, null), " +
        f"conv(' 101010FFCC', {from_base}, {to_base}) from tab")

# valid base range is [2, 36], to_base can be negative, out of range results nulls
# When base is 36, the valid alphabets are: [0-9], [a-z] and [A-Z]
@disable_ansi_mode
def test_conv_with_more_invalid_values():
    gen = [
        ("str_col", mk_str_gen(r'-?[0-9a-zA-Z]{1,15}')),
        ("from_col", IntegerGen(min_val=0, max_val=38)), # will generate invalid base: 0, 1, 37, 38
        ("to_col", IntegerGen(min_val=-38, max_val=38))] # will generate invalid base: 37, -37 ...
    data_gen_seed = get_datagen_seed()
    r = random.Random(data_gen_seed)
    # here use [0, 38] to test invalid from bases: 0, 1, 37 and 38
    from_base_scalar = r.randint(0, 38)
    # here use [-38, 38] to test invalid to bases: 0, 1, 37, 38, -1, -37, -38
    to_base_scalar = r.randint(-38, 38)
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, gen),
        "tab",
        f"select " +
        f"conv('1112222', 2, 10), " + # Spark will fold with constant value
        f"conv(str_col, from_col, to_col), " +
        f"conv(str_col, 0, 36), " + # fixed invalid from base: 0
        f"conv(str_col, 1, 36), " + # fixed invalid from base: 1
        f"conv(str_col, 2, -37), " + # fixed invalid to base: -37
        f"conv(str_col, 5, 38), " + # fixed invalid to base: 38
        f"conv(str_col, from_col, {to_base_scalar}), " +
        f"conv(str_col, {from_base_scalar}, to_col), " +
        f"conv(str_col, {from_base_scalar}, {to_base_scalar}), " +
        f"conv(null, from_col, to_col), " +
        f"conv(str_col, null, to_col), " +
        f"conv(str_col, from_col, null), " +
        f"conv(' 101010FFCC', from_col, to_col), " +
        f"conv('10 1010FFCC', from_col, {to_base_scalar}), " +
        f"conv('1010 10FFCC', {from_base_scalar}, to_col), " +
        f"conv('101010FF CC', {from_base_scalar}, {to_base_scalar}) from tab")

# valid base range is [2, 36], to_base can be negative, out of range results nulls
# When base is 36, the valid alphabets are: [0-9], [a-z] and [A-Z]
def test_conv_ansi_on():
    gen = [
        ("str_col", mk_str_gen(r'[0-9a-zA-Z]{1,8}')), # make sure no overflow
        # here use 38 to test invalid bases: 0, 1, 37 and 38
        ("from_col", IntegerGen(min_val=0, max_val=38)),
        ("to_col", IntegerGen(min_val=-38, max_val=38))]
    data_gen_seed = get_datagen_seed()
    r = random.Random(data_gen_seed)
    from_base_scalar = r.randint(0, 38)
    to_base_scalar = r.randint(-38, 38)
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, gen),
        "tab",
        f"select " +
        f"conv(str_col, from_col, to_col), " +
        f"conv(str_col, from_col, {to_base_scalar}), " +
        f"conv(str_col, {from_base_scalar}, to_col), " +
        f"conv(str_col, {from_base_scalar}, {to_base_scalar}), " +
        f"conv(' 101010FFCC', from_col, to_col), " +
        f"conv('10 1010FFCC', from_col, {to_base_scalar}), " +
        f"conv('1010 10FFCC', {from_base_scalar}, to_col), " +
        f"conv('101010FF CC', {from_base_scalar}, {to_base_scalar}) from tab",
        conf = {"spark.sql.ansi.enabled": True})

@pytest.mark.parametrize('ansi_enabled', [True, False], ids=idfn)
@pytest.mark.parametrize('overflow_value, from_base, to_base', [
    ('184467440737095515991', 10, 15),
    ('184467440737095515991', 10, -15),
    ('9223372036854775807', 36, -2),
    ('9223372036854775807', 36, -2)], ids=idfn)
def test_conv_overflow(ansi_enabled, overflow_value, from_base, to_base):
    ansi_conf = {"spark.sql.ansi.enabled": ansi_enabled}
    def _gen(spark):
        return spark.createDataFrame([(overflow_value,), ], 'str_col string')
    if (not is_before_spark_340()) and ansi_enabled:
        # For Sparks >= 340 and ansi mode is on, throw exception
        error = "Overflow in function conv()"
        assert_gpu_and_cpu_error(
            lambda spark: _gen(spark).selectExpr(
                f"conv(str_col, {from_base}, {to_base})").collect(),
            conf=ansi_conf,
            error_message=error)
    else:
        # Result in NULL
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: _gen(spark).selectExpr(f"conv(str_col, {from_base}, {to_base})"),
            conf=ansi_conf)

# This is to test negative to_base or input is negative long
def test_conv_negative_to_base_negative_value():
    def _query_conv(spark):
        data = [
            ('-18446744073709551599', 10, 16),
            ('-15', 10, 16),
            ('18446744073709551599', 10, -16)]
        schema = StructType([
            StructField("str_col", StringType()),
            StructField("from_base", IntegerType()),
            StructField("to_base", IntegerType())])
        return spark.createDataFrame(data, schema).selectExpr("conv(str_col, from_base, to_base)")
    assert_gpu_and_cpu_are_equal_collect(_query_conv)

def test_conv_other_types():
    gen = [
        ("input_int_col", IntegerGen()),
        ("from_col", IntegerGen(min_val=0, max_val=38)),
        ("to_col", IntegerGen(min_val=-38, max_val=38)),
        ("from_col_byte", ByteGen(min_val=0, max_val=38)),
        ("to_col_long", LongGen(min_val=-38, max_val=38)),
    ]
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, gen),
        "tab",
        "select " +
        "conv(input_int_col, from_col, to_col), " +
        "conv(input_int_col, from_col_byte, to_col), " +
        "conv(input_int_col, from_col, to_col_long)" +
        "from tab")

def test_conv_with_str_cv_all_nulls():
    def _gen_all_null_string(spark):
        data = [(None,), (None,), (None,), (None,)] # all null values
        schema = StructType([StructField("str_cv", StringType())])
        return spark.createDataFrame(data, schema)
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: _gen_all_null_string(spark),
        "tab",
        f"select conv(str_cv, 3, 5) from tab")

@pytest.mark.skipif(is_before_spark_330(), reason='contains is not exposed until 3.3.0')
@pytest.mark.parametrize('ansi', [True, False], ids=["ANSI", "NO_ANSI"])
def test_multi_contains_basic(ansi):
    data_gen = StringGen(r'\d{0,10}')
    conf={'spark.sql.ansi.enabled': ansi}
    assert_gpu_and_cpu_are_equal_collect(lambda spark:
            gen_df(spark, [('a', data_gen)]).selectExpr(
                'or(contains(a, "100"), contains(a, "200")) as result'),
            conf = conf)

@pytest.mark.skipif(is_before_spark_330(), reason='contains is not exposed until 3.3.0')
@pytest.mark.parametrize('ansi', [True, False], ids=["ANSI", "NO_ANSI"])
def test_multi_contains_conditional(ansi):
    """
    The point of this test is that case/when statements behave differently when an operation under
    them can have side effects. When this happens the combining code does not combine expressions
    that might not execute, becuase there could be exceptions thrown there too. So this purposely
    causes a case when some can be combined, but others cannot.
    """
    data_gen = StringGen(r'\d{0,10}')
    conf={'spark.sql.ansi.enabled': ansi}
    assert_gpu_and_cpu_are_equal_collect(lambda spark:
            gen_df(spark, [('a', data_gen)]).selectExpr(
                '''CASE
                    WHEN or(contains(a, "100"), contains(a, "101")) THEN 1
                    WHEN contains(a, "200") THEN 2
                    WHEN contains(a, "300") THEN 3
                    ELSE CAST(a AS LONG)
                END as result'''),
            conf = conf)
