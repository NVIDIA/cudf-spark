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

import pytest

from asserts import (assert_cpu_and_gpu_are_equal_collect_with_capture,
                     assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect,
                     with_gpu_session)
from data_gen import *
from pyspark.sql.types import *
from marks import *
from spark_init_internal import spark_version
from spark_session import is_before_spark_400, is_databricks113_or_later, is_databricks_runtime

def mk_json_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

@pytest.mark.parametrize('json_str_pattern', [r'\{"store": \{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\], ' \
                   r'"bicycle":\{"price":[1-9]\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                   r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
                   r'\{"a": "[a-z]{1,3}"\}'], ids=idfn)
def test_get_json_object(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    scalar_json = '{"store": {"fruit": [{"name": "test"}]}}'
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
            'get_json_object(a,"$.a")',
            'get_json_object(a, "$.owner")',
            'get_json_object(a, "$.store.fruit[0]")',
            'get_json_object(\'%s\', "$.store.fruit[0]")' % scalar_json,
            ),
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

def test_get_json_object_quoted_index():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"a":"A"}'],
            [r'{"b":"B"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
        f.get_json_object('jsonStr',r'''$['a']''').alias('sub_a'),
        f.get_json_object('jsonStr',r'''$['b']''').alias('sub_b')))

@pytest.mark.skipif(is_databricks_runtime() and not is_databricks113_or_later(), reason="get_json_object on \
                    DB 10.4 shows incorrect behaviour with single quotes")
def test_get_json_object_single_quotes():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'''{'a':'A'}''']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
        f.get_json_object('jsonStr',r'''$['a']''').alias('sub_a'),
        f.get_json_object('jsonStr',r'''$['b']''').alias('sub_b'),
        f.get_json_object('jsonStr',r'''$['c']''').alias('sub_c')))

@pytest.mark.parametrize('query',["$.store.bicycle",
    "$['store'].bicycle",
    "$.store['bicycle']",
    "$['store']['bicycle']",
    "$['key with spaces']",
    "$.store.book",
    "$.store.book[0]",
    "$",
    "$.store.book[0].category",
    "$.store.basket[0][1]",
    "$.store.basket[0][2].b",
    "$.zip code",
    "$.fb:testid",
    "$.a",
    "$.non_exist_key",
    "$..no_recursive",
    "$.store.book[0].non_exist_key",
    "$.store.basket[0][*].b", 
    "$.store.book[*].reader",
    "$.store.book[*]",
    "$.store.book[*].category",
    "$.store.book[*].isbn",
    "$.store.basket[*]",
    "$.store.basket[*][0]",
    "$.store.basket[0][*]",
    "$.store.basket[*][*]",
    "$.store.basket[*].non_exist_key"])
def test_get_json_object_spark_unit_tests(query):
    schema = StructType([StructField("jsonStr", StringType())])
    data = [
            ['''{"store":{"fruit":[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}],"basket":[[1,2,{"b":"y","a":"x"}],[3,4],[5,6]],"book":[{"author":"Nigel Rees","title":"Sayings of the Century","category":"reference","price":8.95},{"author":"Herman Melville","title":"Moby Dick","category":"fiction","price":8.99,"isbn":"0-553-21311-3"},{"author":"J. R. R. Tolkien","title":"The Lord of the Rings","category":"fiction","reader":[{"age":25,"name":"bob"},{"age":26,"name":"jack"}],"price":22.99,"isbn":"0-395-19395-8"}],"bicycle":{"price":19.95,"color":"red"}},"email":"amy@only_for_json_udf_test.net","owner":"amy","zip code":"94025","fb:testid":"1234"}'''],
            ['''{ "key with spaces": "it works" }'''],
            ['''{"a":"b\nc"}'''],
            ['''{"a":"b\"c"}'''],
            ["\u0000\u0000\u0000A\u0001AAA"],
            ['{"big": "' + ('x' * 3000) + '"}']]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr', query)))

def test_get_json_object_normalize_non_string_output():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[' { "a": "A" } '],
            ['''{'a':'A"'}'''],
            [r'''{'a':"B\'"}'''],
            ['''['a','b','"C"']'''],
            ['[100.0,200.000,351.980]'],
            ['[12345678900000000000.0]'],
            ['[12345678900000000000]'],
            ['[1' + '0'* 400 + ']'],
            ['[1E308]'],
            ['[1.0E309,-1E309,1E5000]'],
            ['[true,false]'],
            ['[100,null,10]'],
            ['{"a":"A","b":null}']]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$')))

def test_get_json_object_quoted_question():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"?":"QUESTION"}']]

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr',r'''$['?']''').alias('question')),
        exist_classes='GpuGetJsonObject')


def test_multi_get_json_object_quoted_question():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"?":"QUESTION","a?b":"EMBEDDED","outer":{"?":"NESTED"}}']]

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr',r'''$['?']''').alias('question'),
            f.get_json_object('jsonStr',r'''$['a?b']''').alias('embedded'),
            f.get_json_object('jsonStr',r'''$.outer['?']''').alias('nested')),
        exist_classes='GpuProjectExec,GpuGetJsonObject')


def test_multi_get_json_object_all_invalid_paths():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":"A"}']]

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data,schema=schema).selectExpr(
            'get_json_object(jsonStr, CAST(NULL AS STRING)) AS null_path',
            'get_json_object(jsonStr, "$[") AS malformed_path',
            'get_json_object(jsonStr, "not_a_path") AS missing_root'),
        exist_classes='GpuProjectExec,GpuGetJsonObject')


def test_get_json_object_escaped_string_data():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"a":"A\"B"}'],
            [r'''{"a":"A\'B"}'''],
            [r'{"a":"A\/B"}'],
            [r'{"a":"A\\B"}'],
            [r'{"a":"A\bB"}'],
            [r'{"a":"A\fB"}'],
            [r'{"a":"A\nB"}'],
            [r'{"a":"A\tB"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).selectExpr('get_json_object(jsonStr,"$.a")'))

def test_get_json_object_escaped_key():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [
            [r'{"a\"":"Aq"}'],
            [r'''{"\'a":"sqA1"}'''],
            [r'''{"'a":"sqA2"}'''],
            [r'{"a\/":"Afs"}'],
            [r'{"a\\":"Abs"}'],
            [r'{"a\b":"Ab1"}'],
            ['{"a\b":"Ab2"}'],
            [r'{"a\f":"Af1"}'],
            ['{"a\f":"Af2"}'],
            [r'{"a\n":"An1"}'],
            ['{"a\n":"An2"}'],
            [r'{"a\t":"At1"}'],
            ['{"a\t":"At2"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', r'$.a\"').alias('qaq1'),
            f.get_json_object('jsonStr', '$.a"').alias('qaq2'),
            f.get_json_object('jsonStr', r'''$.\'a''').alias('qsqa1'),
            f.get_json_object('jsonStr', r'$.a\/').alias('qafs1'),
            f.get_json_object('jsonStr', '$.a/').alias('qafs2'), 
            f.get_json_object('jsonStr', r'''$['a\/']''').alias('qafs3'), 
            f.get_json_object('jsonStr', r'$.a\\').alias('qabs1'),
            f.get_json_object('jsonStr', r'$.a\b').alias('qab1'),
            f.get_json_object('jsonStr','$.a\b').alias('qab2'),
            f.get_json_object('jsonStr', r'$.a\f').alias('qaf1'),
            f.get_json_object('jsonStr','$.a\f').alias('qaf2'),
            f.get_json_object('jsonStr', r'$.a\n').alias('qan1'),
            f.get_json_object('jsonStr','$.a\n').alias('qan2'),
            f.get_json_object('jsonStr', r'$.a\t').alias('qat1'),
            f.get_json_object('jsonStr','$.a\t').alias('qat2')
            ))

def test_get_json_object_invalid_path():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":"A"}'],
            [r'{"a\"":"A"}'],
            [r'''{"'a":"A"}'''],
            ['{"b":"B"}'],
            ['["A","B"]'],
            ['{"c":["A","B"]}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '''$ ['a']''').alias('with_space'),
            f.get_json_object('jsonStr', r'''$['\'a']''').alias('qsqa2'),
            f.get_json_object('jsonStr', '''$.'a''').alias('qsqa2'),
            f.get_json_object('jsonStr', r'''$.['a\"']''').alias('qaq3'),
            f.get_json_object('jsonStr', '''$['a]''').alias('qsqa2'), # jsonpath.com thinks it is fine and ignores uncompleted ' and ], but not Spark
            f.get_json_object('jsonStr', 'a').alias('just_a'),
            f.get_json_object('jsonStr', '[-1]').alias('neg_one_index'),
            f.get_json_object('jsonStr', '$.c[-1]').alias('c_neg_one_index'),
            ))

def test_get_json_object_top_level_array_notation():
    # This is a special version of invalid path. It is something that the GPU supports
    # but the CPU thinks is invalid
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['["A","B"]'],
            ['{"a":"A","b":"B"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '[0]').alias('zero_index'),
            f.get_json_object('jsonStr', '$[1]').alias('one_index'),
            f.get_json_object('jsonStr', '''['a']''').alias('sub_a'),
            f.get_json_object('jsonStr', '''$['b']''').alias('sub_b'),
            ))

def test_get_json_object_unquoted_array_notation():
    # This is a special version of invalid path. It is something that the GPU supports
    # but the CPU thinks is invalid
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":"A","b":"B"}'],
            ['{"1":"ONE","a1":"A_ONE"}']]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$[a]').alias('a_index'),
            f.get_json_object('jsonStr', '$[1]').alias('one_index'),
            f.get_json_object('jsonStr', '''$['1']''').alias('quoted_one_index'),
            f.get_json_object('jsonStr', '$[a1]').alias('a_one_index')))


def test_get_json_object_white_space_removal():
    # This is a special version of invalid path. It is something that the GPU supports
    # but the CPU thinks is invalid
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{" a":" A"," b":" B"}'],
            ['{"a":"A","b":"B"}'],
            ['{"a ":"A ","b ":"B "}'],
            ['{" a ":" A "," b ":" B "}'],
            ['{" a ": {" a ":" A "}," b ": " B "}'],
            ['{" a":"b","a.a":"c","b":{"a":"ab"}}'],
            ['{" a":"b"," a. a":"c","b":{"a":"ab"}}'],
            ['{" a":"b","a .a ":"c","b":{"a":"ab"}}'],
            ['{" a":"b"," a . a ":"c","b":{"a":"ab"}}']
            ]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$.a').alias('dot_a'),
            f.get_json_object('jsonStr', '$. a').alias('dot_space_a'),
            f.get_json_object('jsonStr', '$.\ta').alias('dot_tab_a'),
            f.get_json_object('jsonStr', '$.    a').alias('dot_spaces_a3'),
            f.get_json_object('jsonStr', '$.a ').alias('dot_a_space'),
            f.get_json_object('jsonStr', '$. a ').alias('dot_space_a_space'),
            f.get_json_object('jsonStr', "$['b']").alias('dot_b'),
            f.get_json_object('jsonStr', "$[' b']").alias('dot_space_b'),
            f.get_json_object('jsonStr', "$['b ']").alias('dot_b_space'),
            f.get_json_object('jsonStr', "$[' b ']").alias('dot_space_b_space'),
            f.get_json_object('jsonStr', "$. a. a").alias('dot_space_a_dot_space_a'),
            f.get_json_object('jsonStr', "$.a .a ").alias('dot_a_space_dot_a_space'),
            f.get_json_object('jsonStr', "$. a . a ").alias('dot_space_a_space_dot_space_a_space'),
            f.get_json_object('jsonStr', "$[' a. a']").alias('space_a_dot_space_a'),
            f.get_json_object('jsonStr', "$['a .a ']").alias('a_space_dot_a_space'),
            f.get_json_object('jsonStr', "$[' a . a ']").alias('space_a_space_dot_space_a_space'),
            ))

def test_get_json_object_jni_java_tests():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['\'abc\''],
            ['[ [11, 12], [21, [221, [2221, [22221, 22222]]]], [31, 32] ]'],
            ['123'],
            ['{ \'k\' : \'v\'  }'],
            ['[  [[[ {\'k\': \'v1\'} ], {\'k\': \'v2\'}]], [[{\'k\': \'v3\'}], {\'k\': \'v4\'}], {\'k\': \'v5\'}  ]'],
            ['[1, [21, 22], 3]'],
            ['[ {\'k\': [0, 1, 2]}, {\'k\': [10, 11, 12]}, {\'k\': [20, 21, 22]}  ]'],
            ['[ [0], [10, 11, 12], [2] ]'],
            ['[[0, 1, 2], [10, [111, 112, 113], 12], [20, 21, 22]]'],
            ['[[0, 1, 2], [10, [], 12], [20, 21, 22]]'],
            ['{\'k\' : [0,1,2]}'],
            ['{\'k\' : null}']
            ]

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.col('jsonStr'),
            f.get_json_object('jsonStr', '$').alias('dollor'),
            f.get_json_object('jsonStr', '$[*][*]').alias('s_w_s_w'),
            f.get_json_object('jsonStr', '$.k').alias('dot_k'),
            f.get_json_object('jsonStr', '$[*]').alias('s_w'),
            f.get_json_object('jsonStr', '$[*].k[*]').alias('s_w_k_s_w'),
            f.get_json_object('jsonStr', '$[1][*]').alias('s_1_s_w'),
            f.get_json_object('jsonStr', "$[1][1][*]").alias('s_1_s_1_s_w'),
            f.get_json_object('jsonStr', "$.k[1]").alias('dot_k_s_1'),
            f.get_json_object('jsonStr', "$.*").alias('w'),
            ))

def test_get_json_object_deep_nested_json():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":{"m":{"n":{"o":{"p":{"q":{"r":{"s":{"t":{"u":{"v":{"w":{"x":{"y":{"z":"A"}}'
            ]]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr', '$.a.b.c.d.e.f.g.h.i').alias('i'),
            f.get_json_object('jsonStr', '$.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p').alias('p')
            ))

@allow_non_gpu('GetJsonObject')
def test_get_json_object_deep_nested_json_fallback():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":{"m":{"n":{"o":{"p":{"q":{"r":{"s":{"t":{"u":{"v":{"w":{"x":{"y":{"z":"A"}}'
            ]]
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(data,schema=schema).select(
            f.get_json_object('jsonStr', '$.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z').alias('z')),
        'GetJsonObject')

@allow_non_gpu('GetJsonObject')
@pytest.mark.parametrize('json_str_pattern', [r'\{"store": \{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\], ' \
                   r'"bicycle":\{"price":[1-9]\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                   r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
                   r'\{"a": "[a-z]{1,3}"\}'], ids=idfn)
def test_unsupported_fallback_get_json_object(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    scalar_json = '{"store": {"fruit": "test"}}'
    pattern = StringGen(pattern=r'\$\.[a-z]{1,9}')
    def assert_gpu_did_fallback(sql_text):
        assert_gpu_fallback_collect(lambda spark:
            gen_df(spark, [('a', gen), ('b', pattern)], length=10).selectExpr(sql_text),
        'GetJsonObject',
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

    assert_gpu_did_fallback('get_json_object(a, b)')
    assert_gpu_did_fallback('get_json_object(\'%s\', b)' % scalar_json)

@pytest.mark.parametrize('data_gen', [StringGen(r'''-?[1-9]\d{0,5}\.\d{1,20}''', nullable=False),
                                      StringGen(r'''-?[1-9]\d{0,20}\.\d{1,5}''', nullable=False),
                                      StringGen(r'''-?[1-9]\d{0,5}E-?\d{1,20}''', nullable=False),
                                      StringGen(r'''-?[1-9]\d{0,20}E-?\d{1,5}''', nullable=False)], ids=idfn)
def test_get_json_object_floating_normalization(data_gen):
    normalization = lambda spark: unary_op_df(spark, data_gen).selectExpr(
                        'a',
                        'get_json_object(a,"$")'
                        ).collect()
    gpu_res = [[row[1]] for row in with_gpu_session(
        normalization)]
    cpu_res = [[row[1]] for row in with_cpu_session(normalization)]
    def json_string_to_float(x):
        if x == '"-Infinity"':
            return float('-inf')
        elif x == '"Infinity"':
            return float('inf')
        else:
            return float(x)
    for i in range(len(gpu_res)):
        # verify relatively diff < 1e-9 (default value for is_close)
        assert math.isclose(json_string_to_float(gpu_res[i][0]), json_string_to_float(cpu_res[i][0]))


@pytest.mark.parametrize('ansi', [True, False], ids=["ANSI", "NO_ANSI"])
def test_multi_get_json_object_basic(ansi):
    data_gen = StringGen(r'''\{"num_a":[1-9]\d{0,5},"num_b":[1-9]\d{0,5}\}''')
    conf={'spark.sql.ansi.enabled': ansi}
    assert_gpu_and_cpu_are_equal_collect(lambda spark:
            gen_df(spark, [('jsonStr', data_gen)]).selectExpr(
                'CAST(get_json_object(jsonStr, "$.num_a") AS INTEGER) / CAST(get_json_object(jsonStr, "$.num_b") AS INTEGER) as result'),
            conf = conf)

@pytest.mark.parametrize('ansi', [True, False], ids=["ANSI", "NO_ANSI"])
def test_multi_get_json_object_conditional(ansi):
    """
    The point of this test is that case/when statements behave differently when an operation under
    them can have side effects. When this happens the combining code does not combine expressions
    that might not execute, because there could be exceptions thrown there too. So this purposely
    causes a case when some can be combined, but others cannot.
    """
    data_gen = StringGen(r'''\{"num_a":[1-9]\d{0,5},"num_b":[1-9]\d{0,5},"num_c":[1-9]\d{0,5}\}''')
    conf={'spark.sql.ansi.enabled': ansi}
    assert_gpu_and_cpu_are_equal_collect(lambda spark:
            gen_df(spark, [('jsonStr', data_gen)]).selectExpr(
                '''CASE
                    WHEN CAST(get_json_object(jsonStr, "$.num_a") AS INTEGER) / CAST(get_json_object(jsonStr, "$.num_b") AS INTEGER) > 0.5 THEN 1
                    WHEN CAST(get_json_object(jsonStr, "$.num_c") AS INTEGER) / CAST(get_json_object(jsonStr, "$.num_b") AS INTEGER) > 0.5 THEN 2
                    ELSE 3
                END as result'''),
            conf = conf)

# get_json_object UTF-16 surrogate and unicode-escape handling, pinned against vanilla-Spark CPU.
# The query selects the code path: '$' re-serializes through Jackson writeString (ESCAPED) while
# '$.a' returns a scalar leaf through writeRaw (UNESCAPED). The capture variant is used so a silent
# CPU fallback, which would make GPU == CPU trivially, fails the test instead of passing it.

def test_get_json_object_escaped_unicode_reescape():
    """ESCAPED path: a unicode escape decoding to a quote, backslash, or control character must be
    re-escaped when query '$' re-serializes the enclosing structure."""
    schema = StructType([StructField("jsonStr", StringType())])
    # Raw strings keep the literal escape text, so the parser sees it verbatim.
    data = [[r'{"a":"\u0022"}'],   # decodes to a quote, re-escaped as a quote escape
            [r'{"a":"\u005c"}'],   # decodes to a backslash, re-escaped as a double backslash
            [r'["\u0001"]'],       # control character with no short escape form
            [r'{"a":"\u0000"}'],   # NUL, the low boundary of the control range
            [r'{"a":"\u000a"}']]   # newline, re-escaped using its short form
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_escaped_surrogate_uppercase_escape():
    """ESCAPED path: every UTF-16 surrogate code unit, whether a valid pair or lone or broken, is
    re-emitted as an uppercase escape when query '$' re-serializes the structure."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"a":"\uD83D\uDE00"}'],     # valid surrogate pair
            [r'{"a":"\uD800"}'],           # lone high surrogate
            [r'{"a":"\uDC00"}'],           # lone low surrogate
            [r'{"a":"\uD800A"}']]          # high surrogate followed by a non-low character
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_unescaped_lone_surrogate_returns_null():
    """UNESCAPED path: a lone or broken surrogate in a scalar string leaf (query '$.a') nulls the
    row, because Jackson writeRaw throws on a split surrogate and Spark maps that to null."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"a":"\uD800"}'],      # lone high surrogate
            [r'{"a":"\uDC00"}'],      # lone low surrogate
            [r'{"a":"\uD800A"}'],     # high surrogate followed by a non-low character
            [r'{"a":"abc\uD800"}']]   # surrogate trailing valid text
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$.a')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_escaped_raw_astral_becomes_surrogate_pair():
    """ESCAPED path: a raw astral character (real 4-byte UTF-8, not an escape) is emitted as an
    uppercase surrogate-pair escape when query '$' re-serializes the structure."""
    schema = StructType([StructField("jsonStr", StringType())])
    # The actual U+1F600 astral character, not an escape sequence.
    data = [['{"a":"\U0001F600"}']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_unescaped_valid_astral_preserved():
    """UNESCAPED path guard: a valid astral character in a scalar leaf (query '$.a') is kept
    unchanged, written raw or as a valid escaped surrogate pair. Only lone or broken surrogates
    null the row, so nulling must not spill onto valid astral input."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":"\U0001F600"}'],        # raw astral character
            [r'{"a":"\uD83D\uDE00"}']]     # valid escaped surrogate pair, same character
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$.a')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_field_name_with_lone_surrogate_no_match():
    """Field-name guard: a key carrying an escaped lone surrogate plus 'a' is two UTF-16 code
    units and must not match the single-unit query 'a', so the surrogate is never skipped."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"\uD800a": 1}'],            # only key is the surrogate key, no match, null
            [r'{"\uD800a": 1, "a": 2}']]    # real "a" matches, surrogate key does not, so 2
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$.a')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_escaped_valid_bmp_unchanged():
    """ESCAPED path guard: a valid BMP unicode escape decodes to its character and is emitted raw
    (Jackson does not escape non-ASCII), so re-escaping must not over-escape ordinary BMP text."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"a":"\u4e2d"}']]   # decodes to U+4E2D, a CJK ideograph
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$')),
        exist_classes='GpuGetJsonObject')

# Malformed / boundary UTF-8 in the raw document bytes, which Spark's strict UTF-8 reader turns
# into U+FFFD before Jackson tokenizes. Inputs are carried as BinaryType and cast to string because
# a Python str cannot hold invalid UTF-8; both the CPU and GPU casts reinterpret without
# validating, so the malformed bytes reach get_json_object intact on either side.
_MALFORMED_UTF8_DOCS = [
    "7B2261223A22EDA080227D",          # cesu8_lone_high
    "7B2261223A22EDB080227D",          # cesu8_lone_low
    "7B2261223A22EDA0BDEDB880227D",    # cesu8_pair_emoji
    "7B2261223A22C080227D",            # overlong_2byte
    "7B2261223A22E08080227D",          # overlong_3byte
    "7B2261223A22E4B8227D",            # truncated_3byte
    "7B2261223A22F09F98227D",          # truncated_4byte
    "7B2261223A2280227D",              # bare_continuation
    "7B2261223A22F5227D",              # bad_lead_F5
    "7B2261223A22FF227D",              # bad_lead_FF
    "7B2261223A22ED9FBF227D",          # valid_below_surr
    "7B2261223A22F48FBFBF227D",        # valid_max_astral
    "7B2261223A22F4908080227D",        # above_10FFFF
    "7B2261223A2241EDA08042227D",      # mixed_A_bad_B
    "7B2261223A22E428AD227D",          # b2_not_cont
    "7B2261223A22E4B828227D",          # b3_not_cont
    "7B2261223A22E4B8AD227D",          # valid_3byte_cjk
    "7B2261223A22C228227D",            # c2_bad_cont
    "7B2261223A22F09F9828227D",        # f0_bad_b4
    "7B2261223A22F09F9880227D",        # valid_astral
]

def _malformed_utf8_df(spark):
    schema = StructType([StructField("jsonBin", BinaryType())])
    data = [[bytearray.fromhex(h)] for h in _MALFORMED_UTF8_DOCS]
    return spark.createDataFrame(data, schema=schema).selectExpr(
        "jsonBin", "CAST(jsonBin AS STRING) AS jsonStr")

def test_get_json_object_malformed_utf8_unescaped_matches_cpu():
    """UNESCAPED path: malformed UTF-8 in a scalar string leaf must be replaced exactly as Spark's
    decoder replaces it. The result is compared as hex on purpose: collecting the string maps both
    the GPU bytes and the CPU bytes to U+FFFD, so a plain collect would pass even when they
    differ."""
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _malformed_utf8_df(spark).select(
            f.hex('jsonBin'),
            f.hex(f.get_json_object('jsonStr', '$.a'))),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_malformed_utf8_escaped_matches_cpu():
    """ESCAPED path: the same replacement must hold when query '$' re-serializes the structure,
    while a valid astral character still becomes an uppercase surrogate-pair escape. Compared as
    hex for the same reason as the unescaped case."""
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: _malformed_utf8_df(spark).select(
            f.hex('jsonBin'),
            f.hex(f.get_json_object('jsonStr', '$'))),
        exist_classes='GpuGetJsonObject')

# get_json_object path-evaluation and JSON-validation cases, under the same capture guard as above.
# A test applying more than one get_json_object to a column also turns off combining, or the plugin
# would fold the calls into one GpuMultiGetJsonObject and run a different kernel than intended.
_NO_COMBINE_CONF = {'spark.rapids.sql.expression.combined.GpuGetJsonObject': 'false'}

def test_get_json_object_index_past_inner_array_end():
    """A subscript that runs past the end of one inner array fails only that element, so an
    enclosing wildcard is still free to match a later one."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['[[1],[8,9]]'], ['[[1],[7,8,9]]'], ['[[1,2],[8,9]]'], ['[[1],[9,{"a":7}]]']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$[*][1]')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_nested_wildcard_nesting_level():
    """A nested wildcard match contributes exactly one to the enclosing counter, so the result gains
    no extra array nesting level."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['[[{"a":[1,2]}]]'], ['[[{"a":[1]}]]'], ['[[{"a":[]}]]']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$[*][*][*].a[*]')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_null_at_matched_field():
    """A matched field holding the null literal fails only that field, so a later element or a later
    duplicate key can still supply the value."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['[{"a":null},{"a":1}]'], ['[{"a":null}]'], ['[{"a":1},{"a":null}]']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$[*].a')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_duplicate_key_scan_continues():
    """Jackson does not reject duplicate keys, so a second field with the same name gets another
    chance to satisfy the rest of the path."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":{"c":1},"a":{"b":2}}'], ['{"a":null,"a":1}'], ['{"a":{"b":2},"a":{"c":1}}']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$.a.b'), f.get_json_object('jsonStr', '$.a')),
        exist_classes='GpuGetJsonObject', conf=_NO_COMBINE_CONF)

def test_get_json_object_duplicate_key_commits_at_root():
    """A double wildcard writes its array tokens straight to the shared generator, so after a
    re-scan two of those writes can land side by side at root, where no separator is emitted.
    Spark decides here whether one belongs; the second query is the same shape inside an array,
    where a comma is written."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":[],"a":[[1]]}'], ['{"a":[[1]],"a":[]}'], ['{"a":[],"a":[]}'],
            ['{"a":[],"a":[[1]],"a":[[2]]}'], ['[{"a":[],"a":[[1]]}]']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$.a[*][*]'),
            f.get_json_object('jsonStr', '$[*].a[*][*]')),
        exist_classes='GpuGetJsonObject', conf=_NO_COMBINE_CONF)

def test_get_json_object_field_name_requires_quote_delimiter():
    """A field name must be quote-delimited; accepting any byte as the delimiter would make
    documents Spark rejects silently produce a value."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{xax:1}'], ['{aa:1}'], ['{ xax :1}'], ['{"a":1,xbx:2}'], ['{"a":1}']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$'), f.get_json_object('jsonStr', '$.a')),
        exist_classes='GpuGetJsonObject', conf=_NO_COMBINE_CONF)

def test_get_json_object_root_scalar_trailing_junk():
    """Jackson rejects a root-level scalar followed by trailing content, so the row is null rather
    than the prefix that parsed."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['truex'], ['123abc'], ['1e5x'], ['nullx'], ['1,2'], ['true'], ['123']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$')),
        exist_classes='GpuGetJsonObject')

def test_get_json_object_truncated_after_separator():
    """A document that ends immediately after ':' or ',' leaves the parser nothing to read next.
    Without the bounds checks these rows read one byte past the end of the row, which only a
    sanitizer observes -- what is asserted here is that GPU and CPU agree on every truncation,
    whatever Spark decides each one means."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"a":'], ['{"a":1,'], ['[1,'], ['['], ['{'], ['{"a"'], ['{"a":[1,'],
            ['{"a":{"b":'], ['{"b":1,"a":'], ['{"a":1,"'], ['[{"a":1},'], ['{"a":,']]
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$.a'),
            f.get_json_object('jsonStr', '$'),
            f.get_json_object('jsonStr', '$[0]')),
        exist_classes='GpuGetJsonObject', conf=_NO_COMBINE_CONF)

def test_multi_get_json_object_edge_cases_match_cpu():
    """Several calls on one column let the plugin fold them into its combined kernel, which is a
    second code path no other test in this file drives. That fold cannot be asserted directly: it
    happens when the project list is bound for execution, so the combined expression never reaches
    the plan tree a capture assertion walks. Leaving the combine conf at its default is what
    selects it, and naming GpuGetJsonObject still fails this test if the paths fall back to CPU.

    The corpus is one row per divergence class this PR set fixes, limited to documents a Python
    string can hold -- malformed UTF-8 needs the binary path above. Each row is queried with a
    path deep enough to reach the behaviour it names."""
    schema = StructType([StructField("jsonStr", StringType())])
    data = [[r'{"a":"\uD800"}'],            # lone high surrogate
            [r'{"a":"\uD83D\uDE00"}'],   # escaped valid surrogate pair
            [r'{"a":"\u0022"}'],        # escape decoding to a quote
            ['{"a":{"c":1},"a":{"b":2}}'],  # duplicate key whose first match yields nothing
            ['{"a":[],"a":[[1]]}'],         # empty-array match must not stop the object scan
            ['[[1],[8,9]]'],                # subscript past one inner array end
            ['{xax:1}'],                    # unquoted field name, which Spark rejects
            ['truex'],                      # root keyword with trailing junk
            ['"abc"xyz'],                   # root string with trailing content, which Spark keeps
            ['{"a":']]                      # truncated immediately after a separator
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: spark.createDataFrame(data, schema=schema).select(
            f.get_json_object('jsonStr', '$'),
            f.get_json_object('jsonStr', '$.a'),
            f.get_json_object('jsonStr', '$.a.b'),
            f.get_json_object('jsonStr', '$.a[*][*]'),
            f.get_json_object('jsonStr', '$[*][1]')),
        exist_classes='GpuGetJsonObject')
