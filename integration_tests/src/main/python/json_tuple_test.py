# Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import allow_non_gpu

def mk_json_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

json_str_patterns = [r'\{"store": \{"fruit": \[\{"weight":\d,"type":"[a-z]{1,9}"\}\], ' \
                     r'"bicycle":\{"price":[1-9]\d\.\d\d,"color":"[a-z]{0,4}"\}\},' \
                     r'"email":"[a-z]{1,5}\@[a-z]{3,10}\.com","owner":"[a-z]{3,8}"\}',
                     r'\{"a": "[a-z]{1,3}", "b\$":"[b-z]{1,3}"\}']

json_int_dict_patterns = [r'\{"1": [1-9]{1,6}, "2": -[1-9]{1,6}, ' \
                          r'"3": \{ "[1-9]{1,6}": [1-9]{1,6}, "-[1,10]": [1-9]{1,6}, '\
                          r'"-45": -[1-9]{1,6}\}\}']

json_whitespace_patterns = [r'\{"\\r\\n":"value\\n!", ' \
                            r'"cheddar\rcheese":"\\n[a-z]{0,10}\\r!", ' \
                            r'"fried\\nchicken":"[a-z]{0,2}\\n[a-z]{0,10}\\r[a-z]{0,2}!",' \
                            r'"fish":"salmon\\r\\ncod"\}']

json_eol_garbage_patterns = [r'\{"store":"Albertsons"\}this should not break',
                             r'\{"1":2\} freedom',
                             r'\{"email":gmail@outlook.com, "2":-5\}gmail better']


@pytest.mark.parametrize('json_str_pattern', json_str_patterns, ids=idfn)
def test_json_tuple(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
            'json_tuple(a, "a", "email", "owner", "b", "b$", "b$$")'),
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

@pytest.mark.parametrize('json_int_dict_pattern', json_int_dict_patterns, ids=idfn)
def test_int_dict_json(json_int_dict_pattern):
    gen = mk_json_str_gen(json_int_dict_pattern)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
            'json_tuple(a, "1", "2", "-45", "3.-45")'),
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

@pytest.mark.parametrize('json_whitespace_pattern', json_whitespace_patterns, ids=idfn)
def test_whitespace_json(json_whitespace_pattern):
    gen = mk_json_str_gen(json_whitespace_pattern)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
            'json_tuple(a, "\\r\\n", "fish", "fried\\nchicken", "cheddar\\rcheese")'),
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

@pytest.mark.parametrize('json_eol_garbage_pattern', json_eol_garbage_patterns, ids=idfn)
def test_json_eol_garbage_json(json_eol_garbage_pattern):
    gen = mk_json_str_gen(json_eol_garbage_pattern)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
            'json_tuple(a, "store", "email", "1", "cheddar\\rcheese")'),
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

def test_json_tuple_select_non_generator_col():
    gen = StringGen(pattern="{\"Zipcode\":\"abc\",\"ZipCodeType\":\"STANDARD\",\"City\":\"PARC PARQUE\",\"State\":\"PR\"}")
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, [('a', gen)]),
            'table',
            'select a, json_tuple(a, \"Zipcode\", \"ZipCodeType\", \"City\", \"State\") from table',
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

@allow_non_gpu('GenerateExec', 'JsonTuple')
@pytest.mark.parametrize('json_str_pattern', json_str_patterns, ids=idfn)
def test_json_tuple_with_large_number_of_fields_fallback(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
            'json_tuple(a, "a", "email", "owner", "bicycle", "b", "aa", "ab", "type", "color", "name", \
                           "weight", "x", "y", "z", "category", "address", "phone", "mobile", "aaa", "c", \
                           "date", "time", "second", "d", "abc", "e", "hour", "minute", "when", "what", \
                           "location", "city", "country", "zip", "code", "region", "state", "street", "block", "loc", \
                           "height", "h", "author", "title", "price", "isbn", "book", "rating", "score", "popular")'),
        "JsonTuple",
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})

@pytest.mark.parametrize('json_str_pattern', json_str_patterns, ids=idfn)
def test_json_tuple_with_special_characters(json_str_pattern):
    gen = mk_json_str_gen(json_str_pattern)
    special_characters = ['.', '[', ']', '{', '}', '\\\\', '\'', '\\\"']
    for special_character in special_characters:
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen, length=10).selectExpr(
                'json_tuple(a, "a", "a' + special_character + '")'),
            conf={'spark.sql.parser.escapedStringLiterals': 'true'})

def test_json_tuple_with_slash_backslash():
    schema = StructType([StructField("jsonStr", StringType())])
    data = [['{"url":"https:\/\/www.nvidia.com\/1\/pic\/-1234.jpg","item":[],"info":{"id":12345}}'],
            ['{"info":[{"foo":0}],"from":"bar","url":[{"title":"测试\\\\\测试 测试","value_3":"测试;测试;测试"}]}'],
            ['{"number":"1234567890","info":[{"foo":0}],"from":"bar"}']]
    
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.createDataFrame(data, schema).selectExpr(
            'json_tuple(jsonStr, "url", "info")'),
        conf={'spark.sql.parser.escapedStringLiterals': 'true'})