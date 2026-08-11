# Copyright (c) 2022, NVIDIA CORPORATION.
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

import locale
import pytest

from asserts import assert_gpu_fallback_collect
from data_gen import *
from marks import *
from pyspark.sql.types import *

from spark_session import is_jvm_charset_utf8

if is_jvm_charset_utf8():
    pytestmark = pytest.mark.skip(reason=str("Current locale uses UTF-8, fallback will not occur"))

_regexp_conf = { 'spark.rapids.sql.regexp.enabled': 'true' }

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

@allow_non_gpu('ProjectExec', 'RLike')
def test_rlike_no_unicode_fallback():
    gen = mk_str_gen('[abcd]{1,3}')
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'a rlike "ab+"'),
        'RLike',
        conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'RegExpReplace')
def test_re_replace_no_unicode_fallback():
    gen = mk_str_gen('.{0,5}TEST[\ud720 A]{0,5}')
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr(
            'REGEXP_REPLACE(a, "[A-Z]+", "PROD")'),
        'RegExpReplace',
        conf=_regexp_conf)

@allow_non_gpu('ProjectExec', 'StringSplit')
def test_split_re_no_unicode_fallback():
    data_gen = mk_str_gen('([bf]o{0,2}:){1,7}') \
        .with_special_case('boo:and:foo')
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'split(a, "[o]", 2)'),
        'StringSplit',
        conf=_regexp_conf)
