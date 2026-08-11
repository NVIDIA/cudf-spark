# Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import allow_non_gpu, ignore_order

_atomic_gens = [
    null_gen,
    boolean_gen,
    byte_gen,
    short_gen,
    int_gen,
    long_gen,
    date_gen,
    timestamp_gen,
    decimal_gen_32bit,
    decimal_gen_64bit,
    decimal_gen_128bit,
    float_gen,
    double_gen
]

_struct_of_xxhash_gens = StructGen([(f"c{i}", g) for i, g in enumerate(_atomic_gens)])

# This is also used by HyperLogLogPlusPLus(approx_count_distinct)
xxhash_gens = (_atomic_gens + [_struct_of_xxhash_gens] + single_level_array_gens
               + nested_array_gens_sample + [
                   all_basic_struct_gen,
                   struct_array_gen,
                   _struct_of_xxhash_gens
               ] + map_gens_sample)


@ignore_order(local=True)
@pytest.mark.parametrize("gen", xxhash_gens, ids=idfn)
def test_xxhash64_single_column(gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen).selectExpr("a", "xxhash64(a)"),
        {"spark.sql.legacy.allowHashOnMapType": True})


@ignore_order(local=True)
def test_xxhash64_multi_column():
    gen = StructGen(_struct_of_xxhash_gens.children, nullable=False)
    col_list = ",".join(gen.data_type.fieldNames())
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr("c0", f"xxhash64({col_list})"),
        {"spark.sql.legacy.allowHashOnMapType": True})


def test_xxhash64_8_depth():
    gen_8_depth = (
        StructGen([('l1',  # level 1
                    StructGen([('l2',
                                StructGen([('l3',
                                            StructGen([('l4',
                                                        StructGen([('l5',
                                                                    StructGen([('l6',
                                                                                StructGen([('l7',
                                                                                            int_gen)]))]))]))]))]))]))]))  # level 8
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen_8_depth).selectExpr("a", "xxhash64(a)"))


@allow_non_gpu("XxHash64")
def test_xxhash64_fallback_exceeds_stack_size_array_of_structure():
    gen_9_depth = (
        ArrayGen(  # depth += 1
            StructGen([('c',  # depth += 1
                        ArrayGen(  # depth += 1
                            StructGen([('c',  # depth += 1
                                        ArrayGen(  # depth += 1
                                            StructGen([('c',  # depth += 1
                                                        ArrayGen(  # depth += 1
                                                            StructGen([('c',  # depth += 1
                                                                        int_gen)]),  # depth += 1
                                                            max_length=1))]),
                                            max_length=1))]),
                            max_length=1))]),
            max_length=1))
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen_9_depth).selectExpr("a", "xxhash64(a)"),
        "XxHash64")


@allow_non_gpu("XxHash64")
def test_xxhash64_array_of_other():
    gen_9_depth = (
        ArrayGen(  # array(other: not struct): depth += 0
            ArrayGen(  # array(other: not struct): depth += 0
                ArrayGen(  # array(other: not struct): depth += 0
                    MapGen(  # map: depth += 2
                        IntegerGen(nullable=False),
                        ArrayGen(  # array(other: not struct): depth += 0
                            MapGen(  # map: depth += 2
                                IntegerGen(nullable=False),
                                ArrayGen(  # array(other: not struct): depth += 0
                                    MapGen(  # map: depth += 2
                                        IntegerGen(nullable=False),
                                        int_gen,  # primitive: depth += 1
                                        max_length=1),
                                    max_length=1),
                                max_length=1),
                            max_length=1),
                        max_length=1),
                    max_length=1),
                max_length=1),
            max_length=1))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, gen_9_depth).selectExpr("a", "xxhash64(a)"),
        {"spark.sql.legacy.allowHashOnMapType": True})


@allow_non_gpu("XxHash64")
def test_xxhash64_fallback_exceeds_stack_size_structure():
    gen_9_depth = (
        StructGen([('l1',  # level 1
                    StructGen([('l2',
                                StructGen([('l3',
                                            StructGen([('l4',
                                                        StructGen([('l5',
                                                                    StructGen([('l6',
                                                                                StructGen([('l7',
                                                                                            StructGen([('l8',
                                                                                                        int_gen)]))]))]))]))]))]))]))]))  # level 9
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen_9_depth).selectExpr("a", "xxhash64(a)"),
        "XxHash64")


@allow_non_gpu("XxHash64")
def test_xxhash64_fallback_exceeds_stack_size_map():
    gen_9_depth = (
        MapGen(  # depth += 2
            IntegerGen(nullable=False),
            MapGen(  # depth += 2
                IntegerGen(nullable=False),
                MapGen(  # depth += 2
                    IntegerGen(nullable=False),
                    MapGen(  # depth += 2
                        IntegerGen(nullable=False),  # depth += 1
                        IntegerGen(nullable=False),
                        max_length=1),
                    max_length=1),
                max_length=1),
            max_length=1))
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, gen_9_depth).selectExpr("a", "xxhash64(a)"),
        "XxHash64",
        {"spark.sql.legacy.allowHashOnMapType": True})

def test_binary_sha1():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, binary_gen).selectExpr('sha1(a)'))

def test_str_sha1():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, string_gen).selectExpr('sha1(a)'))

def test_str_special_characters_sha1():
    special_string_gen = StringGen().with_special_case('好').with_special_case('吃')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, special_string_gen).selectExpr('sha1(a)'))


@pytest.mark.parametrize("datagen", [binary_gen, string_gen])
@pytest.mark.parametrize("bitlength", [-20, 0, 123, 224, 256, 384, 512], ids=idfn)
def test_sha2_with_literal_bitlengths(bitlength, datagen):
    """
    Tests sha2 with bitlengths provided as literals, for string and binary inputs.
    For bitlength==0, the output is the same as with 256.  Should run on the GPU.
    For bitlength ∈ [-1, 123], the output should be nulls.  Should still run on the GPU.
    All the other bitlengths are valid and should run on the GPU.
    """
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, datagen).selectExpr(f'SHA2(a, {bitlength})'))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, datagen).selectExpr(f'SHA2("asdf", {bitlength})'))


@allow_non_gpu("Sha2", "Cast", "Length")
def test_sha2_bitlength_fallback():
    """
    Verifies that sha2 falls back to the CPU when the bitlength argument is not a literal.
    """
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, string_gen).selectExpr('SHA2(a, LENGTH(a))'),
        "Sha2")
