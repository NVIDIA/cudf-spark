# Copyright (c) 2022-2023, NVIDIA CORPORATION.
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
import os

from spark_session import with_cpu_session, with_gpu_session
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal
from data_gen import *
from marks import *

if os.environ.get('INCLUDE_SPARK_AVRO_JAR', 'false') == 'false':
    pytestmark = pytest.mark.skip(reason=str("INCLUDE_SPARK_AVRO_JAR is disabled"))

support_gens = numeric_gens + [string_gen, boolean_gen]

_enable_all_types_conf = {
    'spark.rapids.sql.format.avro.enabled': 'true',
    'spark.rapids.sql.format.avro.read.enabled': 'true'}

rapids_reader_types = ['PERFILE', 'COALESCING', 'MULTITHREADED']

# 50 files for the coalescing reading case
coalescingPartitionNum = 50

def gen_avro_files(gen_list, out_path):
    with_cpu_session(
        lambda spark: gen_df(spark,
            gen_list).repartition(coalescingPartitionNum).write.format("avro").save(out_path)
    )


@pytest.mark.parametrize('v1_enabled_list', ["avro", ""], ids=["v1", "v2"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_basic_read(spark_tmp_path, v1_enabled_list, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(support_gens)]
    data_path = spark_tmp_path + '/AVRO_DATA'
    gen_avro_files(gen_list, data_path)

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.rapids.sql.format.avro.reader.type': reader_type,
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path),
        conf=all_confs)


@pytest.mark.parametrize('v1_enabled_list', ["", "avro"], ids=["v1", "v2"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_avro_simple_partitioned_read(spark_tmp_path, v1_enabled_list, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(support_gens)]
    data_path = spark_tmp_path + '/AVRO_DATA'
    # generate partitioned files
    for v in [0, 1, 2]:
        out_path = data_path + '/key={}/key2=2{}'.format(v, v)
        gen_avro_files(gen_list, out_path)

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.rapids.sql.format.avro.reader.type': reader_type,
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path),
        conf=all_confs)


@pytest.mark.parametrize('v1_enabled_list', ["", "avro"], ids=["v1", "v2"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_avro_input_meta(spark_tmp_path, v1_enabled_list, reader_type):
    data_path = spark_tmp_path + '/AVRO_DATA'
    for v in [0, 1]:
        out_path = data_path + '/key={}'.format(v)
        with_cpu_session(
            lambda spark: unary_op_df(spark, long_gen).write.format("avro").save(out_path))

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.rapids.sql.format.avro.reader.type': reader_type,
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format("avro").load(data_path)
            .filter(f.col('a') > 0)
            .selectExpr('a',
                        'input_file_name()',
                        'input_file_block_start()',
                        'input_file_block_length()'),
        conf=all_confs)


# This is for https://github.com/NVIDIA/spark-rapids/issues/5312
@pytest.mark.parametrize('v1_enabled_list', ["avro", ""], ids=["v1", "v2"])
def test_coalescing_uniform_sync(spark_tmp_path, v1_enabled_list):
    # Generate the data files
    data_path = spark_tmp_path + '/AVRO_DATA'
    with_cpu_session(
        lambda spark: unary_op_df(spark, long_gen).repartition(coalescingPartitionNum)\
            .write.format("avro").save(data_path))
    # dump the coalesced files
    dump_path = spark_tmp_path + '/AVRO_DUMP/'
    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.rapids.sql.format.avro.reader.type': 'COALESCING',
        'spark.rapids.sql.avro.debug.dumpPrefix': dump_path,
        'spark.rapids.sql.avro.debug.dumpAlways': 'true',
        'spark.sql.sources.useV1SourceList': v1_enabled_list})
    with_gpu_session(
        lambda spark: spark.read.format("avro").load(data_path).collect(),
        conf=all_confs)
    # read the coalesced files by CPU
    with_cpu_session(
        lambda spark: spark.read.format("avro").load(dump_path).collect())


@ignore_order(local=True)
@pytest.mark.parametrize('v1_enabled_list', ["", "avro"], ids=["v1", "v2"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_avro_read_with_corrupt_files(spark_tmp_path, reader_type, v1_enabled_list):
    first_dpath = spark_tmp_path + '/AVRO_DATA/first'
    with_cpu_session(lambda spark : spark.range(1).toDF("a").write.format("avro").save(first_dpath))
    second_dpath = spark_tmp_path + '/AVRO_DATA/second'
    with_cpu_session(lambda spark : spark.range(1, 2).toDF("a").write.format("avro").save(second_dpath))
    third_dpath = spark_tmp_path + '/AVRO_DATA/third'
    with_cpu_session(lambda spark : spark.range(2, 3).toDF("a").write.json(third_dpath))

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.sql.files.ignoreCorruptFiles': "true",
        'spark.sql.sources.useV1SourceList': v1_enabled_list})

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.format("avro").load([first_dpath, second_dpath, third_dpath]),
            conf=all_confs)


# 10 rows is a small batch size for multi-batch read test, 2147483647 is the default value
@pytest.mark.parametrize('batch_size_rows', [10, 2147483647])
@pytest.mark.parametrize('v1_enabled_list', ["avro", ""], ids=["v1", "v2"])
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_read_count(spark_tmp_path, v1_enabled_list, reader_type, batch_size_rows):
    data_path = spark_tmp_path + '/AVRO_DATA'

    # the default block size of the avro file is about 64kb, so we need to generate a larger file
    # to test multi-batch read. length=30000 will generate 2 blocks in each partition.
    with_cpu_session(
        lambda spark: gen_df(spark, [('_c0', int_gen)], length=30000)
            .repartition(2).write.format("avro").save(data_path)
    )

    all_confs = copy_and_update(_enable_all_types_conf, {
        'spark.rapids.sql.format.avro.reader.type': reader_type,
        'spark.sql.sources.useV1SourceList': v1_enabled_list,
        'spark.rapids.sql.reader.batchSizeRows': batch_size_rows})

    assert_gpu_and_cpu_row_counts_equal(
        lambda spark: spark.read.format("avro").load(data_path),
        conf=all_confs)

@pytest.mark.parametrize('col_name', ['K0', 'k0', 'K3', 'k3', 'V0', 'v0'], ids=idfn)
@ignore_order
def test_read_case_col_name(spark_tmp_path, col_name):
    gen_list =[('k0', LongGen(nullable=False, min_val=0, max_val=0)), 
            ('k1', LongGen(nullable=False, min_val=1, max_val=1)),
            ('k2', LongGen(nullable=False, min_val=2, max_val=2)),
            ('k3', LongGen(nullable=False, min_val=3, max_val=3)),
            ('v0', LongGen()),
            ('v1', LongGen()),
            ('v2', LongGen()),
            ('v3', LongGen())]
 
    gen = StructGen(gen_list, nullable=False)
    data_path = spark_tmp_path + '/AVRO_DATA'
    with_cpu_session(
            lambda spark : gen_df(spark, gen).write.partitionBy('k0', 'k1', 'k2', 'k3').format('avro').save(data_path))

    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.read.format('avro').load(data_path).selectExpr(col_name),
            conf=_enable_all_types_conf)
