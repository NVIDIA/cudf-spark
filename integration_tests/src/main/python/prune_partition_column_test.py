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

import os
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, run_with_cpu_and_gpu, assert_equal
from data_gen import *
from marks import *
from pyspark.sql.types import IntegerType
from spark_session import with_cpu_session, is_before_spark_320
from conftest import spark_jvm

# Several values to avoid generating too many folders for partitions.
part1_gen = SetValuesGen(IntegerType(), [-10, -1, 0, 1, 10])
part2_gen = SetValuesGen(LongType(), [-100, 0, 100])

file_formats = ['parquet', 'orc', 'csv', 'json']
if os.environ.get('INCLUDE_SPARK_AVRO_JAR', 'false') == 'true':
    file_formats = file_formats + ['avro']

_enable_read_confs = {
    'spark.rapids.sql.format.avro.enabled': 'true',
    'spark.rapids.sql.format.avro.read.enabled': 'true',
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true',
}


def do_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled, file_format,
                                           gpu_project_enabled=True):
    data_path = spark_tmp_path + '/PARTED_DATA/'
    with_cpu_session(
        lambda spark: three_col_df(spark, int_gen, part1_gen, part2_gen).write \
            .partitionBy('b', 'c').format(file_format).save(data_path))

    all_confs = copy_and_update(_enable_read_confs, {
        'spark.rapids.sql.exec.ProjectExec': gpu_project_enabled,
        'spark.sql.sources.useV1SourceList': file_format,
        'spark.rapids.sql.fileScanPrunePartition.enabled': prune_part_enabled})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format(file_format).schema('a int, b int, c long') \
            .load(data_path).select('a', 'c'),
        conf=all_confs)


@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
def test_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled, file_format):
    do_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled, file_format)


@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
def test_prune_partition_column_when_fallback_project(spark_tmp_path, prune_part_enabled,
                                                      file_format):
    do_prune_partition_column_when_project(spark_tmp_path, prune_part_enabled, file_format,
                                           gpu_project_enabled=False)


def do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col, gpu_project_enabled=True,
                                                  gpu_filter_enabled=True):
    data_path = spark_tmp_path + '/PARTED_DATA/'
    with_cpu_session(
        lambda spark: three_col_df(spark, int_gen, part1_gen, part2_gen).write \
            .partitionBy('b', 'c').format(file_format).save(data_path))

    all_confs = copy_and_update(_enable_read_confs, {
        'spark.rapids.sql.exec.ProjectExec': gpu_project_enabled,
        'spark.rapids.sql.exec.FilterExec': gpu_filter_enabled,
        'spark.sql.sources.useV1SourceList': file_format,
        'spark.rapids.sql.fileScanPrunePartition.enabled': prune_part_enabled})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format(file_format).schema('a int, b int, c long').load(data_path) \
            .filter('{} > 0'.format(filter_col)) \
            .select('a', 'c'),
        conf=all_confs)


@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('filter_col', ['a', 'b', 'c'])
def test_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, filter_col,
                                                    file_format):
    do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col)


@allow_non_gpu('ProjectExec', 'FilterExec')
@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('filter_col', ['a', 'b', 'c'])
def test_prune_partition_column_when_fallback_filter_and_project(spark_tmp_path, prune_part_enabled,
                                                                 filter_col, file_format):
    do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col, gpu_project_enabled=False,
                                                  gpu_filter_enabled=False)


@allow_non_gpu('FilterExec')
@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('filter_col', ['a', 'b', 'c'])
def test_prune_partition_column_when_fallback_filter_project(spark_tmp_path, prune_part_enabled,
                                                             filter_col, file_format):
    do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col, gpu_filter_enabled=False)


@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('prune_part_enabled', [False, True])
@pytest.mark.parametrize('file_format', file_formats)
@pytest.mark.parametrize('filter_col', ['a', 'b', 'c'])
def test_prune_partition_column_when_filter_fallback_project(spark_tmp_path, prune_part_enabled,
                                                             filter_col, file_format):
    do_prune_partition_column_when_filter_project(spark_tmp_path, prune_part_enabled, file_format,
                                                  filter_col, gpu_project_enabled=False)

# This method creates two tables and saves them to partitioned Parquet/ORC files. The file is then
# read in using the read function that is passed in
def create_contacts_table_and_read(is_partitioned, format, data_path, expected_schemata, func, conf, table_name):
    full_name_type = StructGen([('first', StringGen()), ('middle', StringGen()), ('last', StringGen())])
    name_type = StructGen([('first', StringGen()), ('last', StringGen())])
    contacts_data_gen = StructGen([
        ('id', IntegerGen()),
        ('name', full_name_type),
        ('address', StringGen()),
        ('friends', ArrayGen(full_name_type, max_length=10, nullable=False))], nullable=False)

    brief_contacts_data_gen = StructGen([
        ('id', IntegerGen()),
        ('name', name_type),
        ('address', StringGen())], nullable=False)

    # We are adding the field 'p' twice just like it is being done in Spark tests
    # https://github.com/apache/spark/blob/85e252e8503534009f4fb5ea005d44c9eda31447/sql/core/src/test/scala/org/apache/spark/sql/execution/datasources/SchemaPruningSuite.scala#L193
    def contact_gen_df(spark, data_gen, partition):
        gen = gen_df(spark, data_gen)
        if is_partitioned:
            return gen.withColumn('p', f.lit(partition))
        else:
            return gen

    with_cpu_session(lambda spark: contact_gen_df(spark, contacts_data_gen, 1).write.format(format).save(data_path + f"/{table_name}/p=1"))
    with_cpu_session(lambda spark: contact_gen_df(spark, brief_contacts_data_gen, 2).write.format(format).save(data_path + f"/{table_name}/p=2"))

    # Schema to read in.
    read_schema = contacts_data_gen.data_type.add("p", IntegerType(), True) if is_partitioned else contacts_data_gen.data_type

    (from_cpu, cpu_df), (from_gpu, gpu_df) = run_with_cpu_and_gpu(
        func(read_schema),
        'COLLECT_WITH_DATAFRAME',
        conf=conf)

    jvm = spark_jvm()
    jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertSchemataMatch(cpu_df._jdf, gpu_df._jdf, expected_schemata)
    assert_equal(from_cpu, from_gpu)

# https://github.com/NVIDIA/spark-rapids/issues/8712
# https://github.com/NVIDIA/spark-rapids/issues/8713
# https://github.com/NVIDIA/spark-rapids/issues/8714
@pytest.mark.parametrize('query,expected_schemata', [("select friends.middle, friends from {} where p=1", "struct<friends:array<struct<first:string,middle:string,last:string>>>"),
                                                     pytest.param("select name.middle, address from {} where p=2", "struct<name:struct<middle:string>,address:string>", marks=pytest.mark.skip(reason='https://github.com/NVIDIA/spark-rapids/issues/8788')),
                                                     ("select name.first from {} where name.first = 'Jane'", "struct<name:struct<first:string>>")])
@pytest.mark.parametrize('is_partitioned', [True, False])
@pytest.mark.parametrize('format', ["parquet", "orc"])
def test_select_complex_field(format, spark_tmp_path, query, expected_schemata, is_partitioned, spark_tmp_table_factory):
    table_name = spark_tmp_table_factory.get()
    data_path = spark_tmp_path + "/DATA"
    def read_temp_view(schema):
        def do_it(spark):
            spark.read.format(format).schema(schema).load(data_path + f"/{table_name}").createOrReplaceTempView(table_name)
            return spark.sql(query.format(table_name))
        return do_it
    conf={"spark.sql.parquet.enableVectorizedReader": "true"}
    create_contacts_table_and_read(is_partitioned, format, data_path, expected_schemata, read_temp_view, conf, table_name)

# https://github.com/NVIDIA/spark-rapids/issues/8715
@pytest.mark.parametrize('query, expected_schemata', [("friend.First", "struct<friends:array<struct<first:string>>>"),
                                                          ("friend.MIDDLE", "struct<friends:array<struct<middle:string>>>")])
@pytest.mark.skipif(is_before_spark_320(), reason='https://issues.apache.org/jira/browse/SPARK-34638')
@pytest.mark.parametrize('is_partitioned', [True, False])
@pytest.mark.parametrize('format', ["parquet", "orc"])
def test_nested_column_prune_on_generator_output(format, spark_tmp_path, query, expected_schemata, is_partitioned, spark_tmp_table_factory):
    table_name = spark_tmp_table_factory.get()
    data_path = spark_tmp_path + "/DATA"
    def read_temp_view(schema):
        def do_it(spark):
            spark.read.format(format).schema(schema).load(data_path + f"/{table_name}").createOrReplaceTempView(table_name)
            return spark.table(table_name).select(f.explode(f.col("friends")).alias("friend")).select(query)
        return do_it
    conf = {"spark.sql.caseSensitive": "false",
            "spark.sql.parquet.enableVectorizedReader": "true"}
    create_contacts_table_and_read(is_partitioned, format, data_path, expected_schemata, read_temp_view, conf, table_name)