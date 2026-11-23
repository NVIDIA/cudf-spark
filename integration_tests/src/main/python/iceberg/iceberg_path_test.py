# Copyright (c) 2026, NVIDIA CORPORATION.
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

from iceberg import (get_full_table_name, iceberg_read_enabled_conf,
                     iceberg_read_format_versions, iceberg_table_properties_sql,
                     iceberg_unsupported_mark)
from marks import iceberg, ignore_order
from path_test_utils import PATH_ROWS, PATH_SCHEMA, assert_path_read
from spark_session import with_cpu_session


pytestmark = iceberg_unsupported_mark


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('format_version', iceberg_read_format_versions)
@pytest.mark.parametrize('reader', ['PERFILE', 'COALESCING', 'MULTITHREADED'])
def test_iceberg_path_characters(spark_tmp_table_factory, format_version, reader, request):
    # The catalog owns table locations and filenames. Exercise its FileIO via a
    # table scan rather than reading the underlying Parquet files with Spark.
    table = get_full_table_name(spark_tmp_table_factory)
    view = spark_tmp_table_factory.get()

    def setup(spark):
        spark.createDataFrame(PATH_ROWS, PATH_SCHEMA).coalesce(1).createOrReplaceTempView(view)
        spark.sql(f'CREATE TABLE {table} USING iceberg PARTITIONED BY (p) '
                  f'{iceberg_table_properties_sql(format_version)} AS SELECT * FROM {view}')

    def input_paths(spark):
        files = [row.file_path for row in spark.sql(
            f'SELECT file_path FROM {table}.files').collect()]
        print('Iceberg data file paths:', files)
        # Iceberg encodes partition values; these paths contain literal + and %,
        # but the partition directories do not contain raw spaces or Unicode.
        expected_partitions = {
            'p=a+b', 'p=a%2Bb', 'p=a%2520b', 'p=a%252Fb', 'p=%E8%B7%AF%E5%BE%84'}
        assert {path.rsplit('/', 2)[-2] for path in files} == expected_partitions, files
        return files

    def setup_and_check_input_files(spark):
        setup(spark)
        iceberg_table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table)
        file_io = iceberg_table.io()
        print('Iceberg table FileIO:', file_io.getClass().getName())
        files = input_paths(spark)
        assert files, 'Iceberg backend checks require non-empty data file paths'
        s3_inputs = []
        for file_path in files:
            input_class = file_io.newInputFile(file_path).getClass()
            print('Iceberg data InputFile:', file_path, input_class.getName())
            # The adapter accepts BaseS3File inputs, including those returned by
            # ResolvingFileIO delegates. Inspect actual inputs without loading
            # AWS classes in catalogs that only have HadoopFileIO available.
            is_s3_input = False
            while input_class is not None:
                if input_class.getName() == 'org.apache.iceberg.aws.s3.BaseS3File':
                    is_s3_input = True
                    break
                input_class = input_class.getSuperclass()
            s3_inputs.append(is_s3_input)
        return all(s3_inputs)

    s3_input_files = with_cpu_session(setup_and_check_input_files)
    backend = request.config.getoption('path_backend')
    if backend.startswith('s3-'):
        assert s3_input_files, 'Iceberg S3 PerfIO requires BaseS3File data inputs'
    elif backend == 'auto' and not s3_input_files:
        # Non-S3 InputFiles cannot exercise the Iceberg S3 adapter.
        # Keep zero-PerfIO assertions: incorrect attribution is tracked in #16846.
        backend = 'hadoop'
    assert_path_read(
        lambda spark: spark.sql(f'SELECT id, token, p FROM {table}'), PATH_ROWS,
        {**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader},
        'GpuBatchScanExec', backend=backend,
        # Iceberg's FileIO adapter currently has a PerfIO implementation for S3,
        # while GCS uses the catalog's InputFile streams.
        perfio_backends=('s3-netty', 's3-crt') if s3_input_files else (),
        input_paths=input_paths)
