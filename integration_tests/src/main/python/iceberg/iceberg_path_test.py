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

    with_cpu_session(setup)
    assert_path_read(
        lambda spark: spark.sql(f'SELECT id, token, p FROM {table}'), PATH_ROWS,
        {**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader},
        'GpuBatchScanExec', backend=request.config.getoption('path_backend'),
        # Iceberg's FileIO adapter currently has a PerfIO implementation for S3,
        # while GCS uses the catalog's InputFile streams.
        perfio_backends=('s3-netty', 's3-crt'),
        input_paths=lambda spark: [row.file_path for row in spark.sql(
            f'SELECT file_path FROM {table}.files').collect()])
