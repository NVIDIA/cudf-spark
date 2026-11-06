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

import os

import pytest

from delta_lake_utils import delta_meta_allow, setup_delta_dest_table
from marks import allow_non_gpu, delta_lake, ignore_order
from path_test_utils import (BACKEND_METRICS, PATH_ROWS, PATH_SCHEMA, PATH_VALUES,
                             assert_path_read, path_character_data, read_path_data)
from spark_session import with_cpu_session


# CSV/JSON and Avro read through Hadoop streams; Parquet and ORC also have
# PerfIO range readers. Each cloud job runs the same matrix against --tmp_path.
_readers = [(fmt, reader) for fmt in ('parquet', 'orc', 'avro')
            for reader in ('PERFILE', 'COALESCING', 'MULTITHREADED')]
_readers += [('csv', 'PERFILE'), ('json', 'PERFILE')]


@ignore_order(local=True)
@pytest.mark.parametrize('file_format,reader', [
    pytest.param(fmt, reader, id=fmt + '-' + reader, marks=(
        pytest.mark.skipif(os.environ.get('INCLUDE_SPARK_AVRO_JAR', 'false') != 'true',
                           reason='Avro requires INCLUDE_SPARK_AVRO_JAR=true')
        if fmt == 'avro' else [])) for fmt, reader in _readers])
@pytest.mark.parametrize('v1', [True, False], ids=['v1', 'v2'])
@pytest.mark.parametrize('layout', ['base', 'partitioned'])
def test_path_characters(path_character_data, file_format, reader, v1, layout, request):
    data_path = path_character_data(file_format, layout)
    conf = {
        'spark.sql.sources.useV1SourceList': file_format if v1 else '',
        'spark.rapids.sql.format.' + file_format + '.enabled': 'true',
        'spark.rapids.sql.format.' + file_format + '.read.enabled': 'true',
    }
    if file_format in ('parquet', 'orc', 'avro'):
        conf['spark.rapids.sql.format.' + file_format + '.reader.type'] = reader
    expected = [row if layout == 'partitioned' else (row[0], row[1], row[0]) for row in PATH_ROWS]
    assert_path_read(
        lambda spark: read_path_data(spark, file_format, data_path, layout),
        expected, conf, 'GpuFileSourceScanExec' if v1 else 'GpuBatchScanExec',
        backend=request.config.getoption('path_backend'),
        perfio_backends=BACKEND_METRICS if file_format in ('parquet', 'orc') else ())


@delta_lake
@allow_non_gpu(*delta_meta_allow)
@ignore_order(local=True)
@pytest.mark.parametrize('base', PATH_VALUES)
@pytest.mark.parametrize('reader', ['PERFILE', 'COALESCING', 'MULTITHREADED'])
def test_delta_path_characters(spark_tmp_path, base, reader, request):
    data_path = spark_tmp_path + '/' + base
    # Keep path coverage independent of runtime defaults for deletion vectors.
    with_cpu_session(lambda spark: setup_delta_dest_table(
        spark, data_path, lambda session: session.createDataFrame(PATH_ROWS, PATH_SCHEMA).coalesce(1),
        use_cdf=False, partition_columns=['p'], enable_deletion_vectors=False))
    assert_path_read(
        lambda spark: spark.read.format('delta').load(data_path).select('id', 'token', 'p'),
        PATH_ROWS, {'spark.rapids.sql.format.parquet.reader.type': reader},
        r'GpuFile(GpuScan|SourceScanExec)', backend=request.config.getoption('path_backend'))
