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

from urllib.parse import urlsplit
from functools import reduce
import uuid

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import lit

from asserts import assert_equal_with_local_sort
from conftest import is_dataproc_runtime
from spark_init_internal import get_spark_i_know_what_i_am_doing
from spark_session import with_cpu_session, with_gpu_session


PATH_VALUES = ('a b', 'a+b', 'a%20b', 'a%2Fb', '路径')
PATH_ROWS = [(i, value, value) for i, value in enumerate(PATH_VALUES)]
PATH_SCHEMA = 'id long, token string, p string'

BACKEND_METRICS = {
    's3-netty': 'perfio.s3.netty.executors',
    's3-crt': 'perfio.s3.crt.executors',
    'gcs-http': 'perfio.gcs.http.executors',
    'gcs-grpc': 'perfio.gcs.grpc.executors',
}
FALLBACK_METRICS = ('perfio.s3.s3a.executors', 'perfio.s3.iceberg.fallbacks')


def _has_class(spark, name):
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    try:
        loader.loadClass(name)
        return True
    except Py4JJavaError as error:
        if error.java_exception.getClass().getSimpleName() in (
                'ClassNotFoundException', 'NoClassDefFoundError'):
            return False
        raise


def _completed_stages(spark):
    sc = spark.sparkContext._jsc.sc()
    sc.listenerBus().waitUntilEmpty()
    store = sc.statusStore()
    completed = spark._jvm.java.util.Collections.singletonList(
        spark._jvm.org.apache.spark.status.api.v1.StageStatus.COMPLETE)
    stages = store.stageList(completed, False, False,
        getattr(store, 'stageList$default$4')(),
        getattr(store, 'stageList$default$5')()).iterator()
    while stages.hasNext():
        yield stages.next()


def _expected_backend(spark, files, requested):
    schemes = {urlsplit(file).scheme for file in files}
    assert files and len(schemes) == 1, files
    scheme = schemes.pop()
    startup = spark.sparkContext.getConf()
    if requested != 'auto':
        if requested.startswith('s3-'):
            assert scheme in ('s3', 's3a', 's3n'), (requested, files)
        elif requested.startswith('gcs-'):
            assert scheme in ('gs', 'gcs'), (requested, files)
        return requested
    # Predict from the job's startup settings, independently of observed metrics.
    # Explicit --path_backend also detects transport substitutions (e.g. CRT -> Netty).
    if scheme in ('s3', 's3a', 's3n'):
        enabled = startup.get('spark.rapids.perfio.s3.enabled', None)
        client = startup.get('spark.rapids.perfio.s3.httpClient', 'NETTY').lower()
        if enabled is None:
            # Opportunistic startup is allowed to use Hadoop when the job only
            # supplies SDK v1. Explicit backend runs never take this branch.
            service = _has_class(spark, 'software.amazon.awssdk.services.s3.S3AsyncClient')
            netty = _has_class(spark, 'software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient')
            crt = _has_class(spark, 'software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient')
            # NETTY does not fall back to CRT when its client is unavailable.
            if not service or not (netty or crt) or (client == 'netty' and not netty):
                return 'hadoop'
            if client == 'crt' and not crt:
                client = 'netty'
        if enabled is None or enabled.lower() == 'true':
            return 's3-' + client
    if scheme in ('gs', 'gcs'):
        enabled = startup.get('spark.rapids.perfio.gcs.enabled', None)
        if enabled is not None:
            return 'gcs-http' if enabled.lower() == 'true' else 'hadoop'
        if is_dataproc_runtime() and _has_class(spark,
                'com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.GrpcStorageOptions'):
            return 'gcs-http'
    return 'hadoop'


def assert_path_read(read, expected, conf, gpu_scan, backend='auto',
                     perfio_backends=BACKEND_METRICS, input_paths=None):
    """Check a fixed oracle, CPU/GPU parity, the GPU scan, and actual read-backend metrics.

    Only stages completed by this GPU read can satisfy the backend assertion. CPU setup,
    previous tests, and file-cache hits cannot supply evidence of a PerfIO read.
    """
    def collect_cpu(spark):
        df = read(spark)
        rows = df.collect()
        # Some table-format V2 scans do not implement Dataset.inputFiles().
        files = input_paths(spark) if input_paths else df.inputFiles()
        return rows, files

    cpu, files = with_cpu_session(collect_cpu, conf=conf)
    assert_equal_with_local_sort([tuple(row) for row in cpu], expected)
    spark = get_spark_i_know_what_i_am_doing()
    backend = _expected_backend(spark, files, backend)
    last_stage_id = max((stage.stageId() for stage in _completed_stages(spark)), default=-1)

    def collect_gpu(spark):
        df = read(spark)
        rows = df.collect()
        spark._jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.assertContains(
            df._jdf, gpu_scan)
        return rows

    gpu = with_gpu_session(collect_gpu, conf=conf)
    assert_equal_with_local_sort(cpu, gpu)
    metrics = dict.fromkeys((*BACKEND_METRICS.values(), *FALLBACK_METRICS), 0)
    for stage in _completed_stages(spark):
        if stage.stageId() > last_stage_id:
            updates = stage.accumulatorUpdates().iterator()
            while updates.hasNext():
                update = updates.next()
                if update.name() in metrics:
                    metrics[update.name()] += int(update.value())
    expected_metric = BACKEND_METRICS.get(backend) if backend in perfio_backends else None
    if expected_metric:
        assert metrics[expected_metric] > 0, (backend, metrics)
        for name, count in metrics.items():
            if name != expected_metric:
                assert count == 0, (backend, metrics)
    else:
        # S3A is the expected backend when PerfIO is disabled. Its metric is also
        # recorded for ordinary Hadoop reads by some readers.
        for name in BACKEND_METRICS.values():
            assert metrics[name] == 0, (backend, metrics)
    print('Path read backend:', backend, 'Expected PerfIO metric:', expected_metric, metrics)


def make_path_data(spark, root, file_format, layout):
    """Write five distinct payloads once, then rename using Hadoop on any storage backend."""
    seed = root + '/seed'
    spark.createDataFrame(PATH_ROWS, PATH_SCHEMA).coalesce(1).write \
        .partitionBy('p').format(file_format).save(seed)
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path
    fs = path(root).getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    data_path = root + '/base with spaces'
    paths = []
    for value in PATH_VALUES:
        escaped = jvm.org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName(value)
        source_dir = path(seed + '/p=' + escaped)
        files = [status.getPath() for status in fs.listStatus(source_dir)
                 if status.isFile() and status.getPath().getName().startswith('part-')]
        assert len(files) == 1, files
        directory = data_path + ('/p=' + escaped if layout == 'partitioned' else '/' + value)
        destination = path(directory + '/' + value + '.' + file_format)
        assert fs.mkdirs(path(directory))
        assert fs.rename(files[0], destination), str(destination)
        paths.append(directory)
    # Reading the base directories explicitly exercises each spelling during path
    # resolution. The partitioned layout also verifies Spark's partition unescaping.
    return data_path if layout == 'partitioned' else paths


def read_path_data(spark, file_format, data_path, layout):
    reader = spark.read.schema('id long, token string').format(file_format)
    if layout == 'partitioned':
        # Partition values come from the requested paths, independently of the
        # file payload. They must stay associated with the correct id/token.
        return reader.load(data_path).select('id', 'token', 'p')
    # Label each requested base path independently of its payload. Reading the
    # union without labels could hide a permutation of two wrong-object reads.
    # Static labels also avoid input_file_name(), which changes coalescing readers.
    frames = [reader.load(path).select('id', 'token').withColumn('path_id', lit(i))
              for i, path in enumerate(data_path)]
    return reduce(lambda left, right: left.unionByName(right), frames)


@pytest.fixture(scope='module')
def path_character_data(request):
    # Without file caching, share immutable datasets across reader/V1/V2 parameters
    # to keep cloud fixture creation to ten tiny datasets rather than 44 writes.
    base = request.config.getoption('tmp_path') or '/tmp/pyspark_tests'
    root = base.rstrip('/') + '/path-characters-' + uuid.uuid4().hex
    spark = get_spark_i_know_what_i_am_doing()
    file_cache_enabled = spark.sparkContext.getConf().get(
        'spark.rapids.filecache.enabled', 'false').lower() == 'true'
    path = spark._jvm.org.apache.hadoop.fs.Path(root)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    cache = {}

    def get_data(file_format, layout):
        key = (file_format, layout)
        fresh_files = file_cache_enabled and file_format in ('parquet', 'orc')
        if key not in cache or fresh_files:
            data_root = root + '/' + file_format + '/' + layout
            if fresh_files:
                # Cached reads cannot prove transport use. Give each case new files.
                data_root += '/' + uuid.uuid4().hex
            cache[key] = with_cpu_session(lambda spark: make_path_data(
                spark, data_root, file_format, layout))
        return cache[key]

    yield get_data
    if not request.config.getoption('debug_tmp_path'):
        fs.delete(path, True)
