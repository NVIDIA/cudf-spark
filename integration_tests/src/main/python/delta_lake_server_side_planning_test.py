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

import uuid

import pytest

from asserts import (assert_cpu_and_gpu_are_equal_collect_with_capture,
                     assert_gpu_and_cpu_error, assert_gpu_fallback_collect)
from conftest import spark_jvm
from delta_lake_utils import is_oss_delta_lake_42_or_43
from marks import allow_non_gpu, delta_lake, ignore_order
from spark_session import with_cpu_session


pytestmark = [delta_lake, pytest.mark.skipif(
    not is_oss_delta_lake_42_or_43(), reason="Server-side planning requires OSS Delta Lake 4.2 or 4.3")]


@pytest.fixture
def planned_scan_conf(spark_tmp_path, request):
    pushed = getattr(request, "param", None) == "pushed"

    def write(spark):
        ranges = [(0, 10, "selected"), (10, 20, "matching")] if pushed else \
            [(0, 30, "selected")]
        for start, end, directory in ranges:
            spark.range(start, end).selectExpr(
                "id", "cast(id / 10.0 as decimal(12, 2)) amount",
                "named_struct('a.b', cast(id as string)) nested") \
                .repartition(2).write.parquet(spark_tmp_path + "/" + directory)
        if not pushed:
            # Valid Parquet data outside the authoritative manifest must never be scanned.
            spark.range(100, 110).write.parquet(spark_tmp_path + "/unplanned")
        path = spark._jvm.org.apache.hadoop.fs.Path(spark_tmp_path + "/corrupt.parquet")
        stream = path.getFileSystem(spark._jsc.hadoopConfiguration()).create(path)
        try:
            stream.writeBytes("not parquet data")
        finally:
            stream.close()

    with_cpu_session(write)
    # Spark caches catalog instances across test sessions; each fixture needs its own options.
    catalog = "planned_" + uuid.uuid4().hex
    return catalog, {
        "spark.sql.catalog." + catalog:
            "com.nvidia.spark.rapids.tests.delta.ServerSidePlannedTestCatalog",
        "spark.sql.catalog." + catalog + ".path": spark_tmp_path,
        "spark.sql.adaptive.enabled": "false",
    }


@ignore_order
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
@pytest.mark.parametrize("query", [
    "SELECT * FROM planned.db.table",
    "SELECT id, amount FROM planned.db.table WHERE id >= 13",
    "SELECT nested.`a.b` FROM planned.db.table",
    "SELECT id FROM planned.db.table ORDER BY id LIMIT 3",
    "SELECT count(*), sum(amount) FROM planned.db.table",
    "SELECT * FROM planned.db.empty",
])
def test_server_side_planned_scan(planned_scan_conf, reader, query):
    catalog, settings = planned_scan_conf
    conf = dict(settings, **{"spark.rapids.sql.format.parquet.reader.type": reader})

    def check_results(spark):
        df = spark.sql(query.replace("planned.", catalog + "."))
        if query == "SELECT * FROM planned.db.table":
            assert sorted(row.id for row in df.collect()) == list(range(30))
        return df

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        check_results, exist_classes="GpuBatchScanExec", conf=conf)


@ignore_order
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
@pytest.mark.parametrize("planned_scan_conf", ["pushed"], indirect=True)
def test_server_side_planned_scan_pushed_filter(planned_scan_conf, reader):
    catalog, settings = planned_scan_conf
    conf = dict(settings, **{"spark.rapids.sql.format.parquet.reader.type": reader})

    def read(spark):
        # Project out id so correctness depends on the service's selective manifest,
        # rather than a reader or Spark filter evaluating id after the scan.
        df = spark.sql("SELECT amount, nested.`a.b` AS label FROM " + catalog +
                       ".db.pushed WHERE id >= 10")
        assert sorted(int(row.label) for row in df.collect()) == list(range(10, 20))
        return df

    def assert_no_residual_filter(cpu_plan, gpu_plan):
        callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
        for plan in (cpu_plan, gpu_plan):
            assert not callback.contains(plan, "FilterExec"), plan.toString()
            assert not callback.contains(plan, "GpuFilterExec"), plan.toString()

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        read, exist_classes="GpuBatchScanExec", require_non_empty=True,
        gpu_plan_assertion=assert_no_residual_filter, conf=conf)


@ignore_order
@allow_non_gpu("BatchScanExec")
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
def test_server_side_planned_scan_disabled(planned_scan_conf, reader):
    catalog, settings = planned_scan_conf
    conf = dict(settings, **{"spark.rapids.sql.format.parquet.reader.type": reader})
    conf["spark.rapids.sql.input.ServerSidePlannedScan"] = "false"
    assert_gpu_fallback_collect(
        lambda spark: spark.sql("SELECT id FROM " + catalog + ".db.table"),
        "BatchScanExec", conf=conf)


@ignore_order
@allow_non_gpu("BatchScanExec", "ProjectExec", "Alias", "InputFileName",
               "InputFileBlockStart", "InputFileBlockLength")
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
def test_server_side_planned_scan_input_file_fallback(planned_scan_conf, reader):
    catalog, settings = planned_scan_conf
    conf = dict(settings, **{"spark.rapids.sql.format.parquet.reader.type": reader})
    query = ("SELECT id, input_file_name(), input_file_block_start(), "
             "input_file_block_length() FROM " + catalog + ".db.table")
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(query), "BatchScanExec", conf=conf)


@ignore_order
@allow_non_gpu("BatchScanExec")
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
@pytest.mark.parametrize("setting", ["ignoreMissingFiles", "ignoreCorruptFiles"])
def test_server_side_planned_scan_file_skipping_fallback(planned_scan_conf, reader, setting):
    catalog, settings = planned_scan_conf
    conf = dict(settings, **{"spark.rapids.sql.format.parquet.reader.type": reader,
                            "spark.sql.files." + setting: "true"})
    assert_gpu_fallback_collect(
        lambda spark: spark.sql("SELECT id FROM " + catalog + ".db.table"),
        "BatchScanExec", conf=conf)


@allow_non_gpu("BatchScanExec")
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
@pytest.mark.parametrize("table,setting,error", [
    ("missing", "ignoreMissingFiles", "does not exist"),
    ("corrupt", "ignoreCorruptFiles", "is not a Parquet file"),
])
def test_server_side_planned_scan_file_errors(planned_scan_conf, reader, table, setting, error):
    catalog, settings = planned_scan_conf
    conf = dict(settings, **{"spark.rapids.sql.format.parquet.reader.type": reader,
                            "spark.sql.files." + setting: "true"})
    assert_gpu_and_cpu_error(
        lambda spark: spark.sql("SELECT id FROM " + catalog + ".db." + table).collect(),
        conf=conf, error_message=error)


@allow_non_gpu("BatchScanExec")
@pytest.mark.parametrize("reader", ["PERFILE", "MULTITHREADED", "COALESCING"])
def test_server_side_planned_scan_missing_required_column(planned_scan_conf, reader):
    catalog, settings = planned_scan_conf
    conf = dict(settings, **{"spark.rapids.sql.format.parquet.reader.type": reader})
    assert_gpu_and_cpu_error(
        lambda spark: spark.sql("SELECT absent FROM " + catalog + ".db.required").collect(),
        conf=conf, error_message="Required column is missing")
