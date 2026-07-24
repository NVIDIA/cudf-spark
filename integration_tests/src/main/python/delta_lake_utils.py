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

import json
import os.path
import pytest
import re

from spark_session import is_databricks122_or_later, supports_delta_lake_deletion_vectors, \
    is_databricks173_or_later, with_cpu_session, with_gpu_session
from asserts import assert_equal
from conftest import is_databricks_runtime, spark_jvm

delta_meta_allow = [
    "DeserializeToObjectExec",
    "ShuffleExchangeExec",
    "FileSourceScanExec",
    "FilterExec",
    "MapPartitionsExec",
    "MapElementsExec",
    "ObjectHashAggregateExec",
    "ProjectExec",
    "SerializeFromObjectExec",
    "SortExec",
    "Add",
    "ScalaUDF",
    "And",
    "Subtract",
    "GetStructField",
    "Coalesce",
    "AssertNotNull",
    "Divide",
    "Cast",
    "CaseWhen",
    "GreaterThan",
    "LessThan",
    "KnownNotNull",
    "Or",
    "IsNull",
    "Not",
    "EqualTo",
    "CheckOverflowInTableWrite",
    "JsonToStructs"
]

if is_databricks173_or_later():
    # DBR 17.3 DV write/update paths can serialize Delta stats JSON on the CPU bridge.
    delta_meta_allow += ["StructsToJson", "CreateNamedStruct"]

delta_write = ["RapidsDeltaWrite"]

# Parameterize Deletion Vectors only on runtimes that expose the feature in these tests.
def deletion_vector_values_with_xfail_reasons(enabled_xfail_reason=None, disabled_xfail_reason=None):
    # Always include the DV-disabled case. On supported Databricks runtimes it can be marked xfail
    # when the caller needs to document a runtime-specific expectation.
    if is_databricks_runtime() and disabled_xfail_reason is not None:
        enable_deletion_vector = [pytest.param(False, marks=pytest.mark.xfail(reason=disabled_xfail_reason))]
    else:
        enable_deletion_vector = [False]

    # Add the DV-enabled case on supported Databricks runtimes. This parameterizes the feature;
    # it does not imply every runtime has GPU DV scan coverage.
    if is_databricks_runtime():
        if enabled_xfail_reason is None:
            enable_deletion_vector.append(True)
        else:
            enable_deletion_vector.append(pytest.param(True, marks=pytest.mark.xfail(reason=enabled_xfail_reason)))

    return enable_deletion_vector

deletion_vector_values = deletion_vector_values_with_xfail_reasons()

delta_writes_enabled_conf = {"spark.rapids.sql.format.delta.write.enabled": "true"}

delta_row_tracking_dml_conf = {
    "spark.databricks.delta.optimizeWrite.enabled": "false",
    "spark.databricks.delta.autoCompact.enabled": "false"
}

# DB-17.3 serializes generated wide-schema rows into RDDScanExec task closures for these
# Delta write tests. The default 2048 rows can produce ~33 MB task bodies and OOM in
# TaskSetManager.prepareLaunchingTask. Keep this DB-17.3-only reduction visible until
# https://github.com/NVIDIA/spark-rapids/issues/14775 restores the normal 2048-row coverage.
delta_db173_wide_schema_gen_length = 128 if is_databricks173_or_later() else 2048

delta_write_fallback_allow = "ExecutedCommandExec,DataWritingCommandExec,WriteFilesExec,DeltaInvariantCheckerExec" if is_databricks122_or_later() else "ExecutedCommandExec"
delta_write_fallback_check = "DataWritingCommandExec" if is_databricks122_or_later() else "ExecutedCommandExec"

delta_optimized_write_fallback_allow = "ExecutedCommandExec,DataWritingCommandExec,DeltaOptimizedWriterExec,WriteFilesExec" if is_databricks122_or_later() else "ExecutedCommandExec"

def _fixup_operation_metrics(opm):
    """Update the specified operationMetrics node to facilitate log comparisons"""
    # note that we remove many byte metrics because number of bytes can vary
    # between CPU and GPU.
    metrics_to_remove = ["executionTimeMs", "numOutputBytes", "rewriteTimeMs", "scanTimeMs",
                         "numRemovedBytes", "numAddedBytes", "numTargetBytesAdded", "numTargetBytesInserted",
                         "numTargetBytesUpdated", "numTargetBytesRemoved", "materializeSourceTimeMs",
                         # For OPTIMIZE command, file size distribution can legitimately differ
                         # across CPU and GPU implementations. Ignore percentile and min/max metrics.
                         "p25FileSize", "p50FileSize", "p75FileSize", "minFileSize", "maxFileSize"]
    for k in metrics_to_remove:
        opm.pop(k, None)

TMP_TABLE_PATTERN=re.compile(r"tmp_table_\w+")
TMP_TABLE_PATH_PATTERN=re.compile(r"delta.`[^`]*`")
REF_ID_PATTERN=re.compile(r"#[0-9]+")
ROW_TRACKING_COLUMN_NAME_KEYS = (
    "delta.rowTracking.materializedRowCommitVersionColumnName",
    "delta.rowTracking.materializedRowIdColumnName")


def _normalize_row_tracking_column_names(properties):
    # DBR 17.3 can materialize row tracking column names with random UUIDs.
    # Preserve the presence of these properties while normalizing their values.
    changed = False
    for key in ROW_TRACKING_COLUMN_NAME_KEYS:
        if key in properties:
            properties[key] = key
            changed = True
    return changed


def _fixup_operation_parameters(opp):
    """Update the specified operationParameters node to facilitate log comparisons"""
    for key in ("predicate", "matchedPredicates", "notMatchedPredicates"):
        pred = opp.get(key)
        if pred:
            subbed = TMP_TABLE_PATTERN.sub("tmp_table", pred)
            subbed = TMP_TABLE_PATH_PATTERN.sub("tmp_table", subbed)
            opp[key] = REF_ID_PATTERN.sub("#refid", subbed)
    properties = opp.get("properties")
    if properties:
        try:
            properties_json = json.loads(properties)
        except ValueError:
            return
        if _normalize_row_tracking_column_names(properties_json):
            opp["properties"] = json.dumps(properties_json, sort_keys=True)

def assert_delta_history_equal(conf, cpu_table, gpu_table):
    # Project all columns except for the `timestamp` column, which won't match between CPU and GPU.
    cols = ["version", "userId", "userName", "operation", "operationParameters", "job", "notebook",
            "clusterId", "readVersion", "isolationLevel", "isBlindAppend", "operationMetrics", "userMetadata"]
    cpu_history = with_cpu_session(lambda spark: spark.sql("DESCRIBE HISTORY {}".format(cpu_table))
                                   .select(cols).collect(), conf=conf)
    gpu_history = with_cpu_session(lambda spark: spark.sql("DESCRIBE HISTORY {}".format(gpu_table))
                                   .select(cols).collect(), conf=conf)
    assert_equal(cpu_history, gpu_history)


def assert_delta_log_json_equivalent(filename, c_json, g_json):
    assert c_json.keys() == g_json.keys(), "Delta log {} has mismatched keys:\nCPU: {}\nGPU: {}".format(filename, c_json, g_json)
    def fixup_path(d):
        """Modify the 'path' value to remove random IDs in the pathname"""
        parts = d["path"].split("-")
        d["path"] = "-".join(parts[0:1]) + ".".join(parts[-1].split(".")[-2:])
    def del_keys(key_list, c_val, g_val):
        for key in key_list:
            c_val.pop(key, None)
            g_val.pop(key, None)
    def fixup_domain_metadata(d):
        """Normalize generated values in domainMetadata configuration."""
        if d.get("domain") == "com.databricks.liquid" and d.get("configuration"):
            configuration = json.loads(d["configuration"])
            for config_key in ("metadataId", "revisionId"):
                configuration.pop(config_key, None)
            d["configuration"] = json.dumps(configuration, sort_keys=True)
    def fixup_deletion_vector(c_val, g_val):
        # DV storage IDs and offsets are table-specific; compare stable descriptor fields.
        c_dv = c_val.get("deletionVector")
        g_dv = g_val.get("deletionVector")
        if c_dv and g_dv:
            del_keys(("pathOrInlineDv", "offset"), c_dv, g_dv)

    for key, c_val in c_json.items():
        g_val = g_json[key]
        # Strip out the values that are expected to be different
        c_tags = c_val.get("tags", {})
        g_tags = g_val.get("tags", {})
        del_keys(["INSERTION_TIME", "MAX_INSERTION_TIME", "MIN_INSERTION_TIME", "ZCUBE_ID",
                  "compactedInto", "optimizeCommandId"], c_tags, g_tags)
        if key == "metaData":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'metaData' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("createdTime", "id"), c_val, g_val)
            _normalize_row_tracking_column_names(c_val.get("configuration", {}))
            _normalize_row_tracking_column_names(g_val.get("configuration", {}))
        elif key == "add":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'add' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("modificationTime", "size"), c_val, g_val)
            fixup_path(c_val)
            fixup_path(g_val)
        elif key == "cdc":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'cdc' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("size",), c_val, g_val)
            fixup_path(c_val)
            fixup_path(g_val)
        elif key == "commitInfo":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'commitInfo' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("timestamp", "txnId"), c_val, g_val)
            for v in c_val, g_val:
                _fixup_operation_metrics(v.get("operationMetrics", {}))
                _fixup_operation_parameters(v.get("operationParameters", {}))
        elif key == "domainMetadata":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'domainMetadata' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            fixup_domain_metadata(c_val)
            fixup_domain_metadata(g_val)
        elif key == "remove":
            assert c_val.keys() == g_val.keys(), "Delta log {} 'remove' keys mismatch:\nCPU: {}\nGPU: {}".format(filename, c_val, g_val)
            del_keys(("deletionTimestamp", "size"), c_val, g_val)
            fixup_deletion_vector(c_val, g_val)
            fixup_path(c_val)
            fixup_path(g_val)
        assert c_val == g_val, "Delta log {} is different at key '{}':\nCPU: {}\nGPU: {}".format(filename, key, c_val, g_val)

def _decode_jsons(json_data):
    """Decode the JSON records in a string"""
    jsons = []
    idx = 0
    decoder = json.JSONDecoder()
    while idx < len(json_data):
        js, idx = decoder.raw_decode(json_data, idx=idx)
        jsons.append(js)
        # Skip whitespace between records
        while idx < len(json_data) and json_data[idx].isspace():
            idx += 1
    # reorder to produce a consistent output for comparison
    def json_to_sort_key(j):
        keys = sorted(j.keys())
        stats = sorted([ v.get("stats", "") for v in j.values() ])
        paths = sorted([ v.get("path", "") for v in j.values() ])
        return ','.join(keys + stats + paths)
    jsons.sort(key=json_to_sort_key)
    return jsons

def read_delta_logs(spark, path):
    log_data = spark.sparkContext.wholeTextFiles(path).collect()
    return dict([(os.path.basename(x), _decode_jsons(y)) for x, y in log_data])

def assert_gpu_and_cpu_delta_logs_equivalent(spark, data_path):
    cpu_log_data = spark.sparkContext.wholeTextFiles(data_path + "/CPU/_delta_log/*").collect()
    gpu_log_data = spark.sparkContext.wholeTextFiles(data_path + "/GPU/_delta_log/*").collect()
    assert len(cpu_log_data) == len(gpu_log_data), "Different number of Delta log files:\nCPU: {}\nGPU: {}".format(cpu_log_data, gpu_log_data)
    cpu_logs_data = [ (os.path.basename(x), y) for x, y in cpu_log_data if x.endswith(".json") ]
    gpu_logs_dict = dict([ (os.path.basename(x), y) for x, y in gpu_log_data if x.endswith(".json") ])
    for file, cpu_json_data in cpu_logs_data:
        gpu_json_data = gpu_logs_dict.get(file)
        assert gpu_json_data, "CPU Delta log file {} is missing from GPU Delta logs".format(file)
        cpu_jsons = _decode_jsons(cpu_json_data)
        gpu_jsons = _decode_jsons(gpu_json_data)
        assert len(cpu_jsons) == len(gpu_jsons), "Different line counts in {}:\nCPU: {}\nGPU: {}".format(file, cpu_json_data, gpu_json_data)
        for cpu_json, gpu_json in zip(cpu_jsons, gpu_jsons):
            assert_delta_log_json_equivalent(file, cpu_json, gpu_json)

def assert_gpu_and_cpu_latest_delta_log_equivalent(spark, data_path):
    cpu_logs = read_delta_logs(spark, data_path + "/CPU/_delta_log/*.json")
    gpu_logs = read_delta_logs(spark, data_path + "/GPU/_delta_log/*.json")
    assert len(cpu_logs) == len(gpu_logs), "Different number of Delta log JSON files:\nCPU: {}\nGPU: {}".format(
        sorted(cpu_logs.keys()), sorted(gpu_logs.keys()))
    latest_file = sorted(cpu_logs.keys())[-1]
    gpu_latest_file = sorted(gpu_logs.keys())[-1]
    assert latest_file == gpu_latest_file, \
        "Latest Delta log differs:\nCPU: {}\nGPU: {}".format(latest_file, gpu_latest_file)
    cpu_jsons = cpu_logs[latest_file]
    gpu_jsons = gpu_logs[latest_file]
    assert len(cpu_jsons) == len(gpu_jsons), "Different line counts in {}:\nCPU: {}\nGPU: {}".format(
        latest_file, cpu_jsons, gpu_jsons)
    for cpu_json, gpu_json in zip(cpu_jsons, gpu_jsons):
        assert_delta_log_json_equivalent(latest_file, cpu_json, gpu_json)

def read_delta_path(spark, path):
    return spark.read.format("delta").load(path)

def read_delta_path_with_cdf(spark, path):
    df = spark.read.format("delta") \
        .option("readChangeFeed", "true").option("startingVersion", 0) \
        .load(path)
    assert "_change_type" in df.columns
    assert "_commit_version" in df.columns
    assert "_commit_timestamp" in df.columns
    # Drop the commit timestamp column since it will differ between CPU and GPU
    return df.drop("_commit_timestamp")

def schema_to_ddl(spark, schema):
    return spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json()).toDDL()

def setup_delta_dest_table(spark, path, dest_table_func, use_cdf, partition_columns=None, enable_deletion_vectors=False, options=None):
    dest_df = dest_table_func(spark)
    # append to SQL-created table
    writer = dest_df.write.format("delta").mode("append")
    ddl = schema_to_ddl(spark, dest_df.schema)
    table_properties = {}
    table_properties['delta.enableChangeDataFeed'] = str(use_cdf).lower()
    if supports_delta_lake_deletion_vectors():
        table_properties['delta.enableDeletionVectors'] = str(enable_deletion_vectors).lower()
    # if any table properties are specified then we need to use SQL to define the table
    sql_text = "CREATE TABLE delta.`{path}` ({ddl}) USING DELTA".format(path=path, ddl=ddl)
    if partition_columns:
        sql_text += " PARTITIONED BY ({})".format(",".join(partition_columns))
        writer = writer.partitionBy(*partition_columns)
    properties = ', '.join(key + ' = ' + value for key, value in table_properties.items())
    sql_text += " TBLPROPERTIES ({})".format(properties)
    if options:
        options_str = ', '.join(key + ' = ' + value for key, value in options.items())
        sql_text += f" OPTIONS ({options_str}) "
    spark.sql(sql_text)
    writer.save(path)

def setup_delta_dest_tables(spark, data_path, dest_table_func, use_cdf, enable_deletion_vectors, partition_columns=None):
    for name in ["CPU", "GPU"]:
        path = "{}/{}".format(data_path, name)
        setup_delta_dest_table(spark, path, dest_table_func, use_cdf, partition_columns, enable_deletion_vectors)

def setup_delta_row_tracking_dest_table(spark, path, dest_table_func):
    dest_df = dest_table_func(spark)
    ddl = schema_to_ddl(spark, dest_df.schema)
    spark.sql("CREATE TABLE delta.`{}` ({}) USING DELTA "
              "TBLPROPERTIES (delta.enableRowTracking = true, "
              "delta.enableDeletionVectors = false)".format(path, ddl))
    dest_df.write.format("delta").mode("append").save(path)

def setup_delta_row_tracking_dest_tables(spark, data_path, dest_table_func):
    for name in ["CPU", "GPU"]:
        path = "{}/{}".format(data_path, name)
        setup_delta_row_tracking_dest_table(spark, path, dest_table_func)

def row_tracking_dml_test_df(spark):
    return spark.createDataFrame(
        [(1, "a", "x"), (2, "b", "x"), (3, "c", "x"), (4, "d", "x")],
        "a INT, b STRING, c STRING").coalesce(1)

def assert_delta_row_tracking_dml(spark_tmp_path, dml_sql, conf,
                                  dest_table_func=row_tracking_dml_test_df):
    data_path = spark_tmp_path + "/DELTA_DATA"
    with_cpu_session(lambda spark: setup_delta_row_tracking_dest_tables(
        spark, data_path, dest_table_func), conf=conf)

    cpu_path = data_path + "/CPU"
    gpu_path = data_path + "/GPU"
    cpu_result = with_cpu_session(lambda spark: spark.sql(dml_sql.format(path=cpu_path)).collect(),
                                  conf=conf)
    gpu_result = assert_rapids_delta_write(
        lambda spark: spark.sql(dml_sql.format(path=gpu_path)).collect(), conf=conf)
    assert_equal(cpu_result, gpu_result)

    def read_sorted_delta_path(spark, path):
        df = read_delta_path(spark, path)
        return df.sort(df.columns).collect()
    cpu_data = with_cpu_session(lambda spark: read_sorted_delta_path(spark, cpu_path), conf=conf)
    gpu_data = with_cpu_session(lambda spark: read_sorted_delta_path(spark, gpu_path), conf=conf)
    assert_equal(cpu_data, gpu_data)
    with_cpu_session(lambda spark: assert_gpu_and_cpu_latest_delta_log_equivalent(spark, data_path),
                     conf=conf)

def assert_rapids_delta_write(do_test, conf):
    """
    Validates that a Delta write operation executed on the GPU produces the expected execution plans.
    This function starts a plan capture mechanism using the Spark JVM's ExecutionPlanCaptureCallback,
    runs the provided test function (`do_test`) within a GPU session, and collects the execution plans
    generated during the write operation. It then checks that each expected Delta write class is present
    in at least one captured plan.

    Parameters
    ----------
    do_test : callable
        A function that performs the Delta write operation to be validated.
    conf : dict
        A dictionary of configuration options to be passed to the GPU session.

    Returns
    -------
    result : Any
        The result returned by the `do_test` function.
    """
    jvm = spark_jvm()
    jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.startCapture()
    try:
        result = with_gpu_session(do_test, conf=conf)
        captured_plans = jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.getResultsWithTimeout(10000)
        # Some write functions are no-op. We may not capture any GPU plan.
        if len(captured_plans) > 0:
            for cls in delta_write:
                found = False
                for plan in captured_plans:
                    found = jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.contains(plan, cls)
                    if found:
                        break
                assert found, f"{cls} is not found in any captured plan"
        return result
    finally:
        jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback.endCapture()

def assert_rapids_gpu_delete_ran(do_test, conf):
    """
    Runs a Delta DELETE on the GPU and asserts the GpuDeleteCommand actually executed
    (i.e. the command did not fall back to CPU). Unlike assert_rapids_delta_write, this
    does not require a write plan, so it works for metadata-only/whole-table deletes that
    remove files via Delta metadata without writing any data files.
    """
    jvm = spark_jvm()
    callback = jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        result = with_gpu_session(do_test, conf=conf)
        captured_plans = callback.getResultsWithTimeout(10000)
        assert any(callback.contains(plan, "GpuDeleteCommand") for plan in captured_plans), \
            "GpuDeleteCommand not found in any captured plan; DELETE may have fallen back to CPU"
        return result
    finally:
        callback.endCapture()
