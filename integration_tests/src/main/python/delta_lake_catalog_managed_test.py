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

import json
import os
import stat
import uuid
from urllib.parse import urlparse

import pytest
from pyspark.sql import functions as F

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import spark_jvm, unity_catalog_storage_root, unity_catalog_uri
from delta_lake_utils import (assert_rapids_delta_write, assert_rapids_gpu_delete_ran,
                              delta_meta_allow, delta_writes_enabled_conf,
                              is_oss_delta_lake_42)
from marks import allow_non_gpu, delta_lake, unity_catalog
from spark_session import with_cpu_session, with_gpu_session


pytestmark = pytest.mark.skipif(
    not is_oss_delta_lake_42(),
    reason="OSS Unity Catalog managed tables require OSS Delta Lake 4.2.0")

_CATALOG = "unity"
_SCHEMA = "default"
_CATALOG_MANAGED_PROPERTY = "delta.feature.catalogManaged"
_UC_TABLE_ID_PROPERTY = "io.unitycatalog.tableId"
_DEPRECATED_UC_TABLE_ID_PROPERTY = "ucTableId"
_STATIC_TOKEN = "static-token"
_COMMIT_COORDINATOR_PROPERTY = "delta.coordinatedCommits.commitCoordinator-preview"
_COMMIT_COORDINATOR_CONF_PROPERTY = \
    "delta.coordinatedCommits.commitCoordinatorConf-preview"
_COMMIT_TABLE_CONF_PROPERTY = "delta.coordinatedCommits.tableConf-preview"


def _api_client(jvm, uri):
    token_config = jvm.java.util.HashMap()
    token_config.put("type", "static")
    token_config.put("token", _STATIC_TOKEN)
    token_provider = jvm.io.unitycatalog.client.auth.TokenProvider.create(token_config)
    return jvm.io.unitycatalog.client.ApiClientBuilder.create() \
        .uri(uri) \
        .tokenProvider(token_provider) \
        .build()


def _create_if_absent(create, already_exists_code):
    """
    Runs a Unity Catalog create call, tolerating an object that is already there.

    This fixture is module scoped, so it runs once per pytest-xdist worker rather than once per
    session, and a server started by run_unity_catalog_server.sh outlives a single pytest run.
    Either way only the first caller creates the object and the rest see ALREADY_EXISTS.
    """
    try:
        create()
    except Exception as error:
        if already_exists_code not in str(error):
            raise


@pytest.fixture(scope="module")
def unity_catalog_server():
    """
    Connects to the Unity Catalog server started by the test harness.

    The server runs in its own JVM so that only the Unity Catalog Spark connector, and not the
    server and its dependency tree, ends up on the Spark classpath. `DELTA_UC_URI` and
    `DELTA_UC_STORAGE_ROOT` are exported by integration_tests/run_unity_catalog_server.sh and by
    jenkins/spark-tests.sh; see integration_tests/README.md for running this suite by hand.
    """
    jvm = spark_jvm()
    uri = unity_catalog_uri()
    storage_root = unity_catalog_storage_root()
    assert storage_root, "DELTA_UC_STORAGE_ROOT must be set alongside DELTA_UC_URI"

    client = _api_client(jvm, uri)
    catalogs_api = jvm.io.unitycatalog.client.api.CatalogsApi(client)
    _create_if_absent(
        lambda: catalogs_api.createCatalog(
            jvm.io.unitycatalog.client.model.CreateCatalog()
            .name(_CATALOG)
            .comment("RAPIDS catalog-managed table integration tests")),
        "CATALOG_ALREADY_EXISTS")
    schemas_api = jvm.io.unitycatalog.client.api.SchemasApi(client)
    _create_if_absent(
        lambda: schemas_api.createSchema(
            jvm.io.unitycatalog.client.model.CreateSchema()
            .name(_SCHEMA)
            .catalogName(_CATALOG)),
        "SCHEMA_ALREADY_EXISTS")

    yield {
        "uri": uri,
        "storage_root": storage_root,
        "tables_api": jvm.io.unitycatalog.client.api.TablesApi(client),
    }


def _catalog_conf(unity_catalog_server):
    prefix = f"spark.sql.catalog.{_CATALOG}"
    return {
        **delta_writes_enabled_conf,
        prefix: "io.unitycatalog.spark.UCSingleCatalog",
        f"{prefix}.uri": unity_catalog_server["uri"],
        f"{prefix}.token": _STATIC_TOKEN,
        f"{prefix}.warehouse": _CATALOG,
        # Both of the following default to true in Unity Catalog and are deliberately turned off.
        #
        # renewCredential.enabled=false makes Unity Catalog publish the vended credentials as
        # plain fs.s3a.access.key/secret.key/session.token values, which is what
        # CredentialTestFileSystem asserts. With renewal on it would instead install a
        # credential-provider class that needs the AWS SDK on the classpath.
        #
        # credScopedFs.enabled=false keeps Unity Catalog from overriding fs.s3.impl with its own
        # wrapper filesystem. The wrapper does preserve the original implementation, but only if
        # it can read it back from an active Spark session, so leaving it off keeps
        # CredentialTestFileSystem unambiguously in the path. This default flipped to true in
        # Unity Catalog 0.6.0, so the setting is load-bearing rather than merely explicit.
        #
        # deltaRestApi.enabled is intentionally NOT set. It defaults to true but Unity Catalog
        # ignores it below Delta Lake 4.3.0, so it is that version gate, not this config, that
        # keeps the pre-4.3 staging path in use. A future Delta upgrade is expected to surface
        # here rather than silently switch Unity Catalog onto an untested staging mechanism.
        f"{prefix}.renewCredential.enabled": "false",
        f"{prefix}.credScopedFs.enabled": "false",
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
    }


def _new_table_name(prefix):
    table = f"{prefix}_{uuid.uuid4().hex}"
    return table, f"{_CATALOG}.{_SCHEMA}.{table}"


def _drop_table(table, conf):
    with_cpu_session(lambda spark: spark.sql(f"DROP TABLE IF EXISTS {table}").collect(), conf=conf)


def _table_rows(table, conf):
    return with_cpu_session(
        lambda spark: [tuple(row) for row in spark.table(table).orderBy("id").collect()],
        conf=conf)


def _assert_three_dml_change_feed(table, conf):
    change_counts = with_cpu_session(
        lambda spark: {
            row["_change_type"]: row["count"]
            for row in spark.sql(f"""
                SELECT _change_type, count(*) AS count
                FROM table_changes('{table}', 1)
                GROUP BY _change_type
                """).collect()
        }, conf=conf)
    assert change_counts == {
        "delete": 1,
        "update_preimage": 2,
        "update_postimage": 2,
        "insert": 1,
    }


def _assert_catalog_gpu_write(
        do_test, conf, expected_command=None, expected_classes=None):
    """Require catalog-managed writes to capture a real GPU Delta write plan."""
    return assert_rapids_delta_write(
        do_test, conf=conf, required_gpu_classes=["GpuRapidsDeltaWriteExec"],
        forbidden_cpu_fallback_classes=["RapidsDeltaWriteExec"], require_non_empty=True,
        expected_command=expected_command, expected_classes=expected_classes)


def _assert_catalog_command_fallback(do_test, conf):
    """Require fallback before any managed-table GPU command or transaction starts."""
    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        result = with_gpu_session(do_test, conf=conf)
        plans = callback.getResultsWithTimeout(10000)
        assert len(plans) > 0, "No execution plans captured for catalog fallback"
        assert any(callback.didFallBack(plan, "ExecutedCommandExec") for plan in plans), \
            "Catalog command did not fall back through ExecutedCommandExec"
        forbidden_gpu_classes = [
            "GpuAtomicCreateTableAsSelectExec",
            "GpuAtomicReplaceTableAsSelectExec",
            "GpuDeleteCommand",
            "GpuMergeIntoCommand",
            "GpuRapidsDeltaWriteExec",
            "GpuUpdateCommand",
        ]
        for gpu_class in forbidden_gpu_classes:
            assert not any(callback.contains(plan, gpu_class) for plan in plans), \
                f"Fallback unexpectedly started {gpu_class}"
        return result
    finally:
        callback.endCapture()


def _assert_cached_read(spark, table, expected):
    """Materialize a cached read and prove it used an in-memory scan."""
    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        rows = [tuple(row) for row in spark.table(table).orderBy("id").collect()]
        plans = callback.getResultsWithTimeout(10000)
        assert any(callback.contains(plan, "InMemoryTableScanExec") or
                   callback.contains(plan, "GpuInMemoryTableScanExec") for plan in plans), \
            f"Read of {table} did not use its materialized cache"
        assert rows == expected
    finally:
        callback.endCapture()


def _normalize_catalog_properties(properties):
    normalized = dict(properties)
    normalized.pop(_UC_TABLE_ID_PROPERTY, None)
    normalized.pop("delta.lastCommitTimestamp", None)
    for key in [
            "delta.rowTracking.materializedRowIdColumnName",
            "delta.rowTracking.materializedRowCommitVersionColumnName",
            _COMMIT_TABLE_CONF_PROPERTY]:
        if key in normalized:
            normalized[key] = "<generated>"
    return normalized


def _normalize_operation_metrics(metrics):
    """Remove nondeterministic byte and timing metrics from a Delta history entry."""
    ignored = {
        "executionTimeMs", "materializeSourceTimeMs", "numAddedBytes", "numOutputBytes",
        "numRemovedBytes", "numTargetBytesAdded", "numTargetBytesInserted",
        "numTargetBytesRemoved", "numTargetBytesUpdated", "rewriteTimeMs", "scanTimeMs",
    }
    return {key: value for key, value in dict(metrics or {}).items() if key not in ignored}


def _normalize_operation_parameters(parameters):
    normalized = dict(parameters or {})
    properties = normalized.get("properties")
    if properties:
        decoded = json.loads(properties)
        normalized["properties"] = json.dumps(
            _normalize_catalog_properties(decoded), sort_keys=True)
    return normalized


def _stable_table_state(spark, table):
    """Return the stable public Delta state needed for CPU/GPU equivalence checks."""
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(recursive=True)
    history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().asDict(recursive=True)
    rows = [tuple(row) for row in spark.table(table).orderBy("id").collect()]
    return {
        "rows": rows,
        "schema": spark.table(table).schema.jsonValue(),
        "detail": {
            "clusteringColumns": detail.get("clusteringColumns"),
            "format": detail["format"],
            "minReaderVersion": detail["minReaderVersion"],
            "minWriterVersion": detail["minWriterVersion"],
            "numFiles": detail["numFiles"],
            "partitionColumns": detail["partitionColumns"],
            "properties": _normalize_catalog_properties(detail["properties"]),
            "tableFeatures": sorted(detail["tableFeatures"]),
        },
        "latestCommit": {
            "isBlindAppend": history["isBlindAppend"],
            "operation": history["operation"],
            "operationMetrics": _normalize_operation_metrics(history["operationMetrics"]),
            "operationParameters": _normalize_operation_parameters(
                history["operationParameters"]),
        },
    }


def _catalog_table_state(tables_api, table):
    table_info = tables_api.getTable(table, None, None)
    columns = table_info.getColumns()
    assert columns is not None, f"Unity Catalog returned no column metadata for {table}"
    return {
        "catalog": table_info.getCatalogName(),
        "columns": sorted([{
            "name": column.getName(),
            "nullable": column.getNullable(),
            "partitionIndex": column.getPartitionIndex(),
            "position": column.getPosition(),
            "typeText": column.getTypeText(),
        } for column in columns], key=lambda column: column["position"]),
        "dataSourceFormat": table_info.getDataSourceFormat().toString(),
        "properties": _normalize_catalog_properties(table_info.getProperties()),
        "schemaName": table_info.getSchemaName(),
        "tableType": table_info.getTableType().toString(),
    }


def _assert_catalog_tables_equivalent(cpu_table, gpu_table, tables_api, conf):
    cpu_state = with_cpu_session(
        lambda spark: _stable_table_state(spark, cpu_table), conf=conf)
    gpu_state = with_cpu_session(
        lambda spark: _stable_table_state(spark, gpu_table), conf=conf)
    assert cpu_state == gpu_state
    assert _catalog_table_state(tables_api, cpu_table) == \
        _catalog_table_state(tables_api, gpu_table)


def _preserved_table_state(spark, table):
    state = _stable_table_state(spark, table)
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(recursive=True)
    state["identity"] = {
        "activeFiles": sorted(spark.table(table).inputFiles()),
        "deltaId": detail["id"],
        "location": detail["location"],
        "version": spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"],
    }
    return state


def _catalog_identity_state(tables_api, table):
    table_info = tables_api.getTable(table, None, None)
    return {
        "id": table_info.getTableId(),
        "location": table_info.getStorageLocation(),
        "properties": dict(table_info.getProperties()),
    }


def _assert_catalog_commit_state(table, detail, table_info, conf):
    """Check public catalog-owned/coordinated-commit invariants for a registered table."""
    catalog_name, schema_name, table_name = table.split(".")
    assert table_info.getCatalogName() == catalog_name
    assert table_info.getSchemaName() == schema_name
    assert table_info.getName() == table_name
    assert table_info.getTableType().toString() == "MANAGED"
    assert table_info.getDataSourceFormat().toString() == "DELTA"
    assert table_info.getStorageLocation() == detail["location"]

    properties = detail["properties"]
    if _COMMIT_COORDINATOR_PROPERTY in properties:
        assert properties[_COMMIT_COORDINATOR_PROPERTY] == "unity-catalog"
        assert isinstance(json.loads(properties[_COMMIT_COORDINATOR_CONF_PROPERTY]), dict)
        assert isinstance(json.loads(properties[_COMMIT_TABLE_CONF_PROPERTY]), dict)
    else:
        # OSS UC 0.6.0 does not currently publish the preview coordinated-commit properties.
        # If that changes, require the complete property set above instead of accepting a
        # partially configured coordinator.
        assert _COMMIT_COORDINATOR_CONF_PROPERTY not in properties
        assert _COMMIT_TABLE_CONF_PROPERTY not in properties
    assert {"catalogManaged", "inCommitTimestamp", "vacuumProtocolCheck"}.issubset(
        set(detail["tableFeatures"]))

    catalog_properties = dict(table_info.getProperties())
    if "delta.lastUpdateVersion" in catalog_properties:
        latest_version = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"],
            conf=conf)
        # This is the most recent Delta version whose metadata was synchronized to the catalog,
        # not necessarily the current data-only commit version.
        assert 0 <= int(catalog_properties["delta.lastUpdateVersion"]) <= latest_version


def _error_class(action):
    try:
        action()
    except Exception as error:
        if hasattr(error, "getErrorClass"):
            error_class = error.getErrorClass()
            if error_class is not None:
                return error_class
        java_error = getattr(error, "java_exception", None)
        for _ in range(20):
            if java_error is None:
                break
            for method_name in ("getCondition", "getErrorClass"):
                try:
                    error_class = getattr(java_error, method_name)()
                    if error_class is not None:
                        return error_class
                except Exception:
                    pass
            try:
                java_error = java_error.getCause()
            except Exception:
                break
        return type(error).__name__
    raise AssertionError("Expected operation to fail")


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_ctas_insert_and_deletion_vector_scan(unity_catalog_server):
    table_name, table = _new_table_name("catalog_managed_smoke")
    conf = _catalog_conf(unity_catalog_server)

    try:
        def create_table(spark):
            return spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    '{_DEPRECATED_UC_TABLE_ID_PROPERTY}' = 'stale-caller-id')
                AS SELECT /*+ COALESCE(1) */ * FROM VALUES
                    (1L, 'one'), (2L, 'two'), (3L, 'three') AS source(id, value)
                """).collect()

        _assert_catalog_gpu_write(
            create_table, conf=conf,
            expected_command="GpuAtomicCreateTableAsSelectExec")
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(
                f"INSERT INTO {table} VALUES (4L, 'four')").collect(),
            conf=conf)

        session_catalog_matches = with_cpu_session(
            lambda spark: spark.sql(
                f"SHOW TABLES IN spark_catalog.default LIKE '{table_name}'").collect(),
            conf=conf)
        assert session_catalog_matches == []

        detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        table_info = unity_catalog_server["tables_api"].getTable(table, None, None)
        catalog_columns = _catalog_table_state(
            unity_catalog_server["tables_api"], table)["columns"]
        catalog_properties = dict(table_info.getProperties())
        assert [column["name"] for column in catalog_columns] == ["id", "value"]
        assert [column["position"] for column in catalog_columns] == [0, 1]
        assert detail["location"].startswith("s3://test-bucket0/")
        assert catalog_properties[_CATALOG_MANAGED_PROPERTY] == "supported"
        assert catalog_properties[_UC_TABLE_ID_PROPERTY] == table_info.getTableId()
        assert detail["properties"][_UC_TABLE_ID_PROPERTY] == table_info.getTableId()
        assert _DEPRECATED_UC_TABLE_ID_PROPERTY not in catalog_properties
        assert _DEPRECATED_UC_TABLE_ID_PROPERTY not in detail["properties"]
        assert detail["id"] != table_info.getTableId()
        assert detail["properties"]["delta.enableDeletionVectors"] == "true"
        assert detail["properties"]["delta.enableRowTracking"] == "true"
        assert detail["properties"]["delta.enableInCommitTimestamps"] == "true"
        assert detail["properties"]["delta.checkpointPolicy"] == "v2"
        _assert_catalog_commit_state(table, detail, table_info, conf)

        with_cpu_session(
            lambda spark: spark.sql(f"DELETE FROM {table} WHERE id = 2").collect(),
            conf=conf)
        delete_metrics = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
            .first()["operationMetrics"],
            conf=conf)
        assert int(delete_metrics["numDeletionVectorsAdded"]) > 0

        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"SELECT id, value FROM {table} ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)

        with pytest.raises(Exception):
            with_cpu_session(
                lambda spark: spark.read.format("delta").load(detail["location"]).collect(),
                conf=conf)
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_cpu_gpu_metadata_equivalence(unity_catalog_server):
    """CPU and GPU mutations publish equivalent public Delta and catalog state."""
    _, cpu_table = _new_table_name("catalog_managed_cpu_metadata")
    _, gpu_table = _new_table_name("catalog_managed_gpu_metadata")
    conf = _catalog_conf(unity_catalog_server)
    conf = {
        **conf,
        "spark.databricks.delta.delete.deletionVectors.persistent": "false",
        "spark.databricks.delta.optimizeWrite.enabled": "false",
    }

    def create(spark, table):
        return spark.sql(f"""
            CREATE TABLE {table}
            USING DELTA
            PARTITIONED BY (p)
            TBLPROPERTIES (
                '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                'delta.enableChangeDataFeed' = 'true',
                'user.test.property' = 'preserved')
            AS SELECT * FROM VALUES
                (1L, 'one', 0), (2L, 'two', 1) AS source(id, value, p)
            """).collect()

    try:
        with_cpu_session(lambda spark: create(spark, cpu_table), conf=conf)
        _assert_catalog_gpu_write(lambda spark: create(spark, gpu_table), conf=conf)

        cpu_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {cpu_table}").first().asDict(),
            conf=conf)
        gpu_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {gpu_table}").first().asDict(),
            conf=conf)
        cpu_info = unity_catalog_server["tables_api"].getTable(cpu_table, None, None)
        gpu_info = unity_catalog_server["tables_api"].getTable(gpu_table, None, None)

        assert cpu_info.getTableType().toString() == gpu_info.getTableType().toString() == \
            "MANAGED"
        assert cpu_info.getDataSourceFormat().toString() == \
            gpu_info.getDataSourceFormat().toString()
        assert _normalize_catalog_properties(cpu_info.getProperties()) == \
            _normalize_catalog_properties(gpu_info.getProperties())
        assert _normalize_catalog_properties(cpu_detail["properties"]) == \
            _normalize_catalog_properties(gpu_detail["properties"])
        assert cpu_detail["format"] == gpu_detail["format"] == "delta"
        assert cpu_detail["partitionColumns"] == gpu_detail["partitionColumns"] == ["p"]
        assert cpu_detail["properties"][_UC_TABLE_ID_PROPERTY] == cpu_info.getTableId()
        assert gpu_detail["properties"][_UC_TABLE_ID_PROPERTY] == gpu_info.getTableId()
        assert cpu_info.getTableId() != gpu_info.getTableId()
        _assert_catalog_commit_state(cpu_table, cpu_detail, cpu_info, conf)
        _assert_catalog_commit_state(gpu_table, gpu_detail, gpu_info, conf)
        assert with_cpu_session(
            lambda spark: spark.table(cpu_table).schema.simpleString(), conf=conf) == \
            with_cpu_session(
                lambda spark: spark.table(gpu_table).schema.simpleString(), conf=conf)
        _assert_catalog_tables_equivalent(
            cpu_table, gpu_table, unity_catalog_server["tables_api"], conf)

        def replace(spark, table):
            return spark.sql(f"""
                CREATE OR REPLACE TABLE {table}
                USING DELTA
                PARTITIONED BY (p)
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    'delta.enableChangeDataFeed' = 'true',
                    'user.test.property' = 'preserved')
                AS SELECT * FROM VALUES
                    (10L, 'ten', 0), (11L, 'eleven', 1) AS source(id, value, p)
                """).collect()

        with_cpu_session(lambda spark: replace(spark, cpu_table), conf=conf)
        _assert_catalog_gpu_write(lambda spark: replace(spark, gpu_table), conf=conf)
        _assert_catalog_tables_equivalent(
            cpu_table, gpu_table, unity_catalog_server["tables_api"], conf)

        def dynamic_overwrite(spark, table):
            return spark.createDataFrame(
                [(20, "twenty", 1)], "id LONG, value STRING, p INT") \
                .writeTo(table).overwritePartitions()

        with_cpu_session(lambda spark: dynamic_overwrite(spark, cpu_table), conf=conf)
        _assert_catalog_gpu_write(
            lambda spark: dynamic_overwrite(spark, gpu_table), conf=conf,
            expected_command="GpuDeltaDynamicPartitionOverwriteCommand")
        _assert_catalog_tables_equivalent(
            cpu_table, gpu_table, unity_catalog_server["tables_api"], conf)
    finally:
        _drop_table(cpu_table, conf)
        _drop_table(gpu_table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_atomic_replace_time_travel_and_cdf(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_replace")
    conf = _catalog_conf(unity_catalog_server)

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    'delta.enableChangeDataFeed' = 'true')
                AS SELECT * FROM VALUES
                    (1L, 'original-one'), (2L, 'original-two') AS source(id, value)
                """).collect(),
            conf=conf)

        original_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        original_table_id = unity_catalog_server["tables_api"].getTable(
            table, None, None).getTableId()

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE OR REPLACE TABLE {table}
                USING DELTA
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    'delta.enableChangeDataFeed' = 'true')
                AS SELECT * FROM VALUES
                    (3L, 'replacement-three'), (4L, 'replacement-four')
                    AS source(id, value)
                """).collect(),
            conf=conf)

        replaced_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        replaced_table_id = unity_catalog_server["tables_api"].getTable(
            table, None, None).getTableId()
        assert replaced_detail["id"] == original_detail["id"]
        assert replaced_detail["location"] == original_detail["location"]
        assert replaced_table_id == original_table_id
        assert _table_rows(table, conf) == [
            (3, "replacement-three"), (4, "replacement-four")]

        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(
                f"SELECT id, value FROM {table} VERSION AS OF 0 ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"""
                SELECT id, value, _change_type, _commit_version
                FROM table_changes('{table}', 0)
                ORDER BY _commit_version, _change_type, id
                """),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)
    finally:
        _drop_table(table, conf)


@allow_non_gpu("CreateTableExec", "AtomicReplaceTableExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_create_replace_and_rtas(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_create_replace")
    conf = _catalog_conf(unity_catalog_server)

    try:
        with_gpu_session(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table} (id BIGINT, value STRING)
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                """).collect(),
            conf=conf)
        original_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        original_table_id = unity_catalog_server["tables_api"].getTable(
            table, None, None).getTableId()

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(
                f"INSERT INTO {table} VALUES (1L, 'one')").collect(),
            conf=conf)
        with_gpu_session(
            lambda spark: spark.sql(f"""
                REPLACE TABLE {table} (id BIGINT, value STRING)
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == []

        replaced_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        replaced_table_id = unity_catalog_server["tables_api"].getTable(
            table, None, None).getTableId()
        assert replaced_detail["id"] == original_detail["id"]
        assert replaced_detail["location"] == original_detail["location"]
        assert replaced_table_id == original_table_id

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                REPLACE TABLE {table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT * FROM VALUES
                    (2L, 'two'), (3L, 'three') AS source(id, value)
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(2, "two"), (3, "three")]
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_liquid_clustering(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_clustered")
    conf = _catalog_conf(unity_catalog_server)

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                CLUSTER BY (id)
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT * FROM VALUES
                    (1L, 'one'), (2L, 'two') AS source(id, value)
                """).collect(),
            conf=conf)

        detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        catalog_properties = dict(
            unity_catalog_server["tables_api"].getTable(table, None, None).getProperties())
        assert detail["clusteringColumns"] == ["id"]
        assert catalog_properties["clusteringColumns"] == '[["id"]]'
        assert catalog_properties["delta.feature.clustering"] == "supported"

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(
                f"INSERT INTO {table} VALUES (3L, 'three')").collect(),
            conf=conf)
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"SELECT id, value FROM {table} ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)

        updated_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(),
            conf=conf)
        assert updated_detail["clusteringColumns"] == ["id"]
    finally:
        _drop_table(table, conf)


def _create_catalog_managed_table(spark, table):
    return spark.sql(f"""
        CREATE TABLE {table}
        USING DELTA
        TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
        AS SELECT 1L AS id, 'original' AS value
        """).collect()


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_metadata_changing_replace_rejected(unity_catalog_server):
    """A replace that changes the schema is rejected identically on CPU and GPU."""
    _, cpu_table = _new_table_name("catalog_managed_cpu_reject")
    _, gpu_table = _new_table_name("catalog_managed_gpu_reject")
    conf = _catalog_conf(unity_catalog_server)

    def metadata_changing_replace(spark, table):
        return spark.sql(f"""
            CREATE OR REPLACE TABLE {table}
            USING DELTA
            TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
            AS SELECT 2L AS id, 'replacement' AS value, 'new' AS extra
            """).collect()

    try:
        with_cpu_session(lambda spark: _create_catalog_managed_table(spark, cpu_table), conf=conf)
        _assert_catalog_gpu_write(
            lambda spark: _create_catalog_managed_table(spark, gpu_table), conf=conf)
        cpu_before = with_cpu_session(
            lambda spark: _preserved_table_state(spark, cpu_table), conf=conf)
        gpu_before = with_cpu_session(
            lambda spark: _preserved_table_state(spark, gpu_table), conf=conf)
        cpu_catalog_before = _catalog_identity_state(
            unity_catalog_server["tables_api"], cpu_table)
        gpu_catalog_before = _catalog_identity_state(
            unity_catalog_server["tables_api"], gpu_table)

        cpu_error = _error_class(
            lambda: with_cpu_session(
                lambda spark: metadata_changing_replace(spark, cpu_table), conf=conf))
        gpu_error = _error_class(
            lambda: with_gpu_session(
                lambda spark: metadata_changing_replace(spark, gpu_table), conf=conf))
        assert cpu_error == gpu_error == "DELTA_OPERATION_NOT_ALLOWED"
        assert _table_rows(cpu_table, conf) == [(1, "original")]
        assert _table_rows(gpu_table, conf) == [(1, "original")]
        assert with_cpu_session(
            lambda spark: _preserved_table_state(spark, cpu_table), conf=conf) == cpu_before
        assert with_cpu_session(
            lambda spark: _preserved_table_state(spark, gpu_table), conf=conf) == gpu_before
        assert _catalog_identity_state(
            unity_catalog_server["tables_api"], cpu_table) == cpu_catalog_before
        assert _catalog_identity_state(
            unity_catalog_server["tables_api"], gpu_table) == gpu_catalog_before
    finally:
        _drop_table(cpu_table, conf)
        _drop_table(gpu_table, conf)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_create_or_replace_missing_table(unity_catalog_server):
    """CREATE OR REPLACE of a missing table creates it through Unity Catalog only."""
    table_name, table = _new_table_name("catalog_managed_or_create")
    conf = _catalog_conf(unity_catalog_server)

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE OR REPLACE TABLE {table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT 3L AS id, 'created' AS value
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(3, "created")]
        assert with_cpu_session(
            lambda spark: spark.sql(
                f"SHOW TABLES IN spark_catalog.default LIKE '{table_name}'").collect(),
            conf=conf) == []
    finally:
        _drop_table(table, conf)


@allow_non_gpu("CreateTableExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_idempotent_create_and_overwrite_forms(unity_catalog_server):
    """Exercise idempotent create plus SQL-static and V2-filtered partition overwrites."""
    _, table = _new_table_name("catalog_managed_overwrite_forms")
    conf = {
        **_catalog_conf(unity_catalog_server),
        "spark.databricks.delta.delete.deletionVectors.persistent": "false",
    }

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                PARTITIONED BY (p)
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT * FROM VALUES
                    (1L, 'one', 0), (2L, 'two', 1) AS source(id, value, p)
                """).collect(),
            conf=conf)
        before = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)

        # The existing table must not be restaged or replaced.
        with_gpu_session(
            lambda spark: spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table} (id LONG, value STRING, p INT)
                USING DELTA PARTITIONED BY (p)
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(1, "one", 0), (2, "two", 1)]

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                INSERT OVERWRITE TABLE {table} PARTITION (p = 0)
                SELECT 10L AS id, 'ten' AS value
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(2, "two", 1), (10, "ten", 0)]

        _assert_catalog_gpu_write(
            lambda spark: spark.createDataFrame(
                [(20, "twenty", 1)], "id LONG, value STRING, p INT")
                .writeTo(table).overwrite(F.col("p") == 1),
            conf=conf)
        assert _table_rows(table, conf) == [(10, "ten", 0), (20, "twenty", 1)]

        after = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
        assert after["id"] == before["id"]
        assert after["location"] == before["location"]
    finally:
        _drop_table(table, conf)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_failed_ctas_leaves_no_table(unity_catalog_server):
    """A CTAS that fails while running leaves nothing registered in the catalog."""
    table_name, table = _new_table_name("catalog_managed_failed_create")
    conf = _catalog_conf(unity_catalog_server)

    try:
        with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
            with_gpu_session(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {table}
                    USING DELTA
                    TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                    AS SELECT id, 1L / (id - id) AS invalid
                    FROM range(1, 3)
                    """).collect(),
                conf=conf)
        assert with_cpu_session(
            lambda spark: spark.sql(
                f"SHOW TABLES IN {_CATALOG}.{_SCHEMA} LIKE '{table_name}'").collect(),
            conf=conf) == []
    finally:
        _drop_table(table, conf)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
@pytest.mark.parametrize("fail_suffix", [".parquet", ".json"],
                         ids=["data-file", "delta-commit"])
def test_catalog_managed_failed_storage_write_can_retry(unity_catalog_server, fail_suffix):
    """Failed data and log writes abort catalog staging and leave the table name reusable."""
    table_name, table = _new_table_name("catalog_managed_failed_storage_write")
    conf = _catalog_conf(unity_catalog_server)
    credential_fs = spark_jvm().com.nvidia.spark.rapids.tests.delta.CredentialTestFileSystem

    try:
        credential_fs.failNextCreateEndingWith(fail_suffix)
        with pytest.raises(Exception, match="Injected create failure"):
            with_gpu_session(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {table}
                    USING DELTA
                    TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                    AS SELECT 1L AS id, 'failed' AS value
                    """).collect(),
                conf=conf)
        assert with_cpu_session(
            lambda spark: spark.sql(
                f"SHOW TABLES IN {_CATALOG}.{_SCHEMA} LIKE '{table_name}'").collect(),
            conf=conf) == []

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT 2L AS id, 'retry' AS value
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(2, "retry")]
        history = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table}").collect(), conf=conf)
        assert len(history) == 1
    finally:
        credential_fs.clearInjectedFailure()
        _drop_table(table, conf)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
@pytest.mark.parametrize("failure_stage", ["data-file", "delta-commit"],
                         ids=["data-file", "delta-commit"])
def test_catalog_managed_failed_rtas_preserves_table(unity_catalog_server, failure_stage):
    """Replacement data and log failures preserve the table and can be retried safely."""
    _, table = _new_table_name("catalog_managed_failed_rtas")
    conf = _catalog_conf(unity_catalog_server)
    credential_fs = spark_jvm().com.nvidia.spark.rapids.tests.delta.CredentialTestFileSystem

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    'user.atomicity.property' = 'preserved')
                AS SELECT 1L AS id, 'original' AS value
                """).collect(),
            conf=conf)
        before_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
        before_state = with_cpu_session(
            lambda spark: _preserved_table_state(spark, table), conf=conf)
        before_catalog = _catalog_identity_state(
            unity_catalog_server["tables_api"], table)
        before_version = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"],
            conf=conf)

        protected_modes = {}
        try:
            if failure_stage == "data-file":
                credential_fs.failNextCreateEndingWith(".parquet")
            else:
                # CredentialTestFileSystem maps the synthetic s3 URI path directly onto local
                # storage. Make both possible commit destinations read-only: Delta 4.2 can
                # publish through _staged_commits before materializing the numbered JSON file.
                delta_log_path = os.path.join(
                    urlparse(before_detail["location"]).path, "_delta_log")
                protected_paths = [delta_log_path]
                staged_commits_path = os.path.join(delta_log_path, "_staged_commits")
                if os.path.isdir(staged_commits_path):
                    protected_paths.append(staged_commits_path)
                for path in protected_paths:
                    protected_modes[path] = stat.S_IMODE(os.stat(path).st_mode)
                    os.chmod(path, 0o500)

            error_match = "Injected create failure" if failure_stage == "data-file" else None
            with pytest.raises(Exception, match=error_match):
                with_gpu_session(
                    lambda spark: spark.sql(f"""
                        REPLACE TABLE {table}
                        USING DELTA
                        TBLPROPERTIES (
                            '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                            'user.atomicity.property' = 'preserved')
                        AS SELECT 2L AS id, 'failed' AS value
                        """).collect(),
                    conf=conf)
        finally:
            restore_error = None
            for path, mode in protected_modes.items():
                try:
                    os.chmod(path, mode)
                except OSError as error:
                    restore_error = restore_error or error
            if restore_error is not None:
                raise restore_error

        after_failure_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
        assert _table_rows(table, conf) == [(1, "original")]
        assert after_failure_detail["id"] == before_detail["id"]
        assert after_failure_detail["location"] == before_detail["location"]
        assert after_failure_detail["properties"]["user.atomicity.property"] == "preserved"
        assert with_cpu_session(
            lambda spark: _preserved_table_state(spark, table), conf=conf) == before_state
        assert _catalog_identity_state(
            unity_catalog_server["tables_api"], table) == before_catalog
        assert with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"],
            conf=conf) == before_version

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                REPLACE TABLE {table}
                USING DELTA
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    'user.atomicity.property' = 'preserved')
                AS SELECT 3L AS id, 'replacement' AS value
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(3, "replacement")]
        assert with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"],
            conf=conf) == before_version + 1
        retry_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
        assert retry_detail["id"] == before_detail["id"]
        assert retry_detail["location"] == before_detail["location"]
        assert _catalog_identity_state(
            unity_catalog_server["tables_api"], table) == before_catalog
    finally:
        credential_fs.clearInjectedFailure()
        _drop_table(table, conf)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
@pytest.mark.parametrize("statement", [
    "OPTIMIZE {table}",
    "REORG TABLE {table} APPLY (PURGE)"], ids=["optimize", "reorg_purge"])
def test_catalog_managed_unsupported_operation_rejected(unity_catalog_server, statement):
    """Operations Delta forbids on catalog-managed tables fail the same way on CPU and GPU."""
    _, table = _new_table_name("catalog_managed_unsupported")
    conf = _catalog_conf(unity_catalog_server)
    sql = statement.format(table=table)

    try:
        _assert_catalog_gpu_write(
            lambda spark: _create_catalog_managed_table(spark, table), conf=conf)
        before = with_cpu_session(
            lambda spark: _preserved_table_state(spark, table), conf=conf)
        catalog_before = _catalog_identity_state(unity_catalog_server["tables_api"], table)
        cpu_error = _error_class(
            lambda: with_cpu_session(lambda spark: spark.sql(sql).collect(), conf=conf))
        gpu_error = _error_class(
            lambda: with_gpu_session(lambda spark: spark.sql(sql).collect(), conf=conf))
        assert cpu_error == gpu_error == "DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION"
        assert with_cpu_session(
            lambda spark: _preserved_table_state(spark, table), conf=conf) == before
        assert _catalog_identity_state(
            unity_catalog_server["tables_api"], table) == catalog_before
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_v1_v2_and_overwrite_writes(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_writes")
    conf = _catalog_conf(unity_catalog_server)
    optimized_conf = {
        **conf,
        "spark.databricks.delta.optimizeWrite.enabled": "true",
    }

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                PARTITIONED BY (p)
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT * FROM VALUES
                    (1L, 'one', 0), (2L, 'two', 1) AS source(id, value, p)
                """).collect(),
            conf=conf)

        _assert_catalog_gpu_write(
            lambda spark: spark.range(3, 2003)
                .selectExpr("id", "CAST(id AS STRING) AS value", "0 AS p")
                .repartition(20)
                .write.format("delta").mode("append").saveAsTable(table),
            conf=optimized_conf)
        optimized_metrics = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
            .first()["operationMetrics"],
            conf=conf)
        # The 20 input partitions are coalesced by Delta optimized write inside the transaction;
        # that internal plan is not exposed as a separately captured QueryExecution.
        assert int(optimized_metrics["numFiles"]) == 1
        _assert_catalog_gpu_write(
            lambda spark: spark.createDataFrame(
                [(4, "four", 1)], "id LONG, value STRING, p INT")
                .writeTo(table).append(),
            conf=conf)
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                INSERT INTO {table} REPLACE WHERE p = 0
                VALUES (5L, 'five', 0)
                """).collect(),
            conf=conf)
        assert _table_rows(table, conf) == [
            (2, "two", 1), (4, "four", 1), (5, "five", 0)]

        dynamic_conf = {
            **conf,
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.databricks.delta.delete.deletionVectors.persistent": "false",
        }
        _assert_catalog_gpu_write(
            lambda spark: spark.createDataFrame(
                [(6, "six", 1)], "id LONG, value STRING, p INT")
                .writeTo(table).overwritePartitions(),
            conf=dynamic_conf,
            expected_command="GpuDeltaDynamicPartitionOverwriteCommand")
        assert _table_rows(table, conf) == [(5, "five", 0), (6, "six", 1)]

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(
                f"INSERT OVERWRITE {table} VALUES (7L, 'seven', 2)").collect(),
            conf=conf)
        assert _table_rows(table, conf) == [(7, "seven", 2)]
    finally:
        _drop_table(table, conf)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_persistent_dv_dynamic_overwrite_falls_back(unity_catalog_server):
    """Dynamic partition overwrite stays on CPU when it would persist deletion vectors."""
    _, table = _new_table_name("catalog_managed_dpo_dv_fallback")
    conf = {
        **_catalog_conf(unity_catalog_server),
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
    }

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                PARTITIONED BY (p)
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT * FROM VALUES
                    (1L, 'one', 0), (2L, 'two', 1) AS source(id, value, p)
                """).collect(),
            conf=conf)
        _assert_catalog_command_fallback(
            lambda spark: spark.createDataFrame(
                [(3, "three", 1)], "id LONG, value STRING, p INT")
                .writeTo(table).overwritePartitions(),
            conf=conf)
        assert _table_rows(table, conf) == [(1, "one", 0), (3, "three", 1)]
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_schema_merge_and_overwrite(unity_catalog_server):
    _, cpu_table = _new_table_name("catalog_managed_cpu_schema")
    _, gpu_table = _new_table_name("catalog_managed_gpu_schema")
    conf = _catalog_conf(unity_catalog_server)

    def create(spark, table):
        return spark.sql(f"""
            CREATE TABLE {table}
            USING DELTA
            TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
            AS SELECT 1L AS id, 'original' AS value
            """).collect()

    def append_with_schema_merge(spark, table, with_extra_column):
        schema = "id LONG, value STRING, extra STRING" if with_extra_column else \
            "id LONG, value STRING"
        row = (2, "merged", "extra") if with_extra_column else (2, "merged")
        return spark.createDataFrame([row], schema) \
            .write.format("delta").mode("append") \
            .option("mergeSchema", "true").saveAsTable(table)

    def overwrite_schema(spark, table):
        return spark.createDataFrame([(3, "replacement")], "id LONG, replacement STRING") \
            .write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(table)

    try:
        with_cpu_session(lambda spark: create(spark, cpu_table), conf=conf)
        _assert_catalog_gpu_write(lambda spark: create(spark, gpu_table), conf=conf)

        _assert_catalog_gpu_write(
            lambda spark: append_with_schema_merge(spark, gpu_table, False),
            conf=conf)
        assert _table_rows(gpu_table, conf) == [(1, "original"), (2, "merged")]

        cpu_merge_error = _error_class(
            lambda: with_cpu_session(
                lambda spark: append_with_schema_merge(spark, cpu_table, True), conf=conf))
        gpu_merge_error = _error_class(
            lambda: with_gpu_session(
                lambda spark: append_with_schema_merge(spark, gpu_table, True), conf=conf))
        assert cpu_merge_error == gpu_merge_error == "DELTA_OPERATION_NOT_ALLOWED"

        cpu_overwrite_error = _error_class(
            lambda: with_cpu_session(
                lambda spark: overwrite_schema(spark, cpu_table), conf=conf))
        gpu_overwrite_error = _error_class(
            lambda: with_gpu_session(
                lambda spark: overwrite_schema(spark, gpu_table), conf=conf))
        assert cpu_overwrite_error == gpu_overwrite_error == "DELTA_OPERATION_NOT_ALLOWED"
        assert _table_rows(cpu_table, conf) == [(1, "original")]
        assert _table_rows(gpu_table, conf) == [(1, "original"), (2, "merged")]
    finally:
        _drop_table(cpu_table, conf)
        _drop_table(gpu_table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_external_table_regression(unity_catalog_server):
    _, table = _new_table_name("unity_external")
    location = f"s3://test-bucket0{unity_catalog_server['storage_root']}/{uuid.uuid4().hex}"
    conf = _catalog_conf(unity_catalog_server)

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                LOCATION '{location}'
                AS SELECT * FROM VALUES
                    (1L, 'one'), (2L, 'two') AS source(id, value)
                """).collect(),
            conf=conf)
        table_info = unity_catalog_server["tables_api"].getTable(table, None, None)
        assert table_info.getTableType().toString() == "EXTERNAL"
        assert table_info.getStorageLocation() == location
        assert _CATALOG_MANAGED_PROPERTY not in dict(table_info.getProperties())

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(
                f"INSERT INTO {table} VALUES (3L, 'three')").collect(),
            conf=conf)
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(f"SELECT id, value FROM {table} ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)
    finally:
        _drop_table(table, conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_path_and_session_catalog_regression(
        unity_catalog_server, spark_tmp_path, spark_tmp_table_factory):
    """Enabling the UC path does not change ordinary path or session-catalog Delta writes."""
    path = f"{spark_tmp_path}/catalog_managed_path_regression"
    session_table = f"spark_catalog.default.{spark_tmp_table_factory.get()}"
    conf = _catalog_conf(unity_catalog_server)

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE delta.`{path}`
                USING DELTA
                AS SELECT 1L AS id, 'path-one' AS value
                """).collect(),
            conf=conf)
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(
                f"INSERT INTO delta.`{path}` VALUES (2L, 'path-two')").collect(),
            conf=conf)
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(
                f"SELECT id, value FROM delta.`{path}` ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {session_table}
                USING DELTA
                AS SELECT 3L AS id, 'session-three' AS value
                """).collect(),
            conf=conf)
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(
                f"INSERT INTO {session_table} VALUES (4L, 'session-four')").collect(),
            conf=conf)
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: spark.sql(
                f"SELECT id, value FROM {session_table} ORDER BY id"),
            exist_classes="GpuFileSourceScanExec",
            conf=conf,
            require_non_empty=True)
        detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {session_table}").first().asDict(),
            conf=conf)
        assert _CATALOG_MANAGED_PROPERTY not in detail["properties"]
        assert _UC_TABLE_ID_PROPERTY not in detail["properties"]
    finally:
        with_cpu_session(
            lambda spark: spark.sql(f"DROP TABLE IF EXISTS {session_table}").collect(),
            conf=conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_uc_shape_guard(unity_catalog_server):
    """An unrecognized UC delegate is rejected before GPU catalog conversion."""
    conf = _catalog_conf(unity_catalog_server)

    def check_guard(spark):
        jvm = spark_jvm()
        manager = spark._jsparkSession.sessionState().catalogManager()
        catalog = manager.catalog(_CATALOG)
        guard = jvm.com.nvidia.spark.rapids.delta.delta42x.GpuDeltaCatalog
        normal_reason = guard.unsupportedUnityCatalogReason(catalog)
        assert not normal_reason.isDefined(), normal_reason.get()

        delegate_field = catalog.getClass().getDeclaredField("delegate")
        delegate_field.setAccessible(True)
        original_delegate = delegate_field.get(catalog)
        try:
            # A null delegate simulates an incompatible UC implementation without constructing
            # another catalog in the process JVM. The guard must fail closed and explain why.
            delegate_field.set(catalog, None)
            reason = guard.unsupportedUnityCatalogReason(catalog)
            assert reason.isDefined()
            assert "internals are not recognized" in reason.get()
        finally:
            delegate_field.set(catalog, original_delegate)

    with_gpu_session(check_guard, conf=conf)


@allow_non_gpu(*delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_delete_update_and_merge(unity_catalog_server):
    _, table = _new_table_name("catalog_managed_dml")
    conf = {
        **_catalog_conf(unity_catalog_server),
        "spark.databricks.delta.delete.deletionVectors.persistent": "false",
        "spark.databricks.delta.update.deletionVectors.persistent": "false",
        "spark.databricks.delta.merge.deletionVectors.persistent": "false",
    }

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES (
                    '{_CATALOG_MANAGED_PROPERTY}' = 'supported',
                    'delta.enableChangeDataFeed' = 'true')
                AS SELECT /*+ COALESCE(1) */ * FROM VALUES
                    (1L, 'one'), (2L, 'two'), (3L, 'three') AS source(id, value)
                """).collect(),
            conf=conf)

        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"DELETE FROM {table} WHERE id = 1").collect(),
            conf=conf,
            expected_command="GpuDeleteCommand",
            expected_classes=["GpuFileSourceScanExec"])
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(
                f"UPDATE {table} SET value = 'updated-two' WHERE id = 2").collect(),
            conf=conf,
            expected_command="GpuUpdateCommand",
            expected_classes=["GpuFileSourceScanExec"])

        def merge(spark):
            source = f"catalog_managed_merge_source_{uuid.uuid4().hex}"
            spark.createDataFrame(
                [(2, "merged-two"), (4, "four")], "id LONG, value STRING") \
                .createOrReplaceTempView(source)
            return spark.sql(f"""
                MERGE INTO {table} AS target
                USING {source} AS source
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET value = source.value
                WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
                """).collect()

        _assert_catalog_gpu_write(
            merge, conf=conf, expected_command="GpuMergeIntoCommand",
            expected_classes=["GpuFileSourceScanExec"])
        assert _table_rows(table, conf) == [
            (2, "merged-two"), (3, "three"), (4, "four")]
        _assert_three_dml_change_feed(table, conf)
    finally:
        _drop_table(table, conf)


@allow_non_gpu("ExecutedCommandExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_persistent_dv_dml_falls_back(unity_catalog_server):
    """Keep persistent-DV mutation out of the base PR until its dedicated support lands."""
    _, table = _new_table_name("catalog_managed_persistent_dv_fallback")
    conf = {
        **_catalog_conf(unity_catalog_server),
        "spark.databricks.delta.delete.deletionVectors.persistent": "true",
        "spark.databricks.delta.update.deletionVectors.persistent": "true",
        "spark.databricks.delta.merge.deletionVectors.persistent": "true",
    }

    try:
        _assert_catalog_gpu_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table}
                USING DELTA
                TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                AS SELECT /*+ COALESCE(1) */ * FROM VALUES
                    (1L, 'one'), (2L, 'two'), (3L, 'three') AS source(id, value)
                """).collect(),
            conf=conf)

        _assert_catalog_command_fallback(
            lambda spark: spark.sql(f"DELETE FROM {table} WHERE id = 1").collect(),
            conf=conf)

        # The asymmetric settings catch accidental use of DELETE's flag for UPDATE tagging.
        update_conf = {
            **conf,
            "spark.databricks.delta.delete.deletionVectors.persistent": "false",
            "spark.databricks.delta.update.deletionVectors.persistent": "true",
        }
        _assert_catalog_command_fallback(
            lambda spark: spark.sql(
                f"UPDATE {table} SET value = 'updated-two' WHERE id = 2").collect(),
            conf=update_conf)

        def merge(spark):
            source = f"catalog_managed_fallback_source_{uuid.uuid4().hex}"
            spark.createDataFrame(
                [(2, "merged-two"), (4, "four")], "id LONG, value STRING") \
                .createOrReplaceTempView(source)
            return spark.sql(f"""
                MERGE INTO {table} AS target
                USING {source} AS source
                ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET value = source.value
                WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
                """).collect()

        _assert_catalog_command_fallback(merge, conf=conf)
        assert _table_rows(table, conf) == [
            (2, "merged-two"), (3, "three"), (4, "four")]
        metrics = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 3").collect(), conf=conf)
        assert any(int(row["operationMetrics"].get("numDeletionVectorsAdded", "0")) > 0
                   for row in metrics)
    finally:
        _drop_table(table, conf)


@allow_non_gpu("CacheTableExec", "InMemoryTableScanExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_cached_rtas(unity_catalog_server):
    """RTAS refreshes the managed table and its dependent cached view."""
    _, table = _new_table_name("catalog_managed_cached_rtas")
    dependent_view = f"catalog_managed_cached_rtas_view_{uuid.uuid4().hex}"
    conf = _catalog_conf(unity_catalog_server)

    def check_cache_refresh(spark):
        try:
            _create_catalog_managed_table(spark, table)
            spark.sql(f"CACHE TABLE {table}").collect()
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {dependent_view} "
                      f"AS SELECT id, value FROM {table}").collect()
            spark.sql(f"CACHE TABLE {dependent_view}").collect()
            before_table = [tuple(row) for row in spark.table(table).orderBy("id").collect()]
            before_view = [
                tuple(row) for row in spark.table(dependent_view).orderBy("id").collect()]
            assert before_table == before_view == [(1, "original")]
            assert spark.catalog.isCached(table)
            assert spark.catalog.isCached(dependent_view)
            _assert_cached_read(spark, table, [(1, "original")])
            _assert_cached_read(spark, dependent_view, [(1, "original")])

            callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
            callback.startCapture()
            try:
                spark.sql(f"""
                    REPLACE TABLE {table}
                    USING DELTA
                    TBLPROPERTIES ('{_CATALOG_MANAGED_PROPERTY}' = 'supported')
                    AS SELECT * FROM VALUES
                        (2L, 'replacement-two'), (3L, 'replacement-three')
                        AS source(id, value)
                    """).collect()
                plans = callback.getResultsWithTimeout(10000)
                assert any(callback.contains(plan, "GpuAtomicReplaceTableAsSelectExec")
                           for plan in plans), \
                    "GpuAtomicReplaceTableAsSelectExec was not executed"
            finally:
                callback.endCapture()

            expected = [(2, "replacement-two"), (3, "replacement-three")]
            after_table = [tuple(row) for row in spark.table(table).orderBy("id").collect()]
            after_view = [
                tuple(row) for row in spark.table(dependent_view).orderBy("id").collect()]
            assert after_table == after_view == expected
            # Spark invalidates RTAS caches rather than retaining a stale cached relation.
            assert not spark.catalog.isCached(table)
            assert not spark.catalog.isCached(dependent_view)
        finally:
            spark.sql(f"DROP VIEW IF EXISTS {dependent_view}").collect()

    try:
        with_gpu_session(check_cache_refresh, conf=conf)
    finally:
        _drop_table(table, conf)


@allow_non_gpu("CacheTableExec", "InMemoryTableScanExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_catalog_managed_cached_v1_writes(unity_catalog_server):
    """V1 append refreshes and V1 overwrite invalidates a managed-table cache."""
    _, table = _new_table_name("catalog_managed_cached_v1")
    conf = _catalog_conf(unity_catalog_server)

    def check_cache_refresh(spark):
        _create_catalog_managed_table(spark, table)
        spark.sql(f"CACHE TABLE {table}").collect()
        assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [
            (1, "original")]
        assert spark.catalog.isCached(table)
        _assert_cached_read(spark, table, [(1, "original")])

        callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback

        def run_and_assert_gpu_write(write):
            callback.startCapture()
            try:
                write()
                plans = callback.getResultsWithTimeout(10000)
                assert len(plans) > 0, "No execution plans captured for catalog-managed write"
                assert any(callback.contains(plan, "GpuRapidsDeltaWriteExec") for plan in plans), \
                    "GpuRapidsDeltaWriteExec was not executed"
                assert all(
                    not callback.didFallBack(plan, "RapidsDeltaWriteExec") for plan in plans), \
                    "Catalog-managed V1 write used CPU RapidsDeltaWriteExec"
            finally:
                callback.endCapture()

        run_and_assert_gpu_write(
            lambda: spark.createDataFrame([(2, "appended")], "id LONG, value STRING")
            .write.format("delta").mode("append").saveAsTable(table))
        assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [
            (1, "original"), (2, "appended")]
        assert spark.catalog.isCached(table)
        _assert_cached_read(spark, table, [(1, "original"), (2, "appended")])

        run_and_assert_gpu_write(
            lambda: spark.createDataFrame([(3, "overwritten")], "id LONG, value STRING")
            .write.format("delta").mode("overwrite").saveAsTable(table))
        assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [
            (3, "overwritten")]
        assert not spark.catalog.isCached(table)

    try:
        with_gpu_session(check_cache_refresh, conf=conf)
    finally:
        _drop_table(table, conf)
