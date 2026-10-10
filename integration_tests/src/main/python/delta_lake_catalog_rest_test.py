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

import http.client
import json
import os
import re
import threading
import uuid
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pytest

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from conftest import spark_jvm
from delta_lake_catalog_managed_test import (
    _assert_catalog_commit_state, _catalog_conf, _catalog_identity_state, _drop_table,
    _error_class, _new_table_name, _normalize_catalog_properties, _preserved_table_state,
    _table_rows,
    unity_catalog_server)
from delta_lake_utils import (assert_rapids_delta_write, delta_meta_allow,
                              is_oss_delta_lake_43)
from marks import allow_non_gpu, delta_lake, unity_catalog
from spark_session import with_cpu_session, with_gpu_session


pytestmark = pytest.mark.skipif(
    not is_oss_delta_lake_43(),
    reason="Unity Catalog Delta REST API routing requires OSS Delta Lake 4.3.0")

_SERVER_REJECTION_MESSAGE = "Injected server-side Delta commit rejection"


class _DeltaCommitRejectingProxy:
    """Forward Unity Catalog traffic and reject one armed Delta commit request."""

    _HOP_BY_HOP_HEADERS = {
        "connection", "content-length", "host", "http2-settings", "keep-alive",
        "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade",
    }

    def __init__(self, target_uri):
        target = urlparse(target_uri)
        assert target.scheme == "http" and target.hostname and target.port
        self._target = target
        self._lock = threading.Lock()
        self._reject_next = False
        self.rejected_body = None
        self.commit_requests = []
        self._create_request_paths = []
        self.errors = []
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_DELETE(self):
                proxy._dispatch(self)

            def do_GET(self):
                proxy._dispatch(self)

            def do_PATCH(self):
                proxy._dispatch(self)

            def do_POST(self):
                proxy._dispatch(self)

            def do_PUT(self):
                proxy._dispatch(self)

            def log_message(self, _format, *_args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def uri(self):
        return f"http://127.0.0.1:{self._server.server_port}/"

    def reject_next_commit(self):
        with self._lock:
            self._reject_next = True
            self.rejected_body = None

    def commit_request_count(self):
        with self._lock:
            return len(self.commit_requests)

    def commit_requests_since(self, index):
        with self._lock:
            return self.commit_requests[index:]

    def create_request_paths(self):
        with self._lock:
            return list(self._create_request_paths)

    def _record_and_take_rejection(self, handler, body):
        path = handler.path.split("?", 1)[0]
        is_commit = handler.command == "POST" and "/delta/v1/" in path and \
            b'"add-commit"' in body
        is_create = handler.command == "POST" and "/delta/v1/" in path and \
            path.endswith(("/staging-tables", "/tables"))
        with self._lock:
            if is_commit:
                self.commit_requests.append(json.loads(body))
            if is_create:
                self._create_request_paths.append(path)
            if not is_commit or not self._reject_next:
                return False
            self._reject_next = False
            self.rejected_body = body.decode("utf-8")
            return True

    @staticmethod
    def _send(handler, status, headers, body):
        handler.send_response(status)
        for key, value in headers:
            if key.lower() not in _DeltaCommitRejectingProxy._HOP_BY_HOP_HEADERS:
                handler.send_header(key, value)
        handler.send_header("Content-Length", str(len(body)))
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.write(body)
        handler.close_connection = True

    def _handle(self, handler):
        content_length = int(handler.headers.get("Content-Length", "0"))
        body = handler.rfile.read(content_length) if content_length else b""
        if self._record_and_take_rejection(handler, body):
            response = json.dumps({
                "error_code": "INJECTED_DELTA_COMMIT_REJECTION",
                "message": _SERVER_REJECTION_MESSAGE,
            }).encode("utf-8")
            self._send(handler, 400, [("Content-Type", "application/json")], response)
            return

        headers = {
            key: value for key, value in handler.headers.items()
            if key.lower() not in self._HOP_BY_HOP_HEADERS
        }
        connection = http.client.HTTPConnection(
            self._target.hostname, self._target.port, timeout=30)
        try:
            connection.request(handler.command, handler.path, body=body, headers=headers)
            upstream = connection.getresponse()
            response = upstream.read()
            self._send(handler, upstream.status, upstream.getheaders(), response)
        finally:
            connection.close()

    def _dispatch(self, handler):
        try:
            self._handle(handler)
        except Exception as error:
            with self._lock:
                self.errors.append(repr(error))
            response = repr(error).encode("utf-8")
            try:
                self._send(handler, 502, [("Content-Type", "text/plain")], response)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@contextmanager
def _proxied_unity_catalog(unity_catalog_server):
    """Reload the cached catalog for a proxy URI, then evict it before the proxy closes."""
    with _DeltaCommitRejectingProxy(unity_catalog_server["uri"]) as proxy:
        conf = _catalog_conf({**unity_catalog_server, "uri": proxy.uri})

        def reset_catalog(spark):
            spark._jsparkSession.sessionState().catalogManager().reset()

        with_cpu_session(reset_catalog, conf=conf)
        try:
            yield proxy, conf
        finally:
            with_cpu_session(reset_catalog, conf=conf)


def _catalog_options(rest_api_enabled=None):
    jvm = spark_jvm()
    options = jvm.java.util.HashMap()
    options.put("uri", "http://localhost:1")
    options.put("token", "test-token")
    if rest_api_enabled is not None:
        options.put("deltaRestApi.enabled", str(rest_api_enabled).lower())
    return jvm.org.apache.spark.sql.util.CaseInsensitiveStringMap(options)


def _new_delta_catalog(rest_api_enabled=None):
    jvm = spark_jvm()
    catalog = jvm.org.apache.spark.sql.delta.catalog.DeltaCatalog()
    catalog.initialize("test_uc", _catalog_options(rest_api_enabled))
    catalog.setDelegateCatalog(jvm.io.unitycatalog.rapids.TestUnityCatalog())
    return catalog


def _identifier(spark, namespace, name):
    jvm = spark_jvm()
    namespace_array = spark.sparkContext._gateway.new_array(jvm.java.lang.String, 1)
    namespace_array[0] = namespace
    return jvm.org.apache.spark.sql.connector.catalog.Identifier.of(namespace_array, name)


def _routing_properties(location=None, external=None):
    properties = spark_jvm().java.util.HashMap()
    properties.put("provider", "delta")
    if location is not None:
        properties.put("location", location)
    if external is not None:
        properties.put("external", str(external).lower())
    return properties


def _tag_direct_delta_catalog_write(
        spark, catalog, replace=False, provider="delta", table_properties=None):
    """Exercise the Delta 4.3 provider with a physical write using a direct DeltaCatalog."""
    jvm = spark_jvm()
    empty_map = getattr(getattr(jvm.scala.collection.immutable, "Map$"), "MODULE$").empty()
    empty_seq = getattr(getattr(jvm.scala.collection.immutable, "Nil$"), "MODULE$")
    no_value = jvm.scala.Option.empty()
    properties = empty_map
    for key, value in (table_properties or {}).items():
        properties = properties.updated(key, value)
    provider_option = no_value if provider is None else jvm.scala.Option.apply(provider)
    table_spec_args = [
        properties, provider_option, empty_map,
        no_value, no_value, no_value, no_value, False,
    ]
    if not spark.version.startswith("4.0."):
        # Spark 4.1 adds table constraints to TableSpec.
        table_spec_args.append(empty_seq)
    table_spec = jvm.org.apache.spark.sql.catalyst.plans.logical.TableSpec(*table_spec_args)
    ident = _identifier(spark, "default", f"tagging_{uuid.uuid4().hex}")
    query = spark.range(1)._jdf.queryExecution().analyzed()
    execs = jvm.org.apache.spark.sql.execution.datasources.v2
    rapids = jvm.com.nvidia.spark.rapids
    if replace:
        cpu_exec = execs.AtomicReplaceTableAsSelectExec(
            catalog, ident, empty_seq, query, table_spec, empty_map, True, None)
        meta = rapids.AtomicReplaceTableAsSelectExecMeta(
            cpu_exec, rapids.RapidsConf(empty_map), no_value,
            rapids.NoRuleDataFromReplacementRule())
    else:
        cpu_exec = execs.AtomicCreateTableAsSelectExec(
            catalog, ident, empty_seq, query, table_spec, empty_map, False)
        meta = rapids.AtomicCreateTableAsSelectExecMeta(
            cpu_exec, rapids.RapidsConf(empty_map), no_value,
            rapids.NoRuleDataFromReplacementRule())
    if provider is None:
        assert cpu_exec.properties().get("provider").isEmpty()
        for key, value in (table_properties or {}).items():
            assert cpu_exec.properties().get(key).get() == value
    meta.initReasons()
    jvm.com.nvidia.spark.rapids.delta.delta43x.Delta43xProvider.tagForGpu(cpu_exec, meta)
    return meta.explain(True)


def _assert_rest_cpu_plans(plans, callback, expected_cpu_class):
    assert len(plans) > 0, "No execution plans captured for Delta REST fallback"
    assert any(callback.contains(plan, expected_cpu_class) for plan in plans), \
        f"Delta REST operation did not retain {expected_cpu_class}"
    forbidden_gpu_classes = [
        "GpuAtomicCreateTableAsSelectExec",
        "GpuAtomicReplaceTableAsSelectExec",
        "GpuDeleteCommand",
        "GpuDeltaDynamicPartitionOverwriteCommand",
        "GpuMergeIntoCommand",
        "GpuRapidsDeltaWriteExec",
        "GpuUpdateCommand",
    ]
    for gpu_class in forbidden_gpu_classes:
        assert not any(callback.contains(plan, gpu_class) for plan in plans), \
            f"REST fallback unexpectedly started {gpu_class}"


def _capture_rest_plans(do_test, conf):
    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        result = with_gpu_session(do_test, conf=conf)
        plans = callback.getResultsWithTimeout(10000)
        return result, plans, callback
    finally:
        callback.endCapture()


def _assert_rest_fallback(do_test, conf, expected_cpu_class):
    """Require fallback before a GPU staged table or Delta transaction is constructed."""
    result, plans, callback = _capture_rest_plans(do_test, conf)
    _assert_rest_cpu_plans(plans, callback, expected_cpu_class)
    return result


def _assert_rest_error(do_test, conf, expected_cpu_class):
    """Capture a failing REST operation and prove it remained on the CPU path."""
    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        error_class = _error_class(
            lambda: with_gpu_session(do_test, conf=conf))
        plans = callback.getResultsWithTimeout(10000)
        _assert_rest_cpu_plans(plans, callback, expected_cpu_class)
        return error_class
    finally:
        callback.endCapture()


def _assert_rest_failure(do_test, conf, expected_cpu_class, error_match=None):
    """Prove an injected write failure occurred after selecting the CPU REST plan."""
    callback = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    callback.startCapture()
    try:
        with pytest.raises(Exception, match=error_match):
            with_gpu_session(do_test, conf=conf)
        plans = callback.getResultsWithTimeout(10000)
        _assert_rest_cpu_plans(plans, callback, expected_cpu_class)
    finally:
        callback.endCapture()


def _delta_log_actions(table, conf, version):
    """Read one catalog table commit through the harness's local backing store."""
    detail = with_cpu_session(
        lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
    commit_path = os.path.join(
        urlparse(detail["location"]).path, "_delta_log", f"{version:020}.json")
    with open(commit_path, encoding="utf-8") as commit_file:
        return [json.loads(line) for line in commit_file]


def _latest_version(table, conf):
    return with_cpu_session(
        lambda spark: spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"],
        conf=conf)


def _row_tracking_domain_state(table, conf, require_latest_action=False):
    latest_version = _latest_version(table, conf)
    latest_domains = []
    current_domain = None
    for version in range(latest_version + 1):
        actions = _delta_log_actions(table, conf, version)
        domains = [action["domainMetadata"] for action in actions
                   if action.get("domainMetadata", {}).get("domain") == "delta.rowTracking"]
        assert len(domains) <= 1, \
            f"Expected at most one row-tracking domain action in version {version}: {domains}"
        if domains:
            current_domain = None if domains[0]["removed"] else domains[0]
        if version == latest_version:
            latest_domains = domains

    if require_latest_action:
        assert len(latest_domains) == 1, \
            f"Version {latest_version} did not commit its row-tracking domain intent"
        assert not latest_domains[0]["removed"]
    assert current_domain is not None, "The row-tracking domain was removed"
    configuration = json.loads(current_domain["configuration"])
    assert isinstance(configuration["rowIdHighWaterMark"], int)
    return configuration["rowIdHighWaterMark"]


def _server_owned_delta_state(table, conf):
    detail = with_cpu_session(
        lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(recursive=True),
        conf=conf)
    properties = _normalize_catalog_properties(detail["properties"])
    return {
        "minReaderVersion": detail["minReaderVersion"],
        "minWriterVersion": detail["minWriterVersion"],
        "properties": {
            key: value for key, value in properties.items() if not key.startswith("user.")
        },
        "tableFeatures": sorted(detail["tableFeatures"]),
    }


def _stable_catalog_properties(properties):
    stable = _normalize_catalog_properties(properties)
    stable.pop("delta.lastUpdateVersion", None)
    return {key: value for key, value in stable.items() if not key.startswith("user.")}


def _assert_failed_create_did_not_publish(credential_fs, storage_root):
    failed_uri = credential_fs.getLastFailedCreatePath()
    assert failed_uri, "The injected filesystem failure did not record its target path"
    failed_path = os.path.realpath(urlparse(failed_uri).path)
    tables_root = os.path.realpath(os.path.join(storage_root, "__unitystorage", "tables"))
    assert os.path.commonpath([tables_root, failed_path]) == tables_root

    relative_path = os.path.relpath(failed_path, tables_root)
    table_directory = os.path.join(tables_root, relative_path.split(os.sep, 1)[0])
    assert not os.path.exists(failed_path), \
        f"The failed create left a partial target file at {failed_path}"
    delta_log = os.path.join(table_directory, "_delta_log")
    published_commits = [] if not os.path.isdir(delta_log) else [
        name for name in os.listdir(delta_log) if re.fullmatch(r"[0-9]{20}\.json", name)
    ]
    assert published_commits == [], \
        f"The failed create published Delta commits: {published_commits}"


def _assert_row_tracking_commit_reaches_catalog(
        proxy, start_index, table, conf, expected_high_water_mark):
    """Check generated row tracking in the exact staged file submitted to REST.

    Delta 4.3.0 does not send this generated DML metadata as a separate
    set-domain-metadata update. A clustered RTAS below covers that REST intent form.
    """
    requests = proxy.commit_requests_since(start_index)
    assert len(requests) == 1, f"Expected one Delta commit request, found {requests}"
    add_commits = [
        update["commit"] for update in requests[0]["updates"]
        if update.get("action") == "add-commit"
    ]
    assert len(add_commits) == 1, json.dumps(requests, sort_keys=True)
    commit = add_commits[0]
    assert commit["version"] == _latest_version(table, conf)

    detail = with_cpu_session(
        lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
    staged_path = os.path.join(
        urlparse(detail["location"]).path, "_delta_log", "_staged_commits", commit["file-name"])
    assert os.path.getsize(staged_path) == commit["file-size"]
    with open(staged_path, encoding="utf-8") as commit_file:
        staged_actions = [json.loads(line) for line in commit_file]
    domains = [
        action["domainMetadata"] for action in staged_actions
        if action.get("domainMetadata", {}).get("domain") == "delta.rowTracking"
    ]
    assert len(domains) == 1 and not domains[0]["removed"], staged_actions
    assert json.loads(domains[0]["configuration"]) == {
        "domainName": "delta.rowTracking",
        "rowIdHighWaterMark": expected_high_water_mark,
    }


def _assert_clustering_intent_reaches_catalog(proxy, start_index, table, conf):
    requests = proxy.commit_requests_since(start_index)
    assert len(requests) == 1, f"Expected one Delta commit request, found {requests}"
    domain_updates = [
        update["updates"]["delta.clustering"]
        for update in requests[0]["updates"]
        if update.get("action") == "set-domain-metadata" and
        "delta.clustering" in update.get("updates", {})
    ]
    assert len(domain_updates) == 1, json.dumps(requests, sort_keys=True)

    add_commits = [
        update["commit"] for update in requests[0]["updates"]
        if update.get("action") == "add-commit"
    ]
    assert len(add_commits) == 1, json.dumps(requests, sort_keys=True)
    detail = with_cpu_session(
        lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
    staged_path = os.path.join(
        urlparse(detail["location"]).path, "_delta_log", "_staged_commits",
        add_commits[0]["file-name"])
    with open(staged_path, encoding="utf-8") as commit_file:
        staged_actions = [json.loads(line) for line in commit_file]
    domains = [
        action["domainMetadata"] for action in staged_actions
        if action.get("domainMetadata", {}).get("domain") == "delta.clustering"
    ]
    assert len(domains) == 1 and not domains[0]["removed"], staged_actions
    staged_configuration = json.loads(domains[0]["configuration"])
    assert staged_configuration.pop("domainName") == "delta.clustering"
    assert domain_updates == [staged_configuration]


@delta_lake
@unity_catalog
def test_delta_rest_api_client_and_routing_detection():
    def check(spark):
        shim = spark_jvm().org.apache.spark.sql.delta.catalog.DeltaCatalogRestApiShim
        enabled = _new_delta_catalog()
        disabled = _new_delta_catalog(False)
        managed = _identifier(spark, "default", "managed_table")
        path = _identifier(spark, "delta", "/tmp/path_table")

        assert shim.isRestApiEnabled(enabled)
        assert not shim.isRestApiEnabled(disabled)
        assert shim.shouldRouteCreate(
            enabled, managed, _routing_properties())
        assert not shim.shouldRouteCreate(
            enabled, managed, _routing_properties(location="/tmp/external"))
        assert not shim.shouldRouteCreate(
            enabled, path, _routing_properties())
        assert shim.shouldRouteOrValidateReplace(
            enabled, managed, _routing_properties())
        assert shim.shouldRouteOrValidateReplace(
            enabled, managed, _routing_properties(external=True))
        assert not shim.shouldRouteOrValidateReplace(
            enabled, path, _routing_properties())

        # The catalog's captured session, not the active caller session, owns path routing.
        catalog_session = spark._jsparkSession.newSession()
        catalog_session.conf().set("spark.sql.runSQLOnFiles", "true")
        caller_session = spark._jsparkSession.newSession()
        caller_session.conf().set("spark.sql.runSQLOnFiles", "false")
        jvm_session = spark_jvm().org.apache.spark.sql.SparkSession
        try:
            jvm_session.setActiveSession(catalog_session)
            path_catalog = _new_delta_catalog()
            jvm_session.setActiveSession(caller_session)
            assert path_catalog.spark().equals(catalog_session)
            assert path_catalog.spark().sessionState().conf().runSQLonFile()
            assert not caller_session.sessionState().conf().runSQLonFile()
            assert not shim.shouldRouteCreate(
                path_catalog, path, _routing_properties())

            # In the reverse mismatch, the caller would incorrectly skip CPU REST routing.
            catalog_session.conf().set("spark.sql.runSQLOnFiles", "false")
            caller_session.conf().set("spark.sql.runSQLOnFiles", "true")
            jvm_session.setActiveSession(catalog_session)
            managed_catalog = _new_delta_catalog()
            jvm_session.setActiveSession(caller_session)
            assert managed_catalog.spark().equals(catalog_session)
            assert not managed_catalog.spark().sessionState().conf().runSQLonFile()
            assert caller_session.sessionState().conf().runSQLonFile()
            assert shim.isRestApiEnabled(managed_catalog)
            assert shim.shouldRouteCreate(
                managed_catalog, path, _routing_properties())
            assert shim.shouldRouteOrValidateReplace(
                managed_catalog, path, _routing_properties())
        finally:
            jvm_session.setActiveSession(spark._jsparkSession)

    with_cpu_session(check)


@delta_lake
@unity_catalog
def test_delta_rest_direct_catalog_provider_tags_ctas_and_rtas(unity_catalog_server):
    conf = {**_catalog_conf(unity_catalog_server), "spark.sql.sources.default": "delta"}

    def check(spark):
        catalog = spark._jsparkSession.sessionState().catalogManager().catalog("unity")
        delegate_field = catalog.getClass().getDeclaredField("delegate")
        delegate_field.setAccessible(True)
        delta_catalog = delegate_field.get(catalog)
        shim = spark_jvm().org.apache.spark.sql.delta.catalog.DeltaCatalogRestApiShim
        assert shim.isRestApiEnabled(delta_catalog)

        # No catalog-owned feature property: the initialized REST client alone must tag both.
        for replace in (False, True):
            reason = _tag_direct_delta_catalog_write(spark, delta_catalog, replace=replace)
            assert "Unity Catalog Delta REST API operations must run on CPU" in reason, reason
            # Delta treats only the exact lowercase "provider" key as a provider override.
            reason = _tag_direct_delta_catalog_write(
                spark, delta_catalog, replace=replace, provider=None,
                table_properties={"Provider": "parquet"})
            assert "Unity Catalog Delta REST API operations must run on CPU" in reason, reason

    with_cpu_session(check, conf=conf)


@delta_lake
@unity_catalog
def test_delta_rest_direct_catalog_non_delta_default_stays_on_cpu():
    def check(spark):
        catalog_session = spark._jsparkSession.newSession()
        catalog_session.conf().set("spark.sql.sources.default", "parquet")
        jvm_session = spark_jvm().org.apache.spark.sql.SparkSession
        try:
            jvm_session.setActiveSession(catalog_session)
            catalog = _new_delta_catalog(False)
        finally:
            jvm_session.setActiveSession(spark._jsparkSession)

        assert catalog.spark().equals(catalog_session)
        assert spark.conf.get("spark.sql.sources.default") == "delta"
        assert catalog_session.conf().get("spark.sql.sources.default") == "parquet"
        shim = spark_jvm().org.apache.spark.sql.delta.catalog.DeltaCatalogRestApiShim
        assert not shim.isRestApiEnabled(catalog)
        for replace in (False, True):
            reason = _tag_direct_delta_catalog_write(
                spark, catalog, replace=replace, provider=None)
            assert "non-Delta catalog writes must run on CPU" in reason, reason

    with_cpu_session(check, conf={"spark.sql.sources.default": "delta"})


@allow_non_gpu("AtomicReplaceTableExec", "CreateTableExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_delta_rest_schema_only_create_and_replace_fall_back(unity_catalog_server):
    _, table = _new_table_name("delta_rest_schema_only")
    conf = _catalog_conf(unity_catalog_server)

    try:
        _assert_rest_fallback(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table} (id BIGINT, value STRING)
                USING DELTA
                """).collect(), conf=conf, expected_cpu_class="CreateTableExec")
        original_identity = _catalog_identity_state(
            unity_catalog_server["tables_api"], table)

        _assert_rest_fallback(
            lambda spark: spark.sql(f"""
                REPLACE TABLE {table} (id BIGINT, value STRING, replacement BOOLEAN)
                USING DELTA
                """).collect(), conf=conf,
            expected_cpu_class="AtomicReplaceTableExec")
        replaced_identity = _catalog_identity_state(
            unity_catalog_server["tables_api"], table)
        assert replaced_identity["id"] == original_identity["id"]
        assert replaced_identity["location"] == original_identity["location"]
        assert with_cpu_session(
            lambda spark: spark.table(table).schema.simpleString(), conf=conf) == \
            "struct<id:bigint,value:string,replacement:boolean>"
    finally:
        _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec",
               "AtomicReplaceTableAsSelectExec", "OverwriteByExpressionExecV1",
               *delta_meta_allow)
@delta_lake
@unity_catalog
def test_delta_rest_domain_metadata_intent_reaches_catalog(unity_catalog_server):
    _, table = _new_table_name("delta_rest_domain_metadata")
    with _proxied_unity_catalog(unity_catalog_server) as (proxy, conf):
        try:
            _assert_rest_fallback(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {table}
                    USING DELTA
                    CLUSTER BY (id)
                    AS SELECT * FROM VALUES
                        (1L, 'one'), (2L, 'two') AS source(id, value)
                    """).collect(), conf=conf,
                expected_cpu_class="AtomicCreateTableAsSelectExec")

            commit_index = proxy.commit_request_count()
            _assert_rest_fallback(
                lambda spark: spark.sql(f"""
                    REPLACE TABLE {table}
                    USING DELTA
                    CLUSTER BY (id)
                    AS SELECT * FROM VALUES
                        (3L, 'three'), (4L, 'four') AS source(id, value)
                    """).collect(), conf=conf,
                expected_cpu_class="AtomicReplaceTableAsSelectExec")
            _assert_clustering_intent_reaches_catalog(proxy, commit_index, table, conf)
            assert _table_rows(table, conf) == [(3, "three"), (4, "four")]
            assert proxy.errors == []
        finally:
            _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_delta_rest_non_delta_catalog_write_stays_on_cpu(unity_catalog_server):
    table_name, table = _new_table_name("delta_rest_non_delta")
    conf = _catalog_conf(unity_catalog_server)

    def create_parquet(spark):
        return spark.sql(f"""
            CREATE TABLE {table} USING PARQUET
            AS SELECT 1L AS id, 'parquet' AS value
            """).collect()

    try:
        cpu_error = _error_class(
            lambda: with_cpu_session(create_parquet, conf=conf))
        gpu_error = _assert_rest_error(
            create_parquet, conf=conf,
            expected_cpu_class="AtomicCreateTableAsSelectExec")
        assert gpu_error == cpu_error
        assert with_cpu_session(
            lambda spark: spark.sql(
                f"SHOW TABLES IN unity.default LIKE '{table_name}'").collect(), conf=conf) == []
    finally:
        _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec",
               "AtomicReplaceTableAsSelectExec", "OverwriteByExpressionExecV1",
               *delta_meta_allow)
@delta_lake
@unity_catalog
def test_delta_rest_managed_ctas_and_rtas_fall_back(unity_catalog_server):
    _, table = _new_table_name("delta_rest_create_replace")
    with _proxied_unity_catalog(unity_catalog_server) as (proxy, conf):
        try:
            _assert_rest_fallback(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {table}
                    USING DELTA
                    TBLPROPERTIES (
                        'ucTableId' = 'caller-must-not-own-this-id',
                        'user.intent.property' = 'before')
                    AS SELECT * FROM VALUES
                        (1L, 'one'), (2L, 'two') AS source(id, value)
                    """).collect(),
                conf=conf, expected_cpu_class="AtomicCreateTableAsSelectExec")
            create_paths = proxy.create_request_paths()
            assert len(create_paths) == 2, create_paths
            assert create_paths[0].endswith("/staging-tables"), create_paths
            assert create_paths[1].endswith("/tables"), create_paths
            assert create_paths[0].rsplit("/", 1)[0] == create_paths[1].rsplit("/", 1)[0]
            detail = with_cpu_session(
                lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
            table_info = unity_catalog_server["tables_api"].getTable(table, None, None)
            original_identity = _catalog_identity_state(
                unity_catalog_server["tables_api"], table)
            original_server_state = _server_owned_delta_state(table, conf)
            original_catalog_properties = _stable_catalog_properties(
                original_identity["properties"])
            _assert_catalog_commit_state(table, detail, table_info, conf)
            assert detail["properties"]["io.unitycatalog.tableId"] == table_info.getTableId()
            assert "ucTableId" not in detail["properties"]

            commit_index = proxy.commit_request_count()
            _assert_rest_fallback(
                lambda spark: spark.sql(f"""
                    REPLACE TABLE {table}
                    USING DELTA
                    TBLPROPERTIES ('user.intent.property' = 'after')
                    AS SELECT * FROM VALUES
                        (3L, 'three'), (4L, 'four') AS source(id, value)
                    """).collect(),
                conf=conf, expected_cpu_class="AtomicReplaceTableAsSelectExec")
            requests = proxy.commit_requests_since(commit_index)
            assert len(requests) == 1, f"Expected one Delta commit request, found {requests}"
            property_updates = [
                update for update in requests[0]["updates"]
                if update.get("action") == "set-properties"
            ]
            assert len(property_updates) == 1, json.dumps(requests, sort_keys=True)
            assert property_updates[0]["updates"]["user.intent.property"] == "after", \
                json.dumps(requests, sort_keys=True)
            replaced = _catalog_identity_state(unity_catalog_server["tables_api"], table)
            replaced_detail = with_cpu_session(
                lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
            replaced_info = unity_catalog_server["tables_api"].getTable(table, None, None)
            _assert_catalog_commit_state(table, replaced_detail, replaced_info, conf)
            assert replaced["id"] == original_identity["id"]
            assert replaced["location"] == original_identity["location"]
            assert _server_owned_delta_state(table, conf) == original_server_state
            assert _stable_catalog_properties(replaced["properties"]) == original_catalog_properties
            assert replaced_detail["properties"]["user.intent.property"] == "after"
            assert _table_rows(table, conf) == [(3, "three"), (4, "four")]
            assert_cpu_and_gpu_are_equal_collect_with_capture(
                lambda spark: spark.sql(f"SELECT id, value FROM {table} ORDER BY id"),
                exist_classes="GpuFileSourceScanExec", conf=conf, require_non_empty=True)
            assert proxy.errors == []
        finally:
            _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec",
               "AtomicReplaceTableAsSelectExec", "OverwriteByExpressionExecV1",
               *delta_meta_allow)
@delta_lake
@unity_catalog
def test_delta_rest_replace_validation_propagates_and_preserves_table(
        unity_catalog_server):
    _, table = _new_table_name("delta_rest_replace_validation")
    conf = _catalog_conf(unity_catalog_server)

    def invalid_replace(spark):
        return spark.sql(f"""
            REPLACE TABLE {table} USING DELTA LOCATION '/tmp/forbidden-replace-location'
            AS SELECT 2L AS id, 'invalid' AS value
            """).collect()

    try:
        _assert_rest_fallback(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table} USING DELTA
                TBLPROPERTIES ('user.validation.property' = 'preserved')
                AS SELECT 1L AS id, 'original' AS value
                """).collect(), conf=conf,
            expected_cpu_class="AtomicCreateTableAsSelectExec")
        before_state = with_cpu_session(
            lambda spark: _preserved_table_state(spark, table), conf=conf)
        before_catalog = _catalog_identity_state(
            unity_catalog_server["tables_api"], table)

        cpu_error = _error_class(
            lambda: with_cpu_session(invalid_replace, conf=conf))
        gpu_error = _assert_rest_error(
            invalid_replace, conf=conf,
            expected_cpu_class="AtomicReplaceTableAsSelectExec")
        assert gpu_error == cpu_error
        assert with_cpu_session(
            lambda spark: _preserved_table_state(spark, table), conf=conf) == before_state
        assert _catalog_identity_state(
            unity_catalog_server["tables_api"], table) == before_catalog
    finally:
        _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec",
               "AtomicReplaceTableAsSelectExec", "OverwriteByExpressionExecV1",
               *delta_meta_allow)
@delta_lake
@unity_catalog
def test_delta_rest_server_commit_rejection_propagates_and_aborts(unity_catalog_server):
    _, table = _new_table_name("delta_rest_server_rejection")

    with _proxied_unity_catalog(unity_catalog_server) as (proxy, conf):
        try:
            _assert_rest_fallback(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {table} USING DELTA
                    TBLPROPERTIES ('user.rejection.property' = 'preserved')
                    AS SELECT 1L AS id, 'original' AS value
                    """).collect(), conf=conf,
                expected_cpu_class="AtomicCreateTableAsSelectExec")
            before_state = with_cpu_session(
                lambda spark: _preserved_table_state(spark, table), conf=conf)
            before_catalog = _catalog_identity_state(
                unity_catalog_server["tables_api"], table)
            before_server_state = _server_owned_delta_state(table, conf)
            before_catalog_properties = _stable_catalog_properties(
                before_catalog["properties"])

            proxy.reject_next_commit()
            _assert_rest_failure(
                lambda spark: spark.sql(f"""
                    REPLACE TABLE {table} USING DELTA
                    TBLPROPERTIES ('user.rejection.property' = 'preserved')
                    AS SELECT 2L AS id, 'rejected' AS value
                    """).collect(), conf=conf,
                expected_cpu_class="AtomicReplaceTableAsSelectExec",
                error_match=_SERVER_REJECTION_MESSAGE)
            assert proxy.rejected_body is not None
            assert before_catalog["id"] in proxy.rejected_body
            assert with_cpu_session(
                lambda spark: _preserved_table_state(spark, table), conf=conf) == before_state
            assert _catalog_identity_state(
                unity_catalog_server["tables_api"], table) == before_catalog

            _assert_rest_fallback(
                lambda spark: spark.sql(f"""
                    REPLACE TABLE {table} USING DELTA
                    TBLPROPERTIES ('user.rejection.property' = 'preserved')
                    AS SELECT 3L AS id, 'retry' AS value
                    """).collect(), conf=conf,
                expected_cpu_class="AtomicReplaceTableAsSelectExec")
            retry_catalog = _catalog_identity_state(
                unity_catalog_server["tables_api"], table)
            assert retry_catalog["id"] == before_catalog["id"]
            assert retry_catalog["location"] == before_catalog["location"]
            assert _stable_catalog_properties(
                retry_catalog["properties"]) == before_catalog_properties
            assert _server_owned_delta_state(table, conf) == before_server_state
            assert _latest_version(table, conf) == before_state["identity"]["version"] + 1
            assert _table_rows(table, conf) == [(3, "retry")]
            assert proxy.errors == []
        finally:
            _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec", "ExecutedCommandExec",
               "OverwriteByExpressionExecV1", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_delta_rest_metadata_changing_writes_fall_back(unity_catalog_server):
    _, table = _new_table_name("delta_rest_metadata_writes")
    with _proxied_unity_catalog(unity_catalog_server) as (proxy, conf):
        try:
            _assert_rest_fallback(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {table}
                    USING DELTA
                    PARTITIONED BY (p)
                    AS SELECT * FROM VALUES
                        (1L, 'one', 0), (2L, 'two', 1), (3L, 'three', 1)
                        AS source(id, value, p)
                    """).collect(), conf=conf,
                expected_cpu_class="AtomicCreateTableAsSelectExec")
            create_high_water_mark = _row_tracking_domain_state(
                table, conf, require_latest_action=True)

            def dynamic_overwrite(spark):
                return spark.createDataFrame(
                    [(10, "ten", 0)], "id LONG, value STRING, p INT") \
                    .writeTo(table).overwritePartitions()

            commit_index = proxy.commit_request_count()
            _assert_rest_fallback(dynamic_overwrite, conf=conf,
                                  expected_cpu_class="ExecutedCommandExec")
            overwrite_high_water_mark = _row_tracking_domain_state(
                table, conf, require_latest_action=True)
            assert overwrite_high_water_mark > create_high_water_mark
            _assert_row_tracking_commit_reaches_catalog(
                proxy, commit_index, table, conf, overwrite_high_water_mark)

            commit_index = proxy.commit_request_count()
            _assert_rest_fallback(
                lambda spark: spark.sql(
                    f"UPDATE {table} SET value = 'TEN' WHERE id = 10").collect(),
                conf=conf, expected_cpu_class="ExecutedCommandExec")
            update_high_water_mark = _row_tracking_domain_state(
                table, conf, require_latest_action=True)
            assert update_high_water_mark > overwrite_high_water_mark
            _assert_row_tracking_commit_reaches_catalog(
                proxy, commit_index, table, conf, update_high_water_mark)

            _assert_rest_fallback(
                lambda spark: spark.sql(f"DELETE FROM {table} WHERE id = 2").collect(), conf=conf,
                expected_cpu_class="ExecutedCommandExec")
            assert _row_tracking_domain_state(table, conf) == update_high_water_mark

            def merge(spark):
                source = f"delta_rest_source_{uuid.uuid4().hex}"
                spark.createDataFrame(
                    [(3, "THREE", 1), (4, "four", 1)], "id LONG, value STRING, p INT") \
                    .createOrReplaceTempView(source)
                return spark.sql(f"""
                    MERGE INTO {table} target USING {source} source ON target.id = source.id
                    WHEN MATCHED THEN UPDATE SET value = source.value
                    WHEN NOT MATCHED THEN INSERT *
                    """).collect()

            commit_index = proxy.commit_request_count()
            _assert_rest_fallback(merge, conf=conf, expected_cpu_class="ExecutedCommandExec")
            merge_high_water_mark = _row_tracking_domain_state(
                table, conf, require_latest_action=True)
            assert merge_high_water_mark > update_high_water_mark
            _assert_row_tracking_commit_reaches_catalog(
                proxy, commit_index, table, conf, merge_high_water_mark)
            assert _table_rows(table, conf) == [
                (3, "THREE", 1), (4, "four", 1), (10, "TEN", 0)]
            detail = with_cpu_session(
                lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
            assert "rowTracking" in detail["tableFeatures"]
            assert proxy.errors == []
        finally:
            _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec", *delta_meta_allow)
@delta_lake
@unity_catalog
@pytest.mark.parametrize("fail_suffix", [".parquet", ".json"],
                         ids=["data-file", "delta-commit"])
def test_delta_rest_failed_ctas_does_not_publish_partial_table(
        unity_catalog_server, fail_suffix):
    table_name, table = _new_table_name("delta_rest_failed_create")
    credential_fs = spark_jvm().com.nvidia.spark.rapids.tests.delta.CredentialTestFileSystem

    with _proxied_unity_catalog(unity_catalog_server) as (proxy, conf):
        try:
            credential_fs.failNextCreateEndingWith(fail_suffix)
            _assert_rest_failure(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {table} USING DELTA
                    AS SELECT 1L AS id, 'failed' AS value
                    """).collect(), conf=conf,
                expected_cpu_class="AtomicCreateTableAsSelectExec",
                error_match="Injected create failure")
            create_paths = proxy.create_request_paths()
            assert len(create_paths) == 1, create_paths
            assert create_paths[0].endswith("/staging-tables"), create_paths
            assert with_cpu_session(
                lambda spark: spark.sql(
                    f"SHOW TABLES IN unity.default LIKE '{table_name}'").collect(),
                conf=conf) == []
            _assert_failed_create_did_not_publish(
                credential_fs, unity_catalog_server["storage_root"])

            _assert_rest_fallback(
                lambda spark: spark.sql(f"""
                    CREATE TABLE {table} USING DELTA
                    AS SELECT 2L AS id, 'retry' AS value
                    """).collect(), conf=conf,
                expected_cpu_class="AtomicCreateTableAsSelectExec")
            assert _table_rows(table, conf) == [(2, "retry")]
            history = with_cpu_session(
                lambda spark: spark.sql(f"DESCRIBE HISTORY {table}").collect(), conf=conf)
            assert len(history) == 1
            assert proxy.errors == []
        finally:
            credential_fs.clearInjectedFailure()
            _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec",
               "AtomicReplaceTableAsSelectExec", "OverwriteByExpressionExecV1",
               *delta_meta_allow)
@delta_lake
@unity_catalog
@pytest.mark.parametrize("failure_stage", ["data-file", "delta-commit"],
                         ids=["data-file", "delta-commit"])
def test_delta_rest_failed_rtas_preserves_table(
        unity_catalog_server, failure_stage):
    _, table = _new_table_name("delta_rest_failed_replace")
    conf = _catalog_conf(unity_catalog_server)
    credential_fs = spark_jvm().com.nvidia.spark.rapids.tests.delta.CredentialTestFileSystem

    try:
        _assert_rest_fallback(
            lambda spark: spark.sql(f"""
                CREATE TABLE {table} USING DELTA
                TBLPROPERTIES ('user.atomicity.property' = 'preserved')
                AS SELECT 1L AS id, 'original' AS value
                """).collect(), conf=conf,
            expected_cpu_class="AtomicCreateTableAsSelectExec")
        before_state = with_cpu_session(
            lambda spark: _preserved_table_state(spark, table), conf=conf)
        before_detail = with_cpu_session(
            lambda spark: spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(), conf=conf)
        before_catalog = _catalog_identity_state(
            unity_catalog_server["tables_api"], table)

        fail_suffix = ".parquet" if failure_stage == "data-file" else ".json"
        credential_fs.failNextCreateEndingWith(fail_suffix)
        failure_conf = dict(conf)
        if failure_stage == "delta-commit":
            # A one-shot staged-commit failure is otherwise retried by Delta.
            failure_conf["spark.databricks.delta.maxNonConflictCommitAttempts"] = "0"
        _assert_rest_failure(
            lambda spark: spark.sql(f"""
                REPLACE TABLE {table} USING DELTA
                TBLPROPERTIES ('user.atomicity.property' = 'preserved')
                AS SELECT 2L AS id, 'failed' AS value
                """).collect(), conf=failure_conf,
            expected_cpu_class="AtomicReplaceTableAsSelectExec",
            error_match="Injected create failure")
        failed_uri = credential_fs.getLastFailedCreatePath()
        assert failed_uri, "The injected filesystem failure did not record its target path"
        if failure_stage == "delta-commit":
            delta_log_path = os.path.realpath(os.path.join(
                urlparse(before_detail["location"]).path, "_delta_log"))
            staged_commits_path = os.path.join(delta_log_path, "_staged_commits")
            failed_path = os.path.realpath(urlparse(failed_uri).path)
            assert os.path.commonpath([staged_commits_path, failed_path]) == \
                staged_commits_path
            assert not os.path.exists(failed_path)

        assert with_cpu_session(
            lambda spark: _preserved_table_state(spark, table), conf=conf) == before_state
        assert _catalog_identity_state(
            unity_catalog_server["tables_api"], table) == before_catalog

        _assert_rest_fallback(
            lambda spark: spark.sql(f"""
                REPLACE TABLE {table} USING DELTA
                TBLPROPERTIES ('user.atomicity.property' = 'preserved')
                AS SELECT 3L AS id, 'replacement' AS value
                """).collect(), conf=conf,
            expected_cpu_class="AtomicReplaceTableAsSelectExec")
        assert _table_rows(table, conf) == [(3, "replacement")]
        assert _latest_version(table, conf) == before_state["identity"]["version"] + 1
    finally:
        credential_fs.clearInjectedFailure()
        _drop_table(table, conf)


@allow_non_gpu("AppendDataExecV1", "AtomicCreateTableAsSelectExec", *delta_meta_allow)
@delta_lake
@unity_catalog
def test_delta_rest_external_and_path_boundaries(
        unity_catalog_server, spark_tmp_path):
    _, external_table = _new_table_name("delta_rest_external")
    external_location = \
        f"s3://test-bucket0{unity_catalog_server['storage_root']}/{uuid.uuid4().hex}"
    path = f"{spark_tmp_path}/delta_rest_path"
    conf = _catalog_conf(unity_catalog_server)

    try:
        _assert_rest_fallback(
            lambda spark: spark.sql(f"""
                CREATE TABLE {external_table} USING DELTA LOCATION '{external_location}'
                AS SELECT 1L AS id, 'external' AS value
                """).collect(), conf=conf,
            expected_cpu_class="AtomicCreateTableAsSelectExec")
        assert_rapids_delta_write(
            lambda spark: spark.sql(f"""
                CREATE TABLE delta.`{path}` USING DELTA
                AS SELECT 2L AS id, 'path' AS value
                """).collect(), conf=conf,
            required_gpu_classes=["GpuRapidsDeltaWriteExec"], require_non_empty=True)
    finally:
        _drop_table(external_table, conf)
