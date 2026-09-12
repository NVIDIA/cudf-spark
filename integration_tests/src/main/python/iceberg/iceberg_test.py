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

from concurrent.futures import ThreadPoolExecutor
import re

import pytest

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture, \
    assert_equal_with_local_sort, assert_gpu_and_cpu_are_equal_collect, \
    assert_gpu_and_cpu_row_counts_equal, assert_gpu_fallback_collect, assert_spark_exception
from conftest import is_iceberg_remote_catalog, is_iceberg_rest_catalog
from data_gen import *
from iceberg import (
    iceberg_format_versions, iceberg_read_enabled_conf, iceberg_read_format_versions,
    iceberg_table_properties_sql, get_full_table_name, iceberg_unsupported_mark, _build_tblprops,
    _BASE_TBLPROPS_SQL, create_iceberg_table, iceberg_write_enabled_conf, supports_iceberg_v3,
    ICEBERG_V3_UNSUPPORTED_REASON, supports_iceberg_row_lineage_inheritance,
    ICEBERG_ROW_LINEAGE_INHERITANCE_UNSUPPORTED_REASON, row_lineage_df)
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_databricks_runtime, is_spark_35x, is_spark_400_or_later, \
    is_spark_40x, is_spark_41x, spark_version, with_cpu_session, with_gpu_session

iceberg_map_gens = [MapGen(f(nullable=False), f()) for f in [
    BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen, TimestampGen ]] + \
                    [simple_string_to_string_map_gen,
                     MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
                     MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
                     MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen)]

iceberg_primitive_gens_list = [[byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                               string_gen, boolean_gen, date_gen, timestamp_gen, binary_gen] + decimal_gens]

iceberg_gens_list = [
    [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
     string_gen, boolean_gen, date_gen, timestamp_gen, binary_gen, ArrayGen(binary_gen),
     ArrayGen(byte_gen), ArrayGen(long_gen), ArrayGen(string_gen), ArrayGen(date_gen),
     ArrayGen(timestamp_gen), ArrayGen(decimal_gen_64bit), ArrayGen(ArrayGen(byte_gen)),
     StructGen([['child0', ArrayGen(byte_gen)], ['child1', byte_gen], ['child2', float_gen], ['child3', decimal_gen_64bit]]),
     ArrayGen(StructGen([['child0', string_gen], ['child1', double_gen], ['child2', int_gen]]))
    ] + iceberg_map_gens + decimal_gens ]

rapids_reader_types = ['PERFILE', 'MULTITHREADED', 'COALESCING']
_NO_FANOUT = _BASE_TBLPROPS_SQL
_ROW_LINEAGE_WRITE_CONF = {
    **iceberg_write_enabled_conf,
    "spark.rapids.sql.format.iceberg.v3.enabled": "true"
}


def _assert_gpu_and_cpu_lineage_writes_are_equal(
        spark_tmp_table_factory, setup_func, write_func, read_func):
    base_table = get_full_table_name(spark_tmp_table_factory)
    cpu_table = f"{base_table}_cpu"
    gpu_table = f"{base_table}_gpu"

    def setup_tables(spark):
        setup_func(spark, cpu_table)
        setup_func(spark, gpu_table)

    def run_write(spark, table):
        write_func(spark, table)

    def next_row_id(spark, table):
        iceberg_table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, table)
        return iceberg_table.operations().current().nextRowId()

    with_cpu_session(setup_tables)
    with_cpu_session(lambda spark: run_write(spark, cpu_table), conf=_ROW_LINEAGE_WRITE_CONF)
    with_gpu_session(lambda spark: run_write(spark, gpu_table), conf=_ROW_LINEAGE_WRITE_CONF)

    cpu_data = with_cpu_session(
        lambda spark: read_func(spark, cpu_table).collect(), conf=_ROW_LINEAGE_WRITE_CONF)
    gpu_data = with_cpu_session(
        lambda spark: read_func(spark, gpu_table).collect(), conf=_ROW_LINEAGE_WRITE_CONF)
    assert_equal_with_local_sort(cpu_data, gpu_data)
    cpu_next_row_id = with_cpu_session(
        lambda spark: next_row_id(spark, cpu_table), conf=_ROW_LINEAGE_WRITE_CONF)
    gpu_next_row_id = with_cpu_session(
        lambda spark: next_row_id(spark, gpu_table), conf=_ROW_LINEAGE_WRITE_CONF)
    assert cpu_next_row_id == gpu_next_row_id

pytestmark = iceberg_unsupported_mark


def _is_spark_patch_at_least(version, minimum):
    patch = version.split(".")[2].split("-", 1)[0]
    return int(patch) >= minimum


def _is_spark_58783_affected():
    return (
        (is_spark_40x() and not _is_spark_patch_at_least(spark_version(), 5))
        or (is_spark_41x() and not _is_spark_patch_at_least(spark_version(), 4))
    )


def _is_spark_58783_fixed():
    return (
        (is_spark_40x() and _is_spark_patch_at_least(spark_version(), 5))
        or (is_spark_41x() and _is_spark_patch_at_least(spark_version(), 4))
    )


@pytest.mark.parametrize("version, minimum, expected", [
    ("3.5.9", 9, True),
    ("3.5.9-SNAPSHOT", 9, True),
    ("4.1.2-amzn-0", 2, True),
    ("3.5.8-SNAPSHOT", 9, False),
])
def test_is_spark_patch_at_least(version, minimum, expected):
    assert _is_spark_patch_at_least(version, minimum) == expected


def _collect_plan_nodes(plan):
    nodes = [plan]
    children = plan.children().iterator()
    while children.hasNext():
        nodes.extend(_collect_plan_nodes(children.next()))
    return nodes


def _nodes_of_class(plan, class_name):
    return [node for node in _collect_plan_nodes(plan)
            if node.getClass().getSimpleName() == class_name]


def _assert_spj_join_shape(plan, expect_spj):
    """Two GPU scans feeding one GPU join. `expect_spj` requires the join's own inputs to be
    shuffle-free, which is what separates a storage-partitioned join from a shuffled one."""
    scans = _nodes_of_class(plan, "GpuBatchScanExec")
    joins = _nodes_of_class(plan, "GpuShuffledSymmetricHashJoinExec")

    assert len(scans) == 2, f"Expected two GPU batch scans, found {len(scans)}:\n{plan}"
    assert len(joins) == 1, f"Expected one GPU join, found {len(joins)}:\n{plan}"

    join_exchanges = _nodes_of_class(joins[0], "GpuShuffleExchangeExec")
    if expect_spj:
        assert not join_exchanges, f"Expected shuffle-free SPJ inputs:\n{plan}"
    else:
        assert join_exchanges, f"Expected shuffled join inputs without SPJ:\n{plan}"

    return scans


def _assert_partial_clustering_spj_plan(_cpu_plan, plan):
    scans = _assert_spj_join_shape(plan, expect_spj=True)

    exchanges = _nodes_of_class(plan, "GpuShuffleExchangeExec")
    assert len(exchanges) == 1, \
        f"Expected one post-join GPU shuffle, found {len(exchanges)}:\n{plan}"
    assert any(scan.outputPartitioning().isPartiallyClustered() for scan in scans), \
        f"Expected at least one partially clustered GPU batch scan:\n{plan}"


def _scan_for_table(scans, table, plan):
    table_name = table.rsplit(".", 1)[-1]
    matching_scans = [scan for scan in scans
                      if str(scan.table().name()).endswith(table_name)]
    assert len(matching_scans) == 1, \
        f"Expected one {table_name} scan, found {len(matching_scans)}:\n{plan}"
    return matching_scans[0]


def _evaluated_dynamic_pruning_values(scan, plan):
    runtime_filters = []
    filters = scan.runtimeFilters().iterator()
    while filters.hasNext():
        runtime_filters.append(filters.next())
    dpp_filters = [runtime_filter for runtime_filter in runtime_filters
                   if runtime_filter.getClass().getSimpleName() == "DynamicPruningExpression"
                   and runtime_filter.child().getClass().getSimpleName() == "InSubqueryExec"]
    assert len(dpp_filters) == 1, \
        f"Expected one nontrivial dynamic pruning filter on the scan:\n{plan}"
    values = dpp_filters[0].child().values()
    assert values.isDefined(), f"Expected the dynamic pruning filter to be evaluated:\n{plan}"
    return values.get()


def _collect_with_plan_assertion(spark, query, plan_assertion):
    df = query(spark)
    result = df.collect()
    plan_assertion(df._jdf.queryExecution().executedPlan())
    return result


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not (
        (is_spark_35x() and _is_spark_patch_at_least(spark_version(), 9))
        or (is_spark_40x() and _is_spark_patch_at_least(spark_version(), 3))
        or (is_spark_41x() and _is_spark_patch_at_least(spark_version(), 2))
    ),
    reason="Requires Spark's partial-clustering correctness fix and GPU Iceberg scan support")
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_spj_partial_clustering_distinct(format_version, spark_tmp_table_factory):
    left_table = get_full_table_name(spark_tmp_table_factory)
    right_table = get_full_table_name(spark_tmp_table_factory)
    table_props = _build_tblprops({
        "format-version": format_version,
        # Keep separate INSERTs as separate scan splits so that id=1 is partially clustered.
        "read.split.target-size": "1",
        "read.split.open-file-cost": "1",
    })
    table_props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in table_props.items())

    def setup_iceberg_tables(spark):
        spark.sql(
            f"CREATE TABLE {left_table} (id INT, price DOUBLE) USING ICEBERG "
            f"PARTITIONED BY (id) TBLPROPERTIES ({table_props_sql})")
        spark.sql(
            f"CREATE TABLE {right_table} (id INT, value STRING) USING ICEBERG "
            f"PARTITIONED BY (id) TBLPROPERTIES ({table_props_sql})")

        # The two id=1 rows land in different files. Partial clustering assigns them to
        # different join tasks and replicates the matching row from the other side. The missing
        # id=3 on the right also pads that scan with an empty partition.
        spark.sql(f"INSERT INTO {left_table} VALUES (1, 40.0), (2, 10.0), (3, 15.5)")
        spark.sql(f"INSERT INTO {left_table} VALUES (1, 41.0)")
        spark.sql(f"INSERT INTO {right_table} VALUES (1, 'a'), (2, 'b')")

    with_cpu_session(setup_iceberg_tables)

    conf = {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled": "true",
        "spark.sql.iceberg.planning.preserve-data-grouping": "true",
    }

    def distinct_after_spj(spark):
        return spark.sql(
            f"""
            SELECT DISTINCT l.id
            FROM {left_table} l
            JOIN {right_table} r ON l.id = r.id
            """)

    # The SPJ itself is shuffle-free, so the distinct introduces a post-join shuffle.
    # Comparing the results also exercises the replicated and padded scan partitions.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        distinct_after_spj,
        conf={**iceberg_read_enabled_conf, **conf},
        require_non_empty=True,
        gpu_plan_assertion=_assert_partial_clustering_spj_plan)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not is_spark_400_or_later(),
    reason="spark.sql.sources.v2.bucketing.partition.filter.enabled was added in Spark 4.0.0")
@pytest.mark.parametrize("partition_filter", [True, False], ids=["filtered", "unfiltered"])
@pytest.mark.parametrize("partially_clustered", [True, False],
                         ids=["partially_clustered", "clustered"])
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_spj_partition_filter(format_version, spark_tmp_table_factory, partition_filter,
                                      partially_clustered):
    left_table = get_full_table_name(spark_tmp_table_factory)
    right_table = get_full_table_name(spark_tmp_table_factory)
    table_props = _build_tblprops({
        "format-version": format_version,
        # Keep separate INSERTs as separate scan splits so that id=1 is partially clustered.
        "read.split.target-size": "1",
        "read.split.open-file-cost": "1",
    })
    table_props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in table_props.items())

    def setup_iceberg_tables(spark):
        spark.sql(
            f"CREATE TABLE {left_table} (id INT, price DOUBLE) USING ICEBERG "
            f"PARTITIONED BY (id) TBLPROPERTIES ({table_props_sql})")
        spark.sql(
            f"CREATE TABLE {right_table} (id INT, value STRING) USING ICEBERG "
            f"PARTITIONED BY (id) TBLPROPERTIES ({table_props_sql})")

        # Each side gets a key the other lacks (left id=3, right id=4), so partition filtering
        # has something to prune on both scans. Spark decides which side pads and which
        # replicates, so covering both branches of that decision means neither side may be a
        # subset of the intersection. Each side also gets a key split across two files, inside
        # the intersection, so whichever side pads reports numSplits=2 and partial clustering
        # stays observable in the partition counts.
        spark.sql(f"INSERT INTO {left_table} VALUES (1, 40.0), (2, 10.0), (3, 15.5)")
        spark.sql(f"INSERT INTO {left_table} VALUES (1, 41.0)")
        spark.sql(f"INSERT INTO {right_table} VALUES (1, 'a'), (2, 'b'), (4, 'd')")
        spark.sql(f"INSERT INTO {right_table} VALUES (2, 'c')")

    with_cpu_session(setup_iceberg_tables)

    conf = {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
        "spark.sql.sources.v2.bucketing.partition.filter.enabled":
            str(partition_filter).lower(),
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled":
            str(partially_clustered).lower(),
        "spark.sql.iceberg.planning.preserve-data-grouping": "true",
    }

    def join_after_spj(spark):
        return spark.sql(
            f"""
            SELECT l.id, l.price, r.value
            FROM {left_table} l
            JOIN {right_table} r ON l.id = r.id
            """)

    # Both scans plan one partition per common partition value: the intersection {1, 2} when
    # filtering, otherwise the union {1, 2, 3, 4}. Partial clustering adds one more, for the
    # second file of the padding side's split key. KeyGroupedPartitioning.isPartiallyClustered
    # would say this more directly but only exists on Spark 3.5.9+/4.0.3+/4.1.2+, which is the
    # gate this test is deliberately avoiding.
    expected_partitions = (2 if partition_filter else 4) + (1 if partially_clustered else 0)

    def assert_plan(_cpu_plan, plan):
        scans = _assert_spj_join_shape(plan, expect_spj=True)
        counts = [scan.outputPartitioning().numPartitions() for scan in scans]
        assert counts == [expected_partitions] * len(scans), \
            f"Expected {expected_partitions} partitions per scan, found {counts}:\n{plan}"

    # Asserting join output rather than SELECT DISTINCT keeps this off SPARK-55848, which is
    # what forces the patch-level gate on test_iceberg_spj_partial_clustering_distinct.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        join_after_spj,
        conf={**iceberg_read_enabled_conf, **conf},
        require_non_empty=True,
        gpu_plan_assertion=assert_plan)


def _setup_partition_filter_runtime_filter_tables(spark_tmp_table_factory):
    left_table = get_full_table_name(spark_tmp_table_factory)
    right_table = get_full_table_name(spark_tmp_table_factory)
    dim_table = get_full_table_name(spark_tmp_table_factory)

    def setup_iceberg_tables(spark):
        spark.sql(
            f"CREATE TABLE {left_table} (id INT, price DOUBLE) USING ICEBERG "
            f"PARTITIONED BY (id) {_NO_FANOUT}")
        spark.sql(
            f"CREATE TABLE {right_table} (id INT, value STRING) USING ICEBERG "
            f"PARTITIONED BY (id) {_NO_FANOUT}")
        spark.sql(
            f"CREATE TABLE {dim_table} (id INT, tag STRING) USING ICEBERG {_NO_FANOUT}")
        spark.sql(f"INSERT INTO {left_table} VALUES (1, 40.0), (2, 10.0), (3, 15.5)")
        spark.sql(f"INSERT INTO {right_table} VALUES (1, 'a'), (2, 'b')")
        spark.sql(f"INSERT INTO {dim_table} VALUES (1, 'keep'), (2, 'drop'), (3, 'keep')")

    with_cpu_session(setup_iceberg_tables)
    return left_table, right_table, dim_table


def _partition_filter_runtime_filter_conf():
    return {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
        "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly": "false",
        "spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio": "10",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
        "spark.sql.sources.v2.bucketing.partition.filter.enabled": "true",
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled": "false",
        "spark.sql.iceberg.planning.preserve-data-grouping": "true",
    }


def _partition_filter_runtime_filter_query(spark, left_table, right_table, dim_table):
    return spark.sql(
        f"""
        SELECT /*+ BROADCAST(d) */ l.id, l.price, r.value
        FROM {left_table} l
        JOIN {right_table} r ON l.id = r.id
        JOIN {dim_table} d ON l.id = d.id
        WHERE d.tag = 'keep'
        """)


def _assert_partition_filter_runtime_filter_plan(plan, left_table):
    spj_joins = _nodes_of_class(plan, "GpuShuffledSymmetricHashJoinExec")
    assert len(spj_joins) == 1, \
        f"Expected one GPU storage-partitioned join, found {len(spj_joins)}:\n{plan}"
    scans = _assert_spj_join_shape(spj_joins[0], expect_spj=True)
    left_scan = _scan_for_table(scans, left_table, plan)
    values = _evaluated_dynamic_pruning_values(left_scan, plan)
    assert set(values) == {1, 3}, \
        f"Expected dynamic pruning values {{1, 3}}, found {list(values)}:\n{plan}"

    partitioning = left_scan.outputPartitioning()
    assert partitioning.getClass().getSimpleName() == "KeyGroupedPartitioning", plan
    partition_values = partitioning.partitionValues()
    planned_values = {
        partition_values.apply(i).getInt(0) for i in range(partition_values.size())
    }
    assert planned_values == {1, 2}, \
        f"Expected SPJ intersection {{1, 2}}, found {planned_values}:\n{plan}"


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not _is_spark_58783_fixed(),
    reason="SPARK-58783 was fixed in Spark 4.0.5 and 4.1.4; Spark 4.2+ is unaffected")
def test_iceberg_spj_partition_filter_with_runtime_filter(spark_tmp_table_factory):
    left_table, right_table, dim_table = \
        _setup_partition_filter_runtime_filter_tables(spark_tmp_table_factory)
    conf = _partition_filter_runtime_filter_conf()

    def join_with_runtime_filter(spark):
        return _partition_filter_runtime_filter_query(
            spark, left_table, right_table, dim_table)

    def assert_plan(_cpu_plan, plan):
        _assert_partition_filter_runtime_filter_plan(plan, left_table)

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        join_with_runtime_filter,
        conf=conf,
        require_non_empty=True,
        gpu_plan_assertion=assert_plan)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not _is_spark_58783_affected(),
    reason="Requires an affected Spark 4.0.x or 4.1.x release")
def test_iceberg_spj_partition_filter_with_runtime_filter_cpu_fails_gpu_succeeds(
        spark_tmp_table_factory):
    left_table, right_table, dim_table = \
        _setup_partition_filter_runtime_filter_tables(spark_tmp_table_factory)
    conf = _partition_filter_runtime_filter_conf()

    def join_with_runtime_filter(spark):
        return _partition_filter_runtime_filter_query(
            spark, left_table, right_table, dim_table)

    def assert_plan(plan):
        _assert_partition_filter_runtime_filter_plan(plan, left_table)

    assert_spark_exception(
        lambda: with_cpu_session(lambda spark: join_with_runtime_filter(spark).collect(), conf=conf),
        "During runtime filtering, data source must not report new partition values")
    gpu_result = with_gpu_session(
        lambda spark: _collect_with_plan_assertion(
            spark, join_with_runtime_filter, assert_plan),
        conf=conf)
    assert_equal_with_local_sort([Row(id=1, price=40.0, value="a")], gpu_result)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not (is_spark_40x() or is_spark_41x()),
    reason="Requires the scan-side join-key projection in Spark 4.0.x and 4.1.x")
def test_iceberg_spj_runtime_filter_with_trailing_join_key(spark_tmp_table_factory):
    left_table = get_full_table_name(spark_tmp_table_factory)
    right_table = get_full_table_name(spark_tmp_table_factory)

    def setup_iceberg_tables(spark):
        spark.sql(
            f"CREATE TABLE {left_table} (store_id INT, dept_id INT, data STRING) USING ICEBERG "
            f"PARTITIONED BY (dept_id, data) {_NO_FANOUT}")
        spark.sql(
            f"CREATE TABLE {right_table} (store_id INT, dept_id INT, data STRING) USING ICEBERG "
            f"PARTITIONED BY (data) {_NO_FANOUT}")
        spark.sql(
            f"INSERT INTO {left_table} VALUES "
            "(100, 1, 'aa'), (200, 2, 'aa'), (300, 3, 'bb')")
        spark.sql(f"INSERT INTO {right_table} VALUES (500, 1, 'aa'), (500, 2, 'bb')")

    with_cpu_session(setup_iceberg_tables)

    conf = {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
        "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly": "false",
        "spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio": "10",
        "spark.sql.requireAllClusterKeysForCoPartition": "false",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.allowJoinKeysSubsetOfPartitionKeys.enabled": "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
        "spark.sql.sources.v2.bucketing.partition.filter.enabled": "false",
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled": "false",
        "spark.sql.iceberg.planning.preserve-data-grouping": "true",
    }

    def join_with_runtime_filter(spark):
        return spark.sql(
            f"""
            SELECT /*+ MERGE(l, r) */
                l.store_id, l.dept_id, l.data, r.store_id AS right_store_id
            FROM {left_table} l
            JOIN {right_table} r ON l.data = r.data
            WHERE r.dept_id = 1
            """)

    def assert_plan(plan):
        scans = _assert_spj_join_shape(plan, expect_spj=True)
        left_scan = _scan_for_table(scans, left_table, plan)
        values = _evaluated_dynamic_pruning_values(left_scan, plan)
        actual_values = {str(value) for value in values}
        assert actual_values == {"aa"}, \
            f"Expected dynamic pruning value aa, found {actual_values}:\n{plan}"

        positions_option = left_scan.spjParams().joinKeyPositions()
        assert positions_option.isDefined(), \
            f"Expected projected join-key positions on the left scan:\n{plan}"
        positions = positions_option.get()
        actual_positions = [positions.apply(i) for i in range(positions.size())]
        assert actual_positions == [1], \
            f"Expected trailing join-key position [1], found {actual_positions}:\n{plan}"
        assert left_scan.spjParams().commonPartitionValues().isDefined(), \
            f"Expected common partition values on the left SPJ scan:\n{plan}"

    expected = [
        Row(store_id=100, dept_id=1, data="aa", right_store_id=500),
        Row(store_id=200, dept_id=2, data="aa", right_store_id=500),
    ]

    if _is_spark_58783_affected():
        assert_spark_exception(
            lambda: with_cpu_session(
                lambda spark: join_with_runtime_filter(spark).collect(), conf=conf),
            # Iceberg's partition type check may fail before Spark reaches the projected-key cast.
            re.compile(
                r"Wrong class, expected java\.lang\.CharSequence, but was java\.lang\.Integer"
                r"|java\.lang\.Integer cannot be cast to class "
                r"org\.apache\.spark\.unsafe\.types\.UTF8String"))
        gpu_result = with_gpu_session(
            lambda spark: _collect_with_plan_assertion(
                spark, join_with_runtime_filter, assert_plan),
            conf=conf)
        assert_equal_with_local_sort(expected, gpu_result)
    else:
        assert_cpu_and_gpu_are_equal_collect_with_capture(
            join_with_runtime_filter,
            conf=conf,
            require_non_empty=True,
            gpu_plan_assertion=lambda _cpu_plan, plan: assert_plan(plan))


# Enough rows that every bucket of the wider bucket(4) side is populated, so reducing it to
# gcd(4, 2) = 2 buckets moves rows into partition values the raw-keyed lookup cannot find.
_SPJ_REDUCIBLE_ROWS = 64

# Iceberg supplies the Reducer implementations that make these pairs compatible:
# BucketFunction.BucketReducer reduces bucket(4) to bucket(2), and HoursFunction.HourToDaysReducer
# reduces hours(ts) to days(ts). Hours against days is the harsher case because day numbers and
# hour numbers share no range, so every raw lookup misses and the join returns nothing.
_spj_reducible_transforms = [
    pytest.param("k INT", "CAST(id AS INT)", "bucket(4, k)", "bucket(2, k)",
                 id="bucket4_bucket2"),
    pytest.param("k TIMESTAMP",
                 "TIMESTAMP'2024-01-01 00:00:00' + MAKE_DT_INTERVAL(0, CAST(id AS INT), 0, 0)",
                 "hours(k)", "days(k)", id="hours_days"),
]


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not is_spark_400_or_later(),
    reason="spark.sql.sources.v2.bucketing.allowCompatibleTransforms.enabled was added in "
           "Spark 4.0.0")
@pytest.mark.parametrize("allow_compatible_transforms", [True, False],
                         ids=["reduced", "control"])
@pytest.mark.parametrize("key_ddl, key_value_sql, left_transform, right_transform",
                         _spj_reducible_transforms)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_spj_reducible_transforms(format_version, spark_tmp_table_factory, key_ddl, key_value_sql,
                                          left_transform, right_transform,
                                          allow_compatible_transforms):
    left_table = get_full_table_name(spark_tmp_table_factory)
    right_table = get_full_table_name(spark_tmp_table_factory)

    def setup_iceberg_tables(spark):
        spark.sql(
            f"CREATE TABLE {left_table} ({key_ddl}, price DOUBLE) USING ICEBERG "
            f"PARTITIONED BY ({left_transform}) {iceberg_table_properties_sql(format_version)}")
        spark.sql(
            f"CREATE TABLE {right_table} ({key_ddl}, value STRING) USING ICEBERG "
            f"PARTITIONED BY ({right_transform}) {iceberg_table_properties_sql(format_version)}")
        spark.sql(
            f"INSERT INTO {left_table} SELECT {key_value_sql}, CAST(id AS DOUBLE) "
            f"FROM range({_SPJ_REDUCIBLE_ROWS})")
        spark.sql(
            f"INSERT INTO {right_table} SELECT {key_value_sql}, CAST(id AS STRING) "
            f"FROM range({_SPJ_REDUCIBLE_ROWS})")

    with_cpu_session(setup_iceberg_tables)

    conf = {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
        "spark.sql.sources.v2.bucketing.allowCompatibleTransforms.enabled":
            str(allow_compatible_transforms).lower(),
        # Must stay off: with partial clustering on, key compatibility degrades to
        # isSameFunction, which rejects these transform pairs, so SPJ never engages at all.
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled": "false",
        "spark.sql.iceberg.planning.preserve-data-grouping": "true",
    }

    def join_on_reducible_transforms(spark):
        return spark.sql(
            f"""
            SELECT l.k, l.price, r.value
            FROM {left_table} l
            JOIN {right_table} r ON l.k = r.k
            """)

    # With compatible transforms disabled the same tables join through a shuffle instead, which
    # is the control: it must stay correct whether or not the reduced grouping works.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        join_on_reducible_transforms,
        conf={**iceberg_read_enabled_conf, **conf},
        require_non_empty=True,
        gpu_plan_assertion=lambda _cpu_plan, plan: _assert_spj_join_shape(
            plan, allow_compatible_transforms))


@allow_non_gpu("BatchScanExec")
@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_fallback_not_unsafe_row(format_version, spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)
    def setup_iceberg_table(spark):
        spark.sql(
            f"CREATE TABLE {full_table} (id BIGINT, data STRING) USING ICEBERG {iceberg_table_properties_sql(format_version)}")
        spark.sql(f"INSERT INTO {full_table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT COUNT(DISTINCT id) from {full_table}"),
        conf={**iceberg_read_enabled_conf, "spark.rapids.sql.format.iceberg.enabled": "false"}
    )


@iceberg
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_scan_from_background_thread(format_version, spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)

    def setup_iceberg_table(spark):
        spark.sql(
            f"CREATE TABLE {full_table} (id BIGINT) USING ICEBERG {iceberg_table_properties_sql(format_version)}")
        spark.sql(f"INSERT INTO {full_table} VALUES (1), (2), (3)")

    with_cpu_session(setup_iceberg_table)

    def scan_iceberg_table(spark):
        def run_query():
            df = spark.sql(f"SELECT SUM(id) FROM {full_table}")
            return df.collect(), df

        with ThreadPoolExecutor(max_workers=1) as executor:
            result, df = executor.submit(run_query).result()

        assert result[0][0] == 6
        callback = spark._sc._jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
        callback.assertContains(df._jdf, "GpuBatchScanExec")

    with_gpu_session(scan_iceberg_table, conf=iceberg_read_enabled_conf)

@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(is_databricks_runtime(),
                    reason="AQE+DPP not supported until Spark 3.2.0+ and AQE+DPP not supported on Databricks")
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_aqe_dpp(format_version, spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = two_col_df(spark, int_gen, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(
            f"CREATE TABLE {full_table} (a INT, b INT) USING ICEBERG PARTITIONED BY (a) {iceberg_table_properties_sql(format_version)}")
        spark.sql(f"INSERT INTO {full_table} SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT * from {full_table} as X JOIN {full_table} as Y ON X.a = Y.a "
                                 f"WHERE Y.a > 0"),
        conf={**iceberg_read_enabled_conf, "spark.sql.adaptive.enabled": "true",
              "spark.rapids.sql.format.parquet.reader.type": reader_type,
              "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true"})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("data_gens", iceberg_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_parquet_read_round_trip_select_one(
        format_version, spark_tmp_table_factory, data_gens, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql(
            f"CREATE TABLE {full_table} USING ICEBERG {iceberg_table_properties_sql(format_version)} AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    # explicitly only select 1 column to make sure we test that path in the schema parsing code
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT _c0 FROM {full_table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("data_gens", iceberg_primitive_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_parquet_read_round_trip(format_version, spark_tmp_table_factory, data_gens, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql(
            f"CREATE TABLE {full_table} USING ICEBERG {iceberg_table_properties_sql(format_version)} AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT * FROM {full_table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("data_gens", iceberg_gens_list, ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_parquet_read_round_trip_all_types(
        format_version, spark_tmp_table_factory, data_gens, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        spark.sql(
            f"CREATE TABLE {full_table} USING ICEBERG {iceberg_table_properties_sql(format_version)} AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT * FROM {full_table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@pytest.mark.parametrize("data_gens", [[long_gen]], ids=idfn)
@pytest.mark.parametrize("iceberg_format", ["orc", "avro"], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_unsupported_formats(
        format_version, spark_tmp_table_factory, data_gens, iceberg_format, reader_type):
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(data_gens)]
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = gen_df(spark, gen_list)
        df.createOrReplaceTempView(tmpview)
        props = _build_tblprops({"format-version": format_version, 'write.format.default': iceberg_format})
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG "
                  f"TBLPROPERTIES({props_sql}) "
                  f"AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_spark_exception(
        lambda : with_gpu_session(
            lambda spark : spark.sql(f"SELECT * FROM {full_table}").collect(),
            conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type}),
        "UnsupportedOperationException")

@iceberg
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("disable_conf", ["spark.rapids.sql.format.iceberg.enabled",
                                          "spark.rapids.sql.format.iceberg.read.enabled"], ids=idfn)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_fallback(format_version, spark_tmp_table_factory, disable_conf):
    full_table = get_full_table_name(spark_tmp_table_factory)
    def setup_iceberg_table(spark):
        spark.sql(
            f"CREATE TABLE {full_table} (id BIGINT, data STRING) USING ICEBERG {iceberg_table_properties_sql(format_version)}")
        spark.sql(f"INSERT INTO {full_table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_fallback_collect(
        lambda spark : spark.sql(f"SELECT * FROM {full_table}"),
        "BatchScanExec",
        conf = {**iceberg_read_enabled_conf, disable_conf : "false"})


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@allow_non_gpu("BatchScanExec", "ColumnarToRowExec")
@ignore_order(local=True)
def test_iceberg_v3_read_fallback(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)
    props = _build_tblprops({"format-version": "3"})
    props_sql = ", ".join(f"'{key}' = '{value}'" for key, value in props.items())

    def setup_table(spark):
        spark.sql(
            f"CREATE TABLE {table_name} (id BIGINT, data STRING) USING ICEBERG "
            f"TBLPROPERTIES ({props_sql})")
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    with_cpu_session(setup_table)
    assert_gpu_fallback_collect(
        lambda spark: spark.sql(f"SELECT * FROM {table_name}"),
        "BatchScanExec")


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not supports_iceberg_row_lineage_inheritance,
    reason=ICEBERG_ROW_LINEAGE_INHERITANCE_UNSUPPORTED_REASON)
@pytest.mark.parametrize("reader_type", rapids_reader_types)
def test_iceberg_v3_row_lineage_read(spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)

    def setup_iceberg_table(spark):
        # Snapshots created before a v3 upgrade do not have row lineage. A snapshot created after
        # the upgrade assigns lineage to both existing and newly-added files.
        spark.sql(f"CREATE TABLE {full_table} (id BIGINT) USING ICEBERG "
                  f"TBLPROPERTIES ('format-version' = '2')")
        row_lineage_df(spark).writeTo(full_table).append()
        v2_snapshot_id = spark.sql(
            f"SELECT snapshot_id FROM {full_table}.snapshots ORDER BY committed_at DESC") \
            .head()[0]
        spark.sql(
            f"ALTER TABLE {full_table} SET TBLPROPERTIES ("
            "'format-version' = '3', "
            "'write.parquet.row-group-size-bytes' = '4096', "
            "'read.split.target-size' = '4096', "
            "'read.split.open-file-cost' = '0')")

        row_lineage_df(spark, start=DEFAULT_DATA_GEN_LENGTH).writeTo(full_table).append()
        return v2_snapshot_id

    v2_snapshot_id = with_cpu_session(setup_iceberg_table)
    read_conf = {
        "spark.rapids.sql.format.iceberg.v3.enabled": "true",
        "spark.rapids.sql.format.parquet.reader.type": reader_type
    }
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            f"SELECT id, _row_id, _last_updated_sequence_number FROM {full_table} "
            f"VERSION AS OF {v2_snapshot_id}"),
        conf=read_conf)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            f"SELECT id, _row_id, _last_updated_sequence_number FROM {full_table}"),
        conf=read_conf)


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not supports_iceberg_row_lineage_inheritance,
    reason=ICEBERG_ROW_LINEAGE_INHERITANCE_UNSUPPORTED_REASON)
def test_iceberg_v3_row_lineage_append(spark_tmp_table_factory):
    def setup_iceberg_table(spark, table):
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING ICEBERG "
                  f"TBLPROPERTIES ('format-version' = '2')")
        row_lineage_df(spark, start=1).writeTo(table).append()
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ("
            "'format-version' = '3', "
            "'write.parquet.row-group-size-bytes' = '4096', "
            "'read.split.target-size' = '4096', "
            "'read.split.open-file-cost' = '0')")

    def append_data(spark, table):
        row_lineage_df(
            spark, start=DEFAULT_DATA_GEN_LENGTH + 1).writeTo(table).append()

    _assert_gpu_and_cpu_lineage_writes_are_equal(
        spark_tmp_table_factory,
        setup_iceberg_table,
        append_data,
        lambda spark, table: spark.sql(
            f"SELECT id, _pos, _row_id, _last_updated_sequence_number FROM {table}"))


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not supports_iceberg_row_lineage_inheritance,
    reason=ICEBERG_ROW_LINEAGE_INHERITANCE_UNSUPPORTED_REASON)
def test_iceberg_v3_row_lineage_rewrite_data_files(spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)

    def setup_iceberg_table(spark):
        spark.sql(
            f"CREATE TABLE {full_table} (id BIGINT) USING ICEBERG "
            "TBLPROPERTIES ('format-version' = '3')")
        half_length = DEFAULT_DATA_GEN_LENGTH // 2
        row_lineage_df(spark, length=half_length).writeTo(full_table).append()
        row_lineage_df(spark, start=half_length, length=half_length) \
            .writeTo(full_table).append()
        assert spark.sql(f"SELECT count(*) FROM {full_table}.data_files").head()[0] > 1

        spark.sql(
            f"CALL spark_catalog.system.rewrite_data_files(table => '{full_table}', "
            "options => map('min-input-files', '2'))")
        assert spark.sql(f"SELECT count(*) FROM {full_table}.data_files").head()[0] == 1

    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            f"SELECT id, _pos, _row_id, _last_updated_sequence_number FROM {full_table}"),
        conf={
            "spark.rapids.sql.format.iceberg.v3.enabled": "true",
            "spark.rapids.sql.format.parquet.reader.type": "COALESCING"
        })


@iceberg
@ignore_order(local=True)
@pytest.mark.skipif(
    not supports_iceberg_row_lineage_inheritance,
    reason=ICEBERG_ROW_LINEAGE_INHERITANCE_UNSUPPORTED_REASON)
def test_iceberg_v3_row_lineage_insert_overwrite(spark_tmp_table_factory):
    source_view = spark_tmp_table_factory.get()

    def setup_iceberg_table(spark, table):
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT, v BIGINT) USING ICEBERG "
            "TBLPROPERTIES ('format-version' = '3')")
        row_lineage_df(spark, with_value=True).writeTo(table).append()

    def overwrite(spark, table):
        row_lineage_df(
            spark,
            start=DEFAULT_DATA_GEN_LENGTH,
            with_value=True).createOrReplaceTempView(source_view)
        spark.sql(f"INSERT OVERWRITE {table} SELECT * FROM {source_view}").collect()

    _assert_gpu_and_cpu_lineage_writes_are_equal(
        spark_tmp_table_factory,
        setup_iceberg_table,
        overwrite,
        lambda spark, table: spark.sql(
            f"SELECT id, v, _row_id, _last_updated_sequence_number FROM {table}"))


@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
# Compression codec to test and whether the codec is supported by cudf
# Note that compression codecs brotli and lzo need extra jars
# https://githbub.com/NVIDIA/spark-rapids/issues/143
@pytest.mark.parametrize("codec_info", [
    ("uncompressed", None),
    ("snappy", None),
    ("gzip", None),
    pytest.param(("lz4", "Unsupported Parquet compression type")),
    ("zstd", None)], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_parquet_compression_codec(format_version, spark_tmp_table_factory, codec_info, reader_type):
    codec, error_msg = codec_info
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        props = _build_tblprops({"format-version": format_version, 'write.parquet.compression-codec': codec})
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"CREATE TABLE {full_table} (id BIGINT, data BIGINT) USING ICEBERG "
                  f"TBLPROPERTIES({props_sql})")
        spark.sql(f"INSERT INTO {full_table} SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    query = f"SELECT * FROM {full_table}"
    read_conf = {'spark.rapids.sql.format.parquet.reader.type': reader_type}
    if error_msg:
        assert_spark_exception(
            lambda : with_gpu_session(lambda spark : spark.sql(query).collect(), conf={**iceberg_read_enabled_conf, **read_conf}),
            error_msg)
    else:
        assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.sql(query), conf={**iceberg_read_enabled_conf, **read_conf})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("key_gen", [int_gen, long_gen, string_gen, boolean_gen, date_gen, timestamp_gen, decimal_gen_64bit], ids=idfn)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_partition_key(format_version, spark_tmp_table_factory, key_gen, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = two_col_df(spark, key_gen, long_gen).orderBy("a")
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG PARTITIONED BY (a) {iceberg_table_properties_sql(
            format_version)} " + \
                  f"AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT a FROM {full_table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_input_meta(format_version, spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen).orderBy("a")
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG PARTITIONED BY (a) {iceberg_table_properties_sql(
            format_version)} " + \
                  f"AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(
            "SELECT a, input_file_name(), input_file_block_start(), input_file_block_length() " + \
            f"FROM {full_table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_disorder_read_schema(format_version, spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = three_col_df(spark, long_gen, string_gen, float_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {iceberg_table_properties_sql(format_version)} " + \
                  f"AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql(f"SELECT b,c,a FROM {full_table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_appended_table(format_version, spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {iceberg_table_properties_sql(format_version)} " + \
                  f"AS SELECT * FROM {tmpview}")
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {full_table} " + \
                  f"SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(lambda spark : spark.sql(f"SELECT * FROM {full_table}"), conf=iceberg_read_enabled_conf)

@iceberg
# Some metadata files have types that are not supported on the GPU yet (e.g.: BinaryType)
@allow_non_gpu("BatchScanExec", "ProjectExec")
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_metadata_fallback(format_version, spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {iceberg_table_properties_sql(format_version)} " + \
                  f"AS SELECT * FROM {tmpview}")
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {full_table} " + \
                  f"SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    for subtable in ["all_data_files", "all_manifests", "files", "history",
                     "manifests", "partitions", "snapshots"]:
        # SQL does not have syntax to read table metadata
        assert_gpu_fallback_collect(
            lambda spark : spark.read.format("iceberg").load(f"{full_table}.{subtable}"),
            "BatchScanExec", conf=iceberg_read_enabled_conf)

@iceberg
# Some metadata files have types that are not supported on the GPU yet (e.g.: BinaryType)
@allow_non_gpu("BatchScanExec", "ProjectExec")
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_metadata_count(format_version, spark_tmp_table_factory):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {iceberg_table_properties_sql(format_version)} " + \
                  f"AS SELECT * FROM {tmpview}")
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {full_table} " + \
                  f"SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    for subtable in ["all_data_files", "all_manifests", "files", "history",
                     "manifests", "partitions", "snapshots"]:
        # SQL does not have syntax to read table metadata
        assert_gpu_and_cpu_row_counts_equal(
            lambda spark : spark.read.format("iceberg").load(f"{full_table}.{subtable}"), conf=iceberg_read_enabled_conf)

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_timetravel(format_version, spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_snapshots(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"CREATE TABLE {full_table} USING ICEBERG {iceberg_table_properties_sql(format_version)} " + \
                  f"AS SELECT * FROM {tmpview}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {full_table} " + \
                  f"SELECT * FROM {tmpview}")
        return spark.sql("SELECT snapshot_id FROM {}.snapshots ".format(full_table) + \
                         "ORDER BY committed_at").head()[0]
    first_snapshot_id = with_cpu_session(setup_snapshots)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.option("versionAsOf", first_snapshot_id) \
            .format("iceberg").load("{}".format(full_table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_incremental_read(format_version, spark_tmp_table_factory, reader_type):
    full_table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_snapshots(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(full_table) + \
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(full_table) + \
                  "SELECT * FROM {}".format(tmpview))
        df = binary_op_df(spark, long_gen, seed=2)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(full_table) + \
                  "SELECT * FROM {}".format(tmpview))
        return spark.sql("SELECT snapshot_id FROM {}.snapshots ".format(full_table) + \
                         "ORDER BY committed_at").collect()
    snapshots = with_cpu_session(setup_snapshots)
    start_snapshot, end_snapshot = [ row[0] for row in snapshots[:2] ]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read \
            .option("start-snapshot-id", start_snapshot) \
            .option("end-snapshot-id", end_snapshot) \
            .format("iceberg").load("{}".format(full_table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_reorder_columns(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ALTER COLUMN b FIRST".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_rename_column(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} RENAME COLUMN a TO c".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_column_names_swapped(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} RENAME COLUMN a TO c".format(table))
        spark.sql("ALTER TABLE {} RENAME COLUMN b TO a".format(table))
        spark.sql("ALTER TABLE {} RENAME COLUMN c TO b".format(table))
        df = binary_op_df(spark, long_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_alter_column_type(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = three_col_df(spark, int_gen, float_gen, DecimalGen(precision=7, scale=3))
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ALTER COLUMN a TYPE BIGINT".format(table))
        spark.sql("ALTER TABLE {} ALTER COLUMN b TYPE DOUBLE".format(table))
        spark.sql("ALTER TABLE {} ALTER COLUMN c TYPE DECIMAL(17, 3)".format(table))
        df = three_col_df(spark, long_gen, double_gen, DecimalGen(precision=17, scale=3))
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_add_column(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ADD COLUMNS (c DOUBLE)".format(table))
        df = three_col_df(spark, long_gen, long_gen, double_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_remove_column(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} DROP COLUMN a".format(table))
        df = unary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_add_partition_field(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("ALTER TABLE {} ADD PARTITION FIELD b".format(table))
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {} ORDER BY b".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_drop_partition_field(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(
            "CREATE TABLE {} (a INT, b INT) USING ICEBERG PARTITIONED BY (b) ".format(table) + iceberg_table_properties_sql(format_version))
        spark.sql("INSERT INTO {} SELECT * FROM {} ORDER BY b".format(table, tmpview))
        spark.sql("ALTER TABLE {} DROP PARTITION FIELD b".format(table))
        df = binary_op_df(spark, int_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("INSERT INTO {} ".format(table) + \
                  "SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_v1_delete(spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql("CREATE TABLE {} USING ICEBERG ".format(table) + \
                  _NO_FANOUT + " AS SELECT * FROM {}".format(tmpview))
        spark.sql("DELETE FROM {} WHERE a < 0".format(table))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT * FROM {}".format(table)),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_parquet_read_with_input_file(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(
            "CREATE TABLE {} USING ICEBERG ".format(table) + iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmpview))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.sql("SELECT *, input_file_name() FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(not is_iceberg_remote_catalog(), reason="Filecache is only meaningful with remote storage, skipping for local Hadoop filesystem")
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_with_filecache(format_version, spark_tmp_table_factory, reader_type):
    """Create a table on CPU, read it twice on GPU with file cache enabled,
    and verify both reads match the CPU result."""
    filecache_enabled = with_gpu_session(
        lambda spark: spark.conf.get("spark.rapids.filecache.enabled", "false"), conf=iceberg_read_enabled_conf)
    assert filecache_enabled == "true", \
        "spark.rapids.filecache.enabled must be set to true to run this test"
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        df = binary_op_df(spark, long_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(
            f"CREATE TABLE {table} USING ICEBERG {iceberg_table_properties_sql(format_version)} AS SELECT * FROM {tmpview}")
    with_cpu_session(setup_iceberg_table)
    query = f"SELECT * FROM {table}"
    cpu_result = with_cpu_session(lambda spark: spark.sql(query).collect())
    # Note: spark.rapids.filecache.enabled is a startup-only config, so it must
    # be set via PYSP_TEST_spark_rapids_filecache_enabled env var, not here.
    filecache_conf = {
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
    }
    gpu_result_1 = with_gpu_session(lambda spark: spark.sql(query).collect(), conf={**iceberg_read_enabled_conf, **filecache_conf})
    gpu_result_2 = with_gpu_session(lambda spark: spark.sql(query).collect(), conf={**iceberg_read_enabled_conf, **filecache_conf})
    assert_equal_with_local_sort(cpu_result, gpu_result_1)
    assert_equal_with_local_sort(cpu_result, gpu_result_2)

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_parquet_read_from_url_encoded_path(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmp_view = spark_tmp_table_factory.get()
    partition_gen = StringGen(pattern="(.|\n){1,10}", nullable=False)\
        .with_special_case('%29%3EtkiudF4%3C', 1000)\
        .with_special_case('%2F%23_v9kRtI%27', 1000)\
        .with_special_case('aK%2BAgI%21l8%3E', 1000)\
        .with_special_case('p%2Cmtx%3FCXMd', 1000)
    def setup_iceberg_table(spark):
        df = two_col_df(spark, long_gen, partition_gen).sortWithinPartitions('b')
        df.createOrReplaceTempView(tmp_view)
        spark.sql(
            "CREATE TABLE {} USING ICEBERG PARTITIONED BY (b) ".format(table) + iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmp_view))
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("SELECT * FROM {}".format(table)),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(not is_iceberg_rest_catalog(),
                    reason="S3 path handling is exercised only with the REST catalog")
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_parquet_read_from_uri_invalid_s3_path(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    tmp_view = spark_tmp_table_factory.get()
    partition_gen = StringGen(pattern="(.|\n){1,10}", nullable=False)\
        .with_special_case('uri invalid path', 1000)

    def setup_iceberg_table(spark):
        df = two_col_df(spark, long_gen, partition_gen).sortWithinPartitions('b')
        df.createOrReplaceTempView(tmp_view)
        spark.sql("CREATE TABLE {} USING ICEBERG PARTITIONED BY (b) ".format(table) +
                  iceberg_table_properties_sql(format_version) + " AS SELECT * FROM {}".format(tmp_view))

    with_cpu_session(setup_iceberg_table)
    assert with_gpu_session(
        lambda spark: spark.sparkContext.getConf().get(
            'spark.rapids.perfio.s3.enabled', 'false') == 'true', conf=iceberg_read_enabled_conf), \
        "PerfIO S3 must be enabled at Spark startup for REST catalog tests"
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT * FROM {table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True) # Iceberg plans with a thread pool and is not deterministic in file ordering
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_metadata_columns_with_partition_evolution(
        format_version, spark_tmp_table_factory, reader_type):
    """
    Test reading Iceberg metadata columns (_file, _pos, _spec_id, _partition) with partition evolution.
    """
    table = get_full_table_name(spark_tmp_table_factory)
    tmpview = spark_tmp_table_factory.get()
    def setup_iceberg_table(spark):
        # Create table partitioned by a
        df = three_col_df(spark, long_gen, int_gen, string_gen)
        df.createOrReplaceTempView(tmpview)
        spark.sql(
            f"CREATE TABLE {table} (a BIGINT, b INT, c STRING) USING ICEBERG PARTITIONED BY (a) {iceberg_table_properties_sql(format_version)}")
        spark.sql(f"INSERT INTO {table} SELECT * FROM {tmpview}")
        
        # Evolve partition: add b as partition field
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD b")
        
        # Insert more data after partition evolution
        df = three_col_df(spark, long_gen, int_gen, string_gen, seed=1)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {table} SELECT * FROM {tmpview}")
        
        # Evolve partition again: drop a, keep b
        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD a")
        
        # Insert more data after second partition evolution
        df = three_col_df(spark, long_gen, int_gen, string_gen, seed=2)
        df.createOrReplaceTempView(tmpview)
        spark.sql(f"INSERT INTO {table} SELECT * FROM {tmpview}")
    
    with_cpu_session(setup_iceberg_table)
    
    # Test reading all metadata columns along with data columns
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b, c, _file, _pos, _spec_id, _partition FROM {table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_format_versions)
def test_iceberg_read_pos_with_split_file(format_version, spark_tmp_table_factory, reader_type):
    # Writes a single Parquet data file containing many row groups, then forces
    # Iceberg's planner to split that file across multiple scan tasks at row-group
    # byte boundaries via a tiny row-group size, a tiny split target, and a zero
    # per-file open cost. The query projects Iceberg's _pos metadata column; the
    # GPU result must match CPU regardless of how the planner splits the file.
    # Note: with the COALESCING reader_type, GpuReaderFactory routes _pos scans
    # away from the coalescing reader (canUseCoalescing excludes
    # hasRowPositionMetadata), so that parametrization actually exercises the
    # per-file/multi-thread path. The split-file _pos correctness assertion still
    # holds in whichever reader is chosen.
    table = get_full_table_name(spark_tmp_table_factory)
    def setup_iceberg_table(spark):
        spark.sql(f"CREATE TABLE {table} (id BIGINT) USING ICEBERG {iceberg_table_properties_sql(format_version)}")
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ("
            "'write.parquet.row-group-size-bytes' = '4096', "
            "'read.split.target-size'             = '4096', "
            "'read.split.open-file-cost'          = '0')")
        spark.range(0, 1500).coalesce(1).writeTo(table).append()
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT id, _pos FROM {table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_read_mor_with_pos_deletes_split_file(spark_tmp_table_factory, reader_type):
    # Same split-file conditions as test_iceberg_read_pos_with_split_file, but on
    # a v2 Merge-on-Read table with positional delete files. The query does not
    # project _pos; the Iceberg reader still adds it internally to match against
    # the positional delete list. The test deletes a scattered subset by id and
    # asserts CPU and GPU return the same surviving rows.
    # Note: the COALESCING reader_type does not actually exercise the coalescing
    # reader here either — GpuReaderFactory.canUseCoalescing already excludes any
    # scan with delete files (`hasNoDeletes` is false), so the test runs against
    # the per-file/multi-thread reader regardless of the requested reader_type.
    table = get_full_table_name(spark_tmp_table_factory)
    def setup_iceberg_table(spark):
        spark.sql(
            f"CREATE TABLE {table} (id BIGINT) USING ICEBERG TBLPROPERTIES ("
            "'format-version'                     = '2', "
            "'write.delete.mode'                  = 'merge-on-read', "
            "'write.spark.fanout.enabled'         = 'false', "
            "'write.parquet.row-group-size-bytes' = '4096', "
            "'read.split.target-size'             = '4096', "
            "'read.split.open-file-cost'          = '0')")
        spark.range(0, 1500).coalesce(1).writeTo(table).append()
        spark.sql(f"DELETE FROM {table} WHERE id % 7 = 3")
    with_cpu_session(setup_iceberg_table)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT id FROM {table}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_read_format_versions)
def test_iceberg_small_file_combine_with_schema_evolution(format_version, spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    schema_evolution_gens_v1 = [('a', long_gen), ('b', int_gen)]
    schema_evolution_gens_v2 = schema_evolution_gens_v1 + [('c', string_gen)]
    base_seed = get_datagen_seed()
    create_iceberg_table(
        table,
        partition_col_sql='bucket(2, a)',
        df_gen=lambda spark: gen_df(spark, schema_evolution_gens_v1), format_version=format_version)

    def setup_iceberg_table(spark):
        for seed_offset in range(4):
            gen_df(
                spark,
                schema_evolution_gens_v1,
                length=64,
                seed=base_seed + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(f"ALTER TABLE {table} ADD COLUMN c STRING")
        for seed_offset in range(4):
            gen_df(
                spark,
                schema_evolution_gens_v2,
                length=64,
                seed=base_seed + 100 + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ("
            "'read.split.target-size' = '268435456', "
            "'read.split.planning-lookback' = '100')")
        spark.sql(f"REFRESH TABLE {table}")

    with_cpu_session(setup_iceberg_table)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b, c FROM {table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize("format_version", iceberg_read_format_versions)
def test_iceberg_small_file_combine_with_partition_spec_evolution(
        format_version,         spark_tmp_table_factory, reader_type):
    table = get_full_table_name(spark_tmp_table_factory)
    partition_evolution_gens = [('a', long_gen), ('b', int_gen), ('c', string_gen)]
    base_seed = get_datagen_seed()
    create_iceberg_table(
        table,
        partition_col_sql='bucket(10, a)',
        df_gen=lambda spark: gen_df(spark, partition_evolution_gens), format_version=format_version)

    def setup_iceberg_table(spark):
        for seed_offset in range(4):
            gen_df(
                spark,
                partition_evolution_gens,
                length=64,
                seed=base_seed + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(10, b)")
        for seed_offset in range(4):
            gen_df(
                spark,
                partition_evolution_gens,
                length=64,
                seed=base_seed + 100 + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD bucket(10, a)")
        for seed_offset in range(4):
            gen_df(
                spark,
                partition_evolution_gens,
                length=64,
                seed=base_seed + 200 + seed_offset,
                num_slices=1).writeTo(table).append()

        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES ("
            "'read.split.target-size' = '268435456', "
            "'read.split.planning-lookback' = '100')")
        spark.sql(f"REFRESH TABLE {table}")

    with_cpu_session(setup_iceberg_table)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b, c, _spec_id, _partition FROM {table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason = "S3tables catalog is managed")
@pytest.mark.parametrize("format_version", iceberg_read_format_versions)
def test_iceberg_small_file_combine_with_add_files_identity_partition(
        format_version,         spark_tmp_table_factory, reader_type):
    target_table = get_full_table_name(spark_tmp_table_factory)
    source_table = get_full_table_name(spark_tmp_table_factory)
    create_iceberg_table(
        target_table,
        partition_col_sql='a',
        df_gen=lambda spark: spark.createDataFrame([], 'a long, b string'), format_version=format_version)

    def setup_imported_table(spark):
        spark.sql(
            f"CREATE TABLE {source_table} (a BIGINT, b STRING) "
            "USING PARQUET "
            "PARTITIONED BY (a)")
        source_columns = spark.table(source_table).columns

        partition_values = [2451350, 2452349, 2452323]
        for batch_id in range(4):
            batch_gens = [
                ('a', RepeatSeqGen(partition_values * 2, data_type=long_gen.data_type)),
                ('b', RepeatSeqGen(
                    [f"batch-{batch_id}-row-{row_idx}" for row_idx in range(6)],
                    data_type=string_gen.data_type))
            ]
            (gen_df(
                spark,
                batch_gens,
                length=6,
                num_slices=1)
                .select(*source_columns)
                .write
                .mode('append')
                .insertInto(source_table))

        spark.sql(
            f"CALL spark_catalog.system.add_files("
            f"table => '{target_table}', "
            f"source_table => '{source_table}')")
        spark.sql(
            f"ALTER TABLE {target_table} SET TBLPROPERTIES ("
            "'read.split.target-size' = '268435456', "
            "'read.split.planning-lookback' = '100')")
        spark.sql(f"REFRESH TABLE {target_table}")

    with_cpu_session(setup_imported_table)

    # Imported partitioned Parquet files materialize `a` from the path, not the file payload.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT a, b FROM {target_table}"),
        conf={**iceberg_read_enabled_conf, 'spark.rapids.sql.format.parquet.reader.type': reader_type})
