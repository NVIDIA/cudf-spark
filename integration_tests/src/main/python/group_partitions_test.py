# Copyright (c) 2026, NVIDIA CORPORATION.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from marks import allow_non_gpu, ignore_order
from spark_session import is_spark_35x, is_spark_40x, is_spark_41x, \
    is_spark_420_or_later, is_spark_500_or_later, spark_version


def _is_spark_patch_at_least(version, minimum):
    patch = version.split(".")[2].split("-", 1)[0]
    return int(patch) >= minimum


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


def _assert_partial_clustering_spj_plan(plan):
    nodes = _collect_plan_nodes(plan)

    def nodes_of_class(class_name):
        return [node for node in nodes if node.getClass().getSimpleName() == class_name]

    scans = nodes_of_class("BatchScanExec")
    joins = nodes_of_class("GpuShuffledSymmetricHashJoinExec")
    group_partitions = nodes_of_class("GpuGroupPartitionsExec")
    shuffle_exchanges = nodes_of_class("GpuShuffleExchangeExec")

    assert len(scans) == 2, f"Expected two CPU batch scans, found {len(scans)}:\n{plan}"
    assert len(joins) == 1, f"Expected one GPU SPJ join, found {len(joins)}:\n{plan}"

    join_nodes = _collect_plan_nodes(joins[0])
    join_exchanges = [
        node for node in join_nodes
        if node.getClass().getSimpleName() == "GpuShuffleExchangeExec"
    ]
    assert not join_exchanges, f"Expected shuffle-free SPJ inputs:\n{plan}"
    assert nodes_of_class("GpuRowToColumnarExec"), \
        f"Expected transitions above the CPU batch scans:\n{plan}"
    if is_spark_420_or_later():
        assert not shuffle_exchanges, \
            f"Expected keyed partitioning to remain shuffle-free through DISTINCT:\n{plan}"
        join_group_partitions = [
            node for node in join_nodes
            if node.getClass().getSimpleName() == "GpuGroupPartitionsExec"
        ]
        assert len(join_group_partitions) == 2, \
            f"Expected two GPU group-partitions join inputs:\n{plan}"
        assert len(group_partitions) == 3, \
            f"Expected a third GPU group-partitions node below distinct:\n{plan}"
    else:
        assert len(shuffle_exchanges) == 1, \
            f"Expected DISTINCT to shuffle partially clustered output once:\n{plan}"
        assert not group_partitions, \
            f"GroupPartitionsExec is not expected before Spark 4.2:\n{plan}"


def _assert_sorted_merge_spj_plan(plan):
    nodes = _collect_plan_nodes(plan)
    gpu_groups = [
        node for node in nodes
        if node.getClass().getSimpleName() == "GpuGroupPartitionsExec"
    ]
    sorted_groups = [node for node in gpu_groups if node.enableSortedMerge()]

    sorted_merge_flags = [group.enableSortedMerge() for group in gpu_groups]
    assert sorted_groups, \
        f"Expected GPU sorted-merge GroupPartitionsExec, found flags {sorted_merge_flags}:\n{plan}"
    assert not [
        node for node in nodes
        if node.getClass().getSimpleName() == "GroupPartitionsExec"
    ], f"Expected all GroupPartitionsExec nodes on GPU:\n{plan}"
    for group in sorted_groups:
        metrics = group.metrics()
        assert metrics.contains("sortTime"), \
            f"Expected a sort metric on sorted-merge GroupPartitionsExec:\n{plan}"
        assert metrics.apply("sortTime").value() > 0, \
            f"Expected the sorted-merge path to execute a GPU sort:\n{plan}"


@allow_non_gpu("BatchScanExec")
@ignore_order(local=True)
@pytest.mark.skipif(
    not (
        (is_spark_35x() and _is_spark_patch_at_least(spark_version(), 9))
        or (is_spark_40x() and _is_spark_patch_at_least(spark_version(), 3))
        or (is_spark_41x() and _is_spark_patch_at_least(spark_version(), 2))
        or is_spark_420_or_later()
    ),
    reason="Requires Spark's partial-clustering correctness fix")
@pytest.mark.xfail(
    condition=is_spark_500_or_later(),
    reason="https://github.com/NVIDIA/cudf-spark/issues/15688: Spark 5 requires "
           "KeyedPartitionings in a PartitioningCollection to share keys")
def test_group_partitions_partial_clustering_distinct():
    def distinct_after_spj(spark):
        # A JVM V2 source is required to report KeyGroupedPartitioning and per-partition keys.
        # Iceberg provides those properties too, but its GPU scan is not available on Spark 4.2.
        source = "com.nvidia.spark.rapids.tests.datasourcev2.GroupPartitionsDataSource"
        spark.read.format(source).option("side", "left").load() \
            .createOrReplaceTempView("group_partitions_left")
        spark.read.format(source).option("side", "right").load() \
            .createOrReplaceTempView("group_partitions_right")
        return spark.sql(
            """
            SELECT DISTINCT l.id
            FROM group_partitions_left l
            JOIN group_partitions_right r ON l.id = r.id
            """)

    conf = {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled": "true",
    }

    # The SPJ inputs are shuffle-free. SPARK-55848 requires DISTINCT to shuffle partially
    # clustered output before Spark 4.2. Spark 4.2 groups matching keys below the aggregate instead.
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        distinct_after_spj,
        conf=conf,
        require_non_empty=True,
        gpu_plan_assertion=_assert_partial_clustering_spj_plan)


@allow_non_gpu("BatchScanExec")
@ignore_order(local=True)
@pytest.mark.skipif(not is_spark_420_or_later(), reason="GroupPartitionsExec requires Spark 4.2")
def test_group_partitions_sorted_merge():
    def sorted_merge_spj(spark):
        source = \
            "com.nvidia.spark.rapids.tests.datasourcev2.GroupPartitionsSortedDataSource"
        for side in ("left", "right"):
            spark.read.format(source) \
                .option("side", side) \
                .option("direction", "asc") \
                .option("nulls", "first") \
                .load() \
                .createOrReplaceTempView("group_partitions_sorted_" + side)
        return spark.sql(
            """
            SELECT /*+ MERGE(l, r) */ l.id, l.value
            FROM group_partitions_sorted_left l
            JOIN group_partitions_sorted_right r
              ON l.id = r.id AND l.value = r.value
            """)

    conf = {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.requireAllClusterKeysForCoPartition": "false",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
        "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled": "true",
        "spark.sql.sources.v2.bucketing.preserveOrderingOnCoalesce.enabled": "true",
    }

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        sorted_merge_spj,
        conf=conf,
        require_non_empty=True,
        gpu_plan_assertion=_assert_sorted_merge_spj_plan)
