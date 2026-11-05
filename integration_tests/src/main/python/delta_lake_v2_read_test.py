# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from pyspark.sql import DataFrame

from asserts import (assert_cpu_and_gpu_are_equal_collect_with_capture,
                     assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error,
                     assert_gpu_fallback_collect)
from delta_lake_utils import is_oss_delta_lake_42_or_43, is_oss_delta_lake_43
from marks import allow_non_gpu, delta_lake, ignore_order
from spark_session import with_cpu_session

pytestmark = [delta_lake, pytest.mark.skipif(
    not is_oss_delta_lake_42_or_43(), reason="Delta V2 requires OSS Delta 4.2 or 4.3")]

_conf = {
    'spark.rapids.sql.explain': 'ALL',
    'spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled': 'true',
    'spark.sql.parquet.fieldId.read.enabled': 'true',
    'spark.sql.parquet.fieldId.write.enabled': 'true',
    'spark.databricks.delta.deletionVectors.useMetadataRowIndex': 'true',
    'spark.sql.adaptive.enabled': 'false',
}


def _read_v2(spark, path, options=None):
    # Construct the actual Delta V2 table, rather than letting the V1 DeltaDataSource
    # route a path read back to FileSourceScanExec.
    jvm = spark._jvm
    identifier = jvm.org.apache.spark.sql.connector.catalog.Identifier.of(
        spark.sparkContext._gateway.new_array(jvm.java.lang.String, 0), 'delta_v2')
    table_class = (jvm.io.delta.spark.internal.v2.catalog.DeltaV2Table
                   if is_oss_delta_lake_43() else jvm.io.delta.spark.internal.v2.catalog.SparkTable)
    table = table_class(identifier, path, options or {})
    none = jvm.scala.Option.empty()
    relation = jvm.org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation.create(
        table, none, none)
    jdf = jvm.org.apache.spark.sql.classic.Dataset.ofRows(spark._jsparkSession, relation)
    return DataFrame(jdf, spark)


def _write_table(spark, path, partitioned=False, mapping='none', dvs=False):
    df = spark.range(256).selectExpr(
        'id', 'cast(id % 4 as int) as part', 'concat("value-", id) as value',
        'named_struct("x", id, "y", cast(id % 7 as int)) as nested')
    writer = df.repartition(8).write.format('delta').option(
        'delta.columnMapping.mode', mapping).option('delta.enableDeletionVectors', str(dvs).lower())
    if partitioned:
        writer = writer.partitionBy('part')
    writer.save(path)
    if dvs:
        spark.sql("DELETE FROM delta.`{}` WHERE id % 5 = 0".format(path))


def _require_gpu_batch(spark, df):
    if spark.conf.get('spark.rapids.sql.enabled') == 'true':
        plan = df._jdf.queryExecution().executedPlan()
        callback = spark._jvm.org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
        assert callback.contains(plan, 'GpuBatchScanExec'), str(plan)
    return df


@ignore_order
@pytest.mark.parametrize('partitioned', [False, True])
@pytest.mark.parametrize('mapping', ['none', 'name', 'id'])
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_pruning_and_filters(spark_tmp_path, partitioned, mapping, reader):
    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path, partitioned, mapping),
                     conf=_conf)
    conf = dict(_conf, **{'spark.rapids.sql.format.parquet.reader.type': reader})
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _require_gpu_batch(spark,
        _read_v2(spark, spark_tmp_path).where('part = 2 AND id > 50').selectExpr('nested.x', 'value')),
        conf=conf)


@ignore_order
@pytest.mark.parametrize('partitioned', [False, True])
@pytest.mark.parametrize('mapping', ['none', 'name', 'id'])
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_deletion_vectors(spark_tmp_path, partitioned, mapping, reader):
    with_cpu_session(lambda spark: _write_table(
        spark, spark_tmp_path, partitioned, mapping, dvs=True), conf=_conf)
    conf = dict(_conf, **{'spark.rapids.sql.format.parquet.reader.type': reader})
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _require_gpu_batch(spark,
        _read_v2(spark, spark_tmp_path).where('id > 20').select('id', 'part', 'nested')), conf=conf)


@ignore_order
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_empty_projection(spark_tmp_path, reader):
    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path, dvs=True), conf=_conf)
    conf = dict(_conf, **{'spark.rapids.sql.format.parquet.reader.type': reader})
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _read_v2(spark, spark_tmp_path).select(),
                                        conf=conf)


@ignore_order
def test_delta_v2_snapshot_retained_after_append(spark_tmp_path):
    def read_snapshot(spark):
        path = spark_tmp_path + '/' + spark.conf.get('spark.rapids.sql.enabled')
        _write_table(spark, path)
        df = _read_v2(spark, path)
        # Force analysis/scan construction before a concurrent commit, but do not execute it.
        df._jdf.queryExecution().optimizedPlan()
        spark.range(256, 260).selectExpr(
            'id', 'cast(0 as int) as part', 'concat("value-", id) as value',
            'named_struct("x", id, "y", cast(0 as int)) as nested').write.format(
                'delta').mode('append').save(path)
        assert df.count() == 256
        return _require_gpu_batch(spark, df)

    assert_gpu_and_cpu_are_equal_collect(read_snapshot, conf=_conf)


@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec')
@pytest.mark.parametrize('setting', [
    'spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled',
    'spark.databricks.delta.deletionVectors.useMetadataRowIndex'])
def test_delta_v2_deletion_vector_fallback(spark_tmp_path, setting):
    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path, dvs=True), conf=_conf)
    conf = dict(_conf, **{setting: 'false'})
    assert_gpu_fallback_collect(lambda spark: _read_v2(spark, spark_tmp_path), 'BatchScanExec',
                                conf=conf)


@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_file_order(spark_tmp_path, reader):
    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path), conf=_conf)
    conf = dict(_conf, **{'spark.rapids.sql.format.parquet.reader.type': reader})
    # Deliberately compare without ignore_order: Delta visits files in their planned order.
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _require_gpu_batch(spark,
        _read_v2(spark, spark_tmp_path).select('id')), conf=conf)


def _find_batch_scan(plan):
    if plan.getClass().getSimpleName() == 'GpuBatchScanExec':
        return plan
    children = plan.children().iterator()
    while children.hasNext():
        found = _find_batch_scan(children.next())
        if found is not None:
            return found
    return None


@pytest.mark.parametrize('partition_value', [2, 99])
def test_delta_v2_runtime_filter_preserves_partitions(spark_tmp_path, partition_value):
    from spark_session import with_gpu_session

    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path, partitioned=True), conf=_conf)

    def check(spark):
        jvm = spark._jvm
        df = _read_v2(spark, spark_tmp_path)
        plan = df._jdf.queryExecution().executedPlan()
        batch = _find_batch_scan(plan)
        assert batch is not None, str(plan)
        gpu_scan = batch.scan()
        cpu_scan = gpu_scan.cpu()
        first_copy = gpu_scan.withInputFile()
        second_copy = gpu_scan.withInputFile()
        assert first_copy.equals(second_copy)
        assert first_copy.toBatch().equals(second_copy.toBatch())
        assert first_copy.toBatch().hashCode() == second_copy.toBatch().hashCode()
        original = cpu_scan.toBatch().planInputPartitions()
        gpu = gpu_scan.toBatch().planInputPartitions()
        assert len(original) == len(gpu)
        identity = jvm.java.lang.System.identityHashCode
        for cpu_part, gpu_part in zip(original, gpu):
            if is_oss_delta_lake_43():
                cpu_files, gpu_files = cpu_part.files(), gpu_part.files()
                assert all(c.equals(g) for c, g in zip(cpu_files, gpu_files))
            else:
                assert cpu_part.partitionKey().equals(gpu_part.partitionKey())
                cpu_files = cpu_part.getFilePartition().files()
                gpu_files = gpu_part.getFilePartition().files()
                assert [identity(f) for f in cpu_files] == [identity(f) for f in gpu_files]
            assert len(cpu_files) == len(gpu_files)
        stats_before = gpu_scan.estimateStatistics().sizeInBytes().getAsLong()
        expressions = spark.sparkContext._gateway.new_array(
            jvm.org.apache.spark.sql.connector.expressions.Expression, 2)
        expressions[0] = jvm.org.apache.spark.sql.connector.expressions.Expressions.column('part')
        expressions[1] = jvm.org.apache.spark.sql.connector.expressions.Expressions.literal(partition_value)
        predicates = spark.sparkContext._gateway.new_array(
            jvm.org.apache.spark.sql.connector.expressions.filter.Predicate, 1)
        predicates[0] = jvm.org.apache.spark.sql.connector.expressions.filter.Predicate(
            '=', expressions)
        gpu_scan.filter(predicates)
        remaining = gpu_scan.toBatch().planInputPartitions()
        assert bool(len(remaining)) == (partition_value == 2)
        if is_oss_delta_lake_43():
            # 4.3 repacks files after filtering; the number of task partitions can stay the same.
            assert sum(len(p.files()) for p in remaining) < sum(len(p.files()) for p in original)
            assert all(f.partitionValues().getInt(0) == partition_value for p in remaining for f in p.files())
        else:
            assert all(p.partitionKey().getInt(0) == partition_value for p in remaining)
        assert gpu_scan.estimateStatistics().sizeInBytes().getAsLong() < stats_before
        assert cpu_scan.estimateStatistics().sizeInBytes().getAsLong() == \
            gpu_scan.estimateStatistics().sizeInBytes().getAsLong()

    with_gpu_session(check, conf=_conf)


@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec', 'ProjectExec')
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_input_file_metadata(spark_tmp_path, reader):
    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path), conf=_conf)
    conf = dict(_conf, **{'spark.rapids.sql.format.parquet.reader.type': reader})
    assert_gpu_fallback_collect(lambda spark: _read_v2(spark, spark_tmp_path).selectExpr(
        'id', 'input_file_name() as file', 'input_file_block_start() as start',
        'input_file_block_length() as length'), 'BatchScanExec', conf=conf)


@ignore_order
def test_delta_v2_version_as_of(spark_tmp_path):
    def setup(spark):
        _write_table(spark, spark_tmp_path, dvs=True)
    with_cpu_session(setup, conf=_conf)
    # Version zero predates the delete; snapshot selection belongs to Delta.
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _require_gpu_batch(spark,
        _read_v2(spark, spark_tmp_path, {'versionAsOf': '0'})), conf=_conf)


@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec')
@pytest.mark.parametrize('option', ['datetimeRebaseMode', 'int96RebaseMode'])
def test_delta_v2_parquet_option_fallback(spark_tmp_path, option):
    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path), conf=_conf)
    assert_gpu_fallback_collect(lambda spark: _read_v2(
        spark, spark_tmp_path, {option: 'LEGACY'}), 'BatchScanExec', conf=_conf)


@ignore_order
def test_delta_v2_row_tracking_table(spark_tmp_path):
    def setup(spark):
        _write_table(spark, spark_tmp_path, mapping='name')
        spark.sql("ALTER TABLE delta.`{}` SET TBLPROPERTIES "
                  "('delta.enableRowTracking' = 'true')".format(spark_tmp_path))
    with_cpu_session(setup, conf=_conf)
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _require_gpu_batch(spark,
        _read_v2(spark, spark_tmp_path).select('id', 'nested')), conf=_conf)


@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec')
def test_delta_v2_unsupported_reader_feature_fallback(spark_tmp_path):
    def setup(spark):
        _write_table(spark, spark_tmp_path)
        spark.sql("ALTER TABLE delta.`{}` SET TBLPROPERTIES "
                  "('delta.enableTypeWidening' = 'true')".format(spark_tmp_path))
    with_cpu_session(setup, conf=_conf)
    assert_gpu_fallback_collect(lambda spark: _read_v2(spark, spark_tmp_path),
                                'BatchScanExec', conf=_conf)


@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec', 'ProjectExec')
@pytest.mark.skipif(not is_oss_delta_lake_43(), reason='V2 row metadata is exposed in Delta 4.3')
def test_delta_v2_row_tracking_metadata_fallback(spark_tmp_path):
    def setup(spark):
        _write_table(spark, spark_tmp_path)
        spark.sql("ALTER TABLE delta.`{}` SET TBLPROPERTIES "
                  "('delta.enableRowTracking' = 'true')".format(spark_tmp_path))
    with_cpu_session(setup, conf=_conf)
    assert_gpu_fallback_collect(lambda spark: _read_v2(spark, spark_tmp_path).selectExpr(
        'id', '_metadata.row_id', '_metadata.row_commit_version'), 'BatchScanExec', conf=_conf)


@ignore_order
@pytest.mark.parametrize('aqe', ['false', 'true'])
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_sql_aggregates(spark_tmp_path, aqe, reader):
    def setup(spark):
        spark.range(256).selectExpr(
            'id', 'CASE WHEN id % 5 = 0 THEN NULL ELSE cast(id % 4 as int) END as part',
            'CASE WHEN id % 7 = 0 THEN NULL ELSE cast(id % 101 as decimal(12,2)) END as amount',
            "date_add(DATE '2020-01-01', cast(id % 20 as int)) as day").repartition(8).write.format(
                'delta').option('delta.columnMapping.mode', 'name').partitionBy('part').save(
                    spark_tmp_path)
    with_cpu_session(setup, conf=_conf)
    conf = dict(_conf, **{
        'spark.databricks.delta.v2.enableMode': 'STRICT',
        'spark.sql.adaptive.enabled': aqe,
        'spark.rapids.sql.format.parquet.reader.type': reader})
    # Exercise the public SQL entry point and verify the final plan, including with AQE.
    assert_cpu_and_gpu_are_equal_collect_with_capture(lambda spark: spark.sql(
        "SELECT part, count(*) as n, count(amount) as nonnull, sum(amount) as total, "
        "min(id) as first_id, max(id) as last_id FROM delta.`{}` "
        "WHERE day >= DATE '2020-01-10' AND (amount > 10 OR amount IS NULL) "
        "GROUP BY part".format(spark_tmp_path)), exist_classes='GpuBatchScanExec', conf=conf,
        require_non_empty=True)


@ignore_order
@pytest.mark.parametrize('mapping', ['name', 'id'])
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_schema_evolution(spark_tmp_path, mapping, reader):
    def setup(spark):
        _write_table(spark, spark_tmp_path, partitioned=True, mapping=mapping)
        spark.sql("ALTER TABLE delta.`{}` RENAME COLUMN value TO renamed".format(spark_tmp_path))
        spark.sql("ALTER TABLE delta.`{}` RENAME COLUMN nested.x TO renamed_x".format(
            spark_tmp_path))
        spark.sql("ALTER TABLE delta.`{}` DROP COLUMN nested.y".format(spark_tmp_path))
        spark.sql("ALTER TABLE delta.`{}` ADD COLUMNS (added STRING)".format(spark_tmp_path))
        spark.range(256, 260).selectExpr(
            'id', 'cast(id % 4 as int) as part', 'concat("value-", id) as renamed',
            'named_struct("renamed_x", id) as nested', "'new' as added").write.format(
                'delta').mode('append').save(spark_tmp_path)
    with_cpu_session(setup, conf=_conf)
    conf = dict(_conf, **{'spark.rapids.sql.format.parquet.reader.type': reader})
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _require_gpu_batch(spark,
        _read_v2(spark, spark_tmp_path).select('id', 'part', 'renamed', 'nested', 'added')), conf=conf)


@ignore_order
@pytest.mark.parametrize('empty_table', [False, True])
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_empty_scan(spark_tmp_path, empty_table, reader):
    def setup(spark):
        if empty_table:
            spark.sql("CREATE TABLE delta.`{}` (id BIGINT, part INT) USING DELTA "
                      "PARTITIONED BY (part)".format(spark_tmp_path))
        else:
            _write_table(spark, spark_tmp_path, partitioned=True)
    with_cpu_session(setup, conf=_conf)
    conf = dict(_conf, **{'spark.rapids.sql.format.parquet.reader.type': reader})
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _require_gpu_batch(spark,
        _read_v2(spark, spark_tmp_path).where('part = 99').select('id')), conf=conf)


@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec')
@pytest.mark.parametrize('option', ['ignoreMissingFiles', 'ignoreCorruptFiles'])
@pytest.mark.parametrize('scope,value', [('session', 'true'), ('read', 'true'), ('read', 'false')])
def test_delta_v2_file_error_option_fallback(spark_tmp_path, option, scope, value):
    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path), conf=_conf)
    conf = dict(_conf)
    options = {}
    if scope == 'session':
        conf['spark.sql.files.' + option] = value
    else:
        options[option] = value
    assert_gpu_fallback_collect(lambda spark: _read_v2(spark, spark_tmp_path, options),
                                'BatchScanExec', conf=conf)


@allow_non_gpu('BatchScanExec', 'ColumnarToRowExec')
@pytest.mark.parametrize('ignore_missing', ['false', 'true'])
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_missing_file_error(spark_tmp_path, ignore_missing, reader):
    def setup(spark):
        _write_table(spark, spark_tmp_path)
        files = spark.read.format('delta').load(spark_tmp_path).inputFiles()
        path = spark._jvm.org.apache.hadoop.fs.Path(files[0])
        assert path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).delete(path, False)
    with_cpu_session(setup, conf=_conf)
    # Delta's V2 reader raises an error even when Spark's file-error suppression is enabled.
    conf = dict(_conf, **{
        'spark.sql.files.ignoreMissingFiles': ignore_missing,
        'spark.rapids.sql.format.parquet.reader.type': reader})
    assert_gpu_and_cpu_error(lambda spark: _read_v2(spark, spark_tmp_path).collect(), conf,
                             'FileNotFoundException')


@ignore_order
@pytest.mark.parametrize('selected_id', [2, 999])
def test_delta_v2_dynamic_partition_pruning_join(spark_tmp_path, selected_id):
    with_cpu_session(lambda spark: _write_table(spark, spark_tmp_path, partitioned=True), conf=_conf)
    conf = dict(_conf, **{
        'spark.databricks.delta.v2.enableMode': 'STRICT',
        'spark.sql.optimizer.dynamicPartitionPruning.enabled': 'true',
        'spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly': 'false',
        'spark.sql.optimizer.dynamicPartitionPruning.useStats': 'false',
        'spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio': '1.0'})

    def check_plans(cpu_plan, gpu_plan):
        # Require Spark to inject and execute the runtime filter, rather than manually
        # mutating a scan that Spark is allowed to reset when it binds its runtime filters.
        assert 'dynamicpruning' in cpu_plan.toString().lower(), str(cpu_plan)
        assert 'dynamicpruning' in gpu_plan.toString().lower(), str(gpu_plan)

    assert_cpu_and_gpu_are_equal_collect_with_capture(lambda spark: spark.sql(
        "SELECT f.id, f.part FROM delta.`{0}` f JOIN "
        "(SELECT DISTINCT part FROM delta.`{0}` WHERE id = {1}) d "
        "ON f.part = d.part".format(spark_tmp_path, selected_id)),
        exist_classes='GpuBatchScanExec', conf=conf, gpu_plan_assertion=check_plans)


@ignore_order
@pytest.mark.parametrize('reader', ['PERFILE', 'MULTITHREADED', 'COALESCING'])
def test_delta_v2_partition_only_deletion_vectors(spark_tmp_path, reader):
    with_cpu_session(lambda spark: _write_table(
        spark, spark_tmp_path, partitioned=True, mapping='name', dvs=True), conf=_conf)
    conf = dict(_conf, **{'spark.rapids.sql.format.parquet.reader.type': reader})
    # No Parquet data columns are requested, but deleted rows must still be removed
    # before producing the partition column and its repeated values.
    assert_gpu_and_cpu_are_equal_collect(lambda spark: _require_gpu_batch(spark,
        _read_v2(spark, spark_tmp_path).select('part')), conf=conf)
