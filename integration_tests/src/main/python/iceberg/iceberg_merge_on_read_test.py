# Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
import logging
import tempfile

import pyspark.sql.functions as f
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from conftest import is_iceberg_remote_catalog
from iceberg import rapids_reader_types, \
    setup_base_iceberg_table, _add_eq_deletes, _change_table, \
    representative_eq_column_combinations, eq_reader_canary_pairs, \
    iceberg_unsupported_mark, create_iceberg_table, \
    iceberg_base_table_cols, iceberg_gens_list, get_full_table_name, \
    iceberg_write_enabled_conf, supports_iceberg_v3, ICEBERG_V3_UNSUPPORTED_REASON
from data_gen import gen_df, get_datagen_seed, int_gen, long_gen, string_gen
from marks import iceberg, ignore_order, validate_execs_in_gpu_plan
from spark_session import with_gpu_session, with_cpu_session

pytestmark = iceberg_unsupported_mark


# Eq-delete pair coverage. All 14 eligible eq-delete columns of iceberg_table_gen
# (_c0..c3, _c6..c15; _c4 float and _c5 double are excluded by can_be_eq_delete_col)
# appear in at least one pair in representative_eq_column_combinations; the full
# C(14, 2) matrix is unnecessary because eq-delete correctness depends on
# per-column type handling, not on the cross product of every two columns. Runs
# against the default reader; reader-type compatibility is checked separately by
# test_iceberg_v2_eq_deletes_reader_types.
# In spark/iceberg integration, there is no builtin way to generate eq deletion files using
# sql, we used a low level api to add eq deletion files to iceberg table.
# This does not work with aws s3tables, which is a managed table service.
@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('eq_delete_cols',
                         representative_eq_column_combinations,
                         ids=lambda x: str(x))
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason = "S3tables catalog is managed")
def test_iceberg_v2_eq_deletes(spark_tmp_table_factory, spark_tmp_path,
                               eq_delete_cols, register_iceberg_add_eq_deletes_udf):
    table_name = setup_base_iceberg_table(spark_tmp_table_factory)

    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, list(eq_delete_cols), 120, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name))


# Reader-type canary for eq-deletes: a small set of pairs crossed with all three
# rapids_reader_types confirms reader-type selection composes with the eq-delete
# read path. Full pair coverage runs against the default reader above.
@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('eq_delete_cols',
                         eq_reader_canary_pairs,
                         ids=lambda x: str(x))
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason = "S3tables catalog is managed")
def test_iceberg_v2_eq_deletes_reader_types(spark_tmp_table_factory, spark_tmp_path, reader_type,
                                            eq_delete_cols, register_iceberg_add_eq_deletes_udf):
    table_name = setup_base_iceberg_table(spark_tmp_table_factory)

    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, list(eq_delete_cols), 120, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_v2_position_delete(spark_tmp_table_factory, reader_type):
    table_name = setup_base_iceberg_table(spark_tmp_table_factory)
    _change_table(table_name,
                  lambda spark: spark.sql(f"DELETE FROM {table_name} where _c1 < 0"),
                  "No position deletes generated")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
# This requires setting a write data path for data files, which is hard to confirm with aws
# s3tables.
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason = "S3tables catalog is managed")
def test_iceberg_v2_position_delete_with_url_encoded_path(spark_tmp_table_factory,
                                                          spark_tmp_path,
                                                          reader_type):
    # We use a fixed seed here to ensure that data deletion vector has been generated
    temp_dir = tempfile.mkdtemp(dir=spark_tmp_path)
    data_path = f'{temp_dir}/tb=%2F%23_v9kRtI%27/data'
    table_name = setup_base_iceberg_table(spark_tmp_table_factory,
                                          table_prop={'write.data.path': data_path})
    _change_table(table_name,
                  lambda spark: spark.sql(f"DELETE FROM {table_name} where _c1 < 0"),
                  "No position deletes generated")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})

@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason = "S3tables catalog is managed")
@pytest.mark.xfail(reason = "https://github.com/NVIDIA/spark-rapids/issues/12885")
# When using this datagen, local run is 784 rows
@pytest.mark.datagen_overrides(seed=1749483297, permanent=True,
                               reason="Debug https://github.com/NVIDIA/spark-rapids/issues/12885")
def test_iceberg_v2_mixed_deletes(spark_tmp_table_factory, spark_tmp_path, reader_type,
                                  register_iceberg_add_eq_deletes_udf):
    # We use a fixed seed here to ensure that data deletion vector has been generated
    table_name = setup_base_iceberg_table(spark_tmp_table_factory)
    # Position deletes
    _change_table(table_name,
                  lambda spark: spark.sql(f"DELETE FROM {table_name} where _c1 < 0"),
                  "No position deletes generated")

    # Equation deletes
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c0"], 170, table_name, spark_tmp_path),
                  "No equation deletes generated")


    # Equation deletes
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c2", "_c3", "_c6"], 140, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")

    # Equation deletes
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c1", "_c2"], 110, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")

    # Trigger a count operation to verify that it works
    gpu_count = with_gpu_session(lambda spark: spark.table(table_name).count(),
                     conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})
    cpu_count = with_cpu_session(lambda spark: spark.table(table_name).count(),
                                 conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})
    assert gpu_count == cpu_count, f"Result count diverges, cpu: {cpu_count}, gpu: {gpu_count}"
    logging.info(f"Count is {cpu_count}")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@validate_execs_in_gpu_plan('GpuBatchScanExec')
def test_iceberg_v3_deletion_vector(spark_tmp_table_factory, reader_type):
    table_name = setup_base_iceberg_table(
        spark_tmp_table_factory,
        table_prop={'format-version': '3'})

    def add_deletion_vector(spark):
        spark.sql(f"DELETE FROM {table_name} where _c1 < 0")
        spark.sql(f"REFRESH TABLE {table_name}")
        delete_files = {
            (row.content, row.file_format) for row in
            spark.sql(
                f"SELECT content, file_format FROM {table_name}.delete_files").collect()
        }
        expected_delete_files = {(1, 'PUFFIN')}
        assert delete_files == expected_delete_files, \
            f"Expected only deletion vectors {expected_delete_files}, found {delete_files}"

    with_cpu_session(add_deletion_vector)

    read_conf = {
        'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
    }

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf=read_conf,
        # Reset the GPU plan-validation config before fixture teardown.
        is_cpu_first=False)


def _assert_v3_deletion_vectors(spark, table_name, expected_positions):
    delete_files = spark.sql(f"""
        SELECT content, file_format, record_count, referenced_data_file,
               content_offset, content_size_in_bytes
        FROM {table_name}.delete_files
        WHERE content = 1
    """).collect()
    assert delete_files, "Expected at least one positional delete file"
    assert all(row.file_format == 'PUFFIN' for row in delete_files), \
        f"Expected only Puffin deletion vectors, found {delete_files}"
    referenced_files = [row.referenced_data_file for row in delete_files]
    assert all(path is not None for path in referenced_files), \
        f"Expected every deletion vector to reference a data file, found {delete_files}"
    assert len(referenced_files) == len(set(referenced_files)), \
        f"Expected at most one deletion vector per data file, found {delete_files}"
    assert all(row.content_offset is not None and row.content_offset >= 0 and
               row.content_size_in_bytes is not None and row.content_size_in_bytes > 0
               for row in delete_files), \
        f"Expected valid Puffin ranges, found {delete_files}"
    actual_positions = sum(row.record_count for row in delete_files)
    assert actual_positions == expected_positions, \
        f"Expected {expected_positions} deleted positions, found {actual_positions}"


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.parametrize('fanout_enabled', [False, True], ids=['clustered', 'fanout'])
def test_iceberg_v3_gpu_write_and_merge_deletion_vectors(
        spark_tmp_table_factory, fanout_enabled):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        spark.sql(f"""
            CREATE TABLE {table_name} (id BIGINT) USING ICEBERG
            PARTITIONED BY (bucket(2, id))
            TBLPROPERTIES (
              'format-version' = '3',
              'write.delete.mode' = 'merge-on-read',
              'write.spark.fanout.enabled' = '{str(fanout_enabled).lower()}')
        """)
        spark.sql(f"INSERT INTO {table_name} SELECT id FROM range(128)")

    with_cpu_session(setup_table)
    write_conf = dict(iceberg_write_enabled_conf)
    write_conf['spark.rapids.sql.format.iceberg.v3.enabled'] = 'true'

    # The second delete must merge with the first DV rather than add another DV for a data file.
    with_gpu_session(
        lambda spark: spark.sql(f"DELETE FROM {table_name} WHERE id % 3 = 0").collect(),
        conf=write_conf)
    with_gpu_session(
        lambda spark: spark.sql(f"DELETE FROM {table_name} WHERE id % 5 = 0").collect(),
        conf=write_conf)

    with_cpu_session(lambda spark: _assert_v3_deletion_vectors(spark, table_name, 60))
    actual = with_cpu_session(
        lambda spark: [row.id for row in spark.table(table_name).collect()])
    expected = [value for value in range(128) if value % 3 != 0 and value % 5 != 0]
    assert sorted(actual) == expected


@iceberg
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
def test_iceberg_v3_gpu_write_upgrades_position_deletes(spark_tmp_table_factory):
    table_name = get_full_table_name(spark_tmp_table_factory)

    def setup_table(spark):
        spark.sql(f"""
            CREATE TABLE {table_name} (id BIGINT) USING ICEBERG
            TBLPROPERTIES (
              'format-version' = '2',
              'write.delete.mode' = 'merge-on-read')
        """)
        spark.sql(f"INSERT INTO {table_name} SELECT id FROM range(64)")
        spark.sql(f"DELETE FROM {table_name} WHERE id % 4 = 0")
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version' = '3')")

    with_cpu_session(setup_table)
    write_conf = dict(iceberg_write_enabled_conf)
    write_conf['spark.rapids.sql.format.iceberg.v3.enabled'] = 'true'
    with_gpu_session(
        lambda spark: spark.sql(f"DELETE FROM {table_name} WHERE id % 5 = 0").collect(),
        conf=write_conf)

    with_cpu_session(lambda spark: _assert_v3_deletion_vectors(spark, table_name, 25))
    actual = with_cpu_session(
        lambda spark: [row.id for row in spark.table(table_name).collect()])
    expected = [value for value in range(64) if value % 4 != 0 and value % 5 != 0]
    assert sorted(actual) == expected


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason = "S3tables catalog is managed")
@pytest.mark.skipif(not supports_iceberg_v3, reason=ICEBERG_V3_UNSUPPORTED_REASON)
@pytest.mark.xfail(reason = "https://github.com/NVIDIA/spark-rapids/issues/12885")
# When using this datagen, local run is 784 rows
@pytest.mark.datagen_overrides(seed=1749483297, permanent=True,
                               reason="Debug https://github.com/NVIDIA/spark-rapids/issues/12885")
@validate_execs_in_gpu_plan('GpuBatchScanExec')
def test_iceberg_v3_mixed_deletes(spark_tmp_table_factory, spark_tmp_path, reader_type,
                                  register_iceberg_add_eq_deletes_udf):
    table_name = setup_base_iceberg_table(
        spark_tmp_table_factory,
        table_prop={
            'format-version': '2',
            'write.delete.mode': 'merge-on-read',
        })
    _change_table(table_name,
                  lambda spark: spark.sql(f"DELETE FROM {table_name} where _c1 < 0"),
                  "No position deletes generated")
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c0"], 170, table_name, spark_tmp_path),
                  "No equation deletes generated")
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c2", "_c3", "_c6"], 140, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")
    _change_table(table_name,
                  lambda spark: _add_eq_deletes(spark, ["_c1", "_c2"], 110, table_name,
                                                spark_tmp_path),
                  "No equation deletes generated")

    # Upgrade after creating v2 position deletes, then create a v3 deletion vector. This leaves
    # equality deletes, legacy position deletes, and deletion vectors in the same table.
    def add_deletion_vector(spark):
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version' = '3')")
        spark.sql(f"DELETE FROM {table_name} where _c1 >= 0 and _c2 % 7 = 0")
        spark.sql(f"REFRESH TABLE {table_name}")
        delete_files = {
            (row.content, row.file_format) for row in
            spark.sql(
                f"SELECT content, file_format FROM {table_name}.delete_files").collect()
        }
        expected_delete_files = {
            (1, 'PARQUET'),  # Legacy position delete
            (2, 'PARQUET'),  # Equality delete
            (1, 'PUFFIN'),   # Deletion vector
        }
        assert expected_delete_files.issubset(delete_files), \
            f"Expected mixed delete files {expected_delete_files}, found {delete_files}"

    with_cpu_session(add_deletion_vector)

    read_conf = {
        'spark.rapids.sql.format.iceberg.v3.enabled': 'true',
        'spark.rapids.sql.format.parquet.reader.type': reader_type,
    }

    gpu_count = with_gpu_session(lambda spark: spark.table(table_name).count(),
                                 conf=read_conf)
    cpu_count = with_cpu_session(lambda spark: spark.table(table_name).count(),
                                 conf=read_conf)
    assert gpu_count == cpu_count, f"Result count diverges, cpu: {cpu_count}, gpu: {gpu_count}"
    logging.info(f"Count is {cpu_count}")

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf=read_conf)


def _normalize_position_delete_df(df):
    return df.select(
        f.col('a'),
        (f.pmod(f.col('b'), f.lit(4)) - f.lit(2)).cast('int').alias('b'),
        f.col('c'))


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
def test_iceberg_small_file_combine_with_position_deletes(
        spark_tmp_table_factory, reader_type):
    table_name = get_full_table_name(spark_tmp_table_factory)
    delete_gens = [('a', long_gen), ('b', int_gen), ('c', string_gen)]
    base_seed = get_datagen_seed()
    create_iceberg_table(
        table_name,
        partition_col_sql='bucket(2, a)',
        table_prop={
            'format-version': '2',
            'write.delete.mode': 'merge-on-read',
        },
        df_gen=lambda spark: gen_df(spark, delete_gens))

    def setup_table(spark):
        for seed_offset in range(4):
            _normalize_position_delete_df(
                gen_df(
                    spark,
                    delete_gens,
                    length=64,
                    seed=base_seed + seed_offset,
                    num_slices=1)).writeTo(table_name).append()

        spark.sql(f'DELETE FROM {table_name} WHERE b < 0')
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
            "'read.split.target-size' = '268435456', "
            "'read.split.planning-lookback' = '100')")
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(setup_table)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f'SELECT a, b, c FROM {table_name}'),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})


@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.skipif(is_iceberg_remote_catalog(), reason = "S3tables catalog is managed")
def test_iceberg_small_file_combine_with_eq_deletes(
        spark_tmp_table_factory,
        spark_tmp_path,
        reader_type,
        register_iceberg_add_eq_deletes_udf):
    table_name = get_full_table_name(spark_tmp_table_factory)
    eq_delete_gens = list(zip(iceberg_base_table_cols, iceberg_gens_list))
    base_seed = get_datagen_seed()
    create_iceberg_table(
        table_name,
        partition_col_sql='bucket(2, _c2)',
        table_prop={
            'format-version': '2',
            'write.delete.mode': 'merge-on-read',
        },
        df_gen=lambda spark: gen_df(spark, eq_delete_gens))

    def setup_table(spark):
        for seed_offset in range(4):
            gen_df(
                spark,
                eq_delete_gens,
                length=64,
                seed=base_seed + seed_offset,
                num_slices=1).writeTo(table_name).append()

        _add_eq_deletes(
            spark,
            ['_c0', '_c2'],
            40,
            table_name,
            spark_tmp_path)

        for seed_offset in range(4):
            gen_df(
                spark,
                eq_delete_gens,
                length=64,
                seed=base_seed + 100 + seed_offset,
                num_slices=1).writeTo(table_name).append()

        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
            "'read.split.target-size' = '268435456', "
            "'read.split.planning-lookback' = '100')")
        spark.sql(f"REFRESH TABLE {table_name}")

    with_cpu_session(setup_table)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.table(table_name),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})
