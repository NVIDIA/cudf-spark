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

"""Iceberg V3 row-lineage coverage for SPARK-50820 GPU metadata write/reinsert (#15348).

Row lineage requires Iceberg format-version 3 tables. Spark 4.0+ routes copy-on-write
UPDATE through ``write(metadata, row)`` so ``_row_id`` is preserved and
``_last_updated_sequence_number`` is conditionally nullified. This test compares CPU
vs GPU user columns plus lineage metadata after CoW UPDATE.

GPU Iceberg scans do not yet inherit V3 ``_row_id`` / ``_last_updated_sequence_number``,
so reads are forced onto CPU while writes stay on GPU. That isolates the SPARK-50820
write-path fix under test.
"""

import pytest

from asserts import assert_equal_with_local_sort
from conftest import is_iceberg_remote_catalog, spark_jvm
from data_gen import copy_and_update
from iceberg import (create_iceberg_table, get_full_table_name, iceberg_write_enabled_conf,
                     iceberg_unsupported_mark)
from marks import allow_non_gpu, iceberg, ignore_order
from spark_session import is_spark_400_or_later, with_cpu_session, with_gpu_session

pytestmark = [
    iceberg_unsupported_mark,
    pytest.mark.skipif(not is_spark_400_or_later(),
                       reason="SPARK-50820 metadata write/reinsert paths require Spark 4.0+"),
    pytest.mark.skipif(is_iceberg_remote_catalog(),
                       reason="Skip for remote catalog to reduce test time"),
]

# Keep Iceberg GPU writes enabled, but force CPU Iceberg reads so inherited
# row-lineage metadata columns are materialized correctly for the write tasks.
iceberg_row_lineage_conf = copy_and_update(iceberg_write_enabled_conf, {
    "spark.rapids.sql.format.iceberg.read.enabled": "false",
})

# CPU scan / intermediate ops allowed while ReplaceDataExec stays on GPU.
_ROW_LINEAGE_ALLOW_NON_GPU = (
    "BatchScanExec",
    "ColumnarToRowExec",
    "ShuffleExchangeExec",
    "SortExec",
    "ProjectExec",
    "FilterExec",
    "ExpandExec",
    "EmptyRelationExec",
)

ROW_LINEAGE_COLS = "_row_id, _last_updated_sequence_number"


def _iceberg_has_row_lineage_metadata_columns():
    """True when the Iceberg runtime exposes row-lineage metadata columns (1.9+)."""
    meta_class = spark_jvm().java.lang.Class.forName(
        "org.apache.iceberg.MetadataColumns")
    field_names = {field.getName() for field in meta_class.getFields()}
    return {"ROW_ID", "LAST_UPDATED_SEQUENCE_NUMBER"}.issubset(field_names)


def _create_v3_table(table_name, table_properties, partition_col_sql=None, df_gen=None):
    """Create a format-version 3 Iceberg table, skipping when V3 is unsupported."""
    if not _iceberg_has_row_lineage_metadata_columns():
        pytest.skip("Iceberg runtime does not expose row-lineage metadata columns")

    props = {
        'format-version': '3',
        'write.update.mode': 'copy-on-write',
        # Iceberg V3 row lineage reads can require non-vectorized Parquet in some versions.
        'read.parquet.vectorization.enabled': 'false',
    }
    props.update(table_properties or {})

    try:
        create_iceberg_table(table_name, partition_col_sql=partition_col_sql,
                             table_prop=props, df_gen=df_gen)
    except Exception as e:
        msg = str(e).lower()
        if 'format version' in msg or 'format-version' in msg:
            pytest.skip(f"Iceberg runtime does not support format-version 3: {e}")
        raise


def _insert_rows(table_name, rows):
    def insert(spark):
        spark.createDataFrame(rows, "id INT, val STRING").writeTo(table_name).append()
    with_cpu_session(insert)


def _collect_with_lineage(table_name):
    def collect(spark):
        return spark.sql(
            f"SELECT id, val, {ROW_LINEAGE_COLS} FROM {table_name}"
        ).collect()
    return with_cpu_session(collect)


def _empty_df_gen(spark):
    return spark.createDataFrame([], "id INT, val STRING")


def do_row_lineage_update_test(spark_tmp_table_factory, partition_col_sql):
    base = get_full_table_name(spark_tmp_table_factory)
    cpu_table = f"{base}_cpu"
    gpu_table = f"{base}_gpu"

    _create_v3_table(cpu_table, {}, partition_col_sql, df_gen=_empty_df_gen)
    _create_v3_table(gpu_table, {}, partition_col_sql, df_gen=_empty_df_gen)

    rows = [(i, f"v{i}") for i in range(1, 11)]
    _insert_rows(cpu_table, rows)
    _insert_rows(gpu_table, rows)

    update_sql = "UPDATE {table} SET val = concat(val, '_u') WHERE id % 2 = 0"

    def do_gpu_update(spark):
        spark.sql(update_sql.format(table=gpu_table))

    def do_cpu_update(spark):
        spark.sql(update_sql.format(table=cpu_table))

    with_gpu_session(do_gpu_update, conf=iceberg_row_lineage_conf)
    with_cpu_session(do_cpu_update)

    assert_equal_with_local_sort(
        _collect_with_lineage(cpu_table),
        _collect_with_lineage(gpu_table))


@allow_non_gpu(*_ROW_LINEAGE_ALLOW_NON_GPU)
@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize("partition_col_sql", [None, "id"])
def test_iceberg_row_lineage_update_cow(spark_tmp_table_factory, partition_col_sql):
    """CoW UPDATE on V3 tables must preserve/nullify row-lineage metadata like CPU."""
    do_row_lineage_update_test(spark_tmp_table_factory, partition_col_sql)
