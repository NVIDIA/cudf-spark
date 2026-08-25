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

import pytest
import uuid

from asserts import assert_gpu_and_cpu_are_equal_collect
from conftest import is_iceberg_rest_catalog
from data_gen import *
from iceberg import get_full_table_name, rapids_reader_types, create_iceberg_table, iceberg_base_table_cols, iceberg_gens_list, iceberg_unsupported_mark
from marks import iceberg, ignore_order
from spark_session import with_cpu_session

pytestmark = [
    iceberg_unsupported_mark,

    # Iceberg view tests only supported with REST catalog,
    # because the view sql is not supported with Hadoop/S3Table catalog for Iceberg 1.9.x
    pytest.mark.skipif(not is_iceberg_rest_catalog(),
                       reason="Iceberg view tests only supported with REST catalog")
]

@iceberg
@ignore_order(local=True)
@pytest.mark.parametrize('reader_type', rapids_reader_types)
@pytest.mark.parametrize('view_sql', [
    pytest.param("SELECT * FROM {table_name}", id="select_all"),
    pytest.param("SELECT * FROM {table_name} WHERE _c2 > 0", id="filter_condition"),
    pytest.param("SELECT _c0, _c2, _c6 FROM {table_name}", id="projection"),
    pytest.param("SELECT _c7, COUNT(*) as cnt, SUM(_c2) as sum_c2 FROM {table_name} GROUP BY _c7", id="aggregation"),
])
def test_iceberg_view(spark_tmp_table_factory, reader_type, view_sql):
    """Test reading from an Iceberg view."""

    table_name = get_full_table_name(spark_tmp_table_factory)
    view_uuid = str(uuid.uuid4()).replace('-', '_')
    view_name = "iceberg_view_" + view_uuid

    # Create an Iceberg table, and insert data into it
    create_iceberg_table(table_name)
    def insert_data(spark):
        df = gen_df(spark, list(zip(iceberg_base_table_cols, iceberg_gens_list)))
        df.writeTo(table_name).append()
    with_cpu_session(insert_data)

    # Create an Iceberg view on the table with a view sql
    def setup_iceberg_view(spark):
        spark.sql(f"CREATE VIEW {view_name} AS {view_sql.format(table_name=table_name)}")
    with_cpu_session(setup_iceberg_view)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(f"SELECT * FROM {view_name}"),
        conf={'spark.rapids.sql.format.parquet.reader.type': reader_type})
