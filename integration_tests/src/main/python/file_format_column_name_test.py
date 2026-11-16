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

import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import DecimalGen, IntegerGen, StructGen, gen_df
from marks import ignore_order
from spark_session import with_cpu_session


@ignore_order(local=True)
@pytest.mark.parametrize("use_v1", [False, True], ids=["v2", "v1"])
@pytest.mark.parametrize("flat_first", [False, True], ids=["struct-first", "flat-first"])
@pytest.mark.parametrize("nested_precision", [7, 15])
@pytest.mark.parametrize("nested", [False, True], ids=["root", "nested"])
def test_json_literal_dotted_field_collision(
        spark_tmp_path, use_v1, flat_first, nested_precision, nested):
    path = spark_tmp_path + "/json_collision"
    fields = [
        ("a", StructGen([("b", DecimalGen(precision=nested_precision, scale=2)),
                         ("z", IntegerGen(min_val=1, max_val=10))])),
        ("a.b", DecimalGen(precision=25, scale=2)),
        ("c", IntegerGen(min_val=100, max_val=200)),
    ]
    if flat_first:
        fields = [fields[1], fields[0], fields[2]]
    if nested:
        fields = [("s", StructGen(fields, nullable=False))]

    def write(spark):
        df = gen_df(spark, fields, length=32, num_slices=2)
        df.write.json(path)
        # Inference would read decimal JSON numbers as doubles.
        return df.schema

    schema = with_cpu_session(write)
    prefix = "s." if nested else ""
    projections = [
        ["*"],
        [prefix + "`a.b`"],
        [prefix + "a.b AS nested_b", prefix + "`a.b` AS flat_b"],
        [prefix + "c", prefix + "`a.b`", prefix + "a"],
        [prefix + "a", prefix + "`a.b`", prefix + "c"],
    ]
    conf = {
        "spark.sql.sources.useV1SourceList": "json" if use_v1 else "",
        "spark.rapids.sql.json.read.decimal.enabled": "true",
    }
    for projection in projections:
        print("Checking projection:", projection)
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.schema(schema).json(path).selectExpr(*projection),
            conf=conf)


@ignore_order(local=True)
@pytest.mark.parametrize("use_v1", [False, True], ids=["v2", "v1"])
@pytest.mark.parametrize("flat_first", [False, True], ids=["a-first", "a.b-first"])
@pytest.mark.parametrize("header", [False, True], ids=["no-header", "header"])
def test_csv_literal_dotted_field_projection(spark_tmp_path, use_v1, flat_first, header):
    path = spark_tmp_path + "/csv_dotted"
    # CSV has only scalar columns; compare literal a.b against the distinct root a.
    fields = [
        ("a", IntegerGen(min_val=1, max_val=10)),
        ("a.b", IntegerGen(min_val=1000, max_val=2000)),
        ("c", IntegerGen(min_val=100, max_val=200)),
    ]
    if flat_first:
        fields = [fields[1], fields[0], fields[2]]

    def write(spark):
        df = gen_df(spark, fields, length=32, num_slices=2)
        df.write.option("header", header).csv(path)
        return df.schema

    schema = with_cpu_session(write)
    conf = {"spark.sql.sources.useV1SourceList": "csv" if use_v1 else ""}
    for projection in [["*"], ["`a.b`"], ["c", "`a.b`"], ["`a.b`", "a", "c"],
                       ["a", "c"]]:
        print("Checking projection:", projection)
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.schema(schema).option("header", header).csv(path)
                .selectExpr(*projection), conf=conf)
