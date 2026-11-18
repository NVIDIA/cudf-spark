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
from data_gen import ArrayGen, DecimalGen, IntegerGen, MapGen, StringGen, StructGen, gen_df
from marks import ignore_order
from pyspark.sql.types import IntegerType, StructField, StructType
from spark_session import with_cpu_session

# Cover each decoder/footer and both source APIs without crossing every dimension.
_readers = [
    # format, reader, footer, V1, flat first, chunked
    ("parquet", "PERFILE", "JAVA", True, False, False),
    ("parquet", "PERFILE", "NATIVE", False, True, True),
    ("parquet", "MULTITHREADED", "JAVA", False, True, False),
    ("parquet", "MULTITHREADED", "NATIVE", False, False, True),
    ("parquet", "COALESCING", "JAVA", False, False, True),
    ("parquet", "COALESCING", "NATIVE", False, True, False),
    ("orc", "PERFILE", None, True, True, False),
    ("orc", "MULTITHREADED", None, False, False, True),
    ("orc", "COALESCING", None, False, True, False),
]

_collision_gens = [
    ("a", StructGen([
        ("b", IntegerGen(min_val=5, max_val=6, nullable=False)),
        ("z", IntegerGen(min_val=7, max_val=8, nullable=False))], nullable=False)),
    ("a.b", IntegerGen(min_val=1000, max_val=2000, nullable=False)),
    ("c", IntegerGen(min_val=100, max_val=200, nullable=False)),
]


@ignore_order(local=True)
@pytest.mark.parametrize("file_format,reader,footer,use_v1,flat_first,chunked", _readers)
def test_dotted_column_collides_with_struct_field(
        spark_tmp_path, file_format, reader, footer, flat_first, use_v1, chunked):
    path = spark_tmp_path + "/collision"
    gens = ([_collision_gens[1], _collision_gens[0], _collision_gens[2]]
            if flat_first else _collision_gens)
    # Two files exercise coalescing and multithreaded readers as well as per-file reads.
    with_cpu_session(lambda spark: gen_df(spark, gens, length=16, num_slices=2)
                     .write.format(file_format).save(path))
    conf = {
        "spark.sql.sources.useV1SourceList": file_format if use_v1 else "",
        "spark.rapids.sql.format." + file_format + ".reader.type": reader,
        "spark.rapids.sql.reader.chunked": str(chunked).lower(),
    }
    if file_format == "parquet":
        conf["spark.rapids.sql.format.parquet.reader.footer.type"] = footer
    projections = [
        ["c", "`a.b`"],
        ["`a.b`"],
        ["a.b AS nested_b", "`a.b` AS flat_b", "c"],
    ]
    for projection in projections:
        print("Checking projection:", projection)
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.format(file_format).load(path).selectExpr(*projection),
            conf=conf)


@ignore_order(local=True)
@pytest.mark.parametrize("file_format", ["parquet", "orc"])
@pytest.mark.parametrize("case_sensitive", [False, True])
def test_dotted_column_reordered_schema_with_missing_column(
        spark_tmp_path, file_format, case_sensitive):
    path = spark_tmp_path + "/reordered"
    with_cpu_session(lambda spark: gen_df(spark, _collision_gens, length=16, num_slices=1)
                     .write.format(file_format).save(path))
    # The physical dotted name controls cuDF selection, including in case-insensitive reads.
    schema = StructType([
        StructField("c" if case_sensitive else "C", IntegerType()),
        StructField("a.b" if case_sensitive else "A.B", IntegerType()),
        StructField("missing", IntegerType()),
    ])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).format(file_format).load(path),
        conf={"spark.sql.caseSensitive": str(case_sensitive).lower()})


@ignore_order(local=True)
@pytest.mark.parametrize("footer", ["JAVA", "NATIVE"])
def test_parquet_nested_dotted_names(spark_tmp_path, footer):
    path = spark_tmp_path + "/nested_names"
    gens = [("c", IntegerGen(min_val=100, max_val=200, nullable=False)),
            ("s", StructGen(_collision_gens, nullable=False)),
            ("x.y", IntegerGen(min_val=3000, max_val=4000, nullable=False)),
            ("extra", IntegerGen())]
    with_cpu_session(lambda spark: gen_df(spark, gens, length=16, num_slices=1)
                     .write.parquet(path))
    # Reverse roots and nested children, pruning extra fields at both levels.
    schema = StructType([
        StructField("x.y", IntegerType()),
        StructField("s", StructType([
            StructField("a.b", IntegerType()),
            StructField("a", StructType([StructField("b", IntegerType())])),
        ])),
        StructField("c", IntegerType()),
        StructField("missing", IntegerType()),
    ])
    # Selecting s uses includeColumn on its ordinary root. Adding x.y skips the
    # ambiguous selector and relies on the same pruned, ordered footer.
    for projection in [["s", "c", "missing"], ["`x.y`", "s", "c", "missing"]]:
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.schema(schema).parquet(path).selectExpr(*projection),
            conf={"spark.rapids.sql.format.parquet.reader.footer.type": footer})


@ignore_order(local=True)
@pytest.mark.parametrize("footer", ["JAVA", "NATIVE"])
def test_dotted_column_renamed_by_parquet_field_id(spark_tmp_path, footer):
    path = spark_tmp_path + "/field_ids"
    schema = StructType([
        StructField("a", StructType([StructField("b", IntegerType()),
                                     StructField("z", IntegerType())])),
        StructField("a.b", IntegerType(), metadata={"parquet.field.id": 1}),
        StructField("c", IntegerType(), metadata={"parquet.field.id": 2}),
    ])
    gens = [_collision_gens[0], _collision_gens[1], _collision_gens[2]]
    def write(spark):
        data = gen_df(spark, gens, length=16, num_slices=1)
        spark.createDataFrame(data.rdd, schema).write.parquet(path)
    with_cpu_session(write, conf={"spark.sql.parquet.fieldId.write.enabled": "true"})
    read_schema = StructType([
        StructField("other", IntegerType(), metadata={"parquet.field.id": 2}),
        StructField("renamed", IntegerType(), metadata={"parquet.field.id": 1}),
    ])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).parquet(path),
        conf={"spark.sql.parquet.fieldId.read.enabled": "true",
              "spark.rapids.sql.format.parquet.reader.footer.type": footer})


@ignore_order(local=True)
@pytest.mark.parametrize("file_format", ["parquet", "orc"])
@pytest.mark.parametrize("reader,chunked,nested_precision", [
    ("PERFILE", False, 7), ("MULTITHREADED", True, 15), ("COALESCING", False, 15)])
def test_dotted_decimal_column_collides_with_struct_field(
        spark_tmp_path, file_format, chunked, nested_precision, reader):
    path = spark_tmp_path + "/decimal_collision"
    gens = [
        ("a", StructGen([("b", DecimalGen(precision=nested_precision, scale=2)),
                         ("z", IntegerGen())])),
        ("a.b", DecimalGen(precision=25, scale=2)),
        ("c", IntegerGen()),
    ]
    with_cpu_session(lambda spark: gen_df(spark, gens, length=64, num_slices=2)
                     .write.format(file_format).save(path))
    conf = {"spark.rapids.sql.reader.chunked": str(chunked).lower(),
            "spark.rapids.sql.format." + file_format + ".reader.type": reader}
    if file_format == "parquet":
        conf["spark.rapids.sql.format.parquet.reader.footer.type"] = "JAVA"
    # Keep the complete struct: a flat-only projection discards the mis-sized decimal.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.format(file_format).load(path), conf=conf)


@ignore_order(local=True)
@pytest.mark.parametrize("container,reader,chunked", [
    ("struct", "PERFILE", False), ("array", "MULTITHREADED", True),
    ("map", "COALESCING", False)])
def test_orc_nested_dotted_decimal_collision(spark_tmp_path, chunked, reader, container):
    path = spark_tmp_path + "/nested_decimal_collision"
    collision = StructGen([
        ("a", StructGen([("b", DecimalGen(precision=15, scale=2))])),
        ("a.b", DecimalGen(precision=25, scale=2)),
    ])
    if container == "array":
        collision = ArrayGen(collision, min_length=1, max_length=3)
    elif container == "map":
        collision = MapGen(StringGen("[a-z]{2}", nullable=False), collision,
                           min_length=1, max_length=3)
    with_cpu_session(lambda spark: gen_df(spark, [("s", collision)], length=64, num_slices=2)
                     .write.orc(path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.orc(path),
        conf={"spark.rapids.sql.reader.chunked": str(chunked).lower(),
              "spark.rapids.sql.format.orc.reader.type": reader})
