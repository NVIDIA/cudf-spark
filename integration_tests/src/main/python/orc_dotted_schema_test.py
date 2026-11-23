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
from pyspark.sql.types import ArrayType, DecimalType, IntegerType, MapType, StringType, StructField, StructType
from spark_session import with_cpu_session


@ignore_order(local=True)
@pytest.mark.parametrize("reader,chunked", [("PERFILE", False), ("COALESCING", True)])
def test_orc_unrelated_dotted_name_decimal_widening(spark_tmp_path, chunked, reader):
    path = spark_tmp_path + "/unrelated_dotted_decimal_widening"
    decimal = DecimalGen(precision=15, scale=2)
    nested = StructGen([("d", decimal)])
    gens = [
        ("unrelated.dot", IntegerGen()),
        ("unchanged", decimal),
        ("widened", decimal),
        ("s", nested),
        ("a", ArrayGen(nested, min_length=0, max_length=3)),
        ("m", MapGen(StringGen("[a-z]{2}", nullable=False), nested,
                     min_length=0, max_length=3)),
    ]
    with_cpu_session(lambda spark: gen_df(spark, gens, length=64, num_slices=2)
                     .write.orc(path))
    widened_struct = StructType([StructField("d", DecimalType(25, 2))])
    read_schema = StructType([
        StructField("widened", DecimalType(25, 2)),
        StructField("unrelated.dot", IntegerType()),
        StructField("missing", IntegerType()),
        StructField("unchanged", DecimalType(15, 2)),
        StructField("s", widened_struct),
        StructField("a", ArrayType(widened_struct)),
        StructField("m", MapType(StringType(), widened_struct)),
    ])
    # A dotted field disables decimal overrides even for unrelated columns. Keep all
    # columns, including unchanged decimals, and exercise recursive requested widening.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(read_schema).orc(path),
        conf={"spark.rapids.sql.reader.chunked": str(chunked).lower(),
              "spark.rapids.sql.format.orc.reader.type": reader})
