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
from data_gen import ArrayGen, IntegerGen, MapGen, StringGen, StructGen, gen_df
from marks import ignore_order
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType
from spark_session import with_cpu_session


@ignore_order(local=True)
@pytest.mark.parametrize("footer", ["JAVA", "NATIVE"])
@pytest.mark.parametrize("container", ["struct", "array", "map"])
def test_parquet_nested_dotted_names_select_root(spark_tmp_path, footer, container):
    path = spark_tmp_path + "/nested_names"
    collision = StructGen([
        ("a", StructGen([("b", IntegerGen(min_val=1, max_val=9))])),
        ("a.b", IntegerGen(min_val=1000, max_val=2000)),
        ("unrequested", IntegerGen()),
    ])
    # Reverse the physical child order in the requested schema. Selecting the ordinary
    # root must traverse physical child IDs without resolving ambiguous dotted leaf paths.
    requested = StructType([
        StructField("a.b", IntegerType()),
        StructField("a", StructType([StructField("b", IntegerType())])),
    ])
    if container == "array":
        collision = ArrayGen(collision, min_length=1, max_length=3)
        requested = ArrayType(requested)
    elif container == "map":
        collision = MapGen(StringGen("[a-z]{2}", nullable=False), collision,
                           min_length=1, max_length=3)
        requested = MapType(StringType(), requested)
    # Read roots in the opposite order and remove an unrequested root and child.
    with_cpu_session(lambda spark: gen_df(
        spark, [("c", IntegerGen()), ("s", collision), ("extra", IntegerGen())],
        length=32, num_slices=2).write.parquet(path))
    schema = StructType([StructField("s", requested), StructField("c", IntegerType())])
    for projection in [["s", "c"], ["c", "s"], ["s"]]:
        print("Checking nested projection:", container, projection)
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark: spark.read.schema(schema).parquet(path).select(*projection),
            conf={"spark.rapids.sql.format.parquet.reader.footer.type": footer})
