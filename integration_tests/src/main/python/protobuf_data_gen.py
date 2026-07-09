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

import inspect

import pyspark.sql.functions as f

from data_gen import StructGen, gen_df
from spark_session import with_cpu_session


def call_protobuf_function(protobuf_fn, col, message_name,
                           desc_path, desc_bytes, options=None):
    """Call a Spark protobuf function across descriptor API variants."""
    sig = inspect.signature(protobuf_fn)
    if "binaryDescriptorSet" in sig.parameters:
        kwargs = {"binaryDescriptorSet": bytearray(desc_bytes)}
        if options is not None:
            kwargs["options"] = options
        return protobuf_fn(col, message_name, **kwargs)
    if options is not None:
        return protobuf_fn(col, message_name, desc_path, options)
    return protobuf_fn(col, message_name, desc_path)


def materialize_protobuf_data(logical_gen, message_name, desc_path, desc_bytes,
                              *, length=None, logical_rows=None):
    """Return CPU-encoded rows and schema for a later CPU/GPU comparison."""
    if not isinstance(logical_gen, StructGen):
        raise TypeError(
            f"logical_gen must be a StructGen, got {type(logical_gen).__name__}")
    if logical_gen.nullable:
        raise ValueError("top-level protobuf StructGen must be non-nullable")
    if any(field.name.casefold() == "bin" for field in logical_gen.data_type.fields):
        raise ValueError("protobuf field name conflicts with binary column: bin")
    if logical_rows is not None and length is not None:
        raise ValueError("length cannot be used with explicit logical rows")

    def materialize(spark):
        from pyspark.sql.protobuf.functions import to_protobuf

        if logical_rows is None and length is None:
            source = gen_df(spark, logical_gen)
        elif logical_rows is None:
            source = gen_df(spark, logical_gen, length=length)
        else:
            source = spark.createDataFrame(logical_rows, logical_gen.data_type)

        logical_value = f.struct(*(
            f.col(field.name).alias(field.name)
            for field in logical_gen.data_type.fields))
        encoded = source.select(
            "*",
            call_protobuf_function(
                to_protobuf, logical_value, message_name,
                desc_path, desc_bytes).alias("bin"))
        rows = [
            tuple(row[:-1]) + (bytes(row["bin"]),)
            for row in encoded.collect()
        ]
        return rows, encoded.schema

    return with_cpu_session(materialize)
