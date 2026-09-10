# Copyright (c) 2023-2026, NVIDIA CORPORATION.
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
import pyarrow as pa
import pyarrow.parquet as pa_pq

from data_gen import *

def gen_pyarrow_table(data_gen, length=2048, seed=0):
    """Generate pyarrow table from data gen"""
    if isinstance(data_gen, list):
        src = StructGen(data_gen, nullable=False)
    else:
        src = data_gen
        # we cannot create a data frame from a nullable struct
        assert not data_gen.nullable
    # gen row based data
    data = gen_df_help(src, length, seed)
    # convert row based data to column based data
    columnar_data = [[data[row][col] for row in range(length)] for col in range(len(src.children))]
    # generate pa schema
    fields = [(name, get_pyarrow_type(child)) for name, child in src.children]
    pa_schema = pa.schema(fields)
    pa_data = [pa.array(col, type=field[1]) for col, field in zip(columnar_data, fields)]
    # generate pa table
    return pa.Table.from_arrays(pa_data, schema=pa_schema)


def convert_pyarrow_table_to_cpu_object(pa_table):
    """
    Convert pyarrow table to cpu type data to reuse the utils in asserts.py
     e.g.:
      - convert pyarrow map scalar in pyarrow table to Python dict
      - convert pyarrow struct scalar in pyarrow table to Pyspark Row
      - convert pyarrow binary scala in pyarrow table to python bytearray
      ...
    """
    columnar_data = []
    for pa_column in pa_table.columns:
        converted_column = _convert_pyarrow_column_to_cpu_object(pa_column)
        columnar_data.append(converted_column)
    cpu_data = [
        _gen_row_from_columnar_cpu_data(pa_table.schema.names, columnar_data, row_idx)
        for row_idx in range(pa_table.num_rows)
    ]
    return cpu_data


def get_pyarrow_type(data_gen):
    """Map from data gen to pyarrow data type"""
    if isinstance(data_gen, BooleanGen):
        return pa.bool_()
    elif isinstance(data_gen, ByteGen):
        return pa.int8()
    elif isinstance(data_gen, ShortGen):
        return pa.int16()
    elif isinstance(data_gen, IntegerGen):
        return pa.int32()
    elif isinstance(data_gen, LongGen):
        return pa.int64()
    elif isinstance(data_gen, FloatGen):
        return pa.float32()
    elif isinstance(data_gen, DoubleGen):
        return pa.float64()
    elif isinstance(data_gen, DateGen):
        return pa.date32()
    elif isinstance(data_gen, TimestampGen):
        # use us, because Spark does not support ns
        # Use UTC-adjusted timestamps for Spark TimestampType to avoid
        # TimestampNTZ inference.
        if data_gen._tzinfo is None:
            return pa.timestamp('us')
        return pa.timestamp('us', tz='UTC')
    elif isinstance(data_gen, BinaryGen):
        return pa.binary()
    elif isinstance(data_gen, StringGen):
        return pa.string()
    elif isinstance(data_gen, DecimalGen):
        return pa.decimal128(data_gen.precision, data_gen.scale)
    elif isinstance(data_gen, RepeatSeqGen):
        return get_pyarrow_type(data_gen._child)
    elif isinstance(data_gen, StructGen):
        fields = [pa.field(name, get_pyarrow_type(child)) for name, child in data_gen.children]
        return pa.struct(fields)
    elif isinstance(data_gen, ArrayGen):
        return pa.list_(get_pyarrow_type(data_gen._child_gen))
    elif isinstance(data_gen, MapGen):
        return pa.map_(get_pyarrow_type(data_gen._key_gen), get_pyarrow_type(data_gen._value_gen))
    else:
        raise Exception("unexpected data_gen: " + str(data_gen))


def _convert_pyarrow_struct_object_to_cpu_object(pyarrow_struct_scalar):
    """Convert pyarrow struct to pyspark.Row"""
    # convert pyarrow.StructScalar obj to python dict
    if pyarrow_struct_scalar.as_py() is None:
        return None
    else:
        py_dict = {}
        for column_name in pyarrow_struct_scalar.keys():
            v = _convert_pyarrow_object_to_cpu_object(pyarrow_struct_scalar.get(column_name))
            py_dict[column_name] = v
        return Row(**py_dict)


def _convert_pyarrow_list_object_to_cpu_object(pyarrow_list_scalar):
    """Convert pyarrow list to python list"""
    # pyarrow.ListScalar obj to python list
    if pyarrow_list_scalar.as_py() is None:
        return None
    else:
        return [_convert_pyarrow_object_to_cpu_object(v) for v in pyarrow_list_scalar.values]


def _convert_pyarrow_map_object_to_cpu_object(pyarrow_map_scalar):
    """Convert pyarrow map to python dict"""
    # pyarrow_map_scalar is pyarrow.MapScalar
    if pyarrow_map_scalar.as_py() is None:
        return None
    else:
        py_key_value_pair_list = []
        # Note pyarrow.MapScalar.as_py() not works when there are nested maps
        # e.g.: as_py returns [('k1', [('sub_k1', 'sub_v1'), ('sub_k2', 'sub_v2')])] from
        # {'k1': {'sub_k1': 'sub_v1', 'sub_k2': 'sub_v2'}} which is map(string, map(string, string)) type
        for pair in pyarrow_map_scalar.values:
            # pair is pyarrow.StructScalar
            k = _convert_pyarrow_object_to_cpu_object(pair.get("key"))
            v = _convert_pyarrow_object_to_cpu_object(pair.get("value"))
            py_key_value_pair_list.append((k, v))
        return dict(py_key_value_pair_list)

def _convert_pyarrow_binary_object_to_cpu_object(pyarrow_binary_scalar):
    """Convert pyarrow binary to bytearray"""
    # pyarrow.BinaryScalar obj to bytearray
    py_bytearray = pyarrow_binary_scalar.as_py()
    if py_bytearray is None:
        return None
    else:
        return bytearray(py_bytearray)


def _convert_pyarrow_object_to_cpu_object(pyarrow_obj):
    """ convert pyarrow column to a column contains CPU type objects"""
    if isinstance(pyarrow_obj.type, pa.StructType):
        # pyarrow struct object should be converted to Spark Row
        return _convert_pyarrow_struct_object_to_cpu_object(pyarrow_obj)
    elif isinstance(pyarrow_obj.type, pa.ListType):
        # pyarrow list object should be converted to python list
        return _convert_pyarrow_list_object_to_cpu_object(pyarrow_obj)
    elif isinstance(pyarrow_obj.type, pa.MapType):
        # pyarrow map object should be converted to python dict
        return _convert_pyarrow_map_object_to_cpu_object(pyarrow_obj)
    elif pyarrow_obj.type.equals(pa.binary()):
        # pyarrow binary object should be converted to python bytearray
        return _convert_pyarrow_binary_object_to_cpu_object(pyarrow_obj)
    else:
        # other type
        return pyarrow_obj.as_py()

def _convert_pyarrow_column_to_cpu_object(pyarrow_column):
    return [_convert_pyarrow_object_to_cpu_object(v) for v in pyarrow_column]


def _gen_row_from_columnar_cpu_data(column_names, columnar_data, row_idx):
    """Gen a Pyspark Row from columnar data for a row index"""
    key_value_pair_list = [(column_names[col], columnar_data[col][row_idx]) for col in range(len(column_names))]
    return Row(**dict(key_value_pair_list))
