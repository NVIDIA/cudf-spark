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

from dataclasses import dataclass, replace
from enum import Enum
import struct

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from data_gen import DataGen

# -----------------------------------------------------------------------------
# Protobuf schema-first test modeling / generation / encoding
# -----------------------------------------------------------------------------

_PROTOBUF_WIRE_VARINT = 0
_PROTOBUF_WIRE_64BIT = 1
_PROTOBUF_WIRE_LEN_DELIM = 2
_PROTOBUF_WIRE_32BIT = 5
_PROTOBUF_MAX_FIELD_NUMBER = (1 << 29) - 1
_PROTOBUF_RESERVED_FIELD_START = 19000
_PROTOBUF_RESERVED_FIELD_END = 19999
_PB_MISSING = object()


def _encode_protobuf_uvarint(value):
    """Encode a non-negative integer as protobuf varint."""
    if value is None:
        raise ValueError("value must not be None")
    if value < 0:
        raise ValueError("uvarint only supports non-negative integers")
    out = bytearray()
    v = int(value)
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _encode_protobuf_key(field_number, wire_type):
    return _encode_protobuf_uvarint((int(field_number) << 3) | int(wire_type))


def _encode_protobuf_zigzag32(value):
    return (int(value) << 1) ^ (int(value) >> 31)


def _encode_protobuf_zigzag64(value):
    return (int(value) << 1) ^ (int(value) >> 63)


def _validate_protobuf_field_number(number):
    if (number <= 0 or number > _PROTOBUF_MAX_FIELD_NUMBER or
            _PROTOBUF_RESERVED_FIELD_START <= number <= _PROTOBUF_RESERVED_FIELD_END):
        raise ValueError(f'invalid protobuf field number: {number}')


def _unsigned_wire_value(value, bits, field_name):
    value = int(value)
    signed_min = -(1 << (bits - 1))
    unsigned_max = (1 << bits) - 1
    if value < signed_min or value > unsigned_max:
        raise ValueError(
            f'unsigned {bits}-bit value is out of range for {field_name}: {value}')
    return value & unsigned_max


def _unsigned_spark_value(value, bits, field_name):
    value = _unsigned_wire_value(value, bits, field_name)
    sign_bit = 1 << (bits - 1)
    return value - (1 << bits) if value >= sign_bit else value


def _signed_wire_value(value, bits, kind_name, field_name):
    value = int(value)
    signed_min = -(1 << (bits - 1))
    signed_max = (1 << (bits - 1)) - 1
    if value < signed_min or value > signed_max:
        raise ValueError(
            f'{kind_name} value is out of range for {field_name}: {value}')
    return value


class PbCardinality(Enum):
    OPTIONAL = 'optional'
    REQUIRED = 'required'
    REPEATED = 'repeated'


class PbScalarKind(Enum):
    BOOL = 'bool'
    INT32 = 'int32'
    INT64 = 'int64'
    UINT32 = 'uint32'
    UINT64 = 'uint64'
    SINT32 = 'sint32'
    SINT64 = 'sint64'
    FIXED32 = 'fixed32'
    FIXED64 = 'fixed64'
    SFIXED32 = 'sfixed32'
    SFIXED64 = 'sfixed64'
    FLOAT = 'float'
    DOUBLE = 'double'
    STRING = 'string'
    BYTES = 'bytes'
    ENUM = 'enum'


def _pb_scalar_kind_spark_type(kind):
    if kind in {PbScalarKind.BOOL}:
        return BooleanType()
    if kind == PbScalarKind.ENUM:
        return IntegerType()
    if kind in {PbScalarKind.INT32, PbScalarKind.UINT32, PbScalarKind.SINT32,
                PbScalarKind.FIXED32, PbScalarKind.SFIXED32}:
        return IntegerType()
    if kind in {PbScalarKind.INT64, PbScalarKind.UINT64, PbScalarKind.SINT64,
                PbScalarKind.FIXED64, PbScalarKind.SFIXED64}:
        return LongType()
    if kind == PbScalarKind.FLOAT:
        return FloatType()
    if kind == PbScalarKind.DOUBLE:
        return DoubleType()
    if kind == PbScalarKind.STRING:
        return StringType()
    if kind == PbScalarKind.BYTES:
        return BinaryType()
    raise ValueError(f'Unsupported protobuf scalar kind: {kind}')


def _validate_pb_scalar_default(field_spec):
    default = field_spec.default
    if default is None:
        return
    kind = field_spec.kind
    if kind == PbScalarKind.BOOL:
        if not isinstance(default, bool):
            raise TypeError(
                f'bool field {field_spec.name} requires a bool default')
        return
    if kind in {
            PbScalarKind.INT32, PbScalarKind.SINT32, PbScalarKind.SFIXED32}:
        if isinstance(default, bool) or not isinstance(default, int):
            raise TypeError(
                f'{kind.value} field {field_spec.name} requires an int default')
        _signed_wire_value(default, 32, kind.value, field_spec.name)
        return
    if kind in {
            PbScalarKind.INT64, PbScalarKind.SINT64, PbScalarKind.SFIXED64}:
        if isinstance(default, bool) or not isinstance(default, int):
            raise TypeError(
                f'{kind.value} field {field_spec.name} requires an int default')
        _signed_wire_value(default, 64, kind.value, field_spec.name)
        return
    if kind in {PbScalarKind.UINT32, PbScalarKind.FIXED32}:
        if isinstance(default, bool) or not isinstance(default, int):
            raise TypeError(
                f'{kind.value} field {field_spec.name} requires an int default')
        if default < 0 or default >= 1 << 32:
            raise ValueError(
                f'{kind.value} default is out of range for {field_spec.name}: {default}')
        return
    if kind in {PbScalarKind.UINT64, PbScalarKind.FIXED64}:
        if isinstance(default, bool) or not isinstance(default, int):
            raise TypeError(
                f'{kind.value} field {field_spec.name} requires an int default')
        if default < 0 or default >= 1 << 64:
            raise ValueError(
                f'{kind.value} default is out of range for {field_spec.name}: {default}')
        return
    if kind == PbScalarKind.ENUM:
        if isinstance(default, str):
            field_spec.enum.number_for(default)
        elif isinstance(default, bool) or not isinstance(default, int):
            raise TypeError(
                f'enum field {field_spec.name} requires a string or int default')
        else:
            _signed_wire_value(default, 32, kind.value, field_spec.name)
            field_spec.enum.name_for(default)
        return
    if kind in {PbScalarKind.FLOAT, PbScalarKind.DOUBLE}:
        if isinstance(default, bool) or not isinstance(default, (int, float)):
            raise TypeError(
                f'{kind.value} field {field_spec.name} requires a numeric default')
        return
    if kind == PbScalarKind.STRING:
        if not isinstance(default, str):
            raise TypeError(
                f'string field {field_spec.name} requires a string default')
        return
    if kind == PbScalarKind.BYTES:
        raise NotImplementedError('bytes defaults are not supported')
    raise ValueError(f'Unsupported protobuf scalar kind: {kind}')


@dataclass(frozen=True)
class PbEnumSpec:
    name: str
    values: tuple
    allow_alias: bool = False

    def __post_init__(self):
        object.__setattr__(self, 'name', str(self.name))
        values = tuple((str(name), int(number)) for name, number in self.values)
        if not values:
            raise ValueError('enum spec must contain at least one value')
        names = [name for name, _ in values]
        numbers = [number for _, number in values]
        if len(names) != len(set(names)):
            raise ValueError(f'duplicate enum names in {self.name}')
        if not self.allow_alias and len(numbers) != len(set(numbers)):
            raise ValueError(f'duplicate enum numbers in {self.name}')
        if any(number < -(1 << 31) or number >= (1 << 31) for number in numbers):
            raise ValueError(f'enum number is out of int32 range in {self.name}')
        object.__setattr__(self, 'values', values)

    def number_for(self, value):
        if isinstance(value, str):
            for name, number in self.values:
                if name == value:
                    return number
            raise ValueError(f'Unknown enum name {value!r} for enum {self.name}')
        return int(value)

    def name_for(self, value):
        number = self.number_for(value)
        for name, candidate in self.values:
            if candidate == number:
                return name
        raise ValueError(f'Unknown enum number {number} for enum {self.name}')


@dataclass(frozen=True)
class PbScalarFieldSpec:
    name: str
    number: int
    kind: PbScalarKind
    gen: object = None
    cardinality: PbCardinality = PbCardinality.OPTIONAL
    default: object = None
    packed: bool = False
    min_len: int = 0
    max_len: int = 5
    enum: object = None

    def __post_init__(self):
        object.__setattr__(self, 'name', str(self.name))
        object.__setattr__(self, 'number', int(self.number))
        _validate_protobuf_field_number(self.number)
        if self.gen is not None:
            if not isinstance(self.gen, DataGen):
                raise TypeError(
                    f'{self.kind.value} field {self.name} requires a DataGen, '
                    f'got {type(self.gen).__name__}')
            expected_type = _pb_scalar_kind_spark_type(self.kind)
            valid_types = ((IntegerType, StringType) if self.kind == PbScalarKind.ENUM
                           else (type(expected_type),))
            if not isinstance(self.gen.data_type, valid_types):
                expected_name = ('IntegerType or StringType'
                                 if self.kind == PbScalarKind.ENUM
                                 else type(expected_type).__name__)
                raise TypeError(
                    f'{self.kind.value} field {self.name} requires {expected_name}, '
                    f'got {type(self.gen.data_type).__name__}')
        if self.cardinality != PbCardinality.REPEATED and self.packed:
            raise ValueError(f'packed encoding requires repeated cardinality: {self.name}')
        if self.cardinality == PbCardinality.REPEATED and self.default is not None:
            raise ValueError(f'repeated fields cannot have defaults: {self.name}')
        if self.min_len < 0 or self.max_len < self.min_len:
            raise ValueError(f'invalid repeated length bounds for {self.name}')
        if self.kind == PbScalarKind.ENUM:
            if self.enum is None:
                raise ValueError(f'enum field requires enum spec: {self.name}')
        elif self.enum is not None:
            raise ValueError(f'non-enum field cannot carry enum settings: {self.name}')
        _validate_pb_scalar_default(self)
        if self.packed and self.kind not in {
                PbScalarKind.BOOL, PbScalarKind.INT32, PbScalarKind.INT64,
                PbScalarKind.UINT32, PbScalarKind.UINT64, PbScalarKind.SINT32,
                PbScalarKind.SINT64, PbScalarKind.FIXED32, PbScalarKind.FIXED64,
                PbScalarKind.SFIXED32, PbScalarKind.SFIXED64, PbScalarKind.FLOAT,
                PbScalarKind.DOUBLE, PbScalarKind.ENUM}:
            raise ValueError(f'packed encoding is not supported for {self.kind.value}: {self.name}')


@dataclass(frozen=True)
class PbMessageFieldSpec:
    name: str
    number: int
    fields: tuple
    cardinality: PbCardinality = PbCardinality.OPTIONAL
    min_len: int = 0
    max_len: int = 5

    def __post_init__(self):
        object.__setattr__(self, 'name', str(self.name))
        object.__setattr__(self, 'number', int(self.number))
        object.__setattr__(self, 'fields', tuple(self.fields))
        _validate_protobuf_field_number(self.number)
        if self.min_len < 0 or self.max_len < self.min_len:
            raise ValueError(f'invalid repeated length bounds for {self.name}')
        if self.cardinality == PbCardinality.REQUIRED and self.min_len != 0:
            raise ValueError('required message field cannot define repeated bounds')
        _validate_pb_fields(self.fields, self.name)


@dataclass(frozen=True)
class PbMessageSpec:
    name: str
    fields: tuple
    package: str = ''

    def __post_init__(self):
        object.__setattr__(self, 'name', str(self.name))
        object.__setattr__(self, 'fields', tuple(self.fields))
        object.__setattr__(self, 'package', str(self.package).strip('.'))
        _validate_pb_fields(self.fields, self.name)

    @property
    def full_name(self):
        return '.'.join(part for part in (self.package, self.name) if part)

    def as_datagen(self, binary_col_name='bin', enums_as_ints=True):
        return ProtobufRowGen(
            self, binary_col_name=binary_col_name, enums_as_ints=enums_as_ints)

    def as_logical_datagen(self, enums_as_ints=True):
        return ProtobufRowGen(
            self, binary_col_name=None, enums_as_ints=enums_as_ints)

    def encode(self, value):
        return encode_pb_message(self, value)

    def descriptor_set_bytes(self, spark, file_name=None):
        return _build_descriptor_set_bytes(
            spark, self, file_name or f'{self.name}.proto')


def _validate_pb_fields(fields, owner_name):
    names = [field.name for field in fields]
    numbers = [field.number for field in fields]
    if len(names) != len(set(names)):
        raise ValueError(f'duplicate field names in {owner_name}')
    if len(numbers) != len(set(numbers)):
        raise ValueError(f'duplicate field numbers in {owner_name}')


def _build_descriptor_set_bytes(spark, message_spec, file_name):
    D = spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos
    file_builder = (D.FileDescriptorProto.newBuilder()
                    .setName(file_name)
                    .setSyntax('proto2'))
    if message_spec.package:
        file_builder.setPackage(message_spec.package)

    type_map = {
        PbScalarKind.BOOL: D.FieldDescriptorProto.Type.TYPE_BOOL,
        PbScalarKind.INT32: D.FieldDescriptorProto.Type.TYPE_INT32,
        PbScalarKind.INT64: D.FieldDescriptorProto.Type.TYPE_INT64,
        PbScalarKind.UINT32: D.FieldDescriptorProto.Type.TYPE_UINT32,
        PbScalarKind.UINT64: D.FieldDescriptorProto.Type.TYPE_UINT64,
        PbScalarKind.SINT32: D.FieldDescriptorProto.Type.TYPE_SINT32,
        PbScalarKind.SINT64: D.FieldDescriptorProto.Type.TYPE_SINT64,
        PbScalarKind.FIXED32: D.FieldDescriptorProto.Type.TYPE_FIXED32,
        PbScalarKind.FIXED64: D.FieldDescriptorProto.Type.TYPE_FIXED64,
        PbScalarKind.SFIXED32: D.FieldDescriptorProto.Type.TYPE_SFIXED32,
        PbScalarKind.SFIXED64: D.FieldDescriptorProto.Type.TYPE_SFIXED64,
        PbScalarKind.FLOAT: D.FieldDescriptorProto.Type.TYPE_FLOAT,
        PbScalarKind.DOUBLE: D.FieldDescriptorProto.Type.TYPE_DOUBLE,
        PbScalarKind.STRING: D.FieldDescriptorProto.Type.TYPE_STRING,
        PbScalarKind.BYTES: D.FieldDescriptorProto.Type.TYPE_BYTES,
        PbScalarKind.ENUM: D.FieldDescriptorProto.Type.TYPE_ENUM,
    }
    label_map = {
        PbCardinality.OPTIONAL: D.FieldDescriptorProto.Label.LABEL_OPTIONAL,
        PbCardinality.REQUIRED: D.FieldDescriptorProto.Label.LABEL_REQUIRED,
        PbCardinality.REPEATED: D.FieldDescriptorProto.Label.LABEL_REPEATED,
    }
    packed_options = D.FieldOptions.newBuilder().setPacked(True).build()

    def default_literal(field_spec):
        if field_spec.kind == PbScalarKind.BYTES:
            raise NotImplementedError(
                'bytes default protobuf descriptor generation is not implemented')
        if field_spec.kind == PbScalarKind.BOOL:
            return 'true' if field_spec.default else 'false'
        if field_spec.kind == PbScalarKind.ENUM:
            if isinstance(field_spec.default, str):
                return field_spec.default
            return field_spec.enum.name_for(field_spec.default)
        return str(field_spec.default)

    def build_enum(enum_spec):
        enum_builder = D.EnumDescriptorProto.newBuilder().setName(enum_spec.name)
        if enum_spec.allow_alias:
            enum_builder.setOptions(
                D.EnumOptions.newBuilder().setAllowAlias(True).build())
        for value_name, value_number in enum_spec.values:
            enum_builder.addValue(
                D.EnumValueDescriptorProto.newBuilder()
                .setName(value_name)
                .setNumber(value_number)
                .build())
        return enum_builder.build()

    def build_message(name, fields, full_name):
        message_builder = D.DescriptorProto.newBuilder().setName(name)
        enum_specs = {}
        for field_spec in fields:
            if (isinstance(field_spec, PbScalarFieldSpec) and
                    field_spec.kind == PbScalarKind.ENUM):
                previous = enum_specs.setdefault(field_spec.enum.name, field_spec.enum)
                if previous != field_spec.enum:
                    raise ValueError(
                        f'conflicting enum definitions for {field_spec.enum.name}')
        for enum_spec in enum_specs.values():
            message_builder.addEnumType(build_enum(enum_spec))
        enum_value_names = {
            value_name
            for enum_spec in enum_specs.values()
            for value_name, _ in enum_spec.values
        }
        reserved_names = (
            set(enum_specs) |
            enum_value_names |
            {field_spec.name for field_spec in fields})
        nested_names = {}
        for field_spec in fields:
            if isinstance(field_spec, PbMessageFieldSpec):
                nested_name = f'Nested{field_spec.number}'
                while nested_name in reserved_names:
                    nested_name += '_'
                reserved_names.add(nested_name)
                nested_names[field_spec.number] = nested_name
        for field_spec in fields:
            field_builder = (D.FieldDescriptorProto.newBuilder()
                             .setName(field_spec.name)
                             .setNumber(field_spec.number)
                             .setLabel(label_map[field_spec.cardinality]))
            if isinstance(field_spec, PbScalarFieldSpec):
                if field_spec.kind == PbScalarKind.ENUM:
                    field_builder.setType(D.FieldDescriptorProto.Type.TYPE_ENUM)
                    field_builder.setTypeName(f'.{full_name}.{field_spec.enum.name}')
                else:
                    field_builder.setType(type_map[field_spec.kind])
                if field_spec.default is not None:
                    field_builder.setDefaultValue(default_literal(field_spec))
                if field_spec.packed:
                    field_builder.setOptions(packed_options)
            else:
                nested_name = nested_names[field_spec.number]
                nested_full_name = f'{full_name}.{nested_name}'
                message_builder.addNestedType(
                    build_message(nested_name, field_spec.fields, nested_full_name))
                field_builder.setType(D.FieldDescriptorProto.Type.TYPE_MESSAGE)
                field_builder.setTypeName(f'.{nested_full_name}')
            message_builder.addField(field_builder.build())
        return message_builder.build()

    root_full_name = '.'.join(
        part for part in (message_spec.package, message_spec.name) if part)
    file_builder.addMessageType(
        build_message(message_spec.name, message_spec.fields, root_full_name))
    descriptor_set = D.FileDescriptorSet.newBuilder().addFile(file_builder.build()).build()
    return bytes(descriptor_set.toByteArray())


class _PbBuilder:
    def message(self, name, fields, package=''):
        return PbMessageSpec(name, tuple(fields), package=package)

    def bool(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.BOOL, gen=gen, default=default)

    def int32(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.INT32, gen=gen, default=default)

    def int64(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.INT64, gen=gen, default=default)

    def uint32(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.UINT32, gen=gen, default=default)

    def uint64(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.UINT64, gen=gen, default=default)

    def sint32(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.SINT32, gen=gen, default=default)

    def sint64(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.SINT64, gen=gen, default=default)

    def fixed32(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.FIXED32, gen=gen, default=default)

    def fixed64(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.FIXED64, gen=gen, default=default)

    def sfixed32(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.SFIXED32, gen=gen, default=default)

    def sfixed64(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.SFIXED64, gen=gen, default=default)

    def float(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.FLOAT, gen=gen, default=default)

    def double(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.DOUBLE, gen=gen, default=default)

    def string(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.STRING, gen=gen, default=default)

    def bytes(self, name, number, gen=None, default=None):
        return PbScalarFieldSpec(name, number, PbScalarKind.BYTES, gen=gen, default=default)

    def enum_type(self, name, values, allow_alias=False):
        return PbEnumSpec(name, tuple(values), allow_alias=allow_alias)

    def enum_field(self, name, number, enum_spec, gen=None, default=None):
        return PbScalarFieldSpec(
            name, number, PbScalarKind.ENUM, gen=gen, default=default, enum=enum_spec)

    def message_field(self, name, number, fields):
        return PbMessageFieldSpec(name, number, tuple(fields))

    def nested(self, name, number, fields):
        return self.message_field(name, number, fields)

    def repeated(self, field_spec, min_len=0, max_len=5, packed=False):
        if not isinstance(field_spec, (PbScalarFieldSpec, PbMessageFieldSpec)):
            raise TypeError(f'Unsupported protobuf field for repeated(): {type(field_spec)}')
        if field_spec.cardinality != PbCardinality.OPTIONAL:
            raise ValueError('repeated() expects an optional field')
        if isinstance(field_spec, PbScalarFieldSpec):
            return replace(
                field_spec,
                cardinality=PbCardinality.REPEATED,
                min_len=min_len,
                max_len=max_len,
                packed=packed)
        if isinstance(field_spec, PbMessageFieldSpec):
            if packed:
                raise ValueError('message fields cannot be packed')
            return replace(
                field_spec,
                cardinality=PbCardinality.REPEATED,
                min_len=min_len,
                max_len=max_len)

    def repeated_message(self, name, number, fields, min_len=0, max_len=5):
        return self.repeated(
            self.message_field(name, number, fields),
            min_len=min_len,
            max_len=max_len)

    def required(self, field_spec):
        if not isinstance(field_spec, (PbScalarFieldSpec, PbMessageFieldSpec)):
            raise TypeError(f'Unsupported protobuf field for required(): {type(field_spec)}')
        if field_spec.cardinality != PbCardinality.OPTIONAL:
            raise ValueError('required() expects an optional field')
        if isinstance(field_spec, PbScalarFieldSpec):
            if field_spec.default is not None:
                raise ValueError('required fields cannot have defaults')
            return replace(field_spec, cardinality=PbCardinality.REQUIRED)
        if isinstance(field_spec, PbMessageFieldSpec):
            return replace(field_spec, cardinality=PbCardinality.REQUIRED)


pb = _PbBuilder()


def _pb_gen_cache_repr(gen):
    return 'None' if gen is None else gen._cache_repr()


def _pb_enum_cache_repr(enum_spec):
    return ('None' if enum_spec is None else
            str((enum_spec.values, enum_spec.allow_alias)))


def _pb_field_cache_repr(field_spec):
    if isinstance(field_spec, PbScalarFieldSpec):
        return ('Scalar(' + field_spec.name + ',' + str(field_spec.number) + ',' +
                field_spec.kind.value + ',' + field_spec.cardinality.value + ',' +
                str(field_spec.default) + ',' + str(field_spec.packed) + ',' +
                str(field_spec.min_len) + ',' + str(field_spec.max_len) + ',' +
                _pb_enum_cache_repr(field_spec.enum) + ',' +
                _pb_gen_cache_repr(field_spec.gen) + ')')
    children = ','.join(_pb_field_cache_repr(child) for child in field_spec.fields)
    return ('Message(' + field_spec.name + ',' + str(field_spec.number) + ',' +
            field_spec.cardinality.value + ',' + str(field_spec.min_len) + ',' +
            str(field_spec.max_len) + ',[' + children + '])')


def _pb_message_cache_repr(message_spec):
    children = ','.join(_pb_field_cache_repr(field_spec) for field_spec in message_spec.fields)
    return ('PbMessage(' + message_spec.full_name + ',[' + children + '])')


class ProtobufEncoder:
    def encode_message(self, message_spec, value):
        if value is None:
            value = {}
        if not isinstance(message_spec, PbMessageSpec):
            raise TypeError(f'encode_message expects PbMessageSpec, got {type(message_spec)}')
        return self._encode_message_fields(message_spec.fields, value)

    def encode_field(self, field_spec, value):
        if isinstance(field_spec, PbScalarFieldSpec):
            return self._encode_scalar_field(field_spec, value)
        return self._encode_message_field(field_spec, value)

    def _encode_message_fields(self, fields, value):
        if not isinstance(value, dict):
            raise TypeError(f'protobuf message values must be dicts, got {type(value)}')
        unknown = set(value.keys()) - {field.name for field in fields}
        if unknown:
            raise ValueError(f'unknown protobuf field(s): {sorted(unknown)}')
        return b''.join(
            self.encode_field(field, value.get(field.name, _PB_MISSING))
            for field in fields)

    def _normalize_scalar_input(self, field_spec, value):
        if field_spec.kind == PbScalarKind.ENUM:
            return field_spec.enum.number_for(value)
        if field_spec.kind == PbScalarKind.BOOL:
            return bool(value)
        if field_spec.kind in {
                PbScalarKind.INT32, PbScalarKind.INT64, PbScalarKind.UINT32,
                PbScalarKind.UINT64, PbScalarKind.SINT32, PbScalarKind.SINT64,
                PbScalarKind.FIXED32, PbScalarKind.FIXED64, PbScalarKind.SFIXED32,
                PbScalarKind.SFIXED64}:
            return int(value)
        if field_spec.kind in {PbScalarKind.FLOAT, PbScalarKind.DOUBLE}:
            return float(value)
        if field_spec.kind == PbScalarKind.STRING:
            return str(value)
        if field_spec.kind == PbScalarKind.BYTES:
            return value if isinstance(value, bytes) else bytes(value)
        raise ValueError(f'Unsupported scalar kind: {field_spec.kind}')

    def _encode_scalar_payload(self, field_spec, value):
        scalar_value = self._normalize_scalar_input(field_spec, value)
        kind = field_spec.kind
        if kind == PbScalarKind.BOOL:
            return _PROTOBUF_WIRE_VARINT, _encode_protobuf_uvarint(1 if scalar_value else 0)
        if kind in {PbScalarKind.INT32, PbScalarKind.INT64, PbScalarKind.ENUM}:
            bits = 64 if kind == PbScalarKind.INT64 else 32
            scalar_value = _signed_wire_value(
                scalar_value, bits, kind.value, field_spec.name)
            u64 = scalar_value & 0xFFFFFFFFFFFFFFFF
            return _PROTOBUF_WIRE_VARINT, _encode_protobuf_uvarint(u64)
        if kind == PbScalarKind.UINT32:
            return (_PROTOBUF_WIRE_VARINT,
                    _encode_protobuf_uvarint(
                        _unsigned_wire_value(scalar_value, 32, field_spec.name)))
        if kind == PbScalarKind.UINT64:
            return (_PROTOBUF_WIRE_VARINT,
                    _encode_protobuf_uvarint(
                        _unsigned_wire_value(scalar_value, 64, field_spec.name)))
        if kind == PbScalarKind.SINT32:
            scalar_value = _signed_wire_value(
                scalar_value, 32, kind.value, field_spec.name)
            return (_PROTOBUF_WIRE_VARINT,
                    _encode_protobuf_uvarint(
                        _encode_protobuf_zigzag32(scalar_value)))
        if kind == PbScalarKind.SINT64:
            scalar_value = _signed_wire_value(
                scalar_value, 64, kind.value, field_spec.name)
            return (_PROTOBUF_WIRE_VARINT,
                    _encode_protobuf_uvarint(
                        _encode_protobuf_zigzag64(scalar_value)))
        if kind == PbScalarKind.FIXED32:
            return (_PROTOBUF_WIRE_32BIT,
                    struct.pack(
                        '<I', _unsigned_wire_value(scalar_value, 32, field_spec.name)))
        if kind == PbScalarKind.FIXED64:
            return (_PROTOBUF_WIRE_64BIT,
                    struct.pack(
                        '<Q', _unsigned_wire_value(scalar_value, 64, field_spec.name)))
        if kind == PbScalarKind.SFIXED32:
            scalar_value = _signed_wire_value(
                scalar_value, 32, kind.value, field_spec.name)
            return _PROTOBUF_WIRE_32BIT, struct.pack('<I', scalar_value & 0xFFFFFFFF)
        if kind == PbScalarKind.SFIXED64:
            scalar_value = _signed_wire_value(
                scalar_value, 64, kind.value, field_spec.name)
            return _PROTOBUF_WIRE_64BIT, struct.pack(
                '<Q', scalar_value & 0xFFFFFFFFFFFFFFFF)
        if kind == PbScalarKind.FLOAT:
            return _PROTOBUF_WIRE_32BIT, struct.pack('<f', float(scalar_value))
        if kind == PbScalarKind.DOUBLE:
            return _PROTOBUF_WIRE_64BIT, struct.pack('<d', float(scalar_value))
        if kind == PbScalarKind.STRING:
            data = scalar_value.encode('utf-8')
            return _PROTOBUF_WIRE_LEN_DELIM, _encode_protobuf_uvarint(len(data)) + data
        if kind == PbScalarKind.BYTES:
            data = bytes(scalar_value)
            return _PROTOBUF_WIRE_LEN_DELIM, _encode_protobuf_uvarint(len(data)) + data
        raise ValueError(f'Unsupported scalar kind: {kind}')

    def _encode_scalar_field(self, field_spec, value):
        if value is _PB_MISSING or value is None:
            if field_spec.cardinality == PbCardinality.REQUIRED:
                raise ValueError(f'required field {field_spec.name} is missing')
            return b''
        if field_spec.cardinality == PbCardinality.REPEATED:
            if not isinstance(value, (list, tuple)):
                raise TypeError(f'repeated field {field_spec.name} expects a list/tuple, got {type(value)}')
            if field_spec.packed:
                payloads = []
                for element in value:
                    if element is None:
                        raise ValueError(f'repeated field {field_spec.name} cannot contain null elements')
                    _, payload = self._encode_scalar_payload(field_spec, element)
                    payloads.append(payload)
                packed_data = b''.join(payloads)
                if not packed_data:
                    return b''
                return (_encode_protobuf_key(field_spec.number, _PROTOBUF_WIRE_LEN_DELIM) +
                        _encode_protobuf_uvarint(len(packed_data)) + packed_data)
            parts = []
            for element in value:
                if element is None:
                    raise ValueError(f'repeated field {field_spec.name} cannot contain null elements')
                wire_type, payload = self._encode_scalar_payload(field_spec, element)
                parts.append(_encode_protobuf_key(field_spec.number, wire_type) + payload)
            return b''.join(parts)
        wire_type, payload = self._encode_scalar_payload(field_spec, value)
        return _encode_protobuf_key(field_spec.number, wire_type) + payload

    def _encode_message_field(self, field_spec, value):
        if value is _PB_MISSING or value is None:
            if field_spec.cardinality == PbCardinality.REQUIRED:
                raise ValueError(f'required field {field_spec.name} is missing')
            return b''
        if field_spec.cardinality == PbCardinality.REPEATED:
            if not isinstance(value, (list, tuple)):
                raise TypeError(f'repeated message field {field_spec.name} expects a list/tuple, got {type(value)}')
            parts = []
            for element in value:
                if element is None:
                    raise ValueError(f'repeated message field {field_spec.name} cannot contain null elements')
                child_encoded = self._encode_message_fields(field_spec.fields, element)
                parts.append(
                    _encode_protobuf_key(field_spec.number, _PROTOBUF_WIRE_LEN_DELIM) +
                    _encode_protobuf_uvarint(len(child_encoded)) + child_encoded)
            return b''.join(parts)
        child_encoded = self._encode_message_fields(field_spec.fields, value)
        return (_encode_protobuf_key(field_spec.number, _PROTOBUF_WIRE_LEN_DELIM) +
                _encode_protobuf_uvarint(len(child_encoded)) + child_encoded)


class ProtobufValueGenerator:
    def __init__(self, rand):
        self._rand = rand

    def generate_message(self, message_spec, force_present=False):
        fields = message_spec.fields if isinstance(message_spec, PbMessageSpec) else tuple(message_spec)
        result = {}
        for field_spec in fields:
            value = self.generate_field(field_spec)
            if field_spec.cardinality == PbCardinality.REPEATED:
                if value:
                    result[field_spec.name] = value
            else:
                if value is not None:
                    result[field_spec.name] = value
        if result or force_present:
            return result
        return None

    def generate_field(self, field_spec):
        if isinstance(field_spec, PbScalarFieldSpec):
            return self._generate_scalar_field(field_spec)
        return self._generate_message_field(field_spec)

    def _next_scalar(self, field_spec, force_no_nulls=False):
        if field_spec.gen is None:
            raise ValueError(f'protobuf random generation requires a DataGen for field {field_spec.name}')
        return field_spec.gen.gen(force_no_nulls=force_no_nulls)

    def _generate_scalar_field(self, field_spec):
        if field_spec.cardinality == PbCardinality.REPEATED:
            length = self._rand.randint(field_spec.min_len, field_spec.max_len)
            return [self._next_scalar(field_spec, force_no_nulls=True) for _ in range(length)]
        if field_spec.cardinality == PbCardinality.REQUIRED:
            return self._next_scalar(field_spec, force_no_nulls=True)
        return self._next_scalar(field_spec)

    def _generate_message_field(self, field_spec):
        if field_spec.cardinality == PbCardinality.REPEATED:
            length = self._rand.randint(field_spec.min_len, field_spec.max_len)
            return [self.generate_message(field_spec.fields, force_present=True) for _ in range(length)]
        child_value = self.generate_message(
            field_spec.fields,
            force_present=(field_spec.cardinality == PbCardinality.REQUIRED))
        if field_spec.cardinality == PbCardinality.REQUIRED and child_value is None:
            return {}
        return child_value


class ProtobufSparkAdapter:
    def __init__(self, enums_as_ints=True):
        self._enums_as_ints = bool(enums_as_ints)

    def field_spark_field(self, field_spec):
        if isinstance(field_spec, PbScalarFieldSpec):
            if field_spec.kind == PbScalarKind.ENUM and not self._enums_as_ints:
                data_type = StringType()
            else:
                data_type = _pb_scalar_kind_spark_type(field_spec.kind)
            if field_spec.cardinality == PbCardinality.REPEATED:
                return StructField(
                    field_spec.name,
                    ArrayType(data_type, containsNull=False),
                    nullable=True)
            nullable = field_spec.cardinality != PbCardinality.REQUIRED
            return StructField(field_spec.name, data_type, nullable=nullable)
        child_fields = [self.field_spark_field(child) for child in field_spec.fields]
        data_type = StructType(child_fields)
        if field_spec.cardinality == PbCardinality.REPEATED:
            return StructField(
                field_spec.name,
                ArrayType(data_type, containsNull=False),
                nullable=True)
        return StructField(field_spec.name, data_type, nullable=field_spec.cardinality != PbCardinality.REQUIRED)

    def message_tuple(self, fields, value):
        value = {} if value in (_PB_MISSING, None) else value
        if not isinstance(value, dict):
            raise TypeError(f'protobuf message values must be dicts, got {type(value)}')
        unknown = set(value.keys()) - {field.name for field in fields}
        if unknown:
            raise ValueError(f'unknown protobuf field(s): {sorted(unknown)}')
        return tuple(self.field_value(field, value.get(field.name, _PB_MISSING)) for field in fields)

    def field_value(self, field_spec, value):
        if isinstance(field_spec, PbScalarFieldSpec):
            return self._scalar_value(field_spec, value)
        return self._message_value(field_spec, value)

    def _scalar_single_value(self, field_spec, value):
        if value is None:
            return None
        if field_spec.kind == PbScalarKind.ENUM:
            number = field_spec.enum.number_for(value)
            return number if self._enums_as_ints else field_spec.enum.name_for(number)
        if field_spec.kind == PbScalarKind.BOOL:
            return bool(value)
        if field_spec.kind in {PbScalarKind.FLOAT, PbScalarKind.DOUBLE}:
            return float(value)
        if field_spec.kind == PbScalarKind.STRING:
            return str(value)
        if field_spec.kind == PbScalarKind.BYTES:
            return value if isinstance(value, bytes) else bytes(value)
        if field_spec.kind in {PbScalarKind.UINT32, PbScalarKind.FIXED32}:
            return _unsigned_spark_value(value, 32, field_spec.name)
        if field_spec.kind in {PbScalarKind.UINT64, PbScalarKind.FIXED64}:
            return _unsigned_spark_value(value, 64, field_spec.name)
        return int(value)

    def _scalar_value(self, field_spec, value):
        if field_spec.cardinality == PbCardinality.REPEATED:
            if value in (_PB_MISSING, None):
                return []
            if not isinstance(value, (list, tuple)):
                raise TypeError(f'repeated field {field_spec.name} expects a list/tuple, got {type(value)}')
            return [self._scalar_single_value(field_spec, element) for element in value]
        if value in (_PB_MISSING, None):
            if field_spec.default is not None:
                if (field_spec.kind == PbScalarKind.ENUM and
                        not self._enums_as_ints and
                        isinstance(field_spec.default, str)):
                    return field_spec.default
                return self._scalar_single_value(field_spec, field_spec.default)
            return None
        return self._scalar_single_value(field_spec, value)

    def _message_value(self, field_spec, value):
        if field_spec.cardinality == PbCardinality.REPEATED:
            if value in (_PB_MISSING, None):
                return []
            if not isinstance(value, (list, tuple)):
                raise TypeError(f'repeated message field {field_spec.name} expects a list/tuple, got {type(value)}')
            return [self.message_tuple(field_spec.fields, element) for element in value]
        if value in (_PB_MISSING, None):
            return None
        return self.message_tuple(field_spec.fields, value)


class ProtobufRowGen(DataGen):
    """Generate Spark rows from a protobuf message specification."""
    def __init__(
            self, message_spec, binary_col_name='bin', nullable=False,
            enums_as_ints=True):
        if binary_col_name is not None:
            binary_col_name = str(binary_col_name)
            if binary_col_name.casefold() in {
                    field_spec.name.casefold() for field_spec in message_spec.fields}:
                raise ValueError(
                    f'binary column name conflicts with protobuf field: {binary_col_name}')
        self._message_spec = message_spec
        self._binary_col_name = binary_col_name
        self._enums_as_ints = bool(enums_as_ints)
        self._adapter = ProtobufSparkAdapter(enums_as_ints=self._enums_as_ints)
        struct_fields = [self._adapter.field_spark_field(field_spec)
                         for field_spec in message_spec.fields]
        if binary_col_name is not None:
            struct_fields.append(StructField(binary_col_name, BinaryType(), nullable=True))
        super().__init__(StructType(struct_fields), nullable=nullable)

    def __repr__(self):
        return f'ProtobufRowGen({self._message_spec.name})'

    def _cache_repr(self):
        return (super()._cache_repr() + '(' + _pb_message_cache_repr(self._message_spec) +
                ',' + str(self._binary_col_name) + ',' + str(self._enums_as_ints) + ')')

    def __eq__(self, other):
        return isinstance(other, ProtobufRowGen) and self._cache_repr() == other._cache_repr()

    def __hash__(self):
        return hash(self._cache_repr())

    def _start_field_gens(self, fields, rand):
        for field_spec in fields:
            if isinstance(field_spec, PbScalarFieldSpec):
                if field_spec.gen is not None:
                    field_spec.gen.start(rand)
            else:
                self._start_field_gens(field_spec.fields, rand)

    def start(self, rand):
        self._start_field_gens(self._message_spec.fields, rand)
        value_gen = ProtobufValueGenerator(rand)
        encoder = ProtobufEncoder() if self._binary_col_name is not None else None

        def make_row():
            message_value = value_gen.generate_message(self._message_spec, force_present=True)
            logical_values = self._adapter.message_tuple(self._message_spec.fields, message_value)
            if encoder is None:
                return logical_values
            return logical_values + (encoder.encode_message(self._message_spec, message_value),)

        self._start(rand, make_row)


def encode_pb_message(message_spec, value):
    """Encode a logical protobuf message dict using a `PbMessageSpec`."""
    return ProtobufEncoder().encode_message(message_spec, value)
