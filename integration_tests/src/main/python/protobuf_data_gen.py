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

from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
)

from data_gen import ArrayGen, DataGen, StructGen

# -----------------------------------------------------------------------------
# Protobuf schema-first test modeling and generation
# -----------------------------------------------------------------------------

_PROTOBUF_MAX_FIELD_NUMBER = (1 << 29) - 1
_PROTOBUF_RESERVED_FIELD_START = 19000
_PROTOBUF_RESERVED_FIELD_END = 19999


def _validate_protobuf_field_number(number):
    if (number <= 0 or number > _PROTOBUF_MAX_FIELD_NUMBER or
            _PROTOBUF_RESERVED_FIELD_START <= number <= _PROTOBUF_RESERVED_FIELD_END):
        raise ValueError(f'invalid protobuf field number: {number}')


def _validate_signed_default_range(field_spec, bits):
    value = field_spec.default
    signed_min = -(1 << (bits - 1))
    signed_max = (1 << (bits - 1)) - 1
    if value < signed_min or value > signed_max:
        raise ValueError(
            f'{field_spec.kind.value} default is out of range for '
            f'{field_spec.name}: {value}')


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
        return StringType()
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
        _validate_signed_default_range(field_spec, 32)
        return
    if kind in {
            PbScalarKind.INT64, PbScalarKind.SINT64, PbScalarKind.SFIXED64}:
        if isinstance(default, bool) or not isinstance(default, int):
            raise TypeError(
                f'{kind.value} field {field_spec.name} requires an int default')
        _validate_signed_default_range(field_spec, 64)
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
            _validate_signed_default_range(field_spec, 32)
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
            if not isinstance(self.gen.data_type, type(expected_type)):
                raise TypeError(
                    f'{self.kind.value} field {self.name} requires '
                    f'{type(expected_type).__name__}, '
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

    def as_logical_datagen(self):
        return StructGen(
            [(field.name, _logical_field_datagen(field)) for field in self.fields],
            nullable=False)

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


def _logical_field_datagen(field_spec):
    if isinstance(field_spec, PbScalarFieldSpec):
        gen = field_spec.gen
        if gen is None:
            raise ValueError(
                f'protobuf logical generation requires a DataGen for {field_spec.name}')
        if field_spec.kind == PbScalarKind.ENUM and not isinstance(gen.data_type, StringType):
            raise ValueError(
                f'enum field {field_spec.name} requires a StringType DataGen')
        if (field_spec.cardinality in {PbCardinality.REQUIRED, PbCardinality.REPEATED}
                and gen.nullable):
            raise ValueError(
                f'{field_spec.cardinality.value} field {field_spec.name} requires '
                'a non-nullable DataGen')
        if field_spec.cardinality == PbCardinality.REPEATED:
            return ArrayGen(
                gen,
                min_length=field_spec.min_len,
                max_length=field_spec.max_len,
                nullable=False)
        return gen

    children = [
        (child.name, _logical_field_datagen(child)) for child in field_spec.fields]
    if field_spec.cardinality == PbCardinality.REPEATED:
        return ArrayGen(
            StructGen(children, nullable=False),
            min_length=field_spec.min_len,
            max_length=field_spec.max_len,
            nullable=False)
    return StructGen(
        children,
        nullable=field_spec.cardinality == PbCardinality.OPTIONAL)


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
