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

from data_gen import (
    BinaryGen,
    BooleanGen,
    DataGen,
    DoubleGen,
    FloatGen,
    IntegerGen,
    LongGen,
    SetValuesGen,
    StringGen,
)

__all__ = ["pb", "encode_pb_message"]


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


def _parse_protobuf_integer(value):
    sign = -1 if value.startswith('-') else 1
    digits = value[1:] if value[:1] in ('-', '+') else value
    if digits.lower().startswith('0x'):
        base = 16
        digits = digits[2:]
    elif len(digits) > 1 and digits.startswith('0'):
        base = 8
    else:
        base = 10
    return sign * int(digits, base)


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


def _pb_scalar_kind_spark_type(kind, enum_as_ints=True):
    if kind in {PbScalarKind.BOOL}:
        return BooleanType()
    if kind == PbScalarKind.ENUM:
        return IntegerType() if enum_as_ints else StringType()
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


@dataclass(frozen=True)
class PbEnumSpec:
    name: str
    values: tuple
    allow_alias: bool = False

    def __post_init__(self):
        values = tuple((str(name), int(number)) for name, number in self.values)
        if not values:
            raise ValueError('enum spec must contain at least one value')
        names = [name for name, _ in values]
        numbers = [number for _, number in values]
        if len(names) != len(set(names)):
            raise ValueError(f'duplicate enum names in {self.name}')
        if not self.allow_alias and len(numbers) != len(set(numbers)):
            raise ValueError(f'duplicate enum numbers in {self.name}')
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
    enum_as_ints: bool = True

    def __post_init__(self):
        object.__setattr__(self, 'name', str(self.name))
        object.__setattr__(self, 'number', int(self.number))
        _validate_protobuf_field_number(self.number)
        if self.cardinality != PbCardinality.REPEATED and self.packed:
            raise ValueError(f'packed encoding requires repeated cardinality: {self.name}')
        if self.cardinality == PbCardinality.REPEATED and self.default is not None:
            raise ValueError(f'repeated fields cannot have defaults: {self.name}')
        if self.min_len < 0 or self.max_len < self.min_len:
            raise ValueError(f'invalid repeated length bounds for {self.name}')
        if self.kind == PbScalarKind.ENUM:
            if self.enum is None:
                raise ValueError(f'enum field requires enum spec: {self.name}')
        elif self.enum is not None or not self.enum_as_ints:
            raise ValueError(f'non-enum field cannot carry enum settings: {self.name}')
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
    presence_gen: object = None

    def __post_init__(self):
        object.__setattr__(self, 'name', str(self.name))
        object.__setattr__(self, 'number', int(self.number))
        object.__setattr__(self, 'fields', tuple(self.fields))
        _validate_protobuf_field_number(self.number)
        if self.min_len < 0 or self.max_len < self.min_len:
            raise ValueError(f'invalid repeated length bounds for {self.name}')
        if self.cardinality == PbCardinality.REQUIRED and self.min_len != 0:
            raise ValueError('required message field cannot define repeated bounds')
        if self.presence_gen is not None:
            if self.cardinality != PbCardinality.OPTIONAL:
                raise ValueError(
                    f'message presence only applies to optional fields: {self.name}')
            if not isinstance(self.presence_gen, (bool, DataGen)):
                raise TypeError(
                    f'message presence requires a bool or DataGen: {self.name}')
            if (isinstance(self.presence_gen, DataGen) and
                    self.presence_gen.data_type != BooleanType()):
                raise TypeError(
                    f'message presence DataGen must produce booleans: {self.name}')
            if isinstance(self.presence_gen, DataGen) and self.presence_gen.nullable:
                raise ValueError(
                    f'message presence DataGen must be non-nullable: {self.name}')
        _validate_pb_fields(self.fields, self.name)


@dataclass(frozen=True)
class PbMessageSpec:
    name: str
    fields: tuple

    def __post_init__(self):
        object.__setattr__(self, 'name', str(self.name))
        object.__setattr__(self, 'fields', tuple(self.fields))
        _validate_pb_fields(self.fields, self.name)

    def as_datagen(self, binary_col_name='bin'):
        return ProtobufRowGen(self, binary_col_name=binary_col_name)

    def encode(self, value):
        return encode_pb_message(self, value)


def _validate_pb_fields(fields, owner_name):
    names = [field.name for field in fields]
    numbers = [field.number for field in fields]
    if len(names) != len(set(names)):
        raise ValueError(f'duplicate field names in {owner_name}')
    if len(numbers) != len(set(numbers)):
        raise ValueError(f'duplicate field numbers in {owner_name}')


def _normalize_repeated_length(bounds, description):
    if type(bounds) is int:
        min_len = max_len = bounds
    elif (isinstance(bounds, (tuple, list)) and len(bounds) == 2 and
          all(type(bound) is int for bound in bounds)):
        min_len, max_len = bounds
    else:
        raise TypeError(
            f'{description} requires an int or (min, max) pair')
    if min_len < 0 or max_len < min_len:
        raise ValueError(f'invalid {description}: {bounds}')
    return min_len, max_len


class _PbGenerationPolicy:
    def __init__(self, value_gens=None, message_presence=None, repeated_lengths=None,
                 default_repeated_length=None, enums_as_ints=False):
        self.value_gens = dict(value_gens or {})
        self.message_presence = dict(message_presence or {})
        self.repeated_lengths = dict(repeated_lengths or {})
        self.default_repeated_length = (
            None if default_repeated_length is None else
            _normalize_repeated_length(
                default_repeated_length, 'default repeated length'))
        if type(enums_as_ints) is not bool:
            raise TypeError('enums_as_ints must be a bool')
        self.enums_as_ints = enums_as_ints


class _PbBuilder:
    def message(self, name, fields):
        return PbMessageSpec(name, tuple(fields))

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
        raise TypeError(f'Unsupported protobuf field for repeated(): {type(field_spec)}')

    def repeated_message(self, name, number, fields, min_len=0, max_len=5):
        return self.repeated(self.message_field(name, number, fields), min_len=min_len, max_len=max_len)

    def required(self, field_spec):
        if isinstance(field_spec, PbScalarFieldSpec):
            if field_spec.default is not None:
                raise ValueError('required fields cannot have defaults')
            return replace(field_spec, cardinality=PbCardinality.REQUIRED)
        if isinstance(field_spec, PbMessageFieldSpec):
            return replace(field_spec, cardinality=PbCardinality.REQUIRED)
        raise TypeError(f'Unsupported protobuf field for required(): {type(field_spec)}')

    def generation(self, value_gens=None, message_presence=None, repeated_lengths=None,
                   default_repeated_length=None, enums_as_ints=False):
        return _PbGenerationPolicy(
            value_gens=value_gens,
            message_presence=message_presence,
            repeated_lengths=repeated_lengths,
            default_repeated_length=default_repeated_length,
            enums_as_ints=enums_as_ints)

    def from_descriptor(self, spark, descriptor_bytes, message_name, generation=None):
        return _SparkProtobufDescriptorAdapter(
            spark, descriptor_bytes, generation=generation).message(message_name)


pb = _PbBuilder()


_DESCRIPTOR_SCALAR_KINDS = {
    'TYPE_BOOL': PbScalarKind.BOOL,
    'TYPE_INT32': PbScalarKind.INT32,
    'TYPE_INT64': PbScalarKind.INT64,
    'TYPE_UINT32': PbScalarKind.UINT32,
    'TYPE_UINT64': PbScalarKind.UINT64,
    'TYPE_SINT32': PbScalarKind.SINT32,
    'TYPE_SINT64': PbScalarKind.SINT64,
    'TYPE_FIXED32': PbScalarKind.FIXED32,
    'TYPE_FIXED64': PbScalarKind.FIXED64,
    'TYPE_SFIXED32': PbScalarKind.SFIXED32,
    'TYPE_SFIXED64': PbScalarKind.SFIXED64,
    'TYPE_FLOAT': PbScalarKind.FLOAT,
    'TYPE_DOUBLE': PbScalarKind.DOUBLE,
    'TYPE_STRING': PbScalarKind.STRING,
    'TYPE_BYTES': PbScalarKind.BYTES,
}


class _SparkProtobufDescriptorAdapter:
    def __init__(self, spark, descriptor_bytes, generation=None):
        if generation is not None and not isinstance(generation, _PbGenerationPolicy):
            raise TypeError(f'generation must be created by pb.generation(), got {type(generation)}')
        self._generation = generation or _PbGenerationPolicy()
        self._used_value_gen_paths = set()
        self._used_message_presence_paths = set()
        self._used_repeated_length_paths = set()
        descriptor_protos = (
            spark.sparkContext._jvm.org.sparkproject.spark_protobuf.protobuf.DescriptorProtos)
        descriptor_set = descriptor_protos.FileDescriptorSet.parseFrom(
            bytearray(descriptor_bytes))
        self._messages = {}
        self._message_syntax = {}
        self._enums = {}
        self._enum_syntax = {}
        for file_index in range(descriptor_set.getFileCount()):
            file_proto = descriptor_set.getFile(file_index)
            syntax = file_proto.getSyntax() or 'proto2'
            package = file_proto.getPackage()
            for enum_index in range(file_proto.getEnumTypeCount()):
                self._index_enum(
                    package, (), file_proto.getEnumType(enum_index), syntax)
            for message_index in range(file_proto.getMessageTypeCount()):
                message_proto = file_proto.getMessageType(message_index)
                self._index_message(package, (), message_proto, syntax)

    def _index_enum(self, package, parents, enum_proto, syntax):
        full_name = '.'.join(
            part for part in (package,) + parents + (enum_proto.getName(),) if part)
        self._enums[full_name] = PbEnumSpec(
            full_name,
            tuple(
                (enum_proto.getValue(value_index).getName(),
                 enum_proto.getValue(value_index).getNumber())
                for value_index in range(enum_proto.getValueCount())),
            allow_alias=enum_proto.getOptions().getAllowAlias())
        self._enum_syntax[full_name] = syntax

    def _index_message(self, package, parents, message_proto, syntax):
        full_name = '.'.join(
            part for part in (package,) + parents + (message_proto.getName(),) if part)
        self._messages[full_name] = message_proto
        self._message_syntax[full_name] = syntax
        nested_parents = parents + (message_proto.getName(),)
        for enum_index in range(message_proto.getEnumTypeCount()):
            self._index_enum(
                package, nested_parents, message_proto.getEnumType(enum_index), syntax)
        for nested_index in range(message_proto.getNestedTypeCount()):
            self._index_message(
                package, nested_parents, message_proto.getNestedType(nested_index), syntax)

    def message(self, message_name):
        normalized_name = message_name.lstrip('.')
        if normalized_name not in self._messages:
            raise ValueError(f'protobuf message not found: {message_name}')
        fields = self._message_fields(normalized_name, (), ())
        self._validate_generation_paths(
            self._generation.value_gens, self._used_value_gen_paths)
        self._validate_generation_paths(
            self._generation.message_presence, self._used_message_presence_paths)
        self._validate_generation_paths(
            self._generation.repeated_lengths, self._used_repeated_length_paths)
        return PbMessageSpec(normalized_name, fields)

    @staticmethod
    def _validate_generation_paths(configured_paths, used_paths):
        unused_paths = set(configured_paths) - used_paths
        if unused_paths:
            raise ValueError(f'unknown protobuf generation path(s): {sorted(unused_paths)}')

    def _message_fields(self, message_name, ancestors, parent_path):
        if message_name in ancestors:
            raise ValueError(f'recursive protobuf messages are not supported: {message_name}')
        self._require_proto2(self._message_syntax[message_name], message_name)
        message_proto = self._messages[message_name]
        if message_proto.getOneofDeclCount() > 0:
            raise ValueError(f'oneof generation is not supported: {message_name}')
        fields = [
            self._field(
                message_proto.getField(field_index),
                ancestors + (message_name,),
                parent_path + (message_proto.getField(field_index).getName(),))
            for field_index in range(message_proto.getFieldCount())
        ]
        return fields

    def _field(self, field_proto, ancestors, field_path):
        field_type = str(field_proto.getType())
        if field_type == 'TYPE_MESSAGE':
            message_name = field_proto.getTypeName().lstrip('.')
            if message_name not in self._messages:
                raise ValueError(f'protobuf message not found: {field_proto.getTypeName()}')
            if self._messages[message_name].getOptions().getMapEntry():
                raise ValueError(
                    f'protobuf map fields are not supported: {".".join(field_path)}')
            field_spec = PbMessageFieldSpec(
                name=field_proto.getName(),
                number=field_proto.getNumber(),
                fields=self._message_fields(message_name, ancestors, field_path))
            field_spec = self._apply_cardinality(field_spec, field_proto)
            field_spec = self._apply_repeated_length(field_spec, field_path)
            return self._apply_message_presence(field_spec, field_path)
        enum_spec = None
        if field_type == 'TYPE_ENUM':
            enum_name = field_proto.getTypeName().lstrip('.')
            if enum_name not in self._enums:
                raise ValueError(f'protobuf enum not found: {field_proto.getTypeName()}')
            self._require_proto2(self._enum_syntax[enum_name], enum_name)
            kind = PbScalarKind.ENUM
            enum_spec = self._enums[enum_name]
        elif field_type in _DESCRIPTOR_SCALAR_KINDS:
            kind = _DESCRIPTOR_SCALAR_KINDS[field_type]
        else:
            raise ValueError(f'unsupported protobuf field type: {field_type}')
        field_spec = PbScalarFieldSpec(
            name=field_proto.getName(),
            number=field_proto.getNumber(),
            kind=kind,
            gen=self._value_gen(field_path, field_proto, kind, enum_spec),
            default=self._default_value(field_proto, kind),
            enum=enum_spec,
            enum_as_ints=self._generation.enums_as_ints if enum_spec else True)
        field_spec = self._apply_cardinality(field_spec, field_proto)
        return self._apply_repeated_length(field_spec, field_path)

    @staticmethod
    def _require_proto2(syntax, type_name):
        if syntax != 'proto2':
            raise ValueError(
                f'unsupported {syntax} protobuf syntax for type {type_name}')

    @staticmethod
    def _default_value(field_proto, kind):
        if not field_proto.hasDefaultValue():
            return None
        value = field_proto.getDefaultValue()
        if kind == PbScalarKind.BOOL:
            if value not in ('true', 'false'):
                raise ValueError(f'invalid protobuf boolean default: {value}')
            return value == 'true'
        if kind in {
                PbScalarKind.INT32, PbScalarKind.INT64, PbScalarKind.UINT32,
                PbScalarKind.UINT64, PbScalarKind.SINT32, PbScalarKind.SINT64,
                PbScalarKind.FIXED32, PbScalarKind.FIXED64, PbScalarKind.SFIXED32,
                PbScalarKind.SFIXED64}:
            return _parse_protobuf_integer(value)
        if kind in {PbScalarKind.FLOAT, PbScalarKind.DOUBLE}:
            return float(value)
        if kind in {PbScalarKind.STRING, PbScalarKind.ENUM}:
            return value
        raise ValueError(f'unsupported protobuf default for {kind.value}')

    def _value_gen(self, field_path, field_proto, kind, enum_spec):
        path = '.'.join(field_path)
        if path in self._generation.value_gens:
            self._used_value_gen_paths.add(path)
            gen = self._generation.value_gens[path]
            expected_type = _pb_scalar_kind_spark_type(
                kind, enum_as_ints=self._generation.enums_as_ints)
            if not isinstance(gen, DataGen):
                raise TypeError(f'value generator for {path} must be a DataGen')
            if gen.data_type != expected_type:
                raise TypeError(
                    f'value generator for {path} must produce {expected_type}, '
                    f'got {gen.data_type}')
            if (str(field_proto.getLabel()) != 'LABEL_OPTIONAL' and
                    gen.nullable):
                raise ValueError(
                    f'value generator for {path} must be non-nullable for '
                    'required or repeated fields')
            return gen
        nullable = str(field_proto.getLabel()) == 'LABEL_OPTIONAL'
        if kind == PbScalarKind.BOOL:
            return BooleanGen(nullable=nullable)
        if kind in {
                PbScalarKind.INT32, PbScalarKind.SINT32, PbScalarKind.SFIXED32}:
            return IntegerGen(nullable=nullable)
        if kind in {PbScalarKind.UINT32, PbScalarKind.FIXED32}:
            return IntegerGen(nullable=nullable)
        if kind in {
                PbScalarKind.INT64, PbScalarKind.SINT64, PbScalarKind.SFIXED64}:
            return LongGen(nullable=nullable)
        if kind in {PbScalarKind.UINT64, PbScalarKind.FIXED64}:
            return LongGen(nullable=nullable)
        if kind == PbScalarKind.FLOAT:
            return FloatGen(nullable=nullable)
        if kind == PbScalarKind.DOUBLE:
            return DoubleGen(nullable=nullable)
        if kind == PbScalarKind.STRING:
            return StringGen(nullable=nullable)
        if kind == PbScalarKind.BYTES:
            return BinaryGen(nullable=nullable)
        if kind == PbScalarKind.ENUM:
            if self._generation.enums_as_ints:
                values = list(dict.fromkeys(number for _, number in enum_spec.values))
                data_type = IntegerType()
            else:
                values = list(dict.fromkeys(
                    enum_spec.name_for(number) for _, number in enum_spec.values))
                data_type = StringType()
            if nullable:
                values.append(None)
            return SetValuesGen(data_type, values)
        raise ValueError(f'unsupported protobuf generation kind: {kind.value}')

    def _apply_message_presence(self, field_spec, field_path):
        path = '.'.join(field_path)
        if path not in self._generation.message_presence:
            return field_spec
        self._used_message_presence_paths.add(path)
        return replace(
            field_spec,
            presence_gen=self._generation.message_presence[path])

    def _apply_repeated_length(self, field_spec, field_path):
        path = '.'.join(field_path)
        if path in self._generation.repeated_lengths:
            self._used_repeated_length_paths.add(path)
            bounds = self._generation.repeated_lengths[path]
        elif (field_spec.cardinality == PbCardinality.REPEATED and
              self._generation.default_repeated_length is not None):
            bounds = self._generation.default_repeated_length
        else:
            return field_spec
        if field_spec.cardinality != PbCardinality.REPEATED:
            raise ValueError(f'repeated length requires a repeated field: {path}')
        min_len, max_len = _normalize_repeated_length(
            bounds, f'repeated length for {path}')
        return replace(field_spec, min_len=min_len, max_len=max_len)

    def _apply_cardinality(self, field_spec, field_proto):
        label = str(field_proto.getLabel())
        if label == 'LABEL_REPEATED':
            updates = {'cardinality': PbCardinality.REPEATED}
            if isinstance(field_spec, PbScalarFieldSpec):
                updates['packed'] = field_proto.getOptions().getPacked()
            return replace(field_spec, **updates)
        if label == 'LABEL_REQUIRED':
            return replace(field_spec, cardinality=PbCardinality.REQUIRED)
        return field_spec


def _pb_gen_cache_repr(gen):
    return 'None' if gen is None else gen._cache_repr()


def _pb_presence_cache_repr(presence_gen):
    if isinstance(presence_gen, DataGen):
        return presence_gen._cache_repr()
    return str(presence_gen)


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
                str(field_spec.enum_as_ints) + ',' +
                _pb_gen_cache_repr(field_spec.gen) + ')')
    children = ','.join(_pb_field_cache_repr(child) for child in field_spec.fields)
    presence = ('' if field_spec.presence_gen is None else
                ',Presence(' + _pb_presence_cache_repr(field_spec.presence_gen) + ')')
    return ('Message(' + field_spec.name + ',' + str(field_spec.number) + ',' +
            field_spec.cardinality.value + ',' + str(field_spec.min_len) + ',' +
            str(field_spec.max_len) + presence + ',[' + children + '])')


def _pb_message_cache_repr(message_spec):
    children = ','.join(_pb_field_cache_repr(field_spec) for field_spec in message_spec.fields)
    return 'PbMessage(' + message_spec.name + ',[' + children + '])'


class ProtobufEncoder:
    def encode_message(self, message_spec, value):
        if value is None:
            return b''
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
            u64 = int(scalar_value) & 0xFFFFFFFFFFFFFFFF
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
            return _PROTOBUF_WIRE_VARINT, _encode_protobuf_uvarint(_encode_protobuf_zigzag32(scalar_value))
        if kind == PbScalarKind.SINT64:
            return _PROTOBUF_WIRE_VARINT, _encode_protobuf_uvarint(_encode_protobuf_zigzag64(scalar_value))
        if kind == PbScalarKind.FIXED32:
            return (_PROTOBUF_WIRE_32BIT,
                    struct.pack(
                        '<I', _unsigned_wire_value(scalar_value, 32, field_spec.name)))
        if kind == PbScalarKind.FIXED64:
            return (_PROTOBUF_WIRE_64BIT,
                    struct.pack(
                        '<Q', _unsigned_wire_value(scalar_value, 64, field_spec.name)))
        if kind == PbScalarKind.SFIXED32:
            return _PROTOBUF_WIRE_32BIT, struct.pack('<I', int(scalar_value) & 0xFFFFFFFF)
        if kind == PbScalarKind.SFIXED64:
            return _PROTOBUF_WIRE_64BIT, struct.pack('<Q', int(scalar_value) & 0xFFFFFFFFFFFFFFFF)
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
        if field_spec.presence_gen is not None:
            is_present = (field_spec.presence_gen if isinstance(field_spec.presence_gen, bool)
                          else field_spec.presence_gen.gen())
            if not is_present:
                return None
            return self.generate_message(field_spec.fields, force_present=True)
        child_value = self.generate_message(
            field_spec.fields,
            force_present=(field_spec.cardinality == PbCardinality.REQUIRED))
        if field_spec.cardinality == PbCardinality.REQUIRED and child_value is None:
            return {}
        return child_value


class ProtobufSparkAdapter:
    def field_spark_field(self, field_spec):
        if isinstance(field_spec, PbScalarFieldSpec):
            data_type = _pb_scalar_kind_spark_type(
                field_spec.kind, enum_as_ints=field_spec.enum_as_ints)
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
            if field_spec.enum_as_ints:
                return field_spec.enum.number_for(value)
            if isinstance(value, str):
                field_spec.enum.number_for(value)
                return value
            return field_spec.enum.name_for(value)
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
    """Generate Spark rows with logical protobuf fields plus a serialized binary column."""
    def __init__(self, message_spec, binary_col_name='bin', nullable=False):
        binary_col_name = str(binary_col_name)
        if binary_col_name.casefold() in {
                field_spec.name.casefold() for field_spec in message_spec.fields}:
            raise ValueError(
                f'binary column name conflicts with protobuf field: {binary_col_name}')
        self._message_spec = message_spec
        self._binary_col_name = binary_col_name
        self._adapter = ProtobufSparkAdapter()
        struct_fields = [self._adapter.field_spark_field(field_spec)
                         for field_spec in message_spec.fields]
        struct_fields.append(StructField(binary_col_name, BinaryType(), nullable=True))
        super().__init__(StructType(struct_fields), nullable=nullable)

    def __repr__(self):
        return f'ProtobufRowGen({self._message_spec.name})'

    def _cache_repr(self):
        return (super()._cache_repr() + '(' + _pb_message_cache_repr(self._message_spec) +
                ',' + self._binary_col_name + ')')

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
                if isinstance(field_spec.presence_gen, DataGen):
                    field_spec.presence_gen.start(rand)
                self._start_field_gens(field_spec.fields, rand)

    def start(self, rand):
        self._start_field_gens(self._message_spec.fields, rand)
        value_gen = ProtobufValueGenerator(rand)
        encoder = ProtobufEncoder()

        def make_row():
            message_value = value_gen.generate_message(self._message_spec, force_present=True)
            logical_values = self._adapter.message_tuple(self._message_spec.fields, message_value)
            message_bytes = encoder.encode_message(self._message_spec, message_value)
            return logical_values + (message_bytes,)

        self._start(rand, make_row)


def encode_pb_message(message_spec, value):
    """Encode a logical protobuf message dict using a `PbMessageSpec`."""
    return ProtobufEncoder().encode_message(message_spec, value)
