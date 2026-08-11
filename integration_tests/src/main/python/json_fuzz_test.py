# Copyright (c) 2022, NVIDIA CORPORATION.
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

# A JSON generator built based on the context free grammar from https://www.json.org/json-en.html

import random
from marks import allow_non_gpu, fuzz_test
from typing import List
from data_gen import *
from asserts import assert_gpu_and_cpu_are_equal_collect
from marks import approximate_float

_name_gen = StringGen(pattern= "[a-zA-Z]{1,30}",nullable= False)
_name_gen.start(random.Random(0))

def gen_top_schema(depth):
    return gen_object_type(depth)

def gen_schema(depth):
    """
    Abstract data type of JSON schema
    type Schema = Object of Fields 
                | Array of Schema 
                | String
                | Number 
                | Bool
    type Fields = Field list
    type Field = {Name, Schema}
    type Name = String
    """
    if depth > 1 and random.randint(1, 100) < 90:
        return random.choice([gen_object_type, gen_array_type])(depth)
    else:
        return random.choice([gen_string_type, gen_number_type, gen_bool_type])()


def gen_object_type(depth):
    return StructType(gen_fields_type(depth-1))

def gen_array_type(depth):
    return ArrayType(gen_schema(depth-1))

def gen_fields_type(depth):
    length = random.randint(1, 5)
    return [gen_field_type(depth) for _ in range(length)]

def gen_field_type(depth):
    return StructField(gen_name(), gen_schema(depth-1))

def gen_string_type():
    return StringType()

def gen_number_type():
    return random.choice([IntegerType(), FloatType(), DoubleType()])

def gen_bool_type():
    return BooleanType()

def gen_name():
    return _name_gen.gen()

# This is just a simple prototype of JSON generator.
# You need to generate a JSON schema before using it.
#
# #Example
# ```python
# schema = gen_schema()
# with open("./temp.json", 'w') as f:
#     for t in gen_json(schema):
#         f.write(t)
# ```
# to generate a random JSON file.

def gen_json(schema: DataType):
    """
    JSON -> ELEMENT
    """
    lines = random.randint(0, 10)
    for _ in range(lines):
        for t in gen_element(schema):
            yield t
        yield '\n'

def gen_value(schema: DataType):
    """
    VALUE -> OBJECT 
           | ARRAY
           | STRING
           | NUMBER
           | BOOL
    """
    if isinstance(schema, StructType):
        for t in gen_object(schema):
            yield t 
    elif isinstance(schema, ArrayType):
        for t in gen_array(schema.elementType):
            yield t
    elif isinstance(schema, StringType):
        for t in gen_string():
            yield t
    elif isinstance(schema, BooleanType):
        for t in gen_bool():
            yield t 
    elif isinstance(schema, IntegerType):
        for t in gen_integer():
            yield t 
    elif isinstance(schema, (FloatType, DoubleType)):
        for t in gen_number():
            yield t
    else:
        raise Exception("not supported schema")

def gen_object(schema: StructType):
    """
    OBJECT -> '{' WHITESPACE '}' 
            | '{' MEMBERS '}'
    """
    yield "{"
    if len(schema) == 0:
        for t in gen_whitespace():
            yield t
    else:
        for t in gen_members(schema.fields):
            yield t
    yield "}"




def gen_members(schema: List[StructField]):
    """
    MEMBERS -> MEMBER 
             | MEMBER ',' MEMBERS
    """
    if len(schema) == 1:
        for t in gen_member(schema[0]):
            yield t
    else:
        for t in gen_member(schema[0]):
            yield t
        yield ","
        for t in gen_members(schema[1:]):
            yield t

def gen_member(schema: StructField):
    """
    MEMBER -> WHITESPACE STRING WHITESPACE ':' ELEMENT
    """
    for t in gen_whitespace():
        yield t

    yield '"' + schema.name + '"'

    for t in gen_whitespace():
        yield t

    yield ":"

    for t in gen_element(schema.dataType):
        yield t

def gen_array(schema: DataType):
    yield '['

    for t in random.choices([gen_whitespace(), gen_elements(schema)], [10, 90], k=1)[0]:
        yield t

    yield ']'

def gen_elements(schema: DataType):
    """
    ELEMENTS -> ELEMENT 
              | ELEMENT ',' ELEMENTS
    """
    for t in gen_element(schema):
        yield t 
    
    if random.randint(1, 100) < 80:
        yield ','
        for t in gen_elements(schema):
            yield t 

def gen_element(schema: DataType):
    """
    ELEMENT -> WHITESPACE VALUE WHITESPACE
    """
    for t in gen_whitespace():
        yield t
    for t in gen_value(schema):
        yield t
    for t in gen_whitespace():
        yield t


def gen_string():
    """
    STRING -> '"' CHARACTERS '"'
    """
    yield '"' 
    for t in gen_characters():
        yield t 
    yield '"'

def gen_characters():
    """
    CHARACTERS -> '' 
                | CHAR CHARACTERS
    """
    if random.randint(0,100) < 30:
        yield ''
    else:
        for t in gen_char():
            yield t
        for t in gen_characters():
            yield t

def gen_char():
    """
    CHAR -> 0x0020 .. 0x10ffff (exclude 0x0022 and 0x005c) 
          | '\\' ESCAPE
    """
    if random.randint(0, 99) < 80:
        unicode = random.randint(0x0020, 0x10ffff)
        while unicode == 0x22 or unicode == 0x5c:
            unicode = random.randint(0x0020, 0x10ffff)
        yield chr(unicode)
    
    else:
        yield '\\'
        for t in gen_escape():
            yield t
    

def gen_escape():
    """
    ESCAPE -> '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' 
            | 'u' HEX HEX HEX HEX
    """
    if random.randint(0, 8) < 8:
        yield random.choice(['"', '\\', '/', 'b', 'f', 'n', 'r', 't'])
    else:
        yield 'u'
        for _ in range(4):
            for t in gen_hex():
                yield t

def gen_hex():
    """
    HEX -> DIGIT 
         | 'a' .. 'f' 
         | 'A' .. 'F'
    """
    path = random.randint(0, 2)
    if path == 0:
        for t in gen_digit():
            yield t
    elif path == 1:
        yield chr(random.randint(0x41, 0x46))
    else:
        yield chr(random.randint(0x61, 0x66))

def gen_number():
    """
    NUMBER -> INTEGER FRACTION EXPONENT
    """
    for t in gen_integer():
        yield t 
    for t in gen_fraction():
        yield t 
    for t in gen_exponent():
        yield t 

def gen_integer():
    """
    INTEGER -> DIGIT 
             | ONENINE DIGITS 
             | '-' DIGIT 
             | '-' ONENINE DIGITS
    """
    if random.randint(1, 100) <= 50:
        yield '-'
    
    if random.randint(1, 100) <= 50:
        for t in gen_digit():
            yield t 
    else:
        for t in gen_onenine():
            yield t 
        for t in gen_digits():
            yield t 

def gen_digits():
    """
    DIGITS -> DIGIT 
            | DIGIT DIGITS
    """
    for t in gen_digit():
        yield t 

    if random.randint(1, 100) < 70:
        for t in gen_digits():
            yield t 

def gen_digit():
    """
    DIGIT -> '0' 
           | ONENINE
    """
    if random.randint(0, 9) == 0:
        yield '0'
    else:
        for t in gen_onenine():
            yield t

def gen_onenine():
    """
    ONENINE -> '1' .. '9'
    """
    yield chr(random.randint(0x31, 0x39))

def gen_fraction():
    """
    FRACTION -> "" | '.' DIGITS
    """
    if random.randint(1, 100) < 50:
        yield ""
    else:
        yield '.'
        for t in gen_digits():
            yield t 

def gen_exponent():
    """
    EXPONENT -> "" 
              | 'E' SIGN DIGITS 
              | 'e' SIGN DIGITS
    """
    if random.randint(1, 100) < 20:
        yield ""
    else:
        yield random.choice(['E', 'e'])
        for t in gen_sign():
            yield t 
        for t in gen_digits():
            yield t 

def gen_sign():
    """
    SIGN -> "" 
          | '+' 
          | '-'
    """
    yield random.choice(["", '+', '-'])

def gen_whitespace():
    """
    WHITESPACE -> '' 
                | 0x0020 WHITESPACE 
                | 0x000a WHITESPACE (todo)
                | 0x000d WHITESPACE (todo)
                | 0x0009 WHITESPACE (todo)
    """
    if random.randint(0, 4) > 3:
        yield chr(random.choice([0x0020]))
        for t in gen_whitespace():
            yield t
    else:
        yield ''

def gen_bool():
    """
    BOOL -> "true" 
          | "null" 
          | "false"
    """
    yield random.choice(["true", "null", "false"])



_enable_all_types_conf = {
    'spark.rapids.sql.format.json.enabled': 'true',
    'spark.rapids.sql.format.json.read.enabled': 'true'}

@approximate_float
@allow_non_gpu('FileSourceScanExec')
@fuzz_test
def test_json_read_fuzz(enable_fuzz_test, spark_tmp_path):
    depth = random.randint(1, 5)
    schema = gen_top_schema(depth)

    data_path = spark_tmp_path + '/JSON_FUZZ_DATA'
    schema_path = spark_tmp_path + '/JSON_SCHEMA'

    # write the schema for debugging
    with open(schema_path, 'w') as f:
        f.write("{}".format(schema))
    
    with open(data_path, 'w') as f:
        for c in gen_json(schema):
            f.write(c)

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.read.schema(schema).json(data_path),
        _enable_all_types_conf
    )

