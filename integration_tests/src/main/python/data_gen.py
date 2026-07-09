# Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import copy
from datetime import date, datetime, timedelta, timezone
from decimal import *
from enum import Enum
import math
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as f
import random
from spark_session import is_before_spark_340, with_cpu_session, is_spark_341_or_later, is_spark_400_or_later
import sre_yield
import struct
from conftest import skip_unless_precommit_tests, get_datagen_seed, is_not_utc, is_supported_time_zone
import time
import os
from functools import lru_cache
import hashlib

class DataGen:
    """Base class for data generation"""

    def __repr__(self):
        if not self.nullable:
            return self.__class__.__name__[:-3] + '(not_null)'
        return self.__class__.__name__[:-3]

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._cache_repr() == other._cache_repr()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __init__(self, data_type, nullable=True, special_cases =[]):
        self.data_type = data_type
        self.list_of_special_cases = special_cases
        self._special_cases = []
        if isinstance(nullable, tuple):
            self.nullable = nullable[0]
            weight = nullable[1]
        else:
            self.nullable = nullable
            weight = 5.0
        if self.nullable:
            self.with_special_case(None, weight)

        # Special cases can be a value or a tuple of (value, weight). If the
        # special_case itself is a tuple as in the case of StructGen, it MUST be added with a
        # weight like : ((special_case_tuple_v1, special_case_tuple_v2), weight).
        for element in special_cases:
            if isinstance(element, tuple):
                self.with_special_case(element[0], element[1])
            else:
                self.with_special_case(element)

    def _cache_repr(self):
        # repr of DataGens and their children will be used to generate the cache key
        # make sure it is unique for different DataGens
        notnull = '(not_null)' if not self.nullable else ''
        datatype = str(self.data_type)
        specialcases = ''
        for (weight, case) in self._special_cases:
            if (callable(case)):
                case = case.__code__.co_code
            specialcases += str(case) + ', ' + str(weight) + ', '
        specialcases = hashlib.blake2b(specialcases.encode('utf-8'), digest_size=8).hexdigest()
        return self.__class__.__name__[:-3] + notnull + ', ' + datatype + ', ' + str(specialcases)

    def copy_special_case(self, special_case, weight=1.0):
        # it would be good to do a deepcopy, but sre_yield is not happy with that.
        c = copy.copy(self)
        c._special_cases = copy.deepcopy(self._special_cases)

        return c.with_special_case(special_case, weight=weight)

    def with_special_case(self, special_case, weight=1.0):
        """
        Add in a special case with a given weight. A special case can either be
        a function that takes an instance of Random and returns the generated data
        or it can be a constant.  By default the weight is 1.0, and the default
        number generation's weight is 100.0.  The number of lines that are generate in
        the data set should be proportional to the its weight/sum weights
        """
        if callable(special_case):
            sc = special_case
        else:
            sc = lambda rand: special_case
        self._special_cases.append((weight, sc))
        return self

    def get_types(self):
        return 'DataType: {}, nullable: {}, special_cases: {}'.format(self.data_type,
          self.nullable, self.list_of_special_cases)

    def start(self, rand):
        """Start data generation using the given rand"""
        raise TypeError('Children should implement this method and call _start')

    def _start(self, rand, gen_func):
        """Start internally, but use the given gen_func as the base"""
        if not self._special_cases:
            self._gen_func = gen_func
        else:
            weighted_choices = [(100.0, lambda rand: gen_func())]
            weighted_choices.extend(self._special_cases)
            total = float(sum(weight for weight,gen in weighted_choices))
            normalized_choices = [(weight/total, gen) for weight,gen in weighted_choices]

            def choose_one():
                pick = rand.random()
                total = 0
                for (weight, gen) in normalized_choices:
                    total += weight
                    if total >= pick:
                        return gen(rand)
                raise RuntimeError('Random did not pick something we expected')
            self._gen_func = choose_one

    def gen(self, force_no_nulls=False):
        """generate the next line"""
        if not self._gen_func:
            raise RuntimeError('start must be called before generating any data')
        v = self._gen_func()
        if force_no_nulls:
            while v is None:
                v = self._gen_func()
        return v

    def contains_ts(self):
        """Checks if this contains a TimestampGen"""
        return False

class ConvertGen(DataGen):
    """Provides a way to modify the data before it is returned"""
    def __init__(self, child_gen, func, data_type=None, nullable=True):
        if data_type is None:
            data_type = child_gen.data_type
        super().__init__(data_type, nullable=nullable)
        self._child_gen = child_gen
        self._func = func

    def __repr__(self):
        return super().__repr__() + '(' + str(self._child_gen) + ')'

    def _cache_repr(self):
        return (super()._cache_repr() + '(' + self._child_gen._cache_repr() +
                ',' + str(self._func.__code__) + ')' )

    def start(self, rand):
        self._child_gen.start(rand)
        def modify():
            return self._func(self._child_gen.gen())

        self._start(rand, modify)

_MAX_CHOICES = 1 << 64
class StringGen(DataGen):
    """Generate strings that match a pattern"""
    def __init__(self, pattern="(.|\n){1,30}", flags=0, charset=sre_yield.CHARSET, nullable=True, collation=None):
        self._collation = collation
        if is_spark_400_or_later():
            data_type = StringType(collation=collation) if collation is not None else StringType()
            super().__init__(data_type, nullable=nullable)
            self._is_error = False
        else:
            super().__init__(StringType(), nullable=nullable)
            # for Spark versions < 400, do not support collation
            self._is_error = collation is not None
        self.base_strs = sre_yield.AllStrings(pattern, flags=flags, charset=charset, max_count=_MAX_CHOICES)
        # save pattern and charset for cache repr
        charsetrepr = '[' + ','.join(charset) + ']' if charset != sre_yield.CHARSET else 'sre_yield.CHARSET'
        self.stringrepr = pattern + ',' + str(flags) + ',' + charsetrepr + ',' + str(collation)

    def __repr__(self):
        name = f"String(collation={self._collation})" if self._collation is not None else "String"
        if not self.nullable:
            return name + '(not_null)'
        return name

    def _cache_repr(self):
        return super()._cache_repr() + '(' + self.stringrepr + ')'

    def with_special_pattern(self, pattern, flags=0, charset=sre_yield.CHARSET, weight=1.0):
        """
        Like with_special_case but you can provide a regexp pattern
        instead of a hard coded string value.
        """
        strs = sre_yield.AllStrings(pattern, flags=flags, charset=charset, max_count=_MAX_CHOICES)
        length = strs.__len__()
        return self.with_special_case(lambda rand : strs[rand.randint(0, length-1)], weight=weight)

    def start(self, rand):
        if self._is_error:
            raise NotSupportedInSparkVersion("Collation is only supported on Spark 4.0.0+ in python. All tests that use this feature must have a skipif on them...")
        strs = self.base_strs
        length = strs.__len__()
        self._start(rand, lambda : strs[rand.randint(0, length-1)])

BYTE_MIN = -(1 << 7)
BYTE_MAX = (1 << 7) - 1
class ByteGen(DataGen):
    """Generate Bytes"""
    def __init__(self, nullable=True, min_val = BYTE_MIN, max_val = BYTE_MAX, special_cases=[]):
        super().__init__(ByteType(), nullable=nullable, special_cases=special_cases)
        self._min_val = min_val
        self._max_val = max_val

    def start(self, rand):
        self._start(rand, lambda : rand.randint(self._min_val, self._max_val))

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._min_val) + ',' + str(self._max_val) + ')'

SHORT_MIN = -(1 << 15)
SHORT_MAX = (1 << 15) - 1
class ShortGen(DataGen):
    """Generate Shorts, which some built in corner cases."""
    def __init__(self, nullable=True, min_val = SHORT_MIN, max_val = SHORT_MAX,
                 special_cases = [SHORT_MIN, SHORT_MAX, 0, 1, -1]):
        super().__init__(ShortType(), nullable=nullable, special_cases=special_cases)
        self._min_val = min_val
        self._max_val = max_val

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._min_val) + ',' + str(self._max_val) + ')'

    def start(self, rand):
        self._start(rand, lambda : rand.randint(self._min_val, self._max_val))

INT_MIN = -(1 << 31)
INT_MAX = (1 << 31) - 1
class IntegerGen(DataGen):
    """Generate Ints, which some built in corner cases."""
    def __init__(self, nullable=True, min_val = INT_MIN, max_val = INT_MAX,
                 special_cases = [INT_MIN, INT_MAX, 0, 1, -1]):
        super().__init__(IntegerType(), nullable=nullable, special_cases=special_cases)
        self._min_val = min_val
        self._max_val = max_val

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._min_val) + ',' + str(self._max_val) + ')'

    def start(self, rand):
        self._start(rand, lambda : rand.randint(self._min_val, self._max_val))

class DecimalGen(DataGen):
    """Generate Decimals, with some built in corner cases."""
    def __init__(self, precision=None, scale=None, nullable=True, special_cases=None, avoid_positive_values=False, full_precision=False):
        """full_precision: If True, generate decimals with full precision without leading and trailing zeros."""
        if precision is None:
            #Maximum number of decimal digits a Long can represent is 18
            precision = 18
            scale = 0
        DECIMAL_MIN = Decimal('-' + ('9' * precision) + 'e' + str(-scale))
        DECIMAL_MAX = Decimal(('9'* precision) + 'e' + str(-scale))
        if special_cases is None:
            special_cases = [DECIMAL_MIN, Decimal('0')]
            if not avoid_positive_values:
                special_cases.append(DECIMAL_MAX)
        super().__init__(DecimalType(precision, scale), nullable=nullable, special_cases=special_cases)
        self.scale = scale
        self.precision = precision
        self.avoid_positive_values = avoid_positive_values
        self.full_precision = full_precision

    def __repr__(self):
        return super().__repr__() + '(' + str(self.precision) + ',' + str(self.scale) + ')'

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self.precision) + ',' + str(self.scale) + ',' +\
              str(self.avoid_positive_values) + ',' + str(self.full_precision) + ')'

    def start(self, rand):
        def random_decimal(rand):
            if self.avoid_positive_values:
                sign = "-"
            else:
                sign = rand.choice(["-", ""])
            if self.full_precision:
                if self.precision == 1:
                    int_part = rand.choice("123456789")
                else:
                    int_part = rand.choice("123456789") + \
                        "".join([rand.choice("0123456789") for _ in range(self.precision - 2)]) + \
                        rand.choice("123456789")
            else:
                int_part = "".join([rand.choice("0123456789") for _ in range(self.precision)]) 
            result = f"{sign}{int_part}e{str(-self.scale)}" 
            return Decimal(result)

        self._start(rand, lambda : random_decimal(rand))

LONG_MIN = -(1 << 63)
LONG_MAX = (1 << 63) - 1
_MISSING_ARG = object()

class LongGen(DataGen):
    """Generate Longs, which some built in corner cases."""
    def __init__(self, nullable=True, min_val = LONG_MIN, max_val = LONG_MAX,
                 special_cases = _MISSING_ARG):
        _special_cases = [min_val, max_val, 0, 1, -1] if special_cases is _MISSING_ARG else special_cases
        super().__init__(LongType(), nullable=nullable, special_cases=_special_cases)
        self._min_val = min_val
        self._max_val = max_val

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._min_val) + ',' + str(self._max_val) + ')'

    def start(self, rand):
        self._start(rand, lambda : rand.randint(self._min_val, self._max_val))

class UniqueLongGen(DataGen):
    """Generates a sequence of longs with no repeating values except nulls."""
    def __init__(self, nullable=False):
        super().__init__(LongType(), nullable=nullable)
        self._current_val = 0

    def next_val(self):
        if self._current_val < 0:
            self._current_val = -self._current_val + 1
        else:
            self._current_val = -self._current_val - 1
        return self._current_val

    def _cache_repr(self):
        return super()._cache_repr()

    def start(self, rand):
        self._current_val = 0
        self._start(rand, lambda: self.next_val())

class RepeatSeqGen(DataGen):
    """Generate Repeated seq of `length` random items if child is a DataGen,
    otherwise repeat the provided seq when child is a list.

    When child is a list:
        data_type must be specified
        length must be <= length of child
    When child is a DataGen:
        length must be specified
        data_type must be None or match child's
    """
    def __init__(self, child, length=None, data_type=None):
        if isinstance(child, list):
            super().__init__(data_type, nullable=False)
            self.nullable = None in child
            assert (length is None or length < len(child))
            self._length = length if length is not None else len(child)
            self._child = child[:length] if length is not None else child
        else:
            super().__init__(child.data_type, nullable=False)
            self.nullable = child.nullable
            assert(data_type is None or data_type != child.data_type)
            assert(length is not None)
            self._length = length
            self._child = child
        self._vals = []
        self._index = 0

    def __repr__(self):
        return super().__repr__() + '(' + str(self._child) + ')'

    def _cache_repr(self):
        if isinstance(self._child, list):
            return super()._cache_repr() + '(' + str(self._child) + ',' + str(self._length) + ')'
        return super()._cache_repr() + '(' + self._child._cache_repr() + ',' + str(self._length) + ')'

    def _loop_values(self):
        ret = self._vals[self._index]
        self._index = (self._index + 1) % self._length
        return ret

    def start(self, rand):
        self._index = 0
        self._start(rand, self._loop_values)
        if isinstance(self._child, list):
            self._vals = self._child
        else:
            self._child.start(rand)
            self._vals = [self._child.gen() for _ in range(0, self._length)]

class SetValuesGen(DataGen):
    """A set of values that are randomly selected"""
    def __init__(self, data_type, data):
        super().__init__(data_type, nullable=False)
        self.nullable = any(x is None for x in data)
        self._vals = data

    def __repr__(self):
        return super().__repr__() +'(' + str(self.data_type) + ',' + str(self._vals) + ')'

    def _cache_repr(self):
        return super()._cache_repr() +'(' + str(self.data_type) + ',' + str(self._vals) + ')'

    def start(self, rand):
        data = self._vals
        length = len(data)
        self._start(rand, lambda : data[rand.randrange(0, length)])

FLOAT_MIN = -3.4028235E38
FLOAT_MAX = 3.4028235E38
NEG_FLOAT_NAN_MIN_VALUE = struct.unpack('f', struct.pack('I', 0xffffffff))[0]
NEG_FLOAT_NAN_MAX_VALUE = struct.unpack('f', struct.pack('I', 0xff800001))[0]
POS_FLOAT_NAN_MIN_VALUE = struct.unpack('f', struct.pack('I', 0x7f800001))[0]
POS_FLOAT_NAN_MAX_VALUE = struct.unpack('f', struct.pack('I', 0x7fffffff))[0]
class FloatGen(DataGen):
    """Generate floats, which some built in corner cases."""
    def __init__(self, nullable=True,
            no_nans=False, special_cases=None):
        self._no_nans = no_nans
        if special_cases is None:
            special_cases = [FLOAT_MIN, FLOAT_MAX, 0.0, -0.0, 1.0, -1.0]
            if not no_nans:
                special_cases.append(float('inf'))
                special_cases.append(float('-inf'))
                special_cases.append(float('nan'))
                special_cases.append(NEG_FLOAT_NAN_MAX_VALUE)
        super().__init__(FloatType(), nullable=nullable, special_cases=special_cases)

    def _fixup_nans(self, v):
        if self._no_nans and (math.isnan(v) or v == math.inf or v == -math.inf):
            v = None if self.nullable else 0.0
        return v
    
    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._no_nans) + ')'

    def start(self, rand):
        def gen_float():
            i = rand.randint(INT_MIN, INT_MAX)
            p = struct.pack('i', i)
            return self._fixup_nans(struct.unpack('f', p)[0])
        self._start(rand, gen_float)

DOUBLE_MIN_EXP = -1022
DOUBLE_MAX_EXP = 1023
DOUBLE_MAX_FRACTION = int('1'*52, 2)
DOUBLE_MIN = -1.7976931348623157E308
DOUBLE_MAX = 1.7976931348623157E308
NEG_DOUBLE_NAN_MIN_VALUE = struct.unpack('d', struct.pack('L', 0xffffffffffffffff))[0]
NEG_DOUBLE_NAN_MAX_VALUE = struct.unpack('d', struct.pack('L', 0xfff0000000000001))[0]
POS_DOUBLE_NAN_MIN_VALUE = struct.unpack('d', struct.pack('L', 0x7ff0000000000001))[0]
POS_DOUBLE_NAN_MAX_VALUE = struct.unpack('d', struct.pack('L', 0x7fffffffffffffff))[0]
class DoubleGen(DataGen):
    """Generate doubles, which some built in corner cases."""
    def __init__(self, min_exp=DOUBLE_MIN_EXP, max_exp=DOUBLE_MAX_EXP, no_nans=False,
            nullable=True, special_cases = None):
        self._min_exp = min_exp
        self._max_exp = max_exp
        self._no_nans = no_nans
        self._use_full_range = (self._min_exp == DOUBLE_MIN_EXP) and (self._max_exp == DOUBLE_MAX_EXP)
        if special_cases is None:
            special_cases = [
                self.make_from(1, self._max_exp, DOUBLE_MAX_FRACTION),
                self.make_from(0, self._max_exp, DOUBLE_MAX_FRACTION),
                self.make_from(1, self._min_exp, DOUBLE_MAX_FRACTION),
                self.make_from(0, self._min_exp, DOUBLE_MAX_FRACTION)
            ]
            if self._min_exp <= 0 and self._max_exp >= 0:
                special_cases.append(0.0)
                special_cases.append(-0.0)
            if self._min_exp <= 3 and self._max_exp >= 3:
                special_cases.append(1.0)
                special_cases.append(-1.0)
            if not no_nans:
                special_cases.append(float('inf'))
                special_cases.append(float('-inf'))
                special_cases.append(float('nan'))
                special_cases.append(NEG_DOUBLE_NAN_MAX_VALUE)
        super().__init__(DoubleType(), nullable=nullable, special_cases=special_cases)

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._min_exp) + ',' + str(self._max_exp) + ',' + str(self._no_nans) + ')'

    @staticmethod
    def make_from(sign, exp, fraction):
        sign = sign & 1 # 1 bit
        exp = (exp + 1023) & 0x7FF # add bias and 11 bits
        fraction = fraction & DOUBLE_MAX_FRACTION
        i = (sign << 63) | (exp << 52) | fraction
        p = struct.pack('L', i)
        ret = struct.unpack('d', p)[0]
        return ret

    def _fixup_nans(self, v):
        if self._no_nans and (math.isnan(v) or v == math.inf or v == -math.inf):
            v = None if self.nullable else 0.0
        return v

    def start(self, rand):
        if self._use_full_range:
            def gen_double():
                i = rand.randint(LONG_MIN, LONG_MAX)
                p = struct.pack('l', i)
                return self._fixup_nans(struct.unpack('d', p)[0])
            self._start(rand, gen_double)
        else:
            def gen_part_double():
                sign = rand.getrandbits(1)
                exp = rand.randint(self._min_exp, self._max_exp)
                fraction = rand.getrandbits(52)
                return self._fixup_nans(self.make_from(sign, exp, fraction))
            self._start(rand, gen_part_double)

class BooleanGen(DataGen):
    """Generate Bools (True/False)"""
    def __init__(self, nullable=True):
        super().__init__(BooleanType(), nullable=nullable)

    def start(self, rand):
        self._start(rand, lambda : bool(rand.getrandbits(1)))

class StructGen(DataGen):
    """Generate a Struct"""
    def __init__(self, children, nullable=True, special_cases=[]):
        """
        Initialize the struct with children.  The children should be of the form:
        [('name', Gen),('name_2', Gen2)]
        Where name is the name of the strict field and Gens are Generators of
        the type for that entry.
        """
        tmp = [StructField(name, child.data_type, nullable=child.nullable) for name, child in children]
        super().__init__(StructType(tmp), nullable=nullable, special_cases=special_cases)
        self.children = children

    def __repr__(self):
        return super().__repr__() + '(' + ','.join([str(i) for i in self.children]) + ')'

    def _cache_repr(self):
        return super()._cache_repr() + '(' + ','.join([name + child._cache_repr() for name, child in self.children]) + ')'

    def start(self, rand):
        for name, child in self.children:
            child.start(rand)
        def make_tuple():
            data = [child.gen() for name, child in self.children]
            return tuple(data)
        self._start(rand, make_tuple)

    def contains_ts(self):
        return any(child[1].contains_ts() for child in self.children)

class DateGen(DataGen):
    """Generate Dates in a given range"""
    def __init__(self, start=None, end=None, nullable=True):
        super().__init__(DateType(), nullable=nullable)
        if start is None:
            # Spark supports times starting at
            # "0001-01-01 00:00:00.000000"
            start = date(1, 1, 1)
        elif not isinstance(start, date):
            raise RuntimeError('Unsupported type passed in for start {}'.format(start))

        if end is None:
            # Spark supports time through
            # "9999-12-31 23:59:59.999999"
            end = date(9999, 12, 31)
        elif isinstance(end, timedelta):
            end = start + end
        elif not isinstance(start, date):
            raise RuntimeError('Unsupported type passed in for end {}'.format(end))

        self._start_day = self._to_days_since_epoch(start)
        self._end_day = self._to_days_since_epoch(end)

        self.with_special_case(start)
        self.with_special_case(end)

        # we want a few around the leap year if possible
        step = int((end.year - start.year) / 5.0)
        if (step != 0):
            years = {self._guess_leap_year(y) for y in range(start.year, end.year, step)}
            for y in years:
                leap_day = date(y, 2, 29)
                if (leap_day > start and leap_day < end):
                    self.with_special_case(leap_day)
                next_day = date(y, 3, 1)
                if (next_day > start and next_day < end):
                    self.with_special_case(next_day)

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._start_day) + ',' + str(self._end_day) + ')'

    @staticmethod
    def _guess_leap_year(t):
        y = int(math.ceil(t/4.0)) * 4
        if ((y % 100) == 0) and ((y % 400) != 0):
            y = y + 4
        if (y == 10000):
            y = y - 4
        return y

    _epoch = date(1970, 1, 1)
    _days = timedelta(days=1)
    def _to_days_since_epoch(self, val):
        return int((val - self._epoch)/self._days)

    def _from_days_since_epoch(self, days):
        return self._epoch + timedelta(days=days)

    def start(self, rand):
        start = self._start_day
        end = self._end_day
        self._start(rand, lambda : self._from_days_since_epoch(rand.randint(start, end)))

class NotSupportedInSparkVersion(Exception):
    pass

class TimestampGen(DataGen):
    """Generate Timestamps in a given range. All timezones are UTC by default."""
    def __init__(self, start=None, end=None, nullable=True, tzinfo=timezone.utc):
        if is_spark_341_or_later():
            super().__init__(TimestampNTZType() if tzinfo==None else TimestampType(), nullable=nullable)
            self._is_error = False
        elif tzinfo==None:
            super().__init__(TimestampType(), nullable=nullable)
            self._is_error = True
        else:
            super().__init__(TimestampType(), nullable=nullable)
            self._is_error = False

        if start is None:
            # If set to (1,1,1), a timezone with a negative offset would cause an out of bound error with Python
            # Valid range of time: date.min = datetime.date(1, 1, 1)
            start = datetime(1, 2, 1, tzinfo=tzinfo)
        elif not isinstance(start, datetime):
            raise RuntimeError('Unsupported type passed in for start {}'.format(start))

        # If set to (9999,12,31), a timezone with a postiive would cause an out of bound error with Python
        # Valid range of time: date.max = datetime.date(9999, 12, 31)
        max_end = datetime(9999, 12, 30, 23, 59, 59, 999999, tzinfo=tzinfo)
        if end is None:
            end = max_end
        elif isinstance(end, timedelta):
            max_timedelta = max_end - start
            if ( end >= max_timedelta):
                end = max_end
            else:
                end = start + end
        elif not isinstance(start, date):
            raise RuntimeError('Unsupported type passed in for end {}'.format(end))

        self._epoch = datetime(1970, 1, 1, tzinfo=tzinfo)
        self._start_time = self._to_us_since_epoch(start)
        self._end_time = self._to_us_since_epoch(end)
        self._tzinfo = tzinfo

        self.with_special_case(start)
        self.with_special_case(end)

        if (self._epoch >= start and self._epoch <= end):
            self.with_special_case(self._epoch)

    def __repr__(self):
        if self._tzinfo==None:
            name="TimestampNTZ"
        else:
            name="Timestamp"

        if not self.nullable:
            return name + '(not_null)'
        return name

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._start_time) + ',' + str(self._end_time) + ',' + str(self._tzinfo) + ')'

    _us = timedelta(microseconds=1)

    def _to_us_since_epoch(self, val):
        return int((val - self._epoch)/self._us)

    def _from_us_since_epoch(self, us):
        return self._epoch + timedelta(microseconds=us)

    def start(self, rand):
        if self._is_error:
            raise NotSupportedInSparkVersion("TimestampNTZ is only supported on Spark 3.4.1+ in python. All tests that use this feature must have a skipif on them...")
        start = self._start_time
        end = self._end_time
        self._start(rand, lambda : self._from_us_since_epoch(rand.randint(start, end)))

    def contains_ts(self):
        return True

class ArrayGen(DataGen):
    """Generate Arrays of data."""
    def __init__(self, child_gen, min_length=0, max_length=20, nullable=True, all_null=False, convert_to_tuple=False):
        super().__init__(ArrayType(child_gen.data_type, containsNull=child_gen.nullable), nullable=nullable)
        self._min_length = min_length
        self._max_length = max_length
        self._child_gen = child_gen
        self.all_null = all_null
        self.convert_to_tuple = convert_to_tuple

    def __repr__(self):
        return super().__repr__() + '(' + str(self._child_gen) + ')'

    def _cache_repr(self):
        return (super()._cache_repr() + '(' + self._child_gen._cache_repr() +
                ',' + str(self._min_length) + ',' + str(self._max_length) + ',' +
                str(self.all_null) + ',' + str(self.convert_to_tuple) + ')')


    def start(self, rand):
        self._child_gen.start(rand)
        def gen_array():
            if self.all_null:
                return None
            length = rand.randint(self._min_length, self._max_length)
            result = [self._child_gen.gen() for _ in range(0, length)]
            # This is needed for map(array, _) tests because python cannot create
            # a dict(list, _), but it can create a dict(tuple, _)
            if self.convert_to_tuple:
                result = tuple(result)
            return result
        self._start(rand, gen_array)

    def contains_ts(self):
        return self._child_gen.contains_ts()

class MapGen(DataGen):
    """Generate a Map"""
    def __init__(self, key_gen, value_gen, min_length=0, max_length=20, nullable=True, special_cases=[]):
        # keys cannot be nullable
        assert not key_gen.nullable
        self._min_length = min_length
        self._max_length = max_length
        self._key_gen = key_gen
        self._value_gen = value_gen
        super().__init__(MapType(key_gen.data_type, value_gen.data_type, valueContainsNull=value_gen.nullable), nullable=nullable, special_cases=special_cases)

    def __repr__(self):
        return super().__repr__() + '(' + str(self._key_gen) + ',' + str(self._value_gen) + ')'

    def _cache_repr(self):
        return (super()._cache_repr() + '(' + self._key_gen._cache_repr() + ',' + self._value_gen._cache_repr() +
                ',' + str(self._min_length) + ',' + str(self._max_length) + ')')

    def start(self, rand):
        self._key_gen.start(rand)
        self._value_gen.start(rand)
        def make_dict():
            length = rand.randint(self._min_length, self._max_length)
            return {self._key_gen.gen(): self._value_gen.gen() for idx in range(0, length)}
        def make_dict_float():
            # In Spark map, at most one key can be NaN. However, in Python dict, multiple NaN keys 
            # are allowed because NaN != NaN. So we need to ensure that there is at most one NaN 
            # key in the dict when generating map type data.
            length = rand.randint(self._min_length, self._max_length)
            count = 0
            has_nan = False
            result = {}
            while count < length:
                key = self._key_gen.gen()
                if math.isnan(key):
                    if has_nan:
                        continue
                    else:
                        has_nan = True
                result[key] = self._value_gen.gen()
                count += 1
            return result

        if self._key_gen.data_type == FloatType() or self._key_gen.data_type == DoubleType():
            self._start(rand, make_dict_float)
        else:
            self._start(rand, make_dict)

    def contains_ts(self):
        return self._key_gen.contains_ts() or self._value_gen.contains_ts()


class NullGen(DataGen):
    """Generate NullType values"""
    def __init__(self, dt = NullType()):
        super().__init__(dt, nullable=True)

    def start(self, rand):
        def make_null():
            return None
        self._start(rand, make_null)

# DayTimeIntervalGen is for Spark 3.3.0+
# DayTimeIntervalType(startField, endField):
# Represents a day-time interval which is made up of a contiguous subset of the following fields:
#   SECOND, seconds within minutes and possibly fractions of a second [0..59.999999],
#   Note Spark now uses 99 as max second, see issue https://issues.apache.org/jira/browse/SPARK-38324
#   If second is start field, its max value is long.max / microseconds in one second
#   MINUTE, minutes within hours [0..59],
#   If minute is start field, its max value is long.max / microseconds in one minute
#   HOUR, hours within days [0..23],
#   If hour is start field, its max value is long.max / microseconds in one hour
#   DAY, days in the range [0..106751991]. 106751991 is long.max / microseconds in one day
# For more details: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
MIN_DAY_TIME_INTERVAL = timedelta(microseconds=-pow(2, 63))
MAX_DAY_TIME_INTERVAL = timedelta(microseconds=(pow(2, 63) - 1))
class DayTimeIntervalGen(DataGen):
    """Generate DayTimeIntervalType values"""
    def __init__(self, min_value=MIN_DAY_TIME_INTERVAL, max_value=MAX_DAY_TIME_INTERVAL, start_field="day", end_field="second",
                 nullable=True, special_cases=[timedelta(seconds=0)]):
        # Note the nano seconds are truncated for min_value and max_value
        self._min_micros = (math.floor(min_value.total_seconds()) * 1000000) + min_value.microseconds
        self._max_micros = (math.floor(max_value.total_seconds()) * 1000000) + max_value.microseconds
        fields = ["day", "hour", "minute", "second"]
        self._start_index = fields.index(start_field)
        self._end_index = fields.index(end_field)
        if self._start_index > self._end_index:
            raise RuntimeError('Start field {}, end field {}, valid fields is {}, start field index should <= end '
                               'field index'.format(start_field, end_field, fields))
        super().__init__(DayTimeIntervalType(self._start_index, self._end_index), nullable=nullable,
                         special_cases=special_cases)

    def _gen_random(self, rand):
        micros = rand.randint(self._min_micros, self._max_micros)
        # issue: Interval types are not truncated to the expected endField when creating a DataFrame via Duration
        # https://issues.apache.org/jira/browse/SPARK-38577
        # If above issue is fixed, should update this DayTimeIntervalGen.
        return timedelta(microseconds=micros)
    
    def _cache_repr(self):
        return (super()._cache_repr() + '(' + str(self._min_micros) + ',' + str(self._max_micros) +
                ',' + str(self._start_index) + ',' + str(self._end_index) + ')')

    def start(self, rand):
        self._start(rand, lambda: self._gen_random(rand))

class BinaryGen(DataGen):
    """Generate BinaryType values.

    Default mode: random bytes from min_val to max_val.
    With encoding: generates random Unicode codepoints in [min_val, max_val],
    encodes them with the given encoding. Useful for generating valid encoded text.
    E.g. BinaryGen(min_val=0x4E00, max_val=0x9FFF, encoding='gbk') generates
    random CJK text encoded as GBK.
    """
    def __init__(self, min_length=0, max_length=20, nullable=True,
                 min_val=0, max_val=255, encoding=None):
        super().__init__(BinaryType(), nullable=nullable)
        self._min_length = min_length
        self._max_length = max_length
        self._min_val = min_val
        self._max_val = max_val
        self._encoding = encoding

    def _cache_repr(self):
        return super()._cache_repr() + '(' + str(self._min_length) + ',' + \
            str(self._max_length) + ',' + str(self._min_val) + ',' + \
            str(self._max_val) + ',' + str(self._encoding) + ')'

    def start(self, rand):
        if self._encoding is not None:
            def gen_encoded():
                length = rand.randint(self._min_length, self._max_length)
                chars = [chr(rand.randint(self._min_val, self._max_val))
                         for _ in range(length)]
                return ''.join(chars).encode(self._encoding)
            self._start(rand, gen_encoded)
        else:
            def gen_bytes():
                length = rand.randint(self._min_length, self._max_length)
                return bytes([rand.randint(self._min_val, self._max_val)
                              for _ in range(length)])
            self._start(rand, gen_bytes)

# Note: Current(2023/06/06) maxmium IT data size is 7282688 bytes, so LRU cache with maxsize 128
# will lead to 7282688 * 128 = 932 MB additional memory usage in edge case, which is acceptable.
@lru_cache(maxsize=128, typed=True)
def gen_df_help(data_gen, length, seed_value):
    rand = random.Random(seed_value)
    data_gen.start(rand)
    data = [data_gen.gen() for index in range(0, length)]
    return data

def gen_df(spark, data_gen, length=2048, seed=None, num_slices=None):
    """Generate a spark dataframe from the given data generators."""
    if seed is None:
        seed_value = get_datagen_seed()
    else:
        seed_value = seed

    if isinstance(data_gen, list):
        src = StructGen(data_gen, nullable=False)
    else:
        src = data_gen
        # we cannot create a data frame from a nullable struct
        assert not data_gen.nullable

    data = gen_df_help(src, length, seed_value)

    # We use `numSlices` to create an RDD with the specific number of partitions,
    # which is then turned into a dataframe. If not specified, it is `None` (default spark value)
    return spark.createDataFrame(
        SparkContext.getOrCreate().parallelize(data, numSlices=num_slices),
        src.data_type)

def _mark_as_lit(data, data_type):
    # To support nested types, 'data_type' is required.
    assert data_type is not None

    if data is None:
        return f.lit(data).cast(data_type)

    if isinstance(data_type, ArrayType):
        assert isinstance(data, list)
        # Sadly you cannot create a literal from just an array in pyspark
        return f.array([_mark_as_lit(x, data_type.elementType) for x in data])
    elif isinstance(data_type, StructType):
        assert isinstance(data, tuple) and len(data) == len(data_type.fields)
        # Sadly you cannot create a literal from just a dict/tuple in pyspark
        children = zip(data, data_type.fields)
        return f.struct([_mark_as_lit(x, fd.dataType).alias(fd.name) for x, fd in children])
    elif isinstance(data_type, DateType):
        # Due to https://bugs.python.org/issue13305 we need to zero pad for years prior to 1000,
        # but this works for all of them
        dateString = data.strftime("%Y-%m-%d").zfill(10)
        return f.lit(dateString).cast(data_type)
    elif isinstance(data_type, MapType):
        assert isinstance(data, dict)
        # Sadly you cannot create a literal from just a dict/tuple in pyspark
        col_array = []
        for k in data:
            col_array.append(_mark_as_lit(k, data_type.keyType))
            col_array.append(_mark_as_lit(data[k], data_type.valueType))
        return f.create_map(*col_array)
    else:
        # lit does not take a data type so we might have to cast it
        return f.lit(data).cast(data_type)

def _gen_scalars_common(data_gen, count, seed=None):
    if isinstance(data_gen, list):
        src = StructGen(data_gen, nullable=False)
    else:
        src = data_gen

    if seed is None:
        seed_value = get_datagen_seed()
    else:
        seed_value = seed

    rand = random.Random(seed_value)
    src.start(rand)
    return src

def gen_scalars(data_gen, count, seed=None, force_no_nulls=False):
    """Generate scalar values."""
    if force_no_nulls:
        assert(not isinstance(data_gen, NullGen))
    src = _gen_scalars_common(data_gen, count, seed=seed)
    data_type = src.data_type
    return (_mark_as_lit(src.gen(force_no_nulls=force_no_nulls), data_type) for i in range(0, count))

def gen_scalar(data_gen, seed=None, force_no_nulls=False):
    """Generate a single scalar value."""
    v = list(gen_scalars(data_gen, 1, seed=seed, force_no_nulls=force_no_nulls))
    return v[0]

def gen_scalar_values(data_gen, count, seed=None, force_no_nulls=False):
    """Generate scalar values."""
    src = _gen_scalars_common(data_gen, count, seed=seed)
    return (src.gen(force_no_nulls=force_no_nulls) for i in range(0, count))

def gen_scalar_value(data_gen, seed=None, force_no_nulls=False):
    """Generate a single scalar value."""
    v = list(gen_scalar_values(data_gen, 1, seed=seed, force_no_nulls=force_no_nulls))
    return v[0]

def debug_df(df, path = None, file_format = 'json', num_parts = 1):
    """Print out or save the contents and the schema of a dataframe for debugging."""

    if path is not None:
        # Save the dataframe and its schema
        # The schema can be re-created by using DataType.fromJson and used
        # for loading the dataframe
        file_name = f"{path}.{file_format}"
        schema_file_name = f"{path}.schema.json"

        df.coalesce(num_parts).write.format(file_format).save(file_name)
        print(f"SAVED df output for debugging at {file_name}")

        schema_json = df.schema.json()
        schema_file = open(schema_file_name , 'w')
        schema_file.write(schema_json)
        schema_file.close()
        print(f"SAVED df schema for debugging along in the output dir")
    else:
        print('COLLECTED\n{}'.format(df.collect()))

    df.explain()
    df.printSchema()
    return df

def print_params(data_gen):
    print('Test Datagen Params=' + str([(a, b.get_types()) for a, b in data_gen]))

def idfn(val):
    """Provide an API to provide display names for data type generators."""
    return str(val)

def meta_idfn(meta):
    def tmp(something):
        return meta + idfn(something)
    return tmp

def three_col_df(spark, a_gen, b_gen, c_gen, length=2048, seed=None, num_slices=None):
    gen = StructGen([('a', a_gen),('b', b_gen),('c', c_gen)], nullable=False)
    return gen_df(spark, gen, length=length, seed=seed, num_slices=num_slices)

def two_col_df(spark, a_gen, b_gen, length=2048, seed=None, num_slices=None):
    gen = StructGen([('a', a_gen),('b', b_gen)], nullable=False)
    return gen_df(spark, gen, length=length, seed=seed, num_slices=num_slices)

def binary_op_df(spark, gen, length=2048, seed=None, num_slices=None):
    return two_col_df(spark, gen, gen, length=length, seed=seed, num_slices=num_slices)

def unary_op_df(spark, gen, length=2048, seed=None, num_slices=None):
    return gen_df(spark, StructGen([('a', gen)], nullable=False),
        length=length, seed=seed, num_slices=num_slices)

def to_cast_string(spark_type):
    if isinstance(spark_type, ByteType):
        return 'BYTE'
    elif isinstance(spark_type, ShortType):
        return 'SHORT'
    elif isinstance(spark_type, IntegerType):
        return 'INT'
    elif isinstance(spark_type, LongType):
        return 'LONG'
    elif isinstance(spark_type, FloatType):
        return 'FLOAT'
    elif isinstance(spark_type, DoubleType):
        return 'DOUBLE'
    elif isinstance(spark_type, BooleanType):
        return 'BOOLEAN'
    elif isinstance(spark_type, DateType):
        return 'DATE'
    elif isinstance(spark_type, TimestampType):
        return 'TIMESTAMP'
    elif isinstance(spark_type, StringType):
        return 'STRING'
    elif isinstance(spark_type, DecimalType):
        return 'DECIMAL({}, {})'.format(spark_type.precision, spark_type.scale)
    elif isinstance(spark_type, ArrayType):
        return 'ARRAY<{}>'.format(to_cast_string(spark_type.elementType))
    elif isinstance(spark_type, StructType):
        children = [fd.name + ':' + to_cast_string(fd.dataType) for fd in spark_type.fields]
        return 'STRUCT<{}>'.format(','.join(children))
    elif isinstance(spark_type, BinaryType):
        return 'BINARY'
    else:
        raise RuntimeError('CAST TO TYPE {} NOT SUPPORTED YET'.format(spark_type))

def get_null_lit_string(spark_type):
    if isinstance(spark_type, NullType):
        return 'null'
    else:
        string_type = to_cast_string(spark_type)
        return 'CAST(null as {})'.format(string_type)

def _convert_to_sql(spark_type, data):
    if isinstance(data, str):
        d = "'" + data.replace("\\", "\\\\").replace("\'", "\\\'") + "'"
    elif isinstance(data, datetime):
        d = "'" + data.strftime('%Y-%m-%d T%H:%M:%S.%f').zfill(26) + "'"
    elif isinstance(data, date):
        d = "'" + data.strftime('%Y-%m-%d').zfill(10) + "'"
    elif isinstance(data, list):
        assert isinstance(spark_type, ArrayType)
        d = "array({})".format(",".join([_convert_to_sql(spark_type.elementType, x) for x in data]))
    elif isinstance(data, tuple):
        assert isinstance(spark_type, StructType) and len(data) == len(spark_type.fields)
        # Format of each child: 'name',data
        children = ["'{}'".format(fd.name) + ',' + _convert_to_sql(fd.dataType, x)
                for fd, x in zip(spark_type.fields, data)]
        d = "named_struct({})".format(','.join(children))
    elif isinstance(data, bytearray) or isinstance(data, bytes):
        d = "X'{}'".format(data.hex())
    elif not data:
        # data is None
        d = "null"
    else:
        d = "'{}'".format(str(data))

    if isinstance(spark_type, NullType):
        return d
    else:
        return 'CAST({} as {})'.format(d, to_cast_string(spark_type))

def gen_scalars_for_sql(data_gen, count, seed=None, force_no_nulls=False):
    """Generate scalar values, but strings that can be used in selectExpr or SQL"""
    src = _gen_scalars_common(data_gen, count, seed=seed)
    if isinstance(data_gen, NullGen):
        assert not force_no_nulls
        return ('null' for i in range(0, count))
    spark_type = data_gen.data_type
    return (_convert_to_sql(spark_type, src.gen(force_no_nulls=force_no_nulls)) for i in range(0, count))

class EmptyStringType(Enum):
    ALL_NULL = 1
    ALL_EMPTY = 2
    MIXED = 3

all_empty_string_types = EmptyStringType.__members__.values()

empty_string_gens_map = {
  EmptyStringType.ALL_NULL : lambda: NullGen(StringType()),
  EmptyStringType.ALL_EMPTY : lambda: StringGen("", nullable=False),
  EmptyStringType.MIXED : lambda: StringGen("", nullable=True)
}

def mk_empty_str_gen(empty_type):
    return empty_string_gens_map[empty_type]()

byte_gen = ByteGen()
short_gen = ShortGen()
int_gen = IntegerGen()
long_gen = LongGen()
float_gen = FloatGen()
double_gen = DoubleGen()
string_gen = StringGen()
boolean_gen = BooleanGen()
date_gen = DateGen()
timestamp_gen = TimestampGen()
binary_gen = BinaryGen()
null_gen = NullGen()

numeric_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen]

integral_gens = [byte_gen, short_gen, int_gen, long_gen]
# A lot of mathematical expressions only support a double as input
# by parametrizing even for a single param for the test it makes the tests consistent
double_gens = [double_gen]
double_n_long_gens = [double_gen, long_gen]
int_n_long_gens = [int_gen, long_gen]

decimal_gen_32bit = DecimalGen(precision=7, scale=3)
decimal_gen_32bit_neg_scale = DecimalGen(precision=7, scale=-3)
decimal_gen_64bit = DecimalGen(precision=12, scale=2)
decimal_gen_128bit = DecimalGen(precision=20, scale=2)

decimal_gens = [decimal_gen_32bit, decimal_gen_64bit, decimal_gen_128bit]

# all of the basic gens
all_basic_gens_no_null = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
                          string_gen, boolean_gen, date_gen, timestamp_gen]
all_basic_gens = all_basic_gens_no_null + [null_gen]

basic_gen_no_floats = [byte_gen, short_gen, int_gen, long_gen, string_gen, boolean_gen, date_gen, timestamp_gen, null_gen]

all_basic_gens_no_nan = [byte_gen, short_gen, int_gen, long_gen, FloatGen(no_nans=True), DoubleGen(no_nans=True),
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen]

# Many Spark versions have issues sorting large decimals,
# see https://issues.apache.org/jira/browse/SPARK-40089.
orderable_decimal_gen_128bit = decimal_gen_128bit
if is_before_spark_340():
    orderable_decimal_gen_128bit = DecimalGen(precision=20, scale=2, special_cases=[])

orderable_decimal_gens = [decimal_gen_32bit, decimal_gen_64bit, orderable_decimal_gen_128bit ]

# TODO add in some array generators to this once that is supported for sorting
# a selection of generators that should be orderable (sortable and compareable)
orderable_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen] + orderable_decimal_gens

# TODO add in some array generators to this once that is supported for these operations
# a selection of generators that can be compared for equality
eq_gens = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen]

# Include decimal type while testing equalTo and notEqualTo
eq_gens_with_decimal_gen =  eq_gens + decimal_gens

date_gens = [date_gen]
date_n_time_gens = [date_gen, timestamp_gen]

boolean_gens = [boolean_gen]

single_level_array_gens = [ArrayGen(sub_gen) for sub_gen in all_basic_gens + decimal_gens]

single_level_array_gens_no_null = [ArrayGen(sub_gen) for sub_gen in all_basic_gens_no_null + decimal_gens]

single_level_array_gens_no_nan = [ArrayGen(sub_gen) for sub_gen in all_basic_gens_no_nan + decimal_gens]

single_level_array_gens_no_decimal = [ArrayGen(sub_gen) for sub_gen in all_basic_gens]

map_string_string_gen = [MapGen(StringGen(pattern='key_[0-9]', nullable=False), StringGen())]

# Be careful to not make these too large of data generation takes for ever
# This is only a few nested array gens, because nesting can be very deep
nested_array_gens_sample = [ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
        ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10),
        ArrayGen(StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]]))]

# Some array gens, but not all because of nesting
array_gens_sample = single_level_array_gens + nested_array_gens_sample

# all of the basic types in a single struct
all_basic_struct_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(all_basic_gens)])

struct_array_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(single_level_array_gens)])

# Some struct gens, but not all because of nesting
nonempty_struct_gens_sample = [all_basic_struct_gen,
        StructGen([['child0', byte_gen], ['child1', all_basic_struct_gen]]),
        StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]
nonempty_struct_gens_sample_no_list = [all_basic_struct_gen,
        StructGen([['child0', byte_gen], ['child1', all_basic_struct_gen]]),
        StructGen([['child0', short_gen], ['child1', double_gen]])]

struct_gens_sample = nonempty_struct_gens_sample + [StructGen([])]
struct_gens_sample_no_list = nonempty_struct_gens_sample_no_list + [StructGen([])]
struct_gen_decimal128 = StructGen(
    [['child' + str(ind), sub_gen] for ind, sub_gen in enumerate([decimal_gen_128bit])])
struct_gens_sample_with_decimal128 = struct_gens_sample + [struct_gen_decimal128]
struct_gens_sample_with_decimal128_no_list = struct_gens_sample_no_list + [struct_gen_decimal128]

simple_string_to_string_map_gen = MapGen(StringGen(pattern='key_[0-9]', nullable=False),
        StringGen(), max_length=10)

all_basic_map_gens = [MapGen(f(nullable=False), f()) for f in [BooleanGen, ByteGen, ShortGen, IntegerGen, LongGen, FloatGen, DoubleGen, DateGen, TimestampGen]] + [simple_string_to_string_map_gen]
decimal_64_map_gens = [MapGen(key_gen=gen, value_gen=gen, nullable=False) for gen in [DecimalGen(7, 3, nullable=False), DecimalGen(12, 2, nullable=False)]]
decimal_128_map_gens = [MapGen(key_gen=gen, value_gen=gen, nullable=False) for gen in [DecimalGen(20, 2, nullable=False)]]

# Some map gens, but not all because of nesting
map_gens_sample = all_basic_map_gens + [MapGen(StringGen(pattern='key_[0-9]', nullable=False), ArrayGen(string_gen), max_length=10),
        MapGen(RepeatSeqGen(IntegerGen(nullable=False), 10), long_gen, max_length=10),
        MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen),
        MapGen(IntegerGen(False), ArrayGen(int_gen, max_length=3), max_length=3),
        MapGen(ShortGen(False), StructGen([['child0', byte_gen], ['child1', double_gen]]), max_length=3),
        MapGen(ByteGen(False), MapGen(FloatGen(False), date_gen, max_length=3), max_length=3)]

nested_gens_sample = array_gens_sample + struct_gens_sample_with_decimal128 + map_gens_sample + decimal_128_map_gens

ansi_enabled_conf = {'spark.sql.ansi.enabled': 'true'}
ansi_disabled_conf = {'spark.sql.ansi.enabled': 'false'}
legacy_interval_enabled_conf = {'spark.sql.legacy.interval.enabled': 'true'}

def copy_and_update(conf, *more_confs):
    local_conf = conf.copy()
    for more in more_confs:
        local_conf.update(more)
    return local_conf

all_gen = [StringGen(), ByteGen(), ShortGen(), IntegerGen(), LongGen(),
           FloatGen(), DoubleGen(), BooleanGen(), DateGen(), TimestampGen(),
           decimal_gen_32bit, decimal_gen_64bit, decimal_gen_128bit]

# Pyarrow will complain the error as below if the timestamp is out of range for both CPU and GPU,
# so narrow down the time range to avoid exceptions causing test failures.
#
#     "pyarrow.lib.ArrowInvalid: Casting from timestamp[us, tz=UTC] to timestamp[ns]
#      would result in out of bounds timestamp: 51496791452587000"
#
# This issue has been fixed in pyarrow by the PR https://github.com/apache/arrow/pull/7169
# However it still requires PySpark to specify the new argument "timestamp_as_object".
arrow_common_gen = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        string_gen, boolean_gen, date_gen,
        TimestampGen(start=datetime(1970, 1, 1, tzinfo=timezone.utc),
                     end=datetime(2262, 1, 1, tzinfo=timezone.utc))]

arrow_array_gens = [ArrayGen(subGen) for subGen in arrow_common_gen] + nested_array_gens_sample

arrow_one_level_struct_gen = StructGen([
        ['child'+str(i), sub_gen] for i, sub_gen in enumerate(arrow_common_gen)])

arrow_struct_gens = [arrow_one_level_struct_gen,
        StructGen([['child0', ArrayGen(short_gen)], ['child1', arrow_one_level_struct_gen]])]

# This function adds a new column named uniq_int where each row
# has a new unique integer value. It just starts at 0 and
# increments by 1 for each row.
# This can be used to add a column to a dataframe if you need to
# sort on a column with unique values.
# This collects the data to driver though so can be expensive.
def append_unique_int_col_to_df(spark, dataframe):
    def append_unique_to_rows(rows):
        new = []
        for item in range(len(rows)):
            row_dict = rows[item].asDict()
            row_dict['uniq_int'] = item
            new_row = Row(**row_dict)
            new.append(new_row)
        return new

    collected = dataframe.collect()
    if (len(collected) > INT_MAX):
        raise RuntimeError('To many rows to add unique integer values starting from 0 to')
    existing_schema = dataframe.schema
    new_rows = append_unique_to_rows(collected)
    new_schema = StructType(existing_schema.fields + [StructField("uniq_int", IntegerType(), False)])
    return spark.createDataFrame(new_rows, new_schema)

disable_parquet_field_id_write = {"spark.sql.parquet.fieldId.write.enabled": "false"}  # default is true
enable_parquet_field_id_write = {"spark.sql.parquet.fieldId.write.enabled": "true"}
enable_parquet_field_id_read = {"spark.sql.parquet.fieldId.read.enabled": "true"}  # default is false


# generate a df with c1 and c2 column have 25 combinations
def get_25_partitions_df(spark):
    schema = StructType([
        StructField("c1", IntegerType()),
        StructField("c2", IntegerType()),
        StructField("c3", IntegerType())])
    data = [[i, j, k] for i in range(0, 5) for j in range(0, 5) for k in range(0, 100)]
    return spark.createDataFrame(data, schema)


# allow non gpu when time zone is non-UTC because of https://github.com/NVIDIA/spark-rapids/issues/9653'
# This will be deprecated and replaced case specified non GPU allow list
non_utc_allow = ['ProjectExec', 'FilterExec', 'FileSourceScanExec', 'BatchScanExec', 'CollectLimitExec',
                 'DeserializeToObjectExec', 'DataWritingCommandExec', 'WriteFilesExec', 'ShuffleExchangeExec',
                 'ExecutedCommandExec', 'Cast', 'Sequence', 
                 'Length', 'TimeAdd', 'DateAddInterval', 'ScalarSubquery', 'JsonToStructs'] if is_not_utc() else []

non_supported_tz_allow = ['ProjectExec', 'FilterExec', 'FileSourceScanExec', 'BatchScanExec', 'CollectLimitExec',
                 'DeserializeToObjectExec', 'DataWritingCommandExec', 'WriteFilesExec', 'ShuffleExchangeExec',
                 'ExecutedCommandExec', 'Cast', 'Sequence',
                 'Length', 'TimeAdd', 'DateAddInterval', 'ScalarSubquery', 'JsonToStructs'] if not is_supported_time_zone() else []


# date related regexps for generating date strings within python's range limits

# regexp to generate date from 0001-02-01, format is yyyy-MM-dd
date_start_1_2_1 = '(0{0,3}1-(0?[2-9]|[1-3][0-9]))|(([0-9]{0,3}[2-9]|[1-9][0-9]{0,2}[01])-[0-3]?[0-9])-[0-5]?[0-9]'

# regexp to generate year from 0002, format is yyyy
yyyy_start_0002 = '([0-9]{3}[2-9]|([1-9][0-9]{2}|0[1-9][0-9]|00[1-9])[0-1])'

# regexp to generate year from 0001, format is yyyy
yyyy_start_0001 = '([0-9]{3}[1-9]|([1-9][0-9]{2}|0[1-9][0-9]|00[1-9])[0-1])'

# regexp to generate date from 0001-02-01, format is yyyy-MM-dd
date_start_1_1_1 = yyyy_start_0001 + '-[0-9]{1,2}-[0-9]{1,2}'
