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

from datetime import date

from pyspark.sql.types import ArrayType, DateType, MapType, StringType, StructField, StructType

from data_gen import SetValuesGen, gen_scalar_value


def test_gen_scalar_value_normalizes_nested_dates():
    data_type = StructType([
        StructField("dates", ArrayType(DateType(), containsNull=True)),
        StructField(
            "dates_by_day",
            MapType(
                DateType(),
                ArrayType(DateType(), containsNull=True),
                valueContainsNull=True)),
        StructField("label", StringType()),
    ])
    value = (
        [date(4, 3, 1), None],
        {
            date(4, 3, 2): [date(4, 3, 3), None],
            date(4, 3, 4): None,
        },
        "unchanged",
    )

    result = gen_scalar_value(SetValuesGen(data_type, [value]))

    assert result == (
        ["0004-03-01", None],
        {
            "0004-03-02": ["0004-03-03", None],
            "0004-03-04": None,
        },
        "unchanged",
    )


def test_gen_scalar_value_preserves_array_container_type():
    value = (date(4, 3, 1), None)

    result = gen_scalar_value(SetValuesGen(ArrayType(DateType()), [value]))

    assert result == ("0004-03-01", None)
    assert isinstance(result, tuple)


def test_gen_scalar_value_preserves_top_level_null():
    assert gen_scalar_value(SetValuesGen(DateType(), [None])) is None
