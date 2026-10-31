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

import random
import struct

import pytest

from asserts import (assert_equal_with_signed_zero, assert_gpu_fallback_collect,
                     run_with_cpu_and_gpu)
from conftest import spark_jvm
from marks import allow_non_gpu


_CONF = {
    'spark.rapids.sql.incompatibleOps.enabled': 'false',
    'spark.rapids.sql.expression.cpuBridge.enabled': 'true',
}


def _rounding_values(data_type):
    # Raw IEEE representations cover small/large magnitudes and neighbors of ties.
    # Spark's ROUND/BROUND execution, not an implementation of rounding here, is the oracle.
    fmt, bits, mantissa = ('>f', 32, 23) if data_type == 'float' else ('>d', 64, 52)

    def from_bits(value):
        return struct.unpack(fmt, value.to_bytes(bits // 8, 'big'))[0]

    values = [None, 0.0, -0.0, float('nan'), float('inf'), float('-inf')]
    for tie in [0.5, 1.5, 2.5, 3.5, 127.5, 1023.5,
                2.0 ** (mantissa - 1) - 0.5, 2.0 ** mantissa - 0.5]:
        encoded = int.from_bytes(struct.pack(fmt, tie), 'big')
        for adjacent in [encoded - 1, encoded, encoded + 1]:
            value = from_bits(adjacent)
            values += [value, -value]
    max_finite = 0x7f7fffff if data_type == 'float' else 0x7fefffffffffffff
    for encoded in [1, (1 << mantissa) - 1, 1 << mantissa, max_finite]:
        value = from_bits(encoded)
        values += [value, -value]
    rng = random.Random(20261029)
    values += [from_bits(rng.getrandbits(bits)) for _ in range(4096)]
    return values


def _assert_zero_scale(spark_values, data_type, conf):
    def query(spark):
        return spark.createDataFrame([(v,) for v in spark_values], 'a ' + data_type).selectExpr(
            'a', 'round(a)', 'round(a, 0)', 'round(a, 1 - 1)', 'bround(a)', 'bround(a, 0)')

    (cpu, _), (gpu, gpu_df) = run_with_cpu_and_gpu(
        query, 'COLLECT_WITH_DATAFRAME', conf=conf)
    capture = spark_jvm().org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
    capture.assertContains(gpu_df._jdf, 'GpuRound')
    capture.assertContains(gpu_df._jdf, 'GpuBRound')
    capture.assertNotContain(gpu_df._jdf, 'GpuCpuBridgeExpression')
    # Ordinary float equality would miss a negative-zero regression.
    assert_equal_with_signed_zero(cpu, gpu)


@pytest.mark.parametrize('data_type', ['float', 'double'])
@pytest.mark.parametrize('ansi', ['false', 'true'])
@pytest.mark.parametrize('codegen', ['CODEGEN_ONLY', 'NO_CODEGEN'])
def test_floating_round_zero_exact(data_type, ansi, codegen):
    _assert_zero_scale(_rounding_values(data_type), data_type, dict(_CONF, **{
        'spark.sql.ansi.enabled': ansi,
        'spark.sql.codegen.factoryMode': codegen,
        'spark.sql.codegen.wholeStage': str(codegen == 'CODEGEN_ONLY').lower(),
    }))


@pytest.mark.parametrize('data_type', ['float', 'double'])
@pytest.mark.parametrize('values', [[], [None] * 33], ids=['empty', 'all-null'])
def test_floating_round_zero_nulls(data_type, values):
    _assert_zero_scale(values, data_type, _CONF)


@allow_non_gpu('ProjectExec', 'Round', 'BRound')
@pytest.mark.parametrize('data_type', ['float', 'double'])
@pytest.mark.parametrize('function,cpu_class', [('round', 'Round'), ('bround', 'BRound')])
@pytest.mark.parametrize('scale', [-1, 2])
def test_floating_round_nonzero_fallback(data_type, function, cpu_class, scale):
    assert_gpu_fallback_collect(
        lambda spark: spark.createDataFrame(
            [(1.025,), (2.675,), (-2.675,), (25.0,), (None,)], 'a ' + data_type).selectExpr(
                '{}(a, {})'.format(function, scale)), cpu_class,
        conf=dict(_CONF, **{'spark.rapids.sql.expression.cpuBridge.enabled': 'false'}))
