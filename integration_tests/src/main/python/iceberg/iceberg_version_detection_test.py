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

import os
import pytest

from iceberg import iceberg_unsupported_mark
from marks import allow_non_gpu, iceberg
from spark_session import with_gpu_session
from spark_init_internal import spark_version

# Iceberg version detection is exercised on every Spark version that supports
# Iceberg (3.5.x / 4.0.x / 4.1.x) so the per-version commit-id -> version mappings
# in IcebergProbeImpl — including the 1.11.0 mapping used on Spark 4.1 — are covered.
pytestmark = iceberg_unsupported_mark

@allow_non_gpu(any=True)
@iceberg
def test_iceberg_version_detection():
    expected = os.environ.get("EXPECTED_ICEBERG_VERSION")
    if expected is None:
        pytest.skip("EXPECTED_ICEBERG_VERSION env var not set")

    # Shim sub-package selected per Iceberg major.minor, e.g. 1.11.x -> iceberg111x.
    # Mirrors IcebergProbeImpl.icebergVersionToShim.
    major_minor = ".".join(expected.split(".")[:2])
    expected_shim_package = \
        "com.nvidia.spark.rapids.iceberg.iceberg{}x".format(major_minor.replace(".", ""))

    def check(spark):
        access = spark.sparkContext._jvm.com.nvidia.spark.rapids.iceberg.IcebergProviderAccess
        actual = access.detectedVersion()
        assert actual == expected, \
            "Iceberg version detection mismatch: expected '{}' on Spark {}, got '{}'".format(
                expected, spark_version(), actual)
        # Assert the shim package too: a correct detectedVersion() paired with a wrong
        # version -> shim mapping (e.g. the 1.11 -> iceberg111x row) would otherwise
        # still pass.
        actual_shim_package = access.shimPackage()
        assert actual_shim_package == expected_shim_package, \
            "Iceberg shim package mismatch for {} on Spark {}: expected '{}', got '{}'".format(
                expected, spark_version(), expected_shim_package, actual_shim_package)

    with_gpu_session(check)
