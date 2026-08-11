# Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import pytest

allow_non_gpu_databricks = pytest.mark.allow_non_gpu_databricks
allow_non_gpu_conditional = pytest.mark.allow_non_gpu_conditional
allow_non_gpu = pytest.mark.allow_non_gpu
disable_ansi_mode = pytest.mark.disable_ansi_mode
validate_execs_in_gpu_plan = pytest.mark.validate_execs_in_gpu_plan
approximate_float = pytest.mark.approximate_float
ignore_order = pytest.mark.ignore_order
incompat = pytest.mark.incompat
inject_oom = pytest.mark.inject_oom
limit = pytest.mark.limit
qarun = pytest.mark.qarun
cudf_udf = pytest.mark.cudf_udf
shuffle_test = pytest.mark.shuffle_test
nightly_gpu_mem_consuming_case = pytest.mark.nightly_gpu_mem_consuming_case
nightly_host_mem_consuming_case = pytest.mark.nightly_host_mem_consuming_case
fuzz_test = pytest.mark.fuzz_test
iceberg = pytest.mark.iceberg
delta_lake = pytest.mark.delta_lake
large_data_test = pytest.mark.large_data_test
pyarrow_test = pytest.mark.pyarrow_test
datagen_overrides = pytest.mark.datagen_overrides
tz_sensitive_test = pytest.mark.tz_sensitive_test
hybrid_test = pytest.mark.hybrid_test

# Specific mark for allowing non-GPU Delta writes based on certain conditions.
# Use this mark instead of `allow_non_gpu` for Delta writes when you want to
# allow them to fall back. `allow_non_gpu` is not usable in this case as Delta
# writes do not appear in the query plan.
allow_non_gpu_delta_write_if = pytest.mark.allow_non_gpu_delta_write_if
