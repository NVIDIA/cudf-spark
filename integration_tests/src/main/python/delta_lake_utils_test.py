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

import json
from types import SimpleNamespace

import pytest

from delta_lake_utils import assert_delta_logs_at_paths_equivalent


@pytest.mark.parametrize("cpu_ids,gpu_ids,should_match", [
    (("cpu-a", "cpu-a"), ("gpu-x", "gpu-x"), True),
    (("cpu-a", "cpu-b"), ("gpu-x", "gpu-y"), True),
    (("cpu-a", "cpu-a"), ("gpu-x", "gpu-y"), False),
    (("cpu-a", "cpu-b"), ("gpu-x", "gpu-x"), False),
])
def test_delta_log_liquid_metadata_ids_preserve_history_grouping(
        cpu_ids, gpu_ids, should_match):
    def logs(table, metadata_ids):
        return [
            (f"{table}/_delta_log/{version:020d}.json", json.dumps({
                "add": {
                    "path": f"part-{table}-{version}.parquet",
                    "tags": {"LIQUID_METADATA_ID": metadata_id},
                }}))
            for version, metadata_id in enumerate(metadata_ids)
        ]

    histories = {
        # wholeTextFiles does not guarantee chronological enumeration.
        "CPU/_delta_log/*": list(reversed(logs("CPU", cpu_ids))),
        "GPU/_delta_log/*": logs("GPU", gpu_ids),
    }
    spark = SimpleNamespace(sparkContext=SimpleNamespace(
        wholeTextFiles=lambda path: SimpleNamespace(collect=lambda: histories[path])))

    if should_match:
        assert_delta_logs_at_paths_equivalent(spark, "CPU", "GPU")
    else:
        with pytest.raises(AssertionError, match="LIQUID_METADATA_ID"):
            assert_delta_logs_at_paths_equivalent(spark, "CPU", "GPU")
