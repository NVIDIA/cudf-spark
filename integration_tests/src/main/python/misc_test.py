# Copyright (c) 2025, NVIDIA CORPORATION.
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

from data_gen import *
from spark_session import with_gpu_session

import uuid


# GPU uuid function does not guarantee the result is the same as CPU,
# only guarantee it produces unique UUIDs
def test_uuid():
    num_rows = 2048

    def _gen_uuids(spark):
        return spark.range(num_rows).selectExpr("uuid() as uuid").collect()

    def _verify_unique_and_format(rows):
        uuid_set = set()
        for row in rows:
            # verify uuid, if it's invalid, it will raise ValueError
            uuid.UUID(row.uuid)
            uuid_set.add(row.uuid)
        assert len(uuid_set) == len(rows), "all the UUIDs should be different"

    # verify uniqueness across two rounds on GPU
    uuids_round1_gpu = with_gpu_session(lambda spark: _gen_uuids(spark))
    uuids_round2_gpu = with_gpu_session(lambda spark: _gen_uuids(spark))
    uuids_gpu = uuids_round1_gpu + uuids_round2_gpu
    assert len(uuids_gpu) == 2 * num_rows
    _verify_unique_and_format(uuids_gpu)
