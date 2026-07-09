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

from urllib.parse import urlparse

import pyarrow.fs as pa_fs
import pyarrow.parquet as pa_pq


def parquet_row_group_midpoints(path):
    """Returns an approximate byte midpoint for each Parquet row group."""
    if urlparse(path).scheme:
        filesystem, path = pa_fs.FileSystem.from_uri(path)
        meta = pa_pq.read_metadata(path, filesystem=filesystem)
    else:
        meta = pa_pq.read_metadata(path)
    midpoints = []
    for rg_index in range(meta.num_row_groups):
        row_group = meta.row_group(rg_index)
        first_col = row_group.column(0)
        start = first_col.data_page_offset
        dict_offset = first_col.dictionary_page_offset
        if dict_offset is not None and dict_offset > 0:
            start = min(start, dict_offset)
        total_size = 0
        for col_index in range(row_group.num_columns):
            total_size += row_group.column(col_index).total_compressed_size
        midpoints.append(start + total_size // 2)
    return midpoints
