/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.iceberg.parquet.metadata;

import java.util.Map;

/** File-level metadata sufficient to produce rows without opening a data file. */
public final class FileMetadata {
  private final String path;
  private final long rowCount;
  private final Map<Integer, ?> constants;

  public FileMetadata(String path, long rowCount, Map<Integer, ?> constants) {
    this.path = path;
    this.rowCount = rowCount;
    this.constants = constants;
  }

  public String path() {
    return path;
  }

  public long rowCount() {
    return rowCount;
  }

  public Map<Integer, ?> constants() {
    return constants;
  }
}
