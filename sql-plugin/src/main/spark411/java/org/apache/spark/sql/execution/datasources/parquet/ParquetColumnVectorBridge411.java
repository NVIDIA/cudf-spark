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

/*** spark-rapids-shim-json-lines
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.parquet;

import java.util.Set;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public final class ParquetColumnVectorBridge411 {
  private ParquetColumnVectorBridge411() {
  }

  public static Object newParquetColumnVector(
      ParquetColumn column,
      WritableColumnVector vector,
      int capacity,
      MemoryMode memoryMode,
      Set<ParquetColumn> missingColumns,
      boolean isTopLevel,
      Object defaultValue) {
    return ParquetColumnVectorAccess.newVector(
        new Class<?>[] {ParquetColumn.class, WritableColumnVector.class, int.class,
            Set.class, boolean.class, Object.class},
        column, vector, capacity, missingColumns, isTopLevel, defaultValue);
  }
}
