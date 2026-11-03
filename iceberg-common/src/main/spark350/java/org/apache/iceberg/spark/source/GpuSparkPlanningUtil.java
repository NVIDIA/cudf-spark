/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/

package org.apache.iceberg.spark.source;

import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.io.FileIO;

import java.util.List;

public class GpuSparkPlanningUtil  {
  public static String[][] fetchBlockLocations(
      FileIO io, List<? extends ScanTaskGroup<?>> taskGroups) {
    return SparkPlanningUtil.fetchBlockLocations(io, taskGroups);
  }

  public static String[][] assignExecutors(
      List<? extends ScanTaskGroup<?>> taskGroups, List<String> executorLocations) {
    return SparkPlanningUtil.assignExecutors(taskGroups, executorLocations);
  }
}
