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
{"spark": "500"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.shuffle;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.rapids.ProxyRapidsShuffleInternalManagerBase;

/**
 * Spark 5 requires custom shuffle managers to implement BlockingShuffleManager.
 * Keep that package-private Spark type in Spark's package so Scaladoc can resolve it.
 */
public abstract class RapidsShuffleManagerBase extends ProxyRapidsShuffleInternalManagerBase
    implements BlockingShuffleManager {
  protected RapidsShuffleManagerBase(SparkConf conf, boolean isDriver) {
    super(conf, isDriver);
  }
}
