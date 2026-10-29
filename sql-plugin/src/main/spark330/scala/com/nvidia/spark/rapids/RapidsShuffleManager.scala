/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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
{"spark":["330","350"],"carryForward":"all"}
{"spark":["330db","332db"],"carryForward":"none"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.$_spark.version.classifier_

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.ProxyRapidsShuffleInternalManagerBase

/** A shuffle manager optimized for the RAPIDS Plugin for Apache Spark. */
sealed class RapidsShuffleManager(
    conf: SparkConf,
    isDriver: Boolean
) extends ProxyRapidsShuffleInternalManagerBase(conf, isDriver)
