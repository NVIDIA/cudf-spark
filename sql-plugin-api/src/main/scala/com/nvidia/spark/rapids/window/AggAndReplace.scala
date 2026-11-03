/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.window

import ai.rapids.cudf.ReplacePolicy

/**
 * For Scan and GroupBy Scan aggregations nulls are not always treated the same way as they are
 * in window operations. Often we have to run a post processing step and replace them. This
 * groups those two together so we can have a complete picture of how to perform these types of
 * aggregations.
 */
case class AggAndReplace[T](agg: T, nullReplacePolicy: Option[ReplacePolicy])
