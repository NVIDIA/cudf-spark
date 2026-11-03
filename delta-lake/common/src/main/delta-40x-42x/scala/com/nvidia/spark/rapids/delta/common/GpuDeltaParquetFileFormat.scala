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

package com.nvidia.spark.rapids.delta.common

import com.nvidia.spark.rapids.delta.common.GpuDeltaParquetFileFormatBase

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}

/**
 * Thin wrapper delegating the Delta 4.x reader path to the shared Parquet format implementation.
 */
case class GpuDelta4xParquetFileFormat(
    protocol: Protocol,
    metadata: Metadata,
    nullableRowTrackingFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
  ) extends GpuDeltaParquetFileFormatBase(
    protocol,
    metadata,
    nullableRowTrackingFields,
    optimizationsEnabled,
    tablePath,
    isCDCRead)
