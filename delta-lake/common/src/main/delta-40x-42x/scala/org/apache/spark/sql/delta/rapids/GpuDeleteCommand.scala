/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
 *
 * This file was derived from DeleteCommand.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.rapids

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.rapids.{GpuDeleteCommandBase, GpuDeltaLog}

/**
 * GPU version of DeleteCommand shared by classic Spark-backed Delta shims.
 */
case class GpuDeleteCommand(
    gpuDeltaLog: GpuDeltaLog,
    catalogTable: Option[CatalogTable],
    target: LogicalPlan,
    condition: Option[Expression])
  extends GpuDeleteCommandBase(gpuDeltaLog, catalogTable, target, condition)
  with ClassicSessionDeltaCommandShims
