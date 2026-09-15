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

package org.apache.spark.sql.delta.rapids;

import scala.Option;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.delta.commands.DeltaCommand;

/**
 * Common interface for GPU Delta commands.
 *
 * DeltaCommand defines {@code createTableRelation} starting in Delta 4.2, but it does not define
 * that method in earlier supported versions. Declaring the method as a Java default method lets
 * this shared interface compile against all supported versions: it introduces the method for
 * older versions and overrides it for Delta 4.2.
 */
public interface GpuDeltaCommandLike extends DeltaCommand {
  default LogicalPlan createTableRelation(
      OptimisticTransaction txn, Option<String> tableAliasOpt) {
    return GpuDeltaCommandUtils$.MODULE$.createTableRelation(txn, tableAliasOpt);
  }
}
