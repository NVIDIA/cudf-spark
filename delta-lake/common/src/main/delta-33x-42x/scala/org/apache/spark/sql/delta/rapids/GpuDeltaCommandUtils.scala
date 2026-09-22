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

package org.apache.spark.sql.delta.rapids

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.execution.datasources.LogicalRelation

object GpuDeltaCommandUtils {
  def createTableRelation(
      txn: OptimisticTransaction,
      tableAliasOpt: Option[String]): LogicalPlan = {
    val relation = txn.deltaLog.createRelation(
      Seq.empty,
      Some(txn.snapshot),
      txn.catalogTable,
      false)
    val logicalRelation = LogicalRelation(relation)
    if (tableAliasOpt.isDefined) {
      SubqueryAlias(tableAliasOpt.get, logicalRelation)
    } else {
      logicalRelation
    }
  }
}
