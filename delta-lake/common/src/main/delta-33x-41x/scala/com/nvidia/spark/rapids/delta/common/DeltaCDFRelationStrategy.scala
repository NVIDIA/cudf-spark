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

package com.nvidia.spark.rapids.delta.common

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Literal}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.DeltaCDFRelation
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Plans the internal DataFrame of an OSS Delta batch CDF relation directly.
 *
 * DeltaCDFRelation.buildScan returns the internal DataFrame as RDD[Row]. Spark wraps that RDD in a
 * RowDataSourceScanExec, introducing a row boundary around file scans that can otherwise remain
 * columnar. Replanning the internal logical plan exposes those scans to the regular Spark and
 * RAPIDS planning rules.
 */
object DeltaCDFRelationStrategy extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, relation: LogicalRelation)
        if relation.relation.isInstanceOf[DeltaCDFRelation] =>
      val cdf = relation.relation.asInstanceOf[DeltaCDFRelation]
      if (cdf.startingVersion.isEmpty) {
        Nil
      } else {
        val spark = cdf.sqlContext.sparkSession
        val changes = DeltaCDFRelationShim.changesToBatchDF(cdf)

        val changesByName = changes.queryExecution.analyzed.output.map(a => a.name -> a).toMap
        val relationOutput = relation.output.map { attr =>
          Alias(changesByName(attr.name), attr.name)(
            exprId = attr.exprId,
            qualifier = attr.qualifier,
            explicitMetadata = Some(attr.metadata))
        }
        val filter = filters.reduceOption(And).getOrElse(Literal.TrueLiteral)
        val rewritten = Project(projects,
          Filter(filter, Project(relationOutput, changes.queryExecution.analyzed)))

        Seq(planLater(spark.sessionState.optimizer.execute(rewritten)))
      }
    case _ => Nil
  }
}
