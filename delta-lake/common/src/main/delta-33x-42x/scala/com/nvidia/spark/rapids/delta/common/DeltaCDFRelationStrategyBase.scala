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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Literal}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.DeltaCDFRelation
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.DataType

/**
 * Shared planning logic for exposing the internal DataFrame of an OSS Delta batch CDF relation.
 * Concrete strategies provide the version-specific Delta API call that builds the DataFrame.
 */
abstract class DeltaCDFRelationStrategyBase extends SparkStrategy {

  protected def changesToBatchDF(cdf: DeltaCDFRelation): DataFrame

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, relation: LogicalRelation)
        if relation.relation.isInstanceOf[DeltaCDFRelation] =>
      val cdf = relation.relation.asInstanceOf[DeltaCDFRelation]
      if (cdf.startingVersion.isEmpty) {
        Nil
      } else {
        val spark = cdf.sqlContext.sparkSession
        val changes = changesToBatchDF(cdf)

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

        val optimized = spark.sessionState.optimizer.execute(rewritten)
        if (optimized.output == rewritten.output) {
          Seq(planLater(optimized))
        } else {
          // Optimizing this subtree can remove redundant aliases and change its output exprIds.
          // The physical parent still references the original output, so restore it outside the
          // optimizer while retaining predicate pushdown and column pruning within the CDF read.
          // Spark's optimizer preserves the ordered output schema, allowing nullability changes.
          // Check that contract before mapping by position to avoid silently truncating the zip.
          require(DataType.equalsIgnoreNullability(optimized.schema, rewritten.schema),
            "Delta CDF optimization changed the ordered output schema")
          val (expressions, child) = optimized match {
            case Project(projectList, input) => (projectList, input)
            case other => (other.output, other)
          }
          val output = expressions.zip(rewritten.output).map { case (actual, expected) =>
            // Reuse an existing projection's expressions rather than projecting its output twice.
            val expression = actual match {
              case alias: Alias => alias.child
              case other => other
            }
            Alias(expression, expected.name)(
              exprId = expected.exprId,
              qualifier = expected.qualifier,
              explicitMetadata = Some(expected.metadata))
          }
          Seq(ProjectExec(output, planLater(child)))
        }
      }
    case _ => Nil
  }
}
