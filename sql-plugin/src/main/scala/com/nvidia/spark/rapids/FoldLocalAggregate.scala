/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate._

/**
 * Folds two-stage AggregateExecs of Local Aggregate into one-shot CompleteAggregate.
 *
 * Local Aggregate is a concept of physical plan which means no need to do shuffle exchange to
 * redistribute data before final aggregate. The Local Aggregate may emerge under certain
 * circumstance, such as the BucketScan Spec fully matches the groupBy keys.
 */
object FoldLocalAggregate extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transform {
      case p@LocalAggregatePattern(finalAgg: BaseAggregateExec, partAgg: BaseAggregateExec) =>
        // Spark eliminates the filter for the aggExpressions in Final mode. So, we need to copy
        // the filter from the corresponding partial aggExpressions.
        val aggExpressions = finalAgg.aggregateExpressions.zip(
          partAgg.aggregateExpressions).map { case (finalAggExpr, partAggExpr) =>
            finalAggExpr.copy(mode = Complete, filter = partAggExpr.filter)
        }
        val aggAttributes = aggExpressions.map(_.resultAttribute)
        // Need to use partial group expressions. Otherwise, `groupBy(Literal)` cases will fail.
        // Because GpuHashAggregateExec derive input attributes from the child’s output. For a
        // “group by 1”, the literal 1 is used only in the partial aggregate (as an alias for
        // Literal(1)), and the final aggregate uses attributes bound from the partial output
        // rather than the literal itself.
        val groupingExpressions = partAgg.groupingExpressions
        finalAgg match {
          case hash: HashAggregateExec =>
            hash.copy(
              groupingExpressions = groupingExpressions,
              aggregateExpressions = aggExpressions,
              aggregateAttributes = aggAttributes,
              child = partAgg.child)
          case sort: SortAggregateExec =>
            sort.copy(
              // Need to use partial group expressions. Otherwise, groupBy(Literal) will fail.
              groupingExpressions = groupingExpressions,
              aggregateExpressions = aggExpressions,
              aggregateAttributes = aggAttributes,
              child = partAgg.child)
          case obj: ObjectHashAggregateExec =>
            obj.copy(
              // Need to use partial group expressions. Otherwise, groupBy(Literal) will fail.
              groupingExpressions = groupingExpressions,
              aggregateExpressions = aggExpressions,
              aggregateAttributes = aggAttributes,
              child = partAgg.child)
          case _ =>
            logError(
              s"Failed to fold LocalAggregate because of unexpected AggregateExec: $finalAgg")
            p
        }
    }
  }
}

/**
 * Capture the pattern of LocalAggregate, which consists of two-stage AggregateExecs being
 * connected with each other directly (without the ShuffleExchange in between):
 * +- FinalAggregateExec
 *    +- PartialAggregateExec
 *
 * The LocalAggregate can be emerged regardless HashAggregateExec, SortAggregateExec or
 * ObjectHashAggregateExec.
 */
object LocalAggregatePattern extends Logging {
  def unapply(plan: SparkPlan): Option[(BaseAggregateExec, BaseAggregateExec)] = {
    plan match {
      case hashAgg: HashAggregateExec
        if hashAgg.child.isInstanceOf[HashAggregateExec] &&
          isLocalExchange(hashAgg, hashAgg.child.asInstanceOf[HashAggregateExec]) =>
        Some(hashAgg -> hashAgg.child.asInstanceOf[HashAggregateExec])
      case sort: SortAggregateExec
        if sort.child.isInstanceOf[SortAggregateExec] &&
          isLocalExchange(sort, sort.child.asInstanceOf[SortAggregateExec]) =>
        Some(sort -> sort.child.asInstanceOf[SortAggregateExec])
      case obj: ObjectHashAggregateExec
        if obj.child.isInstanceOf[ObjectHashAggregateExec] &&
          isLocalExchange(obj, obj.child.asInstanceOf[ObjectHashAggregateExec])  =>
        Some(obj -> obj.child.asInstanceOf[ObjectHashAggregateExec])
      case _ =>
        None
    }
  }

  // Check if the two connected AggregateExecs are two stages of the same logical Aggregate. If
  // so, group expressions and aggregate expressions should be identical(except AggregateMode)
  private def isLocalExchange(merge: BaseAggregateExec, partial: BaseAggregateExec): Boolean = {
    val groupExpressions = merge.groupingExpressions
    val aggExpressions = merge.aggregateExpressions
    val childGroupExpressions = partial.groupingExpressions
    val childAggExpressions = partial.aggregateExpressions
    val requiredDistribution = merge.requiredChildDistribution

    if ( // Fast check
      groupExpressions.length != childGroupExpressions.length ||
        aggExpressions.length != childAggExpressions.length) {
      false
    } else if ( // Check if partial OutputPartition statisfy distribution of FinalAgg
      !partial.outputPartitioning.satisfies(requiredDistribution.head)) {
      false
    } else if ( // Check AggregateExpressions
      !aggExpressions.zip(childAggExpressions).forall {
        // <Partial -> Final> pair check
        case (s2, s1) if s2.mode != Final || s1.mode != Partial =>
          false
        // TODO: Currently, distinct aggregate is NOT supported
        case (s2, s1) if s2.isDistinct || s1.isDistinct =>
          false
        // AggregateFunctions should be identical
        case (s2, s1) if !s2.aggregateFunction.equals(s1.aggregateFunction) =>
          false
        // Check if the filter of FinalAgg is empty
        case (s2, _) if s2.filter.isDefined =>
          logError(s"AggregateExpression($s2) of FinalAggregate($merge) should not carry filter")
          false
        case _ =>
          true
      }) {
      false
    } else {
      // Check GroupExpressions
      groupExpressions.zip(childGroupExpressions).forall {
        case (s2, s1) => s2.semanticEquals(s1.toAttribute)
      }
    }
  }
}
