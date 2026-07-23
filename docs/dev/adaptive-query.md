---
layout: page
title: Adaptive Query Execution with the RAPIDS Accelerator for Apache Spark
nav_order: 1
parent: Developer Overview
---
# Adaptive Query Execution with the RAPIDS Accelerator for Apache Spark

The benefits of AQE are not specific to CPU execution and can provide
additional performance improvements in conjunction with GPU-acceleration.

The main benefit of AQE is that queries can be optimized during execution based
on statistics that may not be available when initially planning the query.

Specific optimizations offered by AQE include:

- Coalescing shuffle partitions
- Using broadcast hash joins if one side of the join can fit in memory
- Optimizing skew joins

AQE works by converting leaf exchange nodes in the plan to query stages and
then schedules those query stages for execution. As soon as at least one query
stage completes, the rest of the plan is re-optimized (using a combination of
logical and physical optimization rules). This process is repeated until all
child query stages are materialized, and then the final query stage is
executed. This logic is contained in the
`AdaptiveSparkPlanExec.getFinalPhysicalPlan` method.

With AQE enabled, the physical plan will contain an `AdaptiveSparkPlanExec`
operator. This could be the root node or could be wrapped in an
`InsertIntoHadoopFsRelationCommand` operator if the query action is to write
results to disk.

Rather than replace the `AdaptiveSparkPlanExec` operator with a GPU-specific
version, we have worked with the Spark community to allow custom query stage
optimization rules to be provided, to support columnar plans.

However, Spark considers the final output of`AdaptiveSparkPlanExec` to be
row-based. The `supportsColumnar` method always returns `false`, and calling
`doExecuteColumnar` will throw an exception. For this reason, the RAPIDS
optimizer will insert a columnar-to-row transition as the root node, if
necessary. In the case where the adaptive plan is wrapped in a write to a
columnar source, then there is special handling at runtime to avoid an
unnecessary columnar-to-row transition followed by a row-to-columnar
transition.

## Optimizer Rules

On startup, the `SQLExecPlugin` plugin registers two distinct sets of
optimizer rules:

```scala
extensions.injectColumnar(session => ColumnarOverrideRules(session))
extensions.injectQueryStagePrepRule(session => GpuQueryStagePrepOverrides(session))
```

The `ColumnarOverrideRules` are used whether AQE is enabled or not, and the
`GpuQueryStagePrepOverrides` rules are specific to AQE.

There are four sets of optimizer rules used by AQE.

### queryStagePreparationRules

This set of rules is applied once before any query stages are created and is
also applied once for each re-optimization of the plan, after one or more query
stages have completed. The RAPIDS Accelerator `GpuQueryStagePrepOverrides` rule
is applied as part of this rule set.

This rule does not directly transform the plan into a new plan but tags nodes
in the Spark plan where they cannot be supported on the GPU. This is necessary
because when individual query plans are created and then passed to the plugin
for optimization, we do not have any information about the parent query stages,
so we rely on the plan being tagged upfront.

### queryStageOptimizerRules

This set of rules is applied to an Exchange node when creating a query stage
and will result in a `BroadcastQueryStageLike` or `ShuffleQueryStageLike` node
being created. This set of rules does not involve the RAPIDS Accelerator and
applies optimizations such as optimizing skewed joins and coalescing
shuffle partitions.

### postStageCreationRules

This set of rules is applied after a new query stage has been created. This
will apply `ColumnarOverrideRules` and this is where the query stage gets
translated into a GPU plan. These rules rely on the plan being tagged by an
earlier run of the `queryStagePreparationRules` rules.

### finalStageCreationRules

The final query stage is optimized with this set of rules, which is a
combination of the `queryStageOptimizerRules` and the
`postStageCreationRules`, with special handling to filter out some rules that
do not apply to the final query stage.

## Query Stage Re-use

The logic in `AdaptiveSparkPlanExec.getFinalPhysicalPlan` attempts to cache
query stages for re-use. The original Spark logic used the canonicalized
version of the Exchange node as the key for this cache but this can result in
errors if there are both CPU and GPU query stages that are created from
Exchange nodes that have equivalent canonical plan. This issue was resolved in
[SPARK-35093](https://issues.apache.org/jira/browse/SPARK-35093) and the fix is
available in Spark versions 3.0.3+, 3.1.2+, and 3.2.0+.
