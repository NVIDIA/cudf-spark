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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.GpuOverrides._
import com.nvidia.spark.rapids.shims._

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution._

private[rapids] object GpuExecOverrides {
  val rules: Seq[ExecRule[_ <: SparkPlan]] = Seq(
    exec[GenerateExec] (
      "The backend for operations that generate more output rows than input rows like explode",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all),
      GenerateExecConstructorRuleMeta),
    exec[ProjectExec](
      "The backend for most select, withColumn and dropColumn statements",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      ProjectExecConstructorRuleMeta),
    exec[RangeExec](
      "The backend for range operator",
      ExecChecks(TypeSig.LONG, TypeSig.LONG),
      RangeExecRuleMeta),
    exec[BatchScanExec](
      "The backend for most file input",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
          TypeSig.DECIMAL_128 + TypeSig.BINARY).nested(),
        TypeSig.all),
      BatchScanExecConstructorRuleMeta),
    exec[CoalesceExec](
      "The backend for the dataframe coalesce method",
      ExecChecks((gpuCommonTypes + TypeSig.STRUCT + TypeSig.ARRAY +
          TypeSig.MAP + TypeSig.BINARY + GpuTypeShims.additionalArithmeticSupportedTypes).nested(),
        TypeSig.all),
      CoalesceExecRuleMeta),
    exec[DataWritingCommandExec](
      "Writing data",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128.withPsNote(
          TypeEnum.DECIMAL, "128bit decimal only supported for Orc and Parquet") +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
        TypeSig.all),
      DataWritingCommandExecRuleMeta),
    exec[ExecutedCommandExec](
      "Eagerly executed commands",
      ExecChecks(TypeSig.all, TypeSig.all),
      ExecutedCommandExecConstructorRuleMeta),
    exec[TakeOrderedAndProjectExec](
      "Take the first limit elements as defined by the sortOrder, and do projection if needed",
      // The SortOrder TypeSig will govern what types can actually be used as sorting key data type.
      // The types below are allowed as inputs and outputs.
      ExecChecks((pluginSupportedOrderableSig +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
      GpuTakeOrderedAndProjectExecMeta),
    exec[LocalLimitExec](
      "Per-partition limiting of results",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      LocalLimitExecRuleMeta),
    exec[GlobalLimitExec](
      "Limiting of results across partitions",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      GlobalLimitExecRuleMeta),
    exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      CollectLimitExecConstructorRuleMeta)
        .disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
            "of rows in a batch it could help by limiting the number of rows transferred from " +
            "GPU to CPU"),
    exec[FilterExec](
      "The backend for most filter statements",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(), TypeSig.all),
      GpuFilterExecMeta),
    exec[ShuffleExchangeExec](
      "The backend for most data being exchanged between processes",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP +
          GpuTypeShims.additionalArithmeticSupportedTypes).nested()
          .withPsNote(TypeEnum.STRUCT, "Round-robin partitioning is not supported for nested " +
              s"structs if ${SQLConf.SORT_BEFORE_REPARTITION.key} is true")
          .withPsNote(
            Seq(TypeEnum.MAP),
            "Round-robin partitioning is not supported if " +
              s"${SQLConf.SORT_BEFORE_REPARTITION.key} is true"),
        TypeSig.all),
      ShuffleExchangeExecConstructorRuleMeta),
    exec[UnionExec](
      "The backend for the union operator",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT).nested()
        .withPsNote(TypeEnum.STRUCT,
          "unionByName will not optionally impute nulls for missing struct fields " +
          "when the column is a struct and there are non-overlapping fields"), TypeSig.all),
      UnionExecRuleMeta),
    exec[BroadcastExchangeExec](
      "The backend for broadcast exchange of data",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(TypeSig.commonCudfTypes +
          TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT),
        TypeSig.all),
      BroadcastExchangeExecConstructorRuleMeta),
    exec[BroadcastHashJoinExec](
      "Implementation of join using broadcast data",
      JoinTypeChecks.equiJoinExecChecks,
      BroadcastHashJoinExecConstructorRuleMeta),
    exec[BroadcastNestedLoopJoinExec](
      "Implementation of join using brute force. Full outer joins and joins where the " +
          "broadcast side matches the join side (e.g.: LeftOuter with left broadcast) are not " +
          "supported",
      JoinTypeChecks.nonEquiJoinChecks,
      BroadcastNestedLoopJoinExecConstructorRuleMeta),
    exec[CartesianProductExec](
      "Implementation of join using brute force",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT)
          .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
              TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
        TypeSig.all),
      CartesianProductExecRuleMeta),
    exec[HashAggregateExec](
      "The backend for hash based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT)
            .nested()
            .withPsNote(TypeEnum.MAP,
              "not allowed for grouping expressions")
            .withPsNote(TypeEnum.ARRAY,
              "not allowed for grouping expressions if containing Struct as child")
            .withPsNote(TypeEnum.STRUCT,
              "not allowed for grouping expressions if containing Array, Map, or Binary as child"),
        TypeSig.all),
      HashAggregateExecConstructorRuleMeta),
    exec[ObjectHashAggregateExec](
      "The backend for hash based aggregations supporting TypedImperativeAggregate functions",
      ExecChecks(
        // note that binary input is allowed here but there are additional checks later on to
        // check that we have can support binary in the context of aggregate buffer conversions
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY)
            .nested()
            .withPsNote(TypeEnum.BINARY, "not allowed for grouping expressions and " +
              "only allowed when aggregate buffers can be converted between CPU and GPU")
            .withPsNote(Seq(TypeEnum.ARRAY, TypeEnum.MAP),
              "not allowed for grouping expressions")
            .withPsNote(TypeEnum.STRUCT,
              "not allowed for grouping expressions if containing Array, Map, or Binary as child"),
        TypeSig.all),
      ObjectHashAggregateExecConstructorRuleMeta),
    exec[ShuffledHashJoinExec](
      "Implementation of join using hashed shuffled data",
      JoinTypeChecks.equiJoinExecChecks,
      ShuffledHashJoinExecConstructorRuleMeta),
    exec[SortAggregateExec](
      "The backend for sort based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.BINARY)
            .nested()
            .withPsNote(Seq(TypeEnum.ARRAY, TypeEnum.MAP),
              "not allowed for grouping expressions")
            .withPsNote(TypeEnum.STRUCT,
              "not allowed for grouping expressions if containing Array, Map, or Binary as child"),
        TypeSig.all),
      SortAggregateExecConstructorRuleMeta),
    exec[SortExec](
      "The backend for the sort operator",
      // The SortOrder TypeSig will govern what types can actually be used as sorting key data type.
      // The types below are allowed as inputs and outputs.
      ExecChecks((pluginSupportedOrderableSig + TypeSig.ARRAY +
          TypeSig.STRUCT +TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all),
      SortExecConstructorRuleMeta),
    exec[SortMergeJoinExec](
      "Sort merge join, replacing with shuffled hash join",
      JoinTypeChecks.equiJoinExecChecks,
      SortMergeJoinExecConstructorRuleMeta),
    exec[ExpandExec](
      "The backend for the expand operator",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all),
      ExpandExecConstructorRuleMeta),
    exec[WindowExec](
      "Window-operator backend",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP + TypeSig.BINARY).nested(),
        TypeSig.all,
        Map("partitionSpec" ->
            InputCheck(
                TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
                TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
            TypeSig.all))),
      WindowExecConstructorRuleMeta
    ),
    exec[SampleExec](
      "The backend for the sample operator",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
        TypeSig.ARRAY + TypeSig.DECIMAL_128 + GpuTypeShims.additionalCommonOperatorSupportedTypes)
          .nested(), TypeSig.all),
      SampleExecConstructorRuleMeta
    ),
    exec[SubqueryBroadcastExec](
      "Plan to collect and transform the broadcast key values",
      ExecChecks(TypeSig.all, TypeSig.all),
      SubqueryBroadcastExecConstructorRuleMeta
    ),
    SparkShimImpl.aqeShuffleReaderExec,
    // AggregateInPandasExec renamed to ArrowAggregatePythonExec in Spark 4.1.0
    AggregateInPandasExecShims.execRule.orNull,
    exec[ArrowEvalPythonExec](
      "The backend of the Scalar Pandas UDFs. Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      ArrowEvalPythonExecRuleMeta),
    exec[FlatMapCoGroupsInPandasExec](
      "The backend for CoGrouped Aggregation Pandas UDF. Accelerates the data transfer" +
        " between the Java process and the Python process. It also supports scheduling GPU" +
        " resources for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      FlatMapCoGroupsInPandasExecConstructorRuleMeta)
        .disabledByDefault("Performance is not ideal with many small groups"),
    exec[FlatMapGroupsInPandasExec](
      "The backend for Flat Map Groups Pandas UDF, Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      FlatMapGroupsInPandasExecConstructorRuleMeta),
    exec[MapInPandasExec](
      "The backend for Map Pandas Iterator UDF. Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      MapInPandasExecConstructorRuleMeta),
    exec[InMemoryTableScanExec](
      "Implementation of InMemoryTableScanExec to use GPU accelerated caching",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP + GpuTypeShims.additionalCommonOperatorSupportedTypes)
          .nested(), TypeSig.all),
      InMemoryTableScanExecConstructorRuleMeta),
    neverReplaceExec[AlterNamespaceSetPropertiesExec]("Namespace metadata operation"),
    neverReplaceExec[CreateNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[DescribeNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[DropNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[SetCatalogAndNamespaceExec]("Namespace metadata operation"),
    SparkShimImpl.neverReplaceShowCurrentNamespaceCommand,
    ShowNamespacesExecShims.neverReplaceExec.orNull,
    neverReplaceExec[AlterTableExec]("Table metadata operation"),
    neverReplaceExec[CreateTableExec]("Table metadata operation"),
    neverReplaceExec[DeleteFromTableExec]("Table metadata operation"),
    neverReplaceExec[DescribeTableExec]("Table metadata operation"),
    neverReplaceExec[DropTableExec]("Table metadata operation"),
    neverReplaceExec[AtomicReplaceTableExec]("Table metadata operation"),
    neverReplaceExec[RefreshTableExec]("Table metadata operation"),
    neverReplaceExec[RenameTableExec]("Table metadata operation"),
    neverReplaceExec[ReplaceTableExec]("Table metadata operation"),
    neverReplaceExec[ShowTablePropertiesExec]("Table metadata operation"),
    neverReplaceExec[ShowTablesExec]("Table metadata operation"),
    neverReplaceExec[AdaptiveSparkPlanExec]("Wrapper for adaptive query plan"),
    neverReplaceExec[BroadcastQueryStageExec]("Broadcast query stage"),
    neverReplaceExec[ShuffleQueryStageExec]("Shuffle query stage")
  )
}
