/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuOverrides.exec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, PythonUDAF, ToPrettyString}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Discard, Keep, Split}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.TableCacheQueryStageExec
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, MergeRowsExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, ReplaceDataExec, WriteDeltaExec}
import org.apache.spark.sql.execution.window.WindowGroupLimitExec
import org.apache.spark.sql.rapids.execution.python.GpuPythonUDAF
import org.apache.spark.sql.types.{StringType, StructType}

class TableCacheQueryStageExecMeta(
    tcqs: TableCacheQueryStageExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[TableCacheQueryStageExec](tcqs, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] =
    Seq(GpuOverrides.wrapPlan(tcqs.plan, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    willNotWorkOnGpu("TableCacheQueryStageExec wrapper stays on CPU for Spark AQE compatibility; " +
      "child plan may run on GPU")
  }

  override def convertToGpu(): GpuExec = {
    throw new IllegalStateException("TableCacheQueryStageExec should not be converted to GPU")
  }

  override def convertToCpu(): SparkPlan = {
    val wrappedPlan = childPlans.head.convertIfNeeded()

    // If the wrapped plan wasn't converted, return the original TableCacheQueryStageExec
    if (wrappedPlan == tcqs.plan) {
      return tcqs
    }

    // The wrapped plan was converted to GPU - check if we can safely wrap it
    if (InMemoryTableScanUtils.canTableCacheWrapGpuInMemoryTableScan) {
      // For Spark 3.5.2+: GPU InMemoryTableScan implements InMemoryTableScanLike,
      // so TableCacheQueryStageExec can safely wrap it and pass Spark's validation
      tcqs.copy(plan = wrappedPlan)
    } else {
      // For Spark 3.5.0-3.5.1: Missing InMemoryTableScanLike trait causes validation issues.
      // Keep the original CPU plan to avoid AQE complications.
      tcqs
    }
  }
}

trait Spark350PlusNonDBShims extends Spark340PlusNonDBShims {
  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference] = Seq.empty,
      fileFormat: Option[FileFormat]): RDD[InternalRow] = {
      new FileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns,
        metadataExtractors = fileFormat.map(_.fileConstantMetadataExtractors).getOrElse(Map.empty))
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[ToPrettyString]("An internal expressions which is used to " +
        "generate pretty string for all kinds of values",
        new ToPrettyStringChecks(),
        (toPrettyString, conf, p, r) => {
          new CastExprMetaBase[ToPrettyString](toPrettyString, conf, p, r) {

            override def needTimeZoneCheck: Boolean = 
              castNeedsTimeZone(toPrettyString.child.dataType, StringType)

            override val toType: StringType.type = StringType

            override def convertToGpu(child: Expression): GpuExpression = {
              GpuToPrettyString(child)
            }
          }
      }), 
      GpuOverrides.expr[PythonUDAF](
        "UDF run in an external python process. Does not actually run on the GPU, but " +
          "the transfer of data to/from it can be accelerated",
        ExprChecks.fullAggAndProject(
          // Different types of Pandas UDF support different sets of output type. Please refer to
          //   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
          // for more details.
          // It is impossible to specify the exact type signature for each Pandas UDF type in a
          // single expression 'PythonUDF'.
          // So use the 'unionOfPandasUdfOut' to cover all types for Spark. The type signature of
          // plugin is also an union of all the types of Pandas UDF.
          (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested() + TypeSig.STRUCT,
          TypeSig.unionOfPandasUdfOut,
          repeatingParamCheck = Some(RepeatingParamCheck(
            "param",
            (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[PythonUDAF](a, conf, p, r) {
          override def replaceMessage: String = "not block GPU acceleration"

          override def noReplacementPossibleMessage(reasons: String): String =
            s"blocks running on GPU because $reasons"

          override def convertToGpuImpl(): GpuExpression =
            GpuPythonUDAF(a.name, a.func, a.dataType,
              childExprs.map(_.convertToGpu()),
              a.evalType, a.udfDeterministic, a.resultId)
        }),
      GpuOverrides.expr[Keep](
        "Keep instruction for MERGE operations - keeps/updates rows based on condition",
        ExprChecks.projectOnly(
          TypeSig.all,
          TypeSig.all,
          Seq(ParamCheck("condition", TypeSig.all, TypeSig.all)),
          Some(RepeatingParamCheck("outputs", TypeSig.all, TypeSig.all))
        ),
        (keep, conf, p, r) => new GpuKeepInstructionMeta(keep, conf, p, r)),
      GpuOverrides.expr[Discard](
        "Discard instruction for MERGE operations - discards rows based on condition",
        ExprChecks.projectOnly(
          TypeSig.all,
          TypeSig.all,
          Seq(ParamCheck("condition", TypeSig.all, TypeSig.all))),
        (discard, conf, p, r) => new GpuDiscardInstructionMeta(discard, conf, p, r)),
      GpuOverrides.expr[Split](
        "Split instruction for MERGE operations - splits rows into multiple outputs",
        ExprChecks.projectOnly(
          TypeSig.all,
          TypeSig.all,
          Seq(ParamCheck("condition", TypeSig.all, TypeSig.all)),
          Some(RepeatingParamCheck("outputs", TypeSig.all, TypeSig.all))),
        (split, conf, p, r) => new GpuSplitInstructionMeta(split, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      // Use version-specific InMemoryTableScan rule (disabledByDefault for 3.5.0-3.5.1)
      InMemoryTableScanUtils.getInMemoryTableScanExecRule,
      GpuOverrides.exec[WindowGroupLimitExec](
        "Apply group-limits for row groups destined for rank-based window functions like " +
          "row_number(), rank(), and dense_rank()",
        ExecChecks( // Similar to WindowExec.
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all),
        (limit, conf, p, r) => new GpuWindowGroupLimitExecMeta(limit, conf, p, r)),
      exec[AppendDataExec](
        "Append data into a datasource V2 table",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new AppendDataExecMeta(p, conf, parent, r)),
      exec[OverwritePartitionsDynamicExec](
        "Overwrite partitions dynamically in a datasource V2 table",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new OverwritePartitionsDynamicExecMeta(p, conf, parent, r)),
      exec[OverwriteByExpressionExec](
        "Overwrite data in a datasource V2 table",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new OverwriteByExpressionExecMeta(p, conf, parent, r)),
      exec[ReplaceDataExec](
        "Replace data in a datasource V2 table (for copy-on-write DELETE operations)",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new ReplaceDataExecMeta(p, conf, parent, r)),
      exec[MergeRowsExec](
        "Process merge rows for copy-on-write MERGE operations",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new GpuMergeRowsExecMeta(p, conf, parent, r)),
      exec[WriteDeltaExec](
        "Write delta (position deletes) in a datasource V2 table " +
          "(for merge-on-read DELETE operations)",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY + TypeSig.BINARY +
          GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new WriteDeltaExecMeta(p, conf, parent, r))
        .disabledByDefault("Merge on read support for iceberg is experimental"),
      InMemoryTableScanUtils.getTableCacheQueryStageExecRule
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

    super.getExecs ++ shimExecs
  }

  override def getTableCacheNonQueryStagePlan(plan: SparkPlan): Option[SparkPlan] = {
    plan match {
      case tcqs: TableCacheQueryStageExec => Some(tcqs.plan)
      case _ => None
    }
  }
}
