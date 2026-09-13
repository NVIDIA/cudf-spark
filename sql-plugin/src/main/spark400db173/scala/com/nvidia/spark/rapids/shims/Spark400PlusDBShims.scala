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

/*** spark-rapids-shim-json-lines
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.{SparkSession => SqlSparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Expression, KnownNotContainsNull}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.rapids.shims.InvokeExprMeta

trait Spark400PlusDBShims extends Spark341PlusDBShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[KnownNotContainsNull](
        "Tags an array expression as known to not contain null elements (e.g. from array_compact).",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.BINARY + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
          TypeSig.ARRAY.nested(TypeSig.all)),
        (a, conf, p, r) => new UnaryExprMeta[KnownNotContainsNull](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression =
            GpuKnownNotContainsNull(child)
        }),
      GpuOverrides.expr[Invoke](
        "Calls the specified function on an object. This is a wrapper to other expressions, so " +
          "can not know the details in advance. E.g.: between is replaced by " +
          "And(GreaterThanOrEqual(ref, lower), LessThanOrEqual(ref, upper);  StructToJson is " +
          "replaced by Invoke(Literal(StructsToJsonEvaluator), evaluate, string_type, arguments)",
        InvokeCheck,
        InvokeExprMeta)
        .note("The supported types are not deterministic since it's a dynamic expression")
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }

  override def listPartitionsByFilter(
      sparkSession: SqlSparkSession,
      tableName: TableIdentifier,
      predicates: Seq[Expression],
      resolvedCatalogTable: Option[CatalogTable]): Seq[CatalogTablePartition] = {
    sparkSession.sessionState.catalog.listPartitionsByFilter(
      tableName, predicates, resolvedCatalogTable)
  }

  override def listPartitions(
      sparkSession: SqlSparkSession,
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec],
      resolvedCatalogTable: Option[CatalogTable]): Seq[CatalogTablePartition] = {
    sparkSession.sessionState.catalog.listPartitions(
      tableName, partialSpec, resolvedCatalogTable = resolvedCatalogTable)
  }

  override def getPartitionFiles(partition: FilePartition): Seq[PartitionedFile] = {
    partition.filesWithAbsolutePaths.toSeq
  }
}
