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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, NamedExpression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.{GpuInputFileName, GpuInsertIntoHadoopFsRelationCommand, GpuOrcFileFormat}
import org.apache.spark.sql.types.StringType

/**
 * A unary GPU expression that, given a StringType child backed by a STRING-keyed DICTIONARY32
 * column, returns a valid StringType result without reading the strings: it passes the column
 * through, possibly replacing nulls with a scalar. InputFileNameDictionaryRule lets a
 * dictionary-backed input_file_name() column flow through such an expression.
 */
trait GpuStringDictionaryPassThrough

/**
 * Decides, for each `input_file_name()` in the plan, whether GpuInputFileName returns a
 * DICTIONARY32 column instead of a STRING column that repeats the file path on every row
 * (https://github.com/NVIDIA/cudf-spark/issues/13918).
 *
 * A dictionary-backed StringType column is only safe where every consumer can handle it: most
 * string, cast and binary operations, the writers and the shuffle serializers cannot. This walks
 * the plan from the root down, carrying the output columns of each operator that every operator
 * above it accepts as dictionaries. Dictionary output is turned on only for a top-level
 * `GpuAlias(GpuInputFileName(), name)` in a GpuProjectExec whose output column is in that set;
 * every other use keeps the STRING column.
 *
 * Operators that accept a dictionary column:
 *  - GpuColumnarToRowExec, unless it exports the columnar RDD. Rows read strings through
 *    RapidsHostColumnVectorCore, which looks up the key per row, or through
 *    RowConversion.convertToRows, which decodes inside a split-and-retry block.
 *  - GpuDataWritingCommandExec for an unpartitioned, unbucketed Parquet or ORC insert (see
 *    writesDecodedDictionaries), through the GpuWriteFilesLike node of planned writes.
 *    ColumnarOutputWriter decodes dictionaries inside a split-and-retry block before encoding.
 *  - GpuProjectExec, for columns it passes through unchanged (possibly aliased or wrapped in a
 *    GpuStringDictionaryPassThrough expression).
 *  - GpuFilterExec, for columns its condition does not reference.
 *  - GpuCoalesceBatches, GpuLocalLimitExec and GpuGlobalLimitExec.
 *
 * The rule decides both ways, so applying it again to a re-optimized plan is safe.
 */
object InputFileNameDictionaryRule {

  def apply(plan: SparkPlan): SparkPlan = rewrite(plan, AttributeSet.empty)

  /**
   * @param plan    the plan to rewrite
   * @param allowed the output columns of `plan` that every operator above it accepts as
   *                dictionaries
   */
  private def rewrite(plan: SparkPlan, allowed: AttributeSet): SparkPlan = plan match {
    case c2r: GpuColumnarToRowExec =>
      val childAllowed = if (c2r.exportColumnarRdd) AttributeSet.empty else c2r.child.outputSet
      c2r.withNewChildren(Seq(rewrite(c2r.child, childAllowed)))
    case write: GpuDataWritingCommandExec if writesDecodedDictionaries(write.cmd) =>
      val newChild = write.child match {
        case writeFiles if writeFiles.isInstanceOf[GpuWriteFilesLike] =>
          val query = writeFiles.children.head
          writeFiles.withNewChildren(Seq(rewrite(query, query.outputSet)))
        case query =>
          rewrite(query, query.outputSet)
      }
      write.withNewChildren(Seq(newChild))
    case project: GpuProjectExec =>
      rewriteProject(project, allowed)
    case filter: GpuFilterExec =>
      filter.withNewChildren(Seq(rewrite(filter.child, allowed -- filter.condition.references)))
    case coalesce: GpuCoalesceBatches =>
      coalesce.withNewChildren(Seq(rewrite(coalesce.child, allowed)))
    case limit: GpuLocalLimitExec =>
      limit.withNewChildren(Seq(rewrite(limit.child, allowed)))
    case limit: GpuGlobalLimitExec =>
      limit.withNewChildren(Seq(rewrite(limit.child, allowed)))
    case other =>
      other.withNewChildren(other.children.map(rewrite(_, AttributeSet.empty)))
  }

  /**
   * Whether every column a write command receives reaches ColumnarOutputWriter, which decodes
   * dictionaries, without any device operation in between. Only unpartitioned, unbucketed
   * Parquet and ORC inserts qualify. Dynamic partitions and buckets make GpuFileFormatWriter
   * sort the batches, possibly with a GpuSortExec it adds at run time where this rule cannot see
   * it, and the partitioned writers split and group the batches on the device. Hive and Delta
   * writes use other commands.
   */
  private def writesDecodedDictionaries(cmd: GpuDataWritingCommand): Boolean = cmd match {
    case insert: GpuInsertIntoHadoopFsRelationCommand =>
      insert.partitionColumns.isEmpty && insert.bucketSpec.isEmpty &&
        (insert.fileFormat.isInstanceOf[GpuParquetFileFormat] ||
          insert.fileFormat.isInstanceOf[GpuOrcFileFormat])
    case _ => false
  }

  private def rewriteProject(project: GpuProjectExec, allowed: AttributeSet): SparkPlan = {
    val newProjectList = project.projectList.map {
      case alias @ GpuAlias(fileName: GpuInputFileName, _) =>
        val outputDictionary = allowed.contains(alias.toAttribute)
        if (fileName.outputDictionary == outputDictionary) {
          alias
        } else {
          alias.withNewChildren(Seq(fileName.copy(outputDictionary = outputDictionary)))
            .asInstanceOf[NamedExpression]
        }
      case other => other
    }

    // A child column stays allowed only if every use of it in this project is a pass-through to
    // an allowed output column.
    val passThroughs = project.projectList.flatMap { expr =>
      passThroughSource(expr).map(source => (source, expr.toAttribute))
    }
    val otherReferences = AttributeSet(
      project.projectList.filter(passThroughSource(_).isEmpty).flatMap(_.references))
    val childAllowed = AttributeSet(passThroughs.collect {
      case (source, output) if allowed.contains(output) => source
    }).filter { source =>
      !otherReferences.contains(source) &&
        passThroughs.forall { case (s, output) => s.exprId != source.exprId ||
          allowed.contains(output) }
    }

    val newChild = rewrite(project.child, childAllowed)
    if (newProjectList == project.projectList) {
      project.withNewChildren(Seq(newChild))
    } else {
      // Only the encoding of input_file_name() changed, which does not change what the CPU
      // expressions describe, so keep them as they are.
      project.copy(projectList = newProjectList, child = newChild)(project.cpuProjectList)
    }
  }

  /** The child column that `expr` passes through to its output unchanged, if it does. */
  private def passThroughSource(expr: NamedExpression): Option[Attribute] = expr match {
    case attr: AttributeReference => Some(attr)
    case GpuAlias(attr: AttributeReference, _) => Some(attr)
    case GpuAlias(passThrough: GpuStringDictionaryPassThrough, _) =>
      passThrough.children match {
        case Seq(attr: AttributeReference) if attr.dataType.isInstanceOf[StringType] => Some(attr)
        case _ => None
      }
    case _ => None
  }
}
