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

package org.apache.spark.sql.rapids.execution

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RowToColumnarExec, RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, NullType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

/** CPU transition for a PCBS AQE cache whose schema contains NullType. */
case class PCBSNullSafeRowToColumnarExec(child: SparkPlan) extends RowToColumnarTransition {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def supportsColumnar: Boolean = true
  override protected def doExecute(): RDD[InternalRow] = child.execute()
  override def doExecuteBroadcast[T](): Broadcast[T] = child.doExecuteBroadcast[T]()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val outputSchema = schema
    val timeZone = conf.sessionLocalTimeZone
    val batchSize = conf.columnBatchSize
    child.execute().mapPartitions { rows =>
      val batches = new PCBSNullSafeArrowIterator(rows, outputSchema, timeZone, batchSize)
      Option(TaskContext.get()).foreach { tc =>
        onTaskCompletion(tc) { batches.close() }
      }
      batches
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object PCBSNullSafeRowToColumnarExec {
  private[execution] def unambiguousArrowType(dataType: DataType): DataType = dataType match {
    case StructType(fields) =>
      val rename = fields.map(_.name).distinct.length != fields.length
      StructType(fields.zipWithIndex.map { case (field, index) =>
        field.copy(name = if (rename) s"_pcbs_$index" else field.name,
          dataType = unambiguousArrowType(field.dataType))
      })
    case ArrayType(elementType, nullable) =>
      ArrayType(unambiguousArrowType(elementType), nullable)
    case MapType(keyType, valueType, nullable) =>
      MapType(unambiguousArrowType(keyType), unambiguousArrowType(valueType), nullable)
    case _ => dataType
  }

  private def containsNullType(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case ArrayType(elementType, _) => containsNullType(elementType)
    case MapType(keyType, valueType, _) => containsNullType(keyType) || containsNullType(valueType)
    case StructType(fields) => fields.exists(field => containsNullType(field.dataType))
    case _ => false
  }

  def replaceUnsupportedTransition(plan: SparkPlan): SparkPlan = plan match {
    // AQE inserts this root transition at execution time. Checking here also handles a cache
    // registered with GPU SQL enabled and first materialized after disabling it or switching
    // to explainOnly.
    // A schema-only capability check cannot bind itself to that later execution mode.
    case RowToColumnarExec(child)
        if plan.conf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER) ==
          "com.nvidia.spark.ParquetCachedBatchSerializer" && containsNullType(child.schema) =>
      PCBSNullSafeRowToColumnarExec(child)
    case _ => plan
  }
}

private class PCBSNullSafeArrowIterator(
    rows: Iterator[InternalRow],
    schema: StructType,
    timeZone: String,
    batchSize: Int,
    parentAllocator: BufferAllocator = ArrowUtils.rootAllocator)
    extends Iterator[ColumnarBatch] with AutoCloseable {
  // Spark 3.3's Arrow writer can reuse a vector for duplicate struct field names. Physical names
  // change here: row conversion and PCBS both address values by ordinal, and output stays intact.
  private val arrowSchema = ArrowUtilsShim.toArrowSchema(
    PCBSNullSafeRowToColumnarExec.unambiguousArrowType(schema).asInstanceOf[StructType], timeZone,
    errorOnDuplicatedFieldNames = false)
  private var current: ColumnarBatch = null

  override def hasNext: Boolean = {
    if (rows.hasNext) {
      true
    } else {
      close()
      false
    }
  }

  override def next(): ColumnarBatch = {
    close()
    if (!rows.hasNext) {
      throw new NoSuchElementException("No more PCBS rows")
    }
    val allocator = parentAllocator.newChildAllocator("PCBS NullType cache", 0, Long.MaxValue)
    current = closeOnExcept(allocator) { _ =>
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      closeOnExcept(root) { _ =>
        val writer = ArrowWriter.create(root)
        var count = 0
        while (count < batchSize && rows.hasNext) {
          writer.write(rows.next())
          count += 1
        }
        writer.finish()
        val columns = root.getFieldVectors.asScala.map { vector =>
          new ArrowColumnVector(vector): ColumnVector
        }.toArray
        new ColumnarBatch(columns, count) {
          private var closed = false
          override def close(): Unit = synchronized {
            if (!closed) {
              closed = true
              withResource(allocator) { _ => root.close() }
            }
          }
        }
      }
    }
    current
  }

  override def close(): Unit = {
    if (current != null) {
      current.close()
      current = null
    }
  }
}
