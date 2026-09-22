/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, PartitionedTable}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, HiveHash, Murmur3Hash}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.rapids.{GpuHashExpression, GpuHiveHash, GpuMurmur3Hash, GpuPmod}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Trait requiring a hash function to provide the partitioning functionality for its children.
 */
trait GpuHashPartitioner extends Serializable {
  protected def hashFunc: GpuHashExpression

  protected final def hashPartitionAndClose(batch: ColumnarBatch, numPartitions: Int,
      nvtxId: NvtxId): PartitionedTable = {
    withResource(batch) { cb =>
      val parts = nvtxId {
        if (hashFunc.children.isEmpty) {
          // Spark permits HashPartitioning with no expressions. The hash is the initial seed for
          // every row, so preserve the CPU partition ID and partition count without asking cuDF
          // to hash zero input columns.
          val emptyHash = hashFunc match {
            case GpuMurmur3Hash(_, seed) => seed
            case GpuHiveHash(_) => 0
            case other => throw new IllegalStateException(
              s"Unsupported empty hash expression: ${other.getClass.getSimpleName}")
          }
          val partitionId = Math.floorMod(emptyHash, numPartitions)
          withResource(GpuScalar.from(partitionId, IntegerType)) { partLit =>
            ColumnVector.fromScalar(partLit, cb.numRows)
          }
        } else {
          withResource(hashFunc.columnarEval(cb)) { hash =>
            withResource(GpuScalar.from(numPartitions, IntegerType)) { partsLit =>
              hash.getBase.pmod(partsLit, DType.INT32)
            }
          }
        }
      }
      withResource(parts) { parts =>
        withResource(GpuColumnVector.from(cb)) { table =>
          table.partition(parts, numPartitions)
        }
      }
    }
  }
}

abstract class GpuHashPartitioningBase(expressions: Seq[Expression], numPartitions: Int,
    hashMode: HashMode)
  extends GpuExpression with ShimExpression with GpuPartitioning with GpuHashPartitioner {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override lazy val hashFunc: GpuHashExpression =
    GpuHashPartitioningBase.toHashExpr(hashMode, expressions)

  def partitionInternalAndClose(batch: ColumnarBatch): (Array[Int], Array[GpuColumnVector]) = {
    val types = GpuColumnVector.extractTypes(batch)
    val partedTable = hashPartitionAndClose(batch, numPartitions, NvtxRegistry.CALCULATE_PART)
    withResource(partedTable) { partedTable =>
      // Table.partition() returns numPartitions + 1 elements (last is total row count),
      // but downstream code expects numPartitions elements, so drop the last one
      val parts = partedTable.getPartitions.dropRight(1)
      val tp = partedTable.getTable
      val columns = (0 until partedTable.getNumberOfColumns.toInt).zip(types).map {
        case (index, sparkType) =>
          GpuColumnVector.from(tp.getColumn(index).incRefCount(), sparkType)
      }
      (parts, columns.toArray)
    }
  }

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    //  We are doing this here because the cudf partition command is at this level
    NvtxRegistry.HASH_PARTITION {
      val numRows = batch.numRows
      val (partitionIndexes, partitionColumns) = {
        NvtxRegistry.HASH_PARTITION_SLICE {
          partitionInternalAndClose(batch)
        }
      }
      sliceInternalGpuOrCpuAndClose(numRows, partitionIndexes, partitionColumns)
    }
  }

  def partitionIdExpression: GpuExpression = GpuPmod(hashFunc, GpuLiteral(numPartitions))
}

object GpuHashPartitioningBase extends Logging {

  val DEFAULT_HASH_SEED: Int = 42

  private[rapids] def toHashExpr(hashMode: HashMode, keys: Seq[Expression],
      seed: Int = DEFAULT_HASH_SEED): GpuHashExpression = hashMode match {
    case Murmur3Mode => GpuMurmur3Hash(keys, seed)
    case HiveMode => GpuHiveHash(keys)
    case _ => throw new UnsupportedOperationException(s"Unsupported hash mode: $hashMode")
  }

  private[rapids] def hashModeFromCpu(cpuHp: HashPartitioning, conf: RapidsConf): HashMode = {
    var hashMode: HashMode = Murmur3Mode // default to murmur3
    if (conf.isHashFuncPartitioningEnabled && conf.isIncompatEnabled) {
      // One Spark distribution introduces a new field to define the hash function
      // used by HashPartitioning. Since there is no shim for it, so here leverages
      // Java reflection to access it.
      try {
        val hashModeMethod = cpuHp.getClass.getMethod("hashingFunctionClass")
        hashMode = hashModeMethod.invoke(cpuHp) match {
          case m if m == classOf[Murmur3Hash] => Murmur3Mode
          case h if h == classOf[HiveHash] => HiveMode
          case o => UnsupportedMode(o.asInstanceOf[Class[_]].getSimpleName)
        }
        logDebug(s"Found hash function '$hashMode' from CPU hash partitioning.")
      } catch {
        case _: NoSuchMethodException => // not the customized spark distributions, noop
          logDebug("Use murmur3 for GPU hash partitioning.")
      }
    }
    hashMode
  }

}

sealed trait HashMode extends Serializable

case object Murmur3Mode extends HashMode
case object HiveMode extends HashMode
case class UnsupportedMode(modeName: String) extends HashMode {
  override def toString: String = modeName
}
