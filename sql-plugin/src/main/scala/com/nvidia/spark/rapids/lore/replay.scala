/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExec}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class GpuLoreReplayExec(idxInParent: Int, parentRootPath: String,
    hadoopConf: Broadcast[SerializableConfiguration])
  extends LeafExecNode
  with GpuExec {
  private lazy val rdd = new GpuLoreReplayRDD(sparkSession.sparkContext,
    GpuLore.pathOfChild(new Path(parentRootPath), idxInParent).toString, hadoopConf)
  override def output: Seq[Attribute] = rdd.loreRDDMeta.attrs

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("LoreReplayExec does not support row mode")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    rdd
  }
}

class GpuLoreReplayRDD(sc: SparkContext, rootPathStr: String,
    hadoopConf: Broadcast[SerializableConfiguration])
  extends RDD[ColumnarBatch](sc, Nil) with GpuLoreRDD {

  override def rootPath: Path = new Path(rootPathStr)

  private[lore] val loreRDDMeta: LoreRDDMeta = GpuLore.loadObject(pathOfMeta, sc
    .hadoopConfiguration)

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partitionPath = pathOfPartition(split.index)
    withResource(partitionPath.getFileSystem(hadoopConf.value.value)) { fs =>
      if (!fs.exists(partitionPath)) {
        Iterator.empty
      } else {
        val partitionMeta = GpuLore.loadObject[LoreRDDPartitionMeta](
          pathOfPartitionMeta(split.index), hadoopConf.value.value)
        new Iterator[ColumnarBatch] {
          private var batchIdx: Int = 0

          override def hasNext: Boolean = {
            batchIdx < partitionMeta.numBatches
          }

          override def next(): ColumnarBatch = {
            val batchPath = pathOfBatch(split.index, batchIdx)
            val ret = withResource(batchPath.getFileSystem(hadoopConf.value.value)) { fs =>
              if (!fs.exists(batchPath)) {
                throw new IllegalStateException(s"Batch file $batchPath does not exist")
              }
              withResource(fs.open(batchPath)) { fin =>
                val buffer = IOUtils.toByteArray(fin)
                withResource(Table.readParquet(buffer)) { restoredTable =>
                  GpuColumnVector.from(restoredTable, partitionMeta.dataType.toArray)
                }
              }
            }
            batchIdx += 1
            ret
          }
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until loreRDDMeta.numPartitions).map(LoreReplayPartition).toArray
  }
}

case class LoreReplayPartition(override val index: Int) extends Partition
