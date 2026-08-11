/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm._

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.rapids.execution.{GpuBatchSubPartitioner, GpuBatchSubPartitionIterator, GpuSubPartitionPairIterator}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSubPartitionSuite extends SparkQueryCompareTestSuite {
  val attrs: java.util.List[Attribute] =
    java.util.Arrays.asList(AttributeReference("test", IntegerType)())
  private val boundKeys =
    Seq(GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "test"))

  test("Sub-partitioner with empty iterator") {
    withGpuSparkSession { _ =>
      val subPartitioner = new GpuBatchSubPartitioner(
        Iterator.empty,
        boundKeys,
        numPartitions = 0,
        hashSeed = 100)

      // at least two partitions even given 0
      assertResult(expected = 2)(subPartitioner.partitionsCount)
      assertResult(expected = 0)(subPartitioner.batchesCount)
      val ids = 0 until subPartitioner.partitionsCount
      // every partition is empty
      assert(ids.forall(subPartitioner.getBatchesByPartition(_).isEmpty))
      // every partition becomes null after being released
      // no actual resource so no need to close
      ids.foreach(subPartitioner.releaseBatchesByPartition)
      assert(ids.forall(subPartitioner.getBatchesByPartition(_) == null))
      subPartitioner.close()
      // repeated close should be ok
      subPartitioner.close()
    }
  }

  test("Sub-partitioner with nonempty iterator of one empty batch") {
    withGpuSparkSession { _ =>
      closeOnExcept(GpuColumnVector.emptyBatch(attrs)) { emptyBatch =>
        val subPartitioner = new GpuBatchSubPartitioner(
          Seq(emptyBatch).toIterator,
          boundKeys,
          numPartitions = 5,
          hashSeed = 100)

        assertResult(expected = 5)(subPartitioner.partitionsCount)
        // empty batch is skipped
        assertResult(expected = 0)(subPartitioner.batchesCount)
        val ids = 0 until subPartitioner.partitionsCount
        // every partition is empty
        assert(ids.forall(subPartitioner.getBatchesByPartition(_).isEmpty))
        // every partition becomes null after being released.
        // no actual resource so no need to close
        ids.foreach(subPartitioner.releaseBatchesByPartition)
        assert(ids.forall(subPartitioner.getBatchesByPartition(_) == null))
        subPartitioner.close()
      }
    }
  }

  test("Sub-partitioner with nonempty iterator of one nonempty batch") {
    withGpuSparkSession { _ =>
      closeOnExcept {
        val col = GpuColumnVector.from(ColumnVector.fromInts(1,2,2,3,3,3), IntegerType)
        new ColumnarBatch(Array(col), col.getRowCount.toInt)
      } { nonemptyBatch =>
        val subPartitioner = new GpuBatchSubPartitioner(
          Seq(nonemptyBatch).toIterator,
          boundKeys,
          numPartitions = 5,
          hashSeed = 100)

        assertResult(expected = 5)(subPartitioner.partitionsCount)
        // nonempty batches exist
        assert(subPartitioner.batchesCount > 0)
        val ids = 0 until subPartitioner.partitionsCount
        var actualRowNum = 0
        ids.foreach { id =>
          withResource(subPartitioner.releaseBatchesByPartition(id)) { batches =>
            actualRowNum += batches.map(_.numRows()).sum
          }
        }
        assertResult(nonemptyBatch.numRows())(actualRowNum)
        // every partition becomes null after being released
        assert(ids.forall(subPartitioner.getBatchesByPartition(_) == null))
        subPartitioner.close()
      }
    }
  }

  test("Sub-partitioner iterator with empty partitions") {
    withGpuSparkSession { _ =>
      closeOnExcept(GpuColumnVector.emptyBatch(attrs)) { emptyBatch =>
        val subPartitioner = new GpuBatchSubPartitioner(
          Seq(emptyBatch).toIterator,
          boundKeys,
          numPartitions = 5,
          hashSeed = 100)
        val subIter = new GpuBatchSubPartitionIterator(
          subPartitioner,
          targetBatchSize = 12L)

        // return empty partitions one by one
        val partCounts = ArrayBuffer(1, 1, 1, 1, 1)
        while (subIter.hasNext) {
          val (ids, batch) = subIter.next()
          withResource(batch) { _ =>
            partCounts -= ids.length
            assert(ids.nonEmpty)
            assert(batch.isEmpty)
          }
        }
        assert(partCounts.isEmpty, partCounts)
        subPartitioner.close()
      }
    }
  }

  test("Sub-partitioner iterator with nonempty partitions") {
    withGpuSparkSession { _ =>
      closeOnExcept {
        val col = GpuColumnVector.from(ColumnVector.fromInts(1, 2, 2, 3, 3, 3), IntegerType)
        new ColumnarBatch(Array(col), col.getRowCount.toInt)
      } { nonemptyBatch =>
        val subPartitioner = new GpuBatchSubPartitioner(
          Seq(nonemptyBatch).toIterator,
          boundKeys,
          numPartitions = 5,
          hashSeed = 100)
        val subIter = new GpuBatchSubPartitionIterator(
          subPartitioner,
          targetBatchSize = 12L)

        var actualRowNum = 0
        while(subIter.hasNext) {
          val (ids, batch) = subIter.next()
          withResource(batch) { _ =>
            assert(ids.nonEmpty)
            batch.foreach { cb =>
              // got nonempty partition, add its row number
              actualRowNum += cb.numRows()
            }
          }
        }
        assertResult(nonemptyBatch.numRows())(actualRowNum)
        subPartitioner.close()
      }
    }
  }

  test("Sub-pair iterator repartition because of too big batch") {
    withGpuSparkSession { _ =>
      // cudf aligns output to 64 bytes for contiguous split used by the sub partitioner, so
      // generate a little large data for this test.
      val largeData = 0 until 1024
      closeOnExcept {
        val col = GpuColumnVector.from(ColumnVector.fromInts(largeData: _*), IntegerType)
        new ColumnarBatch(Array(col), col.getRowCount.toInt)
      } { nonemptyBatch =>
        val subPairIter = new GpuSubPartitionPairIterator(
          Seq(nonemptyBatch).toIterator, boundKeys,
          Iterator.empty, boundKeys,
          numPartitions = 2, targetBatchSize = 1024)

        var actualRowNum, partCount = 0
        while (subPairIter.hasNext) {
          withResource(subPairIter.next()) { pair =>
            val (buildCb, streamCbs) = pair.get
            assert(streamCbs.isEmpty)
            buildCb.foreach { cb =>
              // got nonempty partition, add its row number
              actualRowNum += cb.numRows()
            }
            partCount += 1
          }
        }
        assertResult(nonemptyBatch.numRows())(actualRowNum)
        // The final partition number should be larger than the original one (2).
        assert(partCount > 2)
        // Repartitioning should happen.
        assert(subPairIter.isRepartitioned)
        subPairIter.close()
      }
    }
  }

  test("Sub-pair iterator repartition because of skewed data") {
    withGpuSparkSession { _ =>
      // cudf aligns output to 64 bytes for contiguous split used by the sub partitioner, so
      // generate a little large data for this test.
      val largeData = (0 until 1000).map(_ => 1) ++ (0 until 24).map(_ => 2)
      closeOnExcept {
        val col = GpuColumnVector.from(ColumnVector.fromInts(largeData: _*), IntegerType)
        new ColumnarBatch(Array(col), col.getRowCount.toInt)
      } { nonemptyBatch =>
        val subPairIter = new GpuSubPartitionPairIterator(
          Seq(nonemptyBatch).toIterator, boundKeys,
          Iterator.empty, boundKeys,
          numPartitions = 10, targetBatchSize = 1024)

        var actualRowNum, partCount = 0
        while (subPairIter.hasNext) {
          withResource(subPairIter.next()) { pair =>
            val (buildCb, streamCbs) = pair.get
            assert(streamCbs.isEmpty)
            buildCb.foreach { cb =>
              // got nonempty partition, add its row number
              actualRowNum += cb.numRows()
              partCount += 1
            }
          }
        }
        assertResult(nonemptyBatch.numRows())(actualRowNum)
        // There should be two nonempty partitions, one for 1000 "1"s and one for 24 "2"s.
        assert(partCount == 2)
        // Repartitioning should happen.
        assert(subPairIter.isRepartitioned)
        subPairIter.close()
      }
    }
  }
}
