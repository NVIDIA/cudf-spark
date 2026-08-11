/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import java.nio.ByteBuffer
import java.util.{Random => JavaRandom}

import scala.collection.mutable
import scala.util.Random
import scala.util.hashing.MurmurHash3

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch

object SamplingUtils {
  private def selectWithoutReplacementFrom(count: Int, rand: Random, cb: ColumnarBatch): Table = {
    val rows = cb.numRows()
    assert(count <= rows)
    if (rows == count) {
      GpuColumnVector.from(cb)
    } else if (count < rows/2) {
      // Randomly select a gather map, without replacement so use a set
      val selected = mutable.Set[Int]()
      while (selected.size < count) {
        selected += rand.nextInt(rows)
      }
      withResource(ColumnVector.fromInts(selected.toSeq: _*)) { gatherMap =>
        withResource(GpuColumnVector.from(cb)) { tab =>
          tab.gather(gatherMap)
        }
      }
    } else {
      // Randomly select rows to remove, without replacement so use a set
      val toRemove = rows - count;
      val notSelected = mutable.Set[Int]()
      while (notSelected.size < toRemove) {
        notSelected += rand.nextInt(rows)
      }
      val selected = (0 until rows).filter(notSelected.contains)
      withResource(ColumnVector.fromInts(selected: _*)) { gatherMap =>
        withResource(GpuColumnVector.from(cb)) { tab =>
          tab.gather(gatherMap)
        }
      }
    }
  }

  /**
   * Random sampling without replacement.
   * @param input iterator to feed batches for sampling.
   * @param fraction the percentage of rows to randomly select
   * @param sorter used to add rows needed for sorting on the CPU later. The sorter should be
   *               setup for the schema of the input data and the output sampled rows will have
   *               any needed rows added to them as the sorter needs to.
   * @param converter used to convert a batch of data to rows. This should have been setup to
   *                  convert to rows based of the expected output for the sorter.
   * @param seed the seed to the random number generator
   * @return the sampled rows
   */
  def randomResample(
      input: Iterator[ColumnarBatch],
      fraction: Double,
      sorter: GpuSorter,
      converter: Iterator[ColumnarBatch] => Iterator[InternalRow],
      seed: Long = Random.nextLong()): Array[InternalRow] = {
    val jRand = new XORShiftRandom(seed)
    val rand = new Random(jRand)
    var runningCb: SpillableColumnarBatch = null
    var totalRowsSeen = 0L
    var totalRowsCollected = 0L
    while (input.hasNext) {
      withResource(input.next()) { cb =>
        // For each batch we need to know how many rows to select from it
        // and how many to throw away from the existing batch
        val rowsInBatch = cb.numRows()
        totalRowsSeen += rowsInBatch
        val totalRowsWanted = (totalRowsSeen * fraction).toLong
        val numRowsToSelectFromBatch = (totalRowsWanted - totalRowsCollected).toInt
        withResource(selectWithoutReplacementFrom(numRowsToSelectFromBatch, rand, cb)) { selected =>
          totalRowsCollected += selected.getRowCount
          if (runningCb == null) {
            runningCb = SpillableColumnarBatch(
              GpuColumnVector.from(selected, GpuColumnVector.extractTypes(cb)),
              SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          } else {
            val concat = withResource(runningCb) { spb =>
              runningCb = null
              withResource(spb.getColumnarBatch()) { cb =>
                withResource(GpuColumnVector.from(cb)) { table =>
                  Table.concatenate(selected, table)
                }
              }
            }
            withResource(concat) { concat =>
              runningCb = SpillableColumnarBatch(
                GpuColumnVector.from(concat, GpuColumnVector.extractTypes(cb)),
                SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
            }
          }
        }
      }
      GpuSemaphore.releaseIfNecessary(TaskContext.get())
    }
    if (runningCb == null) {
      return Array.empty
    }
    // Getting a spilled batch will acquire the semaphore if needed
    val cb = withResource(runningCb) { spb =>
      runningCb = null
      spb.getColumnarBatch()
    }
    val withSortColumns = withResource(cb) { cb =>
      sorter.appendProjectedColumns(cb)
    }
    // The reader will close withSortColumns, but if we want to really be paranoid we should
    // add in a shutdown handler or something like that.
    // Might even want to turn this into a utility function for collect etc.
    val retIterator = converter(new Iterator[ColumnarBatch] {
      var read = false
      override def hasNext: Boolean = !read

      override def next(): ColumnarBatch = {
        read = true
        withSortColumns
      }
    }).map(_.copy())
    retIterator.toArray
  }

  /**
   * Reservoir sampling implementation that also returns the input size.
   * @param input iterator to feed batches for sampling.
   * @param k the number of rows to randomly select.
   * @param sorter used to add rows needed for sorting on the CPU later. The sorter should be
   *               setup for the schema of the input data and the output sampled rows will have
   *               any needed rows added to them as the sorter needs to.
   * @param converter used to convert a batch of data to rows. This should have been setup to
   *                  convert to rows based of the expected output for the sorter.
   * @param seed the seed to the random number generator
   * @return (samples, input size)
   */
  def reservoirSampleAndCount(
      input: Iterator[ColumnarBatch],
      k: Int,
      sorter: GpuSorter,
      converter: Iterator[ColumnarBatch] => Iterator[InternalRow],
      seed: Long = Random.nextLong()) : (Array[InternalRow], Long) = {
    val jRand = new XORShiftRandom(seed)
    val rand = new Random(jRand)
    var runningCb: SpillableColumnarBatch = null
    var numTotalRows = 0L
    var rowsSaved = 0L
    while (input.hasNext) {
      withResource(input.next()) { cb =>
        // For each batch we need to know how many rows to select from it
        // and how many to throw away from the existing batch
        val rowsInBatch = cb.numRows()
        val (numRowsToSelectFromBatch, rowsToDrop) = if (numTotalRows == 0) {
          (Math.min(k, rowsInBatch), 0)
        } else if (numTotalRows + rowsInBatch < k) {
          (rowsInBatch, 0)
        } else {
          val v = (k * rowsInBatch.toDouble / (numTotalRows + rowsInBatch)).toInt
          (v, v)
        }
        numTotalRows += rowsInBatch
        withResource(selectWithoutReplacementFrom(numRowsToSelectFromBatch, rand, cb)) { selected =>
          if (runningCb == null) {
            rowsSaved = selected.getRowCount
            runningCb = SpillableColumnarBatch(
              GpuColumnVector.from(selected, GpuColumnVector.extractTypes(cb)),
              SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          } else {
            withResource(runningCb) { spb =>
              runningCb = null
              withResource(spb.getColumnarBatch()) { cb =>
                val filtered = if (rowsToDrop > 0) {
                  selectWithoutReplacementFrom(cb.numRows() - rowsToDrop, rand, cb)
                } else {
                  GpuColumnVector.from(cb)
                }
                val concat = withResource(filtered) { filtered =>
                  Table.concatenate(selected, filtered)
                }
                withResource(concat) { concat =>
                  rowsSaved = concat.getRowCount
                  runningCb = SpillableColumnarBatch(
                    GpuColumnVector.from(concat, GpuColumnVector.extractTypes(cb)),
                    SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
                }
              }
            }
          }
        }
      }
      GpuSemaphore.releaseIfNecessary(TaskContext.get())
    }
    if (runningCb == null) {
      // Nothing to sort
      return (Array.empty, numTotalRows)
    }
    // Getting a spilled batch will acquire the semaphore if needed
    val cb = withResource(runningCb) { spb =>
      runningCb = null
      spb.getColumnarBatch()
    }
    val withSortColumns = withResource(cb) { cb =>
      sorter.appendProjectedColumns(cb)
    }
    // The reader will close withSortColumns, but if we want to really be paranoid we should
    // add in a shutdown handler or something like that.
    // Might even want to turn this into a utility function for collect etc.
    val retIterator = converter(new Iterator[ColumnarBatch] {
      var read = false
      override def hasNext: Boolean = !read

      override def next(): ColumnarBatch = {
        read = true
        withSortColumns
      }
    }).map(_.copy())
    (retIterator.toArray, numTotalRows)
  }
}

/**
 * This class implements a XORShift random number generator algorithm
 * Source:
 * Marsaglia, G. (2003). Xorshift RNGs. Journal of Statistical Software, Vol. 8, Issue 14.
 * @see <a href="http://www.jstatsoft.org/v08/i14/paper">Paper</a>
 * This implementation is approximately 3.5 times faster than
 * {@link java.util.Random java.util.Random}, partly because of the algorithm, but also due
 * to renouncing thread safety. JDK's implementation uses an AtomicLong seed, this class
 * uses a regular Long. We can forgo thread safety since we use a new instance of the RNG
 * for each thread.
 */
private[spark] class XORShiftRandom(init: Long) extends JavaRandom(init) {

  def this() = this(System.nanoTime)

  private var seed = XORShiftRandom.hashSeed(init)

  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
  override protected def next(bits: Int): Int = {
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    seed = nextSeed
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long): Unit = {
    seed = XORShiftRandom.hashSeed(s)
  }
}

/** Contains benchmark method and main method to run benchmark of the RNG */
private[spark] object XORShiftRandom {
  /** Hash seeds to have 0/1 bits throughout. */
  private def hashSeed(seed: Long): Long = {
    val bytes = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(seed).array()
    val lowBits = MurmurHash3.bytesHash(bytes, MurmurHash3.arraySeed)
    val highBits = MurmurHash3.bytesHash(bytes, lowBits)
    (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
  }
}
