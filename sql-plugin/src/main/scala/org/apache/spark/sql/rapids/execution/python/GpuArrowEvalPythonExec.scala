/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.AssertUtils.assertInTests
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.shims.{GpuArrowPythonRunner, PythonArgumentUtils}
import org.apache.spark.sql.rapids.shims.{ArrowUtilsShim, DataTypeUtilsShim}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This iterator will round incoming batches to multiples of targetRoundoff rows, if possible.
 * The last batch might not be a multiple of it.
 * @param wrapped the incoming ColumnarBatch iterator.
 * @param targetRoundoff the target multiple number of rows
 * @param inputRows metric for rows read.
 * @param inputBatches metric for batches read
 */
class RebatchingRoundoffIterator(
    wrapped: Iterator[ColumnarBatch],
    schema: StructType,
    targetRoundoff: Int,
    inputRows: GpuMetric,
    inputBatches: GpuMetric)
    extends Iterator[ColumnarBatch] {
  var pending: Option[SpillableColumnarBatch] = None

  onTaskCompletion {
    pending.foreach(_.close())
    pending = None
  }

  override def hasNext: Boolean = pending.isDefined || wrapped.hasNext

  private[this] def popPending(): ColumnarBatch = {
    withResource(pending.get) { scb =>
      pending = None
      scb.getColumnarBatch()
    }
  }

  private[this] def concatRowsOnlyBatch(cbs: ColumnarBatch*): ColumnarBatch = {
    if (cbs.length == 1) {
      return cbs.head
    }
    withResource(cbs) { _ =>
      val totalRowsNum = cbs.map(_.numRows().toLong).sum
      if (totalRowsNum != totalRowsNum.toInt) {
        throw new IllegalStateException("Cannot support a batch larger that MAX INT rows")
      }
      new ColumnarBatch(Array.empty, totalRowsNum.toInt)
    }
  }

  private[this] def concat(l: ColumnarBatch, r: ColumnarBatch): ColumnarBatch = {
    assert(l.numCols() == r.numCols())
    if (l.numCols() > 0) {
      withResource(GpuColumnVector.from(l)) { lTable =>
        withResource(GpuColumnVector.from(r)) { rTable =>
          withResource(Table.concatenate(lTable, rTable)) { concatTable =>
            GpuColumnVector.from(concatTable, GpuColumnVector.extractTypes(l))
          }
        }
      }
    } else { // rows only batches
      concatRowsOnlyBatch(l, r)
    }
  }

  private[this] def fillAndConcat(batches: ArrayBuffer[SpillableColumnarBatch]): ColumnarBatch = {
    var rowsSoFar = batches.map(_.numRows()).sum
    while (wrapped.hasNext && rowsSoFar < targetRoundoff) {
      val got = wrapped.next()
      inputBatches += 1
      inputRows += got.numRows()
      rowsSoFar += got.numRows()
      batches.append(SpillableColumnarBatch(got, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
    }
    val toConcat = batches.toArray.safeMap(_.getColumnarBatch())
    assert(toConcat.nonEmpty, "no batches to be concatenated")
    // expect all batches have the same number of columns
    if (toConcat.head.numCols() > 0) {
      ConcatAndConsumeAll.buildNonEmptyBatch(toConcat, schema)
    } else {
      concatRowsOnlyBatch(toConcat: _*)
    }
  }

  override def next(): ColumnarBatch = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val combined : ColumnarBatch = if (pending.isDefined) {
      if (!wrapped.hasNext) {
        // No more data return what is in pending
        popPending()
      } else {
        // Don't read pending yet, because we are going to call next to get enough data.
        // The target number of rows is typically small enough that we will be able to do this
        // in a single call.
        val rowsNeeded = targetRoundoff - pending.get.numRows()
        val cb = wrapped.next()
        inputBatches += 1
        inputRows += cb.numRows()
        if (cb.numRows() >= rowsNeeded) {
          withResource(cb) { cb =>
            withResource(popPending()) { fromPending =>
              concat(fromPending, cb)
            }
          }
        } else {
          // If that does not work then we will need to fall back to slower special case code
          val batches: ArrayBuffer[SpillableColumnarBatch] = ArrayBuffer.empty
          try {
            val localPending = pending.get
            localPending.setSpillPriority(SpillPriorities.ACTIVE_BATCHING_PRIORITY)
            batches.append(localPending)
            pending = None
            batches.append(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
            fillAndConcat(batches)
          } finally {
            batches.safeClose()
          }
        }
      }
    } else {
      val cb = wrapped.next()
      inputBatches += 1
      inputRows += cb.numRows()
      if (cb.numRows() >= targetRoundoff) {
        cb
      } else {
        val batches: ArrayBuffer[SpillableColumnarBatch] = ArrayBuffer.empty
        try {
          batches.append(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
          fillAndConcat(batches)
        } finally {
          batches.safeClose()
        }
      }
    }

    val rc: Long = combined.numRows()
    val numCols = combined.numCols()

    if (rc % targetRoundoff == 0 || rc < targetRoundoff || numCols == 0) {
      return combined
    }

    val splitIndex = (targetRoundoff * (rc/targetRoundoff)).toInt
    val split = withResource(combined) { combinedCb =>
      withResource(GpuColumnVector.from(combinedCb)) { combinedTable =>
        combinedTable.contiguousSplit(splitIndex)
      }
    }
    withResource(split) { split =>
      assert(pending.isEmpty)
      pending =
          Some(SpillableColumnarBatch(GpuColumnVectorFromBuffer.from(split.last,
            GpuColumnVector.extractTypes(schema)),
            SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      GpuColumnVectorFromBuffer.from(split.head, GpuColumnVector.extractTypes(schema))
    }
  }
}

/**
 * A trait provides dedicated APIs for GPU reading batches from python.
 * This is also for easy type declarations since it is implemented by an inner class
 * of BatchProducer.
 */
trait BatchQueue {
  /** Return and remove the first batch in the cache. Caller should close it. */
  def remove(): SpillableColumnarBatch

  /** Get the number of rows in the next batch, without actually getting the batch. */
  def peekBatchNumRows(): Int
}

/**
 * It accepts an iterator as input and will cache the batches when pulling them in from
 * the input for later combination with batches coming back from python by the reader.
 * It also supports an optional converter to convert input batches and put the converted
 * result to the cache queue. This is for GpuAggregateInPandas to build and cache key
 * batches.
 *
 * Call "getBatchQueue" to get the internal cache queue and specify it to the output
 * combination iterator.
 * To access the batches from input, call "asIterator" to get the output iterator.
 */
class BatchProducer(
    input: Iterator[ColumnarBatch],
    converter: Option[ColumnarBatch => ColumnarBatch] = None
) extends AutoCloseable { producer =>

  Option(TaskContext.get()).foreach(onTaskCompletion(_)(close()))

  // A queue that holds the pending batches that need to line up with and combined
  // with batches coming back from python.
  private[this] val batchQueue = new BatchQueueImpl

  /** Get the internal BatchQueue */
  def getBatchQueue: BatchQueue = batchQueue

  // The cache that holds the pending batches pulled in by the "produce" call for
  // the reader peeking the next rows number when the "batchQueue" is empty, and
  // consumed by the iterator returned from "asIterator".
  // (In fact, there is usually only ONE batch. But using a queue here is because in
  // theory "produce" can be called multiple times, then more than one batch can be
  // pulled in.)
  private[this] val pendingOutput = mutable.Queue[SpillableColumnarBatch]()

  private def produce(): ColumnarBatch = {
    if (input.hasNext) {
      val cb = input.next()
      // Need to duplicate this batch for "next"
      pendingOutput.enqueue(SpillableColumnarBatch(GpuColumnVector.incRefCounts(cb),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      cb
    } else {
      null
    }
  }

  /** Return an iterator to access the batches from the input */
  def asIterator: Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {

      override def hasNext: Boolean = producer.synchronized {
        pendingOutput.nonEmpty || input.hasNext
      }

      override def next(): ColumnarBatch = producer.synchronized {
        if (!hasNext) {
          throw new NoSuchElementException()
        }
        if (pendingOutput.nonEmpty) {
          withResource(pendingOutput.dequeue()) { scb =>
            scb.getColumnarBatch()
          }
        } else {
          closeOnExcept(input.next()) { cb =>
            // Need to duplicate it for later combination with Python output
            batchQueue.add(GpuColumnVector.incRefCounts(cb))
            cb
          }
        }
      }
    }
  }

  override def close(): Unit = producer.synchronized {
    batchQueue.close()
    while (pendingOutput.nonEmpty) {
      pendingOutput.dequeue().close()
    }
  }

  // Put this batch queue inside the BatchProducer to share the same lock with the
  // output iterator returned by "asIterator" and make sure the batch movement from
  // input iterator to this queue is an atomic operation.
  // In a two-threaded Python runner, using two locks to protect the batch pulling
  // from the input and the batch queue separately can not ensure batches in the
  // queue has the same order as they are pulled in from the input. Because there is
  // a race when the reader and the writer append batches to the queue.
  // One possible case is:
  //   1) the writer thread gets a batch A, but next it pauses.
  //   2) then the reader thread gets the next Batch B, and appends it to the queue.
  //   3) the writer thread restores and appends batch A to the queue.
  // Therefore, batch A and B have the reversed order in the queue now, leading to data
  // corruption when doing the combination.
  private class BatchQueueImpl extends BatchQueue with AutoCloseable {
    private val queue = mutable.Queue[SpillableColumnarBatch]()

    /** Add a batch to the queue, the input batch will be taken over, do not use it anymore */
    private[python] def add(batch: ColumnarBatch): Unit = {
      val cb = converter.map { convert =>
        withResource(batch)(convert)
      }.getOrElse(batch)
      queue.enqueue(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    }

    /** Return and remove the first batch in the cache. Caller should close it */
    override def remove(): SpillableColumnarBatch = producer.synchronized {
      if (queue.isEmpty) {
        null
      } else {
        queue.dequeue()
      }
    }

    /** Get the number of rows in the next batch, without actually getting the batch. */
    override def peekBatchNumRows(): Int = producer.synchronized {
      // Try to pull in the next batch for peek
      if (queue.isEmpty) {
        val cb = produce()
        if (cb != null) {
          add(cb)
        }
      }

      if (queue.nonEmpty) {
        queue.head.numRows()
      } else {
        0 // Should not go here but just in case.
      }
    }

    override def close(): Unit = producer.synchronized {
      while (queue.nonEmpty) {
        queue.dequeue().close()
      }
    }
  }
}

/**
 * A physical plan that evaluates a [[GpuPythonUDF]]. The transformation of the data to arrow
 * happens on the GPU (practically a noop), But execution of the UDFs are on the CPU.
 */
case class GpuArrowEvalPythonExec(
    udfs: Seq[GpuPythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int) extends ShimUnaryExecNode with GpuPythonExecBase {

  // We split the input batch up into small pieces when sending to python for compatibility reasons
  override def coalesceAfter: Boolean = true

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  private def collectFunctions(
      udf: GpuPythonUDF): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
    udf.children match {
      case Seq(u: GpuPythonUDF) =>
        val ((chained, _), children) = collectFunctions(u)
        ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assertInTests(children.forall(_.find(_.isInstanceOf[GpuPythonUDF]).isEmpty))
        ((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id), udf.children)
    }
  }

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtilsShim.getPythonRunnerConfMap(conf)

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val (numInputRows, numInputBatches, numOutputRows, numOutputBatches) = commonGpuMetrics()

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)

    // cache in a local to avoid serializing the plan
    val inputSchema = child.output.toStructType
    // Build the Python output schema from UDF expressions instead of the 'resultAttrs', because
    // the 'resultAttrs' is NOT always equal to the Python output schema. For example,
    // On Databricks when projecting only one column from a Python UDF output where containing
    // multiple result columns, there will be only one attribute in the 'resultAttrs' for the
    // projecting output, but the output schema for this Python UDF contains multiple columns.
    val pythonOutputSchema = DataTypeUtilsShim.fromAttributes(udfs.map(_.resultAttribute))

    val childOutput = child.output
    val targetBatchSize = batchSize
    val runnerConf = pythonRunnerConf
    val timeZone = sessionLocalTimeZone
    val localMetrics = allMetrics

    val inputRDD = child.executeColumnar()
    inputRDD.mapPartitions { iter =>
      val context = TaskContext.get()
      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // Not sure why we are doing this in every task.  It is not going to change, but it might
      // just be less that we have to ship.
      val udfArgs = PythonArgumentUtils.flatten(inputs)
      val pythonInputSchema = StructType(
        udfArgs.flattenedTypes.zipWithIndex.map { case (dt, i) => StructField(s"_$i", dt)
      }.toArray)

      val boundReferences = GpuBindReferences.bindReferences(udfArgs.flattenedArgs,
        childOutput, localMetrics)
      val batchProducer = new BatchProducer(
        new RebatchingRoundoffIterator(iter, inputSchema, targetBatchSize, numInputRows,
          numInputBatches))
      val pyInputIterator = batchProducer.asIterator.map { batch =>
        GpuProjectExec.projectAndCloseWithRetrySingleBatch(
          SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_BATCHING_PRIORITY),
          boundReferences)
      }

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }

      if (pyInputIterator.hasNext) {
        val pyRunner = new GpuArrowPythonRunner(
          pyFuncs,
          evalType,
          udfArgs.argOffsets,
          pythonInputSchema,
          timeZone,
          runnerConf,
          targetBatchSize,
          pythonOutputSchema,
          udfArgs.argNames)

        val outputIterator = pyRunner.compute(pyInputIterator, context.partitionId(), context)
        new CombiningIterator(batchProducer.getBatchQueue, outputIterator, pyRunner, numOutputRows,
          numOutputBatches)
      } else {
        // Empty partition, return it directly
        iter
      }

    } // End of mapPartitions
  }
}
