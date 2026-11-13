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

package com.nvidia.spark.rapids;

import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;

import ai.rapids.cudf.*;
import com.nvidia.spark.rapids.CoalesceSizeGoal;
import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.jni.RowConversion;
import com.nvidia.spark.rapids.shims.CudfUnsafeRow;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * This class converts InternalRow instances to ColumnarBatches on the GPU through the magic of
 * code generation. This just provides most of the framework a concrete implementation will
 * be generated based off of the schema.
 * The InternalRow instances are first converted to UnsafeRow, cheaply if the instance is already
 * UnsafeRow, and then the UnsafeRow data is collected into a ColumnarBatch.
 */
public abstract class InternalRowToColumnarBatchIterator implements Iterator<ColumnarBatch> {
  protected final Iterator<InternalRow> input;
  protected UnsafeRow pending = null;
  protected int numRowsEstimate = 1;
  protected final int sizePerRowEstimate;
  protected final DType[] rapidsTypes;
  protected final DataType[] outputTypes;
  protected final GpuMetric streamTime;
  protected final GpuMetric opTime;
  protected final GpuMetric numInputRows;
  protected final GpuMetric numOutputRows;
  protected final GpuMetric numOutputBatches;
  // When a retry splits a batch, the remaining pieces of its host-to-GPU transfer and the
  // remaining conversions of the current piece wait here, one output batch per next().
  private Iterator<SpillableColumnarBatch> pendingRows = null;
  private Iterator<ColumnarBatch> pendingBatches = null;
  private static final DataType[] ROWS_TYPES =
      new DataType[] {ArrayType.apply(DataTypes.ByteType, false)};

  protected InternalRowToColumnarBatchIterator(
      Iterator<InternalRow> input,
      Attribute[] schema,
      CoalesceSizeGoal goal,
      GpuMetric streamTime,
      GpuMetric opTime,
      GpuMetric numInputRows,
      GpuMetric numOutputRows,
      GpuMetric numOutputBatches) {
    // The row buffers have a fixed size and do not grow, so a batch that must hold a whole
    // partition would have to reserve for the largest one up front.
    if (goal instanceof RequireSingleBatchLike) {
      throw new IllegalArgumentException(
          "The generated row to columnar iterator cannot require a single batch");
    }
    this.input = input;
    sizePerRowEstimate = CudfUnsafeRow.getRowSizeEstimate(schema);
    numRowsEstimate = calcNumRowsEstimate(goal.targetSizeBytes());
    rapidsTypes = new DType[schema.length];
    outputTypes = new DataType[schema.length];

    for (int i = 0; i < schema.length; i++) {
      rapidsTypes[i] = GpuColumnVector.getNonNestedRapidsType(schema[i].dataType());
      outputTypes[i] = schema[i].dataType();
    }
    this.streamTime = streamTime;
    this.opTime = opTime;
    this.numInputRows = numInputRows;
    this.numOutputRows = numOutputRows;
    this.numOutputBatches = numOutputBatches;
  }

  private int calcNumRowsEstimate(long targetBytes) {
    // A column of rows has int32 offsets, so it holds at most Integer.MAX_VALUE bytes. Bounding
    // the target first also keeps a larger target from overflowing the int cast.
    long rows = Math.min(targetBytes, Integer.MAX_VALUE) / sizePerRowEstimate;
    return (int) Math.max(1, Math.min(Integer.MAX_VALUE - 1, rows));
  }

  private long calcDataLengthEstimate(int numRows) {
    return ((long) sizePerRowEstimate) * numRows;
  }

  private long calcOffsetLengthEstimate(int numRows) {
    int BYTES_PER_OFFSET = DType.INT32.getSizeInBytes();
    return (long)(numRows + 1) * BYTES_PER_OFFSET;
  }

  @Override
  public boolean hasNext() {
    if (hasPendingOutput()) {
      return true;
    }
    boolean ret = true;
    if (pending == null) {
      long start = System.nanoTime();
      ret = input.hasNext();
      long ct = System.nanoTime() - start;
      streamTime.add(ct);
    }
    return ret;
  }

  private boolean hasPendingOutput() {
    return (pendingBatches != null && pendingBatches.hasNext()) ||
        (pendingRows != null && pendingRows.hasNext());
  }

  @Override
  public ColumnarBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    if (!hasPendingOutput()) {
      collectRows();
    }
    // Grab the semaphore because we are about to put data onto the GPU.
    GpuSemaphore$.MODULE$.acquireIfNecessary(TaskContext.get());
    try (NvtxRange ignored = NvtxWithMetrics.apply("RowToColumnar: build", NvtxColor.GREEN,
        Option.apply(opTime))) {
      while (pendingBatches == null || !pendingBatches.hasNext()) {
        pendingBatches = convertFromRowsWithRetry(pendingRows.next());
      }
      ColumnarBatch ret = pendingBatches.next();
      if (numOutputRows != null) {
        numOutputRows.add(ret.numRows());
      }
      if (numOutputBatches != null) {
        numOutputBatches.add(1);
      }
      return ret;
    }
  }

  /**
   * Collect the next rows into host memory and set up their transfer to the GPU, which
   * next() runs one piece at a time.
   */
  private void collectRows() {
    long collectStart = System.nanoTime();
    AutoCloseableTargetSize numRowsWrapper =
        new AutoCloseableTargetSize(numRowsEstimate, 1);
    Tuple2<SpillableHostBuffer, AutoCloseableTargetSize> sBufAndNumRows;

    // The row formatted data is stored as a column of lists of bytes.  The current java CUDF APIs
    // don't do a great job from a performance standpoint with building this type of data structure
    // and we want this to be as efficient as possible so we are going to allocate two host memory
    // buffers.  One will be for the byte data and the second will be for the offsets. We will then
    // write the data directly into those buffers using code generation in a child of this class.
    // that implements fillBatch.
    sBufAndNumRows =
        // Starting with initial num rows estimate, this retry block will recalculate the buffer
        // sizes from the rows estimate, which is split in half if we get a split and retry oom,
        // until we succeed or hit the min of 1 row.  It allocates one spillable host buffer for
        // both data and offsets.
        RmmRapidsRetryIterator.withRetry(numRowsWrapper,
            RmmRapidsRetryIterator.splitTargetSizeInHalfCpu(), (numRows) -> {
          return allocBuffer(numRows);
        }).next();
    // Update our estimate for number of rows with the final size used to allocate the buffers.
    numRowsEstimate = (int) sBufAndNumRows._2.targetSize();
    long dataLength = calcDataLengthEstimate(numRowsEstimate);
    long offsetLength = calcOffsetLengthEstimate(numRowsEstimate);
    int used[];
    try (SpillableHostBuffer spillableBuffer = sBufAndNumRows._1;
    ) {
      HostMemoryBuffer[] hBufs =
          getHostBuffersWithRetry(spillableBuffer, dataLength, offsetLength);
      try(HostMemoryBuffer dataBuffer = hBufs[0];
          HostMemoryBuffer offsetsBuffer = hBufs[1];
      ) {
        used = fillBatch(dataBuffer, offsetsBuffer, dataLength, numRowsEstimate);
        int dataOffset = used[0];
        int currentRow = used[1];
        // We don't want to loop forever trying to copy nothing
        assert (currentRow > 0);
        if (numInputRows != null) {
          numInputRows.add(currentRow);
        }
        // Now that we have filled the buffers with the data, we need to turn them into a
        // HostColumnVector and copy them to the device so the GPU can turn it into a Table.
        // To do this we first need to make a HostColumnCoreVector for the data, and then
        // put that into a HostColumnVector as its child.  This the basics of building up
        // a column of lists of bytes in CUDF but it is typically hidden behind the higer level
        // APIs.
        dataBuffer.incRefCount();
        offsetsBuffer.incRefCount();
        // Only hostColumn closes dataCv: closing a HostColumnVectorCore frees its data at once,
        // and pieces that are not on the GPU yet still read it after this method returns.
        HostColumnVectorCore dataCv = new HostColumnVectorCore(DType.INT8, dataOffset,
            Optional.of(0L), dataBuffer, null, null, new ArrayList<>());
        HostColumnVector hostColumn = null;
        try {
          hostColumn = new HostColumnVector(DType.LIST, currentRow, Optional.of(0L), null, null,
              offsetsBuffer, Collections.singletonList(dataCv));
        } finally {
          if (hostColumn == null) {
            dataCv.close();
            offsetsBuffer.close();
          }
        }
        try (HostColumnVector rowsColumn = hostColumn) {
          long ct = System.nanoTime() - collectStart;
          streamTime.add(ct);

          // Each piece goes to the GPU as a spillable batch, so the conversion can roll back
          // and retry.
          pendingRows = RmmRapidsRetryIterator.withRetry(
              HostColumnarBatchWithRowRange.apply(new HostColumnVector[] {rowsColumn},
                  currentRow, ROWS_TYPES),
              HostColumnarBatchWithRowRange::splitInHalf,
              (piece) -> SpillableColumnarBatch.apply(piece.copyToGpu(),
                  SpillPriorities.ACTIVE_ON_DECK_PRIORITY()));
        }
      }
    }
  }

  /**
   * Convert one piece of rows on the GPU. A split request splits the rows in half by rows.
   */
  private Iterator<ColumnarBatch> convertFromRowsWithRetry(SpillableColumnarBatch rows) {
    return RmmRapidsRetryIterator.withRetry(rows,
        RmmRapidsRetryIterator.splitSpillableInHalfByRows(),
        (attempt) -> {
          try (ColumnarBatch cb = attempt.getColumnarBatch();
               Table tab = convertFromRowsUnderRetry(cb)) {
            return GpuColumnVector.from(tab, outputTypes);
          }
        });
  }

  private HostMemoryBuffer[] getHostBuffersWithRetry(
      SpillableHostBuffer spillableBuffer, long dataLength, long offsetLength) {
    return RmmRapidsRetryIterator.withRetryNoSplit( () -> {
      // One SpillableHostBuffer is used for both data and offsets.  Slice it into the
      // two separate HostMemoryBuffers.
      try (HostMemoryBuffer dataOffsetBuffer = spillableBuffer.getHostBuffer();
           HostMemoryBuffer dataBuffer = dataOffsetBuffer.slice(0, dataLength);
           HostMemoryBuffer offsetsBuffer = dataOffsetBuffer.slice(dataLength, offsetLength);
      ) {
        // Increment these to keep them.
        dataBuffer.incRefCount();
        offsetsBuffer.incRefCount();
        return new HostMemoryBuffer[] { dataBuffer, offsetsBuffer };
      }
    });
  }

  private Tuple2<SpillableHostBuffer, AutoCloseableTargetSize>
  allocBuffer(AutoCloseableTargetSize numRowsWrapper) {
    long dataBytes = calcDataLengthEstimate((int) numRowsWrapper.targetSize());
    long offsetBytes = calcOffsetLengthEstimate((int) numRowsWrapper.targetSize());
    HostMemoryBuffer hBuf = null;
    try {
      hBuf = HostAlloc$.MODULE$.alloc((dataBytes + offsetBytes),true);
      SpillableHostBuffer sBuf = SpillableHostBuffer$.MODULE$.apply(hBuf, hBuf.getLength(),
          SpillPriorities$.MODULE$.ACTIVE_ON_DECK_PRIORITY());
      hBuf = null;  // taken over by spillable host buffer
      return Tuple2.apply(sBuf, numRowsWrapper);
    } finally {
      if (hBuf != null) {
        hBuf.close();
      }
    }
  }

  /**
   * This is exposed so we can verify it is being called N times for OOM retry tests.
   */
  protected Table convertFromRowsUnderRetry(ColumnarBatch cb) {
    ColumnVector devColumn = GpuColumnVector.extractBases(cb)[0];
    return rapidsTypes.length < 100 ?
        // The fixed-width optimized cudf kernel only supports up to 1.5 KB per row which means
        // at most 184 double/long values. We are branching over the size of the output to
        // know which kernel to call. If rapidsTypes.length < 100 we call the fixed-width
        // optimized version, otherwise the generic one
        RowConversion.convertFromRowsFixedWidthOptimized(devColumn, rapidsTypes) :
        RowConversion.convertFromRows(devColumn, rapidsTypes);
  }

  /**
   * Fill a batch with data.  This is the abstraction point because it is faster to have a single
   * virtual function call per batch instead of one per row.
   * @param dataBuffer the data buffer to populate
   * @param offsetsBuffer the offsets buffer to populate
   * @param dataLength the data length corresponding to the current rows estimate.
   * @param numRows the number of rows we can fill
   * @return an array of ints where the first index is the amount of data in bytes copied into
   * the data buffer and the second index is the number of rows copied into the buffers.
   */
  public abstract int[] fillBatch(HostMemoryBuffer dataBuffer, HostMemoryBuffer offsetsBuffer,
      long dataLength, int numRows);
}
