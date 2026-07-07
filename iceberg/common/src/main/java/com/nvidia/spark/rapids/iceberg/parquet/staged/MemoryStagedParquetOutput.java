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

package com.nvidia.spark.rapids.iceberg.parquet.staged;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.SpillPriorities;
import com.nvidia.spark.rapids.SpillableHostBuffer;

import java.io.IOException;
import java.util.Objects;

/**
 * Host-memory implementation of {@link StagedParquetOutput}.
 *
 * <p>The factory transfers ownership of {@code writableBuffer} to this object. The buffer is
 * writable only during I/O and combination. Sealing transfers it to a {@link SpillableHostBuffer}
 * so completed results waiting in the completion queue can participate in normal host spilling.
 * A materialized buffer is a separate, caller-owned reference.</p>
 */
final class MemoryStagedParquetOutput extends StagedParquetOutput {
  /** Owned only before seal; set to null when ownership moves to {@code sealedBuffer}. */
  private HostMemoryBuffer writableBuffer;

  /** Owned only after seal and responsible for spill-framework registration. */
  private SpillableHostBuffer sealedBuffer;

  MemoryStagedParquetOutput(HostMemoryBuffer writableBuffer, long capacityBytes) {
    super(capacityBytes, false);
    this.writableBuffer = Objects.requireNonNull(writableBuffer, "writableBuffer");
    long bufferLength = writableBuffer.getLength();
    if (bufferLength < capacityBytes) {
      writableBuffer.close();
      this.writableBuffer = null;
      throw new IllegalArgumentException(
          "host buffer length " + bufferLength +
              " is less than capacity " + capacityBytes);
    }
  }

  @Override
  HostMemoryBuffer writableBuffer() {
    return writableBuffer;
  }

  @Override
  void sealStorage() throws IOException {
    // SpillableHostBuffer.apply takes ownership even when construction throws. Clear the
    // writable reference before transfer so close() can never release the same buffer twice.
    HostMemoryBuffer toTransfer = writableBuffer;
    writableBuffer = null;
    try {
      sealedBuffer = SpillableHostBuffer.apply(
          toTransfer, exactSizeBytes(), SpillPriorities.ACTIVE_BATCHING_PRIORITY());
    } catch (RuntimeException e) {
      throw new IOException("failed to register staged Parquet buffer for spilling", e);
    }
  }

  @Override
  HostMemoryBuffer materializeStorage() throws IOException {
    try {
      return sealedBuffer.getDataHostBuffer();
    } catch (RuntimeException e) {
      throw new IOException("failed to materialize staged Parquet host buffer", e);
    }
  }

  @Override
  void closeStorage() {
    if (sealedBuffer != null) {
      sealedBuffer.close();
      sealedBuffer = null;
    }
    if (writableBuffer != null) {
      writableBuffer.close();
      writableBuffer = null;
    }
  }
}
