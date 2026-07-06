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
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

/**
 * Writable storage for one synthetic Parquet file produced by the staged reader.
 *
 * <p>The output has a strict lifecycle: {@code WRITABLE -> SEALED -> CLOSED}. The I/O worker
 * owns a writable output, ownership moves to the combine worker, and a sealed output is finally
 * published to the Spark task thread in a {@link StagedReadResult}. Calls from those stages must
 * not overlap. Implementations still synchronize lifecycle transitions so an asynchronous close
 * cannot race a write without producing a deterministic failure.</p>
 *
 * <p>All sizes and offsets are bytes. Source offsets in {@link PlannedReadRange} address the
 * original Parquet file; output offsets address this synthetic Parquet file. A caller owns the
 * {@link HostMemoryBuffer} reference returned by {@link #materialize()} and must close it.</p>
 */
public interface StagedParquetOutput extends AutoCloseable {
  /**
   * Receives one caller-owned view after a remote column-chunk range has been copied.
   *
   * <p>The callback is used to populate the RAPIDS data-range cache without rereading a staged
   * local file. Implementations invoke it once for every input {@link PlannedReadRange}, even when
   * several ranges were fetched concurrently by one vectored request. Ownership of {@code data}
   * transfers to the callback, which must close it or transfer it again.</p>
   */
  @FunctionalInterface
  interface RangeCopyObserver {
    void rangeCopied(PlannedReadRange range, HostMemoryBuffer data) throws IOException;
  }

  /** The durable medium currently holding the synthetic Parquet bytes. */
  enum BackingStore {
    /** Host memory managed by the RAPIDS spill framework after the output is sealed. */
    HOST_MEMORY,

    /** A task-owned file on an executor-local disk. */
    LOCAL_FILE
  }

  /**
   * Return the number of bytes reserved when the output was created.
   *
   * @return capacity in bytes
   */
  long capacityBytes();

  /**
   * Return the valid byte count after sealing, or the reserved capacity while writable.
   *
   * @return size in bytes
   */
  long sizeBytes();

  /** Return the medium backing this output. */
  BackingStore backingStore();

  /** Return whether the combine worker has successfully sealed this output. */
  boolean isSealed();

  /**
   * Copy ranges from one original input file into their planned output offsets.
   *
   * <p>The orchestrator groups ranges by source file before invoking this method. Every range is
   * one Parquet column chunk and must remain a distinct vectored-I/O request so PerfIO can fetch
   * chunks concurrently. File-backed outputs use a writable local-file mapping so the full source
   * range list can remain one vectored call without an aggregate scratch allocation.</p>
   *
   * @param input original Parquet input file; the output does not take ownership
   * @param ranges planned ranges belonging to {@code input}
   * @param scratchBytes target bytes for bounded file-backed vectored-I/O batches
   * @param observer callback receiving an owning view of every copied range
   * @throws IOException if the input cannot be read or the local output cannot be written
   */
  void copyRanges(
      RapidsInputFile input,
      List<PlannedReadRange> ranges,
      int scratchBytes,
      RangeCopyObserver observer) throws IOException;

  /**
   * Copy one exact cached column-chunk range into its synthetic-file offset.
   *
   * <p>The supplied channel is positioned at the beginning of the cached range and remains owned
   * by the caller. Multiple calls for disjoint output offsets may execute concurrently.</p>
   */
  void copyCachedRange(
      SeekableByteChannel channel,
      long outputOffset,
      long length,
      int scratchBytes) throws IOException;

  /**
   * Write combine-stage bytes, such as a Parquet header, footer, or footer length.
   *
   * @param outputOffset absolute byte offset in the synthetic file
   * @param source source byte array
   * @param sourceOffset first byte to copy from {@code source}
   * @param length number of bytes to copy
   * @throws IOException if the bytes cannot be written
   */
  void writeBytes(
      long outputOffset,
      byte[] source,
      int sourceOffset,
      int length) throws IOException;

  /** Write an entire byte array at an absolute synthetic-file offset. */
  default void writeBytes(long outputOffset, byte[] source) throws IOException {
    writeBytes(outputOffset, source, 0, source.length);
  }

  /**
   * Seal the output and publish its final valid size.
   *
   * <p>After this call succeeds, all writes are rejected. For a memory output this transition
   * also registers the buffer with the RAPIDS spill framework.</p>
   *
   * @param actualSizeBytes final synthetic Parquet file size in bytes
   * @throws IOException if finalization fails
   */
  void seal(long actualSizeBytes) throws IOException;

  /**
   * Materialize the sealed synthetic file in host memory for cuDF decoding.
   *
   * <p>The returned reference is independent of this output's ownership. The caller must close
   * it even if decoding fails. A file-backed output returns an owning slice of its local-file
   * mapping, so the orchestrator should invoke this only once per result.</p>
   *
   * @return caller-owned host-memory buffer containing exactly {@link #sizeBytes()} bytes
   * @throws IOException if a disk-backed result cannot be read
   */
  HostMemoryBuffer materialize() throws IOException;

  /**
   * Release the backing buffer or delete the local file.
   *
   * <p>Closing is idempotent. No other method may be called after close.</p>
   */
  @Override
  void close();
}
