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

package com.nvidia.spark.rapids.iceberg.parquet.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.reader.DecodeInput;

/**
 * A zero-copy logical Parquet file presented to cuDF as an ordered set of host buffers.
 *
 * <p>The first and last segments are the small synthetic header and relocated footer. Every
 * segment between them is an owning slice of a downloaded file fragment. Concatenating the
 * segments produces exactly the byte layout described by {@link ReadSubtask}, without copying
 * the encoded column chunks into a second task-sized allocation.</p>
 *
 * <p>The async combiner gives this object retained references to its fragment outputs. It owns
 * those references until {@link #close()}, independently of the planner's base ownership.
 * {@link #materialize()} returns a fresh owning buffer array on every call. That lets the RMM
 * retry loop restore spilled fragments and build a new set of input references for each decode
 * attempt, exactly as the base multithreaded Parquet reader does.</p>
 */
public final class ParquetDecodeInput implements DecodeInput {
  private static final SubtaskStats EMPTY_STATS = new SubtaskStats(
      0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, false,
      0L, 0L, 0L, 0L, 0L);
  private final ReadSubtask plan;
  private final SubtaskStats stats;
  private final byte[] headerBytes;
  private final byte[] footerBytes;
  private final List<FragmentSlice> dataSlices;
  private final List<FileFragment> retainedFragments;
  private boolean closed;

  ParquetDecodeInput(
      ReadSubtask subtask,
      List<FileFragment> fragments) {
    this(subtask, fragments, EMPTY_STATS, false);
  }

  ParquetDecodeInput(
      ReadSubtask subtask,
      List<FileFragment> fragments,
      SubtaskStats stats) {
    this(subtask, fragments, stats, true);
  }

  private ParquetDecodeInput(
      ReadSubtask subtask,
      List<FileFragment> fragments,
      SubtaskStats stats,
      boolean retainFragments) {
    Objects.requireNonNull(subtask, "subtask");
    Objects.requireNonNull(fragments, "fragments");
    List<ReadSubtask.FileSlice> fileSlices = subtask.getFileSlices();
    if (fileSlices.size() != fragments.size()) {
      throw new IllegalArgumentException(
          "one downloaded fragment is required for every planned file slice");
    }

    this.plan = subtask;
    this.stats = Objects.requireNonNull(stats, "stats");
    this.headerBytes = subtask.getHeaderBytes();
    this.footerBytes = subtask.getFooterAndTrailerBytes();
    ArrayList<FragmentSlice> slices = new ArrayList<>(fileSlices.size());
    ArrayList<FileFragment> retained = new ArrayList<>(fragments.size());
    long dataBytes = 0L;
    try {
      for (int index = 0; index < fileSlices.size(); index++) {
        ReadSubtask.FileSlice planned = fileSlices.get(index);
        FileFragment fragment = Objects.requireNonNull(fragments.get(index), "fragment");
        if (retainFragments) {
          fragment.retain();
          retained.add(fragment);
        }
        long length = fragment.sliceBytes(planned.getFirstBlock(), planned.getBlockCount());
        if (length > 0L) {
          slices.add(new FragmentSlice(
              fragment.getData(), fragment.blockStartOffset(planned.getFirstBlock()), length));
          dataBytes = Math.addExact(dataBytes, length);
        }
      }
      if (dataBytes != subtask.getDataSizeBytes()) {
        throw new IllegalArgumentException(
            "fragment slices do not match the planned synthetic data size");
      }
    } catch (Throwable error) {
      retained.forEach(FileFragment::close);
      throw error;
    }
    this.retainedFragments = retained;
    this.dataSlices = slices;
  }

  public ReadSubtask getPlan() {
    return plan;
  }

  public SubtaskStats getStats() {
    return stats;
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      for (FileFragment fragment : retainedFragments) {
        fragment.close();
      }
    }
  }

  /**
   * Materialize one owning buffer per logical segment for a single decode attempt.
   *
   * <p>Only the tiny header and footer are copied. Data buffers are reference-counted slices of
   * the fragment allocations. The caller owns every returned buffer and must close them or pass
   * their ownership to the cuDF Parquet reader. If materialization fails, this method closes all
   * buffers it already created.</p>
   */
  public HostMemoryBuffer[] materialize() throws IOException {
    ArrayList<HostMemoryBuffer> buffers = new ArrayList<>(dataSlices.size() + 2);
    try {
      buffers.add(bufferFromBytes(headerBytes));
      for (FragmentSlice slice : dataSlices) {
        HostMemoryBuffer fragment = slice.output.materialize();
        try {
          buffers.add(fragment.slice(slice.offset, slice.length));
        } finally {
          fragment.close();
        }
      }
      buffers.add(bufferFromBytes(footerBytes));
      return buffers.toArray(new HostMemoryBuffer[0]);
    } catch (Throwable error) {
      for (HostMemoryBuffer buffer : buffers) {
        try {
          buffer.close();
        } catch (Throwable closeError) {
          if (closeError != error) {
            error.addSuppressed(closeError);
          }
        }
      }
      if (error instanceof IOException) {
        throw (IOException) error;
      }
      if (error instanceof RuntimeException) {
        throw (RuntimeException) error;
      }
      if (error instanceof Error) {
        throw (Error) error;
      }
      throw new IOException("failed to materialize asynchronous Parquet input", error);
    }
  }

  /** The number of buffers returned by each successful {@link #materialize()} call. */
  public int getSegmentCount() {
    return dataSlices.size() + 2;
  }

  private static HostMemoryBuffer bufferFromBytes(byte[] bytes) {
    HostMemoryBuffer buffer = HostMemoryBuffer.allocate(bytes.length);
    try {
      buffer.setBytes(0L, bytes, 0, bytes.length);
      return buffer;
    } catch (Throwable error) {
      buffer.close();
      throw error;
    }
  }

  /** One immutable range borrowed from a sealed file-fragment output. */
  private static final class FragmentSlice {
    private final ParquetOutput output;
    private final long offset;
    private final long length;

    private FragmentSlice(ParquetOutput output, long offset, long length) {
      this.output = Objects.requireNonNull(output, "output");
      this.offset = offset;
      this.length = length;
    }
  }
}
