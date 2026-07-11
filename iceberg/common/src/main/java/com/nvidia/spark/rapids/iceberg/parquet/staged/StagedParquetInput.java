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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import ai.rapids.cudf.HostMemoryBuffer;

/**
 * A zero-copy logical Parquet file presented to cuDF as an ordered set of host buffers.
 *
 * <p>The first and last segments are the small synthetic header and relocated footer. Every
 * segment between them is an owning slice of a downloaded file fragment. Concatenating the
 * segments produces exactly the byte layout described by {@link ReadSubtask}, without copying
 * the encoded column chunks into a second task-sized allocation.</p>
 *
 * <p>This object borrows its fragment outputs from the partition reader and is valid only while
 * {@link StagedScanAdapter#decodeAndPostProcess(ReadSubtask, StagedParquetInput)} is executing.
 * {@link #materialize()} returns a fresh owning buffer array on every call. That lets the RMM
 * retry loop restore spilled fragments and build a new set of input references for each decode
 * attempt, exactly as the base multithreaded Parquet reader does.</p>
 */
public final class StagedParquetInput {
  private final byte[] headerBytes;
  private final byte[] footerBytes;
  private final List<FragmentSlice> dataSlices;

  StagedParquetInput(
      ReadSubtask subtask,
      List<FileFragment> fragments) {
    Objects.requireNonNull(subtask, "subtask");
    Objects.requireNonNull(fragments, "fragments");
    List<ReadSubtask.FileSlice> fileSlices = subtask.getFileSlices();
    if (fileSlices.size() != fragments.size()) {
      throw new IllegalArgumentException(
          "one downloaded fragment is required for every planned file slice");
    }

    this.headerBytes = subtask.getHeaderBytes();
    this.footerBytes = subtask.getFooterAndTrailerBytes();
    ArrayList<FragmentSlice> slices = new ArrayList<>(fileSlices.size());
    long dataBytes = 0L;
    for (int index = 0; index < fileSlices.size(); index++) {
      ReadSubtask.FileSlice planned = fileSlices.get(index);
      FileFragment fragment = Objects.requireNonNull(fragments.get(index), "fragment");
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
    this.dataSlices = slices;
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
      throw new IOException("failed to materialize staged Parquet input", error);
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
    private final StagedParquetOutput output;
    private final long offset;
    private final long length;

    private FragmentSlice(StagedParquetOutput output, long offset, long length) {
      this.output = Objects.requireNonNull(output, "output");
      this.offset = offset;
      this.length = length;
    }
  }
}
