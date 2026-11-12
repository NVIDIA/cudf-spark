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

package org.apache.iceberg.aws.s3;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile.CopyRange;
import com.nvidia.spark.rapids.jni.fileio.SeekableInputStream;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

/**
 * Buffered metadata reads using the same PerfIO client as bulk data reads. Each refill is a
 * bounded range that is fully consumed before returning, so seeking or closing never aborts
 * an HTTP response. Buffering also avoids a request per byte when decoding a page header.
 */
final class IcebergS3InputStream extends SeekableInputStream {
  private static final int BUFFER_SIZE = 64 * 1024;

  private final IcebergS3InputFile inputFile;
  private final long fileLength;
  private HostMemoryBuffer buffer;
  private long bufferStart;
  private int bufferLength;
  private long position;
  private boolean closed;

  IcebergS3InputStream(IcebergS3InputFile inputFile, long fileLength) {
    this.inputFile = inputFile;
    this.fileLength = fileLength;
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
  }

  private boolean ensureBuffered() throws IOException {
    ensureOpen();
    if (position >= fileLength) {
      return false;
    }
    if (bufferLength == 0 || position < bufferStart || position >= bufferStart + bufferLength) {
      if (buffer == null) {
        buffer = HostMemoryBuffer.allocate(BUFFER_SIZE, false);
      }
      // A failed refill must not expose bytes left over from the previous range.
      bufferLength = 0;
      // Align the range so nearby backward seeks can reuse the same buffer too.
      long start = position - position % BUFFER_SIZE;
      int length = (int) Math.min(BUFFER_SIZE, fileLength - start);
      inputFile.readVectored(buffer,
          Collections.singletonList(new CopyRange(start, length, 0)));
      bufferStart = start;
      bufferLength = length;
    }
    return true;
  }

  @Override
  public long getPos() throws IOException {
    ensureOpen();
    return position;
  }

  @Override
  public void seek(long newPos) throws IOException {
    ensureOpen();
    if (newPos < 0) {
      throw new IOException("Cannot seek to a negative position: " + newPos);
    }
    position = newPos;
  }

  @Override
  public int read() throws IOException {
    if (!ensureBuffered()) {
      return -1;
    }
    return buffer.getByte(position++ - bufferStart) & 0xff;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    Objects.requireNonNull(bytes, "bytes");
    if (offset < 0 || length < 0 || offset > bytes.length - length) {
      throw new IndexOutOfBoundsException();
    }
    ensureOpen();
    if (length == 0) {
      return 0;
    }
    if (!ensureBuffered()) {
      return -1;
    }
    int count = (int) Math.min(length, bufferStart + bufferLength - position);
    buffer.getBytes(bytes, offset, position - bufferStart, count);
    position += count;
    return count;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      if (buffer != null) {
        buffer.close();
        buffer = null;
      }
    }
  }
}
