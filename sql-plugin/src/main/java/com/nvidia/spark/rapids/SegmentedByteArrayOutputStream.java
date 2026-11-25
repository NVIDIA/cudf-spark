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
package com.nvidia.spark.rapids;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * An in-memory output stream that never copies previously written bytes when it grows.
 * Segments are allocated lazily and written out in order without flattening them into one array.
 * Like ByteArrayOutputStream, close is a no-op: closing a codec must leave its output readable.
 * Publication trims a partially filled last segment so every retained array is fully used.
 * The producer owns the stream until publication; the consumer then owns it exclusively.
 * The transferred owner releases segment references on close; there is no retained buffer pool.
 */
public final class SegmentedByteArrayOutputStream extends OutputStream {
  private final int segmentSize;
  private List<byte[]> segments = new ArrayList<>();
  private long count;
  private int position;

  public SegmentedByteArrayOutputStream(int segmentSize) {
    if (segmentSize <= 0 || segmentSize > Integer.MAX_VALUE - 8) {
      throw new IllegalArgumentException("segmentSize must be between 1 and Integer.MAX_VALUE - 8");
    }
    this.segmentSize = segmentSize;
  }

  private void requireOwned() {
    if (segments == null) {
      throw new IllegalStateException("Record buffer ownership has been transferred");
    }
  }

  private byte[] writableSegment() {
    requireOwned();
    if (segments.isEmpty() || position == segmentSize) {
      segments.add(new byte[segmentSize]);
      position = 0;
    }
    return segments.get(segments.size() - 1);
  }

  @Override
  public void write(int value) {
    writableSegment()[position++] = (byte) value;
    count++;
  }

  @Override
  public void write(byte[] bytes, int offset, int length) {
    requireOwned();
    Objects.requireNonNull(bytes, "bytes");
    if (offset < 0 || length < 0 || offset > bytes.length - length) {
      throw new IndexOutOfBoundsException();
    }
    while (length > 0) {
      byte[] segment = writableSegment();
      int amount = Math.min(length, segmentSize - position);
      System.arraycopy(bytes, offset, segment, position, amount);
      position += amount;
      count += amount;
      offset += amount;
      length -= amount;
    }
  }

  public long getCount() {
    return count;
  }

  /** Actual backing-array capacity, including unused space in the last segment. */
  public long getCapacity() {
    requireOwned();
    return (long) segments.size() * segmentSize;
  }

  /** Write only valid bytes. Does not flush or close the destination or consume this stream. */
  public void writeTo(OutputStream out) throws IOException {
    requireOwned();
    Objects.requireNonNull(out, "out");
    if (out == this) {
      throw new IllegalArgumentException("Cannot write a stream to itself");
    }
    long remaining = count;
    for (byte[] segment : segments) {
      int length = (int) Math.min(remaining, segmentSize);
      out.write(segment, 0, length);
      remaining -= length;
    }
  }

  /** A destination that can consume a record without copying its backing arrays. */
  public interface Sink {
    void writeRecord(OwnedBuffer record) throws IOException;
  }

  /**
   * Trim only the partial last segment, then transfer the arrays exactly once.
   * Full segments keep their original arrays. The producer can no longer write or read them.
   */
  public OwnedBuffer takeBuffer() {
    requireOwned();
    OwnedBuffer result = new OwnedBuffer(segments, segmentSize, count);
    if (position > 0 && position < segmentSize) {
      int last = segments.size() - 1;
      segments.set(last, Arrays.copyOf(segments.get(last), position));
    }
    segments = null;
    return result;
  }

  /** Immutable payload with exclusive ownership; close promptly drops all array references. */
  public static final class OwnedBuffer implements AutoCloseable {
    private List<byte[]> segments;
    private final int segmentSize;
    private final long count;

    private OwnedBuffer(List<byte[]> segments, int segmentSize, long count) {
      this.segments = segments;
      this.segmentSize = segmentSize;
      this.count = count;
    }

    /** Move the payload to a new owner; closing this former owner cannot release its arrays. */
    public synchronized OwnedBuffer takeBuffer() {
      requireOpen();
      OwnedBuffer result = new OwnedBuffer(segments, segmentSize, count);
      segments = null;
      return result;
    }

    public long getCount() { return count; }

    private void requireOpen() {
      if (segments == null) {
        throw new IllegalStateException("Record buffer is closed");
      }
    }

    public void read(long offset, byte[] target, int start, int length) {
      requireOpen();
      Objects.requireNonNull(target, "target");
      if (start < 0 || length < 0 || start > target.length - length ||
          offset < 0 || offset > count - length) {
        throw new IndexOutOfBoundsException();
      }
      while (length > 0) {
        int inSegment = (int) (offset % segmentSize);
        int size = Math.min(length, segmentSize - inSegment);
        System.arraycopy(segments.get((int) (offset / segmentSize)), inSegment,
            target, start, size);
        offset += size;
        start += size;
        length -= size;
      }
    }

    public void writeTo(OutputStream out) throws IOException {
      requireOpen();
      Objects.requireNonNull(out, "out");
      for (byte[] segment : segments) {
        out.write(segment, 0, segment.length);
      }
    }

    @Override
    public synchronized void close() {
      segments = null;
    }
  }

}
