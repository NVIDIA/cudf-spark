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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.utils.ResourceUtil;

/** Writes a shuffle record into fixed-size host buffers owned by the producer until handoff. */
public final class SegmentedHostOutputStream extends OutputStream {
  private final int segmentSize;
  private List<HostMemoryBuffer> segments = new ArrayList<>();
  private long count;
  private int position;

  public SegmentedHostOutputStream(int segmentSize) {
    if (segmentSize <= 0 || segmentSize > Integer.MAX_VALUE - 8) {
      throw new IllegalArgumentException("segmentSize must be between 1 and Integer.MAX_VALUE - 8");
    }
    this.segmentSize = segmentSize;
  }

  private void requireOpen() {
    if (segments == null) {
      throw new IllegalStateException("Record stream is closed or ownership was transferred");
    }
  }

  private HostMemoryBuffer writableSegment() {
    if (segments.isEmpty() || position == segmentSize) {
      // Executors install HostAlloc as cuDF's host allocator. CPU writes prefer pageable memory.
      HostMemoryBuffer segment = HostMemoryBuffer.allocate(segmentSize, false);
      try {
        segments.add(segment);
      } catch (Throwable t) {
        segment.close();
        throw t;
      }
      position = 0;
    }
    return segments.get(segments.size() - 1);
  }

  @Override
  public void write(int value) {
    requireOpen();
    writableSegment().setByte(position, (byte) value);
    position++;
    count++;
  }

  @Override
  public void write(byte[] bytes, int offset, int length) {
    requireOpen();
    Objects.requireNonNull(bytes, "bytes");
    if (offset < 0 || length < 0 || offset > bytes.length - length) {
      throw new IndexOutOfBoundsException();
    }
    while (length > 0) {
      HostMemoryBuffer segment = writableSegment();
      int amount = Math.min(length, segmentSize - position);
      segment.setBytes(position, bytes, offset, amount);
      position += amount;
      count += amount;
      offset += amount;
      length -= amount;
    }
  }

  public interface Sink {
    void writeRecord(OwnedBuffer record) throws IOException;
  }

  /** Move all segments, including the original partially filled tail. */
  public OwnedBuffer takeBuffer() {
    requireOpen();
    OwnedBuffer result = new OwnedBuffer(segments, segmentSize, count);
    segments = null;
    return result;
  }

  private static void closeSegments(List<HostMemoryBuffer> buffers) {
    Throwable failure = null;
    for (HostMemoryBuffer buffer : buffers) {
      try {
        buffer.close();
      } catch (RuntimeException | Error e) {
        if (failure == null) {
          failure = e;
        } else if (failure != e) {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw ResourceUtil.propagate(failure);
    }
  }

  @Override
  public void close() {
    if (segments != null) {
      List<HostMemoryBuffer> toClose = segments;
      segments = null;
      closeSegments(toClose);
    }
  }

  /** Immutable record payload. Moving or closing one owner invalidates only that owner. */
  public static final class OwnedBuffer implements AutoCloseable {
    private List<HostMemoryBuffer> segments;
    private final int segmentSize;
    private final long count;

    private OwnedBuffer(List<HostMemoryBuffer> segments, int segmentSize, long count) {
      this.segments = segments;
      this.segmentSize = segmentSize;
      this.count = count;
    }

    private void requireOpen() {
      if (segments == null) {
        throw new IllegalStateException("Record buffer is closed or ownership was transferred");
      }
    }

    public synchronized OwnedBuffer takeBuffer() {
      requireOpen();
      OwnedBuffer result = new OwnedBuffer(segments, segmentSize, count);
      segments = null;
      return result;
    }

    public long getCount() {
      return count;
    }

    public synchronized long getCapacity() {
      requireOpen();
      return (long) segments.size() * segmentSize;
    }

    public synchronized void read(long offset, byte[] target, int start, int length) {
      requireOpen();
      Objects.requireNonNull(target, "target");
      if (start < 0 || length < 0 || start > target.length - length ||
          offset < 0 || offset > count - length) {
        throw new IndexOutOfBoundsException();
      }
      while (length > 0) {
        int inSegment = (int) (offset % segmentSize);
        int size = Math.min(length, segmentSize - inSegment);
        segments.get((int) (offset / segmentSize)).getBytes(target, start, inSegment, size);
        offset += size;
        start += size;
        length -= size;
      }
    }

    public synchronized void writeTo(OutputStream out) throws IOException {
      requireOpen();
      WritableByteChannel channel = out instanceof FileOutputStream
          ? ((FileOutputStream) out).getChannel() : Channels.newChannel(out);
      long remaining = count;
      for (HostMemoryBuffer segment : segments) {
        int length = (int) Math.min(remaining, segmentSize);
        ByteBuffer data = segment.asByteBuffer(0, length);
        while (data.hasRemaining()) {
          channel.write(data);
        }
        remaining -= length;
      }
    }

    @Override
    public synchronized void close() {
      if (segments != null) {
        List<HostMemoryBuffer> toClose = segments;
        segments = null;
        closeSegments(toClose);
      }
    }
  }
}
