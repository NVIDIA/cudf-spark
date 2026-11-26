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
package com.nvidia.spark.rapids.spill

import java.io.FileOutputStream
import java.util.ArrayList

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.SegmentedHostOutputStream.OwnedBuffer

/** Storage owned by one partial-file handle. Reads/spills/close use the handle's locks. */
private[spill] sealed trait PartialFileBuffer extends AutoCloseable {
  def capacity: Long
  def setByte(offset: Long, value: Byte): Unit
  def setBytes(offset: Long, bytes: Array[Byte], start: Int, length: Int): Unit
  def getBytes(bytes: Array[Byte], start: Int, offset: Long, length: Int): Unit
  def writeTo(out: FileOutputStream, length: Long): Unit
}

private[spill] final class ContiguousPartialFileBuffer(val buffer: HostMemoryBuffer)
    extends PartialFileBuffer {
  override def capacity: Long = buffer.getLength
  override def setByte(offset: Long, value: Byte): Unit = buffer.setByte(offset, value)
  override def setBytes(offset: Long, bytes: Array[Byte], start: Int, length: Int): Unit =
    buffer.setBytes(offset, bytes, start, length)
  override def getBytes(bytes: Array[Byte], start: Int, offset: Long, length: Int): Unit =
    buffer.getBytes(bytes, start, offset, length)
  override def writeTo(out: FileOutputStream, length: Long): Unit = {
    val bytes = buffer.asByteBuffer()
    bytes.limit(Math.toIntExact(length))
    while (bytes.hasRemaining) {
      out.getChannel.write(bytes)
    }
  }
  override def close(): Unit = buffer.close()
}

/** Retains immutable serialized records. Append transfers references, never payload bytes. */
private[spill] final class OwnedRecordBuffer extends PartialFileBuffer {
  private case class Record(start: Long, data: OwnedBuffer)
  private val records = new ArrayList[Record]()
  private var size = 0L
  private var allocatedBytes = 0L
  override def capacity: Long = allocatedBytes

  /** Transfer ownership on success. On exception, the caller still owns data. */
  def append(data: OwnedBuffer): Unit = {
    val recordCapacity = data.getCapacity
    records.add(Record(size, data))
    allocatedBytes += recordCapacity
    size += data.getCount
  }

  override def setByte(offset: Long, value: Byte): Unit =
    throw new UnsupportedOperationException("Owned records are immutable")
  override def setBytes(offset: Long, bytes: Array[Byte], start: Int, length: Int): Unit =
    throw new UnsupportedOperationException("Owned records are immutable")

  override def getBytes(bytes: Array[Byte], start: Int, offset: Long, length: Int): Unit = {
    if (start < 0 || length < 0 || start > bytes.length - length ||
        offset < 0 || offset > size - length) {
      throw new IndexOutOfBoundsException()
    }
    if (length == 0) return
    var low = 0
    var high = records.size() - 1
    while (low < high) {
      val mid = low + (high - low + 1) / 2
      if (records.get(mid).start <= offset) low = mid else high = mid - 1
    }
    var index = low
    var position = offset
    var destination = start
    var remaining = length
    while (remaining > 0) {
      val record = records.get(index)
      val inRecord = position - record.start
      val count = math.min(remaining.toLong, record.data.getCount - inRecord).toInt
      record.data.read(inRecord, bytes, destination, count)
      position += count
      destination += count
      remaining -= count
      index += 1
    }
  }

  override def writeTo(out: FileOutputStream, length: Long): Unit = {
    require(length == size)
    records.forEach(_.data.writeTo(out))
  }

  override def close(): Unit = {
    records.forEach(_.data.close())
    records.clear()
    size = 0L
    allocatedBytes = 0L
  }
}
