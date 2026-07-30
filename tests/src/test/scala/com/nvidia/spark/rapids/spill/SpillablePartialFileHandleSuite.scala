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

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.util.Arrays

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class SpillablePartialFileHandleSuite
    extends AnyFunSuite with BeforeAndAfterEach with MockitoSugar {

  // Use 1GB max buffer size for tests to avoid memory issues on test machines
  private val testMaxBufferSize = 1L * 1024 * 1024 * 1024

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new RapidsConf(Map(
      "spark.rapids.memory.host.partialFileBufferMaxSize" -> testMaxBufferSize.toString
    ))
    SpillFramework.initialize(conf)
  }

  override def afterEach(): Unit = {
    SpillFramework.shutdown()
    super.afterEach()
  }

  test("FILE_ONLY mode: write and read") {
    val tempFile = File.createTempFile("test-file-only-", ".tmp")

    withResource(SpillablePartialFileHandle.createFileOnly(tempFile)) { handle =>
      // Write some data
      val testData = "Hello, World! This is a test.".getBytes("UTF-8")
      handle.write(testData, 0, testData.length)
      
      assert(!handle.isMemoryBased, "FILE_ONLY should not be memory based")
      assert(!handle.isSpilled, "FILE_ONLY should never report as spilled")
      
      // Finish write phase
      handle.finishWrite()
      
      assert(handle.getTotalBytesWritten == testData.length)
      
      // Read data back
      val readBuffer = new Array[Byte](testData.length)
      val bytesRead = handle.read(readBuffer, 0, testData.length)
      
      assert(bytesRead == testData.length)
      assert(readBuffer.sameElements(testData))
      
      // EOF check
      assert(handle.read(readBuffer, 0, 10) == -1)
    }
  }

  test("MEMORY_WITH_SPILL mode: write and read from memory") {
    val tempFile = File.createTempFile("test-memory-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 1024,
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      assert(handle.isMemoryBased, "Should be memory based")
      assert(!handle.isSpilled, "Should not be spilled initially")
      
      // Write small amount of data (fits in buffer)
      val testData = "Small test data".getBytes("UTF-8")
      handle.write(testData, 0, testData.length)
      
      // Finish write phase
      handle.finishWrite()
      
      assert(handle.getTotalBytesWritten == testData.length)
      assert(!handle.isSpilled, "Should still be in memory")
      
      // Read data back
      val readBuffer = new Array[Byte](testData.length)
      val bytesRead = handle.read(readBuffer, 0, testData.length)
      
      assert(bytesRead == testData.length)
      assert(readBuffer.sameElements(testData))
    }
  }

  test("MEMORY_WITH_SPILL mode: buffer expansion") {
    val tempFile = File.createTempFile("test-expansion-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 64,  // Small initial size to force expansion
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      // Write data larger than initial capacity
      val largeData = new Array[Byte](200)
      (0 until 200).foreach(i => largeData(i) = (i % 256).toByte)
      
      handle.write(largeData, 0, largeData.length)
      
      // Should have expanded the buffer
      handle.finishWrite()
      
      assert(handle.getTotalBytesWritten == largeData.length)
      
      // Read and verify
      val readBuffer = new Array[Byte](largeData.length)
      val bytesRead = handle.read(readBuffer, 0, largeData.length)

      assert(!handle.isSpilled, "Should still be in memory")
      assert(bytesRead == largeData.length)
      assert(readBuffer.sameElements(largeData))
    }
  }

  test("MEMORY_WITH_SPILL mode: buffer expansion then switch to file") {
    val tempFile = File.createTempFile("test-expansion-switch-", ".tmp")

    // Use 600MB initial capacity, so doubling (1.2GB) would exceed 1GB limit
    val initialCapacity = 600L * 1024 * 1024
    
    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = initialCapacity,
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      // Write data to fill initial buffer
      val chunkSize = 1024 * 1024  // 1MB chunks
      val chunk = new Array[Byte](chunkSize)
      (0 until chunkSize).foreach(i => chunk(i) = (i % 256).toByte)
      
      // Fill the buffer completely
      val numChunks = (testMaxBufferSize / chunkSize).toInt
      (0 until numChunks).foreach { _ =>
        handle.write(chunk, 0, chunkSize)
      }
      
      // Write one more byte to trigger expansion, which should fail
      // and switch to file mode due to testMaxBufferSize limit
      handle.write(0xFF)
      
      // Write more data to verify file mode is working
      handle.write(chunk, 0, chunkSize)
      
      handle.finishWrite()
      
      val expectedSize = numChunks.toLong * chunkSize + 1 + chunkSize
      assert(handle.getTotalBytesWritten == expectedSize)
      assert(handle.isMemoryBased, "Should still be memory-based mode")
      assert(handle.isSpilled, "Should have switched to file after expansion")
      
      // Verify we can read all data back correctly
      val readBuffer = new Array[Byte](chunkSize)
      
      // Read first chunks
      (0 until numChunks).foreach { _ =>
        val bytesRead = handle.read(readBuffer, 0, chunkSize)
        assert(bytesRead == chunkSize)
        assert(readBuffer.sameElements(chunk))
      }
      
      // Read the single byte
      val singleByte = new Array[Byte](1)
      assert(handle.read(singleByte, 0, 1) == 1)
      assert(singleByte(0) == 0xFF.toByte)
      
      // Read last chunk
      val lastBytesRead = handle.read(readBuffer, 0, chunkSize)
      assert(lastBytesRead == chunkSize)
      assert(readBuffer.sameElements(chunk))
      
      // EOF check
      assert(handle.read(readBuffer, 0, chunkSize) == -1)
    }
  }

  test("MEMORY_WITH_SPILL mode: manual spill") {
    val tempFile = File.createTempFile("test-manual-spill-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 1024,
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      val testData = "Data to be spilled".getBytes("UTF-8")
      handle.write(testData, 0, testData.length)
      handle.finishWrite()
      
      assert(!handle.isSpilled, "Should not be spilled yet")
      
      // Manually spill
      val spilledBytes = handle.spill()
      assert(spilledBytes == testData.length)
      assert(handle.isSpilled, "Should be spilled now")
      
      // Read from spilled file
      val readBuffer = new Array[Byte](testData.length)
      val bytesRead = handle.read(readBuffer, 0, testData.length)
      
      assert(bytesRead == testData.length)
      assert(readBuffer.sameElements(testData))
    }
  }

  test("MEMORY_WITH_SPILL mode: sequential write with single bytes") {
    val tempFile = File.createTempFile("test-single-bytes-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 128,
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      // Write bytes one by one
      val testBytes = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      testBytes.foreach(b => handle.write(b.toInt))
      
      handle.finishWrite()
      
      assert(handle.getTotalBytesWritten == testBytes.length)
      
      // Read back
      val readBuffer = new Array[Byte](testBytes.length)
      val bytesRead = handle.read(readBuffer, 0, testBytes.length)
      
      assert(bytesRead == testBytes.length)
      assert(readBuffer.sameElements(testBytes))
    }
  }

  test("FILE_ONLY mode: multiple partitions sequential read") {
    val tempFile = File.createTempFile("test-partitions-", ".tmp")

    withResource(SpillablePartialFileHandle.createFileOnly(tempFile)) { handle =>
      // Simulate writing 3 partitions
      val partition0 = "Partition 0 data".getBytes("UTF-8")
      val partition1 = "Partition 1 data".getBytes("UTF-8")
      val partition2 = "Partition 2 data".getBytes("UTF-8")
      
      handle.write(partition0, 0, partition0.length)
      handle.write(partition1, 0, partition1.length)
      handle.write(partition2, 0, partition2.length)
      
      handle.finishWrite()
      
      val totalSize = partition0.length + partition1.length + partition2.length
      assert(handle.getTotalBytesWritten == totalSize)
      
      // Read partition 0
      val read0 = new Array[Byte](partition0.length)
      assert(handle.read(read0, 0, partition0.length) == partition0.length)
      assert(read0.sameElements(partition0))
      
      // Read partition 1
      val read1 = new Array[Byte](partition1.length)
      assert(handle.read(read1, 0, partition1.length) == partition1.length)
      assert(read1.sameElements(partition1))
      
      // Read partition 2
      val read2 = new Array[Byte](partition2.length)
      assert(handle.read(read2, 0, partition2.length) == partition2.length)
      assert(read2.sameElements(partition2))
      
      // EOF
      assert(handle.read(read0, 0, 10) == -1)
    }
  }

  test("FILE_ONLY mode: read with logical size larger than Int.MaxValue") {
    val testData = "dummy shuffle bytes".getBytes("UTF-8")
    val tempFile = createTempFileWithData("test-file-only-large-logical-", testData)

    withLargeLogicalFileOnlyHandle(tempFile) { handle =>
      val readBuffer = new Array[Byte](testData.length)
      assert(handle.read(readBuffer, 0, readBuffer.length) == readBuffer.length)
      assert(readBuffer.sameElements(testData))
    }
  }

  test("FILE_ONLY mode: readAt with logical size larger than Int.MaxValue") {
    val testData = "dummy shuffle bytes".getBytes("UTF-8")
    val tempFile = createTempFileWithData("test-file-only-large-logical-random-", testData)

    withLargeLogicalFileOnlyHandle(tempFile) { handle =>
      val readBuffer = new Array[Byte](testData.length)
      assert(handle.readAt(0, readBuffer, 0, readBuffer.length) == readBuffer.length)
      assert(readBuffer.sameElements(testData))
    }
  }

  test("boundedReadLength requires non-negative inputs") {
    assertThrows[IllegalArgumentException] {
      SpillablePartialFileHandle.boundedReadLengthAsInt(-1, 1L)
    }
    assertThrows[IllegalArgumentException] {
      SpillablePartialFileHandle.boundedReadLengthAsInt(1, -1L)
    }
  }

  test("Error handling: write after finish should fail") {
    val tempFile = File.createTempFile("test-error-", ".tmp")

    withResource(SpillablePartialFileHandle.createFileOnly(tempFile)) { handle =>
      handle.write("test".getBytes("UTF-8"), 0, 4)
      handle.finishWrite()
      
      assertThrows[IllegalStateException] {
        handle.write("more".getBytes("UTF-8"), 0, 4)
      }
    }
  }

  test("Error handling: read before finish should fail") {
    val tempFile = File.createTempFile("test-error2-", ".tmp")

    withResource(SpillablePartialFileHandle.createFileOnly(tempFile)) { handle =>
      handle.write("test".getBytes("UTF-8"), 0, 4)
      
      val buffer = new Array[Byte](10)
      assertThrows[IllegalStateException] {
        handle.read(buffer, 0, 10)
      }
    }
  }

  test("MEMORY_WITH_SPILL mode: chunked read") {
    val tempFile = File.createTempFile("test-chunked-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 1024,
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      // Write 100 bytes
      val testData = new Array[Byte](100)
      (0 until 100).foreach(i => testData(i) = (i % 256).toByte)
      handle.write(testData, 0, testData.length)
      handle.finishWrite()
      
      // Read in chunks of 30 bytes
      val allRead = new scala.collection.mutable.ArrayBuffer[Byte]()
      val chunkSize = 30
      val readBuffer = new Array[Byte](chunkSize)
      
      var bytesRead = handle.read(readBuffer, 0, chunkSize)
      while (bytesRead > 0) {
        allRead ++= readBuffer.take(bytesRead)
        bytesRead = handle.read(readBuffer, 0, chunkSize)
      }
      
      assert(allRead.length == testData.length)
      assert(allRead.toArray.sameElements(testData))
    }
  }

  test("MEMORY_WITH_SPILL mode: mixed single byte and array writes") {
    val tempFile = File.createTempFile("test-mixed-write-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 256,
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      // Mix single byte writes and array writes
      handle.write(0x01)
      handle.write(0x02)
      
      val chunk1 = Array[Byte](0x03, 0x04, 0x05, 0x06)
      handle.write(chunk1, 0, chunk1.length)
      
      handle.write(0x07)
      
      val chunk2 = Array[Byte](0x08, 0x09, 0x0A)
      handle.write(chunk2, 0, chunk2.length)
      
      handle.finishWrite()
      
      // Verify total bytes written
      assert(handle.getTotalBytesWritten == 10)
      
      // Read all data back
      val readBuffer = new Array[Byte](10)
      val bytesRead = handle.read(readBuffer, 0, 10)
      
      assert(bytesRead == 10)
      val expected = Array[Byte](0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A)
      assert(readBuffer.sameElements(expected))
    }
  }

  test("MEMORY_WITH_SPILL mode: read after spill during read phase") {
    val tempFile = File.createTempFile("test-spill-during-read-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 1024,
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      // Write some test data
      val testData = new Array[Byte](500)
      (0 until 500).foreach(i => testData(i) = (i % 256).toByte)
      handle.write(testData, 0, testData.length)
      handle.finishWrite()
      
      assert(!handle.isSpilled, "Should not be spilled initially")
      
      // Read first half
      val firstHalf = new Array[Byte](250)
      val firstRead = handle.read(firstHalf, 0, 250)
      assert(firstRead == 250)
      
      // Manually spill after reading first half
      val spilledBytes = handle.spill()
      assert(spilledBytes == testData.length)
      assert(handle.isSpilled, "Should be spilled now")
      
      // Continue reading second half from spilled file
      val secondHalf = new Array[Byte](250)
      val secondRead = handle.read(secondHalf, 0, 250)
      assert(secondRead == 250)
      
      // Verify both halves are correct
      assert(firstHalf.sameElements(testData.slice(0, 250)))
      assert(secondHalf.sameElements(testData.slice(250, 500)))
    }
  }

  test("MEMORY_WITH_SPILL mode: single byte write triggers expansion") {
    val tempFile = File.createTempFile("test-single-byte-expansion-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 16,  // Very small initial size
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      // Write bytes one by one to exceed initial capacity
      val testData = new Array[Byte](50)
      (0 until 50).foreach { i =>
        testData(i) = (i % 256).toByte
        handle.write(testData(i).toInt)
      }
      
      handle.finishWrite()
      
      assert(handle.getTotalBytesWritten == 50)
      assert(!handle.isSpilled, "Should have expanded buffer, not spilled")
      
      // Read and verify
      val readBuffer = new Array[Byte](50)
      val bytesRead = handle.read(readBuffer, 0, 50)
      
      assert(bytesRead == 50)
      assert(readBuffer.sameElements(testData))
    }
  }

  test("MEMORY_WITH_SPILL mode: write phase protected from spill") {
    val tempFile = File.createTempFile("test-spill-protection-", ".tmp")

    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 1024,
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.5,
      spillFile = tempFile)) { handle =>
      
      // During write phase, handle should not be spillable
      assert(!handle.spillable, "Should not be spillable during write phase")
      
      // Write some data
      val testData = "Protected data".getBytes("UTF-8")
      handle.write(testData, 0, testData.length)
      
      // Still protected
      assert(!handle.spillable, "Should still not be spillable during write")
      
      // Attempt to spill during write phase should do nothing
      val spilledBytes = handle.spill()
      assert(spilledBytes == 0, "Should not spill during write phase")
      assert(!handle.isSpilled, "Should not be spilled")
      
      // Finish write phase
      handle.finishWrite()
      
      // After finish, should be spillable
      assert(handle.spillable, "Should be spillable after write phase")
      
      // Now spill should work
      val spilledAfterFinish = handle.spill()
      assert(spilledAfterFinish == testData.length)
      assert(handle.isSpilled, "Should be spilled now")
      
      // Verify data is still readable after spill
      val readBuffer = new Array[Byte](testData.length)
      handle.read(readBuffer, 0, testData.length)
      assert(readBuffer.sameElements(testData))
    }
  }
  
  test("Disk write savings metric - MEMORY_WITH_SPILL kept in memory") {
    // This test verifies that when data stays in memory during write phase,
    // the metric logic correctly identifies it as "saved disk write"
    val tempFile = File.createTempFile("test-metric-", ".tmp")
    
    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 1024 * 1024,  // 1MB
      maxBufferSize = testMaxBufferSize,
      memoryThreshold = 0.9,  // High threshold to avoid spill
      spillFile = tempFile)) { handle =>
      
      val testData = "Test data for disk write savings metric.".getBytes("UTF-8")
      handle.write(testData, 0, testData.length)
      
      // Verify still in memory
      assert(handle.isMemoryBased)
      assert(!handle.isSpilled)
      
      // When finishWrite is called and data is still in memory,
      // it should trigger the metric recording logic
      handle.finishWrite()
      
      // Verify the handle state is correct for metric recording
      assert(handle.getTotalBytesWritten == testData.length)
      assert(handle.isMemoryBased)
      assert(!handle.isSpilled)
      
      // Note: Without a TaskContext in unit tests, the accumulator won't be
      // created, but the code should not fail. The actual metric value can
      // only be verified in integration tests with real TaskContext.
    }
  }
  
  test("Disk write savings metric - FILE_ONLY should not record") {
    // This test verifies that FILE_ONLY mode does not record disk write savings
    val tempFile = File.createTempFile("test-metric-file-", ".tmp")
    
    withResource(SpillablePartialFileHandle.createFileOnly(tempFile)) { handle =>
      val testData = "Test data for FILE_ONLY mode.".getBytes("UTF-8")
      handle.write(testData, 0, testData.length)
      
      // Verify it's file-only mode
      assert(!handle.isMemoryBased)
      
      // When finishWrite is called in FILE_ONLY mode,
      // it should NOT trigger metric recording
      handle.finishWrite()
      
      // Verify state
      assert(handle.getTotalBytesWritten == testData.length)
      assert(!handle.isMemoryBased)
    }
  }
  
  test("Disk write savings metric - MEMORY spilled during write should not record") {
    // This test verifies that if data spills during write phase,
    // it should NOT record disk write savings
    val tempFile = File.createTempFile("test-metric-spill-", ".tmp")
    
    withResource(SpillablePartialFileHandle.createMemoryWithSpill(
      initialCapacity = 100,  // Very small buffer
      maxBufferSize = 200,    // Very small max to force spill
      memoryThreshold = 0.9,
      spillFile = tempFile)) { handle =>
      
      // Write data larger than max buffer size to trigger spill during write
      val largeData = new Array[Byte](300)
      Arrays.fill(largeData, 42.toByte)
      handle.write(largeData, 0, largeData.length)
      
      // After writing, should have spilled during write phase
      assert(handle.isMemoryBased)  // Still memory-based mode
      assert(handle.isSpilled)       // But spilled to disk
      
      // When finishWrite is called after spilling during write,
      // it should NOT record disk write savings
      handle.finishWrite()
      
      // Verify state
      assert(handle.getTotalBytesWritten == largeData.length)
      assert(handle.isSpilled)
    }
  }

  private def withLargeLogicalFileOnlyHandle(tempFile: File)(
      testBody: SpillablePartialFileHandle => Unit): Unit = {
    val outputStream = mock[BufferedOutputStream]
    withResource(SpillablePartialFileHandle.createFileOnly(
      file = tempFile,
      syncWrites = false,
      bufferedOutputStreamFactory = _ => outputStream)) { handle =>
      val ignored = new Array[Byte](1)
      val expectedLogicalSize = Int.MaxValue.toLong + 4096L
      handle.write(ignored, 0, Int.MaxValue)
      handle.write(ignored, 0, 4096)
      handle.finishWrite()
      assert(handle.getTotalBytesWritten == expectedLogicalSize)
      testBody(handle)
    }
  }

  private def createTempFileWithData(prefix: String, data: Array[Byte]): File = {
    val tempFile = File.createTempFile(prefix, ".tmp")
    val out = new FileOutputStream(tempFile)
    try {
      out.write(data)
    } finally {
      out.close()
    }
    tempFile
  }

}

