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

import java.io.{ByteArrayOutputStream, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{ClosedByInterruptException, ClosedChannelException, FileChannel}
import java.nio.file.{Files, StandardOpenOption}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class SpillOutputChannelSuite extends AnyFunSuite {
  Seq("heap", "slice", "direct", "read-only").foreach { kind =>
    test(s"write data from $kind buffer") {
      val data = Array.tabulate[Byte](20003)(i => (i * 17).toByte)
      val backing = if (kind == "direct") {
        ByteBuffer.allocateDirect(data.length + 14)
      } else {
        ByteBuffer.allocate(data.length + 14)
      }
      backing.position(7)
      backing.put(data)
      backing.limit(backing.position())
      backing.position(7)
      val input = kind match {
        case "slice" => backing.slice()
        case "read-only" => backing.asReadOnlyBuffer()
        case _ => backing
      }
      val out = new ByteArrayOutputStream
      withResource(new SpillOutputChannel(out)) { channel =>
        assert(channel.write(input) == data.length)
        assert(input.position() == input.limit())
        assert(channel.write(input) == 0)
      }
      assert(out.toByteArray.sameElements(data))
    }
  }

  test("reject a null buffer") {
    withResource(new SpillOutputChannel(new ByteArrayOutputStream)) { channel =>
      intercept[NullPointerException] { channel.write(null) }
    }
  }

  test("close owns the stream even if flushing fails") {
    var closes = 0
    val out = new ByteArrayOutputStream {
      override def close(): Unit = {
        closes += 1
        throw new IOException("flush failed")
      }
    }
    val channel = new SpillOutputChannel(out)
    intercept[IOException] { channel.close() }
    assert(!channel.isOpen)
    channel.close()
    assert(closes == 1)
    intercept[ClosedChannelException] { channel.write(ByteBuffer.allocate(1)) }
  }

  test("concurrent close waits for the stream close to finish") {
    val closeStarted = new CountDownLatch(1)
    val allowClose = new CountDownLatch(1)
    val secondCloseStarted = new CountDownLatch(1)
    val secondCloseReturned = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]
    val out = new ByteArrayOutputStream {
      override def close(): Unit = {
        closeStarted.countDown()
        allowClose.await()
      }
    }
    val channel = new SpillOutputChannel(out)
    val first = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          channel.close()
        } catch {
          case t: Throwable => failure.compareAndSet(null, t)
        }
      }
    }, "first-close")
    val second = new Thread(new Runnable {
      override def run(): Unit = {
        secondCloseStarted.countDown()
        try {
          channel.close()
        } catch {
          case t: Throwable => failure.compareAndSet(null, t)
        } finally {
          secondCloseReturned.countDown()
        }
      }
    }, "second-close")
    first.setDaemon(true)
    second.setDaemon(true)
    try {
      first.start()
      assert(closeStarted.await(5, TimeUnit.SECONDS))
      second.start()
      assert(secondCloseStarted.await(5, TimeUnit.SECONDS))
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
      while (second.getState != Thread.State.BLOCKED && second.isAlive &&
          System.nanoTime() < deadline) {
        Thread.`yield`()
      }
      assert(second.getState == Thread.State.BLOCKED)
      assert(secondCloseReturned.getCount == 1)
    } finally {
      allowClose.countDown()
      first.join(5000)
      second.join(5000)
    }
    assert(!first.isAlive)
    assert(!second.isAlive)
    assert(secondCloseReturned.getCount == 0)
    assert(failure.get() == null)
  }

  test("interrupt during compressed spill does not deadlock the task killer") {
    val path = Files.createTempFile("spill-interrupt-", ".lz4")
    val file = FileChannel.open(path, StandardOpenOption.WRITE)
    val entered = new CountDownLatch(1)
    val interruptStarted = new CountDownLatch(1)
    val resume = new AtomicBoolean(false)
    val firstWrite = new AtomicBoolean(true)
    val failure = new AtomicReference[Throwable]
    val gate = new OutputStream {
      override def write(value: Int): Unit = write(Array(value.toByte), 0, 1)

      override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = synchronized {
        if (firstWrite.compareAndSet(true, false)) {
          entered.countDown()
          // Keep the destination locked until the interruption path has started.
          while (!resume.get()) {
            Thread.`yield`()
          }
        }
        val src = ByteBuffer.wrap(bytes, offset, length)
        while (src.hasRemaining) {
          file.write(src)
        }
      }

      override def close(): Unit = file.close()
    }
    // The small block makes the write reach the gated destination before interruption.
    val conf = new SparkConf(false)
      .set("spark.io.compression.codec", "lz4")
      .set("spark.io.compression.lz4.blockSize", "64")
    val channel = new SpillOutputChannel(TrampolineUtil.createCodec(conf)
      .compressedOutputStream(gate))
    val writer = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          channel.write(ByteBuffer.wrap(Array.fill[Byte](1024)(90)))
        } catch {
          case t: Throwable => failure.set(t)
        }
      }
    }, "spill-task")
    val killer = new Thread(new Runnable {
      override def run(): Unit = {
        interruptStarted.countDown()
        writer.interrupt()
      }
    }, "dispatcher-Executor")
    writer.setDaemon(true)
    killer.setDaemon(true)
    try {
      writer.start()
      assert(entered.await(5, TimeUnit.SECONDS))
      killer.start()
      assert(interruptStarted.await(5, TimeUnit.SECONDS))
      killer.join(5000)
      assert(!killer.isAlive, "task interrupt is blocked in channel close")
      resume.set(true)
      writer.join(5000)
      assert(!writer.isAlive, "spill writer did not exit after interruption")
      assert(failure.get().isInstanceOf[ClosedByInterruptException])
      assert(!file.isOpen)
    } finally {
      try {
        file.close()
      } finally {
        resume.set(true)
        killer.join(5000)
        writer.join(5000)
        if (!writer.isAlive && !killer.isAlive) {
          try {
            channel.close()
          } catch {
            case _: IOException => ()
          }
        }
        Files.deleteIfExists(path)
      }
    }
  }
}
