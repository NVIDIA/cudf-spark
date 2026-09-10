/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.io.async

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, File, FileInputStream}
import java.io.{FileOutputStream, IOException, OutputStream, PipedInputStream, PipedOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{Callable, CountDownLatch, ExecutorService, Future, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import com.google.common.util.concurrent.ForwardingExecutorService
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.rapids.execution.TrampolineUtil

class AsyncOutputStreamSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val bufLen = 4
  private val buf: Array[Byte] = new Array[Byte](bufLen)
  private val maxBufCount = 10
  private val trafficController = new TrafficController(
    new HostMemoryThrottle(bufLen * maxBufCount))

  def openStream(writeDelayMs: Long = 0L): (AsyncOutputStream, String) = {
    val file = File.createTempFile("async-write-test", "tmp")
    val stream = if (writeDelayMs == 0L) {
      AsyncOutputStream(() => {
        new BufferedOutputStream(new FileOutputStream(file))
      }, trafficController, Seq.empty)
    } else {
      val del = TrampolineUtil.newDaemonCachedThreadPool("AsyncOutputStream", 1, 1)
      val executor = new ThrottlingExecutor(
        new ForwardingExecutorService {
          override def delegate(): ExecutorService = del

          /**
           * Technically, overriding this method is good enough for the test, but we also override
           * the other submit methods as well in case we modify our code to use them in the future.
           */
          override def submit[T](task: Callable[T]): Future[T] = {
            super.submit(() => {
              Thread.sleep(writeDelayMs)
              task.call()
            })
          }

          override def submit(task: Runnable): Future[_] = {
            super.submit(new Runnable {
              override def run(): Unit = {
                Thread.sleep(writeDelayMs)
                task.run()
              }
            })
          }

          override def submit[T](task: Runnable, result: T): Future[T] = {
            super.submit(() => {
              Thread.sleep(writeDelayMs)
              task.run()
            }, result)
          }
        },
        trafficController,
        _ => ())
      new AsyncOutputStream(() => {
        new BufferedOutputStream(new FileOutputStream(file))
      }, executor)
    }
    (stream, file.getAbsolutePath)
  }

  test("open, write, and close") {
    val numBufs = 1000
    val (stream, _) = openStream()
    withResource(stream) { os =>
      for (_ <- 0 until numBufs) {
        os.write(buf)
      }
    }
    assertResult(bufLen * numBufs)(stream.metrics.numBytesScheduled)
    assertResult(bufLen * numBufs)(stream.metrics.numBytesWritten.get())
  }

  test("pipe-backed delegate does not see async writer thread as dead between writes") {
    val pipeSource = new PipedInputStream(bufLen * maxBufCount)
    val pipeSink = new PipedOutputStream(pipeSource)
    val firstBufferRead = new CountDownLatch(1)
    val secondReadStarted = new CountDownLatch(1)
    val pipeConsumerFailure = new AtomicReference[Throwable]()
    val pipeConsumerExecutor =
      TrampolineUtil.newDaemonCachedThreadPool("AsyncOutputStreamPipeConsumer", 1, 1)
    withResource(new AutoCloseable {
      override def close(): Unit = pipeConsumerExecutor.shutdownNow()
    }) { _ =>
      withResource(pipeSource) { _ =>
        val pipeConsumerFuture = pipeConsumerExecutor.submit(new Callable[Unit] {
          override def call(): Unit = {
            val readBuffer = new Array[Byte](bufLen)
            var totalBytesRead = 0
            var readCount = 0
            var done = false

            try {
              while (!done) {
                readCount += 1
                if (readCount == 2) {
                  secondReadStarted.countDown()
                }
                val bytesRead = pipeSource.read(readBuffer)
                if (bytesRead == -1) {
                  done = true
                } else {
                  totalBytesRead += bytesRead
                  if (totalBytesRead >= bufLen) {
                    firstBufferRead.countDown()
                  }
                }
            }
          } catch {
            case t: Throwable =>
                pipeConsumerFailure.set(t)
                throw t
            }
          }
        })

        withResource(AsyncOutputStream(() => pipeSink, trafficController, Seq.empty)) { os =>
          os.write(buf)
          assert(firstBufferRead.await(5, TimeUnit.SECONDS),
            "timed out waiting for first pipe read")
          assert(secondReadStarted.await(5, TimeUnit.SECONDS),
            "timed out waiting for second pipe read")

          Thread.sleep(2500)
          assert(pipeConsumerFailure.get() == null,
            s"pipe consumer failed while waiting for more data: ${pipeConsumerFailure.get()}")

          os.write(buf)
        }
        pipeConsumerFuture.get(5, TimeUnit.SECONDS)
      }
    }
  }

  def testWrite(writeCall: (AsyncOutputStream, Int) => Unit,
      readCall: DataInputStream => Int): Unit = {
    val numInts = 50
    val (asyncStream, outputPath) = openStream(10)
    withResource(asyncStream) { asyncStream =>
      for (i <- 0 until numInts) {
        writeCall(asyncStream, i)
      }
    }

    val file = new File(outputPath)

    withResource(new FileInputStream(file)) {
      fis => withResource(new BufferedInputStream(fis)) {
        bis => withResource(new DataInputStream(bis)) {
          dis =>
            for (i <- 0 until numInts) {
              val value = readCall(dis)
              assert(value == i, s"Expected $i but got $value")
            }
        }
      }
    }
  }

  test("write ints") {
    testWrite(
      { (asyncStream, i) =>
        asyncStream.write(i)
      },
      { dis =>
        dis.read()
      }
    )
  }

  test("write byte arrays") {
    val buf = new Array[Byte](Integer.BYTES)
    val bb = ByteBuffer.wrap(buf)
    testWrite(
      { (asyncStream, i) =>
        bb.clear()
        bb.putInt(i)
        asyncStream.write(buf)
      },
      { dis =>
        dis.readInt()
      }
    )
  }

  test("write byte arrays with offset") {
    // We will use only the Integer.BYTES bytes in the middle of the buffer
    val buf = new Array[Byte](Integer.BYTES * 3)
    val bb = ByteBuffer.wrap(buf)
    bb.position(Integer.BYTES)
    bb.mark()
    testWrite(
      { (asyncStream, i) =>
        bb.reset()
        bb.putInt(i)
        asyncStream.write(buf, Integer.BYTES, Integer.BYTES)
      },
      { dis =>
        dis.readInt()
      }
    )
  }

  test("write after closed") {
    val (os, _) = openStream()
    os.close()
    assertThrows[IOException] {
      os.write(buf)
    }
  }

  test("flush after closed") {
    val (os, _) = openStream()
    os.close()
    assertThrows[IOException] {
      os.flush()
    }
  }

  class ThrowingOutputStream extends OutputStream {

    var failureCount = 0

    override def write(i: Int): Unit = {
      failureCount += 1
      throw new IOException(s"Failed ${failureCount} times")
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      failureCount += 1
      throw new IOException(s"Failed ${failureCount} times")
    }
  }

  def assertThrowsWithMsg[T](fn: Callable[T], clue: String,
      expectedMsgPrefix: String): Unit = {
    withClue(clue) {
      try {
        fn.call()
      } catch {
        case t: Throwable =>
          assertIOExceptionMsg(t, expectedMsgPrefix)
      }
    }
  }

  def assertIOExceptionMsg(t: Throwable, expectedMsgPrefix: String): Unit = {
    if (t.getClass.isAssignableFrom(classOf[IOException])) {
      if (!t.getMessage.contains(expectedMsgPrefix)) {
        fail(s"Unexpected exception message: ${t.getMessage}")
      }
    } else {
      if (t.getCause != null) {
        assertIOExceptionMsg(t.getCause, expectedMsgPrefix)
      } else {
        fail(s"Unexpected exception: $t")
      }
    }
  }

  test("write after error") {
    val os = AsyncOutputStream(() => new ThrowingOutputStream, trafficController, Seq.empty)

    // The first call to `write` should succeed
    os.write(buf)

    // Wait for the first write to fail
    while (os.lastError.get().isEmpty) {
      Thread.sleep(100)
    }

    // The second `write` call should fail with the exception thrown by the first write failure
    assertThrowsWithMsg(() => os.write(buf),
      "The second write should fail with the exception thrown by the first write failure",
      "Failed 1 times")

    // `close` throws the same exception
    assertThrowsWithMsg(() => os.close(),
      "The second write should fail with the exception thrown by the first write failure",
      "Failed 1 times")

    assertResult(bufLen)(os.metrics.numBytesScheduled)
    assertResult(0)(os.metrics.numBytesWritten.get())
    assert(os.lastError.get().get.isInstanceOf[IOException])
  }

  test("flush after error") {
    val os = AsyncOutputStream(() => new ThrowingOutputStream, trafficController, Seq.empty)

    // The first write should succeed
    os.write(buf)

    // The flush should fail with the exception thrown by the write failure
    assertThrowsWithMsg(() => os.flush(),
      "The flush should fail with the exception thrown by the write failure",
      "Failed 1 times")

    // `close` throws the same exception
    assertThrowsWithMsg(() => os.close(),
      "The flush should fail with the exception thrown by the write failure",
      "Failed 1 times")
  }

  test("close after error") {
    val os = AsyncOutputStream(() => new ThrowingOutputStream, trafficController, Seq.empty)

    os.write(buf)

    assertThrowsWithMsg(() => os.close(),
      "Close should fail with the exception thrown by the write failure",
      "Failed 1 times")
  }
}
