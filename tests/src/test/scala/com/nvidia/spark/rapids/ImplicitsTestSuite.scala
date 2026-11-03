/*
 * Copyright (c) 2019-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.{InterruptedIOException, IOException}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class ImplicitsTestSuite extends AnyFlatSpec with Matchers {
  private class RefCountTest (i: Int, throwOnClose: Boolean) extends AutoCloseable {
    var closeAttempted: Boolean = false
    var refCount: Int = 0
    override def close(): Unit = {
      closeAttempted = true
      refCount = refCount - 1
      if (refCount < 0) {
        throw new Exception(s"close called to many times for $i")
      }
      if (throwOnClose) {
        throw new Exception(s"cannot close $i")
      }
    }
    def incRefCount(): RefCountTest = {
      refCount = refCount + 1
      this
    }
    def leaked(): Boolean = {
      refCount > 0
    }
  }

  /** Records whether it was closed and whether its thread was interrupted at the time. */
  private class CloseProbe(failure: Throwable = null) extends AutoCloseable {
    var closed: Boolean = false
    var interruptedAtClose: Boolean = false
    override def close(): Unit = {
      closed = true
      interruptedAtClose = Thread.currentThread().isInterrupted
      if (failure != null) {
        throw failure
      }
    }
  }

  // A wait interrupted inside close() throws with the interrupt flag already cleared.
  private def interruptedClose = new CloseProbe(new InterruptedException("interrupted close"))
  private def failedClose = new CloseProbe(new IOException("failed close"))

  private def interruptsIn(t: Throwable): Int =
    t.getSuppressed.count(_.isInstanceOf[InterruptedException])

  private val threadTimeoutMinutes = 1L

  /**
   * Runs `body` on a new thread, so an interrupt it leaves behind cannot reach the test thread,
   * and returns its result and whether that thread was interrupted when `body` finished.
   */
  private def runOnNewThread[T](afterStart: Thread => Unit = _ => ())(
      body: => T): (Try[T], Boolean) = {
    var result: Try[T] = null
    var interrupted = false
    val thread = new Thread(() => {
      // Not Try(body): Try does not catch InterruptedException.
      result = try Success(body) catch { case t: Throwable => Failure(t) }
      interrupted = Thread.currentThread().isInterrupted
    })
    thread.setDaemon(true)
    thread.start()
    try {
      afterStart(thread)
      thread.join(TimeUnit.MINUTES.toMillis(threadTimeoutMinutes))
      assert(!thread.isAlive, "the closing thread did not finish")
    } finally {
      if (thread.isAlive) {
        // Do not leave the thread blocked behind a failed test.
        thread.interrupt()
        thread.join(TimeUnit.SECONDS.toMillis(1))
      }
    }
    (result, interrupted)
  }

  /**
   * The ways safeClose suppresses a failed close: into the error a caller such as closeOnExcept
   * passes, or into the first failed close, which it throws once the rest are closed. Each
   * closes the resources and returns the exception holding what was suppressed.
   */
  private val suppressingCloses: Seq[(String, Seq[AutoCloseable] => Throwable)] = Seq(
    ("Seq.safeClose(error)", (resources: Seq[AutoCloseable]) => {
      val error = new IOException("caller error")
      resources.safeClose(error)
      error
    }),
    ("Seq.safeClose() after a failed close", (resources: Seq[AutoCloseable]) =>
      intercept[IOException]((failedClose +: resources).safeClose())),
    ("Array.safeClose(error)", (resources: Seq[AutoCloseable]) => {
      val error = new IOException("caller error")
      resources.toArray.safeClose(error)
      error
    }),
    ("Array.safeClose() after a failed close", (resources: Seq[AutoCloseable]) =>
      intercept[IOException]((failedClose +: resources).toArray.safeClose())))

  it should "handle exceptions within safeMap body" in {
    val resources = (0 until 10).map(new RefCountTest(_, false))

    assertThrows[Throwable] {
      resources.zipWithIndex.safeMap {
        case (res, i) =>
          if (i > 5) {
            throw new Exception("bad! " + i)
          }
          res.incRefCount()
      }
    }
    assert(resources.forall(!_.leaked))
  }

  it should "handle exceptions while closing safeMap" in {
    var threw = false
    val resources = (0 until 10).map(new RefCountTest(_, true))
    try {
      resources.zipWithIndex.safeMap { case (res, i) => {
        if (i > 5) {
          throw new Exception("bad!")
        }
        res.incRefCount()
      }}
    } catch {
      case t: Throwable => {
        threw = true
        assert(t.getSuppressed().length == 5)
      }
    }
    assert(threw)
    assert(resources.forall(!_.leaked))
  }

  it should "handle an error in a safeMap from a ColumnarBatch" in {
    val resources = new ArrayBuffer[RefCountTest]()
    val batch = new ColumnarBatch((0 until 10).map { ix =>
      val scalar = GpuScalar.from(ix, IntegerType)
      val col = try {
        GpuColumnVector.from(scalar, 5, IntegerType)
      } finally {
        scalar.close()
      }
      resources += new RefCountTest(ix, false)
      col
    }.toArray)

    var colIx = 0
    assertThrows[java.lang.Exception] {
      batch.safeMap(_ => {
        if (colIx > 5) {
          throw new Exception("this is going to close my cols")
        }
        val res = resources(colIx)
        colIx = colIx + 1
        res.incRefCount()
      })
    }
    batch.close()
    assert(resources.forall(!_.leaked))
  }

  it should "handle an error while closing in a safeMap from a ColumnarBatch" in {
    val resources = new ArrayBuffer[RefCountTest]()
    val batch = new ColumnarBatch((0 until 10).map { ix => {
      val scalar = GpuScalar.from(ix, IntegerType)
      val col = try {
        GpuColumnVector.from(scalar, 5, IntegerType)
      } finally {
        scalar.close()
      }
      resources += new RefCountTest(ix, true)
      col
    }}.toArray)

    var threw = false
    var colIx = 0
    try {
      batch.safeMap(_ => {
        if (colIx > 5) {
          throw new Exception("this is going to close my cols")
        }
        val res = resources(colIx)
        colIx = colIx + 1
        res.incRefCount()
      })
    } catch {
      case t: Throwable => {
        threw = true
        assert(t.getSuppressed().length == 5)
      }
    }
    batch.close()
    assert(threw)
    assert(resources.forall(!_.leaked))
  }

  it should "safeMap/safeClose handle the success case" in {
    val resources = (0 until 10).map(new RefCountTest(_, false))
    val extraReferences = resources.safeMap(_.incRefCount)
    extraReferences.safeClose()
    assert(resources.forall(!_.leaked))
  }

  it should "handle the successful case from a ColumnarBatch" in {
    val resources = new ArrayBuffer[RefCountTest]()
    val batch = new ColumnarBatch((0 until 10).map { ix => {
      val scalar = GpuScalar.from(ix, IntegerType)
      val col = try {
        GpuColumnVector.from(scalar, 5, IntegerType)
      } finally {
        scalar.close()
      }
      resources += new RefCountTest(ix, false)
      col
    }}.toArray)

    var colIx = 0
    val result = batch.safeMap(_ => {
      val res = resources(colIx)
      colIx = colIx + 1
      res.incRefCount()
    })
    assert(resources.forall(_.refCount == 1))
    batch.close()
    result.safeClose()
    assert(resources.forall(!_.leaked))
  }

  it should "handle safeMap on array" in {
    val resources = (0 until 10).map(new RefCountTest(_, false))

    assertThrows[Throwable] {
      resources.toArray.zipWithIndex.safeMap {
        case (res, i) =>
          if (i > 5) {
            throw new Exception("bad! " + i)
          }
          res.incRefCount()
      }
    }
    assert(resources.forall(!_.leaked))
  }

  it should "handle safeMap in the successful case" in {
    val resources = (0 until 10).map(new RefCountTest(_, false))

    val out = resources.toArray.zipWithIndex.safeMap {
      case (res, _) =>
        res.incRefCount()
    }

    assert(resources.forall(_.refCount == 1))
    out.safeClose()
    assert(resources.forall(!_.leaked))
  }

  it should "safeMap on a lazy sequence (Stream) with errors" in {
    // Not used in the plugin, but illustrates how this works for a lazy sequence
    // a) a new RefCountTest(i) gets produced,
    // b) the body of the safeMap executes then (interleaved with the first map)
    // c) if the body of the safeMap throws, it cleans at that point.
    // d) the safeMap stops executing in case of error, but the first map goes until the end of
    //    the stream
    val resources: Stream[RefCountTest] = (0 until 10).toStream.map(i => new RefCountTest(i, false))

    assertThrows[Throwable] {
      resources.zipWithIndex.safeMap {
        case (x, ix) => {
          if (ix > 5) {
            throw new Exception("bad! " + ix)
          }
          x.incRefCount()
        }
      }
    }
    assert(resources.forall(!_.leaked))
  }

  it should "keep the interrupt of a wait in a close after an earlier close failed" in {
    val waiting = new CountDownLatch(1)
    val last = new CloseProbe()
    val resources = Seq[AutoCloseable](
      failedClose,
      () => {
        waiting.countDown()
        new CountDownLatch(1).await()
      },
      last)
    val (result, interrupted) = runOnNewThread { closing =>
      assert(waiting.await(threadTimeoutMinutes, TimeUnit.MINUTES))
      closing.interrupt()
    } {
      intercept[IOException](resources.safeClose())
    }
    assert(interruptsIn(result.get) == 1)
    assert(last.closed)
    assert(interrupted)
  }

  suppressingCloses.foreach { case (name, closeAll) =>
    it should s"restore a suppressed interrupt after the last close in $name" in {
      val last = new CloseProbe()
      val (result, interrupted) =
        runOnNewThread()(closeAll(Seq(interruptedClose, failedClose, last)))
      assert(interruptsIn(result.get) == 1)
      assert(last.closed)
      assert(!last.interruptedAtClose)
      assert(interrupted)
    }

    it should s"stay interrupted after two suppressed interrupts in $name" in {
      val (result, interrupted) =
        runOnNewThread()(closeAll(Seq(interruptedClose, interruptedClose)))
      assert(interruptsIn(result.get) == 2)
      assert(interrupted)
    }

    it should s"not interrupt when it suppresses no interrupt in $name" in {
      val (result, interrupted) = runOnNewThread()(closeAll(Seq(failedClose, new CloseProbe())))
      assert(result.get.getSuppressed.nonEmpty)
      assert(!interrupted)
    }
  }

  it should "restore an interrupt that AutoCloseable.safeClose(error) suppresses" in {
    val error = new IOException("caller error")
    val (result, interrupted) = runOnNewThread()(interruptedClose.safeClose(error))
    assert(result.isSuccess)
    assert(interruptsIn(error) == 1)
    assert(interrupted)
  }

  it should "not interrupt when AutoCloseable.safeClose(error) suppresses no interrupt" in {
    val error = new IOException("caller error")
    val (result, interrupted) = runOnNewThread()(failedClose.safeClose(error))
    assert(result.isSuccess)
    assert(error.getSuppressed.length == 1)
    assert(!interrupted)
  }

  it should "throw an InterruptedException from the first failed close as before" in {
    val (result, interrupted) = runOnNewThread()(interruptedClose.safeClose())
    assert(result.failed.get.isInstanceOf[InterruptedException])
    assert(!interrupted)

    val closers = Seq[(String, Seq[AutoCloseable] => Unit)](
      ("Seq.safeClose()", _.safeClose()),
      ("Array.safeClose()", _.toArray.safeClose()))
    closers.foreach { case (name, closeAll) =>
      val last = new CloseProbe()
      val (result, interrupted) = runOnNewThread()(closeAll(Seq(interruptedClose, last)))
      withClue(name) {
        assert(result.failed.get.isInstanceOf[InterruptedException])
        assert(last.closed)
        assert(!last.interruptedAtClose)
        assert(!interrupted)
      }
    }
  }

  it should "restore at the end of each call, so later closes of an enclosing call see it" in {
    val last = new CloseProbe()
    val (result, interrupted) = runOnNewThread()(Seq[AutoCloseable](
      () => Seq(failedClose, interruptedClose).safeClose(),
      last).safeClose())
    assert(result.failed.get.isInstanceOf[IOException])
    assert(last.closed)
    assert(last.interruptedAtClose)
    assert(interrupted)
  }

  it should "not treat an InterruptedIOException or a wrapped interrupt as an interrupt" in {
    val (result, interrupted) = runOnNewThread()(Seq[AutoCloseable](
      failedClose,
      new CloseProbe(new InterruptedIOException("timed out")),
      new CloseProbe(new IOException(new InterruptedException("wrapped")))).safeClose())
    assert(result.failed.get.getSuppressed.length == 2)
    assert(!interrupted)
  }

  it should "restore a suppressed interrupt when the one it throws is also an interrupt" in {
    val (result, interrupted) =
      runOnNewThread()(Seq(interruptedClose, interruptedClose).safeClose())
    assert(result.failed.get.isInstanceOf[InterruptedException])
    assert(interruptsIn(result.failed.get) == 1)
    assert(interrupted)
  }

  it should "not interrupt when the caller's error is the InterruptedException" in {
    val closers = Seq[(String, (Seq[AutoCloseable], Throwable) => Unit)](
      ("AutoCloseable.safeClose(error)", (resources, error) => resources.head.safeClose(error)),
      ("Seq.safeClose(error)", (resources, error) => resources.safeClose(error)),
      ("Array.safeClose(error)", (resources, error) => resources.toArray.safeClose(error)))
    closers.foreach { case (name, closeAll) =>
      val error = new InterruptedException("caller error")
      val (result, interrupted) = runOnNewThread()(closeAll(Seq(failedClose), error))
      withClue(name) {
        assert(result.isSuccess)
        assert(error.getSuppressed.length == 1)
        assert(!interrupted)
      }
    }
  }
}

