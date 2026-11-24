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

package com.nvidia.spark.rapids.shuffle.ucx

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import org.openucx.jucx.{UcxCallback, UcxException}
import org.openucx.jucx.ucp.UcpRequest
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests `UCX.sendOrReport` and the send task it runs, `UCX.sendTask`. A task that throws must
 * still end its send exactly once, or the send's transaction never completes. A test cannot
 * build a UCX, so the tests stand in for JUCX's send, or for the whole task. The suite also
 * covers how `UCX.queueTask` queues a task for the progress thread.
 */
class UCXSendTaskSuite extends AnyFunSuite {

  /** A sender's callback that records how its send ended. */
  private class Ended extends UcxCallback {
    val calls = new ArrayBuffer[String]()
    override def onSuccess(request: UcpRequest): Unit = calls += "success"
    override def onError(ucsStatus: Int, errorMsg: String): Unit =
      calls += s"error $ucsStatus: $errorMsg"
  }

  test("a send task that throws before UCX calls back reports one error and rethrows") {
    val ended = new Ended
    // what allocating the send's header throws when direct memory runs out
    val oom = new OutOfMemoryError("Direct buffer memory")
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(ended)(_ => throw oom))
    assert(thrown eq oom)
    assertResult(1)(ended.calls.size)
    assert(ended.calls.head.startsWith(s"error ${UCX.SEND_TASK_FAILED}: "))
    assert(ended.calls.head.contains("Direct buffer memory"))
  }

  test("a send task whose completion cannot be built reports through the sender's callback") {
    val ended = new Ended
    // what building the completion throws when the Java heap runs out
    val oom = new OutOfMemoryError("Java heap space")
    var ran = false
    val thrown = intercept[OutOfMemoryError](
      UCX.sendOrReport(ended, _ => throw oom)(_ => ran = true))
    assert(thrown eq oom)
    assert(!ran)
    assertResult(1)(ended.calls.size)
    assert(ended.calls.head.startsWith(s"error ${UCX.SEND_TASK_FAILED}: "))
    // fixed text, since text built from the error would allocate
    assert(!ended.calls.head.contains("Java heap space"))
  }

  test("a send task whose report cannot describe the error still reports it") {
    val ended = new Ended
    // the heap ran out for the task, and again for the text of its report
    val oom = new OutOfMemoryError("Java heap space") {
      override def toString: String = throw new OutOfMemoryError("Java heap space")
    }
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(ended)(_ => throw oom))
    assert(thrown eq oom)
    assertResult(1)(ended.calls.size)
    assert(ended.calls.head.startsWith(s"error ${UCX.SEND_TASK_FAILED}: "))
  }

  test("a send task whose error cannot describe itself still reports it") {
    val ended = new Ended
    val failure = new IllegalStateException("a send failure in test") {
      override def toString: String = throw new IllegalStateException("its text failed in test")
    }
    val thrown = intercept[IllegalStateException](UCX.sendOrReport(ended)(_ => throw failure))
    assert(thrown eq failure)
    assertResult(1)(ended.calls.size)
    assert(ended.calls.head.startsWith(s"error ${UCX.SEND_TASK_FAILED}: "))
  }

  test("a send that UCX failed at once, before JUCX threw, is reported once") {
    // JUCX 1.22 calls the callback for an immediate error, then the exception reaches Java
    val ended = new Ended
    val failed = new UcxException("Connection reset by remote peer", -25)
    val thrown = intercept[UcxException](UCX.sendOrReport(ended) { completion =>
      completion.handOff()
      completion.onError(-25, "Connection reset by remote peer")
      throw failed
    })
    assert(thrown eq failed)
    assertResult(Seq("error -25: Connection reset by remote peer"))(ended.calls)
  }

  test("a send that succeeds, at once or later, reports one success") {
    val atOnce = new Ended
    UCX.sendOrReport(atOnce) { completion =>
      completion.handOff()
      completion.onSuccess(null)
    }
    assertResult(Seq("success"))(atOnce.calls)

    val later = new Ended
    var pending: UCX.SendCompletion = null
    UCX.sendOrReport(later) { completion =>
      completion.handOff()
      pending = completion
    }
    assert(later.calls.isEmpty)
    pending.onSuccess(null)
    assertResult(Seq("success"))(later.calls)
  }

  test("a JVM error after the hand-off is left to the send's own callback") {
    // JUCX can run out of heap after UCX took the send. UCX still calls back, and may read the
    // send's buffers until it does.
    val ended = new Ended
    var pending: UCX.SendCompletion = null
    val oom = new OutOfMemoryError("Java heap space")
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(ended) { completion =>
      completion.handOff()
      pending = completion
      throw oom
    })
    assert(thrown eq oom)
    assert(ended.calls.isEmpty)
    pending.onSuccess(null)
    assertResult(Seq("success"))(ended.calls)
  }

  test("a throw from JUCX before it took the send is reported once") {
    // as from an endpoint that closed before the send reached UCX
    val ended = new Ended
    val failure = new IllegalStateException("the endpoint is closed in test")
    val thrown = intercept[IllegalStateException](UCX.sendOrReport(ended)(
      UCX.sendTask(1L, _, () => ())((_, _, _) => throw failure)))
    assert(thrown eq failure)
    assertResult(1)(ended.calls.size)
    assert(ended.calls.head.startsWith(s"error ${UCX.SEND_TASK_FAILED}: "))
  }

  test("a throw after JUCX took the send is left to the send's own callback") {
    // as when closing the send's NVTX range fails, once JUCX returned with the send in flight
    val ended = new Ended
    var pending: UCX.SendCompletion = null
    val failure = new IllegalStateException("closing the send's range failed in test")
    val thrown = intercept[IllegalStateException](UCX.sendOrReport(ended) { completion =>
      UCX.sendTask(1L, completion, () => ())((_, _, inFlight) => pending = inFlight)
      throw failure
    })
    assert(thrown eq failure)
    // UCX may read the send's buffers until it calls back, so nothing may say the send ended
    assert(ended.calls.isEmpty)
    pending.onSuccess(null)
    assertResult(Seq("success"))(ended.calls)
  }

  test("a callback after the task's error was reported is dropped") {
    val ended = new Ended
    var pending: UCX.SendCompletion = null
    val failure = new IllegalStateException("the endpoint is closed in test")
    intercept[IllegalStateException](UCX.sendOrReport(ended) { completion =>
      completion.handOff()
      pending = completion
      throw failure
    })
    assertResult(1)(ended.calls.size)
    pending.onSuccess(null)
    pending.onError(-25, "a late error in test")
    assertResult(1)(ended.calls.size)
  }

  test("a send task hands JUCX its header's address, what it prepared and the completion") {
    val ended = new Ended
    var header: ByteBuffer = null
    var sent: (Long, String, Long) = null
    UCX.sendOrReport(ended)(UCX.sendTask(0x1234L, _, () => "params",
        allocate = size => {
          header = ByteBuffer.allocateDirect(size)
          header
        },
        addressOf = buffer => {
          assert(buffer eq header)
          42L
        }) { (address, prepared, completion) =>
      sent = (address, prepared, header.getLong(0))
      completion.onSuccess(null)
    })
    assertResult((42L, "params", 0x1234L))(sent)
    assertResult(Seq("success"))(ended.calls)
  }

  test("a send task whose header cannot be allocated reports it once") {
    val ended = new Ended
    val oom = new OutOfMemoryError("Direct buffer memory")
    var called = false
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(ended)(UCX.sendTask(1L, _,
      () => (), allocate = _ => throw oom)((_, _, _) => called = true)))
    assert(thrown eq oom)
    assert(!called)
    assertResult(1)(ended.calls.size)
  }

  test("a send task whose request parameters cannot be built reports it once") {
    val ended = new Ended
    val oom = new OutOfMemoryError("Java heap space")
    var called = false
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(ended)(
      UCX.sendTask[Unit](1L, _, () => throw oom)((_, _, _) => called = true)))
    assert(thrown eq oom)
    assert(!called)
    assertResult(1)(ended.calls.size)
  }

  test("a send task that cannot take its header's address reports it once") {
    // Taking the address boxes it, which can run out of heap before JUCX is called, so UCX
    // never has the send and nothing else would end it.
    val ended = new Ended
    val oom = new OutOfMemoryError("Java heap space")
    var called = false
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(ended)(UCX.sendTask(1L, _,
      () => (), addressOf = _ => throw oom)((_, _, _) => called = true)))
    assert(thrown eq oom)
    assert(!called)
    assertResult(1)(ended.calls.size)
  }

  test("a report that throws keeps the send task's error, with the report's as suppressed") {
    val reportFailure = new IllegalStateException("the sender's callback failed in test")
    val failing = new UcxCallback {
      override def onError(ucsStatus: Int, errorMsg: String): Unit = throw reportFailure
    }
    val oom = new OutOfMemoryError("Direct buffer memory")
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(failing)(_ => throw oom))
    assert(thrown eq oom)
    assert(thrown.getSuppressed.exists(_ eq reportFailure))

    // the same when the completion cannot be built, so the report goes to the sender's callback
    val heap = new OutOfMemoryError("Java heap space")
    val unbuilt = intercept[OutOfMemoryError](UCX.sendOrReport(failing, _ => throw heap)(_ => ()))
    assert(unbuilt eq heap)
    assert(unbuilt.getSuppressed.exists(_ eq reportFailure))
  }

  test("a send's range that fails to close keeps the task's error, which decides the report") {
    val ended = new Ended
    val closeFailure = new IllegalStateException("closing the send's range failed in test")
    val range = new NvtxRange("send in test", NvtxColor.GREEN) {
      override def close(): Unit = throw closeFailure
    }
    // as from an immediate UCX error, which JUCX throws with its status
    val failure = new UcxException("the send failed in test", -3)
    val thrown = intercept[UcxException](UCX.sendOrReport(ended)(
      UCX.sendTaskInRange(range, 1L, _, () => ())((_, _, _) => throw failure)))
    assert(thrown eq failure)
    assert(thrown.getSuppressed.exists(_ eq closeFailure))
    assertResult(1)(ended.calls.size)
    assert(ended.calls.head.startsWith("error -3: "))
  }

  test("a send's range closes once when the send is handed to JUCX") {
    val ended = new Ended
    val range = new CountedRange
    UCX.sendOrReport(ended)(UCX.sendTaskInRange(range, 1L, _, () => ())(
      (_, _, completion) => completion.onSuccess(null)))
    assertResult(1)(range.closes)
    assertResult(Seq("success"))(ended.calls)
  }

  /** An NVTX range that counts its closes. */
  private class CountedRange extends NvtxRange("range in test", NvtxColor.RED) {
    var closes = 0
    override def close(): Unit = closes += 1
  }

  test("a worker task is queued after its signal's range is built, and signalled once queued") {
    val tasks = new ConcurrentLinkedQueue[() => Unit]()
    // what building the worker signal's NVTX range throws when the Java heap runs out
    val oom = new OutOfMemoryError("Java heap space")
    val thrown = intercept[OutOfMemoryError](UCX.queueTask(tasks, () => (), wakeup = true,
      signal = () => (), newRange = () => throw oom))
    assert(thrown eq oom)
    // the caller sees a throw and frees what the task would use, so the task must not run
    assert(tasks.isEmpty)

    // when the queue refuses the task, queueTask closes the range it built and signals nothing
    val unqueued = new CountedRange
    intercept[NullPointerException](UCX.queueTask(tasks, null, wakeup = true,
      signal = () => fail("signalled a task that was not queued"), newRange = () => unqueued))
    assertResult(1)(unqueued.closes)
    assert(tasks.isEmpty)

    // an ordinary failure to wake the worker is only logged, since the task stays queued
    val signalled = new CountedRange
    UCX.queueTask(tasks, () => (), wakeup = true,
      signal = () => throw new IllegalStateException("signal failed in test"),
      newRange = () => signalled)
    assertResult(1)(tasks.size)
    assertResult(1)(signalled.closes)

    // the worker is woken once, after the task is queued, and only when wakeup is on
    val woken = new ArrayBuffer[Boolean]()
    val task: () => Unit = () => ()
    UCX.queueTask(tasks, task, wakeup = true, signal = () => {
      woken += tasks.contains(task)
      ()
    }, newRange = () => new CountedRange)
    UCX.queueTask(tasks, () => (), wakeup = false, signal = () => {
      woken += false
      ()
    })
    assertResult(Seq(true))(woken)
    assertResult(3)(tasks.size)
  }
}
