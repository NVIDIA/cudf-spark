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

package org.apache.spark.sql.rapids

import java.io.{ByteArrayInputStream, InputStream}
import java.util.concurrent.{BlockingQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.{BaseSerializedTableIterator, GpuColumnarBatchSerializer}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when, CALLS_REAL_METHODS}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskContext
import org.apache.spark.serializer.{DeserializationStream, SerializerInstance}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.{BlockId, RapidsShuffleBlockFetcherIterator, ShuffleBlockId}

/** Exercise admission independently of network delivery and native serialization. */
class ShuffleStreamAdmissionSuite extends AnyFunSuite with BeforeAndAfterEach {
  private def field(obj: AnyRef, name: String): java.lang.reflect.Field = {
    val fields = Iterator.iterate[Class[_]](obj.getClass)(_.getSuperclass)
      .takeWhile(_ != null).flatMap(_.getDeclaredFields).toSeq
    val result = fields.find { f =>
      f.getName == name || f.getName.endsWith("$$" + name)
    }.getOrElse(throw new IllegalArgumentException(s"Missing field $name"))
    result.setAccessible(true)
    result
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(0, 2)
  }

  override def afterEach(): Unit = {
    try RapidsShuffleInternalManagerBase.stopThreadPool() finally super.afterEach()
  }

  private class Fixture(counts: Seq[Int], cap: Int, limit: Long = 1024,
      gate: Option[CountDownLatch] = None, entered: Option[CountDownLatch] = None)
      extends AutoCloseable {
    val context = TaskContext.empty()
    // Constructor dependencies are irrelevant to the nested iterator. Keep its real methods.
    val reader = mock(classOf[RapidsShuffleThreadedReaderBase[Int, Int]], CALLS_REAL_METHODS)
    field(reader, "context").set(reader, context)
    field(reader, "maxBytesInFlight").setLong(reader, limit)
    field(reader, "numReaderThreads").setInt(reader, 2)
    classOf[RapidsShuffleThreadedReaderBase[_, _]].getDeclaredFields
      .filter(_.getType == classOf[Option[_]]).foreach { f =>
      f.setAccessible(true)
      f.set(reader, None)
    }
    val created = new AtomicInteger
    val closed = new AtomicInteger
    val workerFinished = new CountDownLatch(1)
    val streams = counts.map(_ => new ByteArrayInputStream(Array.emptyByteArray))
    private var fetched = 0
    private val fetcher = mock(classOf[RapidsShuffleBlockFetcherIterator])
    when(fetcher.hasNext).thenAnswer(_ => Boolean.box(fetched < counts.size))
    when(fetcher.resultCount).thenAnswer(_ => Int.box(counts.size - fetched))
    when(fetcher.next()).thenAnswer { _ =>
      val index = fetched
      fetched += 1
      (ShuffleBlockId(1, index, 0): BlockId, streams(index): InputStream)
    }
    private val serializer = mock(classOf[GpuColumnarBatchSerializer])
    private val instance = mock(classOf[SerializerInstance])
    when(serializer.newInstance()).thenReturn(instance)
    when(instance.deserializeStream(any(classOf[InputStream]))).thenAnswer { invocation =>
      val stream = invocation.getArgument[InputStream](0)
      val index = streams.indexOf(stream)
      // Both real serialized iterators register stream closure at task completion.
      onTaskCompletion(context) { stream.close() }
      var consumed = 0
      val batches = new BaseSerializedTableIterator {
        override def hasNext: Boolean = consumed < counts(index)
        override def peekNextBatchSize(): Option[Long] = if (hasNext) Some(128L) else None
        override def next(): (Int, ColumnarBatch) = {
          entered.foreach(_.countDown())
          gate.foreach { latch =>
            var waiting = true
            while (waiting) {
              try {
                latch.await()
                waiting = false
              } catch {
                case _: InterruptedException => // Simulate native work that finishes after cancel.
              }
            }
          }
          val key = index * 100 + consumed
          consumed += 1
          created.incrementAndGet()
          val batch = new ColumnarBatch(Array.empty[ColumnVector], 1) {
            override def close(): Unit = {
              super.close()
              closed.incrementAndGet()
              workerFinished.countDown()
            }
          }
          (key, batch)
        }
      }
      val ds = mock(classOf[DeserializationStream])
      when(ds.asKeyValueIterator).thenReturn(batches.asInstanceOf[Iterator[(Any, Any)]])
      ds
    }
    val iterator = new reader.RapidsShuffleThreadedBlockIterator(fetcher, serializer, cap)

    def prime(): Unit = {
      val method = iterator.getClass.getDeclaredMethods
        .find(_.getName.endsWith("popFetchedIfAvailable")).get
      method.setAccessible(true)
      method.invoke(iterator)
    }

    def drain(): Seq[Int] = {
      val result = ArrayBuffer.empty[Int]
      while (iterator.hasNext) {
        val (key, value) = iterator.next()
        withResource(value.asInstanceOf[ColumnarBatch]) { _ => result += key.asInstanceOf[Int] }
      }
      result.toSeq
    }

    def open: Int = field(iterator, "openStreams").get(iterator)
      .asInstanceOf[AtomicInteger].get()
    def queued: Int = field(iterator, "queued").get(iterator)
      .asInstanceOf[BlockingQueue[_]].size()
    def reserved: Long = field(iterator, "limiter").get(iterator)
      .asInstanceOf[BytesInFlightLimiter].getBytesInFlight
    override def close(): Unit = context.markTaskCompleted(None)
  }

  for (cap <- Seq(1, 2, 64); limit <- Seq(1L, 128L, 1024L)) {
    test(s"preserve all batches across admission and byte limits: cap=$cap bytes=$limit") {
      val counts = (0 until 80).map(i => i % 5)
      withResource(new Fixture(counts, cap, limit)) { f =>
        val expected = counts.indices.flatMap(i => (0 until counts(i)).map(i * 100 + _))
        assert(f.drain().sorted == expected)
        assert(f.open == 0)
        assert(f.reserved == 0)
        assert(f.created.get() == f.closed.get())
      }
    }
  }

  for (cap <- Seq(1, 64)) {
    test(s"empty streams make progress to exhaustion: cap=$cap") {
      withResource(new Fixture(Seq.fill(65)(0), cap)) { f =>
        assert(!f.iterator.hasNext)
        assert(!f.iterator.hasNext)
        intercept[NoSuchElementException] { f.iterator.next() }
        assert(f.open == 0)
      }
    }
  }

  test("admission does not initialize streams beyond the cap") {
    val gate = new CountDownLatch(1)
    val entered = new CountDownLatch(1)
    withResource(new Fixture(Seq.fill(10)(1), 2, gate = Some(gate),
        entered = Some(entered))) { f =>
      try {
        f.prime()
        assert(entered.await(5, TimeUnit.SECONDS))
        assert(f.open == 2)
      } finally {
        gate.countDown()
      }
      assert(f.drain().size == 10)
    }
  }

  test("a worker finishing after task completion closes its batch instead of publishing") {
    val gate = new CountDownLatch(1)
    val entered = new CountDownLatch(1)
    val f = new Fixture(Seq(1), 1, gate = Some(gate), entered = Some(entered))
    try {
      f.prime()
      assert(entered.await(5, TimeUnit.SECONDS))
      f.close()
    } finally {
      gate.countDown()
    }
    assert(f.workerFinished.await(5, TimeUnit.SECONDS))
    assert(f.queued == 0)
    assert(f.created.get() == f.closed.get())
  }
}
