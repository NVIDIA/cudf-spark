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
package org.apache.spark.sql.rapids.execution

import java.util.concurrent.{CompletableFuture, ExecutionException, TimeoutException, TimeUnit}

import scala.collection.mutable

import org.roaringbitmap.RoaringBitmap

private[rapids] object ExecutorBroadcastCache {
  def generation(stageId: Int, attempt: Int): Long =
    (stageId.toLong << 32) | (attempt.toLong & 0xffffffffL)
}

/**
 * Owns one build value per shuffle and consumer-stage attempt in an executor. A borrower
 * must retain its own reference before returning from `borrow`. Compact invalidated IDs
 * remain until the application ends, preventing late consumers from recreating cleaned data.
 * Stage attempts use separate entries so retries cannot borrow a replica from an old attempt.
 */
private[rapids] class ExecutorBroadcastCache[T](dispose: T => Unit) extends AutoCloseable {
  private class Entry {
    val ready = new CompletableFuture[Unit]()
    var value: Option[T] = None
    var invalidated = false
  }

  private val entries = mutable.Map.empty[Int, mutable.Map[Long, Entry]]
  private val retired = new RoaringBitmap

  private[execution] def activeEntryCount: Int = entries.synchronized {
    entries.valuesIterator.map(_.size).sum
  }

  private[execution] def retiredShuffleCount: Int = entries.synchronized {
    retired.getCardinality
  }

  private[execution] def retiredStateBytes: Int = entries.synchronized {
    retired.getSizeInBytes
  }
  private var closed = false

  private def removed(id: Int): IllegalStateException =
    new IllegalStateException(s"Broadcast build shuffle $id has been unregistered")

  /** `beforeWait` releases task resources; `checkCancelled` must throw for a cancelled task. */
  def getOrBuild[B](
      id: Int,
      build: () => T,
      beforeWait: () => Unit,
      checkCancelled: () => Unit,
      beforeBorrow: () => Unit = () => (),
      generation: Long = 0L)(borrow: T => B): B = {
    checkCancelled()
    var builder = false
    val entry = entries.synchronized {
      require(!closed, "Executor broadcast cache is closed")
      if (retired.contains(id)) {
        throw removed(id)
      }
      entries.getOrElseUpdate(id, mutable.Map.empty).getOrElseUpdate(generation, {
        builder = true
        new Entry
      })
    }
    if (builder) {
      try {
        val value = build()
        val accepted = entry.synchronized {
          if (entry.invalidated) {
            false
          } else {
            entry.value = Some(value)
            entry.ready.complete(())
            true
          }
        }
        if (!accepted) {
          dispose(value)
          throw removed(id)
        }
      } catch {
        case error: Throwable =>
          entry.ready.completeExceptionally(error)
          entries.synchronized {
            entry.synchronized {
              if (!entry.invalidated) {
                entries.get(id).foreach { generations =>
                  if (generations.get(generation).contains(entry)) {
                    generations.remove(generation)
                    if (generations.isEmpty) {
                      entries.remove(id)
                    }
                  }
                }
              }
            }
          }
          throw error
      }
    } else if (!entry.ready.isDone) {
      beforeWait()
    }

    var ready = false
    while (!ready) {
      checkCancelled()
      try {
        entry.ready.get(100, TimeUnit.MILLISECONDS)
        ready = true
      } catch {
        case _: TimeoutException =>
        case failure: ExecutionException => throw failure.getCause
      }
    }
    checkCancelled()
    // Do not wait for task resources while holding the entry's ownership lock.
    beforeBorrow()
    entry.synchronized {
      if (entry.invalidated) {
        throw removed(id)
      }
      borrow(entry.value.get)
    }
  }

  private def retireEntry(id: Int, entry: Entry): Option[T] = entry.synchronized {
    entry.invalidated = true
    val owned = entry.value
    entry.value = None
    // Only an in-flight builder has waiters that need an exception notification.
    if (!entry.ready.isDone) {
      entry.ready.completeExceptionally(removed(id))
    }
    owned
  }

  private def disposeAll(values: Iterable[T]): Unit = {
    var failure: Throwable = null
    values.foreach { value =>
      try {
        dispose(value)
      } catch {
        case error: Throwable =>
          if (failure == null) failure = error else failure.addSuppressed(error)
      }
    }
    if (failure != null) {
      throw failure
    }
  }

  def invalidate(id: Int): Unit = {
    val owned = entries.synchronized {
      if (closed) {
        Seq.empty[T]
      } else {
        // Compact tombstones reject late builders without retaining a future or exception
        // for every ordinary shuffle that never populated this cache.
        retired.add(id)
        entries.remove(id).toSeq.flatMap(_.values).flatMap(retireEntry(id, _))
      }
    }
    disposeAll(owned)
  }

  override def close(): Unit = {
    val owned = entries.synchronized {
      closed = true
      val snapshot = entries.toSeq.flatMap { case (id, generations) =>
        generations.values.flatMap(retireEntry(id, _))
      }
      entries.clear()
      retired.clear()
      snapshot
    }
    disposeAll(owned)
  }
}
