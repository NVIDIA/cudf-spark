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

/**
 * Owns one build value per shuffle in an executor. A borrower must retain its own reference
 * before returning from `borrow`. Invalidated IDs remain until this cache's application ends,
 * preventing a late consumer from recreating a build after shuffle cleanup.
 */
private[rapids] class ExecutorBroadcastCache[T](dispose: T => Unit) extends AutoCloseable {
  private class Entry {
    val ready = new CompletableFuture[Unit]()
    var value: Option[T] = None
    var invalidated = false
  }

  private val entries = mutable.Map.empty[Int, Entry]
  private var closed = false

  private def removed(id: Int): IllegalStateException =
    new IllegalStateException(s"Broadcast build shuffle $id has been unregistered")

  /** `beforeWait` releases task resources; `checkCancelled` must throw for a cancelled task. */
  def getOrBuild[B](
      id: Int,
      build: () => T,
      beforeWait: () => Unit,
      checkCancelled: () => Unit,
      beforeBorrow: () => Unit = () => ())(borrow: T => B): B = {
    checkCancelled()
    var builder = false
    val entry = entries.synchronized {
      require(!closed, "Executor broadcast cache is closed")
      entries.getOrElseUpdate(id, {
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
              if (!entry.invalidated && entries.get(id).contains(entry)) {
                entries.remove(id)
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

  def invalidate(id: Int): Unit = {
    val entry = entries.synchronized {
      if (closed) None else Some(entries.getOrElseUpdate(id, new Entry))
    }
    entry.foreach { value =>
      val owned = value.synchronized {
        value.invalidated = true
        val owned = value.value
        value.value = None
        value.ready.completeExceptionally(removed(id))
        owned
      }
      owned.foreach(dispose)
    }
  }

  override def close(): Unit = {
    val owned = entries.synchronized {
      closed = true
      val snapshot = entries.toSeq
      entries.clear()
      snapshot
    }
    var failure: Throwable = null
    owned.foreach { case (id, entry) =>
      val value = entry.synchronized {
        entry.invalidated = true
        val value = entry.value
        entry.value = None
        entry.ready.completeExceptionally(removed(id))
        value
      }
      value.foreach { v =>
        try {
          dispose(v)
        } catch {
          case error: Throwable =>
            if (failure == null) failure = error else failure.addSuppressed(error)
        }
      }
    }
    if (failure != null) {
      throw failure
    }
  }
}
