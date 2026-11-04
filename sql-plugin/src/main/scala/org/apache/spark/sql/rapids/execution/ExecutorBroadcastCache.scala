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

import org.apache.spark.TaskKilledException

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
    var borrowers = 0
  }

  // Nested locking always acquires entries before an Entry. Never acquire entries while
  // holding an Entry lock; GPU materialization and disposal run outside both locks.
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

  /**
   * `beforeWait` releases task resources; `checkCancelled` throws for a cancelled task.
   * `releaseBorrowed` releases a borrowed result if retirement fails before it can be returned.
   */
  def getOrBuild[B](
      id: Int,
      build: () => T,
      beforeWait: () => Unit,
      checkCancelled: () => Unit,
      beforeBorrow: () => Unit = () => (),
      generation: Long = 0L)(borrow: T => B, releaseBorrowed: B => Unit): B = {
    var result: Option[B] = None
    while (result.isEmpty) {
      checkCancelled()
      var builder = false
      val entry = entries.synchronized {
        require(!closed, "Executor broadcast cache is closed")
        if (retired.contains(id)) throw removed(id)
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
            val rejection = removed(id)
            try dispose(value) catch {
              case cleanup: Throwable => suppress(rejection, cleanup)
            }
            throw rejection
          }
        } catch {
          case error: Throwable =>
            // Detach the failed attempt before waking waiters that may elect a new builder.
            entries.synchronized {
              if (!entry.invalidated) {
                entries.get(id).foreach { generations =>
                  if (generations.get(generation).contains(entry)) {
                    generations.remove(generation)
                    if (generations.isEmpty) entries.remove(id)
                  }
                }
              }
            }
            entry.ready.completeExceptionally(error)
            throw error
        }
      } else if (!entry.ready.isDone) {
        beforeWait()
      }

      var ready = false
      var retryBuilder = false
      while (!ready) {
        checkCancelled()
        try {
          entry.ready.get(100, TimeUnit.MILLISECONDS)
          ready = true
        } catch {
          case _: TimeoutException =>
          case failure: ExecutionException =>
            failure.getCause match {
              case _: TaskKilledException | _: InterruptedException =>
                // A peer task's cancellation does not cancel this task. Invalidation and
                // shutdown are checked when re-entering the cache before another build.
                checkCancelled()
                retryBuilder = true
                ready = true
              case other => throw other
            }
        }
      }
      if (!retryBuilder) {
        checkCancelled()
        beforeBorrow()
        val value = entry.synchronized {
          if (entry.invalidated) throw removed(id)
          entry.borrowers += 1
          entry.value.get
        }
        var failure: Throwable = null
        try {
          // Materialization may allocate or copy from host. A lease keeps the owner alive
          // without holding the entry lock across GPU work, retry, or synchronization.
          result = Some(borrow(value))
        } catch {
          case error: Throwable =>
            failure = error
            throw error
        } finally {
          val released = entry.synchronized {
            entry.borrowers -= 1
            if (entry.invalidated && entry.borrowers == 0) {
              val owned = entry.value
              entry.value = None
              owned
            } else None
          }
          try {
            released.foreach(dispose)
          } catch {
            case cleanup: Throwable =>
              if (failure != null) {
                suppress(failure, cleanup)
              } else {
                // Ownership has not reached the caller when retirement fails.
                result.foreach { borrowed =>
                  try releaseBorrowed(borrowed) catch {
                    case secondary: Throwable => suppress(cleanup, secondary)
                  }
                }
                throw cleanup
              }
          }
        }
      }
    }
    result.get
  }

  private def suppress(primary: Throwable, secondary: Throwable): Unit = {
    if (primary ne secondary) primary.addSuppressed(secondary)
    if (secondary.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
  }

  private def retireEntry(id: Int, entry: Entry): Option[T] = entry.synchronized {
    entry.invalidated = true
    val owned = if (entry.borrowers == 0) {
      val value = entry.value
      entry.value = None
      value
    } else None
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
          if (failure == null) failure = error else suppress(failure, error)
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
