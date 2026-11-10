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

import org.apache.spark.{FetchFailed, TaskContext, TaskKilledException}
import org.apache.spark.shuffle.FetchFailedException

/**
 * Owns one build per source-output identity in an executor. Consumers retain independent
 * references. Invalidation detaches existing entries; a later reader may rebuild the shuffle.
 */
private[rapids] class ExecutorBroadcastCache[T](dispose: T => Unit) extends AutoCloseable {
  private class BuildFailure(val original: Throwable, val fetch: Option[FetchFailed])
      extends Exception(original)

  private class Entry {
    val ready = new CompletableFuture[Unit]()
    var value: Option[T] = None
    var invalidated = false
    var borrowers = 0
  }

  // Nested locking always acquires entries before an Entry. Never acquire entries while
  // holding an Entry lock; GPU materialization and disposal run outside both locks.
  private val entries = mutable.Map.empty[Int, mutable.Map[BroadcastBuildIdentity, Entry]]

  private[execution] def activeEntryCount: Int = entries.synchronized {
    entries.valuesIterator.map(_.size).sum
  }
  private var closed = false

  private def removed(id: Int): IllegalStateException =
    new IllegalStateException(s"Broadcast build shuffle $id cache entry was invalidated")

  /**
   * `beforeWait` releases task resources; `checkCancelled` throws for a cancelled task.
   * `releaseBorrowed` releases a borrowed result if invalidation fails before it can be returned.
   */
  def getOrBuild[B](
      id: Int,
      build: () => T,
      beforeWait: () => Unit,
      checkCancelled: () => Unit,
      beforeBorrow: () => Unit = () => (),
      identity: BroadcastBuildIdentity = BroadcastBuildIdentity.empty)(
      borrow: T => B, releaseBorrowed: B => Unit): B = {
    var result: Option[B] = None
    while (result.isEmpty) {
      checkCancelled()
      var builder = false
      val entry = entries.synchronized {
        require(!closed, "Executor broadcast cache is closed")
        entries.getOrElseUpdate(id, mutable.Map.empty).getOrElseUpdate(identity, {
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
                entries.get(id).foreach { identities =>
                  if (identities.get(identity).contains(entry)) {
                    identities.remove(identity)
                    if (identities.isEmpty) entries.remove(id)
                  }
                }
              }
            }
            val fetch = Option(TaskContext.get()).flatMap(_.fetchFailed).orElse(error match {
              case failed: FetchFailedException => Some(failed)
              case _ => None
            }).map(_.toTaskFailedReason.asInstanceOf[FetchFailed])
            entry.ready.completeExceptionally(new BuildFailure(error, fetch))
            throw error
        }
      } else if (!entry.ready.isDone) {
        beforeWait()
      }

      val retryBuilder = awaitReady(entry, checkCancelled)
      if (!retryBuilder) {
        checkCancelled()
        beforeBorrow()
        val value = entry.synchronized {
          if (entry.invalidated) throw removed(id)
          entry.borrowers += 1
          entry.value.get
        }
        result = Some(borrowEntry(entry, value, borrow, releaseBorrowed))
      }
    }
    result.get
  }

  /** A cancelled peer permits this task to compete for a fresh cache entry. */
  private def awaitReady(entry: Entry, checkCancelled: () => Unit): Boolean = {
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
            case failed: BuildFailure => failed.original match {
              case _: TaskKilledException | _: InterruptedException =>
                // A peer task's cancellation does not cancel this task.
                checkCancelled()
                retryBuilder = true
                ready = true
              case original => failed.fetch match {
                case Some(fetch) =>
                  // Construction marks this waiter's TaskContext, including wrapped failures.
                  throw new FetchFailedException(fetch.bmAddress, fetch.shuffleId, fetch.mapId,
                    fetch.mapIndex, fetch.reduceId, original.getMessage, original)
                case None => throw original
              }
            }
            case other => throw other
          }
      }
    }
    retryBuilder
  }

  private def borrowEntry[B](entry: Entry, value: T, borrow: T => B,
      releaseBorrowed: B => Unit): B = {
    var result: Option[B] = None
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
            // Ownership has not reached the caller when invalidation fails.
            result.foreach { borrowed =>
              try releaseBorrowed(borrowed) catch {
                case secondary: Throwable => suppress(cleanup, secondary)
              }
            }
            throw cleanup
          }
      }
    }
    result.get
  }

  private def suppress(primary: Throwable, secondary: Throwable): Unit = {
    if (primary ne secondary) primary.addSuppressed(secondary)
    if (secondary.isInstanceOf[InterruptedException]) Thread.currentThread().interrupt()
  }

  private def invalidateEntry(id: Int, entry: Entry): Option[T] = entry.synchronized {
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
        entries.remove(id).toSeq.flatMap(_.values).flatMap(invalidateEntry(id, _))
      }
    }
    disposeAll(owned)
  }

  override def close(): Unit = {
    val owned = entries.synchronized {
      closed = true
      val snapshot = entries.toSeq.flatMap { case (id, identities) =>
        identities.values.flatMap(invalidateEntry(id, _))
      }
      entries.clear()
      snapshot
    }
    disposeAll(owned)
  }
}
