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

package com.nvidia.spark.rapids.reader;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import scala.Option;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import com.nvidia.spark.rapids.io.async.AsyncMetrics;
import com.nvidia.spark.rapids.io.async.AsyncResult;
import com.nvidia.spark.rapids.io.async.AsyncRunner;
import com.nvidia.spark.rapids.io.async.Cancelled$;
import com.nvidia.spark.rapids.io.async.DecayReleaseResult;
import com.nvidia.spark.rapids.io.async.Init;
import com.nvidia.spark.rapids.io.async.MemoryBoundedAsyncRunner;
import com.nvidia.spark.rapids.io.async.Pending$;
import com.nvidia.spark.rapids.io.async.Running$;
import com.nvidia.spark.rapids.io.async.UnboundedAsyncRunner;

import org.apache.spark.TaskContext;

/**
 * Owns submitted operations and unclaimed results, but borrows the shared executor.
 * Closing interrupts running operations and waits for them, even if they ignore interruption.
 * Operations must close their intermediate allocations on failure and must not close this scope.
 */
public final class ReaderTaskScope implements AutoCloseable {
  private final ExecutorService executor;
  private final TaskContext context;
  private final Set<Task<?>> tasks = new LinkedHashSet<>();
  private boolean closed;

  public ReaderTaskScope(ExecutorService executor) {
    this.executor = Objects.requireNonNull(executor, "executor");
    this.context = TaskContext.get();
  }

  /**
   * Submit an operation whose output retains the supplied host-memory reservation.
   * Use zero bytes for work on already-reserved buffers, such as combiner copies.
   * A rejected submission closes the scope so no earlier operation can outlive its caller.
   */
  public <T extends AutoCloseable> Task<T> submit(long hostBytes, Supplier<T> operation) {
    if (hostBytes < 0) {
      throw new IllegalArgumentException("Host memory must be non-negative");
    }
    Objects.requireNonNull(operation, "operation");
    try {
      synchronized (this) {
        if (closed) {
          throw new IllegalStateException("Reader task scope is closed");
        }
        Task<T> task = new Task<>(hostBytes, operation);
        task.future = executor.submit(task.runner);
        tasks.add(task);
        return task;
      }
    } catch (Throwable error) {
      ReaderResources.closeAll(error, this);
      throw ReaderResources.propagate(error);
    }
  }

  @Override
  public void close() {
    List<Task<?>> remaining;
    synchronized (this) {
      if (closed) {
        return;
      }
      for (Task<?> task : tasks) {
        if (task.isWorkerThread()) {
          throw new IllegalStateException("A reader operation cannot close its own scope");
        }
      }
      closed = true;
      remaining = new ArrayList<>(tasks);
      tasks.clear();
    }
    // Signal every operation before waiting for any of them.
    for (Task<?> task : remaining) {
      task.cancel();
    }
    ReaderResources.closeAll(null, remaining.toArray(new AutoCloseable[0]));
  }

  /** One operation; taking its result transfers both data and reservation out of the scope. */
  public final class Task<T extends AutoCloseable> implements AutoCloseable {
    private final Supplier<T> operation;
    private final AsyncRunner<T> runner;
    private Future<AsyncResult<T>> future;
    private Thread worker;
    private boolean cancelled;
    private boolean claimed;
    private T produced;

    private Task(long hostBytes, Supplier<T> operation) {
      this.operation = operation;
      if (hostBytes == 0) {
        runner = new UnboundedAsyncRunner<T>() {
          @Override
          public Option<TaskContext> sparkTaskContext() {
            return Option.apply(context);
          }

          @Override
          public T callImpl() {
            return runOperation();
          }
        };
      } else {
        runner = new MemoryBoundedAsyncRunner<T>() {
          @Override
          public long requiredMemoryBytes() {
            return hostBytes;
          }

          @Override
          public Option<TaskContext> sparkTaskContext() {
            return Option.apply(context);
          }

          @Override
          public T callImpl() {
            return runOperation();
          }

          @Override
          public AsyncResult<T> buildResult(T data, AsyncMetrics metrics) {
            return new DecayReleaseResult<>(data, metrics, new AbstractFunction0<BoxedUnit>() {
              @Override
              public BoxedUnit apply() {
                return withStateLock(new AbstractFunction1<AsyncRunner<T>, BoxedUnit>() {
                  @Override
                  public BoxedUnit apply(AsyncRunner<T> value) {
                    if (value.isHoldingResource()) {
                      value.releaseResourceCallback().apply();
                    }
                    return BoxedUnit.UNIT;
                  }
                });
              }
            });
          }
        };
      }
    }

    private T runOperation() {
      synchronized (this) {
        if (cancelled) {
          throw new CancellationException("Reader operation cancelled before start");
        }
        worker = Thread.currentThread();
      }
      try {
        T result = ReaderTaskContext.withContext(context, operation);
        synchronized (this) {
          produced = Objects.requireNonNull(result, "operation result");
        }
        return result;
      } finally {
        synchronized (this) {
          worker = null;
        }
      }
    }

    private synchronized boolean isWorkerThread() {
      return worker == Thread.currentThread();
    }

    /** Non-blocking cancellation request. Close subsequently waits for actual operation exit. */
    public synchronized void cancel() {
      if (claimed) {
        return;
      }
      cancelled = true;
      if (worker != null) {
        // Never cancel the Future: resource acquisition may already be running even
        // before callImpl starts, and get() must wait for actual completion.
        worker.interrupt();
      }
    }

    public boolean isDone() {
      return future.isDone();
    }

    /** Transfer the result exactly once, including its deferred resource-release callback. */
    public Result<T> take() {
      try {
        AsyncResult<T> result = future.get();
        synchronized (this) {
          if (cancelled || claimed) {
            throw new IllegalStateException("Reader result is no longer available");
          }
          claimed = true;
          produced = null;
        }
        synchronized (ReaderTaskScope.this) {
          tasks.remove(this);
        }
        return new Result<>(result.data(), result);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        ReaderResources.closeAll(error, ReaderTaskScope.this);
        throw ReaderResources.propagate(error);
      } catch (ExecutionException error) {
        Throwable cause = error.getCause();
        ReaderResources.closeAll(cause, ReaderTaskScope.this);
        throw ReaderResources.propagate(cause);
      }
    }

    @Override
    public void close() {
      if (isWorkerThread()) {
        throw new IllegalStateException("A reader operation cannot wait for itself");
      }
      cancel();
      runner.withStateLock(new AbstractFunction1<AsyncRunner<T>, BoxedUnit>() {
        @Override
        public BoxedUnit apply(AsyncRunner<T> value) {
          // Prevent queued/requeued reads from acquiring memory. The bounded executor
          // holds this lock during acquisition and execution, so this also joins them.
          if (value.getState() instanceof Init || value.getState() == Pending$.MODULE$ ||
              value.getState() == Running$.MODULE$) {
            value.setState(Cancelled$.MODULE$);
          }
          return BoxedUnit.UNIT;
        }
      });
      AsyncResult<T> result = null;
      boolean interrupted = false;
      try {
        for (;;) {
          try {
            result = future.get();
            break;
          } catch (InterruptedException error) {
            interrupted = true;
          } catch (CancellationException | ExecutionException finished) {
            break;
          }
        }
        T data;
        synchronized (this) {
          if (claimed) {
            return;
          }
          claimed = true;
          data = produced;
          produced = null;
        }
        // produced also covers an allocation followed by a failing runner post-hook.
        ReaderResources.closeAll(null, data, result, () -> runner.withStateLock(
            new AbstractFunction1<AsyncRunner<T>, BoxedUnit>() {
              @Override
              public BoxedUnit apply(AsyncRunner<T> value) {
                // A failed runner has no result carrying its reservation. Do not rely
                // on afterExecute racing to release it after Future.get has returned.
                if (value.isHoldingResource()) {
                  value.releaseResourceCallback().apply();
                }
                return BoxedUnit.UNIT;
              }
            }));
      } finally {
        synchronized (ReaderTaskScope.this) {
          tasks.remove(this);
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /** Owns the result and any retained pool reservation; data() only borrows its value. */
  public static final class Result<T extends AutoCloseable> implements AutoCloseable {
    private T data;
    private AsyncResult<T> reservation;

    private Result(T data, AsyncResult<T> reservation) {
      this.data = data;
      this.reservation = reservation;
    }

    public synchronized T data() {
      if (data == null) {
        throw new IllegalStateException("Reader result is closed");
      }
      return data;
    }

    @Override
    public void close() {
      T value;
      AsyncResult<T> release;
      synchronized (this) {
        value = data;
        release = reservation;
        data = null;
        reservation = null;
      }
      ReaderResources.closeAll(null, value, release);
    }
  }
}
