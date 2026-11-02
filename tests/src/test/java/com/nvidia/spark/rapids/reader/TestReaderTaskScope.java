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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.nvidia.spark.rapids.io.async.AsyncRunner;
import com.nvidia.spark.rapids.io.async.AcquireStatus;
import com.nvidia.spark.rapids.io.async.HostMemoryPool;
import com.nvidia.spark.rapids.io.async.ResourceBoundedThreadExecutor$;

import org.apache.spark.TaskContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class TestReaderTaskScope {
  private static final class Value implements AutoCloseable {
    private final AtomicInteger closes = new AtomicInteger();
    private RuntimeException closeError;

    @Override
    public void close() {
      closes.incrementAndGet();
      if (closeError != null) {
        throw closeError;
      }
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(error);
    }
  }

  private static void stop(ExecutorService executor) throws Exception {
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }

  @Test
  public void transferredResultOutlivesScopeAndExecutorRemainsUsable() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      ReaderTaskScope scope = new ReaderTaskScope(executor);
      Value value = new Value();
      ReaderTaskScope.Task<Value> task = scope.submit(0, () -> value);
      ReaderTaskScope.Result<Value> result = task.take();
      assertSame(value, result.data());
      assertThrows(IllegalStateException.class, task::take);
      scope.close();
      assertEquals(0, value.closes.get());
      result.close();
      result.close();
      assertEquals(1, value.closes.get());
      assertThrows(IllegalStateException.class, result::data);
      assertEquals(7, executor.submit(() -> 7).get(5, TimeUnit.SECONDS).intValue());
      assertThrows(IllegalStateException.class, () -> scope.submit(0, Value::new));
    } finally {
      stop(executor);
    }
  }

  @Test
  public void closeWaitsForUninterruptibleOperationAndCancelsQueuedWork() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ExecutorService closer = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    Value value = new Value();
    try {
      ReaderTaskScope scope = new ReaderTaskScope(executor);
      scope.submit(0, () -> {
        started.countDown();
        boolean done = false;
        while (!done) {
          try {
            done = release.await(5, TimeUnit.SECONDS);
            assertTrue(done);
          } catch (InterruptedException ignored) {
            interrupted.countDown();
          }
        }
        return value;
      });
      await(started);
      AtomicInteger queuedCalls = new AtomicInteger();
      scope.submit(0, () -> {
        queuedCalls.incrementAndGet();
        return new Value();
      });
      Future<?> close = closer.submit(scope::close);
      await(interrupted);
      assertFalse(close.isDone());
      assertEquals(0, value.closes.get());
      release.countDown();
      close.get(5, TimeUnit.SECONDS);
      assertEquals(1, value.closes.get());
      assertEquals(0, queuedCalls.get());
    } finally {
      release.countDown();
      stop(closer);
      stop(executor);
    }
  }

  @Test
  public void partialSubmissionFailureStopsPriorWork() throws Exception {
    ExecutorService executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
        new SynchronousQueue<>());
    CountDownLatch started = new CountDownLatch(1);
    Value value = new Value();
    try (ReaderTaskScope scope = new ReaderTaskScope(executor)) {
      scope.submit(0, () -> ReaderResources.closeOnExcept(value, owned -> {
        started.countDown();
        await(new CountDownLatch(1));
        return owned;
      }));
      await(started);
      assertThrows(RejectedExecutionException.class, () -> scope.submit(0, Value::new));
      assertEquals(1, value.closes.get());
      assertFalse(executor.isShutdown());
    } finally {
      stop(executor);
    }
  }

  @Test
  public void workerFailureClosesOtherUnclaimedResults() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Value value = new Value();
    try (ReaderTaskScope scope = new ReaderTaskScope(executor)) {
      ReaderTaskScope.Task<Value> ready = scope.submit(0, () -> value);
      executor.submit(() -> null).get(5, TimeUnit.SECONDS);
      assertTrue(ready.isDone());
      RuntimeException failure = new IllegalStateException("read failure");
      ReaderTaskScope.Task<Value> failed = scope.submit(0, () -> { throw failure; });
      assertSame(failure, assertThrows(IllegalStateException.class, failed::take));
      assertEquals(1, value.closes.get());
    } finally {
      stop(executor);
    }
  }

  @Test
  public void capturedTaskContextIsRestoredOnPooledThread() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    TaskContext context = mock(TaskContext.class);
    try {
      ReaderTaskScope scope = ReaderTaskContext.withContext(context,
          () -> new ReaderTaskScope(executor));
      try (ReaderTaskScope.Result<Value> result = scope.submit(0, () -> {
        assertSame(context, TaskContext.get());
        return new Value();
      }).take()) {
        assertNotNull(result.data());
      }
      scope.close();
      assertNull(executor.submit(TaskContext::get).get(5, TimeUnit.SECONDS));
    } finally {
      stop(executor);
    }
  }

  @Test
  public void workerCannotDeadlockByClosingItsOwnScope() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (ReaderTaskScope scope = new ReaderTaskScope(executor)) {
      ReaderTaskScope.Task<Value> task = scope.submit(0, () -> {
        scope.close();
        return new Value();
      });
      IllegalStateException error = assertThrows(IllegalStateException.class, task::take);
      assertEquals("A reader operation cannot close its own scope", error.getMessage());
    } finally {
      stop(executor);
    }
  }

  @Test
  public void boundedExecutorAlsoAcceptsUnreservedCopyWork() throws Exception {
    ExecutorService executor = ResourceBoundedThreadExecutor$.MODULE$.apply(
        "reader-copy-test", new HostMemoryPool(4), 1, 10);
    try (ReaderTaskScope scope = new ReaderTaskScope(executor)) {
      Value value = new Value();
      try (ReaderTaskScope.Result<Value> result = scope.submit(0, () -> value).take()) {
        assertSame(value, result.data());
      }
      assertEquals(1, value.closes.get());
      assertFalse(executor.isShutdown());
    } finally {
      stop(executor);
    }
  }

  @Test
  public void closeWaitsForResourceAcquisitionBeforeCancellingTheRead() throws Exception {
    CountDownLatch acquiring = new CountDownLatch(1);
    CountDownLatch releaseAcquisition = new CountDownLatch(1);
    CountDownLatch closing = new CountDownLatch(1);
    AtomicInteger releases = new AtomicInteger();
    AtomicInteger reads = new AtomicInteger();
    HostMemoryPool pool = new HostMemoryPool(4) {
      @Override
      public <T> AcquireStatus acquireResource(AsyncRunner<T> runner, long timeout) {
        AcquireStatus status = super.acquireResource(runner, timeout);
        acquiring.countDown();
        await(releaseAcquisition);
        return status;
      }

      @Override
      public <T> void releaseResource(AsyncRunner<T> runner) {
        super.releaseResource(runner);
        releases.incrementAndGet();
      }
    };
    ExecutorService executor = ResourceBoundedThreadExecutor$.MODULE$.apply(
        "reader-acquisition-test", pool, 1, 10);
    ExecutorService closer = Executors.newSingleThreadExecutor();
    try {
      ReaderTaskScope scope = new ReaderTaskScope(executor);
      scope.submit(4, () -> {
        reads.incrementAndGet();
        return new Value();
      });
      await(acquiring);
      Future<?> close = closer.submit(() -> {
        closing.countDown();
        scope.close();
      });
      await(closing);
      assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
      releaseAcquisition.countDown();
      close.get(5, TimeUnit.SECONDS);
      assertEquals(0, reads.get());
      assertEquals(1, releases.get());
      assertFalse(executor.isShutdown());
    } finally {
      releaseAcquisition.countDown();
      stop(closer);
      stop(executor);
    }
  }

  @Test
  public void boundedPoolReservationFollowsResultLifetime() throws Exception {
    AtomicInteger releases = new AtomicInteger();
    HostMemoryPool pool = new HostMemoryPool(4) {
      @Override
      public <T> void releaseResource(AsyncRunner<T> runner) {
        super.releaseResource(runner);
        releases.incrementAndGet();
      }
    };
    ExecutorService executor = ResourceBoundedThreadExecutor$.MODULE$.apply(
        "reader-scope-test", pool, 2, 10);
    try (ReaderTaskScope scope = new ReaderTaskScope(executor)) {
      Value value = new Value();
      value.closeError = new IllegalStateException("data close");
      ReaderTaskScope.Result<Value> result = scope.submit(4, () -> value).take();
      assertEquals(0, releases.get());
      scope.close();
      assertEquals(0, value.closes.get());
      assertSame(value.closeError, assertThrows(IllegalStateException.class, result::close));
      assertEquals(1, releases.get());
      result.close();
      assertEquals(1, releases.get());
      assertEquals(1, value.closes.get());
    } finally {
      stop(executor);
    }
  }
}
