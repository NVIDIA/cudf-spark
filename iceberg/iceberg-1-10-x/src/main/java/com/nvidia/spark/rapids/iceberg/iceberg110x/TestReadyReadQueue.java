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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.nvidia.spark.rapids.io.async.AsyncRunner;
import com.nvidia.spark.rapids.io.async.HostMemoryPool;
import com.nvidia.spark.rapids.io.async.ResourceBoundedThreadExecutor$;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestReadyReadQueue {
  private static final class Input implements CombineInput {
    private int closes;

    @Override
    public void close() {
      closes++;
    }
  }

  private static InputReadHandle<Input> handle() {
    return new InputReadHandle<>(new Input());
  }

  private static void stop(ExecutorService executor) throws Exception {
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }

  @Test
  public void admissionIncludesReadyAndYieldedReadsUntilCompletion() {
    try (ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(2)) {
      ReadyReadQueue<InputReadHandle<Input>>.Admission first = queue.tryAdmit().get();
      ReadyReadQueue<InputReadHandle<Input>>.Admission second = queue.tryAdmit().get();
      assertFalse(queue.tryAdmit().isPresent());
      InputReadHandle<Input> a = handle();
      InputReadHandle<Input> b = handle();
      second.publish(b);
      first.publish(a);
      assertSame(b, queue.next());
      assertFalse(queue.tryAdmit().isPresent());
      second.close();
      b.close();
      ReadyReadQueue<InputReadHandle<Input>>.Admission replacement = queue.tryAdmit().get();
      replacement.close();
      assertSame(a, queue.next());
      first.close();
      a.close();
      queue.finishAdmission();
      assertFalse(queue.hasNext());
    }
  }

  @Test
  public void continuationKeepsIteratorAliveAfterInitialAdmissionEnds() throws Exception {
    ExecutorService consumer = Executors.newSingleThreadExecutor();
    try (ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(1)) {
      ReadyReadQueue<InputReadHandle<Input>>.Admission read = queue.tryAdmit().get();
      queue.finishAdmission();
      assertThrows(IllegalStateException.class, queue::tryAdmit);
      InputReadHandle<Input> filter = handle();
      read.publish(filter);
      assertSame(filter, queue.next());
      filter.close();
      CountDownLatch entered = new CountDownLatch(1);
      Future<Boolean> waiting = consumer.submit(() -> {
        entered.countDown();
        return queue.hasNext();
      });
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertFalse(waiting.isDone());
      InputReadHandle<Input> complete = handle();
      assertTrue(read.publish(complete));
      assertTrue(waiting.get(5, TimeUnit.SECONDS));
      assertSame(complete, queue.next());
      read.close();
      complete.close();
      assertFalse(queue.hasNext());
    } finally {
      stop(consumer);
    }
  }

  @Test
  public void closeWakesWaitersAndRejectsLateResultsWithoutLeaking() throws Exception {
    ExecutorService consumer = Executors.newSingleThreadExecutor();
    try {
      ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(1);
      ReadyReadQueue<InputReadHandle<Input>>.Admission read = queue.tryAdmit().get();
      CountDownLatch entered = new CountDownLatch(1);
      Future<Boolean> waiting = consumer.submit(() -> {
        entered.countDown();
        return queue.hasNext();
      });
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      queue.close();
      assertFalse(waiting.get(5, TimeUnit.SECONDS));
      Input input = new Input();
      assertFalse(read.publish(new InputReadHandle<>(input)));
      assertEquals(1, input.closes);
      assertFalse(queue.tryAdmit().isPresent());
      read.close();
      queue.close();
    } finally {
      stop(consumer);
    }
  }

  @Test
  public void closeReleasesQueuedHandlesButNeverYieldedInputs() {
    ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(2);
    Input queued = new Input();
    Input yielded = new Input();
    queue.tryAdmit().get().publish(new InputReadHandle<>(yielded));
    queue.tryAdmit().get().publish(new InputReadHandle<>(queued));
    InputReadHandle<Input> transferred = queue.next();
    queue.close();
    assertEquals(1, queued.closes);
    assertEquals(0, yielded.closes);
    transferred.close();
    assertEquals(1, yielded.closes);
  }

  @Test
  public void failedReadClosesReadyWorkAndPreservesFailure() {
    ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(1);
    Input input = new Input();
    queue.tryAdmit().get().publish(new InputReadHandle<>(input));
    RuntimeException failure = new IllegalStateException("body IO");
    queue.fail(failure);
    assertEquals(1, input.closes);
    assertSame(failure, assertThrows(IllegalStateException.class, queue::hasNext));
    queue.close();
    assertEquals(1, input.closes);
  }

  @Test
  public void failureUnblocksAnEmptyQueue() throws Exception {
    ExecutorService consumer = Executors.newSingleThreadExecutor();
    try (ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(1)) {
      queue.tryAdmit();
      Future<Boolean> waiting = consumer.submit(queue::hasNext);
      RuntimeException failure = new IllegalStateException("metadata IO");
      queue.fail(failure);
      assertSame(failure, assertThrows(ExecutionException.class,
          () -> waiting.get(5, TimeUnit.SECONDS)).getCause());
    } finally {
      stop(consumer);
    }
  }

  @Test
  public void rejectedExtraHandoffDoesNotDestroyExistingReadyInput() {
    try (ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(1)) {
      ReadyReadQueue<InputReadHandle<Input>>.Admission read = queue.tryAdmit().get();
      InputReadHandle<Input> first = handle();
      Input rejected = new Input();
      read.publish(first);
      assertThrows(IllegalStateException.class, () -> read.publish(new InputReadHandle<>(rejected)));
      assertEquals(1, rejected.closes);
      assertThrows(IllegalStateException.class, () -> read.publish(first));
      assertSame(first, queue.next());
      first.close();
      read.close();
    }
  }

  @Test
  public void completedAdmissionCleansItsReadyHandleAndPermitsAnotherRead() {
    try (ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(1)) {
      ReadyReadQueue<InputReadHandle<Input>>.Admission read = queue.tryAdmit().get();
      Input input = new Input();
      read.publish(new InputReadHandle<>(input));
      read.close();
      read.close();
      assertEquals(1, input.closes);
      queue.tryAdmit().get().close();
      queue.finishAdmission();
      assertFalse(queue.hasNext());
    }
  }

  @Test
  public void interruptedWaitStopsAdmissionAndPreservesInterruptStatus() throws Exception {
    try (ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(1)) {
      queue.tryAdmit();
      CountDownLatch entered = new CountDownLatch(1);
      AtomicReference<Throwable> observed = new AtomicReference<>();
      AtomicReference<Boolean> interrupted = new AtomicReference<>(false);
      Thread consumer = new Thread(() -> {
        entered.countDown();
        try {
          queue.hasNext();
        } catch (RuntimeException error) {
          observed.set(error.getCause());
          interrupted.set(Thread.currentThread().isInterrupted());
        }
      });
      consumer.start();
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      consumer.interrupt();
      consumer.join(5000);
      assertFalse(consumer.isAlive());
      assertTrue(observed.get() instanceof InterruptedException);
      assertTrue(interrupted.get());
      assertFalse(queue.tryAdmit().isPresent());
    }
  }

  @Test
  public void emptyInputAndInvalidLimit() {
    assertThrows(IllegalArgumentException.class, () -> new ReadyReadQueue<>(0));
    try (ReadyReadQueue<InputReadHandle<Input>> queue = new ReadyReadQueue<>(1)) {
      queue.finishAdmission();
      assertFalse(queue.hasNext());
      assertThrows(java.util.NoSuchElementException.class, queue::next);
    }
  }

  @Test
  public void oversizedInputUsesExistingPoolAndReservationSurvivesHandoff() throws Exception {
    AtomicInteger releases = new AtomicInteger();
    HostMemoryPool pool = new HostMemoryPool(4) {
      @Override
      public <T> void releaseResource(AsyncRunner<T> runner) {
        super.releaseResource(runner);
        releases.incrementAndGet();
      }
    };
    ExecutorService executor = ResourceBoundedThreadExecutor$.MODULE$.apply(
        "ready-queue-test", pool, 1, 10);
    try (ReaderTaskScope scope = new ReaderTaskScope(executor);
         ReadyReadQueue<InputReadHandle<CombineInput>> queue = new ReadyReadQueue<>(1)) {
      ReadyReadQueue<InputReadHandle<CombineInput>>.Admission read = queue.tryAdmit().get();
      Input input = new Input();
      ReaderTaskScope.Result<Input> result = scope.submit(8, () -> input).take();
      CombineInput owned = result::close;
      read.publish(new InputReadHandle<>(owned));
      queue.finishAdmission();
      InputReadHandle<CombineInput> handle = queue.next();
      CombineInput transferred = handle.takeInput();
      handle.close();
      read.close();
      assertFalse(queue.hasNext());
      assertEquals(0, input.closes);
      assertEquals(0, releases.get());
      transferred.close();
      assertEquals(1, input.closes);
      assertEquals(1, releases.get());
    } finally {
      stop(executor);
    }
  }
}
