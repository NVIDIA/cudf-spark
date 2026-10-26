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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.TaskCompletionListener;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestUnifiedReader {
  private enum Failure {
    NONE, SCHEDULE, HANDLES_HAS_NEXT, HANDLES_NEXT, TAKE_INPUT,
    COMBINE, DECODE, CALLBACK, BATCH_HAS_NEXT, BATCH_NEXT
  }

  private static final class Input implements CombineInput, CombinedResult {
    private int closes;

    @Override
    public void close() {
      closes++;
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RuntimeException failure = new IllegalStateException("operation");
    private Failure failAt = Failure.NONE;
    private boolean failCleanup;
    private final Input input = new Input();
    private final ColumnarBatch batch = mock(ColumnarBatch.class);
    private final ColumnarBatch secondBatch = mock(ColumnarBatch.class);
    private boolean multipleBatches;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Queue<ReadHandle<Input>> pending = new ArrayDeque<>();
    private int schedules;
    private int callbacks;
    private int schedulerCloses;
    private int iteratorCloses;
    private int filterHandoffs;
    private Input followup;

    Fixture() {
      pending.add(handle(input));
    }

    private void fail(Failure boundary) {
      if (failAt == boundary) {
        throw failure;
      }
    }

    private ReadHandle<Input> handle(Input value) {
      InputReadHandle<Input> owned = new InputReadHandle<>(value);
      return new ReadHandle<Input>() {
        @Override
        public Input takeInput() {
          fail(Failure.TAKE_INPUT);
          return owned.takeInput();
        }

        @Override
        public void close() {
          owned.close();
        }
      };
    }

    private final Scheduler<ReadSource, ReadHandle<Input>> scheduler =
        new Scheduler<ReadSource, ReadHandle<Input>>() {
          @Override
          public Iterator<ReadHandle<Input>> schedule(
              List<ReadSource> sources, ExecutorService service) {
            schedules++;
            assertSame(executor, service);
            fail(Failure.SCHEDULE);
            return new Iterator<ReadHandle<Input>>() {
              @Override
              public boolean hasNext() {
                fail(Failure.HANDLES_HAS_NEXT);
                return !pending.isEmpty();
              }

              @Override
              public ReadHandle<Input> next() {
                fail(Failure.HANDLES_NEXT);
                return pending.remove();
              }
            };
          }

          @Override
          public void onDecode(ReadHandle<Input> handle,
              CloseableIterator<ColumnarBatch> batches) {
            callbacks++;
            handle.close();
            fail(Failure.CALLBACK);
            if (filterHandoffs-- > 0) {
              while (batches.hasNext()) {
                ReaderResources.closeAll(null, batches.next());
              }
              followup = new Input();
              pending.add(handle(followup));
            }
          }

          @Override
          public void close() {
            schedulerCloses++;
            while (!pending.isEmpty()) {
              pending.remove().close();
            }
            if (failCleanup) {
              throw new IllegalStateException("scheduler cleanup");
            }
          }
        };

    private UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader() {
      return new UnifiedReader<>(Collections.emptyList(), scheduler, (value, service) ->
          ReaderResources.closeOnExcept(value, owned -> {
            fail(Failure.COMBINE);
            return owned;
          }), value -> ReaderResources.closeOnExcept(value, owned -> {
            fail(Failure.DECODE);
            return new CloseableIterator<ColumnarBatch>() {
              private final ColumnarBatch[] output = owned != input ?
                  new ColumnarBatch[]{secondBatch} : multipleBatches ?
                      new ColumnarBatch[]{batch, secondBatch} : new ColumnarBatch[]{batch};
              private int emitted;
              private boolean closed;

              @Override
              public boolean hasNext() {
                fail(Failure.BATCH_HAS_NEXT);
                return emitted < output.length && !closed;
              }

              @Override
              public ColumnarBatch next() {
                fail(Failure.BATCH_NEXT);
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                return output[emitted++];
              }

              @Override
              public void close() {
                if (!closed) {
                  closed = true;
                  iteratorCloses++;
                  AutoCloseable[] remaining = new AutoCloseable[output.length - emitted + 1];
                  remaining[0] = owned;
                  System.arraycopy(output, emitted, remaining, 1, output.length - emitted);
                  ReaderResources.closeAll(null, remaining);
                  if (failCleanup) {
                    throw new IllegalStateException("iterator cleanup");
                  }
                }
              }
            };
          }), executor);
    }

    @Override
    public void close() {
      executor.shutdownNow();
    }
  }

  @Test
  public void schedulingIsLazyAndOutputOwnershipBelongsToCaller() {
    try (Fixture fixture = new Fixture();
         UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader = fixture.reader()) {
      assertEquals(0, fixture.schedules);
      assertTrue(reader.hasNext());
      assertTrue(reader.hasNext());
      assertEquals(1, fixture.schedules);
      assertEquals(1, fixture.callbacks);
      assertSame(fixture.batch, reader.next());
      assertFalse(reader.hasNext());
      assertFalse(reader.hasNext());
      assertEquals(1, fixture.input.closes);
      assertEquals(1, fixture.iteratorCloses);
      assertEquals(1, fixture.schedulerCloses);
      verify(fixture.batch, never()).close();
      ReaderResources.closeAll(null, fixture.batch);
      verify(fixture.batch).close();
      assertFalse(fixture.executor.isShutdown());
      assertThrows(NoSuchElementException.class, reader::next);
    }
  }

  @Test
  public void closeBeforeInitializationReleasesUnscheduledWork() {
    try (Fixture fixture = new Fixture()) {
      UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader = fixture.reader();
      reader.close();
      reader.close();
      assertFalse(reader.hasNext());
      assertEquals(0, fixture.schedules);
      assertEquals(1, fixture.input.closes);
      assertEquals(1, fixture.schedulerCloses);
    }
  }

  @Test
  public void earlyCloseReleasesUnconsumedOutput() {
    try (Fixture fixture = new Fixture()) {
      UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader = fixture.reader();
      assertTrue(reader.hasNext());
      reader.close();
      verify(fixture.batch).close();
      assertEquals(1, fixture.input.closes);
      assertEquals(1, fixture.schedulerCloses);
    }
  }

  @Test
  public void allHandoffFailuresCloseTheCorrectOwners() {
    for (Failure failure : Failure.values()) {
      if (failure == Failure.NONE) {
        continue;
      }
      try (Fixture fixture = new Fixture()) {
        fixture.failAt = failure;
        UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader = fixture.reader();
        assertSame(fixture.failure, assertThrows(IllegalStateException.class, reader::next),
            failure.toString());
        reader.close();
        assertEquals(1, fixture.input.closes, failure.toString());
        assertEquals(1, fixture.schedulerCloses, failure.toString());
        assertFalse(fixture.executor.isShutdown());
      }
    }
  }

  @Test
  public void cleanupDoesNotMaskLazyNextFailure() {
    try (Fixture fixture = new Fixture()) {
      fixture.failAt = Failure.BATCH_NEXT;
      fixture.failCleanup = true;
      UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader = fixture.reader();
      assertSame(fixture.failure, assertThrows(IllegalStateException.class, reader::next));
      assertEquals(1, fixture.schedulerCloses);
      assertEquals(1, fixture.input.closes);
      assertEquals(1, fixture.failure.getSuppressed().length);
      Throwable iteratorFailure = fixture.failure.getSuppressed()[0];
      assertEquals("iterator cleanup", iteratorFailure.getMessage());
      assertEquals("scheduler cleanup", iteratorFailure.getSuppressed()[0].getMessage());
    }
  }

  @Test
  public void schedulerCanConsumeFiltersAndProduceAContinuation() {
    try (Fixture fixture = new Fixture();
         UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader = fixture.reader()) {
      fixture.filterHandoffs = 1;
      assertTrue(reader.hasNext());
      assertEquals(2, fixture.callbacks);
      assertEquals(1, fixture.input.closes);
      verify(fixture.batch).close();
      assertSame(fixture.secondBatch, reader.next());
      ReaderResources.closeAll(null, fixture.secondBatch);
      assertFalse(reader.hasNext());
      assertEquals(1, fixture.followup.closes);
      assertEquals(2, fixture.iteratorCloses);
    }
  }

  @Test
  public void multipleBatchesKeepTheDecodeInputAliveUntilExhausted() {
    try (Fixture fixture = new Fixture();
         UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader = fixture.reader()) {
      fixture.multipleBatches = true;
      assertSame(fixture.batch, reader.next());
      ReaderResources.closeAll(null, fixture.batch);
      assertEquals(0, fixture.input.closes);
      assertSame(fixture.secondBatch, reader.next());
      ReaderResources.closeAll(null, fixture.secondBatch);
      assertFalse(reader.hasNext());
      assertEquals(1, fixture.input.closes);
      assertEquals(1, fixture.callbacks);
    }
  }

  @Test
  public void failedTaskListenerRegistrationReleasesScheduler() {
    TaskContext context = mock(TaskContext.class);
    RuntimeException failure = new IllegalStateException("listener registration");
    when(context.addTaskCompletionListener(any(TaskCompletionListener.class)))
        .thenThrow(failure);
    try (Fixture fixture = new Fixture()) {
      assertSame(failure, assertThrows(IllegalStateException.class,
          () -> ReaderTaskContext.withContext(context, fixture::reader)));
      assertEquals(1, fixture.schedulerCloses);
      assertEquals(1, fixture.input.closes);
    }
  }

  @Test
  public void emptyScheduleClosesWithoutDecode() {
    try (Fixture fixture = new Fixture()) {
      fixture.pending.remove().close();
      try (UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader = fixture.reader()) {
        assertFalse(reader.hasNext());
        assertEquals(0, fixture.callbacks);
        assertEquals(1, fixture.schedulerCloses);
      }
    }
  }

  @Test
  public void taskCompletionClosesReaderWithoutClosingReturnedBatch() {
    TaskContext context = mock(TaskContext.class);
    AtomicReference<TaskCompletionListener> completion = new AtomicReference<>();
    when(context.addTaskCompletionListener(any(TaskCompletionListener.class)))
        .thenAnswer(invocation -> {
          completion.set(invocation.getArgument(0));
          return context;
        });
    try (Fixture fixture = new Fixture()) {
      UnifiedReader<ReadSource, Input, ReadHandle<Input>, Input> reader =
          ReaderTaskContext.withContext(context, fixture::reader);
      assertSame(fixture.batch, reader.next());
      completion.get().onTaskCompletion(context);
      assertFalse(reader.hasNext());
      assertEquals(1, fixture.schedulerCloses);
      verify(fixture.batch, never()).close();
      ReaderResources.closeAll(null, fixture.batch);
    }
  }
}
