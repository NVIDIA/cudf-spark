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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.TaskCompletionListener;

/**
 * Format-neutral handoff and cleanup loop. Iterator operations and close run on the
 * Spark task thread; asynchronous work and cancellation belong to the scheduler.
 * The supplied executor is shared and is never shut down by this reader.
 */
public final class UnifiedReader<S extends ReadSource, I extends CombineInput,
    H extends ReadHandle<I>, C extends CombinedResult>
    implements CloseableIterator<ColumnarBatch> {
  private final List<S> sources;
  private final Scheduler<S, H> scheduler;
  private final Combiner<I, C> combiner;
  private final Decoder<C> decoder;
  private final ExecutorService executor;
  private boolean initialized;
  private boolean closed;
  private Iterator<H> handles;
  private H currentHandle;
  private CloseableIterator<ColumnarBatch> currentBatches;

  public UnifiedReader(List<S> sources, Scheduler<S, H> scheduler,
      Combiner<I, C> combiner, Decoder<C> decoder, ExecutorService executor) {
    this.sources = Collections.unmodifiableList(new ArrayList<>(sources));
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.combiner = Objects.requireNonNull(combiner, "combiner");
    this.decoder = Objects.requireNonNull(decoder, "decoder");
    this.executor = Objects.requireNonNull(executor, "executor");
    TaskContext context = TaskContext.get();
    if (context != null) {
      try {
        context.addTaskCompletionListener((TaskCompletionListener) ignored -> close());
      } catch (Throwable error) {
        ReaderResources.closeAll(error, this);
        throw ReaderResources.propagate(error);
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    try {
      if (!initialized) {
        initialized = true;
        handles = Objects.requireNonNull(scheduler.schedule(sources, executor), "handles");
      }
      while (!closed) {
        if (currentBatches != null) {
          if (currentBatches.hasNext()) {
            return true;
          }
          CloseableIterator<ColumnarBatch> exhausted = currentBatches;
          currentBatches = null;
          exhausted.close();
        }
        if (!handles.hasNext()) {
          close();
          return false;
        }
        currentHandle = handles.next();
        I input = currentHandle.takeInput();
        C combined = combiner.combine(input, executor);
        currentBatches = Objects.requireNonNull(decoder.decode(combined), "decoded batches");
        H decodedHandle = currentHandle;
        currentHandle = null;
        // The callback owns the handle from entry, even if it throws.
        scheduler.onDecode(decodedHandle, currentBatches);
      }
      return false;
    } catch (Throwable error) {
      ReaderResources.closeAll(error, this);
      throw ReaderResources.propagate(error);
    }
  }

  @Override
  public ColumnarBatch next() {
    try {
      if (!hasNext()) {
        throw new NoSuchElementException("Unified reader is exhausted");
      }
      return currentBatches.next();
    } catch (Throwable error) {
      ReaderResources.closeAll(error, this);
      throw ReaderResources.propagate(error);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      CloseableIterator<ColumnarBatch> batches = currentBatches;
      H handle = currentHandle;
      currentBatches = null;
      currentHandle = null;
      ReaderResources.closeAll(null, batches, handle, scheduler);
    }
  }
}
