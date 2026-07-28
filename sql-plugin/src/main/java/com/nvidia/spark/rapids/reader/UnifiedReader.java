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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Format-neutral asynchronous reader coordinator.
 *
 * <p>The reader constructs a footer-to-data future chain for every source and immediately
 * registers both futures with an injected event-driven planner. The planner owns planning and
 * combination. Consequently, the Spark task thread only waits for a combined decoder input and
 * iterates the resulting batches.</p>
 */
public final class UnifiedReader<
    S extends ReadSource,
    F extends ReadFooter,
    D extends ReadData,
    C extends CombinedResult>
    implements Iterator<ColumnarBatch>, AutoCloseable {

  private final List<S> sources;
  private final ReadOps<S, F, D> ops;
  private final ReadPlanner<S, F, D, C> planner;
  private final Decoder<C> decoder;
  private final ExecutorService executor;

  private boolean closed;
  private boolean initialized;
  private Iterator<ColumnarBatch> currentBatches;
  private C currentInput;

  public UnifiedReader(
      List<S> sources,
      ReadOps<S, F, D> ops,
      ReadPlanner<S, F, D, C> planner,
      Decoder<C> decoder,
      ExecutorService executor) {
    this.sources = Objects.requireNonNull(sources, "sources");
    this.ops = Objects.requireNonNull(ops, "ops");
    this.planner = Objects.requireNonNull(planner, "planner");
    this.decoder = Objects.requireNonNull(decoder, "decoder");
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    try {
      initializeIfNeeded();
      while (!closed) {
        if (currentBatches != null && currentBatches.hasNext()) {
          return true;
        }
        closeCurrent();

        Optional<C> nextInput = await(planner.nextReady());
        if (!nextInput.isPresent()) {
          close();
          return false;
        }

        C input = nextInput.get();
        Iterator<ColumnarBatch> batches = null;
        boolean installed = false;
        try {
          batches = Objects.requireNonNull(decoder.decode(input),
              "decoder returned null");
          currentInput = input;
          currentBatches = batches;
          installed = true;
        } finally {
          if (!installed) {
            closeIterator(batches);
            input.close();
          }
        }
      }
      return false;
    } catch (Throwable error) {
      closeAfterFailure(error);
      throw propagate(error);
    }
  }

  @Override
  public ColumnarBatch next() {
    try {
      if (!hasNext()) {
        throw new NoSuchElementException("no more unified-reader batches");
      }
      return currentBatches.next();
    } catch (Throwable error) {
      closeAfterFailure(error);
      throw propagate(error);
    }
  }

  private void initializeIfNeeded() {
    if (initialized) {
      return;
    }
    initialized = true;
    for (int fileId = 0; fileId < sources.size(); fileId++) {
      S source = sources.get(fileId);
      CompletableFuture<F> footer = ops.readFooter(source, executor);
      CompletableFuture<D> data = footer.thenCompose(
          readyFooter -> ops.readData(source, readyFooter, executor));
      planner.addFile(fileId, source, footer, data);
    }
    planner.noMoreFiles();
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    try {
      return future.get();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    } catch (ExecutionException execution) {
      Throwable cause = unwrap(execution);
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  private void closeAfterFailure(Throwable original) {
    try {
      close();
    } catch (Throwable closeError) {
      if (closeError != original) {
        original.addSuppressed(closeError);
      }
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    Throwable failure = null;
    try {
      closeCurrent();
    } catch (Throwable error) {
      failure = error;
    }
    try {
      planner.close();
    } catch (Throwable error) {
      if (failure == null) {
        failure = error;
      } else if (failure != error) {
        failure.addSuppressed(error);
      }
    }
    if (failure != null) {
      throw propagate(failure);
    }
  }

  private void closeCurrent() throws Exception {
    Iterator<ColumnarBatch> batches = currentBatches;
    C input = currentInput;
    currentBatches = null;
    currentInput = null;
    Throwable failure = null;
    try {
      closeIterator(batches);
    } catch (Throwable error) {
      failure = error;
    }
    if (input != null) {
      try {
        input.close();
      } catch (Throwable error) {
        if (failure == null) {
          failure = error;
        } else if (failure != error) {
          failure.addSuppressed(error);
        }
      }
    }
    if (failure != null) {
      if (failure instanceof Exception) {
        throw (Exception) failure;
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      throw new RuntimeException(failure);
    }
  }

  private static void closeIterator(Iterator<ColumnarBatch> iterator) throws Exception {
    if (iterator instanceof AutoCloseable) {
      ((AutoCloseable) iterator).close();
    }
  }

  private static Throwable unwrap(Throwable error) {
    Throwable current = error;
    while ((current instanceof ExecutionException || current instanceof CompletionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static RuntimeException propagate(Throwable error) {
    Throwable cause = unwrap(error);
    if (cause instanceof RuntimeException) {
      return (RuntimeException) cause;
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    return new RuntimeException(cause);
  }
}
