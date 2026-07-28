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

package com.nvidia.spark.rapids.iceberg.parquet.async;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile;
import com.nvidia.spark.rapids.reader.Combiner;
import com.nvidia.spark.rapids.reader.ReadPlanner;

/**
 * Synchronized, event-driven planner for Iceberg Parquet files.
 *
 * <p>Footer and data futures notify this object directly. With combining enabled, files are fed
 * to the stable greedy planner in data-completion order; otherwise they are fed in input order.
 * Every admitted file that leaves an open group starts a fresh combine timeout. Closed plans are
 * immediately submitted to the injected combiner, while {@link #nextReady()} exposes those
 * futures in plan-emission order even when combine workers finish out of order.</p>
 *
 * <p>This class owns every successful {@link FileFragment} after {@link #addFile} returns.
 * Combined inputs retain their own fragment references, so closing the planner cannot invalidate
 * a decoder input already handed to the Spark task thread.</p>
 */
public final class ParquetReadPlanner implements ReadPlanner<
    IcebergPartitionedFile, FooterResult, FileFragment, ParquetDecodeInput> {
  private static final ScheduledExecutorService TIMER =
      Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());

  private final StableGreedyReadPlanner.Session session;
  private final Combiner<ReadSubtask, FileFragment, ParquetDecodeInput> combiner;
  private final ExecutorService executor;
  private final boolean combineEnabled;
  private final long combineWaitMs;
  private final AtomicBoolean closed;
  private final ArrayList<FileState> files = new ArrayList<>();
  private final Map<FooterResult, FileFragment> dataByFooter = new IdentityHashMap<>();
  private final ArrayDeque<CompletableFuture<Optional<ParquetDecodeInput>>> outputs =
      new ArrayDeque<>();
  private final ArrayDeque<CompletableFuture<Optional<ParquetDecodeInput>>> waiters =
      new ArrayDeque<>();
  private final ArrayList<CompletableFuture<ParquetDecodeInput>> combinedInputs =
      new ArrayList<>();

  private int nextOrderedFile;
  private int admittedFiles;
  private boolean registrationComplete;
  private boolean planningComplete;
  private boolean resourcesClosed;
  private Throwable failure;
  private long timerGeneration;
  private ScheduledFuture<?> timer;

  public ParquetReadPlanner(
      StableGreedyReadPlanner planner,
      Combiner<ReadSubtask, FileFragment, ParquetDecodeInput> combiner,
      ExecutorService executor,
      boolean combineEnabled,
      long combineWaitMs,
      AtomicBoolean closed) {
    this.session = Objects.requireNonNull(planner, "planner").newSession();
    this.combiner = Objects.requireNonNull(combiner, "combiner");
    this.executor = Objects.requireNonNull(executor, "executor");
    this.combineEnabled = combineEnabled;
    this.combineWaitMs = Math.max(0L, combineWaitMs);
    this.closed = Objects.requireNonNull(closed, "closed");
  }

  @Override
  public synchronized void addFile(
      int fileId,
      IcebergPartitionedFile source,
      CompletableFuture<FooterResult> footer,
      CompletableFuture<FileFragment> data) {
    checkAccepting();
    if (fileId != files.size()) {
      throw new IllegalArgumentException("file IDs must be registered contiguously");
    }
    FileState state = new FileState(fileId, source, footer, data);
    // Publish the state before callbacks are attached: CompletableFuture invokes callbacks
    // inline when an already-completed future is registered.
    files.add(state);
    footer.whenComplete((value, error) -> onFooter(state, value, error));
    data.whenComplete((value, error) -> onData(state, value, error));
  }

  @Override
  public synchronized void noMoreFiles() {
    checkAccepting();
    registrationComplete = true;
    finishIfPossible();
  }

  @Override
  public synchronized CompletableFuture<Optional<ParquetDecodeInput>> nextReady() {
    if (!outputs.isEmpty()) {
      return outputs.removeFirst();
    }
    if (failure != null) {
      return failedFuture(failure);
    }
    if (planningComplete || closed.get()) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    CompletableFuture<Optional<ParquetDecodeInput>> waiter = new CompletableFuture<>();
    waiters.addLast(waiter);
    return waiter;
  }

  private synchronized void onFooter(
      FileState state,
      FooterResult footer,
      Throwable error) {
    if (error != null) {
      fail(error);
      return;
    }
    if (closed.get()) {
      return;
    }
    state.footerValue = Objects.requireNonNull(footer, "footer future completed with null");
  }

  private synchronized void onData(
      FileState state,
      FileFragment data,
      Throwable error) {
    if (error != null) {
      fail(error);
      return;
    }
    if (data == null) {
      fail(new IllegalStateException("data future completed with null"));
      return;
    }
    if (closed.get() || failure != null) {
      data.close();
      return;
    }
    state.dataValue = data;
    if (state.footerValue == null) {
      try {
        state.footerValue = state.footer.join();
      } catch (Throwable footerError) {
        data.close();
        fail(footerError);
        return;
      }
    }
    if (combineEnabled) {
      admit(state);
    } else {
      drainInputOrder();
    }
    finishIfPossible();
  }

  private void drainInputOrder() {
    while (nextOrderedFile < files.size()) {
      FileState next = files.get(nextOrderedFile);
      if (next.dataValue == null) {
        return;
      }
      nextOrderedFile++;
      admit(next);
    }
  }

  private void admit(FileState state) {
    if (state.admitted) {
      return;
    }
    state.admitted = true;
    admittedFiles++;
    dataByFooter.put(state.footerValue, state.dataValue);
    emitPlans(session.add(state.footerValue));
    if (combineEnabled && session.hasOpenBlocks()) {
      scheduleFreshCombineTimeout();
    } else {
      cancelTimer();
    }
  }

  private void scheduleFreshCombineTimeout() {
    cancelTimer();
    long generation = ++timerGeneration;
    timer = TIMER.schedule(
        () -> onCombineTimeout(generation), combineWaitMs, TimeUnit.MILLISECONDS);
  }

  private synchronized void onCombineTimeout(long generation) {
    if (closed.get() || failure != null || planningComplete
        || generation != timerGeneration || !session.hasOpenBlocks()) {
      return;
    }
    timer = null;
    emitPlans(session.flush());
    finishIfPossible();
  }

  private void finishIfPossible() {
    if (!planningComplete && registrationComplete && admittedFiles == files.size()) {
      cancelTimer();
      emitPlans(session.finish());
      planningComplete = true;
      while (!waiters.isEmpty()) {
        waiters.removeFirst().complete(Optional.empty());
      }
    }
  }

  private void emitPlans(List<ReadSubtask> plans) {
    for (ReadSubtask plan : plans) {
      ArrayList<FileFragment> fragments = new ArrayList<>(plan.getFileSlices().size());
      for (ReadSubtask.FileSlice slice : plan.getFileSlices()) {
        FileFragment fragment = dataByFooter.get(slice.getFooter());
        if (fragment == null) {
          fail(new IllegalStateException("plan references data that is not ready"));
          return;
        }
        fragments.add(fragment);
      }
      CompletableFuture<ParquetDecodeInput> combined =
          combiner.combine(plan, fragments, executor);
      combinedInputs.add(combined);
      combined.whenComplete((input, error) -> {
        if (error != null) {
          synchronized (ParquetReadPlanner.this) {
            fail(error);
          }
        } else if (closed.get() && input != null) {
          input.close();
        }
      });
      CompletableFuture<Optional<ParquetDecodeInput>> output =
          combined.thenApply(Optional::of);
      if (waiters.isEmpty()) {
        outputs.addLast(output);
      } else {
        pipe(output, waiters.removeFirst());
      }
    }
  }

  private void fail(Throwable error) {
    if (failure != null || closed.get()) {
      return;
    }
    failure = unwrap(error);
    cancelTimer();
    while (!waiters.isEmpty()) {
      waiters.removeFirst().completeExceptionally(failure);
    }
    while (!outputs.isEmpty()) {
      outputs.removeFirst();
    }
  }

  private static <T> void pipe(
      CompletableFuture<T> source,
      CompletableFuture<T> destination) {
    source.whenComplete((value, error) -> {
      if (error == null) {
        destination.complete(value);
      } else {
        destination.completeExceptionally(unwrap(error));
      }
    });
  }

  @Override
  public synchronized void close() {
    if (resourcesClosed) {
      return;
    }
    resourcesClosed = true;
    closed.set(true);
    cancelTimer();
    while (!waiters.isEmpty()) {
      waiters.removeFirst().complete(Optional.empty());
    }
    outputs.clear();
    for (CompletableFuture<ParquetDecodeInput> combined : combinedInputs) {
      combined.whenComplete((input, error) -> {
        if (input != null) {
          input.close();
        }
      });
    }
    for (FileState state : files) {
      if (state.dataValue != null && !state.baseReferenceClosed) {
        state.baseReferenceClosed = true;
        state.dataValue.close();
      }
    }
  }

  private void cancelTimer() {
    timerGeneration++;
    if (timer != null) {
      timer.cancel(false);
      timer = null;
    }
  }

  private void checkAccepting() {
    if (registrationComplete) {
      throw new IllegalStateException("all files have already been registered");
    }
    if (closed.get()) {
      throw new IllegalStateException("planner is closed");
    }
    if (failure != null) {
      throw new CompletionException(failure);
    }
  }

  private static Throwable unwrap(Throwable error) {
    Throwable current = error;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> result = new CompletableFuture<>();
    result.completeExceptionally(error);
    return result;
  }

  private static final class FileState {
    private final int fileId;
    private final IcebergPartitionedFile source;
    private final CompletableFuture<FooterResult> footer;
    private final CompletableFuture<FileFragment> data;
    private FooterResult footerValue;
    private FileFragment dataValue;
    private boolean admitted;
    private boolean baseReferenceClosed;

    private FileState(
        int fileId,
        IcebergPartitionedFile source,
        CompletableFuture<FooterResult> footer,
        CompletableFuture<FileFragment> data) {
      this.fileId = fileId;
      this.source = Objects.requireNonNull(source, "source");
      this.footer = Objects.requireNonNull(footer, "footer");
      this.data = Objects.requireNonNull(data, "data");
    }
  }

  private static final class DaemonThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable runnable) {
      Thread thread = new Thread(runnable, "iceberg-parquet-combine-timer");
      thread.setDaemon(true);
      return thread;
    }
  }
}
