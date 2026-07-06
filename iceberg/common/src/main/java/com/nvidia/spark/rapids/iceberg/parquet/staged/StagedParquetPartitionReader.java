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

package com.nvidia.spark.rapids.iceberg.parquet.staged;

import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.GpuSemaphore$;
import com.nvidia.spark.rapids.filecache.FileCache;
import com.nvidia.spark.rapids.filecache.FileCache.FileCacheStartedToken;
import com.nvidia.spark.rapids.jni.RmmSpark;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import scala.Option;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.rapids.execution.TrampolineUtil$;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Coordinates the staged Parquet pipeline for one Spark input partition.
 *
 * <p>The pipeline has a deliberately strict thread boundary:</p>
 * <ol>
 *   <li>CPU workers fetch metadata and perform format-specific row-group filtering.</li>
 *   <li>The Spark task thread waits for the complete footer barrier and creates an immutable
 *       {@link PartitionReadPlan}.</li>
 *   <li>Each planned subtask lazily preallocates one exact-sized host buffer or executor-local
 *       mapping when its first I/O job starts. One I/O-pool job per source file then writes its
 *       distinct column-chunk ranges concurrently into disjoint regions of that shared output. A
 *       CPU-pool continuation adds the synthetic Parquet header and footer after every source job
 *       completes.</li>
 *   <li>The Spark task thread consumes sealed results in completion order and performs GPU
 *       decode and format-specific post-processing.</li>
 * </ol>
 *
 * <p>Only immutable plan objects cross thread boundaries. A {@link SubtaskExecution} is the
 * single mutable ownership cell for an in-flight output. Its atomic handoff guarantees that task
 * cancellation, worker failure, and successful publication cannot close or leak the same output.
 * Adapter contexts are owned by this reader and are closed exactly once with the reader.</p>
 *
 * @param <F> format-specific input-file metadata
 * @param <C> format-specific footer context
 */
public final class StagedParquetPartitionReader<F, C>
    implements Iterator<ColumnarBatch>, AutoCloseable {
  private final List<StagedScanFile<F>> files;
  private final StagedScanAdapter<F, C> adapter;
  private final StableGreedyReadPlanner<C> planner;
  private final StagedScanThreadPools pools;
  private final TaskContext taskContext;
  private final long taskAttemptId;
  private final int fileIoScratchBytes;
  private final Object lifecycleLock = new Object();
  private final Object iteratorLock = new Object();
  private final BlockingQueue<Completion<C>> completions = new LinkedBlockingQueue<>();
  private final List<Future<TimedFooter<C>>> footerFutures = new ArrayList<>();
  private final List<FooterResult<C>> footers = new ArrayList<>();
  private final List<SubtaskExecution<C>> executions = new ArrayList<>();
  private final ConcurrentLinkedQueue<FooterOwnership<C>> footerContexts =
      new ConcurrentLinkedQueue<>();
  private final AtomicInteger activeFooterTasks = new AtomicInteger();
  private final AtomicBoolean adapterCloseRequested = new AtomicBoolean();
  private final AtomicBoolean adapterClosed = new AtomicBoolean();
  private final AtomicReference<Throwable> asynchronousCleanupFailure = new AtomicReference<>();
  private final AtomicBoolean closed = new AtomicBoolean();

  private boolean initialized;
  private int remainingResults;
  private Iterator<ColumnarBatch> currentBatches;
  // Guarded by iteratorLock. Once the caller advances again, the previously returned batch has
  // finished flowing through the downstream GPU pipeline and this reader can yield the task-wide
  // permit before it restores or decodes another batch.
  private boolean batchReturnedSinceLastAdvance;

  /**
   * Creates a lazy partition reader. No footer or data work is submitted until the first
   * {@link #hasNext()} call.
   *
   * @param files inputs in deterministic Spark partition order
   * @param adapter format-specific footer and decode operations
   * @param maxRows maximum planned rows per GPU subtask (soft for a single row group)
   * @param maxEstimatedGpuBytes maximum estimated GPU bytes per subtask
   * @param targetParquetBytes target encoded bytes before closing a combined subtask; a
   *                           non-positive value disables cross-file combining
   * @param cpuThreads executor-wide footer/filter and synthetic-file finalization worker count
   * @param ioThreads executor-wide data-I/O worker count
   * @param fileIoScratchBytes positive copy-size setting supplied to staged outputs
   * @param taskContext Spark task context captured by the task thread; may be null in tests
   */
  public StagedParquetPartitionReader(
      List<StagedScanFile<F>> files,
      StagedScanAdapter<F, C> adapter,
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      int cpuThreads,
      int ioThreads,
      int fileIoScratchBytes,
      TaskContext taskContext) {
    this.files = immutableFileCopy(files);
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    this.planner = new StableGreedyReadPlanner<>(
        maxRows, maxEstimatedGpuBytes, targetParquetBytes,
        adapter::canCombine, adapter::estimateGpuBytes);
    this.pools = StagedScanThreadPools.getOrCreate(cpuThreads, ioThreads);
    if (fileIoScratchBytes <= 0) {
      throw new IllegalArgumentException(
          "fileIoScratchBytes must be positive: " + fileIoScratchBytes);
    }
    this.fileIoScratchBytes = fileIoScratchBytes;
    this.taskContext = taskContext;
    this.taskAttemptId = taskContext == null ? -1L : taskContext.taskAttemptId();
  }

  private static <F> List<StagedScanFile<F>> immutableFileCopy(
      List<StagedScanFile<F>> input) {
    Objects.requireNonNull(input, "files");
    ArrayList<StagedScanFile<F>> copy = new ArrayList<>(input);
    if (copy.contains(null)) {
      throw new IllegalArgumentException("files must not contain null values");
    }
    return java.util.Collections.unmodifiableList(copy);
  }

  @Override
  public boolean hasNext() {
    if (closed.get()) {
      releaseGpuSemaphoreFromTaskThread();
      return false;
    }
    try {
      initializeIfNeeded();
      while (true) {
        boolean semaphoreReleasedForAdvance = false;
        if (closed.get()) {
          releaseGpuSemaphoreFromTaskThread();
          return false;
        }
        synchronized (iteratorLock) {
          if (batchReturnedSinceLastAdvance) {
            // CachedGpuBatchIterator makes multi-batch decode output spillable specifically so
            // the semaphore can be yielded between batches. The iterator protocol guarantees
            // that asking to advance means the caller is done processing the prior batch.
            releaseGpuSemaphoreFromTaskThread();
            semaphoreReleasedForAdvance = true;
            batchReturnedSinceLastAdvance = false;
          }
          if (currentBatches != null) {
            if (currentBatches.hasNext()) {
              return true;
            }
            // The adapter acquires the semaphore while decoding, and next() acquires it while
            // restoring a cached spillable batch. Release as soon as that decoded iterator is
            // exhausted. In particular, this must happen before the remainingResults == 0 return
            // below; otherwise the last subtask holds the GPU until Spark completes the task.
            if (!semaphoreReleasedForAdvance) {
              releaseGpuSemaphoreFromTaskThread();
              semaphoreReleasedForAdvance = true;
            }
            closeIterator(currentBatches);
            currentBatches = null;
          }
        }
        if (remainingResults == 0) {
          if (!semaphoreReleasedForAdvance) {
            releaseGpuSemaphoreFromTaskThread();
          }
          return false;
        }

        Completion<C> completion;
        long waitStart = System.nanoTime();
        try {
          if (!semaphoreReleasedForAdvance) {
            releaseGpuSemaphoreFromTaskThread();
          }
          completion = completions.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("interrupted while waiting for staged Parquet data", e);
        }
        boolean resultPassedToDecode = false;
        Iterator<ColumnarBatch> decoded = null;
        boolean decodedInstalled = false;
        try {
          adapter.onResultWait(System.nanoTime() - waitStart);
          if (closed.get()) {
            return false;
          }
          remainingResults -= 1;
          if (completion.error != null) {
            retireExecution(completion.execution);
            throw propagate(completion.error);
          }
          resultPassedToDecode = true;
          decoded = decode(completion.result);
          retireExecution(completion.execution);
          synchronized (iteratorLock) {
            if (closed.get()) {
              // decodeAndPostProcess may just have acquired the semaphore even though close won
              // the race before this iterator could be installed.
              releaseGpuSemaphoreFromTaskThread();
              return false;
            }
            if (currentBatches != null) {
              throw new IllegalStateException("another decoded iterator is already active");
            }
            currentBatches = decoded;
            decodedInstalled = true;
          }
        } finally {
          if (decoded != null && !decodedInstalled) {
            closeIterator(decoded);
          }
          // A metric callback or any other pre-decode failure must not lose a result that has
          // already been removed from the completion queue.
          if (!resultPassedToDecode && completion.result != null) {
            completion.result.close();
          }
        }
      }
    } catch (CancellationException cancelled) {
      releaseGpuSemaphoreAfterFailure(cancelled);
      if (closed.get()) {
        return false;
      }
      closeAfterFailure(cancelled);
      throw cancelled;
    } catch (Throwable error) {
      releaseGpuSemaphoreAfterFailure(error);
      closeAfterFailure(error);
      throw propagate(error);
    }
  }

  private void retireExecution(SubtaskExecution<C> execution) {
    if (execution == null) {
      return;
    }
    synchronized (lifecycleLock) {
      executions.remove(execution);
    }
  }

  @Override
  public ColumnarBatch next() {
    try {
      if (!hasNext()) {
        throw new NoSuchElementException("no more staged Parquet batches");
      }
      // Do not hold iteratorLock while acquiring: another task can own the GPU for an arbitrary
      // time, and close() must remain able to cancel and reclaim this iterator in the meantime.
      GpuSemaphore$.MODULE$.acquireIfNecessary(taskContext);
      synchronized (iteratorLock) {
        if (closed.get() || currentBatches == null) {
          throw new CancellationException("staged Parquet reader was closed before next()");
        }
        // The acquire above restores a spillable batch and covers Iceberg GPU post-processing.
        // It is idempotent for the first batch, whose decode already acquired the permit.
        ColumnarBatch nextBatch = currentBatches.next();
        batchReturnedSinceLastAdvance = true;
        return nextBatch;
      }
    } catch (Throwable error) {
      releaseGpuSemaphoreAfterFailure(error);
      closeAfterFailure(error);
      throw propagate(error);
    }
  }

  /**
   * Submit every footer operation, wait for the complete barrier on the task thread, plan, and
   * submit every subtask to the I/O pool. Publication order after this point is completion order.
   */
  private void initializeIfNeeded() throws Exception {
    if (initialized) {
      return;
    }
    checkOpen();
    // Footer submission/filtering and task-thread planning are CPU-only. Relinquish any permit
    // still owned by this task before starting that work rather than waiting for the first
    // Future.get().
    releaseGpuSemaphoreFromTaskThread();
    for (StagedScanFile<F> file : files) {
      synchronized (lifecycleLock) {
        checkOpen();
        activeFooterTasks.incrementAndGet();
      }
      Future<TimedFooter<C>> future;
      try {
        future = pools.cpuExecutor().submit(() -> {
          try {
            return runAsTaskPoolThread(() -> {
              long start = System.nanoTime();
              FooterResult<C> footer = adapter.readAndFilterFooter(file);
              if (footer == null) {
                throw new IllegalStateException("footer adapter returned null");
              }
              FooterOwnership<C> ownership = new FooterOwnership<>(footer);
              footerContexts.add(ownership);
              if (closed.get() && footerContexts.remove(ownership)) {
                closeFooterOwnership(ownership);
              }
              return new TimedFooter<>(
                  footer, ownership, System.nanoTime() - start);
            });
          } finally {
            footerTaskFinished();
          }
        });
      } catch (Throwable error) {
        footerTaskFinished();
        throw error;
      }
      synchronized (lifecycleLock) {
        if (closed.get()) {
          throw new CancellationException("staged reader closed while submitting footers");
        }
        footerFutures.add(future);
      }
    }
    List<Future<TimedFooter<C>>> submittedFooters;
    synchronized (lifecycleLock) {
      submittedFooters = new ArrayList<>(footerFutures);
    }
    for (Future<TimedFooter<C>> future : submittedFooters) {
      long waitStart = System.nanoTime();
      TimedFooter<C> timedFooter = getFooterFuture(future);
      adapter.onFooterWait(System.nanoTime() - waitStart);
      FooterResult<C> footer = timedFooter.footer;
      adapter.onFooterCompleted(footer, timedFooter.footerNanos);
      boolean closeFooter = false;
      synchronized (lifecycleLock) {
        if (closed.get()) {
          closeFooter = true;
        } else {
          footers.add(footer);
        }
      }
      if (closeFooter) {
        if (footerContexts.remove(timedFooter.ownership)) {
          closeFooterOwnership(timedFooter.ownership);
        }
        throw new CancellationException("staged reader closed during footer filtering");
      }
    }

    PartitionReadPlan<C> plan;
    synchronized (lifecycleLock) {
      checkOpen();
      plan = planner.plan(new ArrayList<>(footers));
    }
    List<ReadSubtask<C>> plannedSubtasks = plan.getSubtasks();
    remainingResults = plannedSubtasks.size();
    initialized = true;
    for (ReadSubtask<C> subtask : plannedSubtasks) {
      SubtaskExecution<C> execution = new SubtaskExecution<>(subtask);
      synchronized (lifecycleLock) {
        checkOpen();
        executions.add(execution);
      }
      submit(execution);
    }
  }

  private void submit(SubtaskExecution<C> execution) throws Exception {
    CompletableFuture<PendingRead<C>> ioFuture = submitSourceReads(execution);

    CompletableFuture<StagedReadResult<C>> resultFuture = ioFuture.thenApplyAsync(
        pending -> runUncheckedAsTaskPoolThread(() -> combine(execution, pending)),
        pools.cpuExecutor());
    resultFuture.whenComplete((result, error) -> {
      if (error != null) {
        Throwable failure = unwrap(error);
        // The I/O worker may have installed an output before the combine callable could start.
        // Examples include executor rejection and task-context/RMM registration failures. This
        // callback runs only after the whole dependent stage is terminal, so it is the first
        // cancellation-safe place outside a worker to reclaim that still-owned output.
        try {
          execution.closeOutputAfterStage();
        } catch (Throwable closeError) {
          failure.addSuppressed(closeError);
          if (closed.get()) {
            recordAsynchronousCleanupFailure(closeError);
          }
        }
        if (!closed.get()) {
          completions.offer(Completion.failure(execution, failure));
        }
      } else if (closed.get()) {
        if (result != null) {
          closeResultAfterAsync(result);
        }
      } else {
        Completion<C> completion = Completion.success(execution, result);
        completions.offer(completion);
        // close() can race between the first closed check and queue publication. If it wins,
        // remove and close the just-published result so close() cannot miss it while draining.
        if (closed.get() && completions.remove(completion)) {
          closeResultAfterAsync(result);
        }
      }
    });
  }

  /**
   * Submit one directly scheduled I/O-pool job per source file in this subtask.
   *
   * <p>Each source receives at most one {@code readVectored} call containing a distinct range for
   * every cache-miss Parquet column chunk; cache-hit chunks are copied directly from their cached
   * channels. This preserves both levels of remote-I/O concurrency: source files run as independent
   * pool jobs and PerfIO runs the miss ranges within each source concurrently. The first source job
   * lazily creates the exact-sized output; sibling jobs briefly synchronize on that initialization
   * and then write their disjoint output ranges concurrently. The returned future completes only
   * after every writer is terminal, which makes it safe for the dependent CPU continuation to
   * write the synthetic header/footer and seal the output.</p>
   */
  private CompletableFuture<PendingRead<C>> submitSourceReads(
      SubtaskExecution<C> execution) {
    Map<StagedFileSource, List<PlannedReadRange>> rangesBySource = new LinkedHashMap<>();
    for (PlannedReadRange range : execution.subtask.getLayout().getRanges()) {
      rangesBySource.computeIfAbsent(range.getSource(), ignored -> new ArrayList<>())
          .add(range);
    }

    List<CompletableFuture<SourceReadStats>> sourceFutures = new ArrayList<>();
    for (Map.Entry<StagedFileSource, List<PlannedReadRange>> entry
        : rangesBySource.entrySet()) {
      if (closed.get()) {
        sourceFutures.add(failedFuture(
            new CancellationException("staged reader closed before source I/O submission")));
        break;
      }
      try {
        sourceFutures.add(CompletableFuture.supplyAsync(
            () -> runUncheckedAsTaskPoolThread(() -> {
              StagedParquetOutput output = getOrCreateOutput(execution);
              return readSourceRanges(output, entry.getKey(), entry.getValue());
            }),
            pools.ioExecutor()));
      } catch (Throwable submissionError) {
        // Preserve the all-writers-terminal barrier when a later submission fails. Closing the
        // shared output before already-submitted writers finish would race their HMB writes.
        sourceFutures.add(failedFuture(submissionError));
        break;
      }
    }

    // A valid Parquet projection can contain no physical column ranges. It still needs an output
    // for the header/footer, and allocating it as an I/O job preserves the same lazy memory bound.
    if (sourceFutures.isEmpty()) {
      if (closed.get()) {
        sourceFutures.add(failedFuture(
            new CancellationException("staged reader closed before output allocation")));
      } else {
        try {
          sourceFutures.add(CompletableFuture.supplyAsync(
              () -> runUncheckedAsTaskPoolThread(() -> {
                getOrCreateOutput(execution);
                return SourceReadStats.EMPTY;
              }),
              pools.ioExecutor()));
        } catch (Throwable submissionError) {
          sourceFutures.add(failedFuture(submissionError));
        }
      }
    }

    CompletableFuture<?>[] allSourceFutures =
        sourceFutures.toArray(new CompletableFuture<?>[sourceFutures.size()]);
    return CompletableFuture.allOf(allSourceFutures).thenApply(ignored -> {
      long ioNanos = 0L;
      long cacheHitCount = 0L;
      long cacheHitBytes = 0L;
      long cacheMissCount = 0L;
      long cacheMissBytes = 0L;
      long cacheReadNanos = 0L;
      for (CompletableFuture<SourceReadStats> sourceFuture : sourceFutures) {
        SourceReadStats stats = sourceFuture.join();
        ioNanos = Math.addExact(ioNanos, stats.ioNanos);
        cacheHitCount = Math.addExact(cacheHitCount, stats.cacheHitCount);
        cacheHitBytes = Math.addExact(cacheHitBytes, stats.cacheHitBytes);
        cacheMissCount = Math.addExact(cacheMissCount, stats.cacheMissCount);
        cacheMissBytes = Math.addExact(cacheMissBytes, stats.cacheMissBytes);
        cacheReadNanos = Math.addExact(cacheReadNanos, stats.cacheReadNanos);
      }
      StagedParquetOutput output = execution.output.get();
      if (output == null) {
        throw new IllegalStateException("source I/O completed without a staged Parquet output");
      }
      return new PendingRead<>(output, ioNanos, cacheHitCount, cacheHitBytes,
          cacheMissCount, cacheMissBytes, cacheReadNanos);
    });
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> failed = new CompletableFuture<>();
    failed.completeExceptionally(error);
    return failed;
  }

  /**
   * Lazily create the one shared output for a subtask.
   *
   * <p>Output creation happens under an I/O worker's installed Spark task/RMM context instead of
   * on the task thread. The per-execution monitor prevents duplicate exact-sized allocations while
   * allowing every source worker to write concurrently as soon as initialization returns. A failed
   * allocation is memoized so waiting siblings observe the same root failure.</p>
   */
  private StagedParquetOutput getOrCreateOutput(SubtaskExecution<C> execution) {
    synchronized (execution.outputInitializationLock) {
      if (execution.outputInitializationFailure != null) {
        throw propagate(execution.outputInitializationFailure);
      }
      checkOpen();
      StagedParquetOutput existing = execution.output.get();
      if (existing != null) {
        return existing;
      }
      try {
        return createOutput(execution);
      } catch (Throwable error) {
        execution.outputInitializationFailure = error;
        throw propagate(error);
      }
    }
  }

  private StagedParquetOutput createOutput(SubtaskExecution<C> execution) throws Exception {
    checkOpen();
    ReadSubtask<C> subtask = execution.subtask;
    SyntheticParquetLayout layout = subtask.getLayout();
    StagedParquetOutput output = StagedParquetOutputFactory.create(
        layout.getTotalSizeBytes(), taskAttemptId, subtask.getSubtaskId());
    if (!execution.output.compareAndSet(null, output)) {
      output.close();
      throw new CancellationException("staged read was cancelled before I/O started");
    }
    if (closed.get()) {
      execution.closeOutput(output);
      throw new CancellationException("staged reader closed before I/O started");
    }
    return output;
  }

  private SourceReadStats readSourceRanges(
      StagedParquetOutput output,
      StagedFileSource source,
      List<PlannedReadRange> ranges) throws Exception {
    long start = System.nanoTime();
    long cacheHitCount = 0L;
    long cacheHitBytes = 0L;
    long cacheMissCount = 0L;
    long cacheMissBytes = 0L;
    long cacheReadNanos = 0L;
    List<PlannedReadRange> remoteRanges = new ArrayList<>();
    Map<PlannedReadRange, FileCacheStartedToken> cacheTokens = new IdentityHashMap<>();
    Throwable primaryFailure = null;
    try {
      checkOpen();
      RapidsInputFile input = source.getOpener().open();
      FileCache fileCache = FileCache.get();
      for (PlannedReadRange range : ranges) {
        checkOpen();
        Option<SeekableByteChannel> cached = fileCache.getDataRangeChannel(
            input, range.getInputOffset(), range.getLength());
        if (cached.isDefined()) {
          long cacheStart = System.nanoTime();
          try (SeekableByteChannel channel = cached.get()) {
            output.copyCachedRange(
                channel, range.getOutputOffset(), range.getLength(), fileIoScratchBytes);
          }
          cacheReadNanos = Math.addExact(
              cacheReadNanos, System.nanoTime() - cacheStart);
          cacheHitCount += 1L;
          cacheHitBytes = Math.addExact(cacheHitBytes, range.getLength());
        } else {
          remoteRanges.add(range);
          cacheMissCount += 1L;
          cacheMissBytes = Math.addExact(cacheMissBytes, range.getLength());
          Option<FileCacheStartedToken> token = fileCache.startDataRangeCache(
              input, range.getInputOffset(), range.getLength());
          if (token.isDefined()) {
            cacheTokens.put(range, token.get());
          }
        }
      }

      output.copyRanges(input, remoteRanges, fileIoScratchBytes, (range, data) -> {
        FileCacheStartedToken token = cacheTokens.remove(range);
        if (token == null) {
          data.close();
          return;
        }
        // complete() consumes the HMB and marks the token finished before it queues cache work.
        // Ownership therefore transfers even if complete() itself throws; closing/cancelling here
        // would double-release the buffer and can mask the original cache failure.
        token.complete(data);
      });
      checkOpen();
      return new SourceReadStats(System.nanoTime() - start,
          cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos);
    } catch (Exception | Error error) {
      primaryFailure = error;
      throw error;
    } finally {
      Throwable cleanupFailure = null;
      for (FileCacheStartedToken token : cacheTokens.values()) {
        try {
          token.cancel();
        } catch (Throwable error) {
          cleanupFailure = addFailure(cleanupFailure, error);
        }
      }
      if (cleanupFailure != null) {
        if (primaryFailure != null) {
          primaryFailure.addSuppressed(cleanupFailure);
        } else {
          throw propagate(cleanupFailure);
        }
      }
    }
  }

  private StagedReadResult<C> combine(
      SubtaskExecution<C> execution,
      PendingRead<C> pending) throws Exception {
    StagedParquetOutput output = pending.output;
    SyntheticParquetLayout layout = execution.subtask.getLayout();
    long start = System.nanoTime();
    try {
      checkOpen();
      output.writeBytes(0L, layout.getHeaderBytes());
      output.writeBytes(layout.getFooterOffset(), layout.getFooterAndTrailerBytes());
      output.seal(layout.getTotalSizeBytes());
      long combineNanos = System.nanoTime() - start;

      // Move ownership out of the cancellation cell before publishing the immutable result.
      if (!execution.output.compareAndSet(output, null)) {
        throw new CancellationException("staged read was cancelled while combining");
      }
      StagedReadResult<C> result = null;
      try {
        result = new StagedReadResult<>(execution.subtask, output,
            new SubtaskStats(pending.ioNanos, combineNanos, output.sizeBytes(),
                output.backingStore(), pending.cacheHitCount, pending.cacheHitBytes,
                pending.cacheMissCount, pending.cacheMissBytes, pending.cacheReadNanos));
        if (closed.get()) {
          CancellationException cancellation =
              new CancellationException("staged read was cancelled before publication");
          try {
            result.close();
          } catch (Throwable closeError) {
            cancellation.addSuppressed(closeError);
          }
          throw cancellation;
        }
        return result;
      } catch (Throwable error) {
        if (result == null) {
          try {
            output.close();
          } catch (Throwable closeError) {
            error.addSuppressed(closeError);
          }
        }
        throw error;
      }
    } catch (Throwable error) {
      try {
        execution.closeOutput(output);
      } catch (Throwable closeError) {
        error.addSuppressed(closeError);
      }
      throw error;
    }
  }

  private Iterator<ColumnarBatch> decode(StagedReadResult<C> result) throws Exception {
    HostMemoryBuffer materialized = null;
    try {
      // The completion-wait boundary has already yielded the semaphore, so everything through
      // host materialization runs without it. decodeAndPostProcess owns the initial acquire
      // immediately before cuDF; next() reacquires after an inter-batch yield.
      adapter.onSubtaskCompleted(result.getSubtask(), result.getStats());
      long materializeStart = System.nanoTime();
      materialized = result.getOutput().materialize();
      long materializeEnd = System.nanoTime();
      long materializationNanos = materializeEnd - materializeStart;
      adapter.onMaterializationCompleted(result.getSubtask(), materializationNanos);
      // materialize() returns an independent reference, so the spillable buffer or local file can
      // be released before the task thread enters GPU decode.
      result.close();
      HostMemoryBuffer transferred = materialized;
      materialized = null; // ownership transfers when the adapter is invoked, even on failure
      Iterator<ColumnarBatch> decoded = adapter.decodeAndPostProcess(
          result.getSubtask(), transferred);
      if (decoded == null) {
        throw new IllegalStateException("decode adapter returned null");
      }
      return decoded;
    } finally {
      result.close();
      if (materialized != null) {
        materialized.close();
      }
    }
  }

  private void checkOpen() {
    if (closed.get()) {
      throw new CancellationException("staged Parquet reader is closed");
    }
  }

  /**
   * Release the task-wide GPU permit at a task-thread CPU/I/O boundary.
   *
   * <p>This helper must never be called by the footer, source-I/O, or combine pool workers.
   * {@code GpuSemaphore} ownership is keyed by Spark task attempt rather than Java thread, so a
   * worker using the captured {@link TaskContext} could otherwise release the permit while this
   * reader's task thread is concurrently decoding on the GPU.</p>
   */
  private void releaseGpuSemaphoreFromTaskThread() {
    GpuSemaphore$.MODULE$.releaseIfNecessary(taskContext);
  }

  /** Preserve an operation's primary failure if semaphore bookkeeping also fails. */
  private void releaseGpuSemaphoreAfterFailure(Throwable original) {
    try {
      releaseGpuSemaphoreFromTaskThread();
    } catch (Throwable releaseError) {
      if (releaseError != original) {
        original.addSuppressed(releaseError);
      }
    }
  }

  /**
   * Run shared-pool work as part of the Spark task that created this reader.
   *
   * <p>{@link TaskContext} is thread-local and is not inherited by executor-pool workers. The RMM
   * registration associates host allocation, spill, and retry coordination on this worker with
   * the owning Spark task. This is the same paired registration used by the existing asynchronous
   * Parquet, ORC, and Avro readers.</p>
   */
  private <T> T runAsTaskPoolThread(Callable<T> operation) throws Exception {
    if (taskContext == null) {
      return operation.call();
    }
    T result = null;
    Throwable failure = null;
    boolean operationCompleted = false;
    boolean taskContextInstalled = false;
    boolean rmmRegistered = false;
    try {
      TrampolineUtil$.MODULE$.setTaskContext(taskContext);
      taskContextInstalled = true;
      // Tell the RMM retry framework which Spark task temporarily owns this shared worker.
      RmmSpark.poolThreadWorkingOnTask(taskAttemptId);
      rmmRegistered = true;
      result = operation.call();
      operationCompleted = true;
    } catch (Throwable error) {
      failure = error;
    } finally {
      try {
        if (rmmRegistered) {
          RmmSpark.poolThreadFinishedForTask(taskAttemptId);
        }
      } catch (Throwable cleanupError) {
        failure = addFailure(failure, cleanupError);
      }
      try {
        if (taskContextInstalled) {
          TrampolineUtil$.MODULE$.unsetTaskContext();
        }
      } catch (Throwable cleanupError) {
        failure = addFailure(failure, cleanupError);
      }
    }
    if (failure != null) {
      // A Java return value is discarded when a finally/cleanup action throws. In particular,
      // combine() may already have transferred its output into a StagedReadResult. Close any
      // successfully returned resource before propagating the cleanup failure so that handoff
      // cannot leak a host buffer or local file.
      if (operationCompleted && result instanceof AutoCloseable) {
        try {
          ((AutoCloseable) result).close();
        } catch (Throwable closeError) {
          failure.addSuppressed(closeError);
        }
      }
      if (failure instanceof Exception) {
        throw (Exception) failure;
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      throw new RuntimeException(failure);
    }
    return result;
  }

  private <T> T runUncheckedAsTaskPoolThread(Callable<T> operation) {
    try {
      return runAsTaskPoolThread(operation);
    } catch (Throwable error) {
      throw new CompletionException(error);
    }
  }

  private TimedFooter<C> getFooterFuture(Future<TimedFooter<C>> future) throws Exception {
    while (true) {
      checkOpen();
      try {
        return future.get(100L, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ignored) {
        // Polling lets an explicit close wake the task thread without canceling a footer callable
        // that may still be using adapter state.
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      } catch (ExecutionException e) {
        Throwable cause = unwrap(e);
        if (cause instanceof Exception) {
          throw (Exception) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw new RuntimeException(cause);
      }
    }
  }

  private void footerTaskFinished() {
    int remaining = activeFooterTasks.decrementAndGet();
    if (remaining < 0) {
      recordAsynchronousCleanupFailure(
          new IllegalStateException("active footer task count became negative"));
    }
    maybeCloseAdapter();
  }

  private void closeFooterOwnership(FooterOwnership<C> ownership) {
    if (ownership.closed.compareAndSet(false, true)) {
      try {
        adapter.closeContext(ownership.footer.getContext());
      } catch (Throwable error) {
        recordAsynchronousCleanupFailure(error);
      }
    }
  }

  private void maybeCloseAdapter() {
    if (adapterCloseRequested.get() && activeFooterTasks.get() == 0
        && adapterClosed.compareAndSet(false, true)) {
      try {
        adapter.close();
      } catch (Throwable error) {
        recordAsynchronousCleanupFailure(error);
      }
    }
  }

  private void recordAsynchronousCleanupFailure(Throwable error) {
    Throwable current = asynchronousCleanupFailure.get();
    if (current == null) {
      if (asynchronousCleanupFailure.compareAndSet(null, error)) {
        return;
      }
      current = asynchronousCleanupFailure.get();
    }
    if (current != error) {
      synchronized (current) {
        current.addSuppressed(error);
      }
    }
  }

  private void closeResultAfterAsync(StagedReadResult<C> result) {
    try {
      result.close();
    } catch (Throwable error) {
      recordAsynchronousCleanupFailure(error);
    }
  }

  private void closeAfterFailure(Throwable original) {
    try {
      close();
    } catch (Throwable closeError) {
      original.addSuppressed(closeError);
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // Wake a task thread blocked in completions.take(). The closed flag distinguishes this
    // sentinel from a real subtask failure, so hasNext returns false after explicit close.
    completions.offer(Completion.failure(null,
        new CancellationException("staged Parquet reader is closed")));
    Throwable failure = null;
    synchronized (iteratorLock) {
      if (currentBatches != null) {
        try {
          closeIterator(currentBatches);
        } catch (Throwable error) {
          failure = error;
        }
        currentBatches = null;
      }
    }
    synchronized (lifecycleLock) {
      footerFutures.clear();
      executions.clear();
      footers.clear();
    }
    // Footer futures are intentionally left running. Their contexts are registered by the
    // worker itself, and the last worker closes adapter-wide state. Future cancellation can mark
    // a running FutureTask complete before its callable exits, which would reintroduce a close
    // race with the adapter. Dropping our references is sufficient after the closed flag is set.
    // Source futures are likewise left terminal-barrier-driven: queued jobs observe closed before
    // allocating/reading, active writers finish, and only then may the callback close their shared
    // output. Cancelling allOf could otherwise close an output while supplyAsync is still writing.
    // Leave failure/sentinel entries in the queue so a concurrently blocked hasNext stays
    // unblocked. Only successful entries own resources and need removal during cleanup.
    for (Completion<C> completion : completions) {
      if (completion.result != null && completions.remove(completion)) {
        try {
          completion.result.close();
        } catch (Throwable error) {
          failure = addFailure(failure, error);
        }
      }
    }
    FooterOwnership<C> footerOwnership;
    while ((footerOwnership = footerContexts.poll()) != null) {
      closeFooterOwnership(footerOwnership);
    }
    adapterCloseRequested.set(true);
    maybeCloseAdapter();
    Throwable asynchronousFailure = asynchronousCleanupFailure.get();
    if (asynchronousFailure != null) {
      failure = addFailure(failure, asynchronousFailure);
    }
    if (failure != null) {
      throw propagate(failure);
    }
  }

  private static Throwable addFailure(Throwable first, Throwable next) {
    if (first == null) {
      return next;
    }
    first.addSuppressed(next);
    return first;
  }

  private static void closeIterator(Iterator<ColumnarBatch> iterator) throws Exception {
    if (iterator instanceof AutoCloseable) {
      ((AutoCloseable) iterator).close();
    }
  }

  private static Throwable unwrap(Throwable error) {
    Throwable current = error;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static RuntimeException propagate(Throwable error) {
    Throwable unwrapped = unwrap(error);
    if (unwrapped instanceof RuntimeException) {
      return (RuntimeException) unwrapped;
    }
    if (unwrapped instanceof Error) {
      throw (Error) unwrapped;
    }
    return new RuntimeException(unwrapped);
  }

  /** I/O-stage value whose output remains owned by its execution's atomic ownership cell. */
  private static final class PendingRead<C> {
    private final StagedParquetOutput output;
    private final long ioNanos;
    private final long cacheHitCount;
    private final long cacheHitBytes;
    private final long cacheMissCount;
    private final long cacheMissBytes;
    private final long cacheReadNanos;

    private PendingRead(
        StagedParquetOutput output,
        long ioNanos,
        long cacheHitCount,
        long cacheHitBytes,
        long cacheMissCount,
        long cacheMissBytes,
        long cacheReadNanos) {
      this.output = output;
      this.ioNanos = ioNanos;
      this.cacheHitCount = cacheHitCount;
      this.cacheHitBytes = cacheHitBytes;
      this.cacheMissCount = cacheMissCount;
      this.cacheMissBytes = cacheMissBytes;
      this.cacheReadNanos = cacheReadNanos;
    }
  }

  /** Aggregate measurements from one independently scheduled physical source. */
  private static final class SourceReadStats {
    private static final SourceReadStats EMPTY =
        new SourceReadStats(0L, 0L, 0L, 0L, 0L, 0L);

    private final long ioNanos;
    private final long cacheHitCount;
    private final long cacheHitBytes;
    private final long cacheMissCount;
    private final long cacheMissBytes;
    private final long cacheReadNanos;

    private SourceReadStats(
        long ioNanos,
        long cacheHitCount,
        long cacheHitBytes,
        long cacheMissCount,
        long cacheMissBytes,
        long cacheReadNanos) {
      this.ioNanos = ioNanos;
      this.cacheHitCount = cacheHitCount;
      this.cacheHitBytes = cacheHitBytes;
      this.cacheMissCount = cacheMissCount;
      this.cacheMissBytes = cacheMissBytes;
      this.cacheReadNanos = cacheReadNanos;
    }
  }

  /** Footer result paired with worker elapsed time for task-thread metric publication. */
  private static final class TimedFooter<C> {
    private final FooterResult<C> footer;
    private final FooterOwnership<C> ownership;
    private final long footerNanos;

    private TimedFooter(
        FooterResult<C> footer,
        FooterOwnership<C> ownership,
        long footerNanos) {
      this.footer = footer;
      this.ownership = ownership;
      this.footerNanos = footerNanos;
    }
  }

  /** Idempotent context owner registered by a footer worker before its future completes. */
  private static final class FooterOwnership<C> {
    private final FooterResult<C> footer;
    private final AtomicBoolean closed = new AtomicBoolean();

    private FooterOwnership(FooterResult<C> footer) {
      this.footer = Objects.requireNonNull(footer, "footer");
    }
  }

  /**
   * Mutable lifecycle state for one otherwise immutable subtask.
   *
   * <p>{@code outputInitializationLock} protects the lazy, exactly-once allocation and its
   * memoized failure. {@code output} is non-null only while source I/O or combination owns the
   * shared output. Successful combination atomically removes it before constructing
   * {@link StagedReadResult}; terminal-stage cleanup atomically removes and closes it. Exactly one
   * ownership path can win.</p>
   */
  private static final class SubtaskExecution<C> {
    private final ReadSubtask<C> subtask;
    private final Object outputInitializationLock = new Object();
    private final AtomicReference<StagedParquetOutput> output = new AtomicReference<>();
    /** Accessed only while holding {@link #outputInitializationLock}. */
    private Throwable outputInitializationFailure;

    private SubtaskExecution(ReadSubtask<C> subtask) {
      this.subtask = subtask;
    }

    private void closeOutput(StagedParquetOutput expected) {
      if (output.compareAndSet(expected, null)) {
        expected.close();
      }
    }

    /** Reclaim an output after the I/O/combine future has reached a terminal failure. */
    private void closeOutputAfterStage() {
      StagedParquetOutput owned = output.getAndSet(null);
      if (owned != null) {
        owned.close();
      }
    }

  }

  /** Queue value that publishes exactly one success or failure for a planned subtask. */
  private static final class Completion<C> {
    private final SubtaskExecution<C> execution;
    private final StagedReadResult<C> result;
    private final Throwable error;

    private Completion(
        SubtaskExecution<C> execution,
        StagedReadResult<C> result,
        Throwable error) {
      this.execution = execution;
      this.result = result;
      this.error = error;
    }

    private static <C> Completion<C> success(
        SubtaskExecution<C> execution,
        StagedReadResult<C> result) {
      return new Completion<>(Objects.requireNonNull(execution, "execution"),
          Objects.requireNonNull(result, "result"), null);
    }

    private static <C> Completion<C> failure(
        SubtaskExecution<C> execution,
        Throwable error) {
      return new Completion<>(execution, null, Objects.requireNonNull(error, "error"));
    }
  }
}
