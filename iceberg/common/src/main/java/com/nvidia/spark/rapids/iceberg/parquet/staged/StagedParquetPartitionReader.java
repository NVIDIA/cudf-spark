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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.GpuSemaphore$;
import com.nvidia.spark.rapids.filecache.FileCache;
import com.nvidia.spark.rapids.filecache.FileCache.FileCacheStartedToken;
import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile;
import com.nvidia.spark.rapids.jni.RmmSpark;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import scala.Option;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.rapids.execution.TrampolineUtil$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Coordinates the staged Parquet pipeline for one Spark input partition.
 *
 * <p>The pipeline has a deliberately strict thread boundary:</p>
 * <ol>
 *   <li>Shared workers fetch metadata and perform Iceberg row-group filtering.</li>
 *   <li>The Spark task thread waits for the complete footer barrier and plans ordered subtasks.</li>
 *   <li>Each task submits planned subtask I/O in order. Subtask N+1 may submit after N has enqueued
 *       all of its asynchronous reads; it does not wait for N to finish.</li>
 *   <li>The task thread consumes completion futures in plan order and feeds each synthetic file
 *       to GPU decode.</li>
 * </ol>
 */
public final class StagedParquetPartitionReader
    implements Iterator<ColumnarBatch>, AutoCloseable {
  private final List<IcebergPartitionedFile> files;
  private final StagedScanAdapter adapter;
  private final StableGreedyReadPlanner planner;
  private final StagedScanThreadPools pools;
  private final TaskContext taskContext;
  private final long taskAttemptId;
  private final Object lifecycleLock = new Object();
  private final Object iteratorLock = new Object();
  // Guarded by lifecycleLock. Futures remain in planner order even when I/O finishes out of order.
  private final Deque<CompletableFuture<Completion>> pendingResults = new ArrayDeque<>();
  private final CompletableFuture<Void> closeSignal = new CompletableFuture<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> asynchronousCleanupFailure = new AtomicReference<>();

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
   * @param adapter Iceberg footer and decode operations
   * @param expectedSparkSchema final Iceberg output schema used for GPU-size planning
   * @param maxRows maximum planned rows per GPU subtask (soft for a single row group)
   * @param maxEstimatedGpuBytes maximum estimated GPU bytes per subtask
   * @param targetParquetBytes target encoded bytes before closing a combined subtask; a
   *                           non-positive value disables cross-file combining
   * @param workerThreads executor-wide worker count shared by footer, I/O, and finalization jobs
   * @param taskContext Spark task context captured by the task thread; may be null in tests
   */
  public StagedParquetPartitionReader(
      List<IcebergPartitionedFile> files,
      StagedScanAdapter adapter,
      StructType expectedSparkSchema,
      int maxRows,
      long maxEstimatedGpuBytes,
      long targetParquetBytes,
      int workerThreads,
      TaskContext taskContext) {
    this.files = immutableFileCopy(files);
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    this.planner = new StableGreedyReadPlanner(
        maxRows, maxEstimatedGpuBytes, targetParquetBytes, expectedSparkSchema);
    this.pools = StagedScanThreadPools.getOrCreate(workerThreads);
    this.taskContext = taskContext;
    this.taskAttemptId = taskContext == null ? -1L : taskContext.taskAttemptId();
  }

  private static List<IcebergPartitionedFile> immutableFileCopy(
      List<IcebergPartitionedFile> input) {
    Objects.requireNonNull(input, "files");
    ArrayList<IcebergPartitionedFile> copy = new ArrayList<>(input);
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

        CompletableFuture<Completion> resultFuture;
        synchronized (lifecycleLock) {
          resultFuture = pendingResults.peekFirst();
        }
        if (resultFuture == null) {
          throw new IllegalStateException("missing staged result future");
        }

        long waitStart = System.nanoTime();
        if (!semaphoreReleasedForAdvance) {
          releaseGpuSemaphoreFromTaskThread();
        }
        Completion completion = waitForResult(resultFuture);
        boolean resultPassedToDecode = false;
        Iterator<ColumnarBatch> decoded = null;
        boolean decodedInstalled = false;
        try {
          adapter.onResultWait(System.nanoTime() - waitStart);
          if (closed.get()) {
            return false;
          }
          synchronized (lifecycleLock) {
            if (pendingResults.peekFirst() != resultFuture) {
              if (closed.get()) {
                return false;
              }
              throw new IllegalStateException("staged results are not in planner order");
            }
            pendingResults.removeFirst();
            remainingResults -= 1;
          }
          resultPassedToDecode = true;
          decoded = decode(completion);
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
          // already been removed from the ordered result deque.
          if (!resultPassedToDecode) {
            completion.close();
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

  /** Submit all footers, plan on the task thread, and wire ordered subtask futures. */
  private void initializeIfNeeded() throws Exception {
    if (initialized) {
      return;
    }
    checkOpen();
    // Footer submission/filtering and task-thread planning are CPU-only. Relinquish any permit
    // still owned by this task before starting that work rather than waiting for the first
    // Future.get().
    releaseGpuSemaphoreFromTaskThread();
    List<Future<TimedFooter>> footerFutures = new ArrayList<>();
    List<FooterResult> footers = new ArrayList<>();
    for (IcebergPartitionedFile file : files) {
      checkOpen();
      Future<TimedFooter> future = pools.executor().submit(() ->
          runAsTaskPoolThread(() -> {
            long start = System.nanoTime();
            FooterResult footer = adapter.readAndFilterFooter(file);
            if (footer == null) {
              throw new IllegalStateException("footer adapter returned null");
            }
            return new TimedFooter(footer, System.nanoTime() - start);
          }));
      if (closed.get()) {
        throw new CancellationException("staged reader closed while submitting footers");
      }
      footerFutures.add(future);
    }
    for (Future<TimedFooter> future : footerFutures) {
      long waitStart = System.nanoTime();
      TimedFooter timedFooter = getFooterFuture(future);
      adapter.onFooterWait(System.nanoTime() - waitStart);
      FooterResult footer = timedFooter.footer;
      adapter.onFooterCompleted(timedFooter.footerNanos);
      checkOpen();
      footers.add(footer);
    }

    synchronized (lifecycleLock) {
      checkOpen();
      List<ReadSubtask> plan = planner.plan(footers);
      CompletableFuture<Void> previousSubmission = CompletableFuture.completedFuture(null);
      for (ReadSubtask subtask : plan) {
        checkOpen();
        SubtaskFutures futures = submit(subtask, previousSubmission);
        pendingResults.addLast(futures.completion);
        previousSubmission = futures.ioSubmitted;
      }
      remainingResults = pendingResults.size();
      initialized = true;
    }
  }

  /**
   * Build one subtask's ordered submission and completion chains.
   *
   * <p>The I/O-submitted future gates the next subtask in this Spark task. It becomes terminal
   * after every source request has been handed to its asynchronous input implementation, not
   * after those requests finish. The separate completion future owns the output until all I/O and
   * synthetic-Parquet finalization are terminal.</p>
   */
  private SubtaskFutures submit(
      ReadSubtask subtask,
      CompletableFuture<Void> previousIoSubmission) {
    CompletableFuture<SubmittedIo> ioSubmitted = previousIoSubmission
        .thenApplyAsync(ignored -> runUncheckedAsTaskPoolThread(() -> {
          // Step 1: allocate this subtask's exact-sized memory or disk output.
          StagedParquetOutput output = createOutput(subtask);
          try {
            // Step 2: submit every source and column-chunk read in deterministic order. This
            // stage ends as soon as the async read APIs return their futures. Keeping allocation
            // and submission in one continuation prevents executor rejection between them from
            // stranding an allocated output.
            return startSourceReads(subtask, output);
          } catch (Throwable error) {
            try {
              // close() waits behind any async writer stamps already acquired by this subtask.
              output.close();
            } catch (Throwable closeError) {
              if (closeError != error) {
                error.addSuppressed(closeError);
              }
            }
            throw propagate(error);
          }
        }), pools.executor());

    // Step 3: release the next subtask only after this one's async read APIs have returned. A
    // returned future owns any request failure; only a failure before this submission point stops
    // the per-task chain.
    CompletableFuture<Void> nextSubmission = ioSubmitted.thenApply(ignored -> null);

    CompletableFuture<Completion> completion = ioSubmitted.thenCompose(submitted ->
        // Step 4: wait asynchronously for every accepted source read and its cache finalizer.
        submitted.reads
            // Step 5: write the synthetic header/footer and seal the output on a staged worker.
            .thenApplyAsync(stats -> runUncheckedAsTaskPoolThread(
                () -> combine(subtask, submitted.output, stats)), pools.executor())
            // Step 6: after the full terminal barrier, either transfer the result to the task
            // thread or reclaim the still-owned output and preserve the original failure.
            .handle((result, error) -> finishSubtask(submitted.output, result, error)));

    return new SubtaskFutures(nextSubmission, completion);
  }

  private Completion finishSubtask(
      StagedParquetOutput output,
      Completion completion,
      Throwable error) {
    if (error != null) {
      Throwable failure = unwrap(error);
      try {
        output.close();
      } catch (Throwable closeError) {
        if (closeError != failure) {
          failure.addSuppressed(closeError);
        }
        if (closed.get()) {
          recordAsynchronousCleanupFailure(closeError);
        }
      }
      throw new CompletionException(failure);
    }
    if (closed.get()) {
      closeCompletionAfterAsync(completion);
      throw new CompletionException(
          new CancellationException("staged reader closed before result publication"));
    }
    return completion;
  }

  /**
   * Submit every source's async vectored read in deterministic order.
   *
   * <p>Calls to {@code readVectoredAsync} occur sequentially on one staged worker, but their
   * returned futures run concurrently in PerfIO. This keeps each task's request order stable
   * without occupying one worker per source.</p>
   */
  private SubmittedIo startSourceReads(
      ReadSubtask subtask,
      StagedParquetOutput output) {
    // FooterResult represents one scan occurrence. Two occurrences may compare equal at the
    // Iceberg file level while carrying different filters or post-processing state, so grouping
    // must use identity. Keep first-seen order separately because IdentityHashMap is unordered.
    Map<FooterResult, List<PlannedReadRange>> rangesBySource = new IdentityHashMap<>();
    List<FooterResult> sourceOrder = new ArrayList<>();
    for (PlannedReadRange range : subtask.getRanges()) {
      FooterResult footer = range.getFooter();
      List<PlannedReadRange> sourceRanges = rangesBySource.get(footer);
      if (sourceRanges == null) {
        sourceRanges = new ArrayList<>();
        rangesBySource.put(footer, sourceRanges);
        sourceOrder.add(footer);
      }
      sourceRanges.add(range);
    }

    List<CompletableFuture<SourceReadStats>> sourceFutures = new ArrayList<>();
    for (FooterResult footer : sourceOrder) {
      if (closed.get()) {
        sourceFutures.add(failedFuture(new CancellationException(
            "staged reader closed before source I/O submission")));
        break;
      }
      sourceFutures.add(readSourceRangesAsync(
          output, footer, rangesBySource.get(footer)));
    }

    CompletableFuture<?>[] allSourceFutures =
        sourceFutures.toArray(new CompletableFuture<?>[sourceFutures.size()]);
    CompletableFuture<SourceReadStats> allReads =
        CompletableFuture.allOf(allSourceFutures).thenApply(ignored -> {
          SourceReadStats total = SourceReadStats.EMPTY;
          for (CompletableFuture<SourceReadStats> sourceFuture : sourceFutures) {
            total = total.add(sourceFuture.join());
          }
          return total;
        });
    return new SubmittedIo(output, allReads);
  }

  private StagedParquetOutput createOutput(ReadSubtask subtask) throws Exception {
    checkOpen();
    StagedParquetOutput output = StagedParquetOutput.create(
        subtask.getTotalSizeBytes(), taskAttemptId, subtask.getSubtaskId());
    if (closed.get()) {
      output.close();
      throw new CancellationException("staged reader closed before I/O started");
    }
    return output;
  }

  private CompletableFuture<SourceReadStats> readSourceRangesAsync(
      StagedParquetOutput output,
      FooterResult footer,
      List<PlannedReadRange> ranges) {
    long start = System.nanoTime();
    long cacheHitCount = 0L;
    long cacheHitBytes = 0L;
    long cacheMissCount = 0L;
    long cacheMissBytes = 0L;
    long cacheReadNanos = 0L;
    List<PlannedReadRange> remoteRanges = new ArrayList<>();
    Map<PlannedReadRange, FileCacheStartedToken> cacheTokens = new IdentityHashMap<>();
    try {
      checkOpen();
      RapidsInputFile input = adapter.openInputFile(footer.getFile());
      if (input == null) {
        throw new IllegalStateException("input-file adapter returned null");
      }
      FileCache fileCache = FileCache.get();
      for (PlannedReadRange range : ranges) {
        checkOpen();
        Option<SeekableByteChannel> cached = fileCache.getDataRangeChannel(
            input, range.getInputOffset(), range.getLength());
        if (cached.isDefined()) {
          long cacheStart = System.nanoTime();
          try (SeekableByteChannel channel = cached.get()) {
            output.copyCachedRange(
                channel, range.getOutputOffset(), range.getLength());
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

      final long finalCacheHitCount = cacheHitCount;
      final long finalCacheHitBytes = cacheHitBytes;
      final long finalCacheMissCount = cacheMissCount;
      final long finalCacheMissBytes = cacheMissBytes;
      final long finalCacheReadNanos = cacheReadNanos;

      CompletableFuture<SourceReadStats> completion =
          output.copyRangesAsync(input, remoteRanges)
              .thenApplyAsync(ignored -> runUncheckedAsTaskPoolThread(() -> {
                checkOpen();
                for (PlannedReadRange range : remoteRanges) {
                  FileCacheStartedToken token = cacheTokens.get(range);
                  if (token != null) {
                    HostMemoryBuffer data = output.sliceForCache(
                        range.getOutputOffset(), range.getLength());
                    // Keep the token cancellable until the owning slice exists. complete()
                    // consumes the HMB before it queues cache work, so remove the token immediately
                    // before handing over ownership.
                    cacheTokens.remove(range);
                    token.complete(data);
                  }
                }
                return new SourceReadStats(System.nanoTime() - start,
                    finalCacheHitCount, finalCacheHitBytes,
                    finalCacheMissCount, finalCacheMissBytes, finalCacheReadNanos);
              }), pools.executor())
              .handle((stats, error) -> {
                Throwable failure = error == null ? null : unwrap(error);
                failure = cancelCacheTokens(cacheTokens, failure);
                if (failure != null) {
                  throw new CompletionException(failure);
                }
                return stats;
              });
      return completion;
    } catch (Throwable submissionError) {
      Throwable failure = cancelCacheTokens(cacheTokens, submissionError);
      return failedFuture(failure);
    }
  }

  /** Cancel every token still owned by this source and preserve the primary failure. */
  private static Throwable cancelCacheTokens(
      Map<PlannedReadRange, FileCacheStartedToken> cacheTokens,
      Throwable primaryFailure) {
    Throwable failure = primaryFailure;
    for (FileCacheStartedToken token : cacheTokens.values()) {
      try {
        token.cancel();
      } catch (Throwable cleanupError) {
        failure = addFailure(failure, cleanupError);
      }
    }
    cacheTokens.clear();
    return failure;
  }

  private Completion combine(
      ReadSubtask subtask,
      StagedParquetOutput output,
      SourceReadStats ioStats) throws Exception {
    long start = System.nanoTime();
    try {
      checkOpen();
      output.writeBytes(0L, subtask.getHeaderBytes());
      output.writeBytes(subtask.getFooterOffset(), subtask.getFooterAndTrailerBytes());
      output.seal();
      long combineNanos = System.nanoTime() - start;

      Completion completion = Completion.success(subtask, output,
          new SubtaskStats(ioStats.ioNanos, combineNanos, output.isDiskBacked(),
              ioStats.cacheHitCount, ioStats.cacheHitBytes,
              ioStats.cacheMissCount, ioStats.cacheMissBytes, ioStats.cacheReadNanos));
      if (closed.get()) {
        throw new CancellationException("staged read was cancelled before publication");
      }
      return completion;
    } catch (Throwable error) {
      try {
        output.close();
      } catch (Throwable closeError) {
        error.addSuppressed(closeError);
      }
      throw error;
    }
  }

  private Iterator<ColumnarBatch> decode(Completion completion) throws Exception {
    ReadSubtask subtask = completion.subtask;
    HostMemoryBuffer materialized = null;
    try {
      // The completion-wait boundary has already yielded the semaphore, so everything through
      // host materialization runs without it. decodeAndPostProcess owns the initial acquire
      // immediately before cuDF; next() reacquires after an inter-batch yield.
      adapter.onSubtaskCompleted(subtask, completion.stats);
      long materializeStart = System.nanoTime();
      materialized = completion.output.materialize();
      long materializeEnd = System.nanoTime();
      long materializationNanos = materializeEnd - materializeStart;
      adapter.onMaterializationCompleted(materializationNanos);
      // materialize() returns an independent reference, so the spillable buffer or local file can
      // be released before the task thread enters GPU decode.
      completion.close();
      HostMemoryBuffer transferred = materialized;
      materialized = null; // ownership transfers when the adapter is invoked, even on failure
      Iterator<ColumnarBatch> decoded = adapter.decodeAndPostProcess(
          subtask, transferred);
      if (decoded == null) {
        throw new IllegalStateException("decode adapter returned null");
      }
      return decoded;
    } finally {
      completion.close();
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
      // combine() may already have transferred its output into a Completion. Close any
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

  private TimedFooter getFooterFuture(Future<TimedFooter> future) throws Exception {
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

  /** Wait for the next planned result or return promptly when close wins the race. */
  private Completion waitForResult(CompletableFuture<Completion> resultFuture) throws Exception {
    try {
      CompletableFuture.anyOf(resultFuture, closeSignal).get();
      checkOpen();
      return resultFuture.get();
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

  private static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

  private void closeCompletionAfterAsync(Completion completion) {
    try {
      completion.close();
    } catch (Throwable error) {
      recordAsynchronousCleanupFailure(error);
    }
  }

  private void recordAsynchronousCleanupFailure(Throwable error) {
    Throwable first = asynchronousCleanupFailure.get();
    if (first == null && asynchronousCleanupFailure.compareAndSet(null, error)) {
      return;
    }
    first = asynchronousCleanupFailure.get();
    if (first != error) {
      synchronized (first) {
        first.addSuppressed(error);
      }
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
    // Wake a task thread waiting on the next ordered completion without cancelling any writer.
    closeSignal.complete(null);
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
    List<CompletableFuture<Completion>> resultsToClose;
    synchronized (lifecycleLock) {
      resultsToClose = new ArrayList<>(pendingResults);
      pendingResults.clear();
    }
    // Footer futures are intentionally left running. Their results contain immutable metadata
    // and no closeable resources. Future cancellation can mark a running FutureTask complete
    // before its callable exits, so dropping our references after setting closed is safer.
    // Data futures are likewise left terminal-barrier-driven: active SDK writers finish before
    // their future can publish a Completion. Attach cleanup to every ordered result instead of
    // cancelling it, which could otherwise reclaim an output while an async write is still live.
    for (CompletableFuture<Completion> resultFuture : resultsToClose) {
      resultFuture.whenComplete((completion, error) -> {
        if (completion != null) {
          closeCompletionAfterAsync(completion);
        }
      });
    }
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
    if (first != next) {
      first.addSuppressed(next);
    }
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

    private SourceReadStats add(SourceReadStats other) {
      return new SourceReadStats(
          Math.addExact(ioNanos, other.ioNanos),
          Math.addExact(cacheHitCount, other.cacheHitCount),
          Math.addExact(cacheHitBytes, other.cacheHitBytes),
          Math.addExact(cacheMissCount, other.cacheMissCount),
          Math.addExact(cacheMissBytes, other.cacheMissBytes),
          Math.addExact(cacheReadNanos, other.cacheReadNanos));
    }
  }

  /** Footer result paired with worker elapsed time for task-thread metric publication. */
  private static final class TimedFooter {
    private final FooterResult footer;
    private final long footerNanos;

    private TimedFooter(
        FooterResult footer,
        long footerNanos) {
      this.footer = footer;
      this.footerNanos = footerNanos;
    }
  }

  /** The two distinct ordering points produced by {@link #submit}. */
  private static final class SubtaskFutures {
    private final CompletableFuture<Void> ioSubmitted;
    private final CompletableFuture<Completion> completion;

    private SubtaskFutures(
        CompletableFuture<Void> ioSubmitted,
        CompletableFuture<Completion> completion) {
      this.ioSubmitted = ioSubmitted;
      this.completion = completion;
    }
  }

  /** Output ownership plus the terminal barrier for all reads submitted into that output. */
  private final class SubmittedIo implements AutoCloseable {
    private final StagedParquetOutput output;
    private final CompletableFuture<SourceReadStats> reads;

    private SubmittedIo(
        StagedParquetOutput output,
        CompletableFuture<SourceReadStats> reads) {
      this.output = output;
      this.reads = reads;
    }

    /**
     * Reclaim an output returned by the operation if task-context/RMM cleanup then fails.
     * Cleanup remains behind the read barrier and therefore cannot race an accepted SDK writer.
     */
    @Override
    public void close() {
      reads.whenComplete((ignored, error) -> {
        try {
          output.close();
        } catch (Throwable closeError) {
          recordAsynchronousCleanupFailure(closeError);
        }
      });
    }
  }

  /** Sole owner of one successfully sealed subtask output. */
  private static final class Completion implements AutoCloseable {
    private final ReadSubtask subtask;
    private final StagedParquetOutput output;
    private final SubtaskStats stats;
    private boolean closed;

    private Completion(
        ReadSubtask subtask,
        StagedParquetOutput output,
        SubtaskStats stats) {
      this.subtask = subtask;
      this.output = output;
      this.stats = stats;
    }

    private static Completion success(
        ReadSubtask subtask,
        StagedParquetOutput output,
        SubtaskStats stats) {
      return new Completion(Objects.requireNonNull(subtask, "subtask"),
          Objects.requireNonNull(output, "output"),
          Objects.requireNonNull(stats, "stats"));
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        output.close();
      }
    }
  }
}
