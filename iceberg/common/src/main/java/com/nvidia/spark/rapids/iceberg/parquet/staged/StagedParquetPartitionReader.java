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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
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
 *   <li>Only a configured number of whole subtasks are admitted across the executor. All source
 *       files and column chunks in each admitted subtask still read concurrently into one
 *       preallocated output.</li>
 *   <li>When the task thread takes a completed subtask it starts the next one before GPU decode,
 *       overlapping I/O with decode without filling S3 with requests for the whole partition.</li>
 * </ol>
 *
 * <p>A {@link SubtaskExecution} is the single mutable ownership cell for the current output. Its
 * atomic handoff guarantees that task cancellation, worker failure, and successful publication
 * cannot close or leak the same output.</p>
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
  // The queue contains at most the one admitted subtask's terminal result (plus a close sentinel).
  private final BlockingQueue<Completion> completions = new LinkedBlockingQueue<>();
  private final Deque<ReadSubtask> pendingSubtasks = new ArrayDeque<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> asynchronousCleanupFailure = new AtomicReference<>();

  private boolean initialized;
  private int remainingResults;
  // Guarded by lifecycleLock. This is the reader's one queued, running, or ready subtask.
  private SubtaskExecution activeExecution;
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
   * @param maxConcurrentSubtasks maximum data-read subtasks admitted across this executor
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
      int maxConcurrentSubtasks,
      TaskContext taskContext) {
    this.files = immutableFileCopy(files);
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    this.planner = new StableGreedyReadPlanner(
        maxRows, maxEstimatedGpuBytes, targetParquetBytes, expectedSparkSchema);
    this.pools = StagedScanThreadPools.getOrCreate(workerThreads, maxConcurrentSubtasks);
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

        Completion completion;
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
          synchronized (lifecycleLock) {
            if (activeExecution != completion.execution) {
              if (closed.get()) {
                return false;
              }
              throw new IllegalStateException("completion does not belong to the active subtask");
            }
            activeExecution = null;
          }
          if (completion.error != null) {
            throw propagate(completion.error);
          }
          // Admit exactly one successor before decoding. It can read while the task thread uses
          // the GPU, but no third subtask can start until this successor is taken here.
          startNextSubtask();
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
          // already been removed from the completion queue.
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

  /** Submit all footers, plan on the task thread, and admit only the first subtask. */
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

    List<ReadSubtask> plan;
    synchronized (lifecycleLock) {
      checkOpen();
      plan = planner.plan(footers);
      pendingSubtasks.addAll(plan);
      remainingResults = pendingSubtasks.size();
      initialized = true;
    }
    startNextSubtask();
  }

  /**
   * Admit the next planned subtask from this reader.
   *
   * <p>The active execution remains non-null after publication, so a ready result still consumes
   * this reader's single admission slot. The task thread clears it only after taking the result.
   * This bounds running plus ready staged outputs to one per reader.</p>
   */
  private void startNextSubtask() throws Exception {
    SubtaskExecution execution;
    synchronized (lifecycleLock) {
      checkOpen();
      if (activeExecution != null || pendingSubtasks.isEmpty()) {
        return;
      }
      ReadSubtask subtask = pendingSubtasks.removeFirst();
      execution = new SubtaskExecution(subtask);
      activeExecution = execution;
    }
    try {
      StagedScanThreadPools.SubtaskAdmission admission = pools.admitSubtask(
          admitted -> {
            execution.admission = admitted;
            try {
              checkOpen();
              submit(execution);
            } catch (Throwable error) {
              try {
                publishStartupFailure(execution, error);
              } finally {
                admitted.complete();
              }
            }
          });
      execution.admission = admission;
      if (closed.get()) {
        admission.cancel();
      }
    } catch (Throwable error) {
      synchronized (lifecycleLock) {
        if (activeExecution == execution) {
          activeExecution = null;
        }
      }
      throw error;
    }
  }

  private void submit(SubtaskExecution execution) {
    // startSubtask runs allocation and source submission on the same worker. Flattening the future
    // it returns keeps this high-level chain readable without adding a same-pool queue hop between
    // allocation and the same subtask's reads.
    CompletableFuture
        .supplyAsync(() -> startSubtask(execution), pools.executor())
        .thenCompose(completionFuture -> completionFuture)
        .whenComplete((completion, error) -> finishSubtask(execution, completion, error));
  }

  /** Allocate the output, submit every source, and attach combine while on a pool worker. */
  private CompletableFuture<Completion> startSubtask(SubtaskExecution execution) {
    // Only allocation is wrapped here. In particular, an empty projection may combine inline;
    // combine installs its own task context/RMM registration after allocation has unregistered.
    StagedParquetOutput output = runUncheckedAsTaskPoolThread(
        () -> createOutput(execution));
    return startSourceReads(execution, output).thenApply(stats ->
        // This continuation always runs on a pool worker. Fast or empty reads may make that this
        // allocation worker; otherwise it is the worker that completes the source barrier.
        runUncheckedAsTaskPoolThread(() -> combine(execution, output, stats)));
  }

  /** Publish one terminal subtask result and release its executor-wide admission. */
  private void finishSubtask(
      SubtaskExecution execution,
      Completion completion,
      Throwable error) {
    try {
      if (error != null) {
        Throwable failure = unwrap(error);
        // The I/O worker may have installed an output before the combine callable could start.
        // Examples include executor rejection and task-context/RMM registration failures. This
        // callback runs only after the whole dependent stage is terminal, so it is the first
        // cancellation-safe place outside a worker to reclaim that still-owned output.
        try {
          execution.closeOutputAfterStage();
        } catch (Throwable closeError) {
          if (closeError != failure) {
            failure.addSuppressed(closeError);
          }
          if (closed.get()) {
            recordAsynchronousCleanupFailure(closeError);
          }
        }
        if (!closed.get()) {
          completions.offer(Completion.failure(execution, failure));
        }
      } else if (closed.get()) {
        if (completion != null) {
          closeCompletionAfterAsync(completion);
        }
      } else {
        completions.offer(completion);
        // close() can race between the first closed check and queue publication. If it wins,
        // remove and close the just-published result so close() cannot miss it while draining.
        if (closed.get() && completions.remove(completion)) {
          closeCompletionAfterAsync(completion);
        }
      }
    } finally {
      // Release only after all writers and finalization are terminal. The admission queue can
      // now start every source in one later subtask without interleaving its requests here.
      execution.completeAdmission();
    }
  }

  /*
   * Initial supplyAsync rejection happens before a completion chain exists, so the admission
   * starter catches it and calls this method. Failures after acceptance flow through
   * finishSubtask instead.
   */
  private void publishStartupFailure(SubtaskExecution execution, Throwable error) {
    Throwable failure = unwrap(error);
    try {
      execution.closeOutputAfterStage();
    } catch (Throwable closeError) {
      if (closeError != failure) {
        failure.addSuppressed(closeError);
      }
      if (closed.get()) {
        recordAsynchronousCleanupFailure(closeError);
      }
    }
    if (!closed.get()) {
      completions.offer(Completion.failure(execution, failure));
    }
  }

  /**
   * Submit one shared-pool job per source file and return their aggregate barrier.
   *
   * <p>Each source receives at most one {@code readVectored} call containing a distinct range for
   * every cache-miss Parquet column chunk; cache-hit chunks are copied directly from their cached
   * channels. This preserves both levels of remote-I/O concurrency: source files run as independent
   * pool jobs and PerfIO runs the miss ranges within each source concurrently. The exact-sized
   * output is already allocated before these jobs are submitted, so no source worker waits for a
   * sibling to initialize shared state. All futures and their dependent finalizer are installed
   * before these jobs are submitted. This guarantees the last source worker writes the synthetic
   * header/footer and seals the output immediately after every writer is terminal.</p>
   */
  private CompletableFuture<SourceReadStats> startSourceReads(
      SubtaskExecution execution,
      StagedParquetOutput output) {
    // FooterResult represents one scan occurrence. Two occurrences may compare equal at the
    // Iceberg file level while carrying different filters or post-processing state, so grouping
    // must use identity. Keep first-seen order separately because IdentityHashMap is unordered.
    Map<FooterResult, List<PlannedReadRange>> rangesBySource = new IdentityHashMap<>();
    List<FooterResult> sourceOrder = new ArrayList<>();
    for (PlannedReadRange range : execution.subtask.getRanges()) {
      FooterResult footer = range.getFooter();
      List<PlannedReadRange> sourceRanges = rangesBySource.get(footer);
      if (sourceRanges == null) {
        sourceRanges = new ArrayList<>();
        rangesBySource.put(footer, sourceRanges);
        sourceOrder.add(footer);
      }
      sourceRanges.add(range);
    }

    List<Callable<SourceReadStats>> sourceJobs = new ArrayList<>();
    for (FooterResult footer : sourceOrder) {
      List<PlannedReadRange> sourceRanges = rangesBySource.get(footer);
      sourceJobs.add(() -> readSourceRanges(output, footer, sourceRanges));
    }

    List<CompletableFuture<SourceReadStats>> sourceFutures = new ArrayList<>();
    for (int index = 0; index < sourceJobs.size(); index++) {
      sourceFutures.add(new CompletableFuture<>());
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

    // The allOf barrier above must exist before the first job is submitted. Otherwise a fast last
    // writer could finish before the combine continuation is attached.
    for (int index = 0; index < sourceJobs.size(); index++) {
      if (closed.get()) {
        completeUnsubmittedSourceJobs(sourceFutures, index,
            new CancellationException("staged reader closed before source I/O submission"));
        return allReads;
      }
      final int jobIndex = index;
      try {
        pools.executor().execute(() -> {
          CompletableFuture<SourceReadStats> future = sourceFutures.get(jobIndex);
          try {
            SourceReadStats result = runUncheckedAsTaskPoolThread(
                sourceJobs.get(jobIndex));
            future.complete(result);
          } catch (Throwable sourceError) {
            future.completeExceptionally(sourceError);
          }
        });
      } catch (Throwable submissionError) {
        // Already-submitted writers remain part of allOf. Marking only unsubmitted work terminal
        // preserves the barrier and prevents failure cleanup from racing active HMB/file writes.
        completeUnsubmittedSourceJobs(sourceFutures, index, submissionError);
        return allReads;
      }
    }
    return allReads;
  }

  private static void completeUnsubmittedSourceJobs(
      List<CompletableFuture<SourceReadStats>> futures,
      int firstUnsubmitted,
      Throwable error) {
    for (int index = firstUnsubmitted; index < futures.size(); index++) {
      futures.get(index).completeExceptionally(error);
    }
  }

  private StagedParquetOutput createOutput(SubtaskExecution execution) throws Exception {
    checkOpen();
    ReadSubtask subtask = execution.subtask;
    StagedParquetOutput output = StagedParquetOutput.create(
        subtask.getTotalSizeBytes(), taskAttemptId, subtask.getSubtaskId());
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
      FooterResult footer,
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

      output.copyRanges(input, remoteRanges);
      for (PlannedReadRange range : remoteRanges) {
        FileCacheStartedToken token = cacheTokens.get(range);
        if (token != null) {
          HostMemoryBuffer data = output.sliceForCache(
              range.getOutputOffset(), range.getLength());
          cacheTokens.remove(range);
          // complete() consumes the HMB and marks the token finished before it queues cache work.
          // Ownership therefore transfers even if complete() itself throws; closing/cancelling
          // here would double-release the buffer and can mask the original cache failure.
          token.complete(data);
        }
      }
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

  private Completion combine(
      SubtaskExecution execution,
      StagedParquetOutput output,
      SourceReadStats ioStats) throws Exception {
    ReadSubtask subtask = execution.subtask;
    long start = System.nanoTime();
    try {
      checkOpen();
      output.writeBytes(0L, subtask.getHeaderBytes());
      output.writeBytes(subtask.getFooterOffset(), subtask.getFooterAndTrailerBytes());
      output.seal();
      long combineNanos = System.nanoTime() - start;

      // Move ownership out of the cancellation cell before publishing the immutable result.
      if (!execution.output.compareAndSet(output, null)) {
        throw new CancellationException("staged read was cancelled while combining");
      }
      Completion completion = null;
      try {
        completion = Completion.success(execution, output,
            new SubtaskStats(ioStats.ioNanos, combineNanos, output.isDiskBacked(),
                ioStats.cacheHitCount, ioStats.cacheHitBytes,
                ioStats.cacheMissCount, ioStats.cacheMissBytes, ioStats.cacheReadNanos));
        if (closed.get()) {
          CancellationException cancellation =
              new CancellationException("staged read was cancelled before publication");
          try {
            completion.close();
          } catch (Throwable closeError) {
            cancellation.addSuppressed(closeError);
          }
          throw cancellation;
        }
        return completion;
      } catch (Throwable error) {
        if (completion == null) {
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

  private Iterator<ColumnarBatch> decode(Completion completion) throws Exception {
    ReadSubtask subtask = completion.execution.subtask;
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
    SubtaskExecution executionToCancel;
    synchronized (lifecycleLock) {
      pendingSubtasks.clear();
      executionToCancel = activeExecution;
    }
    if (executionToCancel != null) {
      executionToCancel.cancelWaitingAdmission();
    }
    // Footer futures are intentionally left running. Their results contain immutable metadata
    // and no closeable resources. Future cancellation can mark a running FutureTask complete
    // before its callable exits, so dropping our references after setting closed is safer.
    // Source futures are likewise left terminal-barrier-driven: queued jobs observe closed before
    // allocating/reading, active writers finish, and only then may the callback close their shared
    // output. Cancelling allOf could otherwise close an output while supplyAsync is still writing.
    // Leave failure/sentinel entries in the queue so a concurrently blocked hasNext stays
    // unblocked. Only successful entries own resources and need removal during cleanup.
    for (Completion completion : completions) {
      if (completion.output != null && completions.remove(completion)) {
        try {
          completion.close();
        } catch (Throwable error) {
          failure = addFailure(failure, error);
        }
      }
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

  /**
   * Mutable lifecycle state for one otherwise immutable subtask.
   *
   * <p>{@code output} is installed once by the allocation job and remains non-null only while
   * source I/O or combination owns the shared output. Successful combination atomically removes
   * it before constructing
   * the completion queue; terminal-stage cleanup atomically removes and closes it. Exactly one
   * ownership path can win.</p>
   */
  private static final class SubtaskExecution {
    private final ReadSubtask subtask;
    private final AtomicReference<StagedParquetOutput> output = new AtomicReference<>();
    private volatile StagedScanThreadPools.SubtaskAdmission admission;

    private SubtaskExecution(ReadSubtask subtask) {
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

    private void completeAdmission() {
      StagedScanThreadPools.SubtaskAdmission current = admission;
      if (current != null) {
        current.complete();
      }
    }

    private void cancelWaitingAdmission() {
      StagedScanThreadPools.SubtaskAdmission current = admission;
      if (current != null) {
        current.cancel();
      }
    }

  }

  /** Queue value and sole owner of a successfully sealed subtask output. */
  private static final class Completion implements AutoCloseable {
    private final SubtaskExecution execution;
    private final StagedParquetOutput output;
    private final SubtaskStats stats;
    private final Throwable error;
    private boolean closed;

    private Completion(
        SubtaskExecution execution,
        StagedParquetOutput output,
        SubtaskStats stats,
        Throwable error) {
      this.execution = execution;
      this.output = output;
      this.stats = stats;
      this.error = error;
    }

    private static Completion success(
        SubtaskExecution execution,
        StagedParquetOutput output,
        SubtaskStats stats) {
      return new Completion(Objects.requireNonNull(execution, "execution"),
          Objects.requireNonNull(output, "output"),
          Objects.requireNonNull(stats, "stats"), null);
    }

    private static Completion failure(
        SubtaskExecution execution,
        Throwable error) {
      return new Completion(execution, null, null, Objects.requireNonNull(error, "error"));
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        if (output != null) {
          output.close();
        }
      }
    }
  }
}
