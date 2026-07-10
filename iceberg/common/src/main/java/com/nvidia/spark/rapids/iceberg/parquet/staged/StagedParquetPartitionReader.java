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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 *   <li>Each footer job feeds the shared incremental planning session as it completes,
 *       serialized on the session, and submits every closed subtask immediately. A slow footer
 *       never stalls planning or data I/O for files whose footers already resolved, and the task
 *       thread never waits for footers: it claims completed results while planning is still in
 *       flight.</li>
 *   <li>Every planned subtask is enqueued to the shared pool in plan order, but each subtask
 *       executes as one blocking worker job: allocate the exact non-pinned output, copy cache
 *       hits, hand all cache-miss column-chunk reads to the async I/O engine, block until they
 *       finish, publish cache slices, and write/seal the synthetic file into a spillable host
 *       buffer. Publishing the completion finishes the subtask and frees the worker slot, so
 *       pool occupancy bounds the actively executing subtasks — the same natural pacing the
 *       non-staged multithreaded reader gets from its blocking per-file reads — while completed
 *       results wait for decode under normal host spilling.</li>
 *   <li>The task thread feeds completed synthetic files to GPU decode in completion order; a
 *       later subtask is never held behind an earlier subtask that is slow to finish.</li>
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
  // Guarded by lifecycleLock. Tracks every result not yet claimed by the task thread so close()
  // can reclaim outputs regardless of completion order.
  private final Set<CompletableFuture<Completion>> pendingResults = new HashSet<>();
  // Futures are added by their completion threads and consumed by the Spark task thread. Keeping
  // this separate from pendingResults preserves actual completion order instead of plan order.
  private final BlockingQueue<CompletableFuture<Completion>> completedResults =
      new LinkedBlockingQueue<>();
  // A queue sentinel used solely to wake a task thread blocked in waitForNextCompletedResult().
  private final CompletableFuture<Completion> closeWakeup = new CompletableFuture<>();
  // A queue sentinel that makes a blocked task thread re-check planning progress and failures.
  private final CompletableFuture<Completion> planningWakeup = new CompletableFuture<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> asynchronousCleanupFailure = new AtomicReference<>();
  private final AtomicReference<Throwable> planningFailure = new AtomicReference<>();
  private final AtomicInteger pendingFooters = new AtomicInteger();

  // Fed by footer-completion continuations on pool threads; all access synchronizes on the
  // session object itself.
  private StableGreedyReadPlanner.Session session;
  private boolean initialized;
  // Guarded by lifecycleLock together with remainingResults: the claim loop is terminal only
  // when planning has completed and every registered result has been claimed.
  private boolean planningComplete;
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
   * @param workerThreads executor-wide worker count shared by footer and blocking subtask jobs;
   *                      this is also the executor-wide bound on in-flight subtasks
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
        Throwable failure = planningFailure.get();
        if (failure != null) {
          throw propagate(failure);
        }
        boolean terminal;
        synchronized (lifecycleLock) {
          terminal = planningComplete && remainingResults == 0;
        }
        if (terminal) {
          if (!semaphoreReleasedForAdvance) {
            releaseGpuSemaphoreFromTaskThread();
          }
          return false;
        }

        long waitStart = System.nanoTime();
        if (!semaphoreReleasedForAdvance) {
          releaseGpuSemaphoreFromTaskThread();
        }
        CompletableFuture<Completion> resultFuture = waitForNextCompletedResult();
        if (resultFuture == planningWakeup) {
          adapter.onResultWait(System.nanoTime() - waitStart);
          continue;
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
            if (!pendingResults.remove(resultFuture)) {
              if (closed.get()) {
                return false;
              }
              throw new IllegalStateException("completed staged result is not pending");
            }
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

  /** Submit all footer jobs; each drives incremental planning as it completes. */
  private void initializeIfNeeded() {
    if (initialized) {
      return;
    }
    checkOpen();
    // Footer fetch/filter runs on the shared pool and planning runs on whichever pool thread
    // completes each footer, so the task thread never waits here: it proceeds straight to
    // claiming completed results, and GPU decode overlaps both footer I/O and planning.
    releaseGpuSemaphoreFromTaskThread();
    session = planner.newSession();
    pendingFooters.set(files.size());
    initialized = true;
    if (files.isEmpty()) {
      finishPlanning();
      return;
    }
    for (IcebergPartitionedFile file : files) {
      checkOpen();
      pools.executor().submit(() -> runFooterJob(file));
    }
  }

  /**
   * Fetch one footer and, on completion, feed it to the shared planning session.
   *
   * <p>Footers reach the session in whichever order they complete, so one slow footer never
   * stalls the planning and data I/O of files whose footers already resolved. Any feed order is
   * correct: within one footer, row groups keep their file order, and compatibility rules make
   * every resulting combination a valid synthetic file. The subtask grouping is therefore
   * arrival-dependent rather than stable. Synchronizing on the session serializes concurrent
   * footer completions, including the per-task footer metric they publish.</p>
   */
  private void runFooterJob(IcebergPartitionedFile file) {
    TimedFooter timedFooter;
    try {
      timedFooter = runAsTaskPoolThread(() -> {
        long start = System.nanoTime();
        FooterResult footer = adapter.readAndFilterFooter(file);
        if (footer == null) {
          throw new IllegalStateException("footer adapter returned null");
        }
        return new TimedFooter(footer, System.nanoTime() - start);
      });
    } catch (Throwable error) {
      recordPlanningFailure(error);
      footerDone();
      return;
    }
    try {
      synchronized (session) {
        adapter.onFooterCompleted(timedFooter.footerNanos);
        if (!closed.get() && planningFailure.get() == null) {
          for (ReadSubtask subtask : session.add(timedFooter.footer)) {
            submitSubtask(subtask);
          }
        }
      }
    } catch (Throwable error) {
      recordPlanningFailure(error);
    } finally {
      footerDone();
    }
  }

  /** Record the first planning failure and wake the task thread so it can surface it. */
  private void recordPlanningFailure(Throwable error) {
    if (closed.get() && unwrap(error) instanceof CancellationException) {
      // An expected consequence of close racing footer work, not a planning failure.
      return;
    }
    planningFailure.compareAndSet(null, unwrap(error));
    completedResults.offer(planningWakeup);
  }

  /** Flush the final subtask when the last footer resolves, then wake the task thread. */
  private void footerDone() {
    if (pendingFooters.decrementAndGet() == 0) {
      finishPlanning();
    }
  }

  private void finishPlanning() {
    try {
      synchronized (session) {
        if (!closed.get() && planningFailure.get() == null) {
          for (ReadSubtask subtask : session.finish()) {
            submitSubtask(subtask);
          }
        }
      }
    } catch (Throwable error) {
      recordPlanningFailure(error);
    } finally {
      synchronized (lifecycleLock) {
        planningComplete = true;
      }
      // Wake a task thread blocked on the completion queue so it re-checks the terminal state.
      completedResults.offer(planningWakeup);
    }
  }

  /** Register one planned subtask and hand it to a blocking worker. */
  private void submitSubtask(ReadSubtask subtask) {
    CompletableFuture<Completion> completionFuture = new CompletableFuture<>();
    synchronized (lifecycleLock) {
      checkOpen();
      pendingResults.add(completionFuture);
      remainingResults += 1;
    }
    // Record terminal futures in the order they actually finish. whenComplete runs immediately
    // if a very small subtask completed before this callback could be installed.
    completionFuture.whenComplete(
        (completion, error) -> completedResults.offer(completionFuture));
    try {
      pools.executor().submit(() -> runWorker(subtask, completionFuture));
    } catch (Throwable submitError) {
      // Surface a rejected submission through the normal claim path so the registered result
      // can never strand the task thread, then abort initialization.
      completionFuture.completeExceptionally(submitError);
      throw submitError;
    }
  }

  /**
   * Execute one subtask end-to-end on a shared worker.
   *
   * <p>The subtask counts as finished once its completion is published: the sealed output is
   * already spillable at that point, so it waits for decode in the completion queue under normal
   * host-memory management while this worker slot immediately starts the next queued subtask.
   * Worker occupancy therefore bounds the actively executing subtasks, not the decode backlog.</p>
   */
  private void runWorker(ReadSubtask subtask, CompletableFuture<Completion> completionFuture) {
    Completion completion;
    try {
      completion = runAsTaskPoolThread(() -> runSubtask(subtask));
    } catch (Throwable error) {
      completionFuture.completeExceptionally(error);
      return;
    }
    if (!completionFuture.complete(completion)) {
      // The public future contract is internal; this indicates a bug rather than a race.
      closeCompletionAfterAsync(completion);
    }
  }

  /**
   * Run one subtask's blocking pipeline: allocate, copy cache hits, submit async cache-miss
   * reads in deterministic order, block until every accepted read is terminal, publish cache
   * slices, and combine/seal the synthetic file.
   */
  private Completion runSubtask(ReadSubtask subtask) throws Exception {
    checkOpen();
    StagedParquetOutput output = createOutput(subtask);
    List<PendingSourceRead> sources = new ArrayList<>();
    try {
      // FooterResult represents one scan occurrence. Two occurrences may compare equal at the
      // Iceberg file level while carrying different filters or post-processing state, so grouping
      // must use identity. Keep first-seen order separately: IdentityHashMap is unordered.
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

      // Copy cache hits and hand every cache-miss range to the async engine, source by source in
      // first-seen order. The returned futures run concurrently in PerfIO; this worker is the
      // only thread that blocks on them.
      for (FooterResult footer : sourceOrder) {
        checkOpen();
        sources.add(beginSourceRead(output, footer, rangesBySource.get(footer)));
      }

      // The pacing point of the blocking model: wait for every accepted read. All sources are
      // drained even after a failure so the output can never be reclaimed under a live writer.
      Throwable readFailure = null;
      for (PendingSourceRead source : sources) {
        readFailure = addFailure(readFailure, source.awaitRead());
      }
      if (readFailure != null) {
        throw propagate(readFailure);
      }
      checkOpen();

      // Publish owning cache slices inline on this worker; continuations must not depend on the
      // shared pool because every one of its threads may be a blocked subtask worker.
      SourceReadStats stats = SourceReadStats.EMPTY;
      for (PendingSourceRead source : sources) {
        stats = stats.add(source.publishToCache(output));
      }
      return combine(subtask, output, stats);
    } catch (Throwable error) {
      Throwable failure = unwrap(error);
      for (PendingSourceRead source : sources) {
        // Accepted writers must become terminal before the output is reclaimed.
        Throwable drainFailure = source.awaitRead();
        if (drainFailure != null && drainFailure != failure) {
          failure.addSuppressed(drainFailure);
        }
        failure = cancelCacheTokens(source.cacheTokens, failure);
      }
      try {
        output.close();
      } catch (Throwable closeError) {
        if (closeError != failure) {
          failure.addSuppressed(closeError);
        }
      }
      throw propagate(failure);
    }
  }

  private StagedParquetOutput createOutput(ReadSubtask subtask) throws Exception {
    checkOpen();
    StagedParquetOutput output = StagedParquetOutput.create(subtask.getTotalSizeBytes());
    if (closed.get()) {
      output.close();
      throw new CancellationException("staged reader closed before I/O started");
    }
    return output;
  }

  /**
   * Copy one source's cache hits and hand its cache-miss ranges to the async I/O engine.
   *
   * <p>Runs on the owning subtask worker. The returned state carries the in-flight read future
   * plus everything needed to publish cache slices inline after the worker's blocking wait.</p>
   */
  private PendingSourceRead beginSourceRead(
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
      CompletableFuture<Void> read = output.copyRangesAsync(input, remoteRanges);
      return new PendingSourceRead(read, remoteRanges, cacheTokens, start,
          cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos);
    } catch (Throwable submissionError) {
      // Nothing was accepted by the async engine for this source, so only its tokens need care.
      throw propagate(cancelCacheTokens(cacheTokens, submissionError));
    }
  }

  /** One source's in-flight read plus the state needed to publish its cache slices. */
  private static final class PendingSourceRead {
    private final CompletableFuture<Void> read;
    private final List<PlannedReadRange> remoteRanges;
    private final Map<PlannedReadRange, FileCacheStartedToken> cacheTokens;
    private final long startNanos;
    private final long cacheHitCount;
    private final long cacheHitBytes;
    private final long cacheMissCount;
    private final long cacheMissBytes;
    private final long cacheReadNanos;

    private PendingSourceRead(
        CompletableFuture<Void> read,
        List<PlannedReadRange> remoteRanges,
        Map<PlannedReadRange, FileCacheStartedToken> cacheTokens,
        long startNanos,
        long cacheHitCount,
        long cacheHitBytes,
        long cacheMissCount,
        long cacheMissBytes,
        long cacheReadNanos) {
      this.read = read;
      this.remoteRanges = remoteRanges;
      this.cacheTokens = cacheTokens;
      this.startNanos = startNanos;
      this.cacheHitCount = cacheHitCount;
      this.cacheHitBytes = cacheHitBytes;
      this.cacheMissCount = cacheMissCount;
      this.cacheMissBytes = cacheMissBytes;
      this.cacheReadNanos = cacheReadNanos;
    }

    /** Block until this source's accepted reads are terminal; null on success. Idempotent. */
    private Throwable awaitRead() {
      try {
        read.join();
        return null;
      } catch (Throwable error) {
        return unwrap(error);
      }
    }

    /** Hand owning cache slices to their tokens and return this source's final measurements. */
    private SourceReadStats publishToCache(StagedParquetOutput output) throws Exception {
      for (PlannedReadRange range : remoteRanges) {
        FileCacheStartedToken token = cacheTokens.get(range);
        if (token != null) {
          HostMemoryBuffer data = output.sliceForCache(
              range.getOutputOffset(), range.getLength());
          // Keep the token cancellable until the owning slice exists. complete() consumes the
          // HMB before it queues cache work, so remove the token immediately before handing
          // over ownership.
          cacheTokens.remove(range);
          token.complete(data);
        }
      }
      return new SourceReadStats(System.nanoTime() - startNanos,
          cacheHitCount, cacheHitBytes,
          cacheMissCount, cacheMissBytes, cacheReadNanos);
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

  /** Wait for whichever subtask finishes next or return promptly when close wins the race. */
  private CompletableFuture<Completion> waitForNextCompletedResult()
      throws InterruptedException {
    CompletableFuture<Completion> resultFuture = completedResults.take();
    checkOpen();
    if (resultFuture == closeWakeup) {
      throw new IllegalStateException("close wakeup observed while reader is open");
    }
    return resultFuture;
  }

  /** Obtain the already-terminal result and preserve its original failure type. */
  private Completion waitForResult(CompletableFuture<Completion> resultFuture) throws Exception {
    try {
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
    // Wake a task thread waiting for the next completion without cancelling any writer.
    completedResults.offer(closeWakeup);
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
    // their future can publish a Completion. Attach cleanup to every pending result instead of
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
