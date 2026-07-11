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
import java.util.Collections;
import java.util.Comparator;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.HostAlloc$;
import com.nvidia.spark.rapids.filecache.FileCache;
import com.nvidia.spark.rapids.filecache.FileCache.FileCacheStartedToken;
import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile;
import com.nvidia.spark.rapids.jni.RmmSpark;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import scala.Option;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.rapids.execution.TrampolineUtil$;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Coordinates the staged Parquet pipeline for one Spark input partition.
 *
 * <p>The pipeline matches the Iceberg multithreaded reader's admission order:</p>
 * <ol>
 *   <li>One fused pool job per file fetches and filters the footer, publishes it mid-job, and
 *       continues straight into that file's blocking download — pool occupancy paces concurrent
 *       file pipelines exactly like the base multithreaded reader. Each download packs its
 *       chunks contiguously into a per-file {@link FileFragment} whose layout comes from the
 *       file's own footer, so data I/O starts without waiting for planning or for any other
 *       file's footer.</li>
 *   <li>With combining enabled, completed file jobs enter a completion queue. The Spark task
 *       thread blocks for the first result, then gives each additional result a fresh combine
 *       wait. With combining disabled it consumes jobs in file-list order. Closing the reader
 *       wakes either kind of wait without cancelling worker futures.</li>
 *   <li>Subtasks are decoded in plan order. After waiting for their constituent fragments, the
 *       task thread presents the synthetic Parquet file as a small header, zero-copy fragment
 *       slices, and a small relocated footer. Completed fragments wait as spillable host buffers
 *       until consumed; there is no second task-sized assembly allocation.</li>
 * </ol>
 */
public final class StagedParquetPartitionReader
    implements Iterator<ColumnarBatch>, AutoCloseable {
  private static final int CLOSED_COMPLETION = -1;
  /**
   * Coalescing policy for cache-miss reads within one file, matching the base multithreaded
   * reader's contiguous-only {@code coalesceReads}: only ranges with a zero source gap merge,
   * and a contiguous run has no artificial maximum span. Gapped columns remain separate ranges
   * that the shared S3 client can fetch in parallel. No gap bytes are downloaded, so the scratch
   * route stays idle.
   */
  static final long COALESCE_GAP_LIMIT_BYTES = 0L;

  private final List<IcebergPartitionedFile> files;
  private final StagedScanAdapter adapter;
  private final StableGreedyReadPlanner planner;
  private final boolean combineEnabled;
  private final long combineWaitMs;
  private final StagedScanThreadPools pools;
  private final TaskContext taskContext;
  private final long taskAttemptId;
  private final Object lifecycleLock = new Object();
  private final Object iteratorLock = new Object();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> asynchronousCleanupFailure = new AtomicReference<>();
  // Completed by close() to wake the task thread from any per-file wait without interfering
  // with the pipeline futures, whose sole completers stay the pool jobs themselves.
  private final CompletableFuture<Void> closeSignal = new CompletableFuture<>();
  // Ordered, file-indexed pipeline futures; guarded by lifecycleLock so close() can snapshot
  // them while initialization is still appending.
  private final List<CompletableFuture<TimedFooter>> footerFutures = new ArrayList<>();
  private final List<CompletableFuture<FileFragment>> fragmentFutures = new ArrayList<>();
  private final BlockingQueue<Integer> completedFileIndexes = new LinkedBlockingQueue<>();

  // Task-thread-only planning and decode-input state.
  private StableGreedyReadPlanner.Session session;
  private boolean initialized;
  private boolean planningFinished;
  private int filesAdmitted;
  private int orderedFileCursor;
  private final ArrayDeque<ReadSubtask> plannedSubtasks = new ArrayDeque<>();
  private final Map<FooterResult, CompletableFuture<FileFragment>> fragmentByFooter =
      new IdentityHashMap<>();
  private final Set<FileFragment> statsAttributed =
      Collections.newSetFromMap(new IdentityHashMap<>());

  private Iterator<ColumnarBatch> currentBatches;

  /**
   * Creates a lazy partition reader. No footer or data work is submitted until the first
   * {@link #hasNext()} call.
   *
   * @param files inputs in deterministic Spark partition order
   * @param adapter Iceberg footer and decode operations
   * @param maxRows maximum planned rows per GPU subtask (soft for a single row group)
   * @param maxEstimatedGpuBytes maximum estimated GPU bytes per subtask
   * @param combineThreshold encoded-byte combine threshold for one subtask, from
   *                         spark.rapids.sql.reader.multithreaded.combine.sizeBytes; a
   *                         non-positive value disables cross-file combining
   * @param combineWaitMs fresh wait after every admitted result while building a combined
   *                      subtask. The first result is mandatory: baseline may first make a
   *                      timed probe, but an empty probe falls through to an indefinite wait.
   * @param workerThreads executor-wide worker count shared by footer and download jobs; this
   *                      bounds the concurrently downloading files
   * @param taskContext Spark task context captured by the task thread; may be null in tests
   */
  public StagedParquetPartitionReader(
      List<IcebergPartitionedFile> files,
      StagedScanAdapter adapter,
      int maxRows,
      long maxEstimatedGpuBytes,
      long combineThreshold,
      long combineWaitMs,
      int workerThreads,
      TaskContext taskContext) {
    this.files = immutableFileCopy(files);
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    this.planner = new StableGreedyReadPlanner(
        maxRows, maxEstimatedGpuBytes, combineThreshold);
    this.combineEnabled = combineThreshold > 0L;
    this.combineWaitMs = Math.max(combineWaitMs, 0L);
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
    return Collections.unmodifiableList(copy);
  }

  @Override
  public boolean hasNext() {
    if (closed.get()) {
      return false;
    }
    try {
      initializeIfNeeded();
      while (true) {
        if (closed.get()) {
          return false;
        }
        synchronized (iteratorLock) {
          if (currentBatches != null) {
            if (currentBatches.hasNext()) {
              return true;
            }
            closeIterator(currentBatches);
            currentBatches = null;
          }
        }

        // Match MultiFileCloudParquetPartitionReader: GPU decode acquires the task-wide
        // semaphore, and the Spark task-completion listener releases it. The reader deliberately
        // retains that permit while waiting for every later subtask instead of introducing a
        // staged-only release/reacquire cycle that increases concurrent GPU residency and spill.
        ReadSubtask subtask = nextPlannedSubtask();
        if (subtask == null) {
          closeAllFragmentFutures();
          return false;
        }
        Iterator<ColumnarBatch> decoded = null;
        boolean decodedInstalled = false;
        try {
          decoded = assembleAndDecode(subtask);
          synchronized (iteratorLock) {
            if (closed.get()) {
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
        }
      }
    } catch (CancellationException cancelled) {
      if (closed.get()) {
        return false;
      }
      closeAfterFailure(cancelled);
      throw cancelled;
    } catch (Throwable error) {
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
      synchronized (iteratorLock) {
        if (closed.get() || currentBatches == null) {
          throw new CancellationException("staged Parquet reader was closed before next()");
        }
        ColumnarBatch nextBatch = currentBatches.next();
        return nextBatch;
      }
    } catch (Throwable error) {
      closeAfterFailure(error);
      throw propagate(error);
    }
  }

  /**
   * Submit one fused pool job per file: the job fetches and filters the footer, completes the
   * footer future mid-job for independent footer timing/failure publication, and continues
   * straight into that file's blocking download on the same worker — the base reader's
   * one-job-per-file shape. A single queue pass per file matters: a separately re-queued
   * download job would wait behind every other task's queued jobs a second time, roughly
   * doubling the pipeline latency the task thread observes for each file.
   */
  private void initializeIfNeeded() {
    if (initialized) {
      return;
    }
    checkOpen();
    session = planner.newSession();
    initialized = true;
    for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
      synchronized (lifecycleLock) {
        checkOpen();
        IcebergPartitionedFile file = files.get(fileIndex);
        CompletableFuture<TimedFooter> footerFuture = new CompletableFuture<>();
        CompletableFuture<FileFragment> fragmentFuture = CompletableFuture.supplyAsync(
            () -> executeFileJob(file, footerFuture), pools.executor());
        final int completedIndex = fileIndex;
        // Publish every submitted future while holding the same lock close() uses to snapshot
        // them. A close racing submission therefore cannot miss and leak this job's fragment.
        fragmentFuture.whenComplete((fragment, error) -> {
          if (error != null) {
            footerFuture.completeExceptionally(error);
          }
          if (combineEnabled) {
            completedFileIndexes.offer(completedIndex);
          }
        });
        footerFutures.add(footerFuture);
        fragmentFutures.add(fragmentFuture);
      }
    }
  }

  /**
   * Execute the fused footer and download job under one Spark task/RMM registration.
   *
   * <p>The footer future is deliberately published before the download starts, while admission
   * waits for the completed fragment. Keeping both phases inside one registration avoids
   * needlessly detaching and immediately reattaching the same worker to the same Spark task.</p>
   */
  private FileFragment executeFileJob(
      IcebergPartitionedFile file,
      CompletableFuture<TimedFooter> footerFuture) {
    try {
      return runAsTaskPoolThread(() -> {
        long footerStart = System.nanoTime();
        FooterResult result = adapter.readAndFilterFooter(file);
        if (result == null) {
          throw new IllegalStateException("footer adapter returned null");
        }
        TimedFooter timed = new TimedFooter(result, System.nanoTime() - footerStart);
        footerFuture.complete(timed);
        return downloadFragment(result);
      });
    } catch (Throwable error) {
      throw wrapAsCompletion(error);
    }
  }

  private static CompletionException wrapAsCompletion(Throwable error) {
    if (error instanceof CompletionException) {
      return (CompletionException) error;
    }
    return new CompletionException(error);
  }

  /**
   * Download one file's filtered column chunks into a contiguous fragment.
   *
   * <p>Runs as one blocking pool job: allocate the exact pinned-preferred fragment, copy cache hits,
   * hand all cache-miss chunks to the async I/O engine in order, block until every accepted read
   * is terminal, publish owning cache slices, and seal the fragment as spillable.</p>
   */
  private FileFragment downloadFragment(FooterResult footer) throws Exception {
    checkOpen();
    long start = System.nanoTime();
    List<BlockMetaData> blocks = footer.getBlocks();
    long[] blockOffsets = FileFragment.computeBlockOffsets(blocks);
    long totalBytes = blockOffsets[blocks.size()];
    if (totalBytes == 0) {
      return new FileFragment(footer, blockOffsets, null,
          new FileFragment.DownloadStats(System.nanoTime() - start,
              0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    long cacheHitCount = 0L;
    long cacheHitBytes = 0L;
    long cacheMissCount = 0L;
    long cacheMissBytes = 0L;
    long cacheReadNanos = 0L;
    List<MissChunk> misses = new ArrayList<>();
    long allocStart = System.nanoTime();
    StagedParquetOutput output = StagedParquetOutput.create(totalBytes);
    long allocNanos = System.nanoTime() - allocStart;
    HostMemoryBuffer scratch = null;
    try {
      if (closed.get()) {
        throw new CancellationException("staged reader closed before fragment I/O started");
      }
      RapidsInputFile input = adapter.openInputFile(footer.getFile());
      if (input == null) {
        throw new IllegalStateException("input-file adapter returned null");
      }
      FileCache fileCache = FileCache.get();
      long fragmentOffset = 0L;
      for (BlockMetaData block : blocks) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          checkOpen();
          long length = column.getTotalSize();
          long sourceOffset = column.getStartingPos();
          Option<SeekableByteChannel> cached = fileCache.getDataRangeChannel(
              input, sourceOffset, length);
          if (cached.isDefined()) {
            try (SeekableByteChannel channel = cached.get()) {
              // Keep hit accounting before the copy, matching GpuParquetScan.copyLocal.
              cacheHitCount += 1L;
              cacheHitBytes = Math.addExact(cacheHitBytes, length);
              // Match GpuParquetScan.copyLocal: time only the byte copy. Closing the cache
              // channel (including FileCache.finishLocalFileRead bookkeeping) is deliberately
              // outside this metric even though it remains part of the worker's I/O time.
              long cacheStart = System.nanoTime();
              try {
                output.copyCachedRange(channel, fragmentOffset, length);
              } finally {
                cacheReadNanos = Math.addExact(
                    cacheReadNanos, System.nanoTime() - cacheStart);
              }
            }
          } else {
            MissChunk chunk = new MissChunk(sourceOffset, length, fragmentOffset);
            Option<FileCacheStartedToken> token = fileCache.startDataRangeCache(
                input, sourceOffset, length);
            chunk.token = token.isDefined() ? token.get() : null;
            misses.add(chunk);
            cacheMissCount += 1L;
            cacheMissBytes = Math.addExact(cacheMissBytes, length);
          }
          fragmentOffset = Math.addExact(fragmentOffset, length);
        }
      }

      // Merge miss chunks into few ranged reads. Zero-gap merges land directly on their
      // contiguous packed fragment region; gap-carrying merges read their whole source span
      // into a transient scratch buffer whose useful segments are routed into the fragment
      // after the read barrier. Gap bytes are downloaded and discarded — bounded extra
      // bandwidth traded for far fewer request round-trips.
      List<MergedRead> mergedReads = mergeMissChunks(misses);
      List<PlannedReadRange> directRanges = new ArrayList<>();
      List<RapidsInputFile.CopyRange> scratchRanges = new ArrayList<>();
      long scratchBytes = 0L;
      long requestedBytes = 0L;
      for (MergedRead read : mergedReads) {
        requestedBytes = Math.addExact(requestedBytes, read.spanBytes());
        if (read.isDirect()) {
          directRanges.add(new PlannedReadRange(
              footer, read.sourceStart, read.spanBytes(), read.chunks.get(0).fragmentOffset));
        } else {
          read.scratchStart = scratchBytes;
          scratchRanges.add(new RapidsInputFile.CopyRange(
              read.sourceStart, read.spanBytes(), scratchBytes));
          scratchBytes = Math.addExact(scratchBytes, read.spanBytes());
        }
      }
      if (scratchBytes > 0) {
        long scratchAllocStart = System.nanoTime();
        scratch = HostAlloc$.MODULE$.alloc(scratchBytes, false);
        allocNanos += System.nanoTime() - scratchAllocStart;
      }

      // Blocking remote reads on this worker via the same synchronous readVectored the base
      // multithreaded reader uses; the elapsed span is the remote-read metric compared against
      // the base reader. The calls return only when every byte has landed.
      long readStart = System.nanoTime();
      output.copyRanges(input, directRanges);
      if (scratch != null) {
        input.readVectored(scratch, scratchRanges);
      }
      long readWaitNanos = System.nanoTime() - readStart;
      checkOpen();

      // Route the useful segments of gap-merged reads into the packed fragment.
      long routeStart = System.nanoTime();
      for (MergedRead read : mergedReads) {
        if (read.scratchStart >= 0) {
          for (MissChunk chunk : read.chunks) {
            output.copyFromHostBuffer(
                chunk.fragmentOffset,
                scratch,
                read.scratchStart + (chunk.sourceOffset - read.sourceStart),
                chunk.length);
          }
        }
      }
      long routeNanos = System.nanoTime() - routeStart;

      // Publish owning cache slices inline on this worker.
      long finalizeStart = System.nanoTime();
      for (MissChunk chunk : misses) {
        FileCacheStartedToken token = chunk.token;
        if (token != null) {
          HostMemoryBuffer data = output.sliceForCache(chunk.fragmentOffset, chunk.length);
          // Keep the token cancellable until the owning slice exists. complete() consumes the
          // HMB before it queues cache work, so clear the token before handing over ownership.
          chunk.token = null;
          token.complete(data);
        }
      }
      output.seal();
      return new FileFragment(footer, blockOffsets, output,
          new FileFragment.DownloadStats(System.nanoTime() - start,
              allocNanos, readWaitNanos, routeNanos, System.nanoTime() - finalizeStart,
              directRanges.size() + scratchRanges.size(), requestedBytes,
              cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos));
    } catch (Throwable error) {
      // The blocking reads are terminal on return, so no writer can still touch the output or
      // scratch here.
      Throwable failure = unwrap(error);
      for (MissChunk chunk : misses) {
        if (chunk.token != null) {
          try {
            chunk.token.cancel();
          } catch (Throwable cancelError) {
            failure = addFailure(failure, cancelError);
          }
        }
      }
      try {
        output.close();
      } catch (Throwable closeError) {
        if (closeError != failure) {
          failure.addSuppressed(closeError);
        }
      }
      throw propagate(failure);
    } finally {
      if (scratch != null) {
        scratch.close();
      }
    }
  }

  /** One cache-miss column chunk: source range, packed fragment offset, and its cache token. */
  private static final class MissChunk {
    final long sourceOffset;
    final long length;
    final long fragmentOffset;
    FileCacheStartedToken token;

    MissChunk(long sourceOffset, long length, long fragmentOffset) {
      this.sourceOffset = sourceOffset;
      this.length = length;
      this.fragmentOffset = fragmentOffset;
    }
  }

  /** Consecutive source-sorted miss chunks merged into one ranged read. */
  private static final class MergedRead {
    final List<MissChunk> chunks = new ArrayList<>();
    long sourceStart;
    long sourceEnd;
    long scratchStart = -1L;

    long spanBytes() {
      return sourceEnd - sourceStart;
    }

    /** Direct reads have no source gaps and land exactly on one packed fragment region. */
    boolean isDirect() {
      long expectedSource = sourceStart;
      long expectedFragment = chunks.get(0).fragmentOffset;
      for (MissChunk chunk : chunks) {
        if (chunk.sourceOffset != expectedSource || chunk.fragmentOffset != expectedFragment) {
          return false;
        }
        expectedSource += chunk.length;
        expectedFragment += chunk.length;
      }
      return true;
    }
  }

  /**
   * Greedily merge contiguous source-sorted miss chunks. This intentionally has no merged-span
   * cap: the base multithreaded reader coalesces every contiguous run before submitting the
   * resulting ranges together in one vectored-read call.
   */
  private static List<MergedRead> mergeMissChunks(List<MissChunk> misses) {
    ArrayList<MissChunk> sorted = new ArrayList<>(misses);
    sorted.sort(Comparator.comparingLong(chunk -> chunk.sourceOffset));
    ArrayList<MergedRead> merged = new ArrayList<>();
    MergedRead current = null;
    for (MissChunk chunk : sorted) {
      long chunkEnd = Math.addExact(chunk.sourceOffset, chunk.length);
      if (current != null
          && chunk.sourceOffset >= current.sourceEnd
          && chunk.sourceOffset - current.sourceEnd <= COALESCE_GAP_LIMIT_BYTES) {
        current.chunks.add(chunk);
        current.sourceEnd = chunkEnd;
      } else {
        current = new MergedRead();
        current.sourceStart = chunk.sourceOffset;
        current.sourceEnd = chunkEnd;
        current.chunks.add(chunk);
        merged.add(current);
      }
    }
    return merged;
  }

  /** Advance baseline-compatible file admission until the next subtask is available. */
  private ReadSubtask nextPlannedSubtask() throws Exception {
    while (plannedSubtasks.isEmpty()) {
      if (filesAdmitted >= files.size()) {
        if (planningFinished) {
          return null;
        }
        planningFinished = true;
        plannedSubtasks.addAll(session.finish());
        continue;
      }

      if (!combineEnabled) {
        admitFile(orderedFileCursor++);
        continue;
      }

      // Completion-order combine mirrors ExecutorCompletionService in the base reader. The
      // group's first file waits without a deadline. Every admitted file that leaves the group
      // open earns a new full combine wait for the next completion; there is no cumulative
      // subtask budget.
      Integer completedIndex = awaitCompletedFile(!session.hasOpenBlocks());
      if (completedIndex == null) {
        plannedSubtasks.addAll(session.flush());
      } else {
        admitFile(completedIndex);
      }
    }
    return plannedSubtasks.poll();
  }

  /**
   * Wait for a completed file index. A null result is a combine timeout; close uses a sentinel
   * to wake the same blocking queue immediately.
   */
  private Integer awaitCompletedFile(boolean waitForFirst) throws InterruptedException {
    long waitStart = System.nanoTime();
    Integer fileIndex;
    try {
      if (waitForFirst) {
        fileIndex = completedFileIndexes.take();
      } else if (combineWaitMs > 0L) {
        fileIndex = completedFileIndexes.poll(combineWaitMs, TimeUnit.MILLISECONDS);
      } else {
        fileIndex = completedFileIndexes.poll();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    }
    adapter.onResultWait(System.nanoTime() - waitStart);
    if (fileIndex != null && fileIndex == CLOSED_COMPLETION) {
      checkOpen();
      throw new CancellationException("staged Parquet reader completion queue closed");
    }
    checkOpen();
    return fileIndex;
  }

  /** Admit one fully completed file result into the task-thread planner. */
  private void admitFile(int fileIndex) throws Exception {
    CompletableFuture<TimedFooter> footerFuture;
    CompletableFuture<FileFragment> fragmentFuture;
    synchronized (lifecycleLock) {
      footerFuture = footerFutures.get(fileIndex);
      fragmentFuture = fragmentFutures.get(fileIndex);
    }

    if (combineEnabled) {
      // The completion queue publishes only terminal fragment futures.
      awaitOrCancel(fragmentFuture);
    } else {
      long resultWaitStart = System.nanoTime();
      awaitOrCancel(fragmentFuture);
      adapter.onResultWait(System.nanoTime() - resultWaitStart);
    }
    long footerWaitStart = System.nanoTime();
    TimedFooter timed = awaitOrCancel(footerFuture);
    adapter.onFooterWait(System.nanoTime() - footerWaitStart);
    adapter.onFooterCompleted(timed.footerNanos);

    filesAdmitted++;
    fragmentByFooter.put(timed.footer, fragmentFuture);
    plannedSubtasks.addAll(session.add(timed.footer));
  }

  /**
   * Wait for the subtask's fragments and hand a zero-copy logical Parquet input to GPU decode.
   * Fragment download measurements are attributed to the first consuming subtask; combine time
   * now measures only construction of the segment description rather than a full data copy.
   */
  private Iterator<ColumnarBatch> assembleAndDecode(ReadSubtask subtask) throws Exception {
    List<ReadSubtask.FileSlice> slices = subtask.getFileSlices();
    ArrayList<FileFragment> fragments = new ArrayList<>(slices.size());
    for (ReadSubtask.FileSlice slice : slices) {
      CompletableFuture<FileFragment> future = fragmentByFooter.get(slice.getFooter());
      if (future == null) {
        throw new IllegalStateException("subtask references an unconsumed footer");
      }
      fragments.add(awaitOrCancel(future));
    }

    long ioNanos = 0L;
    long ioAllocNanos = 0L;
    long ioReadWaitNanos = 0L;
    long ioRouteNanos = 0L;
    long ioFinalizeNanos = 0L;
    long ioRequestCount = 0L;
    long ioRequestedBytes = 0L;
    long cacheHitCount = 0L;
    long cacheHitBytes = 0L;
    long cacheMissCount = 0L;
    long cacheMissBytes = 0L;
    long cacheReadNanos = 0L;
    for (FileFragment fragment : fragments) {
      if (statsAttributed.add(fragment)) {
        FileFragment.DownloadStats stats = fragment.getStats();
        ioNanos = Math.addExact(ioNanos, stats.ioNanos);
        ioAllocNanos = Math.addExact(ioAllocNanos, stats.allocNanos);
        ioReadWaitNanos = Math.addExact(ioReadWaitNanos, stats.readWaitNanos);
        ioRouteNanos = Math.addExact(ioRouteNanos, stats.routeNanos);
        ioFinalizeNanos = Math.addExact(ioFinalizeNanos, stats.finalizeNanos);
        ioRequestCount += stats.requestCount;
        ioRequestedBytes = Math.addExact(ioRequestedBytes, stats.requestedBytes);
        cacheHitCount += stats.cacheHitCount;
        cacheHitBytes = Math.addExact(cacheHitBytes, stats.cacheHitBytes);
        cacheMissCount += stats.cacheMissCount;
        cacheMissBytes = Math.addExact(cacheMissBytes, stats.cacheMissBytes);
        cacheReadNanos = Math.addExact(cacheReadNanos, stats.cacheReadNanos);
      }
    }

    long assembleStart = System.nanoTime();
    StagedParquetInput parquetInput = new StagedParquetInput(subtask, fragments);
    long combineNanos = System.nanoTime() - assembleStart;
    adapter.onSubtaskCompleted(subtask, new SubtaskStats(
        ioNanos, ioAllocNanos, ioReadWaitNanos, ioRouteNanos, ioFinalizeNanos,
        ioRequestCount, ioRequestedBytes, combineNanos, false,
        cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos));

    // The adapter eagerly drains cuDF's producer, so no returned batch references the borrowed
    // fragments. Only after it returns can we close fragment owners behind the admission cursor.
    Iterator<ColumnarBatch> decoded = adapter.decodeAndPostProcess(subtask, parquetInput);
    if (decoded == null) {
      throw new IllegalStateException("decode adapter returned null");
    }
    closeFullyConsumedFragments(slices, fragments);
    return decoded;
  }

  /**
   * Close every fragment whose final non-empty row group was consumed by this subtask. A file
   * split by a per-file row/GPU limit stays live only until its last slice; complete files in a
   * combined subtask close immediately after cuDF has eagerly consumed their zero-copy slices.
   */
  private void closeFullyConsumedFragments(
      List<ReadSubtask.FileSlice> slices,
      List<FileFragment> fragments) {
    for (int index = 0; index < slices.size(); index++) {
      ReadSubtask.FileSlice slice = slices.get(index);
      List<BlockMetaData> blocks = slice.getFooter().getBlocks();
      boolean hasLaterData = false;
      for (int blockIndex = slice.getFirstBlock() + slice.getBlockCount();
          blockIndex < blocks.size(); blockIndex++) {
        if (blocks.get(blockIndex).getRowCount() > 0L) {
          hasLaterData = true;
          break;
        }
      }
      if (!hasLaterData) {
        FileFragment fragment = fragments.get(index);
        fragmentByFooter.remove(slice.getFooter());
        statsAttributed.remove(fragment);
        closeFragmentAfterAsync(fragment);
      }
    }
  }

  /** Close completed fragments now and attach cleanup to any fragment still finishing. */
  private void closeAllFragmentFutures() {
    List<CompletableFuture<FileFragment>> fragmentSnapshot;
    synchronized (lifecycleLock) {
      fragmentSnapshot = new ArrayList<>(fragmentFutures);
    }
    for (CompletableFuture<FileFragment> future : fragmentSnapshot) {
      future.whenComplete((fragment, error) -> {
        if (fragment != null) {
          closeFragmentAfterAsync(fragment);
        }
      });
    }
  }

  private void closeFragmentAfterAsync(FileFragment fragment) {
    try {
      fragment.close();
    } catch (Throwable error) {
      recordAsynchronousCleanupFailure(error);
    }
  }

  /**
   * Wait for one pipeline future while racing the close signal, so the task thread never stays
   * blocked on a closed reader and close() never has to interfere with the pipeline futures.
   */
  private <T> T awaitOrCancel(CompletableFuture<T> future) throws Exception {
    // The joined any-of future never throws here: a failed stage is rethrown below with its
    // original failure type instead of anyOf's CompletionException wrapper.
    CompletableFuture.anyOf(future, closeSignal).exceptionally(ignored -> null).join();
    checkOpen();
    try {
      return future.get();
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

  private void checkOpen() {
    if (closed.get()) {
      throw new CancellationException("staged Parquet reader is closed");
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
    // Wake the task thread from any per-file wait. The pipeline futures stay untouched — their
    // pool jobs remain the sole completers and are never cancelled, they drain naturally.
    closeSignal.complete(null);
    completedFileIndexes.offer(CLOSED_COMPLETION);
    // Reclaim fragments that completed before close or complete later without a consumer.
    // Consumed fragments close twice harmlessly: fragment close is idempotent.
    closeAllFragmentFutures();
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

  /** One filtered footer paired with the worker's fetch/filter elapsed time. */
  private static final class TimedFooter {
    private final FooterResult footer;
    private final long footerNanos;

    private TimedFooter(FooterResult footer, long footerNanos) {
      this.footer = footer;
      this.footerNanos = footerNanos;
    }
  }

}
