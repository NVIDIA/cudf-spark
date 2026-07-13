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

import java.io.IOException;
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
import java.util.Optional;
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
import com.nvidia.spark.rapids.GpuSemaphore$;
import com.nvidia.spark.rapids.HostAlloc$;
import com.nvidia.spark.rapids.IcebergS3RangeCopier.FileChannelCopyRange;
import com.nvidia.spark.rapids.IcebergS3RangeCopier.FileChannelCopyResult;
import com.nvidia.spark.rapids.filecache.FileCache;
import com.nvidia.spark.rapids.filecache.FileCacheDataRangeLease;
import com.nvidia.spark.rapids.filecache.FileCacheDataRangeReservation;
import com.nvidia.spark.rapids.filecache.FileCacheDataRangeWriter;
import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile;
import com.nvidia.spark.rapids.jni.RmmSpark;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.iceberg.aws.s3.IcebergS3InputFile;
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

        // Waiting below releases an already-held GPU permit only when no file result is ready.
        // The Scala decode adapter reacquires immediately before entering cuDF.
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
   * <p>The file cache is the authoritative destination for every selected column chunk. The
   * worker first reserves every exact range, then makes one bounded host-allocation attempt. If
   * it succeeds, cache-owner downloads tee the same S3 response into both the mandatory cache
   * file and the fragment HMB; cache hits and followers are copied from their committed leases.
   * If it fails, the worker downloads to cache only, waits for every range to become readable,
   * and only then performs a blocking fragment allocation and copies the ranges from disk.</p>
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
              0L, 0L, 0L, 0L, 0L, 0L, false,
              0L, 0L, 0L, 0L, 0L));
    }

    List<CacheRange> ranges = new ArrayList<>();
    List<CacheRange> writerRanges = new ArrayList<>();
    StagedParquetOutput output = null;
    long cacheHitCount = 0L;
    long cacheHitBytes = 0L;
    long cacheMissCount = 0L;
    long cacheMissBytes = 0L;
    long cacheReadNanos = 0L;
    try {
      if (closed.get()) {
        throw new CancellationException("staged reader closed before fragment I/O started");
      }
      RapidsInputFile genericInput = adapter.openInputFile(footer.getFile());
      if (genericInput == null) {
        throw new IllegalStateException("input-file adapter returned null");
      }
      // This POC is deliberately Iceberg/S3-only. Keeping the concrete cast here avoids a
      // second generic reader abstraction around the mandatory cache-file API.
      IcebergS3InputFile input = (IcebergS3InputFile) genericInput;
      FileCache fileCache = FileCache.get();
      long fragmentOffset = 0L;
      for (BlockMetaData block : blocks) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          checkOpen();
          long length = column.getTotalSize();
          long sourceOffset = column.getStartingPos();
          Optional<FileCacheDataRangeReservation> optionalReservation =
              fileCache.reserveDataRangeCache(input, sourceOffset, length);
          if (!optionalReservation.isPresent()) {
            throw new IOException(
                "file cache could not reserve mandatory Iceberg range " +
                    input.path() + " [" + sourceOffset + ", " +
                    Math.addExact(sourceOffset, length) + ")");
          }
          FileCacheDataRangeReservation reservation = optionalReservation.get();
          Optional<FileCacheDataRangeWriter> optionalWriter = reservation.getWriter();
          CacheRange range = new CacheRange(
              sourceOffset,
              length,
              fragmentOffset,
              reservation,
              optionalWriter.isPresent() ? new OwnedCacheWriter(optionalWriter.get()) : null);
          ranges.add(range);
          if (range.writer != null) {
            writerRanges.add(range);
          }
          if (reservation.isCacheHit()) {
            cacheHitCount += 1L;
            cacheHitBytes = Math.addExact(cacheHitBytes, length);
          } else {
            cacheMissCount += 1L;
            cacheMissBytes = Math.addExact(cacheMissBytes, length);
          }
          fragmentOffset = Math.addExact(fragmentOffset, length);
        }
      }

      // The copier coalesces only adjacent ranges in list order. Source sorting keeps the
      // baseline reader's contiguous-only coalescing without downloading gap bytes.
      writerRanges.sort(Comparator.comparingLong(range -> range.sourceOffset));

      // Exactly one bounded allocation cycle decides the branch. tryAlloc2 may spill/retry
      // internally, but it does not start another outer retry cycle for this file.
      long allocStart = System.nanoTime();
      Option<HostMemoryBuffer> optionalAllocation =
          HostAlloc$.MODULE$.tryAlloc2(totalBytes, true);
      long allocNanos = System.nanoTime() - allocStart;
      HostMemoryBuffer directHostDestination = null;
      if (optionalAllocation.isDefined()) {
        directHostDestination = optionalAllocation.get();
        output = new MemoryStagedParquetOutput(directHostDestination, totalBytes);
      }

      List<FileChannelCopyRange> remoteRanges = new ArrayList<>(writerRanges.size());
      long requestedBytes = 0L;
      for (CacheRange range : writerRanges) {
        requestedBytes = Math.addExact(requestedBytes, range.length);
        if (directHostDestination == null) {
          remoteRanges.add(new FileChannelCopyRange(
              range.sourceOffset,
              range.length,
              range.writer.getChannel(),
              0L));
        } else {
          remoteRanges.add(new FileChannelCopyRange(
              range.sourceOffset,
              range.length,
              range.writer.getChannel(),
              0L,
              directHostDestination,
              range.fragmentOffset));
        }
      }

      // Submit each owner range once. The file write is authoritative; host mirroring is best
      // effort and a failed mirror is repaired from that same committed cache file.
      long readStart = System.nanoTime();
      FileChannelCopyResult copyResult =
          input.readVectoredToFileChannelsAndHostMemory(remoteRanges);
      if (copyResult.getBytesCopied() != requestedBytes) {
        throw new IOException(
            "Iceberg S3 copy wrote " + copyResult.getBytesCopied() +
                " bytes; expected " + requestedBytes);
      }
      long readWaitNanos = System.nanoTime() - readStart;
      checkOpen();

      // Publish owners and obtain one independent pinned read lease per exact range. Followers
      // never issue another S3 read; their future completes after the owner's commit.
      long finalizeStart = System.nanoTime();
      for (CacheRange range : writerRanges) {
        range.writer.commit();
      }
      for (CacheRange range : ranges) {
        range.lease = awaitCacheLease(range.reservation.getCompletionFuture());
        range.completionClaimed = true;
      }
      long finalizeNanos = System.nanoTime() - finalizeStart;
      checkOpen();

      if (output == null) {
        // Cache I/O is terminal before this blocking allocation. Host-memory pressure therefore
        // cannot hold a remote request or one unit of remote-read concurrency open.
        long blockingAllocStart = System.nanoTime();
        HostMemoryBuffer allocation = HostAlloc$.MODULE$.alloc(totalBytes, true);
        allocNanos = Math.addExact(allocNanos, System.nanoTime() - blockingAllocStart);
        output = new MemoryStagedParquetOutput(allocation, totalBytes);
      }

      // Disk-first loads every range. The tee branch loads cache hits and followers; if any
      // best-effort host mirror failed, all owner ranges are conservatively repaired from cache.
      boolean loadAllWriterRanges =
          directHostDestination == null || !copyResult.allHostCopiesSucceeded();
      long routeStart = System.nanoTime();
      for (CacheRange range : ranges) {
        if (range.writer == null || loadAllWriterRanges) {
          // This is the same cache-to-HMB work measured by the baseline cache-reader metric.
          // Include immediate hits, followers, the complete disk-first branch, and tee repair.
          long cacheStart = System.nanoTime();
          try {
            output.copyCachedRange(
                range.lease.getChannel(), range.fragmentOffset, range.length);
          } finally {
            cacheReadNanos = Math.addExact(
                cacheReadNanos, System.nanoTime() - cacheStart);
          }
        }
      }
      long routeNanos = System.nanoTime() - routeStart;

      long sealStart = System.nanoTime();
      for (CacheRange range : ranges) {
        range.lease.close();
        range.lease = null;
      }
      output.seal();
      finalizeNanos = Math.addExact(finalizeNanos, System.nanoTime() - sealStart);
      return new FileFragment(footer, blockOffsets, output,
          new FileFragment.DownloadStats(System.nanoTime() - start,
              allocNanos, readWaitNanos, routeNanos, finalizeNanos,
              countCoalescedRequests(writerRanges), requestedBytes,
              directHostDestination == null,
              cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos));
    } catch (Throwable error) {
      // The copier drains accepted requests before returning, so partial files are no longer
      // being written and every nonterminal owner can be cancelled safely.
      Throwable failure = unwrap(error);
      for (CacheRange range : writerRanges) {
        if (!range.writer.isTerminal()) {
          try {
            range.writer.cancel();
          } catch (Throwable cancelError) {
            failure = addFailure(failure, cancelError);
          }
        }
      }

      // A committed owner or follower can complete after this worker fails. Close claimed
      // leases now and attach cleanup to every future whose lease has not yet been claimed.
      for (CacheRange range : ranges) {
        if (range.lease != null) {
          try {
            range.lease.close();
          } catch (Throwable closeError) {
            failure = addFailure(failure, closeError);
          } finally {
            range.lease = null;
          }
        } else if (!range.completionClaimed) {
          range.reservation.getCompletionFuture().whenComplete((lease, completionError) -> {
            if (lease != null) {
              try {
                lease.close();
              } catch (Throwable closeError) {
                recordAsynchronousCleanupFailure(closeError);
              }
            }
          });
        }
      }
      if (output != null) {
        try {
          output.close();
        } catch (Throwable closeError) {
          failure = addFailure(failure, closeError);
        }
      }
      throw propagate(failure);
    }
  }

  /** One selected column chunk and its mandatory exact-range cache reservation. */
  private static final class CacheRange {
    final long sourceOffset;
    final long length;
    final long fragmentOffset;
    final FileCacheDataRangeReservation reservation;
    final OwnedCacheWriter writer;
    FileCacheDataRangeLease lease;
    boolean completionClaimed;

    CacheRange(
        long sourceOffset,
        long length,
        long fragmentOffset,
        FileCacheDataRangeReservation reservation,
        OwnedCacheWriter writer) {
      this.sourceOffset = sourceOffset;
      this.length = length;
      this.fragmentOffset = fragmentOffset;
      this.reservation = Objects.requireNonNull(reservation, "reservation");
      this.writer = writer;
    }
  }

  /** Tracks the exactly-once commit/cancel obligation of one cache-owned writer. */
  private static final class OwnedCacheWriter {
    private final FileCacheDataRangeWriter writer;
    private boolean terminal;

    OwnedCacheWriter(FileCacheDataRangeWriter writer) {
      this.writer = Objects.requireNonNull(writer, "writer");
    }

    java.nio.channels.FileChannel getChannel() {
      return writer.getChannel();
    }

    void commit() {
      if (terminal) {
        throw new IllegalStateException("cache writer is already terminal");
      }
      // commit owns the terminal transition even if cache publication itself throws.
      terminal = true;
      writer.commit();
    }

    void cancel() {
      if (!terminal) {
        terminal = true;
        writer.cancel();
      }
    }

    boolean isTerminal() {
      return terminal;
    }
  }

  /** Count the contiguous request groups the S3 copier submits for source-sorted ranges. */
  private static long countCoalescedRequests(List<CacheRange> sortedWriterRanges) {
    long requests = 0L;
    long previousEnd = -1L;
    boolean hasPrevious = false;
    for (CacheRange range : sortedWriterRanges) {
      if (range.length == 0L) {
        continue;
      }
      if (!hasPrevious || range.sourceOffset != previousEnd) {
        requests += 1L;
      }
      previousEnd = Math.addExact(range.sourceOffset, range.length);
      hasPrevious = true;
    }
    return requests;
  }

  private static FileCacheDataRangeLease awaitCacheLease(
      CompletableFuture<FileCacheDataRangeLease> future) throws Exception {
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
    Integer fileIndex = completedFileIndexes.poll();
    try {
      if (fileIndex == null) {
        if (waitForFirst) {
          // Unlike the baseline reader, the staged task thread is not assembling input while it
          // waits. Do not retain a GPU permit when there is no ready fragment to decode.
          GpuSemaphore$.MODULE$.releaseIfNecessary(taskContext);
          fileIndex = completedFileIndexes.take();
        } else if (combineWaitMs > 0L) {
          GpuSemaphore$.MODULE$.releaseIfNecessary(taskContext);
          fileIndex = completedFileIndexes.poll(combineWaitMs, TimeUnit.MILLISECONDS);
        }
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

    if (!fragmentFuture.isDone()) {
      GpuSemaphore$.MODULE$.releaseIfNecessary(taskContext);
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
      if (!future.isDone()) {
        GpuSemaphore$.MODULE$.releaseIfNecessary(taskContext);
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
    boolean diskBacked = false;
    for (FileFragment fragment : fragments) {
      FileFragment.DownloadStats stats = fragment.getStats();
      diskBacked |= stats.diskBacked;
      if (statsAttributed.add(fragment)) {
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
        ioRequestCount, ioRequestedBytes, combineNanos, diskBacked,
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
