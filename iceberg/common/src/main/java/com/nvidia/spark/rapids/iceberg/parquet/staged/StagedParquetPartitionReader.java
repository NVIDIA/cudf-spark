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
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.GpuSemaphore$;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Coordinates the staged Parquet pipeline for one Spark input partition.
 *
 * <p>The pipeline keeps input order end to end, matching the default {@code keepReadsInOrder}
 * behavior of the non-staged multithreaded reader:</p>
 * <ol>
 *   <li>One footer job per file runs on the shared pool. As soon as a footer resolves, it chains
 *       a blocking download job for that file's filtered column chunks — pool occupancy paces
 *       concurrent downloads. Each download packs its chunks contiguously into a per-file
 *       {@link FileFragment} whose layout comes from the file's own footer, so data I/O starts
 *       without waiting for planning or for any other file's footer.</li>
 *   <li>The Spark task thread consumes footers strictly in file-list order, feeding the
 *       incremental planning session; plans are therefore deterministic for a given file list.
 *       No polling is involved: {@code close()} wakes the task thread by completing the per-file
 *       futures exceptionally.</li>
 *   <li>Subtasks are assembled and decoded in plan order. Assembly waits for the constituent
 *       fragments, copies one contiguous fragment region per file slice into an exact-sized
 *       synthetic Parquet buffer, and writes the header and relocated footer. Completed
 *       fragments wait as spillable host buffers until consumed.</li>
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
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> asynchronousCleanupFailure = new AtomicReference<>();
  // Ordered per-file futures the task thread waits on; guarded by lifecycleLock so close() can
  // snapshot them while initialization is still appending.
  private final List<CompletableFuture<FooterHandle>> footerFutures = new ArrayList<>();
  // Every created fragment future, registered by footer jobs for close-time cleanup.
  private final List<CompletableFuture<FileFragment>> fragmentRegistry = new ArrayList<>();

  // Task-thread-only planning and assembly state.
  private StableGreedyReadPlanner.Session session;
  private boolean initialized;
  private boolean planningFinished;
  private int footerCursor;
  private final ArrayDeque<ReadSubtask> plannedSubtasks = new ArrayDeque<>();
  private final Map<FooterResult, CompletableFuture<FileFragment>> fragmentByFooter =
      new IdentityHashMap<>();
  private final Map<FooterResult, Integer> footerOrder = new IdentityHashMap<>();
  private final List<CompletableFuture<FileFragment>> consumedFragments = new ArrayList<>();
  private final Set<FileFragment> statsAttributed =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private int fragmentsClosedUpTo;

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
   * @param combineThreshold encoded-byte combine threshold for one subtask, from
   *                         spark.rapids.sql.reader.multithreaded.combine.sizeBytes; a
   *                         non-positive value disables cross-file combining
   * @param workerThreads executor-wide worker count shared by footer and download jobs; this
   *                      bounds the concurrently downloading files
   * @param taskContext Spark task context captured by the task thread; may be null in tests
   */
  public StagedParquetPartitionReader(
      List<IcebergPartitionedFile> files,
      StagedScanAdapter adapter,
      StructType expectedSparkSchema,
      int maxRows,
      long maxEstimatedGpuBytes,
      long combineThreshold,
      int workerThreads,
      TaskContext taskContext) {
    this.files = immutableFileCopy(files);
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    this.planner = new StableGreedyReadPlanner(
        maxRows, maxEstimatedGpuBytes, combineThreshold, expectedSparkSchema);
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
            // exhausted, and before the footer/fragment waits or the terminal return below.
            if (!semaphoreReleasedForAdvance) {
              releaseGpuSemaphoreFromTaskThread();
              semaphoreReleasedForAdvance = true;
            }
            closeIterator(currentBatches);
            currentBatches = null;
          }
        }
        if (!semaphoreReleasedForAdvance) {
          releaseGpuSemaphoreFromTaskThread();
        }

        ReadSubtask subtask = nextPlannedSubtask();
        if (subtask == null) {
          closeFragmentsThrough(consumedFragments.size());
          return false;
        }
        Iterator<ColumnarBatch> decoded = null;
        boolean decodedInstalled = false;
        try {
          decoded = assembleAndDecode(subtask);
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

  /** Submit one footer job per file; each chains its file's download when the footer resolves. */
  private void initializeIfNeeded() {
    if (initialized) {
      return;
    }
    checkOpen();
    // Footer fetch/filter and downloads run on the shared pool. Relinquish any permit still
    // owned by this task before the in-order footer waits below.
    releaseGpuSemaphoreFromTaskThread();
    session = planner.newSession();
    initialized = true;
    for (IcebergPartitionedFile file : files) {
      checkOpen();
      CompletableFuture<FooterHandle> footerFuture = new CompletableFuture<>();
      synchronized (lifecycleLock) {
        footerFutures.add(footerFuture);
      }
      pools.executor().submit(() -> runFooterJob(file, footerFuture));
    }
  }

  /** Fetch one footer and chain its file's blocking download job. */
  private void runFooterJob(
      IcebergPartitionedFile file,
      CompletableFuture<FooterHandle> footerFuture) {
    FooterResult footer;
    long footerNanos;
    try {
      long start = System.nanoTime();
      footer = runAsTaskPoolThread(() -> {
        FooterResult result = adapter.readAndFilterFooter(file);
        if (result == null) {
          throw new IllegalStateException("footer adapter returned null");
        }
        return result;
      });
      footerNanos = System.nanoTime() - start;
    } catch (Throwable error) {
      footerFuture.completeExceptionally(error);
      return;
    }
    CompletableFuture<FileFragment> fragmentFuture = new CompletableFuture<>();
    synchronized (lifecycleLock) {
      fragmentRegistry.add(fragmentFuture);
    }
    try {
      pools.executor().submit(() -> runDownloadJob(footer, fragmentFuture));
    } catch (Throwable submitError) {
      fragmentFuture.completeExceptionally(submitError);
    }
    footerFuture.complete(new FooterHandle(footer, footerNanos, fragmentFuture));
  }

  /** Execute one file's blocking download and publish its fragment. */
  private void runDownloadJob(
      FooterResult footer,
      CompletableFuture<FileFragment> fragmentFuture) {
    FileFragment fragment;
    try {
      fragment = runAsTaskPoolThread(() -> downloadFragment(footer));
    } catch (Throwable error) {
      fragmentFuture.completeExceptionally(error);
      return;
    }
    if (!fragmentFuture.complete(fragment)) {
      // close() completed this future exceptionally first; the writers are already terminal, so
      // reclaim the fragment here.
      closeFragmentAfterAsync(fragment);
    }
  }

  /**
   * Download one file's filtered column chunks into a contiguous fragment.
   *
   * <p>Runs as one blocking pool job: allocate the exact non-pinned fragment, copy cache hits,
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
          new FileFragment.DownloadStats(System.nanoTime() - start, 0L, 0L, 0L, 0L, 0L));
    }

    long cacheHitCount = 0L;
    long cacheHitBytes = 0L;
    long cacheMissCount = 0L;
    long cacheMissBytes = 0L;
    long cacheReadNanos = 0L;
    List<PlannedReadRange> missRanges = new ArrayList<>();
    List<FileCacheStartedToken> missTokens = new ArrayList<>();
    StagedParquetOutput output = StagedParquetOutput.create(totalBytes);
    CompletableFuture<Void> read = null;
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
            long cacheStart = System.nanoTime();
            try (SeekableByteChannel channel = cached.get()) {
              output.copyCachedRange(channel, fragmentOffset, length);
            }
            cacheReadNanos = Math.addExact(cacheReadNanos, System.nanoTime() - cacheStart);
            cacheHitCount += 1L;
            cacheHitBytes = Math.addExact(cacheHitBytes, length);
          } else {
            missRanges.add(new PlannedReadRange(footer, sourceOffset, length, fragmentOffset));
            cacheMissCount += 1L;
            cacheMissBytes = Math.addExact(cacheMissBytes, length);
            Option<FileCacheStartedToken> token = fileCache.startDataRangeCache(
                input, sourceOffset, length);
            missTokens.add(token.isDefined() ? token.get() : null);
          }
          fragmentOffset = Math.addExact(fragmentOffset, length);
        }
      }

      // The pacing point of the blocking model: wait for every accepted read.
      read = output.copyRangesAsync(input, missRanges);
      read.join();
      checkOpen();

      // Publish owning cache slices inline on this worker.
      for (int index = 0; index < missRanges.size(); index++) {
        FileCacheStartedToken token = missTokens.get(index);
        if (token != null) {
          PlannedReadRange range = missRanges.get(index);
          HostMemoryBuffer data = output.sliceForCache(
              range.getOutputOffset(), range.getLength());
          // Keep the token cancellable until the owning slice exists. complete() consumes the
          // HMB before it queues cache work, so clear the token before handing over ownership.
          missTokens.set(index, null);
          token.complete(data);
        }
      }
      output.seal();
      return new FileFragment(footer, blockOffsets, output,
          new FileFragment.DownloadStats(System.nanoTime() - start,
              cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos));
    } catch (Throwable error) {
      Throwable failure = unwrap(error);
      if (read != null) {
        // Accepted writers must become terminal before the output is reclaimed.
        try {
          read.join();
        } catch (Throwable drainError) {
          Throwable unwrapped = unwrap(drainError);
          if (unwrapped != failure) {
            failure.addSuppressed(unwrapped);
          }
        }
      }
      for (FileCacheStartedToken token : missTokens) {
        if (token != null) {
          try {
            token.cancel();
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
    }
  }

  /**
   * Advance in-order planning until the next subtask is available.
   *
   * <p>Waits for footer futures strictly in file-list order, so the plan is deterministic for a
   * given file list. Downloads are unaffected by this ordering: they were chained on each
   * footer's own completion.</p>
   */
  private ReadSubtask nextPlannedSubtask() throws Exception {
    while (plannedSubtasks.isEmpty()) {
      List<CompletableFuture<FooterHandle>> futures;
      synchronized (lifecycleLock) {
        futures = footerFutures;
      }
      if (footerCursor >= futures.size()) {
        if (planningFinished) {
          return null;
        }
        planningFinished = true;
        plannedSubtasks.addAll(session.finish());
        continue;
      }
      long waitStart = System.nanoTime();
      FooterHandle handle = getFutureResult(futures.get(footerCursor));
      adapter.onFooterWait(System.nanoTime() - waitStart);
      footerCursor++;
      adapter.onFooterCompleted(handle.footerNanos);
      checkOpen();
      fragmentByFooter.put(handle.footer, handle.fragment);
      footerOrder.put(handle.footer, consumedFragments.size());
      consumedFragments.add(handle.fragment);
      plannedSubtasks.addAll(session.add(handle.footer));
    }
    return plannedSubtasks.poll();
  }

  /**
   * Wait for the subtask's fragments, assemble the exact synthetic Parquet buffer on the task
   * thread, and hand it to GPU decode. Fragment download measurements are attributed to the
   * first consuming subtask; assembly time is reported as combine time.
   */
  private Iterator<ColumnarBatch> assembleAndDecode(ReadSubtask subtask) throws Exception {
    List<ReadSubtask.FileSlice> slices = subtask.getFileSlices();
    ArrayList<FileFragment> fragments = new ArrayList<>(slices.size());
    long waitStart = System.nanoTime();
    for (ReadSubtask.FileSlice slice : slices) {
      CompletableFuture<FileFragment> future = fragmentByFooter.get(slice.getFooter());
      if (future == null) {
        throw new IllegalStateException("subtask references an unconsumed footer");
      }
      fragments.add(getFutureResult(future));
    }
    adapter.onResultWait(System.nanoTime() - waitStart);
    checkOpen();

    long ioNanos = 0L;
    long cacheHitCount = 0L;
    long cacheHitBytes = 0L;
    long cacheMissCount = 0L;
    long cacheMissBytes = 0L;
    long cacheReadNanos = 0L;
    for (FileFragment fragment : fragments) {
      if (statsAttributed.add(fragment)) {
        FileFragment.DownloadStats stats = fragment.getStats();
        ioNanos = Math.addExact(ioNanos, stats.ioNanos);
        cacheHitCount += stats.cacheHitCount;
        cacheHitBytes = Math.addExact(cacheHitBytes, stats.cacheHitBytes);
        cacheMissCount += stats.cacheMissCount;
        cacheMissBytes = Math.addExact(cacheMissBytes, stats.cacheMissBytes);
        cacheReadNanos = Math.addExact(cacheReadNanos, stats.cacheReadNanos);
      }
    }

    long assembleStart = System.nanoTime();
    long materializeNanos = 0L;
    byte[] headerBytes = subtask.getHeaderBytes();
    byte[] footerBytes = subtask.getFooterAndTrailerBytes();
    HostMemoryBuffer synthetic =
        HostAlloc$.MODULE$.alloc(subtask.getTotalSizeBytes(), false);
    boolean handedOff = false;
    try {
      synthetic.setBytes(0L, headerBytes, 0, headerBytes.length);
      long outputOffset = headerBytes.length;
      for (int index = 0; index < slices.size(); index++) {
        ReadSubtask.FileSlice slice = slices.get(index);
        FileFragment fragment = fragments.get(index);
        long sliceBytes = fragment.sliceBytes(slice.getFirstBlock(), slice.getBlockCount());
        if (sliceBytes > 0) {
          long materializeStart = System.nanoTime();
          HostMemoryBuffer fragmentData = fragment.getData().materialize();
          materializeNanos += System.nanoTime() - materializeStart;
          try {
            synthetic.copyFromHostBuffer(outputOffset, fragmentData,
                fragment.blockStartOffset(slice.getFirstBlock()), sliceBytes);
          } finally {
            fragmentData.close();
          }
        }
        outputOffset = Math.addExact(outputOffset, sliceBytes);
      }
      if (outputOffset != subtask.getFooterOffset()) {
        throw new IllegalStateException("assembled data does not match the planned layout");
      }
      synthetic.setBytes(outputOffset, footerBytes, 0, footerBytes.length);

      // Keep the assembly and materialization metrics disjoint: fragment restores are reported
      // through onMaterializationCompleted only.
      long combineNanos = System.nanoTime() - assembleStart - materializeNanos;
      adapter.onSubtaskCompleted(subtask, new SubtaskStats(
          ioNanos, Math.max(combineNanos, 0L), false,
          cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos));
      adapter.onMaterializationCompleted(materializeNanos);
      closeFragmentsThrough(maxFooterOrder(slices));

      // Ownership transfers when the adapter is invoked, even on failure.
      handedOff = true;
      Iterator<ColumnarBatch> decoded = adapter.decodeAndPostProcess(subtask, synthetic);
      if (decoded == null) {
        throw new IllegalStateException("decode adapter returned null");
      }
      return decoded;
    } finally {
      if (!handedOff) {
        synthetic.close();
      }
    }
  }

  private int maxFooterOrder(List<ReadSubtask.FileSlice> slices) {
    int max = 0;
    for (ReadSubtask.FileSlice slice : slices) {
      Integer order = footerOrder.get(slice.getFooter());
      if (order != null && order > max) {
        max = order;
      }
    }
    return max;
  }

  /**
   * Close fragments whose footer position is before {@code exclusiveEnd}. In-order planning
   * guarantees the footer positions across consecutive subtasks never decrease, so a fragment
   * behind the newest consumed position can never be referenced again.
   */
  private void closeFragmentsThrough(int exclusiveEnd) {
    while (fragmentsClosedUpTo < exclusiveEnd) {
      CompletableFuture<FileFragment> future = consumedFragments.get(fragmentsClosedUpTo);
      future.whenComplete((fragment, error) -> {
        if (fragment != null) {
          closeFragmentAfterAsync(fragment);
        }
      });
      fragmentsClosedUpTo++;
    }
  }

  private void closeFragmentAfterAsync(FileFragment fragment) {
    try {
      fragment.close();
    } catch (Throwable error) {
      recordAsynchronousCleanupFailure(error);
    }
  }

  /** Obtain a per-file future's result and preserve its original failure type. */
  private <T> T getFutureResult(CompletableFuture<T> future) throws Exception {
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
   * Release the task-wide GPU permit at a task-thread CPU/I/O boundary.
   *
   * <p>This helper must never be called by the footer or download pool workers.
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
    // Wake the task thread wherever it waits by completing every per-file future exceptionally.
    // A download job that loses this race observes complete() == false and reclaims its own
    // fragment; footer and download callables are never cancelled, they drain naturally.
    CancellationException cancelled =
        new CancellationException("staged Parquet reader is closed");
    List<CompletableFuture<FooterHandle>> footerSnapshot;
    List<CompletableFuture<FileFragment>> fragmentSnapshot;
    synchronized (lifecycleLock) {
      footerSnapshot = new ArrayList<>(footerFutures);
      fragmentSnapshot = new ArrayList<>(fragmentRegistry);
    }
    for (CompletableFuture<FooterHandle> future : footerSnapshot) {
      future.completeExceptionally(cancelled);
    }
    for (CompletableFuture<FileFragment> future : fragmentSnapshot) {
      future.completeExceptionally(cancelled);
      // Reclaim fragments that completed before close or complete later without a consumer.
      // Consumed fragments close twice harmlessly: fragment close is idempotent.
      future.whenComplete((fragment, error) -> {
        if (fragment != null) {
          closeFragmentAfterAsync(fragment);
        }
      });
    }
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

  /** One consumed footer paired with worker timing and its file's in-flight download. */
  private static final class FooterHandle {
    private final FooterResult footer;
    private final long footerNanos;
    private final CompletableFuture<FileFragment> fragment;

    private FooterHandle(
        FooterResult footer,
        long footerNanos,
        CompletableFuture<FileFragment> fragment) {
      this.footer = footer;
      this.footerNanos = footerNanos;
      this.fragment = fragment;
    }
  }
}
