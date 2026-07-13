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

import java.io.EOFException;
import java.nio.ByteBuffer;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.GpuSemaphore$;
import com.nvidia.spark.rapids.IcebergS3RangeCopier.FileChannelCopyRange;
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

import org.apache.spark.TaskContext;
import org.apache.spark.sql.rapids.execution.TrampolineUtil$;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Event-driven, disk-first Iceberg Parquet reader for one Spark input partition.
 *
 * <p>One fused shared-pool job per file publishes its filtered footer and then downloads every
 * selected column chunk directly into an exact file-cache reservation. Synchronized callbacks on
 * this reader serialize the incremental planner in footer-completion order. When a combination
 * closes, the planner asynchronously leases one of a small executor-wide set of assembly buffers
 * and submits a high-priority cache-to-buffer fill. The Spark task thread never plans, downloads,
 * or assembles: it waits for a completed assembly buffer and invokes the existing GPU decoder.</p>
 *
 * <p>The POC intentionally supports completion order only. Scans requiring Iceberg's ordered
 * route are rejected before this reader is constructed.</p>
 */
public final class StagedParquetPartitionReader
    implements Iterator<ColumnarBatch>, AutoCloseable {
  // A reader may prepare one input for immediate decode and one input ahead. Admission starts
  // when a global slot is paired with a plan and ends only after PreparedSubtask.close returns
  // that slot. This prevents one partition from monopolizing the executor-wide assembly pool.
  private static final int MAX_ADMITTED_ASSEMBLIES = 2;

  private final List<IcebergPartitionedFile> files;
  private final StagedScanAdapter adapter;
  private final StableGreedyReadPlanner planner;
  private final boolean combineEnabled;
  private final long combineWaitMs;
  private final StagedScanThreadPools pools;
  private final TaskContext taskContext;
  private final long taskAttemptId;

  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Throwable> asynchronousCleanupFailure = new AtomicReference<>();
  private final Object iteratorLock = new Object();

  // All fields below through terminalFailure are guarded by this reader's monitor. Completable
  // futures invoke the synchronized callbacks directly; there is no planner event queue/thread.
  private StableGreedyReadPlanner.Session session;
  private boolean initialized;
  private boolean planningFinished;
  private int footersCompleted;
  private int downloadsCompleted;
  private long combineGeneration;
  private long unreportedFooterNanos;
  private long unreportedPlanningNanos;
  private Throwable terminalFailure;
  private final Map<Integer, TimedFooter> footerByIndex = new java.util.HashMap<>();
  private final Map<FooterResult, DiskReadyFile> diskFiles = new IdentityHashMap<>();
  private final ArrayDeque<AssemblyPlan> pendingAssembly = new ArrayDeque<>();
  private final ArrayDeque<PreparedSubtask> readyAssembly = new ArrayDeque<>();
  private final List<CompletableFuture<DiskReadyFile>> fileFutures = new ArrayList<>();
  private CompletableFuture<AssemblyBufferPool.Lease> pendingLease;
  private CompletableFuture<PreparedSubtask> taskWaiter;
  private int assembliesInFlight;
  private int assembliesAdmitted;

  // Spark-task iterator state. GPU decode remains task-confined.
  private Iterator<ColumnarBatch> currentBatches;

  public StagedParquetPartitionReader(
      List<IcebergPartitionedFile> files,
      StagedScanAdapter adapter,
      int maxRows,
      long maxEstimatedGpuBytes,
      long combineThreshold,
      long combineWaitMs,
      int workerThreads,
      int assemblyBufferCount,
      TaskContext taskContext) {
    this.files = immutableFileCopy(files);
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    this.planner = new StableGreedyReadPlanner(
        maxRows, maxEstimatedGpuBytes, combineThreshold);
    this.combineEnabled = combineThreshold > 0L;
    this.combineWaitMs = Math.max(0L, combineWaitMs);
    this.pools = StagedScanThreadPools.getOrCreate(workerThreads, assemblyBufferCount);
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

        long waitStart = System.nanoTime();
        PreparedSubtask prepared;
        try {
          CompletableFuture<PreparedSubtask> ready = nextReady();
          if (!ready.isDone() && taskContext != null) {
            // Unlike the baseline reader, the staged reader assembles off the Spark task thread.
            // There is no GPU work left to protect until this future supplies a complete input.
            // A task can arrive here while holding GpuSemaphore because a downstream operator
            // asked the scan for another batch. Do not hold that permit while waiting for one of
            // the bounded assembly slots. Otherwise all slots can be owned by tasks waiting to
            // acquire GpuSemaphore while the permit owners wait here for those same slots.
            // decodeAndPostProcess acquires the permit again after the assembled input is ready.
            GpuSemaphore$.MODULE$.releaseIfNecessary(taskContext);
          }
          prepared = await(ready);
        } finally {
          adapter.onResultWait(System.nanoTime() - waitStart);
        }
        long completedFooterNanos = drainFooterNanos();
        if (completedFooterNanos > 0L) {
          adapter.onFooterCompleted(completedFooterNanos);
        }
        if (prepared == null) {
          closeDiskFilesAfterEnd();
          return false;
        }
        Throwable failureAfterWait;
        synchronized (this) {
          failureAfterWait = terminalFailure;
        }
        if (closed.get()) {
          prepared.close();
          return false;
        }
        if (failureAfterWait != null) {
          prepared.close();
          throw propagate(failureAfterWait);
        }

        Iterator<ColumnarBatch> decoded = null;
        boolean installed = false;
        try {
          adapter.onSubtaskCompleted(prepared.subtask, prepared.stats);
          adapter.onMaterializationCompleted(prepared.assemblyNanos);
          decoded = adapter.decodeAndPostProcess(prepared.subtask, prepared.input);
          if (decoded == null) {
            throw new IllegalStateException("decode adapter returned null");
          }
          synchronized (iteratorLock) {
            if (closed.get()) {
              return false;
            }
            currentBatches = decoded;
            installed = true;
          }
        } finally {
          // The Scala adapter eagerly drains MakeParquetTableProducer before returning, so the
          // reusable encoded-input slot can be returned before downstream consumes GPU batches.
          prepared.close();
          if (decoded != null && !installed) {
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
        if (currentBatches == null || closed.get()) {
          throw new CancellationException("staged Parquet reader closed before next()");
        }
        return currentBatches.next();
      }
    } catch (Throwable error) {
      closeAfterFailure(error);
      throw propagate(error);
    }
  }

  /** Submit every fused footer/direct-cache file job lazily on the first iterator access. */
  private void initializeIfNeeded() {
    synchronized (this) {
      if (initialized) {
        return;
      }
      checkOpen();
      initialized = true;
      session = planner.newSession();
      if (files.isEmpty()) {
        planningFinished = true;
        session.finish();
        return;
      }
    }

    for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
      if (closed.get()) {
        break;
      }
      final int index = fileIndex;
      IcebergPartitionedFile file = files.get(index);
      CompletableFuture<TimedFooter> footerFuture = new CompletableFuture<>();
      footerFuture.whenComplete((footer, error) -> {
        if (error != null) {
          failPlanner(unwrap(error));
        } else {
          try {
            onFooterReady(index, footer);
          } catch (Throwable callbackError) {
            failPlanner(unwrap(callbackError));
          }
        }
      });
      CompletableFuture<DiskReadyFile> fileFuture = pools.submitFile(
          () -> executeFileJob(file, footerFuture));
      fileFuture.whenComplete((readyFile, error) -> {
        if (error != null) {
          footerFuture.completeExceptionally(unwrap(error));
          failPlanner(unwrap(error));
        } else if (readyFile == null) {
          IllegalStateException missingResult = new IllegalStateException(
              "file job completed without a cache-ready file");
          footerFuture.completeExceptionally(missingResult);
          failPlanner(missingResult);
        } else {
          try {
            onDownloadReady(index, readyFile);
          } catch (Throwable callbackError) {
            readyFile.close();
            failPlanner(unwrap(callbackError));
          }
        }
      });
      synchronized (this) {
        fileFutures.add(fileFuture);
      }
    }
  }

  /** One fused worker job publishes its footer, then writes selected bytes directly to cache. */
  private DiskReadyFile executeFileJob(
      IcebergPartitionedFile file,
      CompletableFuture<TimedFooter> footerFuture) throws Exception {
    return runAsTaskPoolThread(() -> {
      long footerStart = System.nanoTime();
      FooterResult footer = adapter.readAndFilterFooter(file);
      if (footer == null) {
        throw new IllegalStateException("footer adapter returned null");
      }
      long footerNanos = System.nanoTime() - footerStart;
      TimedFooter timedFooter = new TimedFooter(footer, footerNanos);
      footerFuture.complete(timedFooter);
      return downloadToCache(timedFooter);
    });
  }

  /**
   * Reserve one exact cache file per original column chunk and stream cache misses directly from
   * the Iceberg S3 client. Adjacent source ranges are coalesced by the private copier while its
   * response is routed across the distinct cache-file channels.
   */
  private DiskReadyFile downloadToCache(TimedFooter timedFooter) throws Exception {
    checkOpen();
    long ioStart = System.nanoTime();
    RapidsInputFile input = adapter.openInputFile(timedFooter.footer.getFile());
    if (input == null) {
      throw new IllegalStateException("input-file adapter returned null");
    }
    FileCache cache = FileCache.get();
    ArrayList<List<PendingRange>> pendingByBlock = new ArrayList<>();
    ArrayList<PendingRange> allRanges = new ArrayList<>();
    ArrayList<OwnedWriter> ownedWriters = new ArrayList<>();
    ArrayList<FileChannelCopyRange> remoteRanges = new ArrayList<>();
    ArrayList<DiskReadyFile.CachedRange> claimedRanges = new ArrayList<>();
    boolean leasesTransferred = false;
    long cacheHitCount = 0L;
    long cacheHitBytes = 0L;
    long cacheMissCount = 0L;
    long cacheMissBytes = 0L;

    try {
      for (BlockMetaData block : timedFooter.footer.getBlocks()) {
        ArrayList<PendingRange> blockRanges = new ArrayList<>();
        for (ColumnChunkMetaData column : block.getColumns()) {
          long sourceOffset = column.getStartingPos();
          long length = column.getTotalSize();
          Optional<FileCacheDataRangeReservation> optional =
              cache.reserveDataRangeCache(input, sourceOffset, length);
          if (!optional.isPresent()) {
            throw new IllegalStateException(
                "staged Iceberg POC requires a writable file-cache reservation for " +
                    input.path() + " range " + sourceOffset + ":" + length);
          }
          FileCacheDataRangeReservation reservation = optional.get();
          boolean cacheHit = reservation.isCacheHit();
          PendingRange pending = new PendingRange(length, cacheHit, reservation);
          blockRanges.add(pending);
          allRanges.add(pending);
          if (cacheHit) {
            cacheHitCount++;
            cacheHitBytes = Math.addExact(cacheHitBytes, length);
          } else {
            cacheMissCount++;
            cacheMissBytes = Math.addExact(cacheMissBytes, length);
          }

          Optional<FileCacheDataRangeWriter> writer = reservation.getWriter();
          if (writer.isPresent()) {
            OwnedWriter owned = new OwnedWriter(writer.get());
            ownedWriters.add(owned);
            if (length > 0L) {
              remoteRanges.add(new FileChannelCopyRange(
                  sourceOffset, length, owned.writer.getChannel(), 0L));
            }
          }
        }
        pendingByBlock.add(blockRanges);
      }

      remoteRanges.sort(Comparator.comparingLong(FileChannelCopyRange::getInputOffset));
      long requestCount = countCoalescedRequests(remoteRanges);
      long requestedBytes = totalRangeBytes(remoteRanges);
      long remoteStart = System.nanoTime();
      if (!(input instanceof IcebergS3InputFile)) {
        throw new IllegalStateException(
            "staged Iceberg POC requires IcebergS3InputFile, found " +
                input.getClass().getName());
      }
      long actualRemoteBytes =
          ((IcebergS3InputFile) input).readVectoredToFileChannels(remoteRanges);
      long remoteNanos = System.nanoTime() - remoteStart;
      if (actualRemoteBytes != requestedBytes) {
        throw new EOFException(
            "direct cache download wrote " + actualRemoteBytes +
                " bytes, expected " + requestedBytes);
      }
      checkOpen();

      long commitStart = System.nanoTime();
      for (OwnedWriter owned : ownedWriters) {
        // commit() makes the writer terminal before it starts publication. Mark our wrapper
        // first so a synchronous commit failure is not followed by an invalid cancel().
        owned.terminal = true;
        owned.writer.commit();
      }
      CompletableFuture<?>[] completions = allRanges.stream()
          .map(range -> range.reservation.getCompletionFuture())
          .toArray(CompletableFuture<?>[]::new);
      CompletableFuture.allOf(completions).get();

      ArrayList<List<DiskReadyFile.CachedRange>> readyByBlock = new ArrayList<>();
      for (List<PendingRange> block : pendingByBlock) {
        ArrayList<DiskReadyFile.CachedRange> readyBlock = new ArrayList<>();
        for (PendingRange pending : block) {
          FileCacheDataRangeLease lease = pending.reservation.getCompletionFuture().get();
          pending.leaseClaimed = true;
          DiskReadyFile.CachedRange readyRange = new DiskReadyFile.CachedRange(
              pending.length, pending.cacheHit, lease);
          claimedRanges.add(readyRange);
          readyBlock.add(readyRange);
        }
        readyByBlock.add(readyBlock);
      }
      long commitNanos = System.nanoTime() - commitStart;
      DiskReadyFile readyFile = new DiskReadyFile(timedFooter.footer, readyByBlock,
          new DiskReadyFile.DownloadStats(
              System.nanoTime() - ioStart,
              remoteNanos,
              commitNanos,
              requestCount,
              requestedBytes,
              cacheHitCount,
              cacheHitBytes,
              cacheMissCount,
              cacheMissBytes));
      leasesTransferred = true;
      return readyFile;
    } catch (Throwable error) {
      Throwable failure = unwrap(error);
      if (failure instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      for (OwnedWriter owned : ownedWriters) {
        if (!owned.terminal) {
          try {
            owned.writer.cancel();
            owned.terminal = true;
          } catch (Throwable cancelError) {
            failure = addFailure(failure, cancelError);
          }
        }
      }
      for (PendingRange pending : allRanges) {
        if (!pending.leaseClaimed) {
          pending.reservation.getCompletionFuture().whenComplete((lease, ignored) -> {
            if (lease != null) {
              try {
                lease.close();
              } catch (Throwable cleanupError) {
                recordAsynchronousCleanupFailure(cleanupError);
              }
            }
          });
        }
      }
      if (!leasesTransferred) {
        closeAll(claimedRanges);
      }
      throw propagate(failure);
    }
  }

  private static long countCoalescedRequests(List<FileChannelCopyRange> ranges) {
    long requests = 0L;
    long previousEnd = -1L;
    for (FileChannelCopyRange range : ranges) {
      if (requests == 0L || range.getInputOffset() != previousEnd) {
        requests++;
      }
      previousEnd = Math.addExact(range.getInputOffset(), range.getLength());
    }
    return requests;
  }

  private static long totalRangeBytes(List<FileChannelCopyRange> ranges) {
    long total = 0L;
    for (FileChannelCopyRange range : ranges) {
      total = Math.addExact(total, range.getLength());
    }
    return total;
  }

  /**
   * Admit one footer in callback completion order and advance incremental planning.
   *
   * <p>Planning deliberately does not wait for the corresponding download. Emitted subtasks stay
   * as metadata-only assembly plans until all of their cache-ready files have been published.</p>
   */
  private void onFooterReady(int fileIndex, TimedFooter footer) {
    CompletableFuture<PreparedSubtask> endWaiter = null;
    boolean scheduleTimeout = false;
    long timeoutGeneration = 0L;
    Throwable failure = null;
    synchronized (this) {
      if (closed.get() || terminalFailure != null) {
        return;
      }
      try {
        if (footerByIndex.put(fileIndex, footer) != null) {
          throw new IllegalStateException("footer completed more than once for file " + fileIndex);
        }
        footersCompleted++;
        unreportedFooterNanos = Math.addExact(
            unreportedFooterNanos, footer.footerNanos);
        combineGeneration++;

        long planningStart = System.nanoTime();
        List<ReadSubtask> emitted = session.add(footer.footer);
        unreportedPlanningNanos = Math.addExact(
            unreportedPlanningNanos, System.nanoTime() - planningStart);
        List<AssemblyPlan> plans = createAssemblyPlansLocked(emitted);

        if (footersCompleted == files.size()) {
          planningFinished = true;
          long finishStart = System.nanoTime();
          List<ReadSubtask> finalSubtasks = session.finish();
          unreportedPlanningNanos = Math.addExact(
              unreportedPlanningNanos, System.nanoTime() - finishStart);
          plans = append(plans, createAssemblyPlansLocked(finalSubtasks));
        } else if (combineEnabled && session.hasOpenBlocks()) {
          if (combineWaitMs > 0L) {
            scheduleTimeout = true;
            timeoutGeneration = combineGeneration;
          } else {
            long flushStart = System.nanoTime();
            List<ReadSubtask> flushed = session.flush();
            unreportedPlanningNanos = Math.addExact(
                unreportedPlanningNanos, System.nanoTime() - flushStart);
            plans = append(plans, createAssemblyPlansLocked(flushed));
          }
        }
        pendingAssembly.addAll(plans);
        endWaiter = detachEndWaiterIfReadyLocked();
      } catch (Throwable plannerError) {
        failure = unwrap(plannerError);
      }
    }

    if (failure != null) {
      failPlanner(failure);
      return;
    }
    if (endWaiter != null) {
      endWaiter.complete(null);
    }
    if (scheduleTimeout) {
      try {
        long generation = timeoutGeneration;
        pools.schedule(() -> onCombineTimeout(generation),
            combineWaitMs, TimeUnit.MILLISECONDS);
      } catch (Throwable scheduleError) {
        failPlanner(unwrap(scheduleError));
        return;
      }
    }
    requestAssemblySlotIfNeeded();
  }

  /** Publish one cache-ready file and wake any metadata plan whose inputs are now ready. */
  private void onDownloadReady(int fileIndex, DiskReadyFile readyFile) {
    CompletableFuture<PreparedSubtask> endWaiter = null;
    boolean discard = false;
    Throwable failure = null;
    synchronized (this) {
      if (closed.get() || terminalFailure != null) {
        discard = true;
      } else {
        TimedFooter timed = footerByIndex.get(fileIndex);
        if (timed == null || timed.footer != readyFile.footer()) {
          failure = new IllegalStateException(
              "download completed before its matching footer event");
          discard = true;
        } else if (diskFiles.containsKey(readyFile.footer())) {
          failure = new IllegalStateException(
              "download completed more than once for file " + fileIndex);
          discard = true;
        } else {
          downloadsCompleted++;
          diskFiles.put(readyFile.footer(), readyFile);
          endWaiter = detachEndWaiterIfReadyLocked();
        }
      }
    }

    if (discard) {
      readyFile.close();
      if (failure != null) {
        failPlanner(failure);
      }
      return;
    }
    if (endWaiter != null) {
      endWaiter.complete(null);
    }
    requestAssemblySlotIfNeeded();
  }

  /** Flush the current combination when its latest fresh 200-ms grace expires. */
  private void onCombineTimeout(long generation) {
    CompletableFuture<PreparedSubtask> endWaiter = null;
    Throwable failure = null;
    synchronized (this) {
      if (closed.get() || terminalFailure != null || planningFinished ||
          generation != combineGeneration || !session.hasOpenBlocks()) {
        return;
      }
      try {
        long planningStart = System.nanoTime();
        List<ReadSubtask> flushed = session.flush();
        unreportedPlanningNanos = Math.addExact(
            unreportedPlanningNanos, System.nanoTime() - planningStart);
        List<AssemblyPlan> plans = createAssemblyPlansLocked(flushed);
        pendingAssembly.addAll(plans);
        endWaiter = detachEndWaiterIfReadyLocked();
      } catch (Throwable plannerError) {
        failure = unwrap(plannerError);
      }
    }
    if (failure != null) {
      failPlanner(failure);
      return;
    }
    if (endWaiter != null) {
      endWaiter.complete(null);
    }
    requestAssemblySlotIfNeeded();
  }

  /** Convert planner output to metadata-only plans while still under the planner monitor. */
  private List<AssemblyPlan> createAssemblyPlansLocked(List<ReadSubtask> subtasks) {
    if (subtasks.isEmpty()) {
      return Collections.emptyList();
    }
    ArrayList<AssemblyPlan> plans = new ArrayList<>(subtasks.size());
    try {
      for (ReadSubtask subtask : subtasks) {
        long planningNanos = unreportedPlanningNanos;
        plans.add(new AssemblyPlan(subtask, planningNanos));
        unreportedPlanningNanos = 0L;
      }
    } catch (Throwable error) {
      closeAll(plans);
      throw error;
    }
    return plans;
  }

  private static <T> List<T> append(List<T> first, List<T> second) {
    if (first.isEmpty()) {
      return second;
    }
    if (second.isEmpty()) {
      return first;
    }
    ArrayList<T> combined = new ArrayList<>(first.size() + second.size());
    combined.addAll(first);
    combined.addAll(second);
    return combined;
  }

  /** Request at most one fair global assembly lease ahead of this partition's pending plans. */
  private void requestAssemblySlotIfNeeded() {
    CompletableFuture<AssemblyBufferPool.Lease> request;
    synchronized (this) {
      if (closed.get() || terminalFailure != null || !hasReadyAssemblyPlanLocked() ||
          pendingLease != null || assembliesAdmitted >= MAX_ADMITTED_ASSEMBLIES) {
        return;
      }
      request = pools.assemblyBuffers().acquire();
      pendingLease = request;
    }
    // A queued lease is commonly released by the Spark task after decode. Dispatch the state
    // transition back to the shared pool so the task thread never selects a plan or claims cache
    // ranges. Executor rejection is handled inline only as a terminal cleanup path.
    request.whenComplete((lease, error) -> {
      CompletableFuture<Void> dispatched = pools.submitAssembly(() -> {
        try {
          onAssemblySlotAvailable(request, lease, error);
        } catch (Throwable callbackError) {
          if (lease != null) {
            lease.close();
          }
          failPlanner(unwrap(callbackError));
        }
        return null;
      });
      dispatched.whenComplete((ignored, dispatchError) -> {
        if (dispatchError != null) {
          Throwable failure = unwrap(dispatchError);
          if (error != null) {
            failure = addFailure(unwrap(error), failure);
          }
          onAssemblySlotAvailable(request, lease, failure);
        }
      });
    });
  }

  /** Pair the next planned subtask with a nonblocking assembly slot and submit its fill. */
  private void onAssemblySlotAvailable(
      CompletableFuture<AssemblyBufferPool.Lease> request,
      AssemblyBufferPool.Lease lease,
      Throwable error) {
    AssemblyPlan plan = null;
    AssemblyAdmission admission = null;
    boolean discardLease = false;
    Throwable failure = null;
    synchronized (this) {
      if (pendingLease == request) {
        pendingLease = null;
      }
      if (error != null) {
        // Failure is published below, outside this monitor.
      } else if (closed.get() || terminalFailure != null) {
        discardLease = lease != null;
      } else if (lease == null) {
        failure = new IllegalStateException(
            "assembly buffer acquisition completed without a lease");
      } else {
        plan = pollReadyAssemblyPlanLocked();
        if (plan == null) {
          discardLease = lease != null;
        } else {
          try {
            // Cache leases are claimed only after a bounded assembly slot has been granted.
            plan.claimRanges(diskFiles);
            admission = new AssemblyAdmission(this);
            assembliesInFlight++;
            assembliesAdmitted++;
          } catch (Throwable claimError) {
            failure = unwrap(claimError);
            discardLease = lease != null;
          }
        }
      }
    }

    if (error != null) {
      if (lease != null) {
        lease.close();
      }
      failPlanner(unwrap(error));
      return;
    }
    if (discardLease) {
      lease.close();
    }
    if (failure != null) {
      if (plan != null) {
        plan.close();
      }
      failPlanner(failure);
      return;
    }
    if (discardLease) {
      requestAssemblySlotIfNeeded();
      return;
    }
    if (plan != null) {
      AssemblyPlan selectedPlan = plan;
      AssemblyAdmission selectedAdmission = admission;
      pools.submitAssembly(() -> runAsTaskPoolThread(
          () -> assemble(selectedPlan, lease, selectedAdmission)))
          .whenComplete((prepared, assemblyError) -> {
            Throwable completionError =
                assemblyError == null ? null : unwrap(assemblyError);
            if (assemblyError != null) {
              // submitAssembly reports executor rejection through the future without running the
              // callable. Closing here is idempotent with assemble()'s normal failure cleanup.
              try {
                selectedPlan.close();
              } catch (Throwable cleanupError) {
                completionError = addFailure(completionError, cleanupError);
              }
              try {
                lease.close();
              } catch (Throwable cleanupError) {
                completionError = addFailure(completionError, cleanupError);
              }
            }
            try {
              onAssemblyReady(prepared, completionError, selectedAdmission);
            } catch (Throwable callbackError) {
              if (prepared != null) {
                prepared.close();
              }
              selectedAdmission.close();
              failPlanner(unwrap(callbackError));
            }
          });
    }
    requestAssemblySlotIfNeeded();
  }

  /** Return whether any planned subtask has all of its cache-ready file dependencies. */
  private boolean hasReadyAssemblyPlanLocked() {
    for (AssemblyPlan plan : pendingAssembly) {
      if (plan.isReady(diskFiles)) {
        return true;
      }
    }
    return false;
  }

  /** Remove the first planner-order subtask whose downloads are all cache-ready. */
  private AssemblyPlan pollReadyAssemblyPlanLocked() {
    Iterator<AssemblyPlan> plans = pendingAssembly.iterator();
    while (plans.hasNext()) {
      AssemblyPlan plan = plans.next();
      if (plan.isReady(diskFiles)) {
        plans.remove();
        return plan;
      }
    }
    return null;
  }

  /** Fill one reusable buffer from pinned cache channels in exact synthetic-file order. */
  private PreparedSubtask assemble(
      AssemblyPlan plan,
      AssemblyBufferPool.Lease lease,
      AssemblyAdmission admission) throws Exception {
    long assemblyStart = System.nanoTime();
    long allocNanos = 0L;
    long cacheReadNanos = 0L;
    long allDiskReadNanos = 0L;
    boolean leaseTransferred = false;
    try {
      checkOpen();
      ReadSubtask subtask = plan.subtask;
      allocNanos = lease.ensureCapacity(subtask.getTotalSizeBytes());
      HostMemoryBuffer output = lease.buffer();
      byte[] header = subtask.getHeaderBytes();
      output.setBytes(0L, header, 0, header.length);
      long outputOffset = header.length;

      for (DiskReadyFile.CachedRange range : plan.ranges) {
        long readStart = System.nanoTime();
        copyCachedRange(range.channel(), output, outputOffset, range.length());
        long elapsed = System.nanoTime() - readStart;
        allDiskReadNanos = Math.addExact(allDiskReadNanos, elapsed);
        if (range.initialCacheHit()) {
          cacheReadNanos = Math.addExact(cacheReadNanos, elapsed);
        }
        outputOffset = Math.addExact(outputOffset, range.length());
        range.close();
      }
      if (outputOffset != subtask.getFooterOffset()) {
        throw new IllegalStateException(
            "assembled data ended at " + outputOffset +
                ", expected footer at " + subtask.getFooterOffset());
      }
      byte[] footer = subtask.getFooterAndTrailerBytes();
      output.setBytes(outputOffset, footer, 0, footer.length);

      StagedParquetInput input = new StagedParquetInput(subtask.getTotalSizeBytes(), lease);
      AssemblyBufferPool.CapacitySnapshot capacity =
          pools.assemblyBuffers().capacitySnapshot();
      SubtaskStats stats = new SubtaskStats(
          plan.stats.ioNanos,
          allocNanos,
          plan.stats.remoteReadNanos,
          allDiskReadNanos,
          plan.stats.cacheCommitNanos,
          plan.stats.requestCount,
          plan.stats.requestedBytes,
          plan.planningNanos,
          true,
          plan.stats.cacheHitCount,
          plan.stats.cacheHitBytes,
          plan.stats.cacheMissCount,
          plan.stats.cacheMissBytes,
          cacheReadNanos,
          capacity.getCurrentCapacityBytes(),
          capacity.getPeakCapacityBytes());
      PreparedSubtask prepared = new PreparedSubtask(
          subtask, input, stats, System.nanoTime() - assemblyStart,
          admission);
      leaseTransferred = true;
      return prepared;
    } finally {
      plan.close();
      if (!leaseTransferred) {
        lease.close();
      }
    }
  }

  private static void copyCachedRange(
      SeekableByteChannel source,
      HostMemoryBuffer destination,
      long destinationOffset,
      long length) throws Exception {
    source.position(0L);
    long copied = 0L;
    while (copied < length) {
      int amount = (int) Math.min(length - copied, Integer.MAX_VALUE);
      ByteBuffer output = destination.asByteBuffer(destinationOffset + copied, amount);
      while (output.hasRemaining()) {
        int read = source.read(output);
        if (read < 0) {
          throw new EOFException(
              "cached range ended with " + output.remaining() + " bytes remaining");
        }
        if (read == 0) {
          Thread.yield();
        }
      }
      copied += amount;
    }
  }

  /** Publish completed assemblies in completion order; the task consumes only this output. */
  private void onAssemblyReady(
      PreparedSubtask prepared,
      Throwable error,
      AssemblyAdmission admission) {
    CompletableFuture<PreparedSubtask> waiter = null;
    boolean discard = false;
    Throwable failure = error == null ? null : unwrap(error);
    synchronized (this) {
      if (assembliesInFlight <= 0) {
        failure = addFailure(failure,
            new IllegalStateException("assembly completed without an in-flight plan"));
      } else {
        assembliesInFlight--;
      }
      if (failure == null && prepared == null) {
        failure = new IllegalStateException("assembly completed without a prepared subtask");
      }
      if (failure != null) {
        // Failure is handled below after releasing this monitor.
      } else if (closed.get() || terminalFailure != null) {
        discard = true;
      } else if (taskWaiter != null) {
        waiter = taskWaiter;
        taskWaiter = null;
      } else {
        readyAssembly.addLast(prepared);
      }
    }
    if (failure != null) {
      // Publish terminal state first so admission release cannot schedule replacement work.
      failPlanner(failure);
      if (prepared != null) {
        prepared.close();
      } else {
        admission.close();
      }
    } else if (discard) {
      prepared.close();
    } else if (waiter != null) {
      if (!waiter.complete(prepared)) {
        prepared.close();
      }
    }
    requestAssemblySlotIfNeeded();
  }

  /** Release one idempotent admission after its prepared input has returned the global slot. */
  private void onAssemblyAdmissionReleased() {
    synchronized (this) {
      if (assembliesAdmitted <= 0) {
        throw new IllegalStateException("prepared subtask closed without an assembly admission");
      }
      assembliesAdmitted--;
    }
    requestAssemblySlotIfNeeded();
  }

  /** Return a future for the next completion-order assembly result or the end sentinel (null). */
  private synchronized CompletableFuture<PreparedSubtask> nextReady() {
    // close() flips the atomic flag before taking this monitor. Without this check it can finish
    // between hasNext's outer check and this method, after which a newly installed waiter would
    // have no producer left to wake it.
    if (closed.get()) {
      return CompletableFuture.completedFuture(null);
    }
    if (!readyAssembly.isEmpty()) {
      return CompletableFuture.completedFuture(readyAssembly.pollFirst());
    }
    if (terminalFailure != null) {
      CompletableFuture<PreparedSubtask> failed = new CompletableFuture<>();
      failed.completeExceptionally(terminalFailure);
      return failed;
    }
    if (isEndReadyLocked()) {
      return CompletableFuture.completedFuture(null);
    }
    if (taskWaiter != null) {
      throw new IllegalStateException("Spark task already waits for a prepared subtask");
    }
    taskWaiter = new CompletableFuture<>();
    return taskWaiter;
  }

  private boolean isEndReadyLocked() {
    return planningFinished && downloadsCompleted == files.size() &&
        pendingAssembly.isEmpty() && pendingLease == null &&
        assembliesInFlight == 0 && assembliesAdmitted == 0 && readyAssembly.isEmpty();
  }

  private CompletableFuture<PreparedSubtask> detachEndWaiterIfReadyLocked() {
    if (taskWaiter != null && isEndReadyLocked()) {
      CompletableFuture<PreparedSubtask> waiter = taskWaiter;
      taskWaiter = null;
      return waiter;
    }
    return null;
  }

  /** Release the remaining cache pins after normal terminal consumption. */
  private void closeDiskFilesAfterEnd() {
    List<DiskReadyFile> remaining;
    synchronized (this) {
      if (!isEndReadyLocked() || diskFiles.isEmpty()) {
        return;
      }
      remaining = new ArrayList<>(diskFiles.values());
      diskFiles.clear();
    }
    closeAll(remaining);
  }

  /** Move completed footer-worker timing to the Spark task thread for metric publication. */
  private synchronized long drainFooterNanos() {
    long nanos = unreportedFooterNanos;
    unreportedFooterNanos = 0L;
    return nanos;
  }

  /** First pipeline failure wins and wakes the task; resource closing happens outside locks. */
  private void failPlanner(Throwable error) {
    CompletableFuture<PreparedSubtask> waiter;
    CompletableFuture<AssemblyBufferPool.Lease> leaseRequest;
    List<AssemblyPlan> pending;
    List<PreparedSubtask> ready;
    List<DiskReadyFile> diskReady;
    synchronized (this) {
      if (terminalFailure != null || closed.get()) {
        return;
      }
      terminalFailure = error;
      combineGeneration++;
      waiter = taskWaiter;
      taskWaiter = null;
      leaseRequest = pendingLease;
      pendingLease = null;
      pending = new ArrayList<>(pendingAssembly);
      pendingAssembly.clear();
      ready = new ArrayList<>(readyAssembly);
      readyAssembly.clear();
      diskReady = new ArrayList<>(diskFiles.values());
      diskFiles.clear();
    }
    releasePendingLeaseRequest(leaseRequest);
    closeAll(pending);
    closeAll(ready);
    closeAll(diskReady);
    if (waiter != null) {
      waiter.completeExceptionally(error);
    }
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
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
    Throwable failure;
    synchronized (this) {
      failure = terminalFailure;
    }
    if (failure != null) {
      throw propagate(failure);
    }
  }

  /** Install the captured Spark task/RMM identity around each blocking shared-pool operation. */
  private <T> T runAsTaskPoolThread(Callable<T> operation) throws Exception {
    if (taskContext == null) {
      return operation.call();
    }
    T result = null;
    Throwable failure = null;
    boolean operationCompleted = false;
    boolean contextInstalled = false;
    boolean rmmRegistered = false;
    try {
      TrampolineUtil$.MODULE$.setTaskContext(taskContext);
      contextInstalled = true;
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
        if (contextInstalled) {
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

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    CompletableFuture<PreparedSubtask> waiter;
    CompletableFuture<AssemblyBufferPool.Lease> leaseRequest;
    List<AssemblyPlan> pending;
    List<PreparedSubtask> ready;
    List<DiskReadyFile> diskReady;
    List<CompletableFuture<DiskReadyFile>> futures;
    synchronized (this) {
      combineGeneration++;
      waiter = taskWaiter;
      taskWaiter = null;
      leaseRequest = pendingLease;
      pendingLease = null;
      pending = new ArrayList<>(pendingAssembly);
      pendingAssembly.clear();
      ready = new ArrayList<>(readyAssembly);
      readyAssembly.clear();
      diskReady = new ArrayList<>(diskFiles.values());
      diskFiles.clear();
      futures = new ArrayList<>(fileFutures);
    }
    releasePendingLeaseRequest(leaseRequest);
    if (waiter != null) {
      waiter.complete(null);
    }
    closeAll(pending);
    closeAll(ready);
    closeAll(diskReady);
    for (CompletableFuture<DiskReadyFile> future : futures) {
      future.whenComplete((file, ignored) -> {
        if (file != null) {
          try {
            file.close();
          } catch (Throwable cleanupError) {
            recordAsynchronousCleanupFailure(cleanupError);
          }
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
    Throwable asyncFailure = asynchronousCleanupFailure.get();
    if (asyncFailure != null) {
      failure = addFailure(failure, asyncFailure);
    }
    if (failure != null) {
      throw propagate(failure);
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

  /** Cancel an ungranted slot request, or immediately return a slot granted before dispatch. */
  private static void releasePendingLeaseRequest(
      CompletableFuture<AssemblyBufferPool.Lease> leaseRequest) {
    if (leaseRequest == null || leaseRequest.cancel(false)) {
      return;
    }
    try {
      AssemblyBufferPool.Lease granted = leaseRequest.getNow(null);
      if (granted != null) {
        granted.close();
      }
    } catch (CancellationException | CompletionException ignored) {
      // An exceptional acquisition owns no slot. Its callback will observe the terminal reader.
    }
  }

  private void closeAfterFailure(Throwable original) {
    try {
      close();
    } catch (Throwable closeError) {
      original.addSuppressed(closeError);
    }
  }

  private static void closeAll(Iterable<? extends AutoCloseable> resources) {
    for (AutoCloseable resource : resources) {
      try {
        resource.close();
      } catch (Throwable ignored) {
        // A primary pipeline failure or cancellation is already being reported. Late cleanup
        // failures are best-effort here because there is no task-thread observer for them.
      }
    }
  }

  private static void closeIterator(Iterator<ColumnarBatch> iterator) throws Exception {
    if (iterator instanceof AutoCloseable) {
      ((AutoCloseable) iterator).close();
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

  /** Footer metadata published before its fused worker proceeds to direct-cache download. */
  private static final class TimedFooter {
    private final FooterResult footer;
    private final long footerNanos;

    private TimedFooter(FooterResult footer, long footerNanos) {
      this.footer = footer;
      this.footerNanos = footerNanos;
    }
  }

  /** One direct-cache reservation in original row-group/column order. */
  private static final class PendingRange {
    private final long length;
    private final boolean cacheHit;
    private final FileCacheDataRangeReservation reservation;
    private boolean leaseClaimed;

    private PendingRange(
        long length,
        boolean cacheHit,
        FileCacheDataRangeReservation reservation) {
      this.length = length;
      this.cacheHit = cacheHit;
      this.reservation = reservation;
    }
  }

  /** Tracks whether a writable reservation may still be canceled after a failed S3 operation. */
  private static final class OwnedWriter {
    private final FileCacheDataRangeWriter writer;
    private boolean terminal;

    private OwnedWriter(FileCacheDataRangeWriter writer) {
      this.writer = writer;
    }
  }

  /**
   * Immutable assembly work emitted by the synchronized planner.
   *
   * <p>It owns the exact cache leases for this subtask. Closing before successful assembly
   * releases every pin; successful assembly also closes them immediately after copying.</p>
   */
  private static final class AssemblyPlan implements AutoCloseable {
    private final ReadSubtask subtask;
    private final long planningNanos;
    private List<DiskReadyFile.CachedRange> ranges = Collections.emptyList();
    private DiskReadyFile.DownloadStats stats = DiskReadyFile.DownloadStats.EMPTY;
    private boolean rangesClaimed;

    private AssemblyPlan(
        ReadSubtask subtask,
        long planningNanos) {
      this.subtask = Objects.requireNonNull(subtask, "subtask");
      this.planningNanos = planningNanos;
    }

    /** Return whether every footer referenced by this metadata plan is cache-ready. */
    private boolean isReady(Map<FooterResult, DiskReadyFile> diskFiles) {
      for (ReadSubtask.FileSlice slice : subtask.getFileSlices()) {
        if (!diskFiles.containsKey(slice.getFooter())) {
          return false;
        }
      }
      return true;
    }

    /** Transfer this subtask's exact cache leases after an assembly slot has been granted. */
    private void claimRanges(Map<FooterResult, DiskReadyFile> diskFiles) {
      if (rangesClaimed) {
        throw new IllegalStateException("assembly plan cache ranges were already claimed");
      }
      ArrayList<DiskReadyFile.CachedRange> claimed = new ArrayList<>();
      DownloadStatsAccumulator accumulator = new DownloadStatsAccumulator();
      try {
        Set<DiskReadyFile> statsFiles =
            Collections.newSetFromMap(new IdentityHashMap<DiskReadyFile, Boolean>());
        for (ReadSubtask.FileSlice slice : subtask.getFileSlices()) {
          DiskReadyFile diskFile = diskFiles.get(slice.getFooter());
          if (diskFile == null) {
            throw new IllegalStateException("planned footer has no cache-ready file");
          }
          claimed.addAll(diskFile.takeRanges(slice.getFirstBlock(), slice.getBlockCount()));
          if (statsFiles.add(diskFile)) {
            accumulator.add(diskFile.claimStats());
          }
        }
        long bytes = 0L;
        for (DiskReadyFile.CachedRange range : claimed) {
          bytes = Math.addExact(bytes, range.length());
        }
        if (bytes != subtask.getDataSizeBytes()) {
          throw new IllegalStateException(
              "claimed cache bytes " + bytes +
                  " do not match planned bytes " + subtask.getDataSizeBytes());
        }
      } catch (Throwable error) {
        closeAll(claimed);
        throw error;
      }
      ranges = claimed;
      stats = accumulator.build();
      rangesClaimed = true;
    }

    @Override
    public void close() {
      closeAll(ranges);
    }
  }

  /** One plan's admission, released exactly once across success and exceptional cleanup paths. */
  private static final class AssemblyAdmission implements AutoCloseable {
    private final StagedParquetPartitionReader owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    private AssemblyAdmission(StagedParquetPartitionReader owner) {
      this.owner = owner;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        owner.onAssemblyAdmissionReleased();
      }
    }
  }

  /** Assembly output handed to the Spark task; owns the reusable slot until decode returns. */
  private static final class PreparedSubtask implements AutoCloseable {
    private final ReadSubtask subtask;
    private final StagedParquetInput input;
    private final SubtaskStats stats;
    private final long assemblyNanos;
    private final AssemblyAdmission admission;
    private final AtomicBoolean closed = new AtomicBoolean();

    private PreparedSubtask(
        ReadSubtask subtask,
        StagedParquetInput input,
        SubtaskStats stats,
        long assemblyNanos,
        AssemblyAdmission admission) {
      this.subtask = subtask;
      this.input = input;
      this.stats = stats;
      this.assemblyNanos = assemblyNanos;
      this.admission = admission;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        try {
          input.close();
        } finally {
          admission.close();
        }
      }
    }
  }

  /** Adds per-file worker measurements once when several files combine into one subtask. */
  private static final class DownloadStatsAccumulator {
    private long ioNanos;
    private long remoteReadNanos;
    private long cacheCommitNanos;
    private long requestCount;
    private long requestedBytes;
    private long cacheHitCount;
    private long cacheHitBytes;
    private long cacheMissCount;
    private long cacheMissBytes;

    private void add(DiskReadyFile.DownloadStats stats) {
      ioNanos = Math.addExact(ioNanos, stats.ioNanos);
      remoteReadNanos = Math.addExact(remoteReadNanos, stats.remoteReadNanos);
      cacheCommitNanos = Math.addExact(cacheCommitNanos, stats.cacheCommitNanos);
      requestCount = Math.addExact(requestCount, stats.requestCount);
      requestedBytes = Math.addExact(requestedBytes, stats.requestedBytes);
      cacheHitCount = Math.addExact(cacheHitCount, stats.cacheHitCount);
      cacheHitBytes = Math.addExact(cacheHitBytes, stats.cacheHitBytes);
      cacheMissCount = Math.addExact(cacheMissCount, stats.cacheMissCount);
      cacheMissBytes = Math.addExact(cacheMissBytes, stats.cacheMissBytes);
    }

    private DiskReadyFile.DownloadStats build() {
      return new DiskReadyFile.DownloadStats(
          ioNanos, remoteReadNanos, cacheCommitNanos, requestCount, requestedBytes,
          cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes);
    }
  }
}
