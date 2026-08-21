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

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.filecache.FileCache;
import com.nvidia.spark.rapids.filecache.FileCache.FileCacheStartedToken;
import com.nvidia.spark.rapids.jni.fileio.RapidsInputFile;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import scala.Option;

/**
 * Reads the filtered column chunks for one Iceberg Parquet file.
 *
 * <p>This is deliberately a concrete format implementation rather than a callback layer. The
 * caller opens the Iceberg input file and composes this operation on the shared reader executor.
 * Cache hits and remote ranges are copied into one spillable packed fragment, but an Iceberg S3
 * request does not retain a worker while the AWS future is incomplete.</p>
 */
public final class ParquetDataReader {
  private ParquetDataReader() {
  }

  /**
   * Prepare, asynchronously fetch, and finalize one filtered Parquet file.
   *
   * <p>Preparation and finalization run on {@code executor}. Only the middle S3 stage runs on the
   * AWS async client. The future is deliberately not cancelled on reader close: the destination
   * buffer remains alive until every response writer is terminal, after which finalization notices
   * the closed flag and releases it safely.</p>
   */
  public static CompletableFuture<FileFragment> readAsync(
      FooterResult footer,
      RapidsInputFile input,
      AtomicBoolean closed,
      long requestSizeBytes,
      Executor executor,
      Runnable readsTerminal) {
    if (requestSizeBytes <= 0L) {
      throw new IllegalArgumentException("requestSizeBytes must be positive");
    }
    if (executor == null) {
      throw new NullPointerException("executor");
    }
    if (readsTerminal == null) {
      throw new NullPointerException("readsTerminal");
    }
    long startNanos = System.nanoTime();
    return CompletableFuture.supplyAsync(() -> {
      try {
        return PendingFileRead.prepare(
            footer, input, closed, requestSizeBytes, startNanos);
      } catch (Throwable error) {
        throw asCompletionException(error);
      }
    }, executor).thenCompose(pending -> {
      CompletableFuture<Void> reads;
      try {
        reads = pending.startReads(executor);
      } catch (Throwable error) {
        reads = failedFuture(error);
      }
      // Admission protects live destination writers, not short finalization or asynchronous cache
      // writes. Notify the planner as soon as both the remote and cache-hit writers are terminal.
      return reads.whenComplete((ignored, error) -> readsTerminal.run()).handleAsync(
          (ignored, error) -> {
            if (error != null) {
              throw pending.cleanupAndWrap(unwrap(error));
            }
            try {
              return pending.finish();
            } catch (Throwable finishError) {
              throw pending.cleanupAndWrap(finishError);
            }
          }, executor);
    });
  }

  /** Owns every resource between preparation and terminal finalization of one file read. */
  private static final class PendingFileRead {
    private final FooterResult footer;
    private final RapidsInputFile input;
    private final AtomicBoolean closed;
    private final long startNanos;
    private final long[] blockOffsets;
    private final List<CachedRange> hits;
    private final List<SourceRange> misses;
    private final List<RapidsInputFile.CopyRange> reads;
    private final RangeCompletionTracker rangeCompletions;
    private final long allocNanos;
    private final long requestedBytes;
    private final long cacheHitCount;
    private final long cacheHitBytes;
    private final long cacheMissCount;
    private final long cacheMissBytes;

    private ParquetOutput output;
    private volatile long readWaitNanos;
    private volatile long cacheReadNanos;

    private PendingFileRead(
        FooterResult footer,
        RapidsInputFile input,
        AtomicBoolean closed,
        long startNanos,
        long[] blockOffsets,
        ParquetOutput output,
        List<CachedRange> hits,
        List<SourceRange> misses,
        List<RapidsInputFile.CopyRange> reads,
        long allocNanos,
        long requestedBytes,
        long cacheHitCount,
        long cacheHitBytes,
        long cacheMissCount,
        long cacheMissBytes) {
      this.footer = footer;
      this.input = input;
      this.closed = closed;
      this.startNanos = startNanos;
      this.blockOffsets = blockOffsets;
      this.output = output;
      this.hits = hits;
      this.misses = misses;
      this.reads = reads;
      this.rangeCompletions = new RangeCompletionTracker(
          misses, reads, this::offerCompletedChunkToCache);
      this.allocNanos = allocNanos;
      this.requestedBytes = requestedBytes;
      this.cacheHitCount = cacheHitCount;
      this.cacheHitBytes = cacheHitBytes;
      this.cacheMissCount = cacheMissCount;
      this.cacheMissBytes = cacheMissBytes;
    }

    static PendingFileRead prepare(
        FooterResult footer,
        RapidsInputFile input,
        AtomicBoolean closed,
        long requestSizeBytes,
        long startNanos) throws Exception {
      checkOpen(closed);
      List<BlockMetaData> blocks = footer.getBlocks();
      long[] blockOffsets = FileFragment.computeBlockOffsets(blocks);
      long totalBytes = blockOffsets[blocks.size()];
      if (totalBytes == 0L) {
        return new PendingFileRead(footer, input, closed, startNanos, blockOffsets, null,
            new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), 0L, 0L,
            0L, 0L, 0L, 0L);
      }

      List<CachedRange> hits = new ArrayList<>();
      List<SourceRange> misses = new ArrayList<>();
      ParquetOutput output = null;
      long cacheHitCount = 0L;
      long cacheHitBytes = 0L;
      long cacheMissCount = 0L;
      long cacheMissBytes = 0L;
      long allocStart = System.nanoTime();
      try {
        output = ParquetOutput.create(totalBytes);
        long allocNanos = System.nanoTime() - allocStart;
        FileCache fileCache = FileCache.get();
        long fragmentOffset = 0L;
        for (BlockMetaData block : blocks) {
          for (ColumnChunkMetaData column : block.getColumns()) {
            checkOpen(closed);
            long length = column.getTotalSize();
            long sourceOffset = column.getStartingPos();
            Option<SeekableByteChannel> cached = fileCache.getDataRangeChannel(
                input, sourceOffset, length);
            if (cached.isDefined()) {
              hits.add(new CachedRange(cached.get(), length, fragmentOffset));
              cacheHitCount++;
              cacheHitBytes = Math.addExact(cacheHitBytes, length);
            } else {
              SourceRange chunk = new SourceRange(sourceOffset, length, fragmentOffset);
              Option<FileCacheStartedToken> token = fileCache.startDataRangeCache(
                  input, sourceOffset, length);
              chunk.token = token.isDefined() ? token.get() : null;
              misses.add(chunk);
              cacheMissCount++;
              cacheMissBytes = Math.addExact(cacheMissBytes, length);
            }
            fragmentOffset = Math.addExact(fragmentOffset, length);
          }
        }

        List<RapidsInputFile.CopyRange> reads = planRanges(misses, requestSizeBytes);
        long requestedBytes = 0L;
        for (RapidsInputFile.CopyRange read : reads) {
          requestedBytes = Math.addExact(requestedBytes, read.getLength());
        }
        return new PendingFileRead(footer, input, closed, startNanos, blockOffsets, output,
            hits, misses, reads, allocNanos, requestedBytes,
            cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes);
      } catch (Throwable error) {
        Throwable failure = cleanup(error, output, hits, misses);
        if (failure instanceof Exception) {
          throw (Exception) failure;
        }
        if (failure instanceof Error) {
          throw (Error) failure;
        }
        throw new RuntimeException(failure);
      }
    }

    /** Start local-cache and remote reads together and wait until both writers are terminal. */
    CompletableFuture<Void> startReads(Executor fallbackExecutor) {
      CompletableFuture<Long> remote;
      try {
        remote = startRemoteRead(fallbackExecutor);
      } catch (Throwable error) {
        remote = failedFuture(error);
      }
      CompletableFuture<Void> cached;
      try {
        cached = startCacheRead();
      } catch (Throwable error) {
        cached = failedFuture(error);
      }
      return CompletableFuture.allOf(remote, cached);
    }

    private CompletableFuture<Long> startRemoteRead(
        Executor fallbackExecutor) throws IOException {
      if (output == null || reads.isEmpty()) {
        return CompletableFuture.completedFuture(0L);
      }
      checkOpen(closed);
      long readStart = System.nanoTime();
      return output.copyRangesAsync(
              input, reads, fallbackExecutor, rangeCompletions::requestCompleted)
          .whenComplete((ignored, error) -> readWaitNanos = System.nanoTime() - readStart);
    }

    private CompletableFuture<Void> startCacheRead() {
      if (output == null || hits.isEmpty()) {
        return CompletableFuture.completedFuture(null);
      }
      checkOpen(closed);
      CompletableFuture<Void> read = FileCache.get().submitDataRead(() -> {
        checkOpen(closed);
        long cacheStart = System.nanoTime();
        try {
          output.copyCachedRanges(hits);
        } catch (IOException error) {
          throw new CompletionException(error);
        } finally {
          cacheReadNanos = System.nanoTime() - cacheStart;
        }
      });
      return read.whenComplete((ignored, error) -> {
        Throwable failure = closeCachedRanges(error, hits);
        if (error == null && failure != null) {
          throw asCompletionException(failure);
        }
      });
    }

    FileFragment finish() throws Exception {
      checkOpen(closed);
      if (output == null) {
        return new FileFragment(footer, blockOffsets, null,
            new FileFragment.DownloadStats(System.nanoTime() - startNanos,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
      }
      long finalizeStart = System.nanoTime();
      if (!rangeCompletions.isComplete()) {
        throw new IllegalStateException(
            "remote reads completed without completing every source range");
      }
      output.seal();
      ParquetOutput completedOutput = output;
      FileFragment result = new FileFragment(footer, blockOffsets, completedOutput,
          new FileFragment.DownloadStats(System.nanoTime() - startNanos,
              allocNanos, readWaitNanos, 0L, System.nanoTime() - finalizeStart,
              reads.size(), requestedBytes,
              cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos));
      output = null;
      return result;
    }

    /**
     * Hand an original column chunk to the cache as soon as every S3 request piece covering it
     * has completed. File-cache completion only enqueues the physical disk write, so the packed
     * output and file permit do not wait for that background write.
     */
    private void offerCompletedChunkToCache(SourceRange chunk) {
      FileCacheStartedToken token = chunk.token;
      if (token == null) {
        return;
      }
      try {
        HostMemoryBuffer data = output.sliceForCache(chunk.fragmentOffset, chunk.length);
        // complete() takes ownership of the slice, including when the cache elects not to write it.
        chunk.token = null;
        token.complete(data);
      } catch (IOException error) {
        throw new CompletionException(error);
      }
    }

    CompletionException cleanupAndWrap(Throwable error) {
      return asCompletionException(cleanup(error, output, hits, misses));
    }
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> failed = new CompletableFuture<>();
    failed.completeExceptionally(error);
    return failed;
  }

  private static Throwable cleanup(
      Throwable error,
      ParquetOutput output,
      List<CachedRange> hits,
      List<SourceRange> misses) {
    Throwable failure = closeCachedRanges(error, hits);
    for (SourceRange chunk : misses) {
      if (chunk.token != null) {
        try {
          chunk.token.cancel();
          chunk.token = null;
        } catch (Throwable cancelError) {
          if (failure != cancelError) {
            failure.addSuppressed(cancelError);
          }
        }
      }
    }
    if (output != null) {
      try {
        output.close();
      } catch (Throwable closeError) {
        if (failure != closeError) {
          failure.addSuppressed(closeError);
        }
      }
    }
    return failure;
  }

  private static Throwable closeCachedRanges(
      Throwable error,
      List<CachedRange> hits) {
    Throwable failure = error;
    for (CachedRange hit : hits) {
      try {
        hit.channel.close();
      } catch (Throwable closeError) {
        if (failure == null) {
          failure = closeError;
        } else if (failure != closeError) {
          failure.addSuppressed(closeError);
        }
      }
    }
    return failure;
  }

  private static CompletionException asCompletionException(Throwable error) {
    return error instanceof CompletionException
        ? (CompletionException) error
        : new CompletionException(error);
  }

  private static Throwable unwrap(Throwable error) {
    Throwable current = error;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static void checkOpen(AtomicBoolean closed) {
    if (closed.get()) {
      throw new CancellationException("Parquet reader is closed");
    }
  }

  /** One open file-cache channel owned until the per-file cache-copy future is terminal. */
  static final class CachedRange {
    final SeekableByteChannel channel;
    final long length;
    final long fragmentOffset;

    CachedRange(
        SeekableByteChannel channel,
        long length,
        long fragmentOffset) {
      this.channel = channel;
      this.length = length;
      this.fragmentOffset = fragmentOffset;
    }
  }

  /** One filtered column chunk that must appear in the packed output. */
  static final class SourceRange {
    final long sourceOffset;
    final long length;
    final long fragmentOffset;
    FileCacheStartedToken token;

    SourceRange(long sourceOffset, long length, long fragmentOffset) {
      this.sourceOffset = sourceOffset;
      this.length = length;
      this.fragmentOffset = fragmentOffset;
    }
  }

  /**
   * Maps split/coalesced S3 requests back to the original filtered column chunks.
   *
   * <p>A coalesced request can finish several chunks at once, while a large chunk can require
   * several requests. This task-local tracker invokes {@code chunkCompleted} exactly once per
   * chunk, only after all request pieces covering it have succeeded. Request callbacks may arrive
   * concurrently on AWS completion threads, so the small bookkeeping operation is synchronized;
   * no network or disk I/O is performed while waiting to enter it.</p>
   */
  static final class RangeCompletionTracker {
    private final IdentityHashMap<RapidsInputFile.CopyRange, List<SourceRange>> chunksByRequest =
        new IdentityHashMap<>();
    private final IdentityHashMap<SourceRange, Integer> requestsRemaining =
        new IdentityHashMap<>();
    private final Consumer<SourceRange> chunkCompleted;

    RangeCompletionTracker(
        List<SourceRange> chunks,
        List<RapidsInputFile.CopyRange> requests,
        Consumer<SourceRange> chunkCompleted) {
      this.chunkCompleted = java.util.Objects.requireNonNull(chunkCompleted, "chunkCompleted");
      for (RapidsInputFile.CopyRange request : requests) {
        chunksByRequest.put(request, new ArrayList<>());
      }
      for (SourceRange chunk : chunks) {
        long chunkOutputEnd = Math.addExact(chunk.fragmentOffset, chunk.length);
        long coveredBytes = 0L;
        int coveringRequests = 0;
        for (Map.Entry<RapidsInputFile.CopyRange, List<SourceRange>> entry :
            chunksByRequest.entrySet()) {
          RapidsInputFile.CopyRange request = entry.getKey();
          long requestOutputEnd = Math.addExact(
              request.getOutputOffset(), request.getLength());
          long overlapStart = Math.max(chunk.fragmentOffset, request.getOutputOffset());
          long overlapEnd = Math.min(chunkOutputEnd, requestOutputEnd);
          if (overlapStart < overlapEnd) {
            long requestSourceAtOverlap = Math.addExact(
                request.getInputOffset(), overlapStart - request.getOutputOffset());
            long chunkSourceAtOverlap = Math.addExact(
                chunk.sourceOffset, overlapStart - chunk.fragmentOffset);
            if (requestSourceAtOverlap != chunkSourceAtOverlap) {
              throw new IllegalArgumentException(
                  "request output range does not map to the source column chunk");
            }
            coveredBytes = Math.addExact(coveredBytes, overlapEnd - overlapStart);
            coveringRequests += 1;
            entry.getValue().add(chunk);
          }
        }
        if (coveredBytes != chunk.length || coveringRequests == 0) {
          throw new IllegalArgumentException(
              "requests do not exactly cover source column chunk at " + chunk.sourceOffset);
        }
        requestsRemaining.put(chunk, coveringRequests);
      }
    }

    synchronized void requestCompleted(RapidsInputFile.CopyRange request) {
      List<SourceRange> chunks = chunksByRequest.remove(request);
      if (chunks == null) {
        throw new IllegalArgumentException("unknown or duplicate completed request");
      }
      for (SourceRange chunk : chunks) {
        Integer remaining = requestsRemaining.get(chunk);
        if (remaining == null || remaining <= 0) {
          throw new IllegalStateException("source range completed more than once");
        }
        if (remaining == 1) {
          requestsRemaining.remove(chunk);
          chunkCompleted.accept(chunk);
        } else {
          requestsRemaining.put(chunk, remaining - 1);
        }
      }
    }

    synchronized boolean isComplete() {
      return chunksByRequest.isEmpty() && requestsRemaining.isEmpty();
    }
  }

  /**
   * Build exact requests from the selected column chunks in Parquet-footer order.
   *
   * <p>Consecutive column chunks are first coalesced into a maximal useful run. Each run is then
   * split into requests no larger than {@code requestSizeBytes}. A gap always ends a run, so no
   * request contains bytes that footer filtering excluded. The footer is expected to retain
   * physical file order; rejecting overlap or backwards movement is safer than silently sorting
   * malformed input.</p>
   */
  static List<RapidsInputFile.CopyRange> planRanges(
      List<SourceRange> misses,
      long requestSizeBytes) {
    if (requestSizeBytes <= 0L) {
      throw new IllegalArgumentException("requestSizeBytes must be positive");
    }
    ArrayList<RapidsInputFile.CopyRange> reads = new ArrayList<>();
    long runSourceOffset = 0L;
    long runOutputOffset = 0L;
    long runLength = 0L;
    long previousSourceEnd = -1L;
    long previousOutputEnd = -1L;
    for (SourceRange range : misses) {
      if (range.sourceOffset < 0L || range.length <= 0L || range.fragmentOffset < 0L) {
        throw new IllegalArgumentException(
            "source and output offsets must be non-negative and ranges must be non-empty");
      }
      long sourceEnd = Math.addExact(range.sourceOffset, range.length);
      long outputEnd = Math.addExact(range.fragmentOffset, range.length);
      if (previousSourceEnd >= 0L && range.sourceOffset < previousSourceEnd) {
        throw new IllegalArgumentException(
            "source ranges must be in footer order and must not overlap");
      }
      if (previousOutputEnd >= 0L && range.fragmentOffset < previousOutputEnd) {
        throw new IllegalArgumentException("output ranges must be in packing order");
      }

      if (runLength > 0L
          && range.sourceOffset == previousSourceEnd
          && range.fragmentOffset == previousOutputEnd) {
        runLength = Math.addExact(runLength, range.length);
      } else {
        appendSplitRequests(
            reads, runSourceOffset, runOutputOffset, runLength, requestSizeBytes);
        runSourceOffset = range.sourceOffset;
        runOutputOffset = range.fragmentOffset;
        runLength = range.length;
      }
      previousSourceEnd = sourceEnd;
      previousOutputEnd = outputEnd;
    }
    appendSplitRequests(reads, runSourceOffset, runOutputOffset, runLength, requestSizeBytes);
    return reads;
  }

  private static void appendSplitRequests(
      List<RapidsInputFile.CopyRange> reads,
      long sourceOffset,
      long outputOffset,
      long runLength,
      long requestSizeBytes) {
    for (long offset = 0L; offset < runLength; ) {
      long length = Math.min(requestSizeBytes, runLength - offset);
      reads.add(new RapidsInputFile.CopyRange(
          Math.addExact(sourceOffset, offset),
          length,
          Math.addExact(outputOffset, offset)));
      offset = Math.addExact(offset, length);
    }
  }
}
