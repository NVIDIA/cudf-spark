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

import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * caller opens the Iceberg input file and runs this blocking operation on the shared reader
 * executor. Cache hits and remote ranges are copied into one spillable packed fragment.</p>
 */
public final class ParquetDataReader {
  private ParquetDataReader() {
  }

  public static FileFragment read(
      FooterResult footer,
      RapidsInputFile input,
      AtomicBoolean closed,
      long requestSizeBytes) throws Exception {
    if (requestSizeBytes <= 0L) {
      throw new IllegalArgumentException("requestSizeBytes must be positive");
    }
    checkOpen(closed);
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
    List<SourceRange> misses = new ArrayList<>();
    long allocStart = System.nanoTime();
    ParquetOutput output = ParquetOutput.create(totalBytes);
    long allocNanos = System.nanoTime() - allocStart;
    try {
      checkOpen(closed);
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
            try (SeekableByteChannel channel = cached.get()) {
              cacheHitCount++;
              cacheHitBytes = Math.addExact(cacheHitBytes, length);
              long cacheStart = System.nanoTime();
              try {
                output.copyCachedRange(channel, fragmentOffset, length);
              } finally {
                cacheReadNanos = Math.addExact(
                    cacheReadNanos, System.nanoTime() - cacheStart);
              }
            }
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

      long readStart = System.nanoTime();
      // readVectored submits every range before it waits, so all exact slices for this file are
      // visible to the executor-wide PerfIO/CRT scheduler in one wave.
      output.copyRanges(input, reads);
      long readWaitNanos = System.nanoTime() - readStart;
      checkOpen(closed);

      long finalizeStart = System.nanoTime();
      for (SourceRange chunk : misses) {
        FileCacheStartedToken token = chunk.token;
        if (token != null) {
          HostMemoryBuffer data = output.sliceForCache(chunk.fragmentOffset, chunk.length);
          chunk.token = null;
          token.complete(data);
        }
      }
      output.seal();
      return new FileFragment(footer, blockOffsets, output,
          new FileFragment.DownloadStats(System.nanoTime() - start,
              allocNanos, readWaitNanos, 0L, System.nanoTime() - finalizeStart,
              reads.size(), requestedBytes,
              cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos));
    } catch (Throwable error) {
      Throwable failure = error;
      for (SourceRange chunk : misses) {
        if (chunk.token != null) {
          try {
            chunk.token.cancel();
          } catch (Throwable cancelError) {
            if (failure != cancelError) {
              failure.addSuppressed(cancelError);
            }
          }
        }
      }
      try {
        output.close();
      } catch (Throwable closeError) {
        if (failure != closeError) {
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
  }

  private static void checkOpen(AtomicBoolean closed) {
    if (closed.get()) {
      throw new CancellationException("Parquet reader is closed");
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
