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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.HostAlloc$;
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
  private static final long COALESCE_GAP_LIMIT_BYTES = 0L;

  private ParquetDataReader() {
  }

  public static FileFragment read(
      FooterResult footer,
      RapidsInputFile input,
      AtomicBoolean closed) throws Exception {
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
    List<MissChunk> misses = new ArrayList<>();
    long allocStart = System.nanoTime();
    ParquetOutput output = ParquetOutput.create(totalBytes);
    long allocNanos = System.nanoTime() - allocStart;
    HostMemoryBuffer scratch = null;
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
            MissChunk chunk = new MissChunk(sourceOffset, length, fragmentOffset);
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

      List<MergedRead> mergedReads = mergeMissChunks(misses);
      List<RapidsInputFile.CopyRange> directRanges = new ArrayList<>();
      List<RapidsInputFile.CopyRange> scratchRanges = new ArrayList<>();
      long scratchBytes = 0L;
      long requestedBytes = 0L;
      for (MergedRead read : mergedReads) {
        requestedBytes = Math.addExact(requestedBytes, read.spanBytes());
        if (read.isDirect()) {
          directRanges.add(new RapidsInputFile.CopyRange(
              read.sourceStart, read.spanBytes(), read.chunks.get(0).fragmentOffset));
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

      long readStart = System.nanoTime();
      output.copyRanges(input, directRanges);
      if (scratch != null) {
        input.readVectored(scratch, scratchRanges);
      }
      long readWaitNanos = System.nanoTime() - readStart;
      checkOpen(closed);

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

      long finalizeStart = System.nanoTime();
      for (MissChunk chunk : misses) {
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
              allocNanos, readWaitNanos, routeNanos, System.nanoTime() - finalizeStart,
              directRanges.size() + scratchRanges.size(), requestedBytes,
              cacheHitCount, cacheHitBytes, cacheMissCount, cacheMissBytes, cacheReadNanos));
    } catch (Throwable error) {
      Throwable failure = error;
      for (MissChunk chunk : misses) {
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
    } finally {
      if (scratch != null) {
        scratch.close();
      }
    }
  }

  private static void checkOpen(AtomicBoolean closed) {
    if (closed.get()) {
      throw new CancellationException("Parquet reader is closed");
    }
  }

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

  private static final class MergedRead {
    final List<MissChunk> chunks = new ArrayList<>();
    long sourceStart;
    long sourceEnd;
    long scratchStart = -1L;

    long spanBytes() {
      return sourceEnd - sourceStart;
    }

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
}
