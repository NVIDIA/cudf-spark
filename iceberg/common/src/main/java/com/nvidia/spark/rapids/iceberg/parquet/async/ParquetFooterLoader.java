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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.filecache.FileCache;
import com.nvidia.spark.rapids.filecache.FileCache.FileCacheStartedToken;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.aws.s3.IcebergS3InputFile;
import org.apache.parquet.hadoop.ParquetFileWriter;
import scala.Option;

/** Loads one cache-aware Parquet footer without retaining a worker during S3 requests. */
public final class ParquetFooterLoader {
  private static final byte[] MAGIC = ParquetFileWriter.MAGIC;
  private static final byte[] ENCRYPTED_MAGIC =
      "PARE".getBytes(StandardCharsets.US_ASCII);
  private static final int TRAILER_LENGTH = Integer.BYTES + MAGIC.length;
  private static final int MINIMUM_FILE_LENGTH = MAGIC.length + TRAILER_LENGTH;

  private ParquetFooterLoader() {
  }

  public static CompletableFuture<FooterBufferResult> loadAsync(
      IcebergS3InputFile input,
      Path path,
      AtomicBoolean closed,
      long prefetchBytes,
      Executor executor) {
    if (prefetchBytes < TRAILER_LENGTH) {
      throw new IllegalArgumentException(
          "footer prefetch must be at least " + TRAILER_LENGTH + " bytes");
    }
    long startNanos = System.nanoTime();
    FileCache cache = FileCache.get();
    AtomicReference<HostMemoryBuffer> cachedFooter = new AtomicReference<>();

    // A footer cache hit performs allocation and local-file I/O. Keep both off the compute pool
    // and account for them in the same executor-wide cache I/O concurrency domain as data reads
    // and cache writes.
    CompletableFuture<Void> cacheLookup = cache.submitDataRead(() -> {
      checkOpen(closed);
      Option<HostMemoryBuffer> cached = cache.getFooter(input);
      if (cached.isDefined()) {
        cachedFooter.set(cached.get());
      }
    });

    CompletableFuture<FooterLoadState> prepared = cacheLookup.thenApplyAsync(ignored -> {
      HostMemoryBuffer cached = cachedFooter.getAndSet(null);
      try {
        return FooterLoadState.prepare(
            input, path, closed, prefetchBytes, startNanos, cache, cached);
      } catch (Throwable error) {
        throw asCompletionException(error);
      }
    }, executor);

    // If the compute executor rejects the continuation, prepare never takes ownership of the
    // cached HMB. Close that otherwise-orphaned buffer here.
    prepared.whenComplete((state, error) -> {
      if (error != null) {
        HostMemoryBuffer orphan = cachedFooter.getAndSet(null);
        if (orphan != null) {
          try {
            orphan.close();
          } catch (Throwable closeError) {
            unwrap(error).addSuppressed(closeError);
          }
        }
      }
    });
    return prepared.thenCompose(state -> state.loadAsync(executor));
  }

  /** Owns the cache token and every HMB until a result is transferred or loading fails. */
  private static final class FooterLoadState {
    private final IcebergS3InputFile input;
    private final Path path;
    private final AtomicBoolean closed;
    private final long prefetchBytes;
    private final long startNanos;
    private final boolean cacheHit;

    private FileCacheStartedToken cacheToken;
    private HostMemoryBuffer prefetch;
    private HostMemoryBuffer footer;
    private long remoteStartNanos;
    private long remoteReadNanos;
    private long requestCount;
    private long requestedBytes;

    private FooterLoadState(
        IcebergS3InputFile input,
        Path path,
        AtomicBoolean closed,
        long prefetchBytes,
        long startNanos,
        boolean cacheHit) {
      this.input = input;
      this.path = path;
      this.closed = closed;
      this.prefetchBytes = prefetchBytes;
      this.startNanos = startNanos;
      this.cacheHit = cacheHit;
    }

    static FooterLoadState prepare(
        IcebergS3InputFile input,
        Path path,
        AtomicBoolean closed,
        long prefetchBytes,
        long startNanos,
        FileCache cache,
        HostMemoryBuffer cachedFooter) {
      FooterLoadState state = new FooterLoadState(
          input, path, closed, prefetchBytes, startNanos, cachedFooter != null);
      state.footer = cachedFooter;
      try {
        checkOpen(closed);
        if (cachedFooter != null) {
          return state;
        }
        Option<FileCacheStartedToken> token = cache.startFooterCache(input);
        state.cacheToken = token.isDefined() ? token.get() : null;
        state.prefetch = HostMemoryBuffer.allocate(prefetchBytes);
        return state;
      } catch (Throwable error) {
        throw state.cleanupAndWrap(error);
      }
    }

    CompletableFuture<FooterBufferResult> loadAsync(Executor executor) {
      if (cacheHit) {
        try {
          return CompletableFuture.completedFuture(transferResult());
        } catch (Throwable error) {
          return failedFuture(cleanupAndWrap(error));
        }
      }

      remoteStartNanos = System.nanoTime();
      requestCount = 1L;
      requestedBytes = prefetchBytes;
      CompletableFuture<Long> first;
      try {
        first = input.readTailAsync(prefetchBytes, prefetch, 0L);
      } catch (Throwable error) {
        first = failedFuture(error);
      }

      CompletableFuture<Void> loaded = first.thenComposeAsync(
          actualBytes -> processPrefetch(actualBytes, executor), executor);
      return loaded.handleAsync((ignored, error) -> {
        remoteReadNanos = System.nanoTime() - remoteStartNanos;
        if (error != null) {
          throw cleanupAndWrap(unwrap(error));
        }
        try {
          checkOpen(closed);
          if (cacheToken != null) {
            HostMemoryBuffer cachedCopy = footer.slice(0L, footer.getLength());
            FileCacheStartedToken token = cacheToken;
            cacheToken = null;
            token.complete(cachedCopy);
          }
          return transferResult();
        } catch (Throwable finishError) {
          throw cleanupAndWrap(finishError);
        }
      }, executor);
    }

    private CompletableFuture<Void> processPrefetch(long actualBytes, Executor executor) {
      checkOpen(closed);
      if (actualBytes < MINIMUM_FILE_LENGTH) {
        return failedFuture(new IllegalStateException(
            path + " is not a Parquet file: only " + actualBytes + " suffix bytes returned"));
      }
      long footerLengthPosition = actualBytes - TRAILER_LENGTH;
      byte[] trailingMagic = new byte[MAGIC.length];
      prefetch.getBytes(trailingMagic, 0, actualBytes - MAGIC.length, MAGIC.length);
      verifyMagic(trailingMagic);
      ByteBuffer lengthView = prefetch.asByteBuffer(footerLengthPosition, Integer.BYTES)
          .order(ByteOrder.LITTLE_ENDIAN);
      int footerLength = lengthView.getInt();
      if (footerLength < 0) {
        return failedFuture(new IllegalStateException(
            "negative Parquet footer length " + footerLength + " for " + path));
      }
      long footerStart = footerLengthPosition - footerLength;
      long framedLength = Math.addExact(
          MAGIC.length, Math.addExact((long) footerLength, (long) TRAILER_LENGTH));
      footer = HostMemoryBuffer.allocate(framedLength);
      footer.setBytes(0L, MAGIC, 0, MAGIC.length);

      if (footerStart >= MAGIC.length) {
        footer.copyFromHostBuffer(
            MAGIC.length, prefetch, footerStart, (long) footerLength + TRAILER_LENGTH);
        closePrefetch();
        return CompletableFuture.completedFuture(null);
      }

      closePrefetch();
      long tailLength = (long) footerLength + TRAILER_LENGTH;
      requestCount = Math.addExact(requestCount, 1L);
      requestedBytes = Math.addExact(requestedBytes, tailLength);
      CompletableFuture<Long> second;
      try {
        second = input.readTailAsync(tailLength, footer, MAGIC.length);
      } catch (Throwable error) {
        second = failedFuture(error);
      }
      return second.thenApplyAsync(copied -> {
        if (copied != tailLength) {
          throw new CompletionException(new IllegalStateException(
              "Parquet footer suffix for " + path + " returned " + copied +
                  " bytes, expected " + tailLength));
        }
        return null;
      }, executor);
    }

    private FooterBufferResult transferResult() {
      HostMemoryBuffer resultBuffer = footer;
      if (resultBuffer == null) {
        throw new IllegalStateException("footer loading completed without a buffer for " + path);
      }
      footer = null;
      closePrefetch();
      return new FooterBufferResult(
          resultBuffer,
          cacheHit,
          System.nanoTime() - startNanos,
          remoteReadNanos,
          requestCount,
          requestedBytes);
    }

    private CompletionException cleanupAndWrap(Throwable error) {
      Throwable failure = error;
      if (cacheToken != null) {
        try {
          cacheToken.cancel();
          cacheToken = null;
        } catch (Throwable cancelError) {
          if (failure != cancelError) {
            failure.addSuppressed(cancelError);
          }
        }
      }
      failure = closeBuffer(prefetch, failure);
      prefetch = null;
      failure = closeBuffer(footer, failure);
      footer = null;
      return asCompletionException(failure);
    }

    private void closePrefetch() {
      if (prefetch != null) {
        prefetch.close();
        prefetch = null;
      }
    }

    private void verifyMagic(byte[] magic) {
      if (Arrays.equals(MAGIC, magic)) {
        return;
      }
      if (Arrays.equals(ENCRYPTED_MAGIC, magic)) {
        throw new UnsupportedOperationException(
            "GPU Parquet reader does not support encrypted file " + path);
      }
      throw new IllegalStateException(
          path + " is not a Parquet file: invalid trailing magic " + Arrays.toString(magic));
    }
  }

  private static Throwable closeBuffer(HostMemoryBuffer buffer, Throwable failure) {
    if (buffer != null) {
      try {
        buffer.close();
      } catch (Throwable closeError) {
        if (failure != closeError) {
          failure.addSuppressed(closeError);
        }
      }
    }
    return failure;
  }

  private static void checkOpen(AtomicBoolean closed) {
    if (closed.get()) {
      throw new CancellationException("Parquet reader is closed");
    }
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> failed = new CompletableFuture<>();
    failed.completeExceptionally(error);
    return failed;
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
}
