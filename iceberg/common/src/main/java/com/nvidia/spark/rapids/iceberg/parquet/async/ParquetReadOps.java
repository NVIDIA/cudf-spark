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

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nvidia.spark.rapids.iceberg.parquet.IcebergPartitionedFile;
import com.nvidia.spark.rapids.jni.RmmSpark;
import com.nvidia.spark.rapids.reader.ReadOps;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.rapids.execution.TrampolineUtil$;

/**
 * Iceberg Parquet footer and encoded-data operations used by {@code UnifiedReader}.
 *
 * <p>Both methods submit promptly to the caller-supplied shared executor. Each operation installs
 * the captured Spark task context and RMM pool-thread registration because these callbacks run
 * away from the Spark task thread and may allocate spillable host memory.</p>
 */
public final class ParquetReadOps
    implements ReadOps<IcebergPartitionedFile, FooterResult, FileFragment> {
  private final ParquetReaderAdapter adapter;
  private final TaskContext taskContext;
  private final long taskAttemptId;
  private final AtomicBoolean closed;

  public ParquetReadOps(
      ParquetReaderAdapter adapter,
      TaskContext taskContext,
      AtomicBoolean closed) {
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    this.taskContext = taskContext;
    this.taskAttemptId = taskContext == null ? -1L : taskContext.taskAttemptId();
    this.closed = Objects.requireNonNull(closed, "closed");
  }

  @Override
  public CompletableFuture<FooterResult> readFooter(
      IcebergPartitionedFile source,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(() -> run(() -> {
      checkOpen();
      long start = System.nanoTime();
      FooterResult footer = Objects.requireNonNull(
          adapter.readAndFilterFooter(source), "footer adapter returned null");
      adapter.onFooterCompleted(System.nanoTime() - start);
      return footer;
    }), executor);
  }

  @Override
  public CompletableFuture<FileFragment> readData(
      IcebergPartitionedFile source,
      FooterResult footer,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(() -> run(() -> {
      checkOpen();
      return AsyncParquetPartitionReader.downloadFragment(footer, adapter, closed);
    }), executor);
  }

  private <T> T run(Callable<T> operation) {
    try {
      if (taskContext == null) {
        return operation.call();
      }
      boolean contextInstalled = false;
      boolean rmmRegistered = false;
      try {
        TrampolineUtil$.MODULE$.setTaskContext(taskContext);
        contextInstalled = true;
        RmmSpark.poolThreadWorkingOnTask(taskAttemptId);
        rmmRegistered = true;
        return operation.call();
      } finally {
        if (rmmRegistered) {
          RmmSpark.poolThreadFinishedForTask(taskAttemptId);
        }
        if (contextInstalled) {
          TrampolineUtil$.MODULE$.unsetTaskContext();
        }
      }
    } catch (Throwable error) {
      if (error instanceof CompletionException) {
        throw (CompletionException) error;
      }
      throw new CompletionException(error);
    }
  }

  private void checkOpen() {
    if (closed.get()) {
      throw new CancellationException("Parquet reader is closed");
    }
  }
}
