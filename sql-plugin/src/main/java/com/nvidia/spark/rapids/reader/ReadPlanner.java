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

package com.nvidia.spark.rapids.reader;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Format-specific operations and event-driven planning coordinator.
 *
 * <p>The unified reader asks this component to read each footer and its filtered data, then
 * registers both futures back with it. Implementations observe the futures, form format-specific
 * plans, submit combination work, and decode each combined result. Keeping these operations
 * together avoids splitting one format's pipeline across collaborators that share the same
 * planning state and lifecycle.</p>
 *
 * <p>Calls to {@link #nextReady()} are fulfilled in planner-defined output order. Ownership of a
 * successful data future transfers to the planner when {@link #addFile} returns normally.</p>
 */
public interface ReadPlanner<
    S extends ReadSource,
    F extends ReadFooter,
    D extends ReadData,
    C extends CombinedResult> extends AutoCloseable {

  CompletableFuture<F> readFooter(S source, ExecutorService executor);

  CompletableFuture<D> readData(S source, F footer, ExecutorService executor);

  /** Synchronously decode one combined input on the Spark task thread. */
  Iterator<ColumnarBatch> decode(C input) throws Exception;

  void addFile(
      int fileId,
      CompletableFuture<F> footer,
      CompletableFuture<D> data);

  /** Signal that no additional files will be registered. */
  void noMoreFiles();

  /**
   * Return a future for the next combined input. An empty value marks normal end of input.
   */
  CompletableFuture<Optional<C>> nextReady();

  @Override
  void close();
}
