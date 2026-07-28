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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Event-driven planning and combination coordinator.
 *
 * <p>The reader registers every source together with its footer and data futures. Implementations
 * observe both futures, form format-specific plans, and submit combination work. Calls to
 * {@link #nextReady()} are fulfilled in planner-defined output order. Ownership of a successful
 * data future transfers to the planner when {@link #addFile} returns normally.</p>
 */
public interface ReadPlanner<
    S extends ReadSource,
    F extends ReadFooter,
    D extends ReadData,
    C extends DecodeInput> extends AutoCloseable {

  void addFile(
      int fileId,
      S source,
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
