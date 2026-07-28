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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.nvidia.spark.rapids.reader.Combiner;

/**
 * Builds the logical synthetic Parquet input selected by one Iceberg read plan.
 *
 * <p>Combination is deliberately a separate asynchronous operation even though the current
 * zero-copy implementation only creates a header, footer, and fragment-slice description. A
 * future Parquet implementation can copy or reconstruct data here without changing the reader
 * or planner contracts.</p>
 */
public final class ParquetCombiner
    implements Combiner<ReadSubtask, FileFragment, ParquetDecodeInput> {
  private final Set<FileFragment> attributedFragments =
      Collections.newSetFromMap(new IdentityHashMap<>());

  @Override
  public synchronized CompletableFuture<ParquetDecodeInput> combine(
      ReadSubtask plan,
      List<FileFragment> data,
      ExecutorService executor) {
    Objects.requireNonNull(plan, "plan");
    Objects.requireNonNull(data, "data");
    Objects.requireNonNull(executor, "executor");
    ArrayList<FileFragment> fragments = new ArrayList<>(data);
    ArrayList<FileFragment> metricFragments = new ArrayList<>();
    for (FileFragment fragment : fragments) {
      if (attributedFragments.add(fragment)) {
        metricFragments.add(fragment);
      }
    }
    return CompletableFuture.supplyAsync(() -> {
      long start = System.nanoTime();
      SubtaskStats stats = aggregate(metricFragments, System.nanoTime() - start);
      return new ParquetDecodeInput(plan, fragments, stats);
    }, executor);
  }

  private static SubtaskStats aggregate(
      List<FileFragment> fragments,
      long combineNanos) {
    long ioNanos = 0L;
    long allocNanos = 0L;
    long readWaitNanos = 0L;
    long routeNanos = 0L;
    long finalizeNanos = 0L;
    long requestCount = 0L;
    long requestedBytes = 0L;
    long hitCount = 0L;
    long hitBytes = 0L;
    long missCount = 0L;
    long missBytes = 0L;
    long cacheReadNanos = 0L;
    for (FileFragment fragment : fragments) {
      FileFragment.DownloadStats stats = fragment.getStats();
      ioNanos = Math.addExact(ioNanos, stats.ioNanos);
      allocNanos = Math.addExact(allocNanos, stats.allocNanos);
      readWaitNanos = Math.addExact(readWaitNanos, stats.readWaitNanos);
      routeNanos = Math.addExact(routeNanos, stats.routeNanos);
      finalizeNanos = Math.addExact(finalizeNanos, stats.finalizeNanos);
      requestCount = Math.addExact(requestCount, stats.requestCount);
      requestedBytes = Math.addExact(requestedBytes, stats.requestedBytes);
      hitCount = Math.addExact(hitCount, stats.cacheHitCount);
      hitBytes = Math.addExact(hitBytes, stats.cacheHitBytes);
      missCount = Math.addExact(missCount, stats.cacheMissCount);
      missBytes = Math.addExact(missBytes, stats.cacheMissBytes);
      cacheReadNanos = Math.addExact(cacheReadNanos, stats.cacheReadNanos);
    }
    return new SubtaskStats(
        ioNanos, allocNanos, readWaitNanos, routeNanos, finalizeNanos,
        requestCount, requestedBytes, combineNanos, false,
        hitCount, hitBytes, missCount, missBytes, cacheReadNanos);
  }
}
