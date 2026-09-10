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

package org.apache.iceberg.io;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

/** Root-layout bridge to Iceberg's package-private {@link FanoutWriter}. */
public final class GpuFanoutWriterBridge<T, R> extends FanoutWriter<T, R> {
  private final BiFunction<PartitionSpec, StructLike, FileWriter<T, R>> newWriter;
  private final Consumer<R> addResult;
  private final Supplier<R> aggregatedResult;

  public GpuFanoutWriterBridge(
      BiFunction<PartitionSpec, StructLike, FileWriter<T, R>> newWriter,
      Consumer<R> addResult,
      Supplier<R> aggregatedResult) {
    this.newWriter = newWriter;
    this.addResult = addResult;
    this.aggregatedResult = aggregatedResult;
  }

  @Override
  protected FileWriter<T, R> newWriter(PartitionSpec partitionSpec, StructLike partition) {
    return newWriter.apply(partitionSpec, partition);
  }

  @Override
  protected void addResult(R result) {
    addResult.accept(result);
  }

  @Override
  protected R aggregatedResult() {
    return aggregatedResult.get();
  }
}
