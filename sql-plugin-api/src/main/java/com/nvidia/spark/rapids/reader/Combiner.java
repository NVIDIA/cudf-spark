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

import java.util.concurrent.ExecutorService;

/** Stateless, format-specific assembly of ready inputs. */
@FunctionalInterface
public interface Combiner<I extends CombineInput, C extends CombinedResult> {
  /**
   * Consume input on entry and return an owned, complete result.
   *
   * <p>On failure, release input and all allocations. Independent copies may use the
   * borrowed executor, but no worker may access input/output after this call finishes,
   * even on rejection or cancellation. Never shut down the shared executor.
   */
  C combine(I input, ExecutorService executor);
}
