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
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.spark.sql.vectorized.ColumnarBatch;

/** Plans compatible reads and owns admission, IO, readiness, and continuation. */
public interface Scheduler<S extends ReadSource, H extends ReadHandle<?>> extends AutoCloseable {
  /**
   * Called once with the complete source list and a borrowed executor.
   *
   * <p>The iterator transfers only ready handles. It may wait for work and ends only
   * when all initial and follow-up reads finish. The scheduler owns all unyielded handles.
   */
  Iterator<H> schedule(List<S> sources, ExecutorService executor);

  /**
   * Take ownership of handle on entry; borrow batches only for this callback.
   *
   * <p>The callback may drain intermediate batches (closing each consumed batch) and
   * arrange a continuation, or leave output for the reader. It must not close or retain
   * the iterator: the reader owns it. Release or transfer handle resources on every exit.
   */
  void onDecode(H handle, CloseableIterator<ColumnarBatch> batches);

  /** Stop admission, unblock iterator waiters, and clean up owned work; keep executor alive. */
  @Override
  void close();
}
