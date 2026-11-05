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

import java.util.Objects;

/**
 * A handoff owning only its input. Schedulers may keep logical-read context separately.
 * Transfer and close are synchronized so cancellation cannot release a transferred input.
 */
public final class InputReadHandle<I extends CombineInput> implements ReadHandle<I> {
  private I input;
  private boolean closed;

  public InputReadHandle(I input) {
    this.input = Objects.requireNonNull(input, "input");
  }

  @Override
  public synchronized I takeInput() {
    if (closed || input == null) {
      throw new IllegalStateException("Read input is no longer owned by this handle");
    }
    I result = input;
    input = null;
    return result;
  }

  @Override
  public void close() {
    I remaining;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      remaining = input;
      input = null;
    }
    if (remaining != null) {
      remaining.close();
    }
  }
}
