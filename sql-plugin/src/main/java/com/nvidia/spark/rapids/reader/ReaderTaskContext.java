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

import java.util.function.Supplier;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.rapids.execution.TrampolineUtil$;

/** Confines Spark's thread-local context interoperability to the reader boundary. */
public final class ReaderTaskContext {
  private ReaderTaskContext() {
  }

  /** Restore a pooled worker's previous context on both normal and exceptional exit. */
  public static <T> T withContext(TaskContext context, Supplier<T> operation) {
    TaskContext previous = TaskContext.get();
    set(context);
    try {
      return operation.get();
    } finally {
      set(previous);
    }
  }

  private static void set(TaskContext context) {
    if (context == null) {
      TrampolineUtil$.MODULE$.unsetTaskContext();
    } else {
      TrampolineUtil$.MODULE$.setTaskContext(context);
    }
  }
}
