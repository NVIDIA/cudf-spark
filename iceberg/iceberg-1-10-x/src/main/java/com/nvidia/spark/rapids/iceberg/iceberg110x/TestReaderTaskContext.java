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

import org.apache.spark.TaskContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class TestReaderTaskContext {
  @Test
  public void nestedContextsAreRestoredEvenOnFailure() {
    TaskContext original = TaskContext.get();
    TaskContext outer = mock(TaskContext.class);
    TaskContext inner = mock(TaskContext.class);
    ReaderTaskContext.withContext(outer, () -> {
      assertSame(outer, TaskContext.get());
      ReaderTaskContext.withContext(inner, () -> {
        assertSame(inner, TaskContext.get());
        return null;
      });
      assertSame(outer, TaskContext.get());
      assertThrows(IllegalStateException.class, () ->
          ReaderTaskContext.withContext(null, () -> {
            assertNull(TaskContext.get());
            throw new IllegalStateException("worker failure");
          }));
      assertSame(outer, TaskContext.get());
      return null;
    });
    assertSame(original, TaskContext.get());
  }
}
