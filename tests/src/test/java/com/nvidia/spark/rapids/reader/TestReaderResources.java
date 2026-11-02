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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestReaderResources {
  @Test
  public void withResourceClosesOnSuccess() {
    AtomicInteger closes = new AtomicInteger();
    String result = ReaderResources.withResource(
        (AutoCloseable) closes::incrementAndGet, ignored -> "result");
    assertEquals("result", result);
    assertEquals(1, closes.get());
  }

  @Test
  public void closeOnExceptTransfersOnSuccess() {
    AtomicInteger closes = new AtomicInteger();
    AutoCloseable value = closes::incrementAndGet;
    assertSame(value, ReaderResources.closeOnExcept(value, resource -> resource));
    assertEquals(0, closes.get());
    ReaderResources.closeAll(null, value);
    assertEquals(1, closes.get());
  }

  @Test
  public void operationFailureSurvivesCleanupFailure() {
    RuntimeException operationError = new IllegalStateException("operation");
    RuntimeException cleanupError = new IllegalStateException("cleanup");
    AtomicInteger closes = new AtomicInteger();
    AutoCloseable value = () -> {
      closes.incrementAndGet();
      throw cleanupError;
    };
    assertSame(operationError, assertThrows(IllegalStateException.class,
        () -> ReaderResources.withResource(value, ignored -> {
          throw operationError;
        })));
    assertArrayEquals(new Throwable[]{cleanupError}, operationError.getSuppressed());
    assertEquals(1, closes.get());
  }

  @Test
  public void allOwnersAreClosedDespiteFailures() {
    RuntimeException first = new IllegalStateException("first");
    RuntimeException second = new IllegalStateException("second");
    AtomicInteger closedLast = new AtomicInteger();
    assertSame(first, assertThrows(IllegalStateException.class, () ->
        ReaderResources.closeAll(null, () -> { throw first; },
            () -> { throw second; }, null, closedLast::incrementAndGet)));
    assertArrayEquals(new Throwable[]{second}, first.getSuppressed());
    assertEquals(1, closedLast.get());
  }

  @Test
  public void primaryAndErrorIdentityArePreserved() {
    AssertionError primary = new AssertionError("primary");
    ReaderResources.closeAll(primary, () -> { throw primary; });
    assertEquals(0, primary.getSuppressed().length);
    assertSame(primary, assertThrows(AssertionError.class,
        () -> ReaderResources.propagate(primary)));
    Exception checked = new Exception("checked");
    assertSame(checked, ReaderResources.propagate(checked).getCause());
  }

  @Test
  public void retryAdapterReturnsAResult() {
    assertEquals("result", ReaderResources.withRetryNoSplit(() -> "result"));
  }
}
