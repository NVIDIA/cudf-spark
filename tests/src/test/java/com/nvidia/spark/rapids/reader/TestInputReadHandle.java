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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestInputReadHandle {
  private static final class Input implements CombineInput {
    private final AtomicInteger closes = new AtomicInteger();
    private RuntimeException failure;

    @Override
    public void close() {
      closes.incrementAndGet();
      if (failure != null) {
        throw failure;
      }
    }
  }

  @Test
  public void transferLeavesInputWithCaller() {
    Input input = new Input();
    InputReadHandle<Input> handle = new InputReadHandle<>(input);
    assertSame(input, handle.takeInput());
    assertThrows(IllegalStateException.class, handle::takeInput);
    handle.close();
    handle.close();
    assertEquals(0, input.closes.get());
    input.close();
    assertEquals(1, input.closes.get());
  }

  @Test
  public void closeBeforeTransferReleasesInputExactlyOnce() {
    Input input = new Input();
    InputReadHandle<Input> handle = new InputReadHandle<>(input);
    handle.close();
    handle.close();
    assertEquals(1, input.closes.get());
    assertThrows(IllegalStateException.class, handle::takeInput);
  }

  @Test
  public void failedCloseStillRelinquishesOwnership() {
    Input input = new Input();
    input.failure = new IllegalStateException("close failed");
    InputReadHandle<Input> handle = new InputReadHandle<>(input);
    assertSame(input.failure, assertThrows(IllegalStateException.class, handle::close));
    handle.close();
    assertThrows(IllegalStateException.class, handle::takeInput);
    assertEquals(1, input.closes.get());
  }

  @Test
  public void nullInputIsRejected() {
    assertThrows(NullPointerException.class, () -> new InputReadHandle<Input>(null));
  }

  @Test
  public void concurrentCloseAndTransferHaveOneOwner() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int attempt = 0; attempt < 100; attempt++) {
        Input input = new Input();
        InputReadHandle<Input> handle = new InputReadHandle<>(input);
        CountDownLatch start = new CountDownLatch(1);
        Future<Boolean> transfer = executor.submit(() -> {
          start.await();
          try {
            assertSame(input, handle.takeInput());
            return true;
          } catch (IllegalStateException closed) {
            return false;
          }
        });
        Future<?> close = executor.submit(() -> {
          start.await();
          handle.close();
          return null;
        });
        start.countDown();
        boolean transferred = transfer.get(5, TimeUnit.SECONDS);
        close.get(5, TimeUnit.SECONDS);
        assertEquals(transferred ? 0 : 1, input.closes.get());
        if (transferred) {
          input.close();
        }
        assertEquals(1, input.closes.get());
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
