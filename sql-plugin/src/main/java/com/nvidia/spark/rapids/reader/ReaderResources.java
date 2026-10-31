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

import java.util.function.Function;
import java.util.function.Supplier;

import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

import com.nvidia.spark.rapids.Arm$;
import com.nvidia.spark.rapids.RmmRapidsRetryIterator$;

/** Java entry points to the existing resource and retry policies. */
public final class ReaderResources {
  private ReaderResources() {
  }

  /** Consume resource, preserving the operation's failure if cleanup also fails. */
  public static <T extends AutoCloseable, R> R withResource(
      T resource, Function<? super T, ? extends R> operation) {
    R result = closeOnExcept(resource, operation);
    return Arm$.MODULE$.withResource(resource, new AbstractFunction1<T, R>() {
      @Override
      public R apply(T ignored) {
        return result;
      }
    });
  }

  /** Transfer resource on success; close it on failure using the existing ARM implementation. */
  public static <T extends AutoCloseable, R> R closeOnExcept(
      T resource, Function<? super T, ? extends R> operation) {
    return Arm$.MODULE$.closeOnExcept(resource, new AbstractFunction1<T, R>() {
      @Override
      public R apply(T value) {
        return operation.apply(value);
      }
    });
  }

  /** The operation must be idempotent, as required by the underlying retry utility. */
  public static <R> R withRetryNoSplit(Supplier<R> operation) {
    return RmmRapidsRetryIterator$.MODULE$.withRetryNoSplit(new AbstractFunction0<R>() {
      @Override
      public R apply() {
        return operation.get();
      }
    });
  }

  /**
   * Attempt every cleanup. If primary is supplied, attach cleanup failures to it.
   * Otherwise throw the first failure, retaining the others as suppressed exceptions.
   */
  public static void closeAll(Throwable primary, AutoCloseable... resources) {
    Throwable failure = primary;
    for (AutoCloseable resource : resources) {
      if (resource != null) {
        try {
          withResource(resource, ignored -> null);
        } catch (Throwable error) {
          if (failure == null) {
            failure = error;
          } else if (error != failure) {
            failure.addSuppressed(error);
          }
        }
      }
    }
    if (primary == null && failure != null) {
      throw propagate(failure);
    }
  }

  /** Preserve unchecked failures; retain a checked failure as the cause of the reader error. */
  public static RuntimeException propagate(Throwable error) {
    if (error instanceof Error) {
      throw (Error) error;
    }
    if (error instanceof RuntimeException) {
      return (RuntimeException) error;
    }
    return new IllegalStateException("Reader operation failed", error);
  }
}
