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
package com.nvidia.spark.history.local;

import java.util.EnumMap;
import java.util.Objects;
import java.util.logging.Logger;

/** Per-store, per-category limiter for fixed and redacted record diagnostics. */
final class LocalRecordDiagnostics {
  static final long COOLDOWN_NANOS = 60_000_000_000L;

  enum Category {
    INVALID("history metrics record dropped: invalid input"),
    NOT_DECLARED("history metrics record dropped: metric version is not declared"),
    FUTURE_TIMESTAMP("history metrics record dropped: timestamp is too far in the future"),
    CLOCK_FAILURE("history metrics record dropped: driver clock failed"),
    PROVENANCE_FAILURE("history metrics record dropped: provenance source failed"),
    OVERFLOW("history metrics record dropped: local queue is full"),
    POST_STOP("history metrics record dropped: local store is stopping"),
    BACKEND_DENIED("history metrics record dropped: backend denied the write"),
    BACKEND_UNAVAILABLE("history metrics record dropped: backend is unavailable"),
    BACKEND_AMBIGUOUS("history metrics record outcome is ambiguous");

    private final String message;

    Category(String message) {
      this.message = message;
    }

    String message() {
      return message;
    }
  }

  private static final Logger LOGGER =
      Logger.getLogger(LocalRecordDiagnostics.class.getName());
  private static final LocalRecordDiagnosticSink SYSTEM_SINK =
      new LocalRecordDiagnosticSink() {
        @Override
        public void recordFailure(Category category, String message) {
          LOGGER.warning(message);
        }
      };

  private final LocalMetricStorePlanningAdapter.Ticker ticker;
  private final LocalRecordDiagnosticSink sink;
  private final EnumMap<Category, Long> lastEmitted =
      new EnumMap<Category, Long>(Category.class);

  LocalRecordDiagnostics(
      LocalMetricStorePlanningAdapter.Ticker ticker,
      LocalRecordDiagnosticSink sink) {
    this.ticker = Objects.requireNonNull(ticker, "ticker");
    this.sink = Objects.requireNonNull(sink, "sink");
  }

  static LocalRecordDiagnostics system(
      LocalMetricStorePlanningAdapter.Ticker ticker) {
    return new LocalRecordDiagnostics(ticker, SYSTEM_SINK);
  }

  void candidate(Category category) {
    Objects.requireNonNull(category, "category");
    final long now;
    try {
      now = ticker.readNanos();
    } catch (RuntimeException ignored) {
      return;
    } catch (LinkageError ignored) {
      return;
    }

    synchronized (lastEmitted) {
      Long previous = lastEmitted.get(category);
      if (previous != null) {
        long elapsed = now - previous;
        if (elapsed < 0L || elapsed < COOLDOWN_NANOS) {
          return;
        }
      }
      // Reserve before calling an untrusted sink so sink failures cannot cause a log storm.
      lastEmitted.put(category, now);
    }

    try {
      sink.recordFailure(category, category.message());
    } catch (RuntimeException ignored) {
      // Diagnostics must not escape the total record boundary.
    } catch (LinkageError ignored) {
      // A diagnostic linkage failure must not escape the total record boundary.
    }
  }

}
