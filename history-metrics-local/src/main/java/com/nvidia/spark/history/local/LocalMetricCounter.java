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

/** Closed MVP counter vocabulary for immutable local diagnostic snapshots. */
public enum LocalMetricCounter {
  DECLARATION_BATCH,
  DECLARATION_STATUS_ACCEPTED,
  DECLARATION_STATUS_INCOMPATIBLE,
  DECLARATION_STATUS_INVALID_REQUEST,
  DECLARATION_STATUS_UNAVAILABLE,
  DECLARATION_STATUS_DENIED,
  SUMMARY_BATCH,
  SUMMARY_STATUS_OK,
  SUMMARY_STATUS_NOT_DECLARED,
  SUMMARY_STATUS_INVALID_REQUEST,
  SUMMARY_STATUS_DEADLINE_EXCEEDED,
  SUMMARY_STATUS_UNAVAILABLE,
  SUMMARY_STATUS_DENIED,
  SUMMARY_WINDOW_CLIPPED,
  SUMMARY_ROWS,
  RECORD_INVALID,
  RECORD_NOT_DECLARED,
  RECORD_FUTURE_TIMESTAMP,
  RECORD_CLOCK_FAILURE,
  RECORD_PROVENANCE_FAILURE,
  RECORD_OVERFLOW,
  RECORD_POST_STOP,
  RECORD_ENQUEUED,
  BACKEND_ACCEPTED,
  BACKEND_REJECTED,
  BACKEND_AMBIGUOUS,
  SNAPSHOT_CLEANUP_FAILURE,
  QUEUE_CURRENT,
  QUEUE_HIGH_WATER,
  DRAIN_SUCCESS,
  DRAIN_TIMEOUT,
  BREAKER_SAMPLE,
  BREAKER_FAILURE,
  BREAKER_SLOW,
  BREAKER_OPEN,
  BREAKER_SUPPRESSED,
  BREAKER_HALF_OPEN,
  BREAKER_CLOSE,
  SHUTDOWN_DROPPED,
  SHUTDOWN_TIMEOUT,
  SHUTDOWN_COMPLETE
}
