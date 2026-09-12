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

/**
 * Counter vocabulary exposed only for tests and prototypes of the local implementation.
 *
 * <p>This enum is not part of the provider-neutral API or a supported production monitoring
 * contract. It is public so tests outside this package can inspect local implementation behavior.
 */
public enum LocalMetricCounter {
  /** Calls made to the declaration API. */
  DECLARATION_BATCH,
  /** Declaration positions accepted by the provider. */
  DECLARATION_STATUS_ACCEPTED,
  /** Declaration positions rejected as incompatible with an existing declaration. */
  DECLARATION_STATUS_INCOMPATIBLE,
  /** Declaration positions rejected as invalid. */
  DECLARATION_STATUS_INVALID_REQUEST,
  /** Declaration positions that could not reach an available provider. */
  DECLARATION_STATUS_UNAVAILABLE,
  /** Declaration positions denied by the provider. */
  DECLARATION_STATUS_DENIED,
  /** Calls made to the summary API. */
  SUMMARY_BATCH,
  /** Summary positions completed successfully, including absence of matching evidence. */
  SUMMARY_STATUS_OK,
  /** Summary positions for undeclared metric versions. */
  SUMMARY_STATUS_NOT_DECLARED,
  /** Summary positions rejected as invalid. */
  SUMMARY_STATUS_INVALID_REQUEST,
  /** Summary positions that exhausted their deadline. */
  SUMMARY_STATUS_DEADLINE_EXCEEDED,
  /** Summary positions that could not reach an available provider. */
  SUMMARY_STATUS_UNAVAILABLE,
  /** Summary positions denied by the provider. */
  SUMMARY_STATUS_DENIED,
  /** Application-level observations included in successful summaries. */
  SUMMARY_ROWS,
  /** Invalid record calls or observation elements dropped before enqueue. */
  RECORD_INVALID,
  /** Observations dropped because their metric version was not declared. */
  RECORD_NOT_DECLARED,
  /** Observations dropped because their timestamp was too far in the future. */
  RECORD_FUTURE_TIMESTAMP,
  /** Observations dropped because the driver clock failed. */
  RECORD_CLOCK_FAILURE,
  /** Observations dropped because provenance could not be created. */
  RECORD_PROVENANCE_FAILURE,
  /** Observations dropped because the bounded local queue was full. */
  RECORD_OVERFLOW,
  /** Observations dropped after recording began to stop. */
  RECORD_POST_STOP,
  /** Observations admitted to the asynchronous writer queue. */
  RECORD_ENQUEUED,
  /** Queued observations reported as accepted by the backend. */
  BACKEND_ACCEPTED,
  /** Queued observations reported as rejected by the backend. */
  BACKEND_REJECTED,
  /** Queued observations whose backend outcome could not be determined. */
  BACKEND_AMBIGUOUS,
  /** Successful saves followed by a failure to clean a temporary snapshot file. */
  SNAPSHOT_CLEANUP_FAILURE,
  /** Observations currently waiting in the writer queue. */
  QUEUE_CURRENT,
  /** Greatest number of observations simultaneously waiting in the writer queue. */
  QUEUE_HIGH_WATER,
  /** Drain calls that reached their captured recording watermark. */
  DRAIN_SUCCESS,
  /** Drain calls that did not reach their captured recording watermark before timeout. */
  DRAIN_TIMEOUT,
  /** Planning calls sampled by the local circuit breaker. */
  BREAKER_SAMPLE,
  /** Sampled planning calls classified as failures. */
  BREAKER_FAILURE,
  /** Sampled planning calls classified as slow. */
  BREAKER_SLOW,
  /** Transitions from the closed circuit-breaker state to open. */
  BREAKER_OPEN,
  /** Planning calls suppressed while the circuit breaker was open or probing. */
  BREAKER_SUPPRESSED,
  /** Transitions from the open circuit-breaker state to half-open. */
  BREAKER_HALF_OPEN,
  /** Transitions from the half-open circuit-breaker state to closed. */
  BREAKER_CLOSE,
  /** Observations dropped while shutdown made queued work terminal. */
  SHUTDOWN_DROPPED,
  /** Shutdown calls that did not complete within their supplied budget. */
  SHUTDOWN_TIMEOUT,
  /** Owners that completed shutdown; this increments at most once per owner. */
  SHUTDOWN_COMPLETE
}
