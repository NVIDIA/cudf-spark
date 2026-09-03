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
/**
 * Explicitly owned, single-process driver-local history metrics implementation for conformance
 * testing and early prototypes.
 *
 * <p>{@link com.nvidia.spark.history.local.LocalHistoryMetricsFactory} requires the catalog,
 * driver clock, caller-redacted provenance source, maximum planning age, and all queue, execution,
 * and circuit-breaker policies. It supplies no defaults, does not install the returned store in the
 * process locator, and plain open performs no file access. The owner must call
 * {@link com.nvidia.spark.history.local.LocalHistoryMetrics#shutdown(java.time.Duration)}
 * with an explicit budget; it deliberately is not {@link java.lang.AutoCloseable}.
 *
 * <p>Recording uses bounded observation capacity and a single asynchronous FIFO writer. Admission
 * may drop optional evidence rather than block query work. Declaration and summary use bounded
 * planning execution and the provider-neutral relative timeout and positional-status contract.
 * {@link com.nvidia.spark.history.local.LocalHistoryMetricsTestHandle} exposes only immutable
 * point-in-time diagnostic snapshots and closed counter/state vocabularies; it is companion test
 * support, not portable raw read-back.
 *
 * <p>Snapshot save and restore are explicit, same-version local test-support operations using one
 * monotonic budget. Snapshot source, target, and residual temporary files contain unencrypted
 * sensitive data: callers must supply permitted dimensions and redacted provenance, protect the
 * paths, and clean residual files. Callers branch on
 * {@link com.nvidia.spark.history.local.LocalSnapshotException.Reason}, never diagnostic
 * message text. The snapshot facility is not a portable persistence or durability contract.
 */
package com.nvidia.spark.history.local;
