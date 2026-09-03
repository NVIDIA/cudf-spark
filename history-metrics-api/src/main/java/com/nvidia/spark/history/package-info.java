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
 * Provider-neutral contracts for recording historical observations and consulting bounded evidence
 * during planning.
 *
 * <p>Metric identities and ordered schemas come from the governed
 * {@link com.nvidia.spark.history.HistoryMetricCatalog}. A caller declares schemas, records
 * raw observations, and asks for summaries through
 * {@link com.nvidia.spark.history.MetricStore}. Declaration and summary responses are
 * positional: consumers branch on the closed status codes at the same list position and never parse
 * diagnostic reason or warning text. Unexpected cardinality requires whole-batch abstention.
 *
 * <p>An {@link com.nvidia.spark.history.Status.Code#OK OK} summary response may contain no
 * summary; this is normal absence of evidence and calls for the consumer's unchanged static
 * behavior. {@link com.nvidia.spark.history.Coverage#WINDOW_CLIPPED WINDOW_CLIPPED} is
 * informational rather than an error. Dimensions omitted from a summary request are equality
 * wildcards aggregated into one result, and a request containing a wildcard must use a zero limit.
 *
 * <p>Declaration and summary timeouts are relative, nonnegative, end-to-end budgets measured with
 * monotonic elapsed time from method entry. Record submission is non-blocking and total: configured
 * stores count and drop invalid, rejected, unavailable, or excess optional evidence rather than
 * failing a query. The planning-facing contract intentionally exposes no flush, close, update, or
 * destructive operation.
 *
 * <p>{@link com.nvidia.spark.history.MetricStores} always supplies a non-null current store,
 * using a no-op store until an owner explicitly installs another one. A registration handle owns
 * only the scoped installation; the provider owner remains responsible for store resources and
 * lifecycle.
 */
package com.nvidia.spark.history;
