---
layout: page
title: History Metrics Integration
nav_order: 16
parent: Developer Overview
---
# History Metrics Integration

History metrics let a driver-side planning consumer learn from earlier observations without making
query success depend on history. The central integration rule is **abstain safely**: use history only
after the entire response needed for one decision is structurally usable and the metric owner's
reviewed evidence policy accepts it. Otherwise preserve the existing static decision.

This is an MVP developer guide. It does not define a metric-specific estimator, evidence threshold,
or optimizer policy. The
[compiled integration example](../../history-metrics-local/src/test/java/com/nvidia/spark/history/local/HistoryMetricsIntegrationExampleTest.java)
exercises the lifecycle described here.

## Artifact roles

The source tree separates three Java 8 artifacts:

| Artifact | Role | Dependency direction |
| --- | --- | --- |
| `history-metrics-api` | Dependency-free planning contract, governed production catalog, no-op store, and process locator | Consumer-facing base |
| `history-metrics-local` | Explicitly owned, in-memory driver companion for tests and early prototypes, including snapshots and local observability | Depends on the API; its runtime remains API/JDK-only |
| `history-metrics-tck` | Reusable provider-conformance fixtures and suites | Test dependency for provider implementations |

Use artifacts built from a compatible project revision. These names describe repository roles; this
guide does not claim released Maven coordinates or make the local companion a production service.
Depending on the API leaves `MetricStores.current()` on its non-null no-op implementation.
Depending on the local companion does not construct or install a provider, enable persistence, read
configuration, or access a network.

## Govern the metric before integrating it

Production metric families come from the source-controlled `HistoryMetricCatalog`, not from
runtime registration. The catalog contains one permanent ID/name/tombstone entry per family, never
one entry per version. Every family-scoped contract version reuses that ID/name; the same name under
a different ID is invalid. Exact versioned requests use `MetricVersionId`. The metric owner should
review, together:

- the stable governed metric-family/catalog ID and name;
- the observed quantity, unit, occurrence, and producer-side reduction;
- the ordered dimension names and kinds;
- the observation timestamp meaning and recommended retention;
- the planning request shapes and their cost;
- the consumer's evidence, staleness, and static-fallback policy;
- the selected Spark owner and production metric-emission and request-building planning hooks that
  will satisfy the NFR-16B first-heuristic failure-boundary gate.

A family ID/name association is permanent. Retire it in source rather than reusing it. Increment the
positive family-scoped contract version when the contract changes in an incompatible way, including
quantity semantics or dimension identity, kind, or order. Versions retain the family ID/name but
remain isolated. Providers never translate or combine them. A consumer may compare separate
exact-version responses only under an explicit metric-owner-reviewed mapping and within the same
single-call deadline and 128-request cap; otherwise it abstains.

`LocalTestCatalog.builder()` accepts source-declared live and retired entries for an isolated local
test. It neither allocates nor reserves a production ID. The numeric ID `61001` and version `1` in
the compiled example are test inputs only.

## Construct an explicit local owner

The local factory requires every policy and identity input explicitly. For example:

```java
HistoryMetricCatalog catalog = LocalTestCatalog.builder()
    .addLive(61001, "example.scan.expansion")
    .build();

LocalHistoryMetrics owner = LocalHistoryMetricsFactory.open(
    catalog,
    Clock.fixed(Instant.ofEpochMilli(10_000L), ZoneOffset.UTC),
    () -> LocalProvenanceIdentity.of(
        "redacted-example-app", "attempt-1", "example-build"),
    Duration.ofHours(2),
    LocalQueuePolicy.of(19, 7),
    LocalExecutionPolicy.of(2, 11),
    LocalCircuitBreakerPolicy.of(
        9, 5, 0.75, Duration.ofMillis(850), 0.60, Duration.ofSeconds(3)));
```

Every number and duration above is an intentionally distinctive example value, not a default or
recommendation. Choose and validate bounds for the actual driver workload. Provenance text is
diagnostic input supplied by the caller: redact it before construction. The local provider validates
encoding and bounds but does not discover secrets or authenticate that identity.

`LocalHistoryMetrics` owns its executors and backend. It deliberately is not `AutoCloseable`
because shutdown requires an explicit time budget. Always invoke `shutdown(Duration)` in a
`finally` block and handle a `false` result according to the caller's bounded cleanup policy.

## Declare, record, drain, then summarize

A producer first declares the complete schema for its exact governed `MetricVersionId`. An
identical declaration is safe; an incompatible declaration is not repaired by overwriting stored meaning.

```java
MetricVersionId metric = new MetricVersionId(61001, 1);
MetricSchema schema = new MetricSchema(
    metric,
    Arrays.asList(
        new DimensionSpec("relation", DimValue.Kind.STRING),
        new DimensionSpec("format", DimValue.Kind.STRING)),
    new Retention(Duration.ofMinutes(37), Duration.ofHours(13)));

SchemaStatus declared =
    owner.store().declare(Collections.singletonList(schema), operationBudget).get(0);
if (declared.code() != SchemaStatus.Code.ACCEPTED) {
  // Disable this history-backed decision and retain the static behavior.
}
```

Those retention values are also example inputs, not policy guidance. `declare` and `summarize`
are synchronous but bounded by their relative operation budgets. Do not record under a version whose
declaration was not accepted. Each observation supplies every declared dimension, a finite
value, and an observation-time timestamp. `record` is non-blocking and may drop input; `drain`
waits, within its explicit budget, for observations admitted before its watermark to become
terminal.

```java
owner.store().record(Collections.singletonList(
    new Observation(metric, dimensions, 2.0, 9_000L)));
if (!owner.drain(operationBudget)) {
  // Do not assume the queued observation is available to the following read.
}
```

A summary request always has an explicit window. Binding every declared dimension asks for an exact
context:

```java
SummaryRequest exact = SummaryRequest.builder(metric)
    .bind("relation", DimValue.of("orders"))
    .bind("format", DimValue.of("parquet"))
    .window(8_000L, 11_000L)
    .build();
```

Omitting a dimension makes it a deliberate equality wildcard. It is not a fuzzy or pattern match.
With the schema above, binding only the leading `relation` dimension aggregates all `format`
identities in the window:

```java
SummaryRequest wildcard = SummaryRequest.builder(metric)
    .bind("relation", DimValue.of("orders"))
    .window(8_000L, 11_000L)
    .build();
```

Dimension order is a one-time contract and an access-order choice. Benchmark every declared request
shape before putting it on a planning path.

## Treat responses as permission to consider evidence

A valid batch returns one positional response per request. Apply these rules before metric-specific
decision logic:

| Result | Integration action |
| --- | --- |
| `OK` with a non-null summary | Structurally eligible; apply the separately reviewed evidence policy |
| `OK` with no summary | Successful absence of evidence; abstain |
| `NOT_DECLARED` | Abstain; the provider has no authoritative declaration |
| `INVALID_REQUEST` | Abstain and fix the integration |
| `DEADLINE_EXCEEDED` | Abstain; do not extend the planning budget by retrying inline |
| `UNAVAILABLE` | Abstain while preserving query behavior |
| `DENIED` | Abstain; do not bypass the provider's decision |
| Null, malformed, or wrong-cardinality batch | Reject the whole batch and abstain |

`WINDOW_CLIPPED` is informational coverage on an `OK` response. It says the provider clipped the
requested start to the effective planning window. It is not, by itself, an error or an automatic
abstention. The metric owner must define whether the returned summary is adequate; the generic
integration must not invent a count, age, or formula.

Make the fallback atomic at the natural optimizer-decision scope. If one required response is absent,
an error, malformed, or missing because cardinality is wrong, do not combine partial history with
static inputs. Use the entire pre-existing static decision. Also retain static behavior when strict
request construction fails before the store can be called. Record the realized decision source in
the consumer's existing bounded driver diagnostics, without putting raw dimensions, provenance, or
provider text into it.

This query-safety boundary contains ordinary `RuntimeException` and compatibility `LinkageError`
failures, including failures in strict construction before the store and provider/store calls inside
the boundary. It intentionally does not convert `VirtualMachineError`, `ThreadDeath`, or any
non-`LinkageError` `Error`, including `AssertionError`, into `UNAVAILABLE`, a dropped
observation, or static fallback; those errors escape under NFR-06. The selected Spark consumer owns
the production emission and request-building adapters at its co-developed hooks, and its first
heuristic must pass NFR-16B failure-injection tests for both boundaries.

The compiled example contains a small whole-decision structural gate. It intentionally returns only
“history eligible” or “static fallback”; it does not pretend that a structurally valid summary is
sufficient evidence for a real heuristic.

## Scope the locator; own the provider separately

The local factory never installs its store. Install only when code that reads
`MetricStores.current()` must share the explicitly constructed owner:

```java
LocalHistoryMetrics owner = LocalHistoryMetricsFactory.open(/* explicit inputs */);
AutoCloseable registration = null;
try {
  registration = MetricStores.install(owner.store());
  runDriverIntegration();
} finally {
  try {
    if (registration != null) {
      registration.close();
    }
  } finally {
    owner.shutdown(shutdownBudget);
  }
}
```

Only one registration may be active. Closing the registration restores the no-op locator; it does not
drain, shut down, or close the provider. Conversely, shutting down the owner does not express locator
ownership. Keep both scopes explicit and close the non-owning registration before owner shutdown.

## Save and restore local snapshots explicitly

Local persistence occurs only when the owner calls `save(path, timeout)`. Restore requires an
explicit `LocalHistoryMetricsFactory.openSnapshot` call with the same governed catalog and fresh
runtime inputs:

```java
try {
  owner.save(snapshotPath, operationBudget);
} finally {
  owner.shutdown(shutdownBudget);
}

LocalHistoryMetrics restored = LocalHistoryMetricsFactory.openSnapshot(
    snapshotPath,
    catalog,
    driverClock,
    provenanceSource,
    maximumPlanningAge,
    queuePolicy,
    executionPolicy,
    breakerPolicy,
    operationBudget);
try {
  // Re-query restored declarations and observations.
} finally {
  restored.shutdown(shutdownBudget);
}
```

A snapshot is same-version local test/prototype support, not a portable or production database.
Source, target, and sibling temporary files are unencrypted local-sensitive data. They can contain
dimensions and caller-supplied provenance. Use a protected directory, restrict access, pass only
permitted/redacted values, and clean residual temporary files after failures. CRC checking detects
accidental corruption; it does not provide confidentiality or authenticity. Snapshot restore starts
new queues, counters, breaker state, executors, and lifecycle state.

## Observe without leaking data

The local test handle exposes immutable point-in-time counter snapshots over the closed
`LocalMetricCounter` vocabulary. Inspect outcome and lifecycle categories such as declaration,
summary, record admission/drop, backend completion, queue, drain, breaker, snapshot cleanup, and
shutdown. Counters are the public diagnostic accounting surface; they are not resettable.

The local implementation also emits bounded, fixed-category, redacted record-failure diagnostics.
These record diagnostics are privately rate-limited. Separately, each successful snapshot save whose
current-operation temporary-file cleanup fails emits exactly one fixed, redacted snapshot-cleanup
diagnostic. Other snapshot failures do not imply that diagnostic, and the snapshot-cleanup diagnostic
is not rate-limited. Diagnostic text does not include dimensions, observations, provenance, paths,
provider text, or exception messages. Do not build an integration around the private record limiter's
timing, add a public limiter knob, or infer its emitted/suppressed counts: those are intentionally not
contracts. Metric-specific decision source and timing belong in the consumer's existing approved
driver telemetry when that heuristic is implemented.
