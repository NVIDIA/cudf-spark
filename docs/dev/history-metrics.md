---
layout: page
title: History Metrics Integration
nav_order: 16
parent: Developer Overview
---
# History Metrics Integration

History metrics let a driver-side planning heuristic learn from earlier applications without making
query success depend on history. The central integration rule is **abstain safely**: use history only
after the response needed for one decision is structurally usable and the heuristic's evidence policy
accepts it. Otherwise preserve the existing static decision.

The store is the final stage of a metric-specific pipeline:

1. Run the application.
2. Collect the task-, stage-, operator-, query-, or application-level inputs needed by the heuristic.
3. Convert those inputs into one or more scalar observations defined by the heuristic.
4. Record each observation in the history store.

Steps 2 and 3 belong to the heuristic developer. This API starts at step 4. An observation can
represent a scan, join, query, application, or another heuristic-defined occurrence in the current
application. The provider stamps application provenance, but does not decide the observation's
granularity or reduce task metrics. Summary requests combine matching stored observations; the
heuristic chooses the dimensions, time window, limit, and whether the result is useful.

The exact `MetricVersionId` is the opaque stand-in for the metric's complete semantic contract.
Observed quantity, unit, occurrence, producer-side reduction, timestamp meaning, and every other
meaning-bearing property belong to that exact version. These properties are intentionally not modeled
exhaustively in `MetricSchema`: the framework cannot enumerate or infer every way a metric's meaning
can change. The metric owner defines, reviews, and enforces this association. A provider validates the
structural declaration but cannot detect a developer assigning incompatible semantics to the same
metric version. Any semantic change requires a new metric version.

The provider validates the structural representation, isolates exact versions, stamps provenance,
stores accepted observations, enforces its effective retention, applies request windows, dimensions,
and limits, computes the API-defined summaries, and reports deadline or availability failures safely.
It does not reduce task metrics, reinterpret scalar values, validate domain-specific units, combine
metric versions, or decide whether the evidence is sufficient for a heuristic.

There are three distinct roles:

- The embedding plugin owns provider selection, construction, installation, persistence, and
  shutdown. The Spark RAPIDS driver plugin is the intended owner for this MVP.
- A heuristic developer defines observations and summary requests. They do not configure provider
  queues, executors, circuit breakers, or lifecycle machinery.
- An operator enables the feature and supplies only operational settings that the embedding plugin
  deliberately exposes.

The RAPIDS driver plugin selects one provider by its stable name. Provider jars advertise a
`HistoryMetricsProvider` through Java service metadata, but discovery does not enable them. The
Java API is kept at the distribution root so application-classpath providers and parallel-world
implementations share one API class identity. RAPIDS constructs only the explicitly configured
provider, installs its store, owns its deadline-bounded shutdown, and keeps the built-in no-op store
when selection or initialization fails.

## Artifact roles

The source tree separates three Java 8 artifacts:

| Artifact | Role | Dependency direction |
| --- | --- | --- |
| `cudf-spark-history-metrics-api` | Planning contract, governed production catalog, no-op store, process locator, and Spark-facing provider SPI | Consumer-facing base; Spark Core is provided at runtime |
| `cudf-spark-history-metrics-local` | Optional in-memory provider for testing and validation, including snapshots and test diagnostics | Depends on the metrics API; Spark Core is provided at runtime |
| `cudf-spark-history-metrics-tck` | Reusable provider-conformance fixtures and suites | Test dependency for provider implementations |

Use artifacts built from a compatible project revision. These names describe the current source-tree
roles and are not a commitment to their long-term repository placement. The API is expected to move
with the planning integration that owns logical-plan changes. Depending on the API leaves
`MetricStores.current()` on its non-null no-op implementation. The local provider is not a normal
SQL plugin dependency and is not included merely because the RAPIDS plugin is present.

## Select a provider

`spark.rapids.sql.history.metrics.provider` selects a provider by its case-insensitive service name.
The default value `none` keeps the built-in no-op store. For example, a validation run can add the
separate local-provider jar to the driver classpath and set:

```
spark.rapids.sql.history.metrics.provider=local
```

A provider reads its own settings from the driver `SparkContext`, including Spark and Hadoop
configuration. The SPI is Java so its `open(SparkContext)` descriptor is independent of the Scala
binary version; a provider must still use only Spark methods supported by the Spark versions it
claims to support. Provider jars must not bundle Spark or the history metrics API.

Providers may be placed in the application jar or supplied before driver startup through `--jars`,
`spark.jars`, `--packages`, `--driver-class-path`, `spark.driver.extraClassPath`, or an equivalent
cluster library mechanism. The provider and RAPIDS distribution must be visible through compatible
driver class loaders. In particular, do not put the provider only on the parent driver class path
while supplying the RAPIDS distribution only through `--jars`: the parent cannot resolve the history
metrics API from its child loader. Supplying both jars through the same mechanism avoids that
asymmetry. Adding a provider later with `SparkContext.addJar()` cannot enable it, because provider
selection occurs while the `SparkContext` is being initialized. A jar on the classpath is only
discoverable; it is never selected implicitly. Discovery logs the configured name and every valid
provider name and implementation it finds. Missing, duplicate, incompatible, or failing providers are
logged with the no-op fallback reason and leave history-backed heuristics disabled.

The current `local` service validates discovery and lifecycle integration. Its plugin-owned catalog
is empty until the first governed production metric family is added, so it does not yet accept
application declarations. The local provider uses the Spark application ID as non-secret provenance,
sampling it when an observation is stamped because Spark assigns the ID after driver plugins are
initialized. It leaves the optional application-attempt ID absent rather than depending on a
scheduler-specific source. It does not expose snapshot save or restore through the plugin integration.

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
- the selected Spark owner and production metric-emission and request-building planning hooks,
  including failure-injection tests for both boundaries.

A family ID/name association is permanent. Retire it in source rather than reusing it. Increment the
positive family-scoped contract version when the contract changes in an incompatible way, including
quantity semantics or dimension identity, kind, or order. Versions retain the family ID/name but
remain isolated. Providers never translate or combine them. A consumer may compare separate
exact-version responses only under an explicit metric-owner-reviewed mapping and within the same
single-call deadline and 128-request cap; otherwise it abstains.

`LocalTestCatalog.builder()` accepts source-declared live and retired entries for an isolated local
test. It neither allocates nor reserves a production ID. The numeric ID `61001` and version `1` in
the example below are test inputs only.

## Construct the local provider from the embedding plugin

The embedding plugin supplies semantic and application identity inputs. Queue sizing, backend
batching, planning executors, and circuit-breaker thresholds are bounded implementation details with
local defaults; heuristic developers and operators do not choose them.

```java
HistoryMetricCatalog catalog = LocalTestCatalog.builder()
    .addLive(61001, "example.scan.expansion")
    .build();

LocalHistoryMetrics owner = LocalHistoryMetricsFactory.open(
    catalog,
    Clock.fixed(Instant.ofEpochMilli(10_000L), ZoneOffset.UTC),
    () -> LocalProvenanceIdentity.of(
        "redacted-example-app", "attempt-1", "example-build"),
    Duration.ofHours(2));
```

The example catalog is test-only. Production integrations use the governed catalog. The maximum
planning age is a semantic envelope chosen by the embedding plugin, and provenance is redacted
application identity supplied by that plugin. The provider validates encoding and bounds but does
not discover secrets or authenticate the identity.

`LocalHistoryMetrics` owns its executors and backend. It deliberately is not `AutoCloseable`
because shutdown is deadline-bounded. Provider ownership belongs in plugin lifecycle code, which
must invoke `shutdown(Duration)` and handle a `false` result. A heuristic should never create or
shut down the provider itself.

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
declaration was not accepted. Each observation is one scalar occurrence defined by the heuristic. It
can describe a scan, join, query, application, or another event within the application. It supplies
every declared dimension, a finite value, and an observation-time timestamp. If task-level inputs
need to be combined, the heuristic does so before this call. `record` accepts one observation, is
non-blocking, and may drop it; the provider may batch queued observations internally. A future
producer batch API may accept unrelated metrics, but it is outside this MVP until ordering, partial
acceptance, and atomicity semantics are defined. `drain` waits, within its explicit budget, for
observations admitted before its watermark to become terminal.

```java
owner.store().record(
    new Observation(metric, dimensions, 2.0, 9_000L));
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

The request belongs to the heuristic. For example, `limit(1)` asks for the latest matching
observation and is appropriate when the most recent comparable application is the best evidence.
Another heuristic may request the latest five observations or the entire bounded window. The store
provides the selection and summary operations; it does not impose one evidence policy on every
heuristic.

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
observation, or static fallback; those errors escape. The selected Spark consumer owns the
production emission and request-building adapters at its co-developed hooks. Its first heuristic
must pass failure-injection tests for both boundaries.

The compiled example contains a small whole-decision structural gate. It intentionally returns only
“history eligible” or “static fallback”; it does not pretend that a structurally valid summary is
sufficient evidence for a real heuristic.

## Scope the locator; own the provider separately

The local factory never installs its store. Install only when code that reads
`MetricStores.current()` must share the explicitly constructed owner:

```java
LocalHistoryMetrics owner = LocalHistoryMetricsFactory.open(
    catalog, driverClock, provenanceSource, maximumPlanningAge);
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
`LocalMetricCounter` vocabulary. These counters are public only so tests and prototypes outside this
package can verify declaration, summary, record, backend, queue, drain, breaker, snapshot, and
shutdown behavior. They are local test support, not part of the provider-neutral API or a supported
production monitoring contract, and they are not resettable.

The local implementation also emits bounded, fixed-category, redacted record-failure diagnostics.
These record diagnostics are privately rate-limited. Separately, each successful snapshot save whose
current-operation temporary-file cleanup fails emits exactly one fixed, redacted snapshot-cleanup
diagnostic. Other snapshot failures do not imply that diagnostic, and the snapshot-cleanup diagnostic
is not rate-limited. Diagnostic text does not include dimensions, observations, provenance, paths,
provider text, or exception messages. Do not build an integration around the private record limiter's
timing, add a public limiter knob, or infer its emitted/suppressed counts: those are intentionally not
contracts. Metric-specific decision source and timing belong in the consumer's existing approved
driver telemetry when that heuristic is implemented.
