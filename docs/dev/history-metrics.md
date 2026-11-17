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

Each `MetricVersionId` identifies the complete meaning of a metric: its quantity, unit, occurrence,
producer-side reduction, timestamp meaning, and dimensions. Define and review these properties before
recording observations. The API validates structure, so the metric owner must ensure that all
observations under one version have compatible meanings. Any semantic change requires a new metric
version.

The provider validates the structural representation, isolates exact versions, stamps provenance,
stores accepted observations, enforces its effective retention, applies request windows, dimensions,
and limits, computes the API-defined summaries, and reports deadline or availability failures safely.
It does not reduce task metrics, reinterpret scalar values, validate domain-specific units, combine
metric versions, or decide whether the evidence is sufficient for a heuristic.

There are three distinct roles:

- The embedding plugin owns provider selection, construction, installation, persistence, and
  shutdown. The Spark RAPIDS driver plugin is the intended owner for this MVP.
- A heuristic developer defines observations and summary requests. They do not configure provider
  storage or lifecycle machinery.
- An operator enables the feature and supplies only operational settings that the embedding plugin
  deliberately exposes.

The RAPIDS driver plugin selects one provider by its stable name and manages its lifecycle.
Heuristics use `MetricStores.current()` to access the selected store. When history is disabled or
provider initialization fails, this returns a no-op store.

The RAPIDS distribution includes the history metrics API and local provider. No additional history
jars are required to use the bundled provider. The Java API has no Spark or Scala dependency and can
be used with either the Scala 2.12 or 2.13 distribution.

## Select and configure the local provider

`spark.rapids.sql.history.metrics.provider` selects a provider by its case-insensitive service name.
The default value `none` keeps the built-in no-op store without loading a provider. To select the
bundled local provider, set:

```
spark.rapids.sql.history.metrics.provider=local
```

Custom providers may be placed in the application jar or supplied before driver startup through `--jars`,
`spark.jars`, `--packages`, `--driver-class-path`, `spark.driver.extraClassPath`, or an
equivalent cluster library mechanism. The provider and RAPIDS distribution must be visible through
compatible driver class loaders. In particular, do not put the provider only on the parent driver
class path while supplying the RAPIDS distribution only through `--jars`: the parent cannot resolve
the history metrics API from its child loader. Supplying both jars through the same mechanism avoids
that asymmetry. Supply providers before driver startup; adding one later with
`SparkContext.addJar()` cannot enable it. Provider jars must advertise a `HistoryMetricsProvider`
through Java service metadata and must not bundle Spark or the history metrics API. Explicitly
select the provider by name. Missing, duplicate, incompatible, or failing providers leave the no-op
store installed.

To retain history across driver restarts, set the local provider's database path:

```
spark.rapids.sql.history.metrics.local.path=/protected/local/history.db
```

When the key is absent, each provider instance uses an isolated SQLite in-memory database. When the
key is present, it must name a file whose parent directory already exists; committed transactions
survive driver restarts through that file. The local mode supports local or block storage owned by one
provider process. URI-like values such as `file:` or `hdfs:`, and syntactic UNC paths, are rejected.
A path on an arbitrary network-mounted filesystem can still look like an ordinary local path to Java
and cannot be detected reliably. Such mounts are unsupported; the deployer is responsible for
selecting local or block storage. New database files use owner-only permissions where the filesystem
supports them, but the database is not encrypted.

The current production catalog is empty. Before using the local provider to record observations,
add a governed metric family as described below. Enabling the provider alone does not collect metrics
or change planning decisions.

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
- where the heuristic records observations and consults history, including its behavior when either
  operation fails.

A family ID/name association is permanent. Retire it in source rather than reusing it. Increment the
positive family-scoped contract version when the contract changes in an incompatible way, including
quantity semantics or dimension identity, kind, or order. Versions retain the family ID/name but
remain isolated. Providers never translate or combine them. A consumer may compare separate
exact-version responses only under an explicit metric-owner-reviewed mapping and within the same
single-call deadline and 128-request cap; otherwise it abstains.

## Declare, record, and summarize

Heuristic code accesses the store installed by the embedding plugin:

```java
MetricStore store = MetricStores.current();
```

A producer first declares the complete schema for its exact governed `MetricVersionId`. An
identical declaration is safe; an incompatible declaration is not repaired by overwriting stored
meaning.

```java
MetricVersionId metric = new MetricVersionId(GOVERNED_FAMILY_ID, 1);
MetricSchema schema = new MetricSchema(
    metric,
    Arrays.asList(
        new DimensionSpec("relation", DimValue.Kind.STRING),
        new DimensionSpec("format", DimValue.Kind.STRING)),
    new Retention(Duration.ofMinutes(37), Duration.ofHours(13)));

SchemaStatus declared =
    store.declare(Collections.singletonList(schema), operationBudget).get(0);
if (declared.code() != SchemaStatus.Code.ACCEPTED) {
  // Disable this history-backed decision and retain the static behavior.
}
```

Those retention values are example inputs, not policy guidance. `operationBudget` is a `Duration`
chosen by the heuristic to bound each call. `declare` and `summarize` are synchronous but bounded by
their relative operation budgets. Do not record under a version whose
declaration was not accepted. Each observation is one scalar occurrence defined by the heuristic. It
supplies every declared dimension, a finite value, and an observation-time timestamp. If task-level
inputs need to be combined, the heuristic does so before this call.

`record` is total, non-blocking, and fire-and-forget. It may drop malformed observations, evidence
offered after shutdown starts, or evidence that cannot enter the bounded queue. Returning does not
mean the observation was persisted, and there is no planning-facing flush or drain operation.

```java
store.record(new Observation(metric, dimensions, 2.0, observationTimeMs));
```

A summary request always has an explicit `[from, to)` window. Binding every declared dimension asks
for an exact context:

```java
SummaryRequest exact = SummaryRequest.builder(metric)
    .bind("relation", DimValue.of("orders"))
    .bind("format", DimValue.of("parquet"))
    .window(windowStartMs, windowEndMs)
    .limit(5)
    .build();

List<SummaryResponse> responses =
    store.summarize(Collections.singletonList(exact), operationBudget);
```

Omitting a dimension makes it a deliberate equality wildcard. It is not a fuzzy or pattern match.
Independently, `limit(0)` means all eligible rows subject to the request deadline, not an invented
cap. Keep dimension order consistent with the declared schema. Benchmark every declared request
shape before putting it on a planning path.

The request belongs to the heuristic. One heuristic may use the latest matching observation while
another uses several observations or the entire bounded window. The store provides selection and
summary operations; it does not impose one evidence policy on every heuristic.

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
the consumer's existing bounded telemetry without putting raw dimensions, provenance, provider text,
or paths into it.

Handle `RuntimeException` and compatibility `LinkageError` failures around request construction and
store calls by retaining the static decision. Do not catch other `Error` types, including
`VirtualMachineError`, `ThreadDeath`, and `AssertionError`, to turn them into static fallback or
dropped observations. Test the heuristic's fallback behavior for failures when constructing requests,
recording observations, and consulting history.

## Persistence and shutdown

With a configured local database path, a later driver using the same file can read committed
declarations and observations. In-memory history is lost when the provider shuts down. Observations
retain the application and attempt identity from when they were recorded.

The RAPIDS driver plugin manages provider shutdown. Heuristic code must not shut down the provider.
Record observations while the application is active: recording is best-effort, and shutdown does not
guarantee that every offered observation was persisted.

The local SQLite database may contain dimensions and application provenance. Protect its directory,
supply only permitted and redacted values, and manage the file according to local-data policy.
Copying a live database file is unsupported; any future backup/export feature must use an
SQLite-supported mechanism.
