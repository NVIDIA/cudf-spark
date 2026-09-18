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
  storage or lifecycle machinery.
- An operator enables the feature and supplies only operational settings that the embedding plugin
  deliberately exposes.

The RAPIDS driver plugin selects one provider by its stable name. Provider jars advertise a
`HistoryMetricsProvider` through Java service metadata, but discovery does not enable them. The
Java API is kept at the distribution root so application-classpath providers and parallel-world
implementations share one API class identity. RAPIDS constructs only the explicitly configured
provider, installs its store, owns its deadline-bounded shutdown, and keeps the built-in no-op store
when selection or initialization fails.

## Artifact roles

The private repository contains a standalone Maven subproject with three Java 8 artifacts. They do
not inherit the private repository's Spark shim, Scala, or classified-output build settings:

| Artifact | Role | Dependency direction |
| --- | --- | --- |
| `cudf-spark-history-metrics-api` | Planning contract, governed production catalog, no-op store, installation/registration holder, and provider SPI | Consumer-facing base with no Spark or Scala dependency |
| `cudf-spark-history-metrics-local` | Optional SQLite-backed provider with isolated memory and durable local-file modes | Depends on the metrics API and exposes SQLite JDBC as a runtime dependency; has no Spark or Scala dependency |
| `cudf-spark-history-metrics-tck` | Reusable provider-conformance fixtures and suites | Test dependency for provider implementations |

Use artifacts built from a compatible private-project revision. The three unsuffixed, unclassified
artifacts are built once from their standalone subproject and shared by both Scala builds. Depending
on the API leaves `MetricStores.current()` on its non-null no-op implementation. The local provider
is not a normal SQL plugin dependency and is not included merely because the RAPIDS plugin is
present.

The history project belongs to neither private Scala reactor. Private developers verify or install it
directly with `mvn -f history-metrics/pom.xml clean verify` or `clean install`.
`spark-rapids-private/build/buildall` performs the install once as a workspace convenience; it does
not define publication ordering. CI/CD must deploy the standalone reactor exactly once, outside the
Spark shim and Scala matrices. Both public Scala builds then resolve the same exact
`history-metrics.version`. Release coordinates are immutable, and stacked development builds need a
unique snapshot coordinate so an older local or remote snapshot cannot satisfy the dependency
silently.

The local artifact is thin and does not shade SQLite native libraries. Its qualified MVP runtime
dependency is `org.xerial:sqlite-jdbc:3.53.4.0`. Normal dependency resolution, including
`--packages`, supplies it transitively. A deployment using only `--jars` must put both the local
provider jar and that SQLite JDBC artifact on the driver classpath.

## Select and configure the local provider

`spark.rapids.sql.history.metrics.provider` selects a provider by its case-insensitive service name.
The default value `none` keeps the built-in no-op store. To select the local provider, add its
separate jar and runtime dependency to the driver classpath and set:

```
spark.rapids.sql.history.metrics.provider=local
```

The Java SPI receives a defensive, unmodifiable copy of the Spark configuration plus the application
ID, optional application-attempt ID, and RAPIDS producer version. The map may contain sensitive
configuration, so a provider is trusted in-process code and must not log or persist the map wholesale.
The API does not filter it or define keys for other providers. It has no direct Spark or Scala
dependency, so the same artifact is binary-compatible with the Scala 2.12 and 2.13 distributions.
Provider jars must not bundle Spark or the history metrics API.

Providers may be placed in the application jar or supplied before driver startup through `--jars`,
`spark.jars`, `--packages`, `--driver-class-path`, `spark.driver.extraClassPath`, or an
equivalent cluster library mechanism. The provider and RAPIDS distribution must be visible through
compatible driver class loaders. In particular, do not put the provider only on the parent driver
class path while supplying the RAPIDS distribution only through `--jars`: the parent cannot resolve
the history metrics API from its child loader. Supplying both jars through the same mechanism avoids
that asymmetry. Adding a provider later with `SparkContext.addJar()` cannot enable it, because
provider selection occurs during driver startup in `DriverPlugin.registerMetrics`. A jar on the
classpath is only discoverable; it is never selected implicitly. Missing, duplicate, incompatible,
or failing providers leave the no-op store installed.

The local provider has one provider-specific key. It is intentionally owned and parsed by the
provider rather than registered in `RapidsConf`:

```
spark.rapids.sql.history.metrics.local.path=/protected/local/history.db
```

When the key is absent, each provider instance uses an isolated SQLite in-memory database. When the
key is present, it must name a file whose parent directory already exists; committed transactions
survive driver restarts through that file. The local mode supports local or block storage owned by one
provider process. URI-like values such as `file:` or `hdfs:`, and syntactic UNC paths, are rejected.
A path on an arbitrary network-mounted filesystem can still look like an ordinary local path to Java
and cannot be detected reliably. Such mounts are unsupported; the deployer is responsible for
selecting local or block storage. Fresh databases deliberately retain SQLite's default rollback
journal for this single-owner MVP; behavioral abrupt-process and external-lock tests validate crash
recovery and locking. The provider does not override an existing database's journal mode. New
database files use owner-only permissions where the filesystem supports them, but the database is
not encrypted.

The current production catalog is empty until the first governed metric family is added, so the local
provider does not yet accept application declarations. This is intentional: tests use test-only
catalog fixtures, and the storage MVP does not invent a production metric merely to demonstrate
persistence.

The local backend's initial unreleased on-disk schema is version 1. It stores the ordered
declaration structure in a canonical versioned blob, and each observation stores zero through eight
typed dimensions inline by declaration ordinal. This is an internal file-format choice, not a new
heuristic contract: integrations continue to bind reviewed dimension names and values through the
API. There is no earlier durable SQLite format to migrate. Normal open validates structural schema
and declaration blobs and enables foreign-key enforcement for future writes. It does not decode
every observation payload or run a full foreign-key scan; that scan belongs to an explicit integrity
check. Corrupt observation payloads fail an affected read or that check. Future format changes
require explicit versioned migrations, but none is implemented for the initial MVP. The inline layout
was selected from measured local and PostgreSQL workloads spanning recurrent and high-cardinality
dimensions. Integrations must still benchmark their own request shapes against their planning budget.

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

Those retention values are example inputs, not policy guidance. `declare` and `summarize` are
synchronous but bounded by their relative operation budgets. Do not record under a version whose
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
```

Omitting a dimension makes it a deliberate equality wildcard. It is not a fuzzy or pattern match.
Independently, `limit(0)` means all eligible rows subject to the request deadline, not an invented
cap. Dimension order is a one-time contract and an access-order choice. Benchmark every declared
request shape before putting it on a planning path.

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

This query-safety boundary contains ordinary `RuntimeException` and compatibility `LinkageError`
failures, including failures in strict construction before the store and provider/store calls inside
the boundary. It intentionally does not convert `VirtualMachineError`, `ThreadDeath`, or any
non-`LinkageError` `Error`, including `AssertionError`, into `UNAVAILABLE`, a dropped
observation, or static fallback; those errors escape. The selected Spark consumer owns the production
emission and request-building adapters at its co-developed hooks. Its first heuristic must pass
failure-injection tests for both boundaries.

## Persistence and shutdown

SQLite commits durable-file writes continuously at transaction boundaries; persistence does not
depend on a shutdown export. A later driver using the same file can read committed declarations and
observations. Observation provenance retains the application and attempt identity supplied when the
provider stamped that write.

The embedding plugin removes the installed store and calls the provider's
`shutdown(Duration)`. Repeated shutdown calls are harmless. Successful shutdown means admission is
closed and every previously admitted observation reached an attempted terminal backend outcome:
confirmed acceptance, validation rejection, or backend failure. It does not promise that every
admitted observation persisted. A `false` result means shutdown did not complete within the budget;
the caller reports that outcome but must not wait indefinitely. Heuristic code does not own or shut
down the provider.

The local SQLite database may contain dimensions and application provenance. Protect its directory,
supply only permitted and redacted values, and manage the file according to local-data policy.
Copying a live database file is unsupported; any future backup/export feature must use an
SQLite-supported mechanism.
