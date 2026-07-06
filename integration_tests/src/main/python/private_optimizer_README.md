# Private optimizer integration tests

These tests exercise the RAPIDS *private* optimizer rules (the ones that ship in
the `spark-rapids-private` plugin) end-to-end from public integration tests, by
enabling the relevant private conf and building a query shape that makes the
rule reachable.

Each rule is semantics-preserving but default-off or otherwise dormant in public
IT. A same-conf
CPU-vs-GPU comparison cannot tell whether a rule actually fired, so these tests
do **two** things:

1. Compare an **OFF-rule CPU baseline** against an **ON-rule GPU run**. Because
   the rules preserve semantics, `OFF-CPU == ON-GPU` proves both data
   correctness and GPU parity while keeping the CPU reference uncontaminated by
   the rule.
2. Assert a **per-rule plan marker** that appears only when the rule fires, so
   the test FAILS if the conf flip is a no-op (wrong conf, wrong query shape, or
   the private jar is not loaded).

## Layout

Flat modules, matching the rest of `integration_tests/src/main/python` (this
repo does not use nested pytest test directories):

```text
private_optimizer_common.py                       # shared helpers, NOT collected (no _test suffix)
private_optimizer_agg_pushdown_test.py            # AggPushdownRule
private_optimizer_decompose_stddev_test.py        # DecomposeStddevPop
private_optimizer_subquery_shared_scan_test.py    # OptimizeSubquerySharedScanRule
private_optimizer_skewed_bhj_join_test.py         # OptimizeSkewedBHJJoinRule
private_optimizer_README.md                       # this file
```

All modules carry `@pytest.mark.private_optimizer`, so the whole area runs with:

```bash
./integration_tests/run_pyspark_from_build.sh -m private_optimizer
```

## Adding a new private-optimizer IT

1. **Create one module per rule**: `private_optimizer_<rule>_test.py`. Do not
   keep growing a single file — one rule (or closely related feature) per
   module keeps the gate confs and query shape readable.
2. **Import the shared helpers** from `private_optimizer_common`:
   - `private_optimizer_conf(*rule_confs, extra_conf=None)` — builds the conf
     dict. It starts from `PRIVATE_OPTIMIZER_BASE_CONF` (which sets
     `spark.rapids.sql.private.enabled=true`), merges each positional rule-conf
     dict in order, then applies the optional `extra_conf` (query-shaping confs
     such as broadcast thresholds or AQE toggles).
   - `assert_rule_fires(fn, on_conf, off_conf, marker, physical=False)`
     — runs the OFF-CPU vs ON-GPU comparison and the plan-marker check.
   - `require_private_optimizer` — the shared compatibility guard (see below).
3. **Decorate** the test with `@pytest.mark.private_optimizer` and
   `@require_private_optimizer`. Add `@approximate_float` only when the rule
   changes floating-point evaluation order — that is enough for floating-point
   tolerance, since `assert_rule_fires` routes result comparison through
   `assert_equal_with_local_sort`, whose `get_float_check()` honors it.

### Choosing the gate conf(s)

Each private rule has its own enable conf in the private `OptimizerConf`. Build
`on`/`off` conf dicts that differ **only** in that one rule conf, e.g.:

```python
on  = private_optimizer_conf({"spark.rapids.sql.optimizer.aggPushdownEnabled": "true"},  extra_conf=base)
off = private_optimizer_conf({"spark.rapids.sql.optimizer.aggPushdownEnabled": "false"}, extra_conf=base)
```

If the rule's conf defaults to **on**, the OFF baseline must set it `false`
explicitly (e.g. `optimizeScalarSubquery`). Put query-shaping confs that must
be identical on both sides into `extra_conf` so they cannot accidentally become
the discriminator.

### Building a query that makes the rule reachable

The rule only fires under specific plan conditions; reproduce them in `fn`:

- Use a **row-counted relation** (write to parquet, then read it back) when the
  rule needs real statistics — `spark.range(...)` alone may not give the
  optimizer the sizes it keys off (e.g. AggPushdown's fact/dim ratio).
- Force the join/AQE shape the rule expects (e.g. keep a join as a shuffle join
  with a low positive `autoBroadcastJoinThreshold`, or force a runtime
  broadcast for the skew rule with `autoBroadcastJoinThreshold=-1` plus
  `adaptive.autoBroadcastJoinThreshold`).
- Toggle AQE to match where the rule runs (logical optimizer vs AQE re-opt).

### Proving the rule fired (the teeth)

Pick a `marker` substring that appears in the plan **only** when the rule
fires, and pass it to `assert_rule_fires`. The helper asserts the marker is
present in the ON plan and absent from the OFF plan, so a no-op conf flip fails
the test. Use `physical=True` when the effect is only visible after AQE (the
executed plan); otherwise the optimized plan is checked. Examples in this area:
`sum(sum(` (pushed partial aggregate), `_stddev_` (decomposed alias),
`named_struct(c_0,` (merged subquery scan), `coalesced and skewed` (skew
reader).

### When a row-count / aggregate-only check is acceptable

The default is a full row-by-row `OFF-CPU == ON-GPU` comparison. A reduced check
(e.g. a small GLOBAL aggregate such as `count/min/max/sum` over the rule's
output, instead of returning the joined rows) is acceptable when returning the
full result is impractical — for example the skew rule is validated with a
global aggregate over the materialized skewed join, intentionally avoiding a
`GROUP BY` on the skew key. Even then, keep the plan-marker assertion: the
aggregate proves data parity on the path that does work, the marker proves the
rule actually ran. Do **not** weaken the marker check to make a test pass.

### Compatibility across Spark versions and runtimes

The private plugin is built for **Spark 3.3.0 and later** (private core build
matrix: 330..411 plus the `400db173` Databricks buildver). Below 3.3.0 the rules
are not present at all, so `require_private_optimizer` (a
`pytest.mark.skipif(is_before_spark_330())` in `private_optimizer_common`) skips
the whole area there. Within that matrix all four current rules apply on every
runtime, including Databricks, so we do **not** add per-runtime skips.

If a future rule is genuinely unsupported on some runtime/version, express it
where the source actually gates it:

- a real version/runtime floor → an extra `@pytest.mark.skipif(...)` on that
  rule's module using the existing helpers in `spark_session.py`
  (`is_before_spark_3xx`, `is_databricks_runtime`, ...);
- an internal kill-switch (the skew rule's
  `AQEUtils.isOptimizeSkewBHJSupported`, which makes the rule a no-op when
  false) is caught for free by the plan-marker assertion — if support is
  dropped, the marker disappears and the test fails loudly instead of passing
  silently. Prefer this over guessing a version number.

### Validating coverage / plan effects locally

```bash
PYSP_TEST_spark_rapids_memory_gpu_allocFraction=0.3 \
PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0 \
PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=0.3 \
TEST_PARALLEL=0 \
./integration_tests/run_pyspark_from_build.sh -m private_optimizer
```

This requires a build whose `dist` jar includes the private plugin. A green run
means: the OFF-CPU and ON-GPU results matched (correctness + parity) and every
marker was present ON / absent OFF (the rules really fired). To inspect a plan
effect by hand, temporarily print the plan string returned by `collect_and_plan`
for the ON and OFF confs and diff them.
