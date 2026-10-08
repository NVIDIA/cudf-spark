# Spark ROI

Compare complete Spark CPU/GPU workload costs using your prices and measured or
estimated runtimes. Reports include per-job costs, annual spending, cash and
economic TCO, ROI, sustained payback, NPV and runtime sensitivity. Python 3.10+
is sufficient; the offline calculator has no Spark, CUDA or cloud dependencies.

## Install and run

From the repository root:

~~~sh
python -m pip install ./tools/spark-roi
spark-roi init --example cloud --output scenario.json
spark-roi evaluate scenario.json --output roi-report
~~~

Open roi-report/report.html. The same directory contains report.json with the
full input, scenarios.csv, workloads.csv, components.csv and summary.txt.
The HTML works offline. Existing files require --overwrite.

All example prices and runtimes are SYNTHETIC. Replace them with your own data.
The init command also accepts --example shared and --example owned. Install a
built wheel with pip or pipx; a Git URL can use #subdirectory=tools/spark-roi.
No PyPI publication is needed.

中文：这个工具算“同一批任务从 CPU 换到 GPU，能省多少钱、多久回本”。
填机器配置和价格、对应任务耗时、一年跑几次，再生成报告。
cloud 是跑完关机；shared 是集群一直开着；owned 是自己买机器。
GPU 跑快了，不代表常开机器的租金自动减少。示例数字不是真实收益。

## Input reference

Start with a generated JSON example. Numbers may be JSON numbers or decimal
strings. Unknown keys, duplicate IDs/JSON keys, nonfinite values, booleans as
numbers and negative costs are rejected; nonzero numbers must be in [1e-18, 1e24].
Use one currency throughout. Same-name workloads are never merged automatically.

### Document and hardware plans

| Object | Required fields | Optional fields |
| --- | --- | --- |
| Document | schema_version (1), name, currency (uppercase three-letter code), years (integer 1–50), baseline (scenario ID), plans, workloads, scenarios, cost_exclusions (list, possibly empty) | discount_rate_percent (0–100, default 0), notes |
| Plan | id, configuration, billing, price_source, price_as_of (YYYY-MM-DD) | label, notes |
| configuration | nodes, spark_version, spark_conf (string-to-string map) | plugin_version |
| Node group | id, instance_type, count (positive integer), vcpus, memory_gib, gpus (nonnegative integer) | gpu_type (required when gpus > 0) |
| billing | mode, rates (list) | fixed_cost_per_year (default 0), mode-specific fields below |
| Rate component | id, unit, hourly_rate | node_group, discount_percent (0–100, default 0), minimum_seconds (default 0), increment_seconds (positive, default 1) |

Node vcpus, memory_gib and gpus are **per node**; count determines the billable
quantity. Include a separate driver group when appropriate. Record relevant Spark
settings, code/software versions and workload scope alongside the measurements.

Rate units node, gpu, vcpu and gib require a node_group reference. Unit cluster
means one whole-plan charge and forbids node_group. Add separately billed GPU,
platform/DBU, memory or driver charges as components. Do not charge a GPU again
when its price is already included in VM rent. Supply net prices or one discount
per component, with a dated source covering the quoted components.

| Billing mode | Additional fields | Cost treatment |
| --- | --- | --- |
| ephemeral | startup_seconds and shutdown_seconds, both default 0 | Billed per attempt, with each component's minimum and increment |
| persistent | uptime_hours_per_year in (0, 8760] | Whole stated uptime, once per plan per scenario |
| owned | uptime_hours_per_year in (0, 8760], ownership | Electricity, maintenance and facility expenses; purchases and depreciation handled separately |

For persistent/owned plans, include startup/shutdown in annual uptime instead of
per-run overhead. fixed_cost_per_year is charged once per plan in every mode.
Storage, network and other costs can be components, annual fixed costs or
extra_cost_per_attempt; explicitly list omitted items in cost_exclusions.

Owned billing.ownership requires the following whole-plan values:

| Field | Meaning |
| --- | --- |
| capital_cost, residual_value | Purchase amount and expected replacement resale proceeds; residual cannot exceed capital |
| life_years | Integer replacement/depreciation lifetime, 1–100 years |
| purchase_required | true for buying now; false for equipment already owned |
| remaining_life_years | Required only for existing equipment; integer 1 through life_years |
| annual_maintenance, annual_facility_cost | Annual expenses, excluding separately entered electricity |
| active_kw, idle_kw | Whole-plan power, including drivers; idle cannot exceed active |
| electricity_per_kwh, pue | Electricity price and facility overhead multiplier (pue >= 1) |

Owned rates may add service/license fees, but should not rent the same hardware
again. Ownership amounts already cover all nodes; do not multiply them twice.

### Workloads, scenarios and evidence

| Object | Required fields | Optional fields |
| --- | --- | --- |
| Workload | id, definition (code/query and dataset identity), frequency, result_validation | name, app_id, deadline_seconds, work_units with unit |
| frequency | source and either runs_per_year or observed_runs with observation_days | None |
| result_validation | status (passed, not_checked or failed), source | None |
| Scenario | id, migration_cost, runs (one for every workload) | name, retained_plans |
| Run | workload_id, plan_id, evidence | allocation_fraction (default 1), attempts_per_success (default 1), extra_cost_per_attempt (default 0) |
| evidence | kind, source, configuration_fingerprint, workload_fingerprint | Kind-specific fields below |
| Measured evidence | kind: measured, samples_seconds (nonempty positive values) | assumptions |
| Estimated evidence | kind: estimated, duration_seconds, assumptions | low_seconds, high_seconds, bracketing duration_seconds |

Every scenario covers the same workload IDs and volume; mixed CPU/GPU execution
and multiple alternatives are supported. definition identifies the same code,
input snapshot/size and parameters in every scenario. Compare complete application
times consistently, not CPU end-to-end time against GPU SQL-only time.

runs_per_year is positive. Alternatively, positive integer observed_runs over
positive observation_days gives annual demand as observed_runs * 365 / observation_days.
This extrapolation must describe a representative period; the tool never guesses
a daily frequency from a single log. Prices and frequency stay fixed across the
runtime sensitivity cases.

result_validation describes result equivalence across all configurations in this
assessment; split assessments if validation covers only some alternatives.
deadline_seconds is a positive per-attempt target including startup, excluding
shutdown and retry/queue tails. Positive work_units with a unit enables cost/unit.

allocation_fraction is in (0, 1] and must be 1 for ephemeral runs. Use a smaller
share only when measured execution occupies that fraction of a shared cluster.
attempts_per_success is at least 1: expected full-attempt-equivalent executions,
including failures. Each attempt uses the stated duration and per-attempt cost.

A shared/owned plan is billed once even when several jobs use it. Its workload
allocations include idle costs and are not independently avoidable cash savings.
Put any CPU plan that stays paid after migration into retained_plans; an idle
retained plan is still charged. Listing an already-used plan does not charge twice.
migration_cost is the scenario's upfront engineering/transition expense.

Measured runtimes use the median; low/high cases use observed min/max. Estimates
use supplied durations and bounds. Bounds are scenarios, not confidence intervals.
Missing bounds leave the estimate unchanged. Incorrect/unchecked results,
over-capacity shared plans and missed deadlines prevent eligible comparisons.
Capacity-hours check annual demand, not a concurrent schedule or an autoscaling
trace. Model different configurations with matching evidence, or supply explicit
effective billed rates/uptime for a stated usage pattern.

Fingerprints bind evidence to the supplied configuration and workload definition.
A hardware, Spark setting or dataset change invalidates that evidence; changing
price alone does not. After obtaining corresponding new evidence, use:

~~~sh
spark-roi bind unbound.json --output scenario.json
spark-roi validate scenario.json
spark-roi fingerprints scenario.json
~~~

Binding fills missing fingerprints and refuses mismatches. It records an assertion,
not proof that a benchmark was run. Import-only manifests may use null evidence;
evaluation requires every record complete and correctly bound.

## Import timings

Prices, configurations, frequency and correctness evidence remain explicit inputs.

| Command | Accepted data | Interpretation |
| --- | --- | --- |
| import-qualification | Ungrouped CSV with App ID, App Duration, Estimated GPU Duration | CPU measured wall time and GPU prediction; default units ms |
| import-runs | CSV columns scenario_id, workload_id, plan_id, configuration_fingerprint, workload_fingerprint, duration_seconds, source | Repetitions of one workload/configuration become measured samples |
| import-eventlogs | One completed plain/gzip JSON-lines Spark application per file | ApplicationStart/End wall time; overlapping task times are not added |

~~~sh
spark-roi import-qualification qualification.csv --scenario scenario.json --candidate gpu --output imported.json
spark-roi import-runs timings.csv --scenario scenario.json --output measured.json
spark-roi import-eventlogs app.json app2.json.gz --scenario scenario.json --scenario-id cpu --workload-id daily-etl --output measured.json
~~~

Qualification app_id (or workload ID) must map exactly once to each CSV App ID;
missing, duplicate and unmapped applications are rejected. Use --cpu-column,
--gpu-column and --duration-unit s for other headings/units. Confirm that the GPU
plan matches the qualification prediction; there is no automatic resizing.
The adapter imports no old frequency or price defaults.

Measured CSV fingerprints come from the fingerprints command and must match.
Event logs require unique application IDs and complete start/end events. Available
Spark versions and declared Spark settings are checked; hardware inventory and
dataset identity are supplied by the manifest. Join rolling segments belonging
to one application before import; decompress unsupported codecs first.

## Accounting rules

Calculations use Decimal; displayed money is rounded to two decimal places.
Keep decimal strings when exact input precision matters.

* Annual attempts = annual successful runs * attempts_per_success.
* Ephemeral component cost = hardware-derived quantity * hourly_rate *
  (1 - discount_percent/100) * rounded seconds/3600 * annual attempts.
  Per-attempt rounded seconds = ceil(max(runtime + startup + shutdown, minimum) /
  increment) * increment. Minimums and rounding apply to each component.
* Persistent rates use annual uptime as one billing window; for multiple windows,
  supply effective billed uptime. Faster jobs do not shorten that obligation.
* Active capacity-hours = (runtime + applicable overhead) * annual attempts *
  allocation_fraction. Shared cost allocations use these hours.
* Owned electricity = (active hours * active_kw + remaining idle hours * idle_kw) *
  pue * electricity_per_kwh. Maintenance/facility expenses are added separately.
* Annual depreciation = (capital_cost - residual_value) / life_years.
  Economic TCO = annual cash expense plus depreciation, times years, plus migration.
* Cash TCO includes actual purchases, migration and recurring expenses, never
  depreciation. Existing purchase cost is sunk; remaining life determines the
  first replacement. New purchases are upfront. Subsequent replacements before
  the horizon ends cost capital minus residual; there is no terminal resale credit.
* Net cash benefit = baseline cash TCO - alternative cash TCO.
  ROI = net benefit / sum of positive incremental investments at capital events.
  Without incremental investment, ROI is N/A.
* Sustained payback is the first whole month from which cumulative cash benefit
  remains nonnegative to the horizon, accounting for later replacements. Recurring
  savings accrue uniformly monthly; no recovery within the horizon is reported.
* NPV discounts recurring cash costs at year ends and capital events at year
  boundaries using discount_rate_percent. Prices stay constant; taxes, financing,
  inflation, future prices, currency conversion and terminal resale are not inferred.

## Design sources

Reviewed 2026-10-08; a bounded public review, not a catalog of all commercial tools.
Earlier savings work is in NVIDIA/cudf-spark-tools, the companion repository.

| Source | Decision applied here |
| --- | --- |
| [PR 163](https://github.com/NVIDIA/cudf-spark-tools/pull/163), [1188](https://github.com/NVIDIA/cudf-spark-tools/pull/1188) | Bind runtime to the actual per-workload configuration; preserve IDs |
| [PR 189](https://github.com/NVIDIA/cudf-spark-tools/pull/189), [217](https://github.com/NVIDIA/cudf-spark-tools/pull/217) | Report annual costs with explicit frequency; avoid same-name averaging and implicit daily runs |
| [PR 583](https://github.com/NVIDIA/cudf-spark-tools/pull/583), [595](https://github.com/NVIDIA/cudf-spark-tools/pull/595) | Use dated customer prices and per-component discounts |
| [PR 1218](https://github.com/NVIDIA/cudf-spark-tools/pull/1218#issuecomment-2246233642), [1230](https://github.com/NVIDIA/cudf-spark-tools/pull/1230) | Use one validated input for configuration, runtime and billing |
| [AWS EMR/G7 benchmark](https://aws.amazon.com/blogs/big-data/gpu-accelerated-apache-spark-with-amazon-emr-and-nvidia-rtx-pro-4500-on-amazon-ec2-g7-instances-runs-up-to-3-7x-faster/), [Spark XGBoost study](https://aws.amazon.com/blogs/big-data/improving-rapids-xgboost-performance-and-reducing-costs-with-amazon-emr-running-amazon-ec2-g4-instances/) | Repeated complete-run timings, separately priced platform charges and explicit exclusions |
| [Dataproc billing](https://docs.cloud.google.com/dataproc/docs/resources/faq), [GPU/data-science discussion](https://cloud.google.com/blog/products/data-analytics/run-data-science-scale-dataproc-and-apache-spark) | Charge cluster lifetime, idle capacity and billing minimums |
| [Databricks usage](https://docs.databricks.com/aws/en/admin/system-tables/billing), [price joins](https://docs.databricks.com/aws/en/admin/system-tables/serverless-billing) | Preserve workload identity and dated price provenance; reconcile corrections before importing prices |
| [Alibaba EMR billing](https://www.alibabacloud.com/help/en/emr/emr-serverless-spark/product-overview/billing-items-and-billing-methods/) | Separate usage charges and continuing resource commitments |

The savings-disable discussion described incompatible per-application tuning and
per-cluster billing flows, not evidence of a generic multiplication bug. The
[pre-disable source](https://github.com/NVIDIA/cudf-spark-tools/blob/56e69620961cc99faeab54740344486c511cc97e/user_tools/src/spark_rapids_pytools/rapids/qualification.py#L1122-L1125)
records the duplicated flow. Published vendor speedups and historical prices are
not prediction defaults; billing references are not proof of GPU acceleration.
The regression suite checks the AWS example's arithmetic, not its performance.

## Development and API

~~~sh
python -m pip install -e ./tools/spark-roi build ruff coverage
ruff check tools/spark-roi
coverage run --source=spark_roi -m unittest discover -s tools/spark-roi/tests -v
coverage report --fail-under=85
python -m build tools/spark-roi
~~~

The Python API is spark_roi.engine.evaluate(spark_roi.model.load_json("scenario.json")).
CLI help: spark-roi --help. Evaluate accepts --format json and --strict.
Exit codes: 0 success; 2 invalid input/I/O error; 3 with --strict when a base or
conservative comparison is unverified/infeasible. Negative savings is valid.

Tests cover independent cost arithmetic, stale evidence, component billing,
shared/retained fleets, replacements, power, payback, imports and report escaping.
These are accounting tests, not a GPU workload benchmark.
The [license](LICENSE) contains Apache-2.0 for code and CC-BY-4.0 for documentation.
