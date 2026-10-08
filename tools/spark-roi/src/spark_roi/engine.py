# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Deterministic cost accounting. Money is Decimal; comparisons never predict speedups."""

from __future__ import annotations

from collections import defaultdict
from decimal import ROUND_CEILING, Decimal

from . import __version__
from .model import ONE, ZERO, annual_runs, fingerprint, number, runtime, validate

HOUR = Decimal(3600)
MONTHS = Decimal(12)


def quantity(plan: dict, rate: dict) -> Decimal:
    """Derive billable quantity from the exact configuration used by the runtime evidence."""
    if rate["unit"] == "cluster":
        return ONE
    node = next(n for n in plan["configuration"]["nodes"] if n["id"] == rate["node_group"])
    per_node = {"node": ONE, "gpu": node["gpus"], "vcpu": node["vcpus"], "gib": node["memory_gib"]}
    return number(node["count"], "count") * number(per_node[rate["unit"]], "quantity")


def billed_seconds(seconds: Decimal, rate: dict) -> Decimal:
    step = number(rate.get("increment_seconds", 1), "increment_seconds", positive=True)
    minimum = number(rate.get("minimum_seconds", 0), "minimum_seconds")
    return (max(seconds, minimum) / step).to_integral_value(rounding=ROUND_CEILING) * step


def _rate_line(plan: dict, rate: dict, seconds: Decimal, repeats: Decimal) -> dict:
    hours = billed_seconds(seconds, rate) * repeats / HOUR
    units = quantity(plan, rate)
    discount = number(rate.get("discount_percent", 0), "discount_percent")
    price = number(rate["hourly_rate"], "hourly_rate")
    return {
        "component": rate["id"],
        "quantity": units,
        "unit": rate["unit"],
        "billed_hours": hours,
        "unit_hourly_rate": price,
        "discount_percent": discount,
        "annual_cost": units * price * (1 - discount / 100) * hours,
    }


def _scenario(document: dict, scenario: dict, bound: str) -> dict:
    plans = {p["id"]: p for p in document["plans"]}
    workloads = {w["id"]: w for w in document["workloads"]}
    years = int(number(document["years"], "years"))
    grouped = defaultdict(list)
    rows = []
    issues = []
    notes = []
    for run in scenario["runs"]:
        workload = workloads[run["workload_id"]]
        plan = plans[run["plan_id"]]
        billing = plan["billing"]
        duration = runtime(run, bound)
        repeats = annual_runs(workload)
        attempts = number(run.get("attempts_per_success", 1), "attempts_per_success")
        share = number(run.get("allocation_fraction", 1), "allocation_fraction")
        overhead = number(billing.get("startup_seconds", 0), "startup_seconds") + number(
            billing.get("shutdown_seconds", 0), "shutdown_seconds"
        )
        active_hours = (duration + overhead) * repeats * attempts * share / HOUR
        row = {
            "workload_id": workload["id"],
            "plan_id": plan["id"],
            "runtime_seconds": duration,
            "runs_per_year": repeats,
            "attempts_per_success": attempts,
            "allocation_fraction": share,
            "capacity_hours_per_year": active_hours,
            "evidence_kind": run["evidence"]["kind"],
            "evidence_source": run["evidence"]["source"],
            "sample_count": len(run["evidence"].get("samples_seconds", [])),
            "configuration_fingerprint": fingerprint(plan["configuration"]),
            "workload_fingerprint": fingerprint(workload["definition"]),
            "annual_direct_cost": number(
                run.get("extra_cost_per_attempt", 0), "extra_cost_per_attempt"
            )
            * repeats
            * attempts,
            "annual_allocated_plan_cost": ZERO,
            "annual_depreciation": ZERO,
            "result_validation": workload["result_validation"]["status"],
        }
        deadline = workload.get("deadline_seconds")
        completion = duration + number(billing.get("startup_seconds", 0), "startup_seconds")
        row["completion_seconds"] = completion
        row["deadline_met"] = (
            None if deadline is None else completion <= number(deadline, "deadline")
        )
        if row["deadline_met"] is False:
            issues.append(
                f"{workload['id']}: {completion}s including startup exceeds "
                f"the {deadline}s deadline"
            )
        if workload["result_validation"]["status"] == "failed":
            issues.append(f"{workload['id']}: output correctness validation failed")
        elif workload["result_validation"]["status"] == "not_checked":
            notes.append(f"{workload['id']}: output correctness has not been verified")
        if run["evidence"]["kind"] == "estimated":
            notes.append(f"{workload['id']}: GPU/CPU time is an estimate, not a measured result")
            row["assumptions"] = run["evidence"]["assumptions"]
            if runtime(run, "low") == runtime(run, "high"):
                notes.append(f"{workload['id']}: no runtime uncertainty range was supplied")
        if "observed_runs" in workload["frequency"]:
            notes.append(
                f"{workload['id']}: annual frequency extrapolates the stated observation "
                "window; seasonality is not inferred"
            )
        grouped[plan["id"]].append((run, row, duration + overhead, repeats * attempts))
        rows.append(row)

    plan_results = []
    for plan_id in scenario.get("retained_plans", []):
        grouped.setdefault(plan_id, [])
    unallocated_cash = ZERO
    unallocated_depreciation = ZERO
    capital_flows = [ZERO for _ in range(years + 1)]
    capital_flows[0] = number(scenario["migration_cost"], "migration_cost")
    for plan_id, entries in grouped.items():
        plan = plans[plan_id]
        b = plan["billing"]
        active = sum((entry[1]["capacity_hours_per_year"] for entry in entries), ZERO)
        lines = []
        depreciation = ZERO
        if b["mode"] == "ephemeral":
            for _, row, duration, attempts in entries:
                for rate in b["rates"]:
                    line = _rate_line(plan, rate, duration, attempts)
                    line["workload_id"] = row["workload_id"]
                    lines.append(line)
                    row["annual_direct_cost"] += line["annual_cost"]
            uptime = None
            utilization = None
        else:
            uptime = number(b["uptime_hours_per_year"], "uptime_hours_per_year")
            utilization = active / uptime * 100
            if active > uptime:
                issues.append(
                    f"{plan_id}: demand needs {active:.2f} full-cluster hours/year, "
                    f"but only {uptime} hours are available"
                )
            for rate in b["rates"]:
                lines.append(_rate_line(plan, rate, uptime * HOUR, ONE))
            if b["mode"] == "owned":
                o = b["ownership"]
                capital = number(o["capital_cost"], "capital_cost")
                residual = number(o["residual_value"], "residual_value")
                life = int(number(o["life_years"], "life_years"))
                depreciation = (capital - residual) / life
                if o["purchase_required"]:
                    capital_flows[0] += capital
                    next_replacement = life
                else:
                    next_replacement = int(
                        number(o["remaining_life_years"], "remaining_life_years")
                    )
                # Replacements occur at year boundaries; no purchase at the end of the horizon.
                for year in range(next_replacement, years, life):
                    capital_flows[year] += capital - residual
                electricity = (
                    (
                        active * number(o["active_kw"], "active_kw")
                        + max(ZERO, uptime - active) * number(o["idle_kw"], "idle_kw")
                    )
                    * number(o["pue"], "pue")
                    * number(o["electricity_per_kwh"], "electricity")
                )
                lines.extend(
                    [
                        {"component": "electricity", "annual_cost": electricity},
                        {
                            "component": "maintenance",
                            "annual_cost": number(o["annual_maintenance"], "annual_maintenance"),
                        },
                        {
                            "component": "facility",
                            "annual_cost": number(
                                o["annual_facility_cost"], "annual_facility_cost"
                            ),
                        },
                    ]
                )
        fixed = number(b.get("fixed_cost_per_year", 0), "fixed_cost_per_year")
        if fixed:
            lines.append({"component": "fixed", "annual_cost": fixed})
        recurring_plan_cost = sum((line["annual_cost"] for line in lines), ZERO)
        shared = fixed if b["mode"] == "ephemeral" else recurring_plan_cost
        if not entries:
            unallocated_cash += shared
            unallocated_depreciation += depreciation
        for _, row, _, _ in entries:
            weight = row["capacity_hours_per_year"] / active
            row["annual_allocated_plan_cost"] = shared * weight
            row["annual_depreciation"] = depreciation * weight
        plan_results.append(
            {
                "plan_id": plan_id,
                "billing_mode": b["mode"],
                "annual_cost": recurring_plan_cost,
                "annual_depreciation": depreciation,
                "capacity_hours_required": active,
                "uptime_hours_per_year": uptime,
                "capacity_utilization_percent": utilization,
                "configuration_fingerprint": fingerprint(plan["configuration"]),
                "price_source": plan["price_source"],
                "price_as_of": plan["price_as_of"],
                "components": lines,
            }
        )
    for row in rows:
        row["annual_cash_cost"] = row["annual_direct_cost"] + row["annual_allocated_plan_cost"]
        row["annual_economic_cost"] = row["annual_cash_cost"] + row["annual_depreciation"]
        row["allocated_cost_per_success"] = row["annual_cash_cost"] / row["runs_per_year"]
        workload = workloads[row["workload_id"]]
        if "work_units" in workload:
            row["cost_per_work_unit"] = row["allocated_cost_per_success"] / number(
                workload["work_units"], "work_units"
            )
            row["work_unit"] = workload["unit"]
        if plans[row["plan_id"]]["billing"]["mode"] != "ephemeral":
            row["allocation_note"] = (
                "Shared plan costs allocated by capacity-hours, including idle "
                "cost; these allocations are not independently avoidable cash savings."
            )
    annual_cash = sum((r["annual_cash_cost"] for r in rows), unallocated_cash)
    annual_economic = sum(
        (r["annual_economic_cost"] for r in rows), unallocated_cash + unallocated_depreciation
    )
    cash_flows = [capital_flows[0]] + [annual_cash + capital_flows[i] for i in range(1, years + 1)]
    return {
        "id": scenario["id"],
        "runtime_bound": bound,
        "annual_cash_cost": annual_cash,
        "annual_economic_cost": annual_economic,
        "upfront_cost": capital_flows[0],
        "capital_cash_flows": capital_flows,
        "cash_outflows_by_year": cash_flows,
        "annual_unallocated_cash_cost": unallocated_cash,
        "cash_tco": sum(cash_flows, ZERO),
        "economic_tco": annual_economic * years + number(scenario["migration_cost"], "migration"),
        "feasible": not issues,
        "issues": issues,
        "notes": sorted(set(notes)),
        "results_verified": all(r["result_validation"] == "passed" for r in rows),
        "contains_estimates": any(r["evidence_kind"] == "estimated" for r in rows),
        "plans": plan_results,
        "workloads": rows,
    }


def _compare(baseline: dict, candidate: dict, years: int, discount: Decimal) -> dict:
    annual_saving = baseline["annual_cash_cost"] - candidate["annual_cash_cost"]
    net = baseline["cash_tco"] - candidate["cash_tco"]
    capital_delta = [
        b - g for b, g in zip(baseline["capital_cash_flows"], candidate["capital_cash_flows"])
    ]
    investment = sum((max(ZERO, -x) for x in capital_delta), ZERO)
    net_flows = [
        b - g for b, g in zip(baseline["cash_outflows_by_year"], candidate["cash_outflows_by_year"])
    ]
    npv = sum((flow / (1 + discount / 100) ** i for i, flow in enumerate(net_flows)), ZERO)
    # Uniform recurring monthly spend, capital events at year boundaries. Report the
    # first month after which cumulative savings stay nonnegative within the horizon.
    cumulative = [capital_delta[0]]
    capital_to_date = capital_delta[0]
    for month in range(1, years * 12 + 1):
        if month % 12 == 0:
            capital_to_date += capital_delta[month // 12]
        # Multiply before dividing instead of repeatedly adding rounded thirds of a cent.
        cumulative.append(capital_to_date + annual_saving * month / MONTHS)
    suffix_minimum = cumulative[-1]
    payback = None
    for month in range(len(cumulative) - 1, -1, -1):
        suffix_minimum = min(suffix_minimum, cumulative[month])
        if suffix_minimum >= 0:
            payback = month
    comparable = (
        baseline["feasible"]
        and candidate["feasible"]
        and baseline["results_verified"]
        and candidate["results_verified"]
    )
    return {
        "annual_cash_savings": annual_saving,
        "annual_economic_savings": baseline["annual_economic_cost"]
        - candidate["annual_economic_cost"],
        "horizon_net_cash_benefit": net,
        "cash_tco_savings_percent": (
            net / baseline["cash_tco"] * 100 if baseline["cash_tco"] else None
        ),
        "incremental_investment": investment,
        "roi_percent": net / investment * 100 if investment > 0 else None,
        "npv": npv,
        "sustained_payback_months": payback,
        "eligible_for_cost_comparison": comparable,
        "lower_cash_tco": comparable and net > 0,
        "evidence_status": (
            "unverified"
            if not comparable
            else "estimated"
            if (baseline["contains_estimates"] or candidate["contains_estimates"])
            else "measured"
        ),
    }


def evaluate(document: dict) -> dict:
    validate(document)
    years = int(number(document["years"], "years"))
    discount = number(document.get("discount_rate_percent", 0), "discount_rate_percent")
    variants = {
        s["id"]: {bound: _scenario(document, s, bound) for bound in ["base", "low", "high"]}
        for s in document["scenarios"]
    }
    base = variants[document["baseline"]]
    results = []
    for scenario in document["scenarios"]:
        group = variants[scenario["id"]]
        result = group["base"]
        if scenario["id"] != document["baseline"]:
            result["comparison"] = _compare(base["base"], result, years, discount)
            result["sensitivity"] = {
                "conservative": _compare(base["low"], group["high"], years, discount),
                "optimistic": _compare(base["high"], group["low"], years, discount),
                "meaning": "Supplied estimate bounds or observed min/max runtimes; not a "
                "statistical confidence interval. Prices and frequency are held fixed.",
            }
            baseline_rows = {r["workload_id"]: r for r in base["base"]["workloads"]}
            for row in result["workloads"]:
                old = baseline_rows[row["workload_id"]]
                row["speedup"] = old["runtime_seconds"] / row["runtime_seconds"]
                row["annual_allocated_cash_savings"] = (
                    old["annual_cash_cost"] - row["annual_cash_cost"]
                )
        results.append(result)
    return {
        "schema_version": 1,
        "tool_version": __version__,
        "name": document["name"],
        "currency": document["currency"],
        "years": years,
        "baseline": document["baseline"],
        "discount_rate_percent": discount,
        "input_fingerprint": fingerprint(document),
        "cost_exclusions": document["cost_exclusions"],
        "scenarios": results,
        "assumptions": [
            "A planning year has 365 days. Workload frequency is supplied, never guessed.",
            "Measured runtimes use the median; all repetitions describe the same workload/config.",
            "Only ephemeral plans release billed capacity when runs complete.",
            "Shared/persistent plans are charged once per scenario, not once per workload.",
            "Capacity-hours check aggregate feasibility, not a concurrent schedule.",
            "Owned asset depreciation is separate from cash purchases; no terminal resale credit.",
            "Owned replacements use stated lifetime, unchanged real prices and residual proceeds.",
            "NPV discounts end-of-year recurring cash costs and year-boundary capital events.",
            "No automatic tax, inflation, financing, currency conversion or live price lookup.",
            "Fingerprints record the supplied scope; they do not prove a benchmark was run.",
        ],
        "input": document,
    }
