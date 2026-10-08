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
"""Versioned input contract, strict validation, and evidence/configuration binding."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ZERO = Decimal(0)
ONE = Decimal(1)
HOURS_PER_YEAR = Decimal(8760)


class InputError(ValueError):
    """An actionable input error, safe to display without a stack trace."""


def number(value: Any, path: str, *, minimum: Any = 0, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise InputError(f"{path}: expected a finite number")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise InputError(f"{path}: expected a finite number") from exc
    if not result.is_finite() or result < Decimal(str(minimum)) or (positive and result == 0):
        constraint = "> 0" if positive else f">= {minimum}"
        raise InputError(f"{path}: expected a finite number {constraint}")
    if result > Decimal("1e24") or (result and result < Decimal("1e-18")):
        raise InputError(f"{path}: nonzero values must be between 1e-18 and 1e24")
    return result


def integer(value: Any, path: str, *, minimum: int = 1, maximum: int = 10**9) -> int:
    n = number(value, path, minimum=minimum)
    if n != n.to_integral_value() or n > maximum:
        raise InputError(f"{path}: expected an integer in [{minimum}, {maximum}]")
    return int(n)


def obj(value: Any, path: str, required: set[str], optional: set[str] | None = None) -> dict:
    if not isinstance(value, dict):
        raise InputError(f"{path}: expected an object")
    missing = required - value.keys()
    unknown = value.keys() - required - (optional or set())
    if missing or unknown:
        details = []
        if missing:
            details.append("missing " + ", ".join(sorted(missing)))
        if unknown:
            details.append("unknown " + ", ".join(sorted(unknown)))
        raise InputError(f"{path}: {'; '.join(details)}")
    return value


def text(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InputError(f"{path}: expected a nonempty string")
    return value


def items(value: Any, path: str, *, nonempty: bool = True) -> list:
    if not isinstance(value, list) or (nonempty and not value):
        raise InputError(f"{path}: expected {'a nonempty' if nonempty else 'an'} array")
    return value


def choice(value: Any, path: str, allowed: set[str]) -> str:
    text(value, path)
    if value not in allowed:
        raise InputError(f"{path}: expected one of {', '.join(sorted(allowed))}")
    return value


def percentage(value: Any, path: str) -> Decimal:
    n = number(value, path)
    if n > 100:
        raise InputError(f"{path}: expected a percentage at most 100")
    return n


def unique(values: list, path: str) -> dict[str, dict]:
    result = {}
    for i, value in enumerate(values):
        if not isinstance(value, dict):
            raise InputError(f"{path}[{i}]: expected an object")
        key = text(value.get("id"), f"{path}[{i}].id")
        if key in result:
            raise InputError(f"{path}: duplicate id {key!r}")
        result[key] = value
    return result


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise InputError(f"JSON: duplicate key {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise InputError(f"JSON: non-finite number {value}")


def load_json(path: str | Path) -> dict:
    try:
        return json.loads(
            Path(path).read_text(encoding="utf-8-sig"),
            object_pairs_hook=_reject_duplicates,
            parse_constant=_reject_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise InputError(f"{path}: {exc}") from exc


def canonical(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def fingerprint(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical(value).encode()).hexdigest()


def annual_runs(workload: dict) -> Decimal:
    frequency = workload["frequency"]
    if "runs_per_year" in frequency:
        return number(frequency["runs_per_year"], "runs_per_year", positive=True)
    return (
        number(frequency["observed_runs"], "observed_runs", positive=True)
        * 365
        / number(frequency["observation_days"], "observation_days", positive=True)
    )


def runtime(run: dict, bound: str = "base") -> Decimal:
    evidence = run["evidence"]
    if evidence["kind"] == "measured":
        samples = sorted(
            number(x, "samples_seconds", positive=True) for x in evidence["samples_seconds"]
        )
        if bound == "low":
            return samples[0]
        if bound == "high":
            return samples[-1]
        middle = len(samples) // 2
        return samples[middle] if len(samples) % 2 else (samples[middle - 1] + samples[middle]) / 2
    return number(
        evidence.get(f"{bound}_seconds", evidence["duration_seconds"]),
        "duration_seconds",
        positive=True,
    )


def _configuration(value: Any, path: str) -> dict:
    c = obj(value, path, {"nodes", "spark_version", "spark_conf"}, {"plugin_version"})
    text(c["spark_version"], path + ".spark_version")
    if "plugin_version" in c:
        text(c["plugin_version"], path + ".plugin_version")
    if not isinstance(c["spark_conf"], dict):
        raise InputError(path + ".spark_conf: expected an object")
    for key, val in c["spark_conf"].items():
        text(key, path + ".spark_conf key")
        text(val, path + ".spark_conf." + key)
    nodes = unique(items(c["nodes"], path + ".nodes"), path + ".nodes")
    for key, node in nodes.items():
        p = f"{path}.nodes[{key}]"
        obj(node, p, {"id", "instance_type", "count", "vcpus", "memory_gib", "gpus"}, {"gpu_type"})
        text(node["instance_type"], p + ".instance_type")
        integer(node["count"], p + ".count")
        number(node["vcpus"], p + ".vcpus", positive=True)
        number(node["memory_gib"], p + ".memory_gib", positive=True)
        gpus = integer(node["gpus"], p + ".gpus", minimum=0)
        if gpus:
            text(node.get("gpu_type"), p + ".gpu_type")
        elif "gpu_type" in node:
            text(node["gpu_type"], p + ".gpu_type")
    return nodes


def _plan(plan: dict, path: str) -> None:
    obj(
        plan,
        path,
        {"id", "configuration", "billing", "price_source", "price_as_of"},
        {"label", "notes"},
    )
    nodes = _configuration(plan["configuration"], path + ".configuration")
    for field in ["label", "notes"]:
        if field in plan:
            text(plan[field], path + "." + field)
    text(plan["price_source"], path + ".price_source")
    try:
        price_date = text(plan["price_as_of"], path + ".price_as_of")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", price_date):
            raise ValueError("invalid date format")
        date.fromisoformat(price_date)
    except ValueError as exc:
        raise InputError(path + ".price_as_of: expected YYYY-MM-DD") from exc
    b = obj(
        plan["billing"],
        path + ".billing",
        {"mode", "rates"},
        {
            "uptime_hours_per_year",
            "startup_seconds",
            "shutdown_seconds",
            "fixed_cost_per_year",
            "ownership",
        },
    )
    mode = choice(b["mode"], path + ".billing.mode", {"ephemeral", "persistent", "owned"})
    if mode != "ephemeral":
        uptime = number(
            b.get("uptime_hours_per_year"), path + ".uptime_hours_per_year", positive=True
        )
        if uptime > HOURS_PER_YEAR:
            raise InputError(path + ": uptime_hours_per_year cannot exceed 8760")
    elif "uptime_hours_per_year" in b:
        raise InputError(path + ": ephemeral plans do not accept uptime_hours_per_year")
    for key in ["startup_seconds", "shutdown_seconds", "fixed_cost_per_year"]:
        n = number(b.get(key, 0), path + ".billing." + key)
        if mode != "ephemeral" and key != "fixed_cost_per_year" and n:
            raise InputError(
                path + ": include persistent startup/shutdown in uptime_hours_per_year"
            )
    rates = unique(items(b["rates"], path + ".rates", nonempty=False), path + ".rates")
    for key, rate in rates.items():
        p = path + ".rates[" + key + "]"
        obj(
            rate,
            p,
            {"id", "unit", "hourly_rate"},
            {"node_group", "discount_percent", "minimum_seconds", "increment_seconds"},
        )
        unit = choice(rate["unit"], p + ".unit", {"cluster", "node", "gpu", "vcpu", "gib"})
        if unit == "cluster":
            if "node_group" in rate:
                raise InputError(p + ": cluster rates must not set node_group")
        elif not isinstance(rate.get("node_group"), str) or rate["node_group"] not in nodes:
            raise InputError(p + ": node_group must reference a configuration node group")
        elif unit == "gpu" and number(nodes[rate["node_group"]]["gpus"], p) == 0:
            raise InputError(p + ": GPU rate refers to a node group without GPUs")
        number(rate["hourly_rate"], p + ".hourly_rate")
        percentage(rate.get("discount_percent", 0), p + ".discount_percent")
        number(rate.get("minimum_seconds", 0), p + ".minimum_seconds")
        number(rate.get("increment_seconds", 1), p + ".increment_seconds", positive=True)
    if mode == "owned":
        o = obj(
            b.get("ownership"),
            path + ".ownership",
            {
                "capital_cost",
                "residual_value",
                "life_years",
                "purchase_required",
                "annual_maintenance",
                "annual_facility_cost",
                "active_kw",
                "idle_kw",
                "electricity_per_kwh",
                "pue",
            },
            {"remaining_life_years"},
        )
        for key in o.keys() - {"purchase_required", "life_years", "remaining_life_years"}:
            number(o[key], path + ".ownership." + key, minimum=1 if key == "pue" else 0)
        life = integer(o["life_years"], path + ".life_years", maximum=100)
        if type(o["purchase_required"]) is not bool:
            raise InputError(path + ".purchase_required: expected true or false")
        if not o["purchase_required"]:
            integer(o.get("remaining_life_years"), path + ".remaining_life_years", maximum=life)
        elif "remaining_life_years" in o:
            raise InputError(path + ": new assets must not set remaining_life_years")
        if number(o["residual_value"], path) > number(o["capital_cost"], path):
            raise InputError(path + ": residual_value exceeds capital_cost")
        if number(o["idle_kw"], path) > number(o["active_kw"], path):
            raise InputError(path + ": idle_kw exceeds active_kw")
    elif "ownership" in b:
        raise InputError(path + ": ownership costs require mode owned")


def validate(document: dict, *, allow_unbound: bool = False, allow_pending: bool = False) -> dict:
    """Reject malformed inputs before any report is produced; return the input unchanged."""
    d = obj(
        document,
        "scenario",
        {
            "schema_version",
            "name",
            "currency",
            "years",
            "baseline",
            "plans",
            "workloads",
            "scenarios",
            "cost_exclusions",
        },
        {"discount_rate_percent", "notes"},
    )
    if type(d["schema_version"]) is not int or d["schema_version"] != 1:
        raise InputError("schema_version: only 1 is supported")
    text(d["name"], "name")
    if "notes" in d:
        text(d["notes"], "notes")
    if not re.fullmatch(r"[A-Z]{3}", text(d["currency"], "currency")):
        raise InputError("currency: use one three-letter currency code for all prices")
    integer(d["years"], "years", maximum=50)
    percentage(d.get("discount_rate_percent", 0), "discount_rate_percent")
    for excluded in items(d["cost_exclusions"], "cost_exclusions", nonempty=False):
        text(excluded, "cost_exclusions")
    plans = unique(items(d["plans"], "plans"), "plans")
    for key, plan in plans.items():
        _plan(plan, f"plans[{key}]")
    workloads = unique(items(d["workloads"], "workloads"), "workloads")
    for key, w in workloads.items():
        p = f"workloads[{key}]"
        obj(
            w,
            p,
            {"id", "definition", "frequency", "result_validation"},
            {"name", "deadline_seconds", "work_units", "unit", "app_id"},
        )
        text(w["definition"], p + ".definition")
        if "name" in w:
            text(w["name"], p + ".name")
        f = obj(
            w["frequency"],
            p + ".frequency",
            {"source"},
            {"runs_per_year", "observed_runs", "observation_days"},
        )
        text(f["source"], p + ".frequency.source")
        if "runs_per_year" in f:
            if {"observed_runs", "observation_days"} & f.keys():
                raise InputError(p + ": specify frequency directly OR an observation window")
        else:
            integer(f.get("observed_runs"), p + ".observed_runs")
            number(f.get("observation_days"), p + ".observation_days", positive=True)
        annual_runs(w)
        rv = obj(w["result_validation"], p + ".result_validation", {"status", "source"})
        choice(rv["status"], p + ".result_validation.status", {"passed", "not_checked", "failed"})
        text(rv["source"], p + ".result_validation.source")
        if "deadline_seconds" in w:
            number(w["deadline_seconds"], p + ".deadline_seconds", positive=True)
        if "work_units" in w or "unit" in w:
            number(w.get("work_units"), p + ".work_units", positive=True)
            text(w.get("unit"), p + ".unit")
        if "app_id" in w:
            text(w["app_id"], p + ".app_id")
    scenarios = unique(items(d["scenarios"], "scenarios"), "scenarios")
    if len(scenarios) < 2 or not isinstance(d["baseline"], str) or d["baseline"] not in scenarios:
        raise InputError("scenarios: need a named baseline and at least one alternative")
    for key, scenario in scenarios.items():
        p = f"scenarios[{key}]"
        obj(scenario, p, {"id", "runs", "migration_cost"}, {"name", "retained_plans"})
        number(scenario["migration_cost"], p + ".migration_cost")
        if "name" in scenario:
            text(scenario["name"], p + ".name")
        retained = items(scenario.get("retained_plans", []), p + ".retained_plans", nonempty=False)
        for retained_id in retained:
            if not isinstance(retained_id, str) or retained_id not in plans:
                raise InputError(p + ": retained_plans must reference existing plans")
            if plans[retained_id]["billing"]["mode"] == "ephemeral":
                raise InputError(p + ": retained plans must be persistent or owned")
        if len(set(retained)) != len(retained):
            raise InputError(p + ": duplicate retained plan")
        seen = set()
        for i, run in enumerate(items(scenario["runs"], p + ".runs")):
            r = f"{p}.runs[{i}]"
            obj(
                run,
                r,
                {"workload_id", "plan_id", "evidence"},
                {"allocation_fraction", "attempts_per_success", "extra_cost_per_attempt"},
            )
            wid, pid = run["workload_id"], run["plan_id"]
            if not isinstance(wid, str) or wid not in workloads or wid in seen:
                raise InputError(r + ": workload_id must exist and occur once per scenario")
            if not isinstance(pid, str) or pid not in plans:
                raise InputError(r + ": unknown plan_id")
            seen.add(wid)
            allocation = number(
                run.get("allocation_fraction", 1), r + ".allocation_fraction", positive=True
            )
            if allocation > 1 or (allocation != 1 and plans[pid]["billing"]["mode"] == "ephemeral"):
                raise InputError(r + ": allocation_fraction must be <= 1; ephemeral runs require 1")
            number(run.get("attempts_per_success", 1), r + ".attempts_per_success", minimum=1)
            number(run.get("extra_cost_per_attempt", 0), r + ".extra_cost_per_attempt")
            e = run["evidence"]
            if allow_pending and e is None:
                continue
            obj(
                e,
                r + ".evidence",
                {"kind", "source"},
                {
                    "configuration_fingerprint",
                    "workload_fingerprint",
                    "samples_seconds",
                    "duration_seconds",
                    "low_seconds",
                    "high_seconds",
                    "assumptions",
                },
            )
            kind = choice(e["kind"], r + ".kind", {"measured", "estimated"})
            text(e["source"], r + ".source")
            if "assumptions" in e:
                text(e["assumptions"], r + ".assumptions")
            if kind == "measured":
                if {"duration_seconds", "low_seconds", "high_seconds"} & e.keys():
                    raise InputError(r + ": measured evidence must use samples_seconds only")
                for n in items(e.get("samples_seconds"), r + ".samples_seconds"):
                    number(n, r + ".samples_seconds", positive=True)
            else:
                if "samples_seconds" in e:
                    raise InputError(r + ": estimated evidence must use duration_seconds")
                base = number(e.get("duration_seconds"), r + ".duration_seconds", positive=True)
                low = number(e.get("low_seconds", base), r + ".low_seconds", positive=True)
                high = number(e.get("high_seconds", base), r + ".high_seconds", positive=True)
                if not low <= base <= high:
                    raise InputError(
                        r + ": require low_seconds <= duration_seconds <= high_seconds"
                    )
                text(e.get("assumptions"), r + ".assumptions")
            expected = {
                "configuration_fingerprint": fingerprint(plans[pid]["configuration"]),
                "workload_fingerprint": fingerprint(workloads[wid]["definition"]),
            }
            for field, value in expected.items():
                if field not in e and allow_unbound:
                    continue
                if e.get(field) != value:
                    raise InputError(
                        r + f": {field} missing or mismatched; obtain evidence for "
                        "this configuration/workload, then bind it explicitly"
                    )
        if seen != workloads.keys():
            raise InputError(p + ": every scenario must include every workload exactly once")
    return d


def bind(document: dict, *, allow_pending: bool = False) -> dict:
    """Bind evidence without changing fingerprints; imports may leave missing evidence pending."""
    result = copy.deepcopy(document)
    validate(result, allow_unbound=True, allow_pending=allow_pending)
    plans = {p["id"]: p for p in result["plans"]}
    workloads = {w["id"]: w for w in result["workloads"]}
    for scenario in result["scenarios"]:
        for run in scenario["runs"]:
            e = run["evidence"]
            if e is None:
                continue
            e["configuration_fingerprint"] = fingerprint(plans[run["plan_id"]]["configuration"])
            e["workload_fingerprint"] = fingerprint(workloads[run["workload_id"]]["definition"])
    return validate(result, allow_pending=allow_pending)
