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
"""Portable reports with explicit scope, evidence, allocations and cash-flow accounting."""

from __future__ import annotations

import csv
import html
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from .model import InputError


def json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: json_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [json_value(val) for val in value]
    return value


def dumps(value: Any) -> str:
    return json.dumps(json_value(value), indent=2, ensure_ascii=False, allow_nan=False) + "\n"


def save_json(path: str | Path, value: Any, *, overwrite: bool = False) -> None:
    p = Path(path)
    if p.exists() and not overwrite:
        raise InputError(f"{p}: already exists; use --overwrite to replace it")
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w" if overwrite else "x", encoding="utf-8") as stream:
        stream.write(dumps(value))


def money(value: Any) -> str:
    return "N/A" if value is None else f"{value:,.2f}"


def summary(result: dict) -> str:
    lines = [
        result["name"],
        f"Currency: {result['currency']} | Horizon: {result['years']} years",
        "Runtime evidence: medians for measured runs; estimates remain labelled.",
    ]
    for scenario in result["scenarios"]:
        comp = scenario.get("comparison")
        status = (
            comp["evidence_status"]
            if comp
            else "baseline"
            if scenario["feasible"]
            else "infeasible baseline"
        )
        lines.append(
            f"\n{scenario['id']} [{status}]: annual cash {money(scenario['annual_cash_cost'])}"
            f"; total cash cost {money(scenario['cash_tco'])}"
        )
        if comp:
            payback = comp["sustained_payback_months"]
            roi_label = "N/A" if comp["roi_percent"] is None else f"{money(comp['roi_percent'])}%"
            payback_label = (
                "not recovered within horizon" if payback is None else f"{payback} months"
            )
            lines.append(
                f"  Annual cash savings: {money(comp['annual_cash_savings'])}; "
                f"net benefit: {money(comp['horizon_net_cash_benefit'])}; "
                f"ROI: {roi_label}"
            )
            lines.append(f"  Payback: {payback_label}; NPV: {money(comp['npv'])}")
            if not comp["eligible_for_cost_comparison"]:
                lines.append("  Comparison is unverified/infeasible; do not treat it as a saving.")
        for issue in scenario["issues"] + scenario["notes"]:
            lines.append("  - " + issue)
    if result["cost_exclusions"]:
        lines.append("\nExcluded costs: " + "; ".join(result["cost_exclusions"]))
    return "\n".join(lines) + "\n"


def _escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    return (
        '<div class="table-wrap"><table><thead><tr>'
        + "".join(f"<th>{_escape(h)}</th>" for h in headers)
        + "</tr></thead><tbody>"
        + "".join("<tr>" + "".join(f"<td>{_escape(c)}</td>" for c in row) + "</tr>" for row in rows)
        + "</tbody></table></div>"
    )


def html_report(result: dict) -> str:
    currency = _escape(result["currency"])
    maximum = max((s["cash_tco"] for s in result["scenarios"]), default=Decimal(1)) or Decimal(1)
    bars = []
    sections = []
    for scenario in result["scenarios"]:
        sid = scenario["id"]
        comparison = scenario.get("comparison", {})
        status = comparison.get("evidence_status", "baseline")
        if not scenario["feasible"]:
            status = "infeasible"
        width = float(scenario["cash_tco"] / maximum * 100)
        bars.append(
            f'<div class="bar-row"><span>{_escape(sid)}</span>'
            f'<div class="track"><div class="bar" style="width:{width:.3f}%"></div></div>'
            f"<strong>{money(scenario['cash_tco'])} {currency}</strong></div>"
        )
        metrics = [
            ["Recurring annual cash cost", money(scenario["annual_cash_cost"])],
            ["Annual cost including depreciation", money(scenario["annual_economic_cost"])],
            ["Upfront purchase / migration cost", money(scenario["upfront_cost"])],
            ["Cash TCO over horizon", money(scenario["cash_tco"])],
            ["Economic TCO (depreciation, no duplicate purchase)", money(scenario["economic_tco"])],
        ]
        if scenario["annual_unallocated_cash_cost"]:
            metrics.append(
                [
                    "Annual cash cost of retained idle plans",
                    money(scenario["annual_unallocated_cash_cost"]),
                ]
            )
        if comparison:
            metrics.extend(
                [
                    ["Annual cash savings vs baseline", money(comparison["annual_cash_savings"])],
                    [
                        "Net cash benefit over horizon",
                        money(comparison["horizon_net_cash_benefit"]),
                    ],
                    ["Return on incremental investment (%)", money(comparison["roi_percent"])],
                    ["NPV of cash savings", money(comparison["npv"])],
                    [
                        "Sustained payback (months)",
                        comparison["sustained_payback_months"]
                        if comparison["sustained_payback_months"] is not None
                        else "Not within horizon",
                    ],
                ]
            )
        issues = "".join(f"<li>{_escape(x)}</li>" for x in scenario["issues"] + scenario["notes"])
        workload_rows = [
            [
                r["workload_id"],
                r["plan_id"],
                r["evidence_kind"],
                money(r["runtime_seconds"]),
                money(r["runs_per_year"]),
                money(r["allocated_cost_per_success"]),
                money(r["annual_cash_cost"]),
                r["deadline_met"] if r["deadline_met"] is not None else "Not specified",
            ]
            for r in scenario["workloads"]
        ]
        breakdown = []
        for plan in scenario["plans"]:
            for line in plan["components"]:
                breakdown.append(
                    [
                        plan["plan_id"],
                        line["component"],
                        line.get("workload_id", "Shared / annual"),
                        money(line.get("quantity")),
                        money(line.get("billed_hours")),
                        money(line["annual_cost"]),
                    ]
                )
        source_rows = [
            [
                p["plan_id"],
                p["billing_mode"],
                p["price_as_of"],
                p["price_source"],
                money(p["capacity_utilization_percent"]),
            ]
            for p in scenario["plans"]
        ]
        sensitivity = ""
        if comparison:
            sensitivity = _table(
                ["Runtime sensitivity", "Net cash benefit", "Feasible / verified"],
                [
                    [
                        bound.title(),
                        money(scenario["sensitivity"][bound]["horizon_net_cash_benefit"]),
                        scenario["sensitivity"][bound]["eligible_for_cost_comparison"],
                    ]
                    for bound in ["conservative", "optimistic"]
                ],
            )
            sensitivity += "<p>" + _escape(scenario["sensitivity"]["meaning"]) + "</p>"
        sections.append(
            f"<section><h2>{_escape(sid)} <small>{_escape(status)}</small></h2>"
            + (f'<ul class="notices">{issues}</ul>' if issues else "")
            + _table(["Measure", result["currency"]], metrics)
            + sensitivity
            + "<details open><summary>Workloads and allocated costs</summary>"
            + _table(
                [
                    "Workload",
                    "Plan",
                    "Evidence",
                    "Seconds/run",
                    "Runs/year",
                    "Cost/success",
                    "Annual cash cost",
                    "Deadline met",
                ],
                workload_rows,
            )
            + "<p>Shared costs are allocated by capacity-hours. An allocation is not a promise "
            "that removing one job reduces your bill. Capacity checks do not prove that a "
            "concurrent schedule meets every deadline.</p></details>"
            + "<details><summary>Billing components and price sources</summary>"
            + _table(
                ["Plan", "Component", "Scope", "Quantity", "Billed hours", "Annual cost"], breakdown
            )
            + _table(["Plan", "Billing", "Price date", "Source", "Capacity used (%)"], source_rows)
            + "</details><details><summary>Cash outflows by year</summary>"
            + _table(
                ["Year (0 = upfront)", "Cash outflow"],
                [[i, money(v)] for i, v in enumerate(scenario["cash_outflows_by_year"])],
            )
            + "</details></section>"
        )
    return (
        """<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>"""
        + _escape(result["name"])
        + """ — Spark ROI</title>
<style>
:root{font-family:system-ui,sans-serif;color:#18283c;background:#f3f6fa;line-height:1.5}
body{max-width:1160px;margin:auto;padding:32px}h1{font-size:2rem;margin-bottom:8px}
section,header{background:white;border:1px solid #dae2ed;border-radius:12px;padding:24px;
margin-bottom:20px}small{font-size:.8rem;background:#e8eef7;padding:5px 9px;border-radius:5px}
.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse;margin:16px 0;font-size:.9rem}
th,td{text-align:left;padding:10px;border-bottom:1px solid #e2e8f0;vertical-align:top}
th{background:#f5f8fc}summary{cursor:pointer;font-weight:650;margin-top:16px}
.notices{background:#fff4d6;padding:16px 16px 16px 36px;border-radius:8px}
.bar-row{display:grid;grid-template-columns:150px 1fr 180px;gap:16px;align-items:center;
margin:14px 0}.track{background:#e9eef5;border-radius:4px;overflow:hidden}.bar{height:22px;
background:#3269bb}pre{overflow:auto;font-size:.8rem;white-space:pre-wrap;overflow-wrap:anywhere}
@media(max-width:650px){body{padding:12px}section,header{padding:16px}.bar-row{
grid-template-columns:1fr}.bar-row strong{font-size:.85rem}}
@media print{body{background:white;padding:0}details{break-inside:avoid}section{break-inside:auto}}
</style><header><p>SPARK ROI · OFFLINE COST ASSESSMENT</p><h1>"""
        + _escape(result["name"])
        + "</h1><p>"
        + str(result["years"])
        + " year(s) · "
        + currency
        + " · Baseline: "
        + _escape(result["baseline"])
        + "</p><p>Costs describe the supplied workload, configurations and evidence. "
        "Estimated performance remains an estimate. No savings are guaranteed.</p><h2>Cash TCO</h2>"
        + "".join(bars)
        + "</header>"
        + "".join(sections)
        + "<section><h2>Scope and assumptions</h2><ul>"
        + "".join("<li>" + _escape(x) + "</li>" for x in result["assumptions"])
        + "</ul><h3>Excluded costs</h3><ul>"
        + "".join("<li>" + _escape(x) + "</li>" for x in result["cost_exclusions"])
        + "</ul><details><summary>Reproducible input and evidence</summary><p>"
        + _escape(result["input_fingerprint"])
        + "</p><pre>"
        + _escape(dumps(result["input"]))
        + "</pre></details></section></html>\n"
    )


def _csv_cell(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    # Do not let untrusted names/sources become spreadsheet formulas.
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def _csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows({k: _csv_cell(v) for k, v in row.items()} for row in rows)


def write_reports(result: dict, directory: str | Path, *, overwrite: bool = False) -> Path:
    target = Path(directory)
    context = {key: result[key] for key in ("currency", "years", "input_fingerprint")}
    scenarios = [{**s, **s.get("comparison", {}), **context} for s in result["scenarios"]]
    workloads = [
        {**r, "scenario_id": s["id"], **context}
        for s in result["scenarios"]
        for r in s["workloads"]
    ]
    components = [
        {
            **line,
            **context,
            "scenario_id": s["id"],
            "plan_id": p["plan_id"],
            "price_source": p["price_source"],
            "price_as_of": p["price_as_of"],
        }
        for s in result["scenarios"]
        for p in s["plans"]
        for line in p["components"]
    ]
    # Keep column order explicit so existing spreadsheet imports stay reproducible.
    tables = {
        "scenarios.csv": (
            scenarios,
            "id currency years input_fingerprint annual_cash_cost annual_economic_cost "
            "annual_unallocated_cash_cost upfront_cost cash_tco economic_tco feasible "
            "contains_estimates annual_cash_savings horizon_net_cash_benefit roi_percent "
            "npv sustained_payback_months eligible_for_cost_comparison",
        ),
        "workloads.csv": (
            workloads,
            "scenario_id currency years input_fingerprint workload_id plan_id evidence_kind "
            "evidence_source configuration_fingerprint workload_fingerprint runtime_seconds "
            "sample_count runs_per_year attempts_per_success capacity_hours_per_year "
            "allocated_cost_per_success annual_cash_cost annual_economic_cost "
            "result_validation deadline_met speedup",
        ),
        "components.csv": (
            components,
            "scenario_id currency years input_fingerprint plan_id component workload_id "
            "quantity unit billed_hours unit_hourly_rate discount_percent annual_cost "
            "price_source price_as_of",
        ),
    }
    renderers = {"report.json": dumps, "report.html": html_report, "summary.txt": summary}
    if not overwrite:
        for name in [*renderers, *tables]:
            if (target / name).exists():
                raise InputError(f"{target / name}: already exists; use --overwrite")
    # Render before writing, so input/report errors cannot leave a partial set.
    rendered = {name: render(result) for name, render in renderers.items()}
    target.mkdir(parents=True, exist_ok=True)
    for name, content in rendered.items():
        (target / name).write_text(content, encoding="utf-8")
    for name, (rows, fields) in tables.items():
        _csv(target / name, rows, fields.split())
    return target.resolve()
