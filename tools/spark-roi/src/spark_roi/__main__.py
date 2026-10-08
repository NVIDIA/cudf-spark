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
"""Command-line interface for the independently installable Spark ROI package."""

from __future__ import annotations

import argparse
import csv
import sys
from decimal import DecimalException
from pathlib import Path

from . import __version__
from .adapters import eventlogs, measured_runs, qualification
from .engine import evaluate
from .model import InputError, bind, fingerprint, load_json, validate
from .report import dumps, save_json, summary, write_reports


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="spark-roi",
        description="Compare Spark CPU/GPU costs using explicit configurations, "
        "prices and runtime evidence. Runs offline without Spark, Java or CUDA.",
    )
    root.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = root.add_subparsers(dest="command", required=True)
    init = sub.add_parser(
        "init", help="Write a synthetic cloud, shared-cluster or owned-hardware example"
    )
    init.add_argument("--example", choices=["cloud", "shared", "owned"], default="cloud")
    init.add_argument("--output", required=True, type=Path)
    init.add_argument("--overwrite", action="store_true")
    for name, help_text in [
        ("validate", "Validate prices, evidence, configuration bindings and comparison coverage"),
        ("fingerprints", "Print configuration and workload fingerprints for measured-run imports"),
        ("bind", "Assert that unbound evidence applies to the supplied configuration and workload"),
        ("evaluate", "Calculate costs, cash ROI, payback, NPV and runtime sensitivity"),
    ]:
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("scenario", type=Path)
        if name == "bind":
            cmd.add_argument("--output", type=Path, required=True)
            cmd.add_argument("--overwrite", action="store_true")
        if name == "evaluate":
            cmd.add_argument(
                "--output", type=Path, help="Directory for HTML, JSON, CSV and text reports"
            )
            cmd.add_argument("--overwrite", action="store_true")
            cmd.add_argument("--format", choices=["text", "json"], default="text")
            cmd.add_argument(
                "--strict",
                action="store_true",
                help="Exit 3 if a base or conservative comparison is unverified/infeasible",
            )
    for name, help_text in [
        ("import-qualification", "Import per-application qualification CSV timing estimates"),
        (
            "import-runs",
            "Import measured timing CSV, with explicit configuration/workload fingerprints",
        ),
        ("import-eventlogs", "Import completed plain/gzip Spark event logs as measured wall times"),
    ]:
        cmd = sub.add_parser(name, help=help_text)
        cmd.add_argument("inputs", nargs="+" if name == "import-eventlogs" else None, type=Path)
        cmd.add_argument("--scenario", required=True, type=Path)
        cmd.add_argument("--output", required=True, type=Path)
        cmd.add_argument("--overwrite", action="store_true")
        if name == "import-qualification":
            cmd.add_argument("--candidate", required=True, help="Alternative scenario ID")
            cmd.add_argument("--duration-unit", choices=["ms", "s"], default="ms")
            cmd.add_argument("--cpu-column", default="App Duration")
            cmd.add_argument("--gpu-column", default="Estimated GPU Duration")
        if name == "import-eventlogs":
            cmd.add_argument("--scenario-id", required=True)
            cmd.add_argument(
                "--workload-id",
                required=True,
                help="All supplied logs must represent this same workload/dataset",
            )
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "init":
            save_json(args.output, example(args.example), overwrite=args.overwrite)
            print(
                f"Written {args.output}. Synthetic example: replace prices, workload and evidence."
            )
            return 0
        document = load_json(args.scenario)
        if args.command.startswith("import-"):
            validate(document, allow_unbound=True, allow_pending=True)
            if args.command == "import-qualification":
                changed = qualification(
                    document,
                    args.inputs,
                    args.candidate,
                    duration_unit=args.duration_unit,
                    cpu_column=args.cpu_column,
                    gpu_column=args.gpu_column,
                )
            elif args.command == "import-runs":
                changed = measured_runs(document, args.inputs)
            else:
                changed = eventlogs(document, args.inputs, args.scenario_id, args.workload_id)
            save_json(args.output, changed, overwrite=args.overwrite)
            pending = sum(r["evidence"] is None for s in changed["scenarios"] for r in s["runs"])
            print(f"Written {args.output}; {pending} run(s) still need evidence.")
            return 0
        if args.command == "bind":
            save_json(args.output, bind(document), overwrite=args.overwrite)
            print(
                f"Written {args.output}. Evidence is bound to these configurations and workloads."
            )
            return 0
        if args.command == "fingerprints":
            validate(document, allow_unbound=True, allow_pending=True)
            print(
                dumps(
                    {
                        "plans": {
                            p["id"]: fingerprint(p["configuration"]) for p in document["plans"]
                        },
                        "workloads": {
                            w["id"]: fingerprint(w["definition"]) for w in document["workloads"]
                        },
                    }
                ),
                end="",
            )
            return 0
        if args.command == "validate":
            validate(document)
            print(f"Valid: {document['name']}")
            return 0
        result = evaluate(document)
        print(dumps(result) if args.format == "json" else summary(result), end="")
        if args.output:
            destination = write_reports(result, args.output, overwrite=args.overwrite)
            print(
                f"Reports: {destination}", file=sys.stderr if args.format == "json" else sys.stdout
            )
        if args.strict and any(
            not s["comparison"]["eligible_for_cost_comparison"]
            or not s["sensitivity"]["conservative"]["eligible_for_cost_comparison"]
            for s in result["scenarios"]
            if "comparison" in s
        ):
            return 3
        return 0
    except (InputError, OSError, UnicodeError, csv.Error, DecimalException) as exc:
        print(f"spark-roi: {exc}", file=sys.stderr)
        return 2


def _plan(name: str, workers: int, gpu: bool, price: float) -> dict:
    nodes = [
        {
            "id": "workers",
            "instance_type": f"synthetic-{'gpu' if gpu else 'cpu'}",
            "count": workers,
            "vcpus": 16,
            "memory_gib": 64,
            "gpus": int(gpu),
        },
        {
            "id": "driver",
            "instance_type": "synthetic-driver",
            "count": 1,
            "vcpus": 4,
            "memory_gib": 16,
            "gpus": 0,
        },
    ]
    if gpu:
        nodes[0]["gpu_type"] = "synthetic-GPU"
    return {
        "id": name,
        "price_source": "Invented prices for the arithmetic example; replace them",
        "price_as_of": "2026-10-08",
        "configuration": {"nodes": nodes, "spark_version": "3.5.6", "spark_conf": {}},
        "billing": {
            "mode": "ephemeral",
            "rates": [
                {
                    "id": "workers",
                    "node_group": "workers",
                    "unit": "node",
                    "hourly_rate": price,
                    "minimum_seconds": 60,
                    "increment_seconds": 1,
                },
                {
                    "id": "driver",
                    "node_group": "driver",
                    "unit": "node",
                    "hourly_rate": 1,
                    "minimum_seconds": 60,
                    "increment_seconds": 1,
                },
                {"id": "platform", "unit": "vcpu", "node_group": "workers", "hourly_rate": 0.01},
            ],
            "startup_seconds": 60,
            "shutdown_seconds": 0,
        },
    }


def _run(workload: str, plan: str, seconds: int, estimated: bool = False) -> dict:
    evidence = {
        "kind": "estimated" if estimated else "measured",
        "source": "Synthetic fixture, not an actual Spark measurement",
    }
    if estimated:
        evidence.update(
            duration_seconds=seconds,
            low_seconds=seconds * 0.8,
            high_seconds=seconds * 1.3,
            assumptions="Invented runtime for this example; replace with a prediction "
            "for this exact configuration or measure it.",
        )
    else:
        evidence["samples_seconds"] = [seconds * 0.95, seconds, seconds * 1.05]
    return {
        "workload_id": workload,
        "plan_id": plan,
        "evidence": evidence,
        "attempts_per_success": 1,
        "extra_cost_per_attempt": 0,
    }


def example(name: str = "cloud") -> dict:
    plans = [_plan("cpu", 8, False, 1), _plan("gpu", 4, True, 3), _plan("gpu-large", 8, True, 3)]
    workloads = [
        {
            "id": "daily-etl",
            "app_id": "app-etl",
            "definition": "Synthetic daily ETL; replace with code revision and dataset snapshot",
            "frequency": {"runs_per_year": 365, "source": "Synthetic daily schedule"},
            "deadline_seconds": 4000,
            "work_units": 1,
            "unit": "TB",
            "result_validation": {
                "status": "passed",
                "source": "Synthetic arithmetic example only",
            },
        },
        {
            "id": "monthly-report",
            "app_id": "app-report",
            "definition": "Synthetic monthly report; replace with query and dataset identity",
            "frequency": {"runs_per_year": 12, "source": "Synthetic monthly schedule"},
            "deadline_seconds": 8000,
            "result_validation": {
                "status": "passed",
                "source": "Synthetic arithmetic example only",
            },
        },
    ]
    d = {
        "schema_version": 1,
        "name": f"SYNTHETIC {name} example — replace prices and evidence",
        "currency": "USD",
        "years": 3,
        "discount_rate_percent": 8,
        "baseline": "cpu",
        "plans": plans,
        "workloads": workloads,
        "cost_exclusions": [
            "Tax, financing and inflation",
            "Storage and network fees (add them if material)",
        ],
        "scenarios": [
            {
                "id": "cpu",
                "migration_cost": 0,
                "runs": [_run("daily-etl", "cpu", 3600), _run("monthly-report", "cpu", 7200)],
            },
            {
                "id": "gpu",
                "migration_cost": 500,
                "runs": [
                    _run("daily-etl", "gpu", 900, True),
                    _run("monthly-report", "gpu", 2400, True),
                ],
            },
            {
                "id": "gpu-large",
                "migration_cost": 500,
                "runs": [
                    _run("daily-etl", "gpu-large", 600, True),
                    _run("monthly-report", "gpu-large", 1500, True),
                ],
            },
        ],
    }
    if name == "shared":
        for p in plans:
            p["billing"].update(mode="persistent", uptime_hours_per_year=8760, startup_seconds=0)
        d["notes"] = "Each scenario owns one persistent cluster shared by both jobs. "
        d["notes"] += "Faster jobs do not reduce its annual rental bill."
    elif name == "owned":
        d["scenarios"] = d["scenarios"][:2]
        d["plans"] = plans[:2]
        for p in d["plans"]:
            gpu = p["id"] != "cpu"
            p["billing"] = {
                "mode": "owned",
                "uptime_hours_per_year": 8760,
                "rates": [],
                "ownership": {
                    "capital_cost": 40000 if gpu else 24000,
                    "residual_value": 4000 if gpu else 2400,
                    "life_years": 4,
                    "purchase_required": gpu,
                    "annual_maintenance": 2000 if gpu else 1200,
                    "annual_facility_cost": 1000,
                    "active_kw": 4 if gpu else 3,
                    "idle_kw": 1 if gpu else 1.5,
                    "electricity_per_kwh": 0.15,
                    "pue": 1.3,
                },
            }
            if not gpu:
                p["billing"]["ownership"]["remaining_life_years"] = 2
        d["notes"] = "Existing CPU purchase is sunk. GPU purchase is new. Replacement timing, "
        d["notes"] += "electricity, maintenance and depreciation are accounted for separately."
    elif name != "cloud":
        raise ValueError(f"unknown example {name!r}")
    return bind(d)


if __name__ == "__main__":
    raise SystemExit(main())
