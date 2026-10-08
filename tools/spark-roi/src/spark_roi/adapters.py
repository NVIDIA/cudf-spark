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
"""Explicit adapters for qualification CSV, measured runs, and Spark JSON event logs."""

from __future__ import annotations

import copy
import csv
import gzip
import hashlib
import json
from pathlib import Path

from .model import InputError, bind, number, validate


def _source(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(chunk)
    return f"{path.name} sha256:{checksum.hexdigest()}"


def _rows(path: Path, required: set[str]) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if (
            reader.fieldnames is None
            or len(set(reader.fieldnames)) != len(reader.fieldnames)
            or not required.issubset(reader.fieldnames)
        ):
            raise InputError(f"{path}: require unique CSV columns: {', '.join(sorted(required))}")
        result = list(reader)
        if not result or any(None in row or any(v is None for v in row.values()) for row in result):
            raise InputError(f"{path}: empty CSV or malformed row")
        return result


def _scenario(document: dict, scenario_id: str) -> dict:
    for scenario in document["scenarios"]:
        if scenario["id"] == scenario_id:
            return scenario
    raise InputError(f"unknown scenario {scenario_id!r}")


def qualification(
    document: dict,
    path: Path,
    candidate: str,
    *,
    duration_unit: str = "ms",
    cpu_column: str = "App Duration",
    gpu_column: str = "Estimated GPU Duration",
) -> dict:
    """Import timings only. Frequency, prices and hardware must come from the user's manifest."""
    d = copy.deepcopy(document)
    if candidate == d["baseline"]:
        raise InputError("qualification candidate must differ from baseline")
    cpu = _scenario(d, d["baseline"])
    gpu = _scenario(d, candidate)
    # Fresh evidence may replace stale evidence, but it must target explicit workload/plan IDs.
    for s in [cpu, gpu]:
        for run in s["runs"]:
            run["evidence"] = None
    validate(d, allow_pending=True, allow_unbound=True)
    cpu_runs = {run["workload_id"]: run for run in cpu["runs"]}
    gpu_runs = {run["workload_id"]: run for run in gpu["runs"]}
    mapping = {}
    for workload in d["workloads"]:
        app_id = workload.get("app_id", workload["id"])
        if app_id in mapping:
            raise InputError("qualification: workload app_id values must be unique")
        mapping[app_id] = workload["id"]
    records = _rows(path, {"App ID", cpu_column, gpu_column})
    seen = set()
    scale = number(1000 if duration_unit == "ms" else 1, "duration unit")
    source = _source(path)
    for row in records:
        app_id = row["App ID"]
        if app_id not in mapping or app_id in seen:
            raise InputError(
                f"qualification: unmapped or duplicate App ID {app_id!r}; "
                "use ungrouped per-application output and explicit app_id mappings"
            )
        seen.add(app_id)
        wid = mapping[app_id]
        cpu_run = cpu_runs[wid]
        gpu_run = gpu_runs[wid]
        cpu_run["evidence"] = {
            "kind": "measured",
            "source": source + " App ID=" + app_id,
            "samples_seconds": [str(number(row[cpu_column], cpu_column, positive=True) / scale)],
        }
        gpu_run["evidence"] = {
            "kind": "estimated",
            "source": source + " App ID=" + app_id,
            "duration_seconds": str(number(row[gpu_column], gpu_column, positive=True) / scale),
            "assumptions": "User maps this estimate to the supplied GPU configuration. "
            "Verify that it matches the qualification recommendation. No automatic "
            "resizing or extrapolation to different GPU hardware is performed.",
        }
    if seen != mapping.keys():
        raise InputError("qualification: CSV must contain each mapped application exactly once")
    return bind(d, allow_pending=True)


def measured_runs(document: dict, path: Path) -> dict:
    """Import one or more timing samples; exact fingerprints are required in every CSV row."""
    d = copy.deepcopy(document)
    required = {
        "scenario_id",
        "workload_id",
        "plan_id",
        "configuration_fingerprint",
        "workload_fingerprint",
        "duration_seconds",
        "source",
    }
    grouped = {}
    for row in _rows(path, required):
        key = (row["scenario_id"], row["workload_id"])
        number(row["duration_seconds"], "duration_seconds", positive=True)
        if not row["source"].strip():
            raise InputError("measured runs: every sample needs a source")
        grouped.setdefault(key, []).append(row)
    source = _source(path)
    scenario_runs = {
        s["id"]: {run["workload_id"]: run for run in s["runs"]} for s in d["scenarios"]
    }
    for (sid, wid), rows in grouped.items():
        if sid not in scenario_runs:
            raise InputError(f"unknown scenario {sid!r}")
        run = scenario_runs[sid].get(wid)
        if run is None:
            raise InputError(f"measured runs: unknown workload {wid!r} in {sid!r}")
        for row in rows:
            if row["plan_id"] != run["plan_id"]:
                raise InputError(f"measured runs: {wid}: plan_id does not match the manifest")
            for field in ["configuration_fingerprint", "workload_fingerprint"]:
                if row[field] != rows[0][field]:
                    raise InputError(f"measured runs: {wid}: mixed {field} values")
        run["evidence"] = {
            "kind": "measured",
            "source": source + "; " + "; ".join(sorted({r["source"] for r in rows})),
            "samples_seconds": [r["duration_seconds"] for r in rows],
            "configuration_fingerprint": rows[0]["configuration_fingerprint"],
            "workload_fingerprint": rows[0]["workload_fingerprint"],
        }
    return bind(d, allow_pending=True)


def eventlogs(document: dict, paths: list[Path], scenario_id: str, workload_id: str) -> dict:
    """Stream completed, uncompressed/gzip Spark event logs; never sum overlapping task times."""
    d = copy.deepcopy(document)
    scenario = _scenario(d, scenario_id)
    run = next((r for r in scenario["runs"] if r["workload_id"] == workload_id), None)
    if run is None:
        raise InputError(f"unknown workload {workload_id!r}")
    plan = next(p for p in d["plans"] if p["id"] == run["plan_id"])
    samples, sources, seen_ids = [], [], set()
    for path in paths:
        opener = gzip.open if path.suffix == ".gz" else open
        start = end = app_id = spark_version = None
        properties = None
        with opener(path, "rt", encoding="utf-8") as stream:
            for lineno, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, UnicodeError) as exc:
                    raise InputError(
                        f"{path}:{lineno}: expected JSON event log (plain or gzip)"
                    ) from exc
                if not isinstance(record, dict):
                    raise InputError(f"{path}:{lineno}: expected event object")
                event = record.get("Event")
                if event == "SparkListenerApplicationStart":
                    if start is not None:
                        raise InputError(f"{path}: multiple application starts")
                    start = number(record.get("Timestamp"), "application start")
                    app_id = record.get("App ID")
                elif event == "SparkListenerApplicationEnd":
                    if end is not None:
                        raise InputError(f"{path}: multiple application ends")
                    end = number(record.get("Timestamp"), "application end")
                elif event == "SparkListenerLogStart":
                    spark_version = record.get("Spark Version")
                elif event == "SparkListenerEnvironmentUpdate":
                    raw = record.get("Spark Properties", {})
                    try:
                        properties = dict(raw)
                    except (TypeError, ValueError) as exc:
                        raise InputError(f"{path}: invalid Spark Properties") from exc
        if start is None or end is None or end <= start:
            raise InputError(f"{path}: require one completed application with positive wall time")
        if not isinstance(app_id, str) or not app_id:
            raise InputError(f"{path}: missing application ID")
        if app_id in seen_ids:
            raise InputError(f"{path}: duplicate application ID {app_id!r}")
        seen_ids.add(app_id)
        if spark_version is not None and spark_version != plan["configuration"]["spark_version"]:
            raise InputError(f"{path}: Spark version differs from the bound configuration")
        for key, value in plan["configuration"]["spark_conf"].items():
            if properties is None or properties.get(key) != value:
                raise InputError(f"{path}: Spark property {key!r} differs or is unavailable")
        samples.append(str((end - start) / 1000))
        sources.append(_source(path) + " App ID=" + app_id)
    if not samples:
        raise InputError("eventlogs: provide at least one completed event log")
    run["evidence"] = {
        "kind": "measured",
        "source": "; ".join(sources),
        "samples_seconds": samples,
        "assumptions": "Application wall time from supplied logs; node inventory and dataset "
        "identity are asserted by the supplied manifest, not inferred from logs.",
    }
    return bind(d, allow_pending=True)
