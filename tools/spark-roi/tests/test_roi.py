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
"""Accounting, import and CLI regressions for the standalone ROI tool."""

import contextlib
import copy
import csv
import gzip
import io
import json
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path

from spark_roi.__main__ import example, main
from spark_roi.adapters import eventlogs, measured_runs, qualification
from spark_roi.engine import billed_seconds, evaluate
from spark_roi.model import InputError, annual_runs, bind, fingerprint, load_json, runtime, validate
from spark_roi.report import html_report, save_json, write_reports


def rebind(document):
    for scenario in document["scenarios"]:
        for run in scenario["runs"]:
            run["evidence"].pop("configuration_fingerprint", None)
            run["evidence"].pop("workload_fingerprint", None)
    return bind(document)


def simple():
    """CPU 4*25*10 = 1000/run; GPU 4*75*2 = 600/run; 1000 successful runs/year."""
    d = example()
    d["plans"] = d["plans"][:2]
    d["workloads"] = d["workloads"][:1]
    d["workloads"][0].pop("deadline_seconds")
    d["workloads"][0]["frequency"]["runs_per_year"] = 1000
    d["scenarios"] = d["scenarios"][:2]
    d["discount_rate_percent"] = 0
    for i, plan in enumerate(d["plans"]):
        plan["configuration"]["nodes"] = plan["configuration"]["nodes"][:1]
        plan["configuration"]["nodes"][0]["count"] = 4
        plan["billing"] = {
            "mode": "ephemeral",
            "rates": [
                {
                    "id": "compute",
                    "unit": "node",
                    "node_group": "workers",
                    "hourly_rate": 25 if i == 0 else 75,
                }
            ],
        }
        scenario = d["scenarios"][i]
        scenario["runs"] = scenario["runs"][:1]
        scenario["runs"][0]["evidence"] = {
            "kind": "measured",
            "source": "test hand calculation",
            "samples_seconds": [36000 if i == 0 else 7200],
        }
        scenario["migration_cost"] = 0 if i == 0 else 200000
    return rebind(d)


class AccountingTests(unittest.TestCase):
    def test_golden_cash_roi_payback_and_npv(self):
        r = evaluate(simple())
        cpu, gpu = r["scenarios"]
        self.assertEqual(cpu["annual_cash_cost"], D(1000000))
        self.assertEqual(gpu["annual_cash_cost"], D(600000))
        self.assertEqual(gpu["cash_tco"], D(2000000))
        c = gpu["comparison"]
        self.assertEqual(c["annual_cash_savings"], D(400000))
        self.assertEqual(c["horizon_net_cash_benefit"], D(1000000))
        self.assertEqual(c["roi_percent"], D(500))
        self.assertEqual(c["npv"], D(1000000))
        self.assertEqual(c["sustained_payback_months"], 6)

    def test_four_workers_are_charged_four_times(self):
        gpu = evaluate(simple())["scenarios"][1]
        line = gpu["plans"][0]["components"][0]
        self.assertEqual(line["quantity"], D(4))
        self.assertEqual(line["billed_hours"], D(2000))
        self.assertEqual(line["annual_cost"], D(600000))

    def test_published_aws_gpu_run_arithmetic(self):
        # AWS EMR/G7 study, 2026-08-25: quoted cluster $26.35/h, 281 seconds => $2.06.
        # This verifies published arithmetic, not a local GPU measurement or a current quote.
        d = simple()
        d["workloads"][0]["frequency"]["runs_per_year"] = 1
        d["plans"][1]["billing"]["rates"] = [
            {"id": "published-total", "unit": "cluster", "hourly_rate": "26.35"}
        ]
        d["scenarios"][1]["runs"][0]["evidence"]["samples_seconds"] = [281]
        cost = evaluate(d)["scenarios"][1]["annual_cash_cost"]
        self.assertEqual(cost.quantize(D("0.01")), D("2.06"))

    def test_price_change_does_not_invalidate_runtime_evidence(self):
        d = simple()
        d["plans"][1]["billing"]["rates"][0]["hourly_rate"] = 100
        self.assertEqual(evaluate(d)["scenarios"][1]["annual_cash_cost"], D(800000))

    def test_config_change_requires_new_evidence(self):
        d = simple()
        d["plans"][1]["configuration"]["nodes"][0]["count"] = 1
        with self.assertRaisesRegex(InputError, "configuration_fingerprint"):
            evaluate(d)
        with self.assertRaisesRegex(InputError, "configuration_fingerprint"):
            bind(d)

    def test_dataset_change_requires_new_evidence(self):
        d = simple()
        d["workloads"][0]["definition"] = "A different input dataset"
        with self.assertRaisesRegex(InputError, "workload_fingerprint"):
            evaluate(d)

    def test_no_annual_frequency_guess(self):
        w = {"frequency": {"observed_runs": 1, "observation_days": 365}}
        self.assertEqual(annual_runs(w), 1)
        w["frequency"]["observation_days"] = 10
        self.assertEqual(annual_runs(w), D("36.5"))

    def test_slow_gpu_and_no_payback_are_preserved(self):
        d = simple()
        d["scenarios"][1]["runs"][0]["evidence"]["samples_seconds"] = [72000]
        c = evaluate(d)["scenarios"][1]["comparison"]
        self.assertLess(c["annual_cash_savings"], 0)
        self.assertLess(c["roi_percent"], 0)
        self.assertFalse(c["lower_cash_tco"])
        self.assertIsNone(c["sustained_payback_months"])

    def test_zero_investment_roi_is_not_infinity(self):
        d = simple()
        d["scenarios"][1]["migration_cost"] = 0
        c = evaluate(d)["scenarios"][1]["comparison"]
        self.assertIsNone(c["roi_percent"])
        self.assertEqual(c["sustained_payback_months"], 0)

    def test_zero_cpu_cost_has_no_savings_percentage(self):
        d = simple()
        d["plans"][0]["billing"]["rates"][0]["hourly_rate"] = 0
        c = evaluate(d)["scenarios"][1]["comparison"]
        self.assertIsNone(c["cash_tco_savings_percent"])

    def test_billing_minimum_and_increment(self):
        rate = {"minimum_seconds": 60, "increment_seconds": 60}
        self.assertEqual(billed_seconds(D(1), rate), 60)
        self.assertEqual(billed_seconds(D(61), rate), 120)
        self.assertEqual(billed_seconds(D(120), rate), 120)

    def test_driver_platform_gpu_memory_discounts_startup_and_retries(self):
        d = simple()
        plan = d["plans"][1]
        plan["configuration"]["nodes"].append(
            {
                "id": "driver",
                "instance_type": "driver",
                "count": 1,
                "vcpus": 4,
                "memory_gib": 16,
                "gpus": 0,
            }
        )
        b = plan["billing"]
        b["startup_seconds"] = 3600
        b["rates"][0]["discount_percent"] = 50
        b["rates"] += [
            {"id": "driver", "unit": "node", "node_group": "driver", "hourly_rate": 2},
            {"id": "service", "unit": "cluster", "hourly_rate": 3},
            {"id": "gpu", "unit": "gpu", "node_group": "workers", "hourly_rate": 4},
            {"id": "cpu", "unit": "vcpu", "node_group": "workers", "hourly_rate": 1},
            {"id": "memory", "unit": "gib", "node_group": "driver", "hourly_rate": 1},
        ]
        run = d["scenarios"][1]["runs"][0]
        run["attempts_per_success"] = 2
        run["extra_cost_per_attempt"] = 5
        result = evaluate(rebind(d))["scenarios"][1]
        # Hourly: 150 + 2 + 3 + 16 + 64 + 16 = 251; 3h*2000 attempts + 5*2000.
        self.assertEqual(result["annual_cash_cost"], D(1516000))

    def test_persistent_cluster_keeps_charging_after_job_finishes(self):
        d = simple()
        d["workloads"][0]["frequency"]["runs_per_year"] = 100
        for plan in d["plans"]:
            plan["billing"].update(mode="persistent", uptime_hours_per_year=8760)
        r = evaluate(d)
        self.assertEqual(r["scenarios"][0]["annual_cash_cost"], D(876000))
        self.assertEqual(r["scenarios"][1]["annual_cash_cost"], D(2628000))
        self.assertLess(r["scenarios"][1]["comparison"]["annual_cash_savings"], 0)

    def test_shared_cluster_is_not_counted_twice(self):
        d = example("shared")
        r = evaluate(d)
        for s in r["scenarios"]:
            plan = s["plans"][0]
            self.assertAlmostEqual(
                sum(x["annual_cash_cost"] for x in s["workloads"]), plan["annual_cost"], places=16
            )
        d2 = copy.deepcopy(d)
        d2["workloads"] = d2["workloads"][:1]
        for s in d2["scenarios"]:
            s["runs"] = s["runs"][:1]
        self.assertEqual(
            evaluate(d2)["scenarios"][1]["annual_cash_cost"], r["scenarios"][1]["annual_cash_cost"]
        )

    def test_capacity_failure_is_not_recommended(self):
        d = example("shared")
        d["plans"][1]["billing"]["uptime_hours_per_year"] = 1
        r = evaluate(d)["scenarios"][1]
        self.assertFalse(r["feasible"])
        self.assertFalse(r["comparison"]["eligible_for_cost_comparison"])

    def test_retained_idle_cpu_cluster_is_still_charged(self):
        d = example("shared")
        before = evaluate(d)
        d["scenarios"][1]["retained_plans"] = ["cpu", "gpu"]
        gpu = evaluate(d)["scenarios"][1]
        self.assertEqual(
            gpu["annual_cash_cost"],
            before["scenarios"][0]["annual_cash_cost"] + before["scenarios"][1]["annual_cash_cost"],
        )
        self.assertEqual(
            gpu["annual_unallocated_cash_cost"], before["scenarios"][0]["annual_cash_cost"]
        )
        self.assertEqual(len(gpu["plans"]), 2)

    def test_result_validation_and_deadline_gate_comparison(self):
        for status in ["not_checked", "failed"]:
            d = simple()
            d["workloads"][0]["result_validation"]["status"] = status
            self.assertFalse(evaluate(d)["scenarios"][1]["comparison"]["lower_cash_tco"])
        d = simple()
        d["workloads"][0]["deadline_seconds"] = 1
        self.assertFalse(evaluate(d)["scenarios"][1]["comparison"]["eligible_for_cost_comparison"])

    def test_median_and_runtime_bounds(self):
        d = simple()
        r = d["scenarios"][1]["runs"][0]
        r["evidence"]["samples_seconds"] = [1, 3, 4, 100]
        self.assertEqual(runtime(r), D("3.5"))
        self.assertEqual(runtime(r, "low"), 1)
        self.assertEqual(runtime(r, "high"), 100)

    def test_estimates_are_labelled_and_sensitivity_is_ordered(self):
        r = evaluate(example())["scenarios"][1]
        self.assertEqual(r["comparison"]["evidence_status"], "estimated")
        self.assertLess(
            r["sensitivity"]["conservative"]["horizon_net_cash_benefit"],
            r["comparison"]["horizon_net_cash_benefit"],
        )
        self.assertGreater(
            r["sensitivity"]["optimistic"]["horizon_net_cash_benefit"],
            r["comparison"]["horizon_net_cash_benefit"],
        )

    def test_hardware_purchase_depreciation_replacements_and_sustained_payback(self):
        d = simple()
        d["scenarios"][1]["migration_cost"] = 0
        d["workloads"][0]["frequency"]["runs_per_year"] = 1
        for i, p in enumerate(d["plans"]):
            p["billing"] = {
                "mode": "owned",
                "rates": [],
                "uptime_hours_per_year": 8760,
                "ownership": {
                    "capital_cost": 1000 if i else 0,
                    "residual_value": 0,
                    "life_years": 2,
                    "purchase_required": bool(i),
                    "annual_maintenance": 100 if i else 1000,
                    "annual_facility_cost": 0,
                    "active_kw": 0,
                    "idle_kw": 0,
                    "electricity_per_kwh": 0,
                    "pue": 1,
                },
            }
            if not i:
                p["billing"]["ownership"]["remaining_life_years"] = 2
        gpu = evaluate(d)["scenarios"][1]
        self.assertEqual(gpu["capital_cash_flows"], [D(1000), D(0), D(1000), D(0)])
        self.assertEqual(gpu["cash_tco"], 2300)
        self.assertEqual(gpu["economic_tco"], 1800)
        self.assertEqual(gpu["comparison"]["horizon_net_cash_benefit"], 700)
        self.assertEqual(gpu["comparison"]["roi_percent"], 35)
        self.assertEqual(gpu["comparison"]["sustained_payback_months"], 27)

    def test_owned_electricity_includes_idle_and_pue(self):
        d = example("owned")
        gpu = evaluate(d)["scenarios"][1]
        busy = D(365) / 4 + D(12) * 2 / 3
        expected = (busy * 4 + (8760 - busy) * 1) * D("1.3") * D("0.15")
        line = next(c for c in gpu["plans"][0]["components"] if c["component"] == "electricity")
        self.assertEqual(line["annual_cost"], expected)

    def test_decimal_arithmetic_and_fingerprint_are_deterministic(self):
        d = simple()
        before = fingerprint(d)
        first, second = evaluate(d), evaluate(d)
        self.assertEqual(first, second)
        self.assertEqual(before, fingerprint(d))


class ValidationTests(unittest.TestCase):
    def test_invalid_inputs_fail_before_costing(self):
        cases = [
            (lambda d: d.update(currency="usd"), "currency"),
            (lambda d: d.update(years=True), "years"),
            (lambda d: d.update(extra="unknown"), "unknown"),
            (lambda d: d["plans"][0]["billing"]["rates"][0].update(hourly_rate=-1), "hourly_rate"),
            (lambda d: d["plans"][0]["billing"]["rates"][0].update(hourly_rate="NaN"), "finite"),
            (lambda d: d["plans"][0]["billing"]["rates"][0].update(hourly_rate="1e999"), "between"),
            (lambda d: d["plans"][0]["billing"]["rates"][0].update(discount_percent=101), "100"),
            (lambda d: d["plans"][0]["billing"]["rates"][0].update(node_group=[]), "node_group"),
            (lambda d: d["plans"][0]["billing"]["rates"][0].update(unit="gpu"), "without GPUs"),
            (lambda d: d["plans"][0].update(price_as_of="today"), "YYYY-MM-DD"),
            (lambda d: d["plans"][0]["configuration"]["nodes"][0].update(count=0), "count"),
            (lambda d: d["workloads"][0]["frequency"].pop("runs_per_year"), "observed_runs"),
            (lambda d: d["scenarios"][1]["runs"][0].update(plan_id="missing"), "plan_id"),
            (lambda d: d["scenarios"][1]["runs"][0].update(allocation_fraction=0.5), "ephemeral"),
            (lambda d: d["scenarios"][1]["runs"][0]["evidence"].update(samples_seconds=[0]), "> 0"),
            (
                lambda d: d["scenarios"][1]["runs"].append(
                    copy.deepcopy(d["scenarios"][1]["runs"][0])
                ),
                "once per scenario",
            ),
            (lambda d: d["plans"].append(copy.deepcopy(d["plans"][0])), "duplicate"),
            (lambda d: d["scenarios"][1].update(runs=[]), "nonempty"),
        ]
        for mutate, error in cases:
            with self.subTest(error=error):
                d = simple()
                mutate(d)
                with self.assertRaisesRegex(InputError, error):
                    validate(d)

    def test_new_asset_and_existing_asset_contracts(self):
        d = example("owned")
        d["plans"][0]["billing"]["ownership"].pop("remaining_life_years")
        with self.assertRaisesRegex(InputError, "remaining_life"):
            validate(d)
        d = example("owned")
        d["plans"][1]["billing"]["ownership"]["residual_value"] = 1000000
        with self.assertRaisesRegex(InputError, "residual"):
            validate(d)

    def test_no_silent_bad_estimation_range(self):
        d = example()
        d["scenarios"][1]["runs"][0]["evidence"]["low_seconds"] = 100000
        with self.assertRaisesRegex(InputError, "low_seconds"):
            validate(d)


class IOTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def csv(self, name, rows):
        path = self.root / name
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return path

    def cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(list(args))
        return code, out.getvalue(), err.getvalue()

    def test_qualification_unit_conversion_and_no_old_frequency_import(self):
        d = simple()
        csv_path = self.csv(
            "qual.csv",
            [
                {
                    "App ID": "app-etl",
                    "App Duration": "36000000",
                    "Estimated GPU Duration": "7200000",
                    "Estimated Job Frequency (monthly)": "30",
                }
            ],
        )
        r = qualification(d, csv_path, "gpu")
        self.assertEqual(r["scenarios"][0]["runs"][0]["evidence"]["samples_seconds"], ["36000"])
        self.assertEqual(r["scenarios"][1]["runs"][0]["evidence"]["duration_seconds"], "7200")
        self.assertEqual(r["workloads"][0]["frequency"]["runs_per_year"], 1000)
        self.assertEqual(evaluate(r)["scenarios"][1]["annual_cash_cost"], 600000)
        self.assertEqual(r["scenarios"][1]["runs"][0]["evidence"]["kind"], "estimated")

    def test_qualification_rejects_duplicate_unmapped_missing_and_invalid_times(self):
        valid = {"App ID": "app-etl", "App Duration": "1000", "Estimated GPU Duration": "500"}
        cases = [
            [valid, valid],
            [{**valid, "App ID": "unknown"}],
            [{**valid, "App Duration": "0"}],
            [{**valid, "Estimated GPU Duration": "NaN"}],
        ]
        for rows in cases:
            with self.subTest(rows=rows):
                with self.assertRaises(InputError):
                    qualification(simple(), self.csv("q.csv", rows), "gpu")
        with self.assertRaises(InputError):
            qualification(example(), self.csv("q.csv", [valid]), "gpu")
        with self.assertRaises(InputError):
            qualification(simple(), self.csv("q.csv", [valid]), "cpu")

    def test_qualification_custom_columns_seconds_and_bom(self):
        p = self.root / "q.csv"
        p.write_text("\ufeffApp ID,cpu_s,gpu_s\napp-etl,36000,7200\n", encoding="utf-8")
        r = qualification(
            simple(), p, "gpu", duration_unit="s", cpu_column="cpu_s", gpu_column="gpu_s"
        )
        self.assertEqual(evaluate(r)["scenarios"][1]["annual_cash_cost"], 600000)

    def test_measured_csv_repetitions_and_bindings(self):
        d = simple()
        row = {
            "scenario_id": "gpu",
            "workload_id": "daily-etl",
            "plan_id": "gpu",
            "configuration_fingerprint": fingerprint(d["plans"][1]["configuration"]),
            "workload_fingerprint": fingerprint(d["workloads"][0]["definition"]),
            "duration_seconds": "7000",
            "source": "benchmark-1",
        }
        p = self.csv(
            "runs.csv", [row, {**row, "duration_seconds": "7400", "source": "benchmark-2"}]
        )
        result = measured_runs(d, p)
        self.assertEqual(evaluate(result)["scenarios"][1]["annual_cash_cost"], 600000)
        for changed in [
            {"configuration_fingerprint": "bad"},
            {"workload_fingerprint": "bad"},
            {"plan_id": "cpu"},
            {"source": ""},
        ]:
            p = self.csv("bad.csv", [{**row, **changed}])
            with self.subTest(changed=changed), self.assertRaises(InputError):
                measured_runs(d, p)

    def events(self, name="eventlog", *, end=10000, version="3.5.6", conf=None, app="app-test"):
        events = [
            {"Event": "SparkListenerLogStart", "Spark Version": version},
            {"Event": "SparkListenerApplicationStart", "Timestamp": 1000, "App ID": app},
            {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": conf or {}},
            {
                "Event": "SparkListenerTaskEnd",
                "Task Info": {"Launch Time": 2000, "Finish Time": 9000},
            },
            {
                "Event": "SparkListenerTaskEnd",
                "Task Info": {"Launch Time": 2000, "Finish Time": 9000},
            },
        ]
        if end is not None:
            events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": end})
        path = self.root / name
        data = "".join(json.dumps(e) + "\n" for e in events)
        if name.endswith(".gz"):
            with gzip.open(path, "wt", encoding="utf-8") as stream:
                stream.write(data)
        else:
            path.write_text(data, encoding="utf-8")
        return path

    def test_eventlogs_use_wall_time_not_sum_of_parallel_tasks(self):
        d = eventlogs(simple(), [self.events()], "cpu", "daily-etl")
        self.assertEqual(d["scenarios"][0]["runs"][0]["evidence"]["samples_seconds"], ["9"])

    def test_eventlogs_gzip_and_multiple_samples(self):
        logs = [self.events("a", app="a"), self.events("b.gz", app="b", end=12000)]
        d = eventlogs(simple(), logs, "cpu", "daily-etl")
        self.assertEqual(d["scenarios"][0]["runs"][0]["evidence"]["samples_seconds"], ["9", "11"])

    def test_eventlogs_reject_incomplete_duplicate_config_and_version_mismatch(self):
        for kwargs in [{"end": None}, {"end": 1000}, {"version": "4.0.0"}]:
            with self.subTest(kwargs=kwargs), self.assertRaises(InputError):
                eventlogs(simple(), [self.events(**kwargs)], "cpu", "daily-etl")
        path = self.events()
        with self.assertRaisesRegex(InputError, "duplicate application"):
            eventlogs(simple(), [path, path], "cpu", "daily-etl")
        d = simple()
        d["plans"][0]["configuration"]["spark_conf"] = {"spark.executor.cores": "8"}
        d = rebind(d)
        with self.assertRaisesRegex(InputError, "Spark property"):
            eventlogs(d, [path], "cpu", "daily-etl")

    def test_partial_manifest_import_does_not_invent_missing_evidence(self):
        d = simple()
        for s in d["scenarios"]:
            s["runs"][0]["evidence"] = None
        d = eventlogs(d, [self.events()], "cpu", "daily-etl")
        self.assertIsNone(d["scenarios"][1]["runs"][0]["evidence"])
        with self.assertRaises(InputError):
            evaluate(d)

    def test_html_escapes_all_untrusted_strings_and_csv_blocks_formulas(self):
        d = simple()
        d["name"] = '<script>alert("x")</script>'
        d["plans"][1]["price_source"] = '=HYPERLINK("https://example.com")'
        d["scenarios"][1]["id"] = "=1+1"
        result = evaluate(d)
        h = html_report(result)
        self.assertNotIn("<script>", h)
        self.assertIn("&lt;script&gt;", h)
        target = write_reports(result, self.root / "reports")
        self.assertIn("'=1+1", (target / "scenarios.csv").read_text(encoding="utf-8"))
        saved = json.loads((target / "report.json").read_text(encoding="utf-8"))
        self.assertEqual(saved["input"], d)
        self.assertEqual(saved["scenarios"][1]["annual_cash_cost"], "600000")
        self.assertEqual(len(list(target.iterdir())), 6)
        with self.assertRaisesRegex(InputError, "already exists"):
            write_reports(result, target)

    def test_unicode_input_and_price_sources_survive_all_exports(self):
        d = simple()
        d["name"] = "CPU 与 GPU 每日任务成本"
        source = "自有服务器采购报价，含安装"
        d["plans"][1]["price_source"] = source
        path = self.root / "任务.json"
        save_json(path, d)
        loaded = load_json(path)
        self.assertEqual(loaded, d)
        target = write_reports(evaluate(loaded), self.root / "报告")
        self.assertIn(d["name"], (target / "report.html").read_text(encoding="utf-8"))
        self.assertIn(d["name"], (target / "summary.txt").read_text(encoding="utf-8"))
        with (target / "components.csv").open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream))
        gpu_rows = [row for row in rows if row["scenario_id"] == "gpu"]
        self.assertTrue(gpu_rows)
        self.assertTrue(all(row["price_source"] == source for row in gpu_rows))
        self.assertEqual(gpu_rows[0]["currency"], d["currency"])
        self.assertEqual(gpu_rows[0]["years"], str(d["years"]))

    def test_bad_json_duplicate_keys_and_nonfinite_values(self):
        p = self.root / "bad.json"
        for value in ['{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}', "{broken"]:
            with self.subTest(value=value):
                p.write_text(value, encoding="utf-8")
                with self.assertRaises(InputError):
                    load_json(p)

    def test_cli_full_workflow_and_no_overwrite(self):
        manifest = self.root / "cloud.json"
        code, _, _ = self.cli("init", "--output", str(manifest))
        self.assertEqual(code, 0)
        self.assertEqual(self.cli("init", "--output", str(manifest))[0], 2)
        self.assertEqual(self.cli("validate", str(manifest))[0], 0)
        code, fingerprints, _ = self.cli("fingerprints", str(manifest))
        self.assertEqual(code, 0)
        self.assertIn("gpu", json.loads(fingerprints)["plans"])
        output = self.root / "report"
        code, stdout, stderr = self.cli(
            "evaluate", str(manifest), "--format", "json", "--output", str(output)
        )
        self.assertEqual(code, 0, stderr)
        self.assertEqual(json.loads(stdout)["currency"], "USD")
        self.assertTrue((output / "report.html").exists())
        self.assertEqual(self.cli("evaluate", str(manifest), "--output", str(output))[0], 2)

    def test_cli_strict_status_and_clean_errors(self):
        d = simple()
        d["workloads"][0]["result_validation"]["status"] = "not_checked"
        p = self.root / "input.json"
        save_json(p, d)
        self.assertEqual(self.cli("evaluate", str(p), "--strict")[0], 3)
        d["plans"][1]["configuration"]["nodes"][0]["count"] = 20
        save_json(p, d, overwrite=True)
        code, _, err = self.cli("evaluate", str(p))
        self.assertEqual(code, 2)
        self.assertIn("configuration_fingerprint", err)
        self.assertNotIn("Traceback", err)

    def test_cli_bind_and_import_commands(self):
        d = simple()
        for s in d["scenarios"]:
            s["runs"][0]["evidence"].pop("configuration_fingerprint")
            s["runs"][0]["evidence"].pop("workload_fingerprint")
        p, bound = self.root / "unbound.json", self.root / "bound.json"
        save_json(p, d)
        self.assertEqual(self.cli("bind", str(p), "--output", str(bound))[0], 0)
        validate(load_json(bound))
        output = self.root / "imported.json"
        code, _, err = self.cli(
            "import-eventlogs",
            str(self.events()),
            "--scenario",
            str(bound),
            "--scenario-id",
            "cpu",
            "--workload-id",
            "daily-etl",
            "--output",
            str(output),
        )
        self.assertEqual(code, 0, err)
        q = self.csv(
            "qual.csv",
            [{"App ID": "app-etl", "App Duration": "1000", "Estimated GPU Duration": "500"}],
        )
        code, _, err = self.cli(
            "import-qualification",
            str(q),
            "--scenario",
            str(bound),
            "--candidate",
            "gpu",
            "--output",
            str(output),
            "--overwrite",
        )
        self.assertEqual(code, 0, err)


if __name__ == "__main__":
    unittest.main()
