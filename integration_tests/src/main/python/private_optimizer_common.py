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

"""Shared helpers for the RAPIDS private-optimizer integration tests.

This module is intentionally named without the ``_test`` suffix so pytest does
not collect it as a test file. Each private-optimizer rule lives in its own
``private_optimizer_<rule>_test.py`` module and imports from here.

The covered rules are semantics-preserving but default-off or otherwise dormant
in public IT.
Comparing a same-conf CPU run against a same-conf GPU run cannot tell whether a
rule actually fired, so every test built on these helpers does two things:

  1. Compares an OFF-rule CPU baseline against an ON-rule GPU run. Because the
     rules preserve semantics, OFF-CPU == ON-GPU proves both data correctness
     and GPU parity while keeping the CPU reference uncontaminated by the rule.
  2. Asserts a per-rule plan marker that appears only when the rule fires, so
     the test FAILS if the conf flip is a no-op (wrong conf, wrong query shape,
     or private jar not loaded).

See ``private_optimizer_README.md`` for how to add a new rule module.
"""

from asserts import assert_equal_with_local_sort
from spark_session import with_cpu_session, with_gpu_session

# The private optimizer rules ship in the spark-rapids-private plugin, which is
# built only for Spark 3.3.0 and later (see the private core pom build matrix:
# 330..411 plus the Databricks 400db173 buildver). Runtimes within the matrix
# (including Databricks) are all supported for these four rules, so we do not
# add per-runtime skips here. A rule that becomes unsupported on some future
# runtime is caught by the plan-marker assertion below (it fails loudly instead
# of passing silently), not by guessing a version here.

PRIVATE_OPTIMIZER_BASE_CONF = {
    "spark.rapids.sql.private.enabled": "true",
}


def private_optimizer_conf(*rule_confs, extra_conf=None):
    """Build a conf dict for a private-optimizer test.

    Starts from PRIVATE_OPTIMIZER_BASE_CONF (which enables the private plugin),
    merges each positional rule-conf dict on top in order, then applies the
    optional extra_conf (query-shaping confs such as broadcast thresholds).
    """
    conf = dict(PRIVATE_OPTIMIZER_BASE_CONF)
    for rule_conf in rule_confs:
        conf.update(rule_conf)
    if extra_conf:
        conf.update(extra_conf)
    return conf


def collect_and_plan(spark, fn, physical):
    """Run fn, returning (collected rows, plan-string). Uses the physical plan
    when the rule effect is only visible after AQE, else the optimized plan."""
    df = fn(spark)
    rows = df.collect()
    qe = df._jdf.queryExecution()
    plan = qe.executedPlan().toString() if physical else qe.optimizedPlan().toString()
    return rows, plan


def assert_rule_fires(fn, on_conf, off_conf, marker, physical=False):
    """OFF-rule CPU baseline vs ON-rule GPU run, plus a plan marker check.

    marker must be present in the ON plan and absent from the OFF plan; results
    of the OFF-CPU and ON-GPU runs must match.
    """
    cpu_rows, off_plan = with_cpu_session(
        lambda s: collect_and_plan(s, fn, physical), conf=off_conf)
    gpu_rows, on_plan = with_gpu_session(
        lambda s: collect_and_plan(s, fn, physical), conf=on_conf)

    assert marker in on_plan, \
        "rule did not fire: marker '%s' absent with rule ON\n%s" % (marker, on_plan)
    assert marker not in off_plan, \
        "marker '%s' present with rule OFF, not a valid discriminator\n%s" % (marker, off_plan)

    assert_equal_with_local_sort(cpu_rows, gpu_rows)
