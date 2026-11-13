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

"""Emit per-attempt CI configuration without creating a cluster or changing manual scripts."""

import json
import os
import sys

from clusterutils import ClusterUtils


def main():
    env = os.environ
    cloud = env["DATABRICKS_CLOUD"]
    if cloud not in {"aws", "azure"}:
        raise ValueError("Unsupported Databricks cloud")
    template = ClusterUtils.generate_create_templ(
        env["DATABRICKS_PUBKEY"], env["CLUSTER_NAME"], env["DATABRICKS_RUNTIME"],
        int(env["IDLE_TIMEOUT"]), int(env["NUM_WORKERS"]), env["DATABRICKS_DRIVER"],
        env["DATABRICKS_WORKER"], cloud,
        env.get("INIT_SCRIPT_PATHS", ""), env.get("DB_ZONE") or "auto", printLoc=sys.stderr)
    # run-tests.py forwards EXTRA_ENVS, not the Jenkins agent's process environment.
    test_env = dict(item.split("=", 1)
                    for item in env.get("EXTRA_ENVS", "").replace(",", " ").split()
                    if "=" in item)
    modes = ["databricks"]
    if ((test_env.get("TEST_MODE") or "DEFAULT") in {"DEFAULT", "CI_PART1"}
            and test_env.get("WITH_DEFAULT_UPSTREAM_SHIM", "1") != "0"):
        modes.append("upstream")
    json.dump({"cluster_spec": template, "readiness_modes": modes}, sys.stdout)
    print()


if __name__ == "__main__":
    main()
