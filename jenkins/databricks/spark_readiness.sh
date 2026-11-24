#!/bin/bash
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

# CI-only: fallback.groovy installs this probe; manual build/test commands do not require it.
# Run before product tests, with no plugin JARs, to separate host readiness from product failures.
set -euo pipefail
# Do not echo inherited JVM options, which may contain credentials.
set +x

case "${1:-}" in
    databricks) master='local[2]' ;;
    upstream) master='local-cluster[1,2,1024]' ;;
    *) exit 1 ;;
esac
directory=/home/ubuntu/.databricks-fallback
status_file="$directory/spark-readiness-$1.json"
log_file="$directory/spark-readiness-$1.log"

diagnostic() {
    printf '\nReadiness diagnostic: %s\n' "$*"
    timeout --kill-after=2s 10s "$@" || printf 'Diagnostic unavailable: %s\n' "$1"
}

# Isolate shell errors and state changes from the actual readiness check.
print_host_diagnostics() (
    printf '\nSpark readiness: pre-test host diagnostics\n'
    diagnostic date -u '+UTC: %Y-%m-%dT%H:%M:%SZ'
    diagnostic uname -a
    diagnostic cat /etc/os-release
    diagnostic hostname -I
    diagnostic ip -brief address
    diagnostic lscpu
    diagnostic uptime
    printf 'SPARK_HOME=%s JAVA_HOME=%s DATABRICKS_RUNTIME_VERSION=%s\n' \
        "${SPARK_HOME:-unset}" "${JAVA_HOME:-unset}" "${DATABRICKS_RUNTIME_VERSION:-unset}"
    if [[ -n "${SPARK_HOME:-}" && -r "$SPARK_HOME/VERSION" ]]; then
        diagnostic head -n 1 "$SPARK_HOME/VERSION"
    fi
    diagnostic free -m
    diagnostic vmstat 1 3
    # Names and resource usage only; process arguments can contain credentials.
    diagnostic ps -eo pid,ppid,comm,pcpu,pmem,rss,vsz --sort=-rss | head -n 18
    diagnostic df -hP /tmp "$directory" "${SPARK_HOME:-/}"
    diagnostic df -iP /tmp "$directory" "${SPARK_HOME:-/}"
    ulimit -a
    # Show pressure and standard cgroup counters without dumping process environment/config.
    for counter in /proc/self/cgroup /proc/pressure/{cpu,memory,io} \
        /sys/fs/cgroup/cpu.{max,stat} /sys/fs/cgroup/memory.{max,current,peak,events} \
        /sys/fs/cgroup/memory/memory.{limit_in_bytes,usage_in_bytes,max_usage_in_bytes,failcnt}; do
        if [[ -r "$counter" ]]; then
            printf '%s:\n' "$counter"
            diagnostic cat "$counter"
        fi
    done
    # Root counters alone miss child-cgroup limits. Match memberships to visible
    # controller mounts, accounting for mounts rooted at a cgroup subtree.
    diagnostic python3 - <<'PY'
from pathlib import Path, PurePosixPath
import re

try:
    memberships = Path("/proc/self/cgroup").read_text().splitlines()
    mounts = Path("/proc/self/mountinfo").read_text().splitlines()
except OSError as error:
    print("Current cgroup counters unavailable (%s)" % type(error).__name__, flush=True)
    raise SystemExit(0)

groups = {}
for membership in memberships:
    _, controllers, group = membership.split(":", 2)
    groups.update((controller, group) for controller in controllers.split(","))

counters = ("cpu.max", "cpu.stat", "cpu.cfs_quota_us", "cpu.cfs_period_us",
            "cpuset.cpus", "cpuset.cpus.effective", "memory.max", "memory.current",
            "memory.peak", "memory.events", "memory.limit_in_bytes",
            "memory.usage_in_bytes", "memory.max_usage_in_bytes", "memory.failcnt")
for mount in mounts:
    fields, filesystem = mount.split(" - ", 1)
    kind, _, options = filesystem.split()[:3]
    if kind not in ("cgroup", "cgroup2"):
        continue
    root, target = [re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), field)
                    for field in fields.split()[3:5]]
    controllers = {""} if kind == "cgroup2" else set(options.split(",")) & {"cpu", "cpuset", "memory"}
    for group in {groups[name] for name in controllers if name in groups}:
        try:
            relative = PurePosixPath(group).relative_to(root)
        except ValueError:
            continue
        if ".." in relative.parts:
            continue
        current = Path(target) / relative
        print("Current %s group: %s (mount root: %s)" % (kind, current, root), flush=True)
        for name in counters:
            counter = current / name
            try:
                value = counter.read_text().strip()
            except FileNotFoundError:
                continue
            except OSError as error:
                value = "unavailable (%s)" % type(error).__name__
            print("%s:\n%s" % (counter, value), flush=True)
PY
    diagnostic nvidia-smi \
        --query-gpu=index,name,uuid,driver_version,memory.total,memory.used,utilization.gpu \
        --format=csv
    # Provider identifiers survive cluster deletion and help support locate the affected VM.
    # Bypass proxies, avoid credential endpoints, and print no user data, tags or access tokens.
    diagnostic python3 - <<'PY'
import json
import urllib.request

opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
base = "http://169.254.169.254"

def metadata(path, headers, method="GET"):
    request = urllib.request.Request(base + path, headers=headers, method=method)
    with opener.open(request, timeout=2) as response:
        return response.read().decode()

try:
    azure = json.loads(metadata("/metadata/instance?api-version=2021-02-01", {"Metadata": "true"}))
    compute = azure["compute"]
    identity = {key: compute.get(key) for key in
                ("vmId", "resourceId", "subscriptionId", "resourceGroupName", "name",
                 "location", "zone", "vmSize")}
    identity["provider"] = "azure"
    identity["addresses"] = [
        {key: address.get(key) for key in ("privateIpAddress", "publicIpAddress")}
        for interface in azure.get("network", {}).get("interface", [])
        for address in interface.get("ipv4", {}).get("ipAddress", [])]
    print("Cloud instance identity: " + json.dumps(identity, sort_keys=True), flush=True)
except Exception as error:
    print("Azure instance metadata unavailable (%s)" % type(error).__name__, flush=True)
    try:
        token = metadata("/latest/api/token", {"X-aws-ec2-metadata-token-ttl-seconds": "60"}, "PUT")
        headers = {"X-aws-ec2-metadata-token": token}
        aws = json.loads(metadata("/latest/dynamic/instance-identity/document", headers))
        identity = {key: aws.get(key) for key in
                    ("instanceId", "accountId", "region", "availabilityZone", "instanceType",
                     "imageId", "privateIp", "pendingTime")}
        identity["provider"] = "aws"
        try:
            identity["publicIp"] = metadata("/latest/meta-data/public-ipv4", headers).strip()
        except Exception as error:
            identity["publicIp"] = "unavailable (%s)" % type(error).__name__
        print("Cloud instance identity: " + json.dumps(identity, sort_keys=True), flush=True)
    except Exception as error:
        print("AWS instance metadata unavailable (%s)" % type(error).__name__, flush=True)
PY
)

# Console-only and best effort: diagnostics must not affect readiness or trigger retries.
if ! print_host_diagnostics 2>&1; then
    printf 'Readiness host diagnostics incomplete; continuing\n' || :
fi

# -X options are JVM-specific. If the bounded capability check fails, keep the
# original options instead of making optional diagnostics a readiness failure.
readiness_jvm_options="${SPARK_SUBMIT_OPTS:-}"
if timeout --kill-after=2s 10s "${JAVA_HOME:+$JAVA_HOME/bin/}java" \
    -XshowSettings:vm -showversion -version >/dev/null 2>&1; then
    readiness_jvm_options+=' -XshowSettings:vm -showversion'
else
    printf 'Readiness JVM diagnostics unavailable; keeping original JVM options\n' || :
fi
printf 'Spark readiness %s: starting plugin-free check (master=%s, timeout=10m)\n' "$1" "$master"
printf '{"phase":"STARTING","runtime":"%s"}\n' "$1" > "$status_file"

# DBR 18.3 spark-shell exhausts the default 1 GiB heap during Scala compiler classpath
# initialization, before the readiness job runs. Use 2 GiB, which passed repeated native probes.
# spark-shell may exit zero after an initialization error; also require the completed job marker.
set +e
# Print the actual driver JVM's version and maximum heap without changing its heap
# or replacing existing JVM options. Do not enable Spark's full command/configuration dumps.
SPARK_SUBMIT_OPTS="$readiness_jvm_options" \
timeout --kill-after=30s 10m "${SPARK_HOME:?}/bin/spark-shell" \
    --master "$master" \
    --driver-memory 2g \
    --conf spark.plugins= \
    --conf spark.shuffle.manager=sort \
    --conf spark.deploy.maxExecutorRetries=0 \
    --conf spark.connect.grpc.binding.port=0 \
    2>&1 <<'SCALA' | tee "$log_file"
try { require(spark.range(0, 100, 1, 2).agg(Map("id" -> "sum")).first().getLong(0) == 4950L); spark.stop(); println("\nDATABRICKS_SPARK_READINESS_OK"); System.exit(0) } catch { case error: Throwable => error.printStackTrace(); System.exit(1) }
SCALA
statuses=("${PIPESTATUS[@]}")
set -e
if [[ "${statuses[0]}" == 0 && "${statuses[1]}" == 0 ]] &&
    grep -qx 'DATABRICKS_SPARK_READINESS_OK' "$log_file"; then
    printf '{"phase":"READY","runtime":"%s"}\n' "$1" > "$status_file"
    printf 'Spark readiness %s: READY\n' "$1"
    # Take one full GPU snapshot after native readiness, not again for upstream Spark.
    if [[ "$1" == databricks ]]; then
        {
            printf '\n=== Databricks GPU diagnostics ===\n'
            diagnostic nvidia-smi -q
            printf '=== End Databricks GPU diagnostics ===\n'
        } || :
    fi
    exit 0
fi

# Generic SparkException, executor loss, OOM, timeout, bind/configuration and class-loading errors
# are not sufficient infrastructure evidence, even in a plugin-free application.
reason=SPARK_STARTUP_UNCLASSIFIED
if grep -Eq '(java\.io\.[A-Za-z]+Exception|java\.nio\.file\.FileSystemException):.*No space left on device' "$log_file"; then
    reason=SPARK_STARTUP_DISK_FULL
elif grep -Eq 'java\.io\.IOException:.*Input/output error' "$log_file"; then
    reason=SPARK_STARTUP_DISK_IO
fi
printf '{"phase":"FAILED","runtime":"%s","reason":"%s"}\n' "$1" "$reason" > "$status_file"
printf 'Spark readiness %s: FAILED (%s, spark exit=%s, log exit=%s)\n' \
    "$1" "$reason" "${statuses[0]}" "${statuses[1]}"
printf '%s\n' \
    'This pre-test probe runs a simple Spark job with the cuDF-Spark plugin disabled.' \
    'Product tests for this runtime have not started; this is not a product-test failure.'
if [[ "$reason" == SPARK_STARTUP_UNCLASSIFIED ]]; then
    printf '%s\n' \
        'No known infrastructure error signature was identified; the cause is still unclassified.' \
        'Check the Spark error and log-write status above, then JVM/heap settings, runtime configuration, and host resources.'
else
    printf '%s\n' \
        'A host filesystem error signature was detected; inspect the disk and I/O diagnostics above.'
fi
printf '%s\n' \
    'This result alone does not implicate PR product code or prove a cloud-provider fault; CI/setup/probe changes may still be relevant.' \
    'For support, include the Spark error, UTC time, cloud instance identity/IPs, and host/JVM diagnostics printed above.'
exit 1
