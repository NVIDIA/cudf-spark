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

# Remote Linux supervisor for run-tests.py with DATABRICKS_CI_TEST_SUPERVISION=1.
# A regular log file decouples command completion from orphan JVM output pipes.
set +m
test_pid=
log_pid=

cleanup() {
    trap '' HUP INT TERM
    if [[ -n "$test_pid" ]] && kill -0 -- "-$test_pid" 2>/dev/null; then
        echo "Databricks: terminating remaining processes from this test invocation"
        ps -eo pid,ppid,pgid,stat,comm | awk -v group="$test_pid" 'NR == 1 || $3 == group'
        kill -TERM -- "-$test_pid" 2>/dev/null || true
        for (( i=0; i<10; i++ )); do
            kill -0 -- "-$test_pid" 2>/dev/null || break
            sleep 1
        done
        if kill -0 -- "-$test_pid" 2>/dev/null; then
            echo "Databricks: forcing remaining test processes to exit after 10s"
            kill -KILL -- "-$test_pid" 2>/dev/null || true
        fi
    fi
    if [[ -n "$log_pid" ]]; then
        kill "$log_pid" 2>/dev/null || true
        wait "$log_pid" 2>/dev/null || true
    fi
}

trap 'cleanup' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Create the file before either child can run, including on a fresh cluster.
: > testout || exit "$?"
setsid "$@" >> testout 2>&1 &
test_pid=$!
tail --pid="$test_pid" --sleep-interval=0.2 -n +1 -f testout &
log_pid=$!
wait "$test_pid"
status=$?
# GNU tail drains the file and exits when the command leader exits, not its descendants.
wait "$log_pid" || true
log_pid=
printf '\nDatabricks remote test command completed (exit=%s)\n' "$status"
cleanup
trap - EXIT
exit "$status"
