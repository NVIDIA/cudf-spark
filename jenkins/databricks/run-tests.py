# Copyright (c) 2020-2026, NVIDIA CORPORATION.
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
"""Upload & run test script on Databricks cluster.

DATABRICKS_CI_TEST_SUPERVISION=1 opts CI into process cleanup and bounded SSH/transfers.
Leave it unset for the existing manual execution behavior.
"""

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys

from clusterutils import ClusterUtils

import params


def shell_join(args):
    """Return shell-escaped command arguments."""
    return ' '.join(shlex.quote(arg) for arg in args)


def get_test_report_path_prefix():
    """Return the remote repo root whose integration_tests/target contains run_dir outputs.

    When jar_path is provided, tests run from that uploaded JAR directory. Otherwise Spark
    4+ Databricks builds run from scala2.13/pom.xml, so run_pyspark_from_build.sh writes
    run_dir* under spark-rapids/scala2.13/integration_tests/target.
    """
    source_path = (params.jar_path or "/home/ubuntu/spark-rapids").rstrip("/")
    if params.jar_path:
        return source_path

    try:
        spark_major_version = int(params.base_spark_pom_version.split(".", 1)[0])
    except ValueError:
        print("WARNING: unable to parse base Spark version '%s'; using the default report path" %
              params.base_spark_pom_version)
        spark_major_version = 0
    if spark_major_version >= 4:
        return "%s/scala2.13" % source_path
    return source_path


def observe_test_output(line, summary):
    line = re.sub(r"\x1b\[[0-9;]*m", "", line).strip()
    if line.startswith("WILL RUN TESTS WITH SPARK_HOME:"):
        # Only the last invocation can explain a failure; earlier suites may have passed.
        summary.clear()
        summary["invocation_started"] = True
    if re.search(r"=+ test session starts =+", line):
        summary["pytest_started"] = True
    if "cudaErrorDevicesUnavailable" in line:
        summary["gpu_unavailable"] = True
    if "IllegalStateException: Could not find a single GPU to use" in line:
        summary["gpu_acquisition_failed"] = True
    if "Could not start Spark Connect GRPC service" in line:
        summary["connect_start_failed"] = True
    if "Address already in use" in line:
        summary["port_in_use"] = True
    if line.startswith("INTERNALERROR>"):
        summary["pytest_internal_error"] = True
    if "Py4JNetworkError: Answer from Java side is empty" in line:
        summary["java_gateway_lost"] = True
    if "Databricks: forcing remaining test processes to exit after 10s" in line:
        summary["forced_shutdown"] = True
    failure = re.match(r"FAILED integration tests \(exit=(\d+), run_dir=.*\)", line)
    if failure:
        summary["test_exit"] = failure.group(1)
    if line.startswith("FAILED spark-shell smoke test ("):
        summary["smoke_failed"] = True
    if line.startswith("=") and re.search(r"\bin [0-9.]+s\b", line):
        counts = re.findall(r"\b\d+ (?:passed|failed|errors?|skipped|xfailed|xpassed)\b", line)
        if "no tests ran" in line:
            summary["pytest_result"] = "no tests ran"
        elif counts:
            summary["pytest_result"] = ", ".join(counts)


def record_readiness_output(line, readiness):
    # Console observations survive a later loss of SSH. They are diagnostic only;
    # the retry classifier still requires the attempt's remote readiness markers.
    match = re.fullmatch(r"Spark readiness (databricks|upstream): (READY|FAILED)(?: \(.*\))?",
                         line.strip())
    if not match:
        return
    readiness[match.group(1)] = match.group(2)
    output = os.environ.get("DATABRICKS_READINESS_OBSERVATIONS_FILE")
    if output:
        path = Path(output)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(readiness) + "\n")
        temporary.replace(path)


def format_test_failure(summary, status):
    lines = ["Remote test command exit: %s" % status]
    if "test_exit" in summary:
        lines.append("Integration test invocation exit: " + summary["test_exit"])
        if status == 255:
            lines.append("SSH exited 255 after an integration-test failure was already observed; "
                         "the transport error does not replace that failure.")
    if summary.get("smoke_failed"):
        lines.append("Spark-shell smoke test failed.")
    if summary.get("gpu_unavailable"):
        lines.append("Observed: 46 cudaErrorDevicesUnavailable: "
                     "CUDA-capable device(s) is/are busy or unavailable")
    if summary.get("gpu_acquisition_failed"):
        lines.append("Observed: IllegalStateException: Could not find a single GPU to use")
    if summary.get("connect_start_failed"):
        lines.append("Observed: Spark Connect gRPC service failed to start")
    if summary.get("port_in_use"):
        lines.append("Observed: port binding failed (Address already in use)")
    if summary.get("java_gateway_lost"):
        lines.append("Observed: Java gateway returned an empty response")
    if summary.get("pytest_internal_error"):
        lines.append("Observed: pytest INTERNALERROR (not a test assertion result)")
    if summary.get("forced_shutdown"):
        lines.append("Remaining test processes required forced shutdown after command completion.")
    if summary.get("invocation_started"):
        lines.append("Pytest session start observed for the last invocation: " +
                     ("yes" if summary.get("pytest_started") else "no"))
    if summary.get("pytest_result"):
        lines.append("Last pytest result: " + summary["pytest_result"])
    lines.append("These are observed symptoms, not an infrastructure root-cause classification.")
    return "\n".join(lines)


def report_test_failure(summary, status):
    # Reporting is optional. Keep failures here separate from the test's exit status.
    try:
        failure = format_test_failure(summary, status)
    except Exception:
        failure = "Remote test command exit: %s\nDetailed failure summary unavailable." % status
    try:
        failure_file = os.environ.get("DATABRICKS_TEST_FAILURE_FILE")
        if failure_file:
            Path(failure_file).write_text(failure + "\n")
    except Exception:
        failure += "\nWARNING: unable to save the test failure summary; console report only."
    try:
        print("\n=== Databricks test failure summary ===\n" + failure +
              "\nRetry classification and cluster cleanup are reported by the CI wrapper.\n"
              "=== End test failure summary ===", flush=True)
    except Exception:
        # Do not attempt another write if the diagnostic output itself is unavailable.
        pass


def main():
    """Define main function."""
    master_addr = ClusterUtils.cluster_get_master_addr(params.workspace, params.clusterid, params.token)
    if master_addr is None:
        print("Error, didn't get master address")
        sys.exit(1)
    print("Master node address is: %s" % master_addr)

    print("Copying script")
    ci_supervision = os.environ.get("DATABRICKS_CI_TEST_SUPERVISION") == "1"
    transfer_timeout = 120 if ci_supervision else None
    ssh_args = ClusterUtils.get_ssh_options(params.private_key_file)
    if ci_supervision:
        ssh_args += ["-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
                     "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3"]
    rsync_ssh = shell_join(["ssh"] + ssh_args)
    rsync_command = ["rsync", "-I", "-Pave", rsync_ssh, "--", params.local_script,
                     "ubuntu@%s:%s" % (master_addr, params.script_dest)]
    print("rsync command: %s" % shell_join(rsync_command))
    subprocess.check_call(rsync_command, timeout=transfer_timeout)

    test_command = [
        "env",
        "LOCAL_JAR_PATH=%s" % params.jar_path,
        "SPARK_CONF=%s" % params.spark_conf,
        "BASE_SPARK_VERSION=%s" % params.base_spark_pom_version,
        "EXTRA_ENVS=%s" % params.extra_envs,
        "TEST_TYPE=%s" % params.test_type,
        "bash",
        params.script_dest,
    ] + params.script_args
    if ci_supervision:
        runner = str(Path(__file__).with_name("run-test-command.sh"))
        remote_runner = params.script_dest + ".runner.sh"
        subprocess.check_call(["rsync", "-I", "-Pave", rsync_ssh, "--", runner,
                               "ubuntu@%s:%s" % (master_addr, remote_runner)],
                              timeout=transfer_timeout)
        remote_command = shell_join(["bash", remote_runner] + test_command)
    else:
        remote_command = "%s 2>&1 | tee testout; exit ${PIPESTATUS[0]}" % shell_join(test_command)
    ssh_command = ["ssh"] + ssh_args + ["ubuntu@%s" % master_addr,
                                         "bash -c %s" % shlex.quote(remote_command)]
    print("ssh command: %s" % shell_join(ssh_command))
    summary = {}
    readiness = {}
    status = None
    try:
        # Keep streaming the full log, retaining only bounded, known diagnostic fields.
        with subprocess.Popen(ssh_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, errors="replace") as process:
            for line in process.stdout:
                try:
                    print(line, end="", flush=True)
                except Exception:
                    # Continue draining SSH output even if forwarding a log line fails.
                    pass
                if readiness is not None:
                    try:
                        record_readiness_output(line, readiness)
                    except Exception:
                        # Losing optional evidence must not interrupt the remote command.
                        readiness = None
                if summary is not None:
                    try:
                        observe_test_output(line, summary)
                    except Exception:
                        # Disable the optional parser, not the test or its output stream.
                        summary = None
            status = process.wait()
        if status:
            # Preserve this wrapper's failure exit (1), without burying the footer under
            # a CalledProcessError traceback containing the entire SSH command.
            raise SystemExit(1)
    finally:
        try:
            print("Copying test reports back")
            report_target_path = "%s/integration_tests/target" % get_test_report_path_prefix()
            subprocess.check_call(["mkdir", "-p", "integration_tests/target"])
            # Keep reports and diagnostics together; Jenkins discovers the XML recursively.
            rsync_command = [
                "rsync", "-I", "-Pave", rsync_ssh,
                "--prune-empty-dirs",
                "--include=run_dir*/",
                "--include=run_dir*/TEST-pytest-*.xml",
                "--include=run_dir*/eventlog_*/***",
                "--include=run_dir*/*_worker_logs.log",
                "--exclude=*",
                "--",
                "ubuntu@%s:%s/" % (master_addr, report_target_path),
                "integration_tests/target/",
            ]
            print("rsync command: %s" % shell_join(rsync_command))
            subprocess.check_call(rsync_command, timeout=transfer_timeout)
        except (subprocess.SubprocessError, OSError) as e:
            try:
                print("WARNING: test report collection failed: %s" % e)
            except OSError:
                pass
        finally:
            if status:
                # The CI wrapper reprints this after classification and cleanup. Manual
                # callers get the same console footer without requiring a state file.
                report_test_failure(summary, status)


if __name__ == '__main__':
    main()
