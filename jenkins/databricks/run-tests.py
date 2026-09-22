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
"""Upload & run test script on Databricks cluster."""

import glob
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


def main():
    """Define main function."""
    master_addr = ClusterUtils.cluster_get_master_addr(params.workspace, params.clusterid, params.token)
    if master_addr is None:
        print("Error, didn't get master address")
        sys.exit(1)
    print("Master node address is: %s" % master_addr)

    print("Copying script")
    ssh_args = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                "-p", "2200", "-i", params.private_key_file]
    rsync_ssh = shell_join(["ssh"] + ssh_args)
    rsync_command = ["rsync", "-I", "-Pave", rsync_ssh, "--", params.local_script,
                     "ubuntu@%s:%s" % (master_addr, params.script_dest)]
    print("rsync command: %s" % shell_join(rsync_command))
    subprocess.check_call(rsync_command)

    test_command = shell_join([
        "env",
        "LOCAL_JAR_PATH=%s" % params.jar_path,
        "SPARK_CONF=%s" % params.spark_conf,
        "BASE_SPARK_VERSION=%s" % params.base_spark_pom_version,
        "EXTRA_ENVS=%s" % params.extra_envs,
        "TEST_TYPE=%s" % params.test_type,
        "bash",
        params.script_dest,
    ] + params.script_args)
    remote_command = "%s 2>&1 | tee testout; exit ${PIPESTATUS[0]}" % test_command
    ssh_command = ["ssh"] + ssh_args + ["ubuntu@%s" % master_addr,
                                         "bash -c %s" % shlex.quote(remote_command)]
    print("ssh command: %s" % shell_join(ssh_command))
    try:
        subprocess.check_call(ssh_command)
    finally:
        print("Copying test reports back")
        try:
            report_target_path = "%s/integration_tests/target" % get_test_report_path_prefix()
            subprocess.check_call(["mkdir", "-p", "integration_tests/target"])
            # Copy the diagnostics needed by Jenkins while avoiding unrelated run_dir contents.
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
            subprocess.check_call(rsync_command)
            # Keep the existing Jenkins JUnit publishing flow, which expects XML files in this
            # directory.
            xml_files = glob.glob("integration_tests/target/run_dir*/TEST-pytest-*.xml")
            if xml_files:
                copy_xml_command = ["cp"] + xml_files + ["./"]
                print("copy xml command: %s" % shell_join(copy_xml_command))
                subprocess.call(copy_xml_command)
        except subprocess.CalledProcessError as e:
            print("WARNING: test report collection failed: %s" % e)


if __name__ == '__main__':
    main()
