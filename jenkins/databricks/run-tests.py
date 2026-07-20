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

import subprocess
import sys

from clusterutils import ClusterUtils

import params


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
    ssh_args = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s" % params.private_key_file
    rsync_command = "rsync -I -Pave \"ssh %s\" %s ubuntu@%s:%s" % \
        (ssh_args, params.local_script, master_addr, params.script_dest)
    print("rsync command: %s" % rsync_command)
    subprocess.check_call(rsync_command, shell=True)

    ssh_command = "ssh %s ubuntu@%s " % (ssh_args, master_addr) + \
        "'LOCAL_JAR_PATH=%s SPARK_CONF=%s BASE_SPARK_VERSION=%s EXTRA_ENVS=%s TEST_TYPE=%s bash %s %s 2>&1 | tee testout; " \
        "if [ ${PIPESTATUS[0]} -ne 0 ]; then false; else true; fi'" % \
        (params.jar_path, params.spark_conf, params.base_spark_pom_version, params.extra_envs, params.test_type,
         params.script_dest, ' '.join(params.script_args))
    print("ssh command: %s" % ssh_command)
    try:
        subprocess.check_call(ssh_command, shell=True)
    finally:
        print("Copying test reports back")
        try:
            report_target_path = "%s/integration_tests/target" % get_test_report_path_prefix()
            subprocess.check_call("mkdir -p integration_tests/target", shell=True)
            # Copy the diagnostics needed by Jenkins while avoiding unrelated run_dir contents.
            rsync_command = "rsync -I -Pave \"ssh %s\" --prune-empty-dirs " \
                "--include='run_dir*/' " \
                "--include='run_dir*/TEST-pytest-*.xml' " \
                "--include='run_dir*/eventlog_*/***' " \
                "--include='run_dir*/*_worker_logs.log' " \
                "--exclude='*' ubuntu@%s:%s/ integration_tests/target/" % \
                (ssh_args, master_addr, report_target_path)
            print("rsync command: %s" % rsync_command)
            subprocess.check_call(rsync_command, shell=True)
            # Keep the existing Jenkins JUnit publishing flow, which expects XML files in this
            # directory.
            copy_xml_command = "cp integration_tests/target/run_dir*/TEST-pytest-*.xml ./ || true"
            print("copy xml command: %s" % copy_xml_command)
            subprocess.check_call(copy_xml_command, shell=True)
        except subprocess.CalledProcessError as e:
            print("WARNING: test report collection failed: %s" % e)


if __name__ == '__main__':
    main()
