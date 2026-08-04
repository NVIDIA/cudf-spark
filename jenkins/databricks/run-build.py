# Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
import json
import requests
import sys
import getopt
import time
import os
import shlex
import subprocess
from clusterutils import ClusterUtils
import params

def shell_join(args):
  """Return shell-escaped command arguments."""
  return ' '.join(shlex.quote(arg) for arg in args)

def main():
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

  print("Copying source")
  rsync_command = ["rsync", "-I", "-Pave", rsync_ssh, "--", params.source_tgz,
                   "ubuntu@%s:%s" % (master_addr, params.tgz_dest)]
  print("rsync command: %s" % shell_join(rsync_command))
  subprocess.check_call(rsync_command)

  build_command = shell_join([
      "env",
      "SPARKSRCTGZ=%s" % params.tgz_dest,
      "BASE_SPARK_VERSION=%s" % params.base_spark_pom_version,
      "BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=%s" %
      params.base_spark_version_to_install_databricks_jars,
      "MVN_OPT=%s" % params.mvn_opt,
      "EXTRA_ENVS=%s" % params.extra_envs,
      "bash",
      params.script_dest,
  ] + params.script_args)
  remote_command = "%s 2>&1 | tee buildout; exit ${PIPESTATUS[0]}" % build_command
  ssh_command = ["ssh"] + ssh_args + ["ubuntu@%s" % master_addr,
                                       "bash -c %s" % shlex.quote(remote_command)]
  print("ssh command: %s" % shell_join(ssh_command))
  subprocess.check_call(ssh_command)

  # Only the nightly build needs to copy the spark-rapids-built.tgz back
  if params.test_type == 'nightly':
      print("Copying built tarball back")
      rsync_command = ["rsync", "-I", "-Pave", rsync_ssh, "--",
                       "ubuntu@%s:/home/ubuntu/spark-rapids-built.tgz" % master_addr, "./"]
      print("rsync command to get built tarball: %s" % shell_join(rsync_command))
      subprocess.check_call(rsync_command)

if __name__ == '__main__':
  main()
