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
from clusterutils import ClusterUtils
import getopt
import sys

# This scripts create and starts a Databricks cluster and waits for it to be running.
#
# The name parameter is meant to be a unique name used when creating the cluster. Note we
# append the epoch time to the end of it to help prevent collisions.
#
# Returns cluster id to stdout, all other logs default to stderr
#
# User is responsible for removing cluster if a failure or when done with cluster.
def main():
  workspace = 'https://dbc-9ff9942e-a9c4.cloud.databricks.com'
  token = ''
  sshkey = ''
  cluster_name = 'CI-GPU-databricks-26.08.0-SNAPSHOT'
  idletime = 240
  runtime = '14.3.x-gpu-ml-scala2.12'
  num_workers = 1
  worker_type = 'g4dn.xlarge'
  driver_type = 'g4dn.xlarge'
  cloud_provider = 'aws'
  # comma separated init scripts in Databricks workspace, e.g. /foo,/bar,...
  init_scripts = ''
  aws_zone='auto'


  try:
      opts, args = getopt.getopt(sys.argv[1:], 'hw:t:k:n:i:r:o:d:e:s:f:z:',
                                 ['workspace=', 'token=', 'sshkey=', 'clustername=', 'idletime=',
                                     'runtime=', 'workertype=', 'drivertype=', 'numworkers=', 'cloudprovider=', 'initscripts=', 'awszone='])
  except getopt.GetoptError:
      print(
          'create.py -w <workspace> -t <token> -k <sshkey> -n <clustername> -i <idletime> -r <runtime> -o <workernodetype> -d <drivernodetype> -e <numworkers> -s <cloudprovider> -f <initscripts> -z <awszone>')
      sys.exit(2)

  for opt, arg in opts:
      if opt == '-h':
          print(
              'create.py -w <workspace> -t <token> -k <sshkey> -n <clustername> -i <idletime> -r <runtime> -o <workernodetype> -d <drivernodetype> -e <numworkers> -s <cloudprovider> -f <initscripts> -z <awszone>')
          sys.exit()
      elif opt in ('-w', '--workspace'):
          workspace = arg
      elif opt in ('-t', '--token'):
          token = arg
      elif opt in ('-k', '--sshkey'):
          sshkey = arg
      elif opt in ('-n', '--clustername'):
          cluster_name = arg
      elif opt in ('-i', '--idletime'):
          idletime = arg
      elif opt in ('-r', '--runtime'):
          runtime = arg
      elif opt in ('-o', '--workertype'):
          worker_type = arg
      elif opt in ('-d', '--drivertype'):
          driver_type = arg
      elif opt in ('-e', '--numworkers'):
          num_workers = arg
      elif opt in ('-s', '--cloudprovider'):
          cloud_provider = arg
      elif opt in ('-f', '--initscripts'):
          init_scripts = arg
      elif opt in ('-z', '--awszone'):
          aws_zone = arg

  print('-w is ' + workspace, file=sys.stderr)
  print('-k is ' + sshkey, file=sys.stderr)
  print('-n is ' + cluster_name, file=sys.stderr)
  print('-i is ' + str(idletime), file=sys.stderr)
  print('-r is ' + runtime, file=sys.stderr)
  print('-o is ' + worker_type, file=sys.stderr)
  print('-d is ' + driver_type, file=sys.stderr)
  print('-e is ' + str(num_workers), file=sys.stderr)
  print('-s is ' + cloud_provider, file=sys.stderr)
  print('-f is ' + init_scripts, file=sys.stderr)
  print('-z is ' + aws_zone, file=sys.stderr)

  if not sshkey:
      print("You must specify an sshkey!", file=sys.stderr)
      sys.exit(2)

  if not token:
      print("You must specify an token!", file=sys.stderr)
      sys.exit(2)

  templ = ClusterUtils.generate_create_templ(sshkey, cluster_name, runtime, idletime,
          num_workers, driver_type, worker_type, cloud_provider, init_scripts, aws_zone, printLoc=sys.stderr)
  clusterid = ClusterUtils.create_cluster(workspace, templ, token, printLoc=sys.stderr)
  ClusterUtils.wait_for_cluster_start(workspace, clusterid, token, printLoc=sys.stderr)

  # only print the clusterid to stdout so a calling script can get it easily
  print(clusterid, file=sys.stdout)

if __name__ == '__main__':
  main()
