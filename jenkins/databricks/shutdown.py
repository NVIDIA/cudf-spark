# Copyright (c) 2020, NVIDIA CORPORATION.
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

# shutdown or delete a databricks cluster
def main():
  workspace = 'https://dbc-9ff9942e-a9c4.cloud.databricks.com'
  token = ''
  clusterid = '0617-140138-umiak14'
  delete = False

  try:
      opts, args = getopt.getopt(sys.argv[1:], 'hs:t:c:d',
                                 ['workspace=', 'token=', 'clusterid=', 'delete'])
  except getopt.GetoptError:
      print(
          'shutdown.py -s <workspace> -t <token> -c <clusterid>')
      sys.exit(2)

  for opt, arg in opts:
      if opt == '-h':
          print(
              'shutdown.py -s <workspace> -t <token> -c <clusterid>')
          sys.exit()
      elif opt in ('-s', '--workspace'):
          workspace = arg
      elif opt in ('-t', '--token'):
          token = arg
      elif opt in ('-c', '--clusterid'):
          clusterid = arg
      elif opt in ('-d', '--delete'):
          delete = True

  print('-s is ' + workspace)
  print('-c is ' + clusterid)

  if not clusterid:
      print("You must specify clusterid!")
      sys.exit(1)

  if not token:
      print("You must specify token!")
      sys.exit(1)

  if delete:
      ClusterUtils.delete_cluster(workspace, clusterid, token)
  else:
      ClusterUtils.terminate_cluster(workspace, clusterid, token)

if __name__ == '__main__':
  main()
