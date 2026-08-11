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

import time
import json
import time
import os
import requests
import sys

class ClusterUtils(object):

    @staticmethod
    def generate_create_templ(sshKey, cluster_name, runtime, idle_timeout,
            num_workers, driver_node_type, worker_node_type, cloud_provider, init_scripts, aws_zone,
            printLoc=sys.stdout):
        timeStr = str(int(time.time()))
        uniq_name = cluster_name + "-" + timeStr
        templ = {}
        templ['cluster_name'] = uniq_name
        print("cluster name is going to be %s" % uniq_name, file=printLoc)
        templ['spark_version'] = runtime
        if (cloud_provider == 'aws'):
            templ['aws_attributes'] = {
                        "zone_id": aws_zone,
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
            }
        templ['autotermination_minutes'] = idle_timeout
        templ['enable_elastic_disk'] = 'false'
        templ['enable_local_disk_encryption'] = 'false'
        templ['node_type_id'] = worker_node_type
        templ['driver_node_type_id'] = driver_node_type
        templ['ssh_public_keys'] = [ sshKey ]
        templ['num_workers'] = num_workers
        if (init_scripts != ''):
            templ['init_scripts']=[]
            path_list = init_scripts.split(',')
            for path in path_list:
                templ['init_scripts'].append(
                    {
                        'workspace' : {
                            'destination' : path
                        }
                    }
                )

        return templ


    @staticmethod
    def create_cluster(workspace, jsonCreateTempl, token, printLoc=sys.stdout):
        resp = requests.post(workspace + "/api/2.0/clusters/create", headers={'Authorization': 'Bearer %s' % token}, json=jsonCreateTempl)
        print("create response is %s" % resp.text, file=printLoc)
        clusterid = resp.json()['cluster_id']
        print("cluster id is %s" % clusterid, file=printLoc)
        return clusterid


    @staticmethod
    def wait_for_cluster_start(workspace, clusterid, token, retries=20, printLoc=sys.stdout):
        p = 0
        waiting = True
        jsonout = None
        while waiting:
            time.sleep(30)
            jsonout = ClusterUtils.cluster_state(workspace, clusterid, token, printLoc=printLoc)
            current_state = jsonout['state']
            print(clusterid + " state:" + current_state, file=printLoc)
            if current_state in ['RUNNING']:
                break
            if current_state in ['INTERNAL_ERROR', 'SKIPPED', 'TERMINATED'] or p >= 150:
                if p >= retries:
                   print("Waited %d times already, stopping" % p)
                # Output the cluster ID to stdout so a calling script can get it easily
                print(clusterid, file=sys.stdout)
                sys.exit(4)
            p = p + 1
        print("Done starting cluster", file=printLoc)
        return jsonout


    @staticmethod
    def is_cluster_running(jsonout):
        current_state = jsonout['state']
        if current_state in ['RUNNING', 'RESIZING']:
            return True
        else:
            return False


    @staticmethod
    def terminate_cluster(workspace, clusterid, token, printLoc=sys.stdout):
        jsonout = ClusterUtils.cluster_state(workspace, clusterid, token, printLoc=printLoc)
        if not ClusterUtils.is_cluster_unning(jsonout):
            print("Cluster is not running", file=printLoc)
            sys.exit(1)

        print("Stopping cluster: " + clusterid, file=printLoc)
        resp = requests.post(workspace + "/api/2.0/clusters/delete", headers={'Authorization': 'Bearer %s' % token}, json={'cluster_id': clusterid})
        print("stop response is %s" % resp.text, file=printLoc)
        print("Done stopping cluster", file=printLoc)


    @staticmethod
    def delete_cluster(workspace, clusterid, token, printLoc=sys.stdout):
        print("Deleting cluster: " + clusterid, file=printLoc)
        resp = requests.post(workspace + "/api/2.0/clusters/permanent-delete", headers={'Authorization': 'Bearer %s' % token}, json={'cluster_id': clusterid})
        print("delete response is %s" % resp.text, file=printLoc)
        print("Done deleting cluster", file=printLoc)


    @staticmethod
    def start_existing_cluster(workspace, clusterid, token, printLoc=sys.stdout):
        print("Starting cluster: " + clusterid, file=printLoc)
        resp = requests.post(workspace + "/api/2.0/clusters/start", headers={'Authorization': 'Bearer %s' % token}, json={'cluster_id': clusterid})
        print("start response is %s" % resp.text, file=printLoc)


    @staticmethod
    def cluster_state(workspace, clusterid, token, printLoc=sys.stdout):
        clusterresp = requests.get(workspace + "/api/2.0/clusters/get?cluster_id=%s" % clusterid, headers={'Authorization': 'Bearer %s' % token})
        clusterjson = clusterresp.text
        print("cluster response is %s" % clusterjson, file=printLoc)
        jsonout = json.loads(clusterjson)
        return jsonout


    @staticmethod
    def get_master_addr_from_json(jsonout):
        master_addr = None
        if ClusterUtils.is_cluster_running(jsonout):
            driver = jsonout['driver']
            master_addr = driver["public_dns"]
        return master_addr


    @staticmethod
    def cluster_list(workspace, token, printLoc=sys.stdout):
        clusterresp = requests.get(workspace + "/api/2.0/clusters/list", headers={'Authorization': 'Bearer %s' % token})
        clusterjson = clusterresp.text
        print("cluster list is %s" % clusterjson, file=printLoc)
        jsonout = json.loads(clusterjson)
        return jsonout


    @staticmethod
    def cluster_get_master_addr(workspace, clusterid, token, printLoc=sys.stdout):
        jsonout = ClusterUtils.cluster_state(workspace, clusterid, token, printLoc=printLoc)
        addr = ClusterUtils.get_master_addr_from_json(jsonout)
        print("master addr is %s" % addr, file=printLoc)
        return addr

