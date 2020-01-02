#   Copyright 2011 OpenStack Foundation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#   Author: Tingjie Chen <tingje.chen@intel.com>
#   Credit: python-rookclient
#

import sys
sys.path.append('../')
import pkg.kube_api as api
import pkg.ceph as ceph

class CephApiTester(object):

    def __init__(self):
        self.kube_op = api.KubeOperator('rook-ceph')
        self.ceph_op = ceph.RookCephOperator('rook-ceph')
        self.api = ceph.RookCephApi('rook-ceph')

    def test_command_get(self):
        #objects = self.op.command_get('CephCluster', 'rook-ceph', 'rook-ceph')
        objects = self.kube_op.command_get('configmap', 'rook-config-override')
        print("Object Print List:\n--------------------")
        print(objects)
        print("-------------------")
    
    def test_command_find_pod(self):
        pod = self.kube_op.command_find_pod('rook-ceph-tools')
        print("Pod Name: " + pod)
        pod = self.kube_op.command_find_pod('rook-ceph-mon', 'ceph_daemon_id', 'a')
        print("Pod Name: " + pod)

    def test_command_execute_cli(self):
        pod = self.kube_op.command_find_pod('rook-ceph-tools')
        output = self.kube_op.command_execute_cli(pod, 'ceph -s --format json-pretty')
        print(output)

    def test_get_rook_mon_count(self):
        count = self.ceph_op.get_rook_mon_count()
        print("Ceph monitor count: " + str(count))

    def test_get_rook_mon_list(self):
        mon_list = self.ceph_op.get_rook_mon_list()
        print(mon_list)

    def test_modify_rook_mon_count(self, count):
        self.ceph_op.modify_rook_mon_count(count)

    def test_add_dedicated_ceph_mon(self):
        self.ceph_op.add_dedicated_ceph_mon('k', '10.97.181.143:6789')

    def test_remove_dedicated_ceph_mon(self):
        self.ceph_op.remove_dedicated_ceph_mon('d')

    def test_ceph_api(self):
        status = self.api.ceph_status()
        print(status)
        status = self.api.ceph_health()
        print(status)
        status = self.api.osd_tree()
        print(status)
        status = self.api.osd_crush_dump()
        print(status)

if __name__ == "__main__":
    tester = CephApiTester()
    tester.test_command_get()
    #tester.test_modify_rook_mon_count(3)
    #tester.test_get_rook_mon_count()
    
    #tester.test_get_rook_mon_list()
    #tester.test_add_dedicated_ceph_mon()
    #tester.test_get_rook_mon_list()
    
    #tester.test_remove_dedicated_ceph_mon()
    #tester.test_command_find_pod()
    #tester.test_command_execute_cli()

    tester.test_ceph_api()

    tester.test_remove_dedicated_ceph_mon()
