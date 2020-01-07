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
import time
sys.path.append('../')
import pkg.kube_api as kube_api
import pkg.ceph as ceph
import pkg.ceph_api as ceph_api

class CephApiTester(object):

    def __init__(self):
        self.kube_op = kube_api.KubeOperator('rook-ceph')
        self.ceph_op = ceph.RookCephOperator('rook-ceph')
        self.api = ceph_api.RookCephApi('rook-ceph')

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

    def test_mon_remove(self):
        self.api.mon_remove('e')

    def test_ceph_api(self):
        status = self.api.ceph_status()
        print(status)
        status = self.api.ceph_health()
        print(status)
        status = self.api.osd_tree()
        print(status)
        status = self.api.osd_crush_dump()
        print(status)
        output = self.api.osd_crush_rule_ls()
        print(output)
        output = self.api.osd_pool_create('aa', 128, 128)
        print(output)

    def test_crushmap_api(self):
        crushmap_txt_file = "crushmap.txt"
        crushmap_bin_file = "crushmap.bin"
        output = self.api.osd_crushmap_get(crushmap_bin_file)
        print(output)
        time.sleep(2)

        output = self.api.osd_crushmap_decompile(crushmap_bin_file,
            crushmap_txt_file)
        print(output)
        time.sleep(2)

        output = self.api.osd_crushmap_compile(crushmap_txt_file,
            crushmap_bin_file)
        print(output)
        time.sleep(2)

        output = self.api.osd_crushmap_set(crushmap_bin_file)
        print(output)


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
    tester.test_crushmap_api()
    #tester.test_mon_remove()
