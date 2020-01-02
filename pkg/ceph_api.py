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
#   Authors: Tingjie Chen <tingjie.chen@intel.com>
#

import enum
import time
import sys
import shutil
import json
sys.path.append('../')
import pkg.ceph as ceph


class RookCephApi(object):
    def __init__(self, namespace):
        self.ceph_op = ceph.RookCephOperator(namespace)

    def status(self, timeout=None):
        status = self.ceph_op.execute_toolbox_cli('status',
            timeout=timeout)
        return status

    def ceph_status(self, timeout=None):
        status = self.ceph_op.execute_toolbox_cli('health',
            timeout=timeout)
        return status

    def ceph_health(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('health',
            timeout=timeout)
        health = self.ceph_op.kube_op.get_object_value(output, 'status')
        return health

    def fsid(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('fsid',
            timeout=timeout)
        fsid = self.ceph_op.kube_op.get_object_value(output, 'fsid')
        return fsid

    def ceph_df(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('df', 
            timeout=timeout)
        return output

    def osd_create(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd create',
            timeout=timeout)
        return output

    def osd_remove(self, ids, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd rm '+str(ids),
            timeout=timeout)
        return output

    def osd_down(self, ids, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd down '+str(ids),
            timeout=timeout)
        return output

    def osd_df(self, output_method='tree', timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd df '+output_method,
            timeout=timeout)
        return output

    def osd_stat(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd stat',
            timeout=timeout)
        return output

    def osd_tree(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd tree',
            timeout=timeout)
        return output

    def osd_pool_create(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd pool create',
            timeout=timeout)
        return output

    def osd_pool_delete(self, pool, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd pool delete ' + pool + ' ' + pool,
            sure=True, timeout=timeout)
        return output

    def osd_pool_ls(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd pool ls',
            timeout=timeout)
        return output

    def osd_crush_dump(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd crush dump',
            timeout=timeout)
        return output

    def osd_crush_remove(self, osdid_str, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd crush rm ' + osdid_str,
            timeout=timeout)
        return output

    def osd_crush_rule_dump(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('osd crush rule dump',
            timeout=timeout)
        return output

    def mon_remove(self, mon_id, timeout=None):
        return self.ceph_op.remove_dedicated_ceph_mon(self, mon_id,
            timeout=timeout)

    def get_tiers_size(self, timeout=None):
        output = self.osd_df(self, timeout=timeout)
        search_tree = {}
        for node in output['nodes']:
            search_tree[node['id']] = node

        # Extract the tiers as we will return a dict for the size of each tier
        tiers = {k: v for k, v in search_tree.items() if v['type'] == 'root'}

        # For each tier, traverse the heirarchy from the root->chassis->host.
        # Sum the host sizes to determine the overall size of the tier
        tier_sizes = {}
        for tier in tiers.values():
            tier_size = 0
            for chassis_id in tier['children']:
                chassis_size = 0
                chassis = search_tree[chassis_id]
                for host_id in chassis['children']:
                    host = search_tree[host_id]
                    if (chassis_size == 0 or
                            chassis_size > host['kb']):
                        chassis_size = host['kb']
                tier_size += chassis_size / (1024**2)
            tier_sizes[tier['name']] = tier_size

        return tier_sizes

    def osd_pool_get_quota(self, pool, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd pool get-quota '+pool,
            timeout=timeout)
        return output

    def osd_pool_set_quota(self, pool, field, val, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd pool set-quota '+pool+' '+field+' '+val,
            timeout=timeout)
        return output

    def osd_pool_get(self, pool, var, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd pool get '+pool+' ' + var,
            timeout=timeout)
        if not output:
            return None
        return output[var]

    def auth_del(self, osdid_str, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'auth del '+osdid_str,
            timeout=timeout)
        return output

    def osd_crush_rule_rm(self, name, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd crush rule rm '+name,
            timeout=timeout)
        return output

    def osd_crush_rule_ls(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd crush rule ls',
            timeout=timeout)
        return output

    def osd_crush_add_bucket(self, name, _type, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd crush add-bucket '+name+' '+_type,
            timeout=timeout)
        return output

    def osd_crush_move(self, name, args, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd crush move '+name+' '+args,
            timeout=timeout)
        return output

    def osd_crush_tree(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd crush tree',
            timeout=timeout)
        return output

    def osd_crush_rename_bucket(self, srcname, dstname, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd crush rename-bucket '+srcname+' '+dstname,
            timeout=timeout)
        return output

    def osd_crush_rule_rename(self, srcname, dstname, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'osd crush rule rename '+srcname+' '+dstname,
            timeout=timeout)
        return output

    def quorum_status(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'quorum_status',
            timeout=timeout)
        return output

    #def pg_dump_stuck(self, stuckops=None, threshold=None, timeout=None):
    def pg_dump_stuck(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'pg dump_stuck',
            timeout=timeout)
        return output
