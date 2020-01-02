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
import pkg.kube_api as api
import pkg.rook as rook


class RookCephApi(object):
    def __init__(self):
        self.ceph_op = RookCephOperator()

    def ceph_status(self, namespace=None, timeout=None):
        status = self.ceph_op.execute_toolbox_cli('ceph health',
            namespace=namespace, timeout=timeout)
        return status

    def ceph_health(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph health',
            namespace=namespace, timeout=timeout)
        health = self.ceph_op.kube_op.get_object_value(output, 'status')
        return health

    def fsid(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph fsid',
            namespace=namespace, timeout=timeout)
        fsid = self.ceph_op.kube_op.get_object_value(output, 'fsid')
        return fsid

    def ceph_df(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph df', 
            namespace=namespace, timeout=timeout)
        return output

    def osd_create(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd create',
            namespace=namespace, timeout=timeout)
        return output

    def osd_remove(self, ids, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd rm '+str(ids),
            namespace=namespace, timeout=timeout)
        return output

    def osd_down(self, ids, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd down '+str(ids),
            namespace=namespace, timeout=timeout)
        return output

    def osd_df(self, output_method='tree', namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd df '+output_method,
            namespace=namespace, timeout=timeout)
        return output

    def osd_stat(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd stat',
            namespace=namespace, timeout=timeout)
        return output

    def osd_tree(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd tree',
            namespace=namespace, timeout=timeout)
        return output

    def osd_pool_create(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd pool create',
            namespace=namespace, timeout=timeout)
        return output

    def osd_pool_delete(self, pool, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'ceph osd pool delete ' + pool + ' ' + pool,
            sure=True, namespace=namespace, timeout=timeout)
        return output

    def osd_pool_ls(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd pool ls',
            namespace=namespace, timeout=timeout)
        return output

    def osd_crush_dump(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd crush dump',
            namespace=namespace, timeout=timeout)
        return output

    def osd_crush_remove(self, osdid_str, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'ceph osd crush rm ' + osdid_str,
            namespace=namespace, timeout=timeout)
        return output

    def osd_crush_rule_dump(self, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli('ceph osd crush rule dump',
            namespace=namespace, timeout=timeout)
        return output

    def mon_remove(self, mon_id, namespace=None, timeout=None):
        return self.ceph_op.remove_dedicated_ceph_mon(self, mon_id,
            namespace=namespace)

    def get_tiers_size(self, namespace=None, timeout=None):
        output = self.osd_df(self, timeout=timeout, namespace=namespace)
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

    def osd_pool_get_quota(self, pool, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'ceph osd pool get-quota '+pool,
            namespace=namespace, timeout=timeout)
        return output

    def osd_pool_set_quota(self, pool, field, val, namespace=None,
        timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'ceph osd pool set-quota '+pool+' '+field+' '+val,
            namespace=namespace, timeout=timeout)
        return output

    def osd_pool_get(self, pool, var, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'ceph osd pool get '+pool+' ' + var,
            namespace=namespace, timeout=timeout)
        if not output:
            return None
        return output[var]

    def auth_del(self, osdid_str, namespace=None, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            'ceph auth del '+osdid_str,
            namespace=namespace, timeout=timeout)
        return output

class ConfigDomain(enum.Enum):
    glb = 0
    clt_adm = 1
    mon = 2
    osd = 3

class CephConfigOperator(object):
    def __init__(self):
        titles = {}

    def initial_domain(self, domain):
        titles[ConfigDomain.glb] =      '[global]'
        titles[ConfigDomain.clt_adm] =  '[client.admin]'
        titles[ConfigDomain.mon] =      '[mon]'
        titles[ConfigDomain.osd] =      '[osd]'

    def build_mon_host(self, mons, namespace=None):
        mon_config = self.titles[ConfigDomain.mon]
        mon_config += '\nmon host = '
        for key in mons:
            mon_config += key
            mon_config += ','
        mon_config.strip(',')
        mon_config += '\nmon addr = '
        for value in mons.itervalues():
            mon_config += value
            mon_config += ","
        return mon_config.strip(',')

    def build_configmap_mon_endpoints_data(self, mons):
        mon_data = ''
        for key,value in mons.items():
            mon_data += key
            mon_data += '='
            mon_data += value
            mon_data += ','
        return mon_data.strip(',')

    def build_configmap_mon_endpoints_mapping(self, mons, map_dict):
        for key_m in list(map_dict["node"]):
            if key_m not in mons:
                map_dict["node"].pop(key_m)
        return map_dict


class RookCephOperator(rook.RookOperator):

    def __init__(self):
        self.name = 'python-rookclient-ceph'
        self.kube_op = api.KubeOperator()
        self.cfg_op = CephConfigOperator()

    def execute_toolbox_cli(self, cli, sure=False, format='json',
        namespace=None, timeout=None):
        pod = self.kube_op.command_find_pod('rook-ceph-tools',
            namespace='rook-ceph')
        if not pod:
            print("Error when get pod rook-ceph-tools.")
            return None

        full_cli = cli
        if sure:
            full_cli += ' --yes-i-really-really-mean-it'
        if format == 'json':
            full_cli += ' --format json-pretty'

        output = self.kube_op.command_execute_cli(pod, full_cli, namespace, timeout)
        return output

    def get_rook_mon_count(self, namespace=None):
        objects = self.kube_op.command_get('CephCluster', 'rook-ceph',
            namespace)
        return self.kube_op.get_object_value(objects, 'spec.mon.count')

    def get_rook_mon_list(self, namespace=None):
        objects = self.kube_op.command_get('configmap',
            'rook-ceph-mon-endpoints', namespace)
        mon_data = self.kube_op.get_object_value(objects, 'data.data')
        mon_list = []
        mon_dict = {}

        if not mon_data:
            return mon_dict

        for item in mon_data.split(','):
            mon_list = item.split('=')
            mon_dict[mon_list[0]] = mon_list[1]
        #print(mon_dict)
        return mon_dict

    def modify_rook_mon_count(self, count, namespace=None, timeout=None):
        self.kube_op.override_resource('CephCluster', 'rook-ceph',
            'spec.mon.count', count, namespace, timeout=timeout)

    def add_dedicated_ceph_mon(self, mon_id, endpoint, namespace=None,
        timeout=None):
        mons = self.get_rook_mon_list(namespace)
        if mon_id in mons.keys() == True:
            print("Error when add dedicated mon: mon_id:%s existed." %mon_id)
            return

        # Clean the overrides values after the setting finished
        #self.kube_op.override_resource(namespace, 'configmap',
        #    'rook-config-override', 'data.config', '')
        #time.sleep(5)
        '''
        mons[mon_id] = endpoint
        mons_hosts = self.config_pa.build_mon_host(ConfigDomain.glob, mons,
            namespace)
        self.kube_op.override_resource(namespace, 'configmap',
            'rook-config-override', 'data.config', mons_hosts)
        '''

        mon_count = self.get_rook_mon_count(namespace)
        if mon_count >= 1 and mon_count < 3:
            mon_count += 1
            self.modify_rook_mon_count(mon_count, namespace, timeout)


    def remove_dedicated_ceph_mon(self, mon_id, namespace=None, timeout=None):
        # remove the ceph monitor with id
        mons = self.get_rook_mon_list(namespace)
        if mon_id not in mons:
            print("Error when remove dedicated mon: mon_id:%s cannot find." %mon_id)
            return False
        
        mons.pop(mon_id)

        mons_hosts = self.cfg_op.build_configmap_mon_endpoints_data(mons)
        self.kube_op.override_resource('configmap', 'rook-ceph-mon-endpoints',
            'data.data', mons_hosts, namespace)
        
        mon_mapping = self.kube_op.fetch_resource('configmap',
            'rook-ceph-mon-endpoints', 'data.mapping', namespace)
        mon_dict = json.loads(mon_mapping)
        mon_dict = self.cfg_op.build_configmap_mon_endpoints_mapping(mons,
            mon_dict)
        mon_mapping = json.dumps(mon_dict)
        self.kube_op.override_resource('configmap', 'rook-ceph-mon-endpoints',
            'data.mapping', mon_mapping, namespace)

        mon_count = self.get_rook_mon_count(namespace)
        if mon_count > 1 and mon_count <= 3:
            mon_count -= 1
            self.modify_rook_mon_count(mon_count, namespace, timeout)
        '''
        self.execute_toolbox_cli('ceph mon rm %s'%mon_id, namespace=namespace)

        objects = self.kube_op.command_get('CephCluster', 'rook-ceph', namespace)
        rookDirPath = self.kube_op.get_object_value(objects,
            'spec.dataDirHostPath')
        shutil.rmtree(rookDirPath+"/mon-"+mon_id)

        pod = self.kube_op.command_find_pod('rook-ceph-mon', 'ceph_daemon_id',
            mon_id, 'rook-ceph')
        if not pod:
            print("Error when get pod rook-ceph-mon.")
            return
        self.kube_op.command_delete('pod', pod, 'rook-ceph')
        '''
        return True

