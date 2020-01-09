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

import six
import enum
import time
import sys
import shutil
import json
import ceph as ceph


class RookCephApi(object):
    def __init__(self, namespace):
        self.ceph_op = ceph.RookCephOperator(namespace)
        self.is_ready = False

    '''
    Get Interfaces, all the get interface can be implemented by toolbox CLI.
    '''

    def status(self, timeout=None):
        status = self.ceph_op.execute_toolbox_cli(['status'],
            timeout=timeout)
        return status

    def health(self, detail=None, timeout=None):
        cli = ['health']
        if detail:
            cli.append('detail')
        health = self.ceph_op.execute_toolbox_cli(cli,
            timeout=timeout)
        return health

    def ceph_status(self, timeout=None):
        status = self.ceph_op.execute_toolbox_cli(['health'],
            timeout=timeout)
        return status

    def ceph_health(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(['health'],
            timeout=timeout)
        health = self.ceph_op.kube_op.get_object_value(output, 'status')
        return health

    def fsid(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(['fsid'],
            timeout=timeout)
        fsid = self.ceph_op.kube_op.get_object_value(output, 'fsid')
        return fsid

    def ceph_df(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(['df'], 
            timeout=timeout)
        return output

    def osd_df(self, output_method='tree', timeout=None):
        output = self.ceph_op.execute_toolbox_cli(['osd', 'df', output_method],
            timeout=timeout)
        return output

    def osd_stat(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(['osd', 'stat'],
            timeout=timeout)
        return output

    def osd_tree(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(['osd', 'tree'],
            timeout=timeout)
        return output

    def osd_pool_ls(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(['osd', 'pool', 'ls'],
            timeout=timeout)
        return output

    def osd_crush_dump(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(['osd', 'crush', 'dump'],
            timeout=timeout)
        return output

    def osd_crush_rule_dump(self, rule_name=None, timeout=None):
        cli = ['osd', 'crush', 'rule', 'dump']
        if rule_name:
            cli.append(rule_name)
        output = self.ceph_op.execute_toolbox_cli(cli, timeout=timeout)
        return output

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
            ['osd', 'pool', 'get-quota', pool],
            timeout=timeout)
        return output

    def osd_pool_get(self, pool, var, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'pool', 'get', pool, var],
            timeout=timeout)
        if not output:
            return None
        return output[var]

    def osd_crush_rule_ls(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'crush', 'rule', 'ls'],
            timeout=timeout)
        return output

    def osd_crush_tree(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'crush', 'tree'],
            timeout=timeout)
        return output

    def quorum_status(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['quorum_status'],
            timeout=timeout)
        return output

    #def pg_dump_stuck(self, stuckops=None, threshold=None, timeout=None):
    def pg_dump_stuck(self, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['pg', 'dump_stuck'],
            timeout=timeout)
        return output

    def _osd_crush_rule_by_ruleset(self, ruleset, timeout=None):
        output = self.osd_crush_rule_dump(timeout=timeout)
        name = None

        for rule in output:
            if rule.get('ruleset') == ruleset:
                name = rule.get('rule_name')
        output = dict(rule=name)
        return output

    '''
    Set Interfaces
    '''

    # CRD/configmap override
    def mon_remove(self, mon_id, timeout=None):
        return self.ceph_op.remove_dedicated_ceph_mon(mon_id, timeout)

    def _sanitize_osdid_to_int(self, _id):
        if isinstance(_id, six.string_types):
            prefix = 'osd.'
            if _id.startswith(prefix):
                _id = _id[len(prefix):]
            try:
                _id = int(_id)
            except ValueError:
                raise ApiError
        elif not isinstance(_id, six.integer_types):
            raise ApiError
        return _id
    # ?
    def osd_create(self, uuid=None, params=None, timeout=None):
        cli = ['osd', 'create']
        if uuid:
            cli.append(str(uuid))
        if params:
            cli.append(self._sanitize_osdid_to_int(params['id']))
        output = self.ceph_op.execute_toolbox_cli(cli, timeout=timeout)
        return output

    # Toolbox CLI ?
    # ceph osd out <ID>
    # ceph osd crush remove osd.<ID>
    # ceph auth del osd.<ID>
    # ceph osd rm <ID> (actually cannot remove since osd is up)
    def osd_remove(self, ids, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'out', str(ids)],
            timeout=timeout)
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'rm', str(ids)],
            timeout=timeout)
        return output

    # Toolbox CLI but pod error
    def osd_down(self, ids, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'down', str(ids)],
            timeout=timeout)
        return output

    # Toolbox CLI
    def osd_pool_create(self, pool, pg_num, pgp_num=None, pool_type=None,
                        erasure_code_profile=None, ruleset=None,
                        expected_num_objects=None, timeout=None):
        crush_rule = self._osd_crush_rule_by_ruleset(ruleset)
        if crush_rule is None:
            print("Create OSD pool failed with empty rule.")
            return None

        rule = crush_rule['rule']
        cli = ['osd', 'pool', 'create', pool, str(pg_num)]
        if pgp_num is not None:
            cli.append(str(pgp_num))
        if pool_type is not None:
            cli.append(pool_type)
        if erasure_code_profile is not None:
            cli.append(erasure_code_profile)
        if rule is not None:
            cli.append(rule)
        if expected_num_objects is not None:
            cli.append(str(expected_num_objects))
        output = self.ceph_op.execute_toolbox_cli(cli, timeout=timeout)
        return output

    # Toolbox CLI
    def osd_pool_delete(self, pool, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'pool', 'delete', pool, pool],
            sure=True, timeout=timeout)
        return output

    OSD_POOL_SET_VAR_VALUES = \
        ['size', 'min_size', 'pg_num', 'pgp_num',
         'crush_rule', 'hashpspool', 'nodelete',
         'nopgchange', 'nosizechange',
         'write_fadvise_dontneed', 'noscrub',
         'nodeep-scrub', 'hit_set_type',
         'hit_set_period', 'hit_set_count',
         'hit_set_fpp', 'use_gmt_hitset',
         'target_max_bytes', 'target_max_objects',
         'cache_target_dirty_ratio',
         'cache_target_dirty_high_ratio',
         'cache_target_full_ratio',
         'cache_min_flush_age', 'cache_min_evict_age',
         'auid', 'min_read_recency_for_promote',
         'min_write_recency_for_promote', 'fast_read',
         'hit_set_grade_decay_rate',
         'hit_set_search_last_n', 'scrub_min_interval',
         'scrub_max_interval', 'deep_scrub_interval',
         'recovery_priority', 'recovery_op_priority',
         'scrub_priority', 'compression_mode',
         'compression_algorithm',
         'compression_required_ratio',
         'compression_max_blob_size',
         'compression_min_blob_size', 'csum_type',
         'csum_min_block', 'csum_max_block',
         'allow_ec_overwrites']

    def osd_pool_set(self, pool, var, val, force=None, timeout=None):
        supported = RookCephApi.OSD_POOL_SET_VAR_VALUES
        if var not in supported:
            print("Invalid Choice in OSD pool setting.")
            return None

        if force is None:
            sure = False
        else:
            sure = True

        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'pool', 'set', pool, var, str(val)], sure=sure,
            timeout=timeout)
        

    def osd_pool_set_param(self, pool, var, val, force=None, timeout=None):
        if var == 'crush_ruleset':
            var = 'crush_rule'
            crush_rule = self._osd_crush_rule_by_ruleset(val, timeout=timeout)
            if crush_rule is None:
                return None
            val = crush_rule['rule']
        return self.osd_pool_set(pool, var, val, force=force,
            timeout=timeout)


    # Toolbox CLI
    def osd_pool_set_quota(self, pool, field, val, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'pool', 'set-quota', pool, field, val],
            timeout=timeout)
        return output

    def auth_get_or_create(self, entity, caps=None, timeout=None):
        cli = ['auth', 'get-or-create', entity]
        if caps:
            cli.append(caps)
        output = self.ceph_op.execute_toolbox_cli(cli, timeout=timeout)
        return output

    # Toolbox CLI
    def auth_del(self, osdid_str, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['auth', 'del', osdid_str],
            timeout=timeout)
        return output

    # Toolbox CLI
    def osd_crush_remove(self, osdid_str, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'crush', 'rm', osdid_str],
            timeout=timeout)
        return output

    # Toolbox CLI
    # ceph osd crush move osd.1 host=controller-0
    def osd_crush_move(self, name, args, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'crush', 'move', name, args],
            timeout=timeout)
        return output

    # Toolbox CLI
    # ceph osd crush rule create-replicated replicated_rule default host
    def osd_crush_rule_rm(self, name, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'crush', 'rule', 'rm', name],
            timeout=timeout)
        return output

    # Toolbox CLI
    def osd_crush_rule_rename(self, srcname, dstname, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'crush', 'rule', 'rename', srcname, dstname],
            timeout=timeout)
        return output

    # Toolbox CLI
    def osd_crush_add_bucket(self, name, _type, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'crush', 'add-bucket', name, _type],
            timeout=timeout)
        return output

    # Toolbox CLI
    def osd_crush_rename_bucket(self, srcname, dstname, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'crush', 'rename-bucket', srcname, dstname],
            timeout=timeout)
        return output


    '''
    CRUSHMAP get/set/dump/compile
    '''

    def osd_crushmap_get(self, crushmap_bin_file, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'getcrushmap', '-o', crushmap_bin_file],
            timeout=timeout)
        return output

    def osd_crushmap_set(self, crushmap_bin_file, timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['osd', 'setcrushmap', '-i', crushmap_bin_file],
            timeout=timeout)
        return output

    def osd_crushmap_compile(self, crushmap_txt_file, crushmap_bin_file,
                             timeout=None):
        output = self.ceph_op.execute_toolbox_cli(
            ['crushtool', '-c', crushmap_txt_file, '-o', crushmap_bin_file],
            ceph_bin=False, timeout=timeout)
        return output

    def osd_crushmap_decompile(self, crushmap_bin_file, crushmap_txt_file,
                               timeout=None):

        output = self.ceph_op.execute_toolbox_cli(
            ['crushtool', '-d', crushmap_bin_file, '-o', crushmap_txt_file],
            ceph_bin=False, timeout=timeout)
        return output
