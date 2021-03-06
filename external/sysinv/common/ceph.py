# vim: tabstop=4 shiftwidth=4 softtabstop=4

#
# Copyright (c) 2016, 2019 Wind River Systems, Inc.
#
# SPDX-License-Identifier: Apache-2.0
#
# All Rights Reserved.
#

""" System Inventory Ceph Utilities and helper functions."""

from __future__ import absolute_import

#from eventlet.green import subprocess
import os
import pecan
import requests
import shutil
import tempfile

#from cephclient import wrapper as ceph
from rookclient import ceph_api as rook_ceph
from requests.exceptions import ReadTimeout
from contextlib import contextmanager

from oslo_log import log as logging
from sysinv.common import constants
from sysinv.common import exception
from sysinv.common import utils as cutils

LOG = logging.getLogger(__name__)


class CephApiOperator(object):
    """Class to encapsulate Ceph operations for System Inventory API
       Methods on object-based storage devices (OSDs).
    """

    def __init__(self):
        self._ceph_api = rook_ceph.RookCephApi(
            constants.K8S_ROOK_CEPH_NAMESPACE_DEFAULT)
        self._default_tier = constants.SB_TIER_DEFAULT_NAMES[
            constants.SB_TIER_TYPE_CEPH]

    def _format_root_name(self, name):
        """Generate normalized crushmap root name. """

        if name.endswith(constants.CEPH_CRUSH_TIER_SUFFIX):
            return name
        return name + constants.CEPH_CRUSH_TIER_SUFFIX

    @staticmethod
    def _format_rule_name(name):
        return "{0}{1}{2}".format(
            name, constants.CEPH_CRUSH_TIER_SUFFIX,
            "-ruleset").replace('-', '_')

    def _crush_rule_status(self, tier_name):
        present = False

        LOG.info("ceph osd crush rule ls")
        try:
            output = self._ceph_api.osd_crush_rule_ls()
        except Exception as e:
            LOG.error("Exception in ls crush rule: {}".format(e))
            return (False, '', 0)

        name = (tier_name + "-ruleset").replace('-', '_')
        if name in output:
            present = True

        return (present, name, len(output))

    def _crush_bucket_add(self, bucket_name, bucket_type):
        try:
            output = self._ceph_api.osd_crush_add_bucket(bucket_name, bucket_type)
        except Exception as e:
            LOG.error("Exception in adding crush bucket: {}".format(e))

        LOG.info("ceph osd crush add-bucket %s %s" % (bucket_name, bucket_type))

    def _crush_bucket_remove(self, bucket_name):
        try:
            output = self._ceph_api.osd_crush_remove(bucket_name)
        except Exception as e:
            LOG.error("Exception in removing crush bucket: {}".format(e))

        LOG.info("ceph osd crush remove %s" % bucket_name)

    def _crush_bucket_move(self, bucket_name, ancestor_type, ancestor_name):
        try:
            output = self._ceph_api.osd_crush_move(
                bucket_name, "%s=%s" % (ancestor_type, ancestor_name))
        except Exception as e:
            LOG.error("Exception in moving crush bucket: {}".format(e))
        LOG.info("ceph osd crush move %s %s=%s" % (bucket_name, ancestor_type,
                                                   ancestor_name))

    def _crushmap_item_create(self, items, name, ancestor_name=None,
                              ancestor_type=None, depth=0):
        """Create crush map entry. """

        # This is a recursive method. Add a safeguard to prevent infinite
        # recursion.
        if depth > constants.CEPH_CRUSH_MAP_DEPTH:
            raise exception.CephCrushMaxRecursion(depth=depth)

        root_name = self._format_root_name(name)

        for i in items:
            bucket_name = (root_name
                           if i['type'] == 'root'
                           else '%s-%s' % (i['name'], name))
            if i['type'] != 'osd':
                LOG.error("bucket_name = %s, depth = %d" % (bucket_name, depth))
                self._crush_bucket_add(bucket_name, i['type'])

                if 'items' in i:
                    self._crushmap_item_create(i['items'], name,
                                               ancestor_name=bucket_name,
                                               ancestor_type=i['type'],
                                               depth=depth + 1)

            if ancestor_type:
                if i['type'] != 'osd':
                    self._crush_bucket_move(bucket_name, ancestor_type,
                                            ancestor_name)

    def _crushmap_item_delete(self, items, name, ancestor_name=None,
                              ancestor_type=None, depth=0, rollback=False):
        """Delete a crush map entry. """

        # This is a recursive method. Add a safeguard to to prevent infinite
        # recursion.
        if depth > constants.CEPH_CRUSH_MAP_DEPTH:
            return depth

        root_name = self._format_root_name(name)
        ret_code = 0

        for i in items:
            if rollback:
                bucket_name = (root_name
                               if i['type'] == 'root'
                               else '%s-%s' % (i['name'], name))
            else:
                bucket_name = root_name if i['type'] == 'root' else i['name']

            if 'items' in i:
                ret_code = self._crushmap_item_delete(i['items'], name,
                                                      ancestor_name=bucket_name,
                                                      ancestor_type=i['type'],
                                                      depth=depth + 1,
                                                      rollback=rollback)

            LOG.error("bucket_name = %s, depth = %d, ret_code = %s" % (
                bucket_name, depth, ret_code))
            self._crush_bucket_remove(bucket_name)

        if ret_code != 0 and depth == 0:
            raise exception.CephCrushMaxRecursion(depth=ret_code)

        return (ret_code if ret_code else 0)

    def _crushmap_root_mirror(self, src_name, dest_name):
        """Create a new root hierarchy that matches an existing root hierarchy.
        """

        # Nomenclature for mirrored tiers:
        # root XXX-tier
        #     chassis group-0-XXX
        #         host storage-0-XXX
        #         host storage-1-XXX
        src_root_name = self._format_root_name(src_name)
        dest_root_name = self._format_root_name(dest_name)

        # currently prevent mirroring of anything other than the source tier
        default_root_name = self._format_root_name(self._default_tier)
        if src_root_name != default_root_name:
            reason = "Can only mirror '%s'." % default_root_name
            raise exception.CephCrushInvalidTierUse(tier=src_name,
                                                    reason=reason)

        try:
            output = self._ceph_api.osd_crush_tree()
        except Exception as e:
            LOG.error("Exception in getting osd tree: {}".format(e))
        
        # Scan for the destination root, should not be present
        dest_root = [r for r in output if r['name'] == dest_root_name]
        if dest_root:
            raise exception.CephCrushTierAlreadyExists(tier=dest_root_name)

        src_root = [r for r in output if r['name'] == src_root_name]
        if not src_root:
            reason = ("The required source root '%s' does not exist." %
                      src_root_name)
            raise exception.CephCrushInvalidTierUse(tier=src_root_name,
                                                    reason=reason)

        # Mirror the root hierarchy
        LOG.info("Mirroring crush root for new tier: src = %s, dest = %s" %
                 (src_root_name, dest_root_name))
        try:
            self._crushmap_item_create(src_root, dest_name)
        except exception.CephCrushMaxRecursion:
            LOG.error("Unexpected recursion level seen while mirroring "
                      "crushmap hierarchy. Rolling back crushmap changes")
            self._crushmap_item_delete(src_root, dest_name, rollback=True)

    def _crushmap_root_delete(self, name):
        """Remove the crushmap root entry. """

        default_root_name = self._format_root_name(self._default_tier)
        root_name = self._format_root_name(name)
        if root_name == default_root_name:
            reason = "Cannot remove tier '%s'." % default_root_name
            raise exception.CephCrushInvalidTierUse(tier=name, reason=reason)

        try:
            output = self._ceph_api.osd_crush_tree()
        except Exception as e:
            LOG.error("Exception in getting osd tree: {}".format(e))
        # Scan for the destinaion root, should not be present
        root = [r for r in output if r['name'] == root_name]

        if not root:
            reason = "The crushmap root '%s' does not exist." % root_name
            raise exception.CephCrushInvalidTierUse(tier=name,
                                                    reason=reason)

        # Delete the root hierarchy
        try:
            self._crushmap_item_delete(root, name)
        except exception.CephCrushMaxRecursion:
            LOG.debug("Unexpected recursion level seen while deleting "
                      "crushmap hierarchy")

    def _insert_crush_rule(self, file_contents, root_name, rule_name, rule_count,
                           replicate_by='host'):
        """ Insert a new crush rule for a new storage tier.
        Valid replicate_by options are 'host' or 'osd'.
        """

        # generate rule
        rule = [
            "rule %s {\n" % rule_name,
            "    ruleset %d\n" % int(rule_count + 1),
            "    type replicated\n",
            "    min_size 1\n",
            "    max_size 10\n",
            "    step take %s\n" % root_name,
            "    step choose firstn 1 type chassis\n",
            "    step chooseleaf firstn 0 type %s\n" % replicate_by,
            "    step emit\n",
            "}\n"
        ]

        # insert rule: maintain comment at the end of the crushmap
        insertion_index = len(file_contents) - 1
        for l in reversed(rule):
            file_contents.insert(insertion_index, l)

    def _crushmap_rule_add(self, tier, replicate_by):
        """Add a tier crushmap rule."""

        crushmap_flag_file = os.path.join(constants.SYSINV_CONFIG_PATH,
                                          constants.CEPH_CRUSH_MAP_APPLIED)
        if not os.path.isfile(crushmap_flag_file):
            reason = "Cannot add any additional rules."
            raise exception.CephCrushMapNotApplied(reason=reason)

        default_root_name = self._format_root_name(self._default_tier)
        root_name = self._format_root_name(tier)
        if root_name == default_root_name:
            raise exception.CephCrushRuleAlreadyExists(
                tier=tier, rule='default')

        # get the current rule count
        rule_is_present, rule_name, rule_count = self._crush_rule_status(root_name)
        if rule_is_present:
            raise exception.CephCrushRuleAlreadyExists(
                tier=tier, rule=rule_name)

        # NOTE: The Ceph API only supports simple single step rule creation.
        # Because of this we need to update the crushmap the hard way.

        tmp_crushmap_bin_file = "/etc/sysinv/crushmap_rule_update.bin"
        tmp_crushmap_txt_file = "/etc/sysinv/crushmap_rule_update.txt"

        # Extract the crushmap
        self.get_osd_crushmap(tmp_crushmap_bin_file)

        if os.path.exists(tmp_crushmap_bin_file):
            # Decompile the crushmap
            self.decompile_osd_crushmap(tmp_crushmap_bin_file,
                                        tmp_crushmap_txt_file)

            if os.path.exists(tmp_crushmap_txt_file):
                # Add the custom rule
                with open(tmp_crushmap_txt_file, 'r') as fp:
                    contents = fp.readlines()

                self._insert_crush_rule(contents, root_name,
                                        rule_name, rule_count, replicate_by)

                with open(tmp_crushmap_txt_file, 'w') as fp:
                    contents = "".join(contents)
                    fp.write(contents)

                # Compile the crush map
                self.compile_osd_crushmap(tmp_crushmap_txt_file,
                                          tmp_crushmap_bin_file)

                # Load the new crushmap
                LOG.info("Loading updated crushmap with elements for "
                         "crushmap root: %s" % root_name)
                self.set_osd_crushmap(tmp_crushmap_bin_file)

        # cleanup
        if os.path.exists(tmp_crushmap_txt_file):
            os.remove(tmp_crushmap_txt_file)
        if os.path.exists(tmp_crushmap_bin_file):
            os.remove(tmp_crushmap_bin_file)

    def _crushmap_rule_delete(self, name):
        """Delete existing tier crushmap rule. """

        crushmap_flag_file = os.path.join(constants.SYSINV_CONFIG_PATH,
                                          constants.CEPH_CRUSH_MAP_APPLIED)
        if not os.path.isfile(crushmap_flag_file):
            reason = "Cannot remove any additional rules."
            raise exception.CephCrushMapNotApplied(reason=reason)

        default_root_name = self._format_root_name(self._default_tier)
        root_name = self._format_root_name(name)
        if root_name == default_root_name:
            reason = (("Cannot remove the rule for tier '%s'.") %
                      default_root_name)
            raise exception.CephCrushInvalidTierUse(tier=name,
                                                    reason=reason)

        # get the current rule count
        rule_is_present, rule_name, rule_count = self._crush_rule_status(root_name)

        if not rule_is_present:
            reason = (("Rule '%s' is not present in the crushmap. No action "
                       "taken.") % rule_name)
            raise exception.CephCrushInvalidRuleOperation(rule=rule_name,
                                                          reason=reason)

        try:
            output = self._ceph_api.osd_crush_rule_rm(rule_name)
        except Exception as e:
            LOG.error("Exception in removing crush rule: {}".format(e))
        LOG.info("ceph osd crush rule rm %s" % rule_name)

    def crushmap_tier_delete(self, name):
        """Delete a custom storage tier to the crushmap. """

        try:
            # First: Delete the custom ruleset
            self._crushmap_rule_delete(name)
        except exception.CephCrushInvalidRuleOperation as e:
            if 'not present' not in str(e):
                raise e

        try:
            # Second: Delete the custom tier
            self._crushmap_root_delete(name)
        except exception.CephCrushInvalidTierUse as e:
            if 'does not exist' not in str(e):
                raise e
        except exception.CephCrushMaxRecursion as e:
            raise e

    def _crushmap_add_tier(self, tier):
        # create crush map tree for tier mirroring default root
        try:
            self._crushmap_root_mirror(self._default_tier, tier.name)
        except exception.CephCrushTierAlreadyExists:
            pass
        if cutils.is_aio_simplex_system(pecan.request.dbapi):
            # Since we have a single host replication is done on OSDs
            # to ensure disk based redundancy.
            replicate_by = 'osd'
        else:
            # Replication is done on different nodes of the same peer
            # group ensuring host based redundancy.
            replicate_by = 'host'
        try:
            self._crushmap_rule_add(tier.name, replicate_by=replicate_by)
        except exception.CephCrushRuleAlreadyExists:
            pass

    def crushmap_tiers_add(self):
        """Add all custom storage tiers to the crushmap. """

        cluster = pecan.request.dbapi.clusters_get_all(
            name=constants.CLUSTER_CEPH_DEFAULT_NAME)
        tiers = pecan.request.dbapi.storage_tier_get_by_cluster(
            cluster[0].uuid)

        for t in tiers:
            if t.type != constants.SB_TIER_TYPE_CEPH:
                continue
            if t.name == self._default_tier:
                continue
            self._crushmap_add_tier(t)

    def _crushmap_tiers_bucket_add(self, bucket_name, bucket_type):
        """Add a new bucket to all the tiers in the crushmap. """

        ceph_cluster_name = constants.CLUSTER_CEPH_DEFAULT_NAME
        cluster = pecan.request.dbapi.clusters_get_all(name=ceph_cluster_name)
        tiers = pecan.request.dbapi.storage_tier_get_by_cluster(
            cluster[0].uuid)
        for t in tiers:
            if t.type == constants.SB_TIER_TYPE_CEPH:
                if t.name == self._default_tier:
                    self._crush_bucket_add(bucket_name, bucket_type)
                else:
                    self._crush_bucket_add("%s-%s" % (bucket_name, t.name),
                                           bucket_type)

    def _crushmap_tiers_bucket_remove(self, bucket_name):
        """Remove an existing bucket from all the tiers in the crushmap. """

        ceph_cluster_name = constants.CLUSTER_CEPH_DEFAULT_NAME
        cluster = pecan.request.dbapi.clusters_get_all(name=ceph_cluster_name)
        tiers = pecan.request.dbapi.storage_tier_get_by_cluster(
            cluster[0].uuid)
        for t in tiers:
            if t.type == constants.SB_TIER_TYPE_CEPH:
                if t.name == self._default_tier:
                    self._crush_bucket_remove(bucket_name)
                else:
                    self._crush_bucket_remove(
                        "%s-%s" % (bucket_name, t.name))

    def _crushmap_tiers_bucket_move(self, bucket_name, ancestor_type,
                                    ancestor_name):
        """Move common bucket in all the tiers in the crushmap. """

        ceph_cluster_name = constants.CLUSTER_CEPH_DEFAULT_NAME
        cluster = pecan.request.dbapi.clusters_get_all(name=ceph_cluster_name)
        tiers = pecan.request.dbapi.storage_tier_get_by_cluster(
            cluster[0].uuid)
        for t in tiers:
            if t.type == constants.SB_TIER_TYPE_CEPH:

                if t.name == self._default_tier:
                    ancestor_name = (self._format_root_name(ancestor_name)
                                     if ancestor_type == 'root'
                                     else ancestor_name)

                    self._crush_bucket_move(bucket_name, ancestor_type,
                                            ancestor_name)
                else:

                    ancestor_name = (self._format_root_name(t.name)
                                     if ancestor_type == 'root'
                                     else "%s-%s" % (ancestor_name, t.name))

                    self._crush_bucket_move(
                        "%s-%s" % (bucket_name, t.name),
                        ancestor_type,
                        ancestor_name)

    def _crushmap_tier_rename(self, old_name, new_name):
        old_root_name = self._format_root_name(old_name)
        new_root_name = self._format_root_name(new_name)
        try:
            output = self._ceph_api.osd_crush_dump()
        except Exception as e:
            LOG.error("Exception in dump osd crush: {}".format(e))
        # build map of buckets to be renamed
        rename_map = {}
        for buck in output['buckets']:
            name = buck['name']
            if buck['type_name'] == 'root':
                if name == old_root_name:
                    rename_map[name] = new_root_name
            else:
                old_suffix = '-{}'.format(old_name)
                new_suffix = '-{}'.format(new_name)
                if name.endswith(old_suffix):
                    rename_map[name] = name[:-len(old_suffix)] + new_suffix
        conflicts = set(b['name'] for b in output['buckets']) \
            .intersection(set(rename_map.values()))
        if conflicts:
            raise exception.CephCrushTierRenameFailure(
                tier=old_name, reason=(
                    "Target buckets already exist: %s"
                    % ', '.join(conflicts)))
        old_rule_name = self._format_rule_name(old_name)
        new_rule_name = self._format_rule_name(new_name)
        try:
            output = self._ceph_api.osd_crush_rule_dump(new_rule_name)
        except Exception as e:
            LOG.error("Exception in dump osd crush rule: {}".format(e))
        for _from, _to in rename_map.items():
            try:
                output = self._ceph_api.osd_crush_rename_bucket(_from, _to)
            except Exception as e:
                LOG.error("Exception in dump osd crush: {}".format(e))
            LOG.info("Rename bucket from '%s' to '%s'", _from, _to)
        try:
            output = self._ceph_api.osd_crush_rule_rename(
                old_rule_name, new_rule_name)
        except Exception as e:
            LOG.error("Exception in rename osd crush rule: {}".format(e))
        LOG.info("Rename crush rule from '%s' to '%s'",
                 old_rule_name, new_rule_name)

    def crushmap_tier_rename(self, old_name, new_name):
        with self.safe_crushmap_update():
            self._crushmap_tier_rename(old_name, new_name)

    '''
    @contextmanager
    def safe_crushmap_update(self):
        with open(os.devnull, 'w') as fnull, tempfile.TemporaryFile() as backup:
            LOG.info("Saving crushmap for safe update")
            try:
                subprocess.check_call(
                    "ceph osd getcrushmap",
                    stdin=fnull, stdout=backup, stderr=fnull,
                    shell=True)
            except subprocess.CalledProcessError as exc:
                raise exception.CephFailure(
                    "failed to backup crushmap: %s" % str(exc))
            try:
                yield
            except exception.CephFailure:
                backup.seek(0, os.SEEK_SET)
                LOG.warn("Crushmap update failed. Restoring from backup")
                subprocess.call(
                    "ceph osd setcrushmap",
                    stdin=backup, stdout=fnull, stderr=fnull,
                    shell=True)
                raise
        '''

    def ceph_status_ok(self, timeout=10):
        """
            returns rc bool. True if ceph ok, False otherwise
            :param timeout: ceph api timeout
        """
        rc = True

        try:
            output = self._ceph_api.status(timeout=timeout)
            ceph_status = output['health']['status']
            if ceph_status != constants.CEPH_HEALTH_OK:
                LOG.warn("ceph status=%s " % ceph_status)
                rc = False
        except Exception as e:
            rc = False
            LOG.warn("ceph status exception: %s " % e)

        return rc

    def get_osd_stats(self, timeout=30):
        try:
            output = self._ceph_api.osd_stat(timeout=timeout)
        except Exception as e:
            LOG.error("Exception in getting osd stats: {}".format(e))
        return output

    def _osd_quorum_names(self, timeout=10):
        quorum_names = []
        try:
            output = self._ceph_api.quorum_status(timeout=timeout)
        except Exception as e:
            LOG.error("Exception in quorum status: {}".format(e))
    
        quorum_names = output['quorum_names']
        return quorum_names

    def remove_osd_key(self, osdid):
        osdid_str = "osd." + str(osdid)
        # Remove the OSD authentication key
        try:
            output = self._ceph_api.auth_del(osdid_str)
        except Exception as e:
            LOG.error("Exception in deleting auth: {}".format(e))

    def osd_host_lookup(self, osd_id):
        try:
            output = self._ceph_api.osd_crush_tree()
        except Exception as e:
            LOG.error("Exception in tree osd crush: {}".format(e))
        for i in range(0, len(output)):
            # there are 2 chassis lists - cache-tier and root-tier
            # that can be seen in the output of 'ceph osd crush tree':
            # [{"id": -2,"name": "cache-tier", "type": "root",
            # "type_id": 10, "items": [...]},
            # {"id": -1,"name": "storage-tier","type": "root",
            # "type_id": 10, "items": [...]}]
            chassis_list = output[i]['items']
            for chassis in chassis_list:
                # extract storage list/per chassis
                storage_list = chassis['items']
                for storage in storage_list:
                    # extract osd list/per storage
                    storage_osd_list = storage['items']
                    for osd in storage_osd_list:
                        if osd['id'] == osd_id:
                            # return storage name where osd is located
                            return storage['name']
        return None

    def _identify_acting_osds(self, line):
        """
        Extract OSDs that act on a PG

        :param line: Input line from `ceph health detail`
        :return: The OSDs that are acting on a PG
        """
        import re
        osds = line.split('acting')[1]
        osds = [obj for obj in re.split(' |,|\[|]', osds) if obj]
        return osds

    def _identify_host_osds(self, osd_tree, target_host):
        """
        Extract OSDs on a host

        :param target_host: The host
        :param osd_tree: JSON of `ceph osd tree`
        :return: Set with OSDs id on host
        """
        import six
        osd_tree = dict([(n['id'], n) for n in osd_tree['nodes']])
        host_osds = set()

        for (_id, _node) in six.iteritems(osd_tree):
            if _node['type'] == 'host' and _node['name'] == target_host:
                for child_id in _node['children']:
                    host_osds.add(osd_tree[child_id]['name'][len('osd.'):])

        return host_osds

    def check_recovery_in_progress(self, target_host, timeout=10):
        """
        Check if ceph is recovering data

        :param target_host: Care only for OSDs on this host
        :param timeout: Ceph api timeout
        :return: True if ceph recovery in progress on target host
        """

        rc = False

        try:
            output = self._ceph_api.health(detail='detail', timeout=timeout)
        except Exception as e:
            rc = True
            LOG.warn("ceph health exception: %s " % e)
        health_detail = output

        try:
            output = self._ceph_api.osd_tree()
        except Exception as e:
            rc = True
            LOG.warn("ceph osd tree exception: %s " % e)
        osd_tree = output

        try:
            this_host_osds = self._identify_host_osds(osd_tree, target_host)

            for line in health_detail['checks']['PG_DEGRADED']['detail']:
                msg = line['message']
                if "recovering" in msg or \
                        "recovery_wait" in msg:
                    acting_osds = self._identify_acting_osds(msg)
                    for osd in acting_osds:
                        if osd in this_host_osds:
                            rc = True
                            break
                    if rc:
                        break

        except Exception as e:
            pass

        return rc

    def check_osds_down_up(self, hostname, upgrade):
        # check if osds from a storage are down/up
        try:
            output = self._ceph_api.osd_tree()
        except Exception as e:
            LOG.error("Exception in tree osd: {}".format(e))
        osd_tree = output['nodes']
        size = len(osd_tree)
        for i in range(1, size):
            if osd_tree[i]['type'] != "host":
                continue
            children_list = osd_tree[i]['children']
            children_num = len(children_list)
            # when we do a storage upgrade, storage node must be locked
            # and all the osds of that storage node must be down
            if (osd_tree[i]['name'] == hostname):
                for j in range(1, children_num + 1):
                    if (osd_tree[i + j]['type'] == constants.STOR_FUNCTION_OSD and
                       osd_tree[i + j]['status'] == "up"):
                        # at least one osd is not down
                        return False
                # all osds are up
                return True

    def host_crush_remove(self, hostname):
        # remove host from crushmap when system host-delete is executed
        try:
            output = self._ceph_api.osd_crush_remove(hostname)
        except Exception as e:
            LOG.error("Exception in removing osd crush: {}".format(e))

    def set_crushmap(self):
        if fix_crushmap():
            self.crushmap_tiers_add()

    def update_crushmap(self, hostupdate):
        self.set_crushmap()
        if hostupdate.ihost_patch.get('personality') != constants.STORAGE:
            return
        storage_num = int(hostupdate.ihost_orig['hostname'][8:])
        if (storage_num >= 2 and
                hostupdate.ihost_orig['invprovision'] !=
                constants.PROVISIONED):

            # update crushmap accordingly with the host and it's peer group
            node_bucket = hostupdate.ihost_orig['hostname']
            ipeer = pecan.request.dbapi.peer_get(
                hostupdate.ihost_orig['peer_id'])

            self._crushmap_tiers_bucket_add(node_bucket, "host")
            self._crushmap_tiers_bucket_add(ipeer.name, "chassis")
            self._crushmap_tiers_bucket_move(ipeer.name, "root", self._default_tier)
            self._crushmap_tiers_bucket_move(node_bucket, "chassis", ipeer.name)

    def host_osd_status(self, hostname):
        # should prevent locking of a host if HEALTH_BLOCK
        host_health = None
        try:
            output = self._ceph_api.pg_dump_stuck()
        except Exception as e:
            LOG.exception(e)
            return host_health
        pg_detail = len(output)
        # osd_list is a list where I add
        # each osd from pg_detail whose hostname
        # is not equal with hostnamge given as parameter
        osd_list = []
        for x in range(pg_detail):
            # extract the osd and return the storage node
            osd = output[x]['acting']
            # osd is a list with osd where a stuck/degraded PG
            # was replicated. If osd is empty, it means
            # PG is not replicated to any osd
            if not osd:
                continue
            osd_id = int(osd[0])
            if osd_id in osd_list:
                continue
            # potential future optimization to cache all the
            # osd to host lookups for the single call to host_osd_status().
            host_name = self.osd_host_lookup(osd_id)
            if (host_name is not None and
               host_name == hostname):
                # mark the selected storage node with HEALTH_BLOCK
                # we can't lock any storage node marked with HEALTH_BLOCK
                return constants.CEPH_HEALTH_BLOCK
            osd_list.append(osd_id)
        return constants.CEPH_HEALTH_OK

    def ceph_pools_empty(self, db_api, pools_usage):
        """ Determine if data CEPH pools are empty.
        :return True if the data CEPH pools are empty
        :return False if the data CEPH pools are not empty
        """
        for ceph_pool in pools_usage:
            # We only need to check data pools.
            if (constants.CEPH_POOL_OBJECT_GATEWAY_NAME_PART in
                    ceph_pool['name']):
                if not (
                    ceph_pool['name'].startswith(
                        constants.CEPH_POOL_OBJECT_GATEWAY_NAME_JEWEL) or
                    ceph_pool['name'].startswith(
                        constants.CEPH_POOL_OBJECT_GATEWAY_NAME_HAMMER)):
                    continue

            # Ceph pool is not empty.
            if int(ceph_pool['stats']['bytes_used']) > 0:
                return False

        return True

    def get_monitors_status(self, db_api):
        num_inv_monitors = 0
        if cutils.is_aio_system(db_api):
            required_monitors = constants.MIN_STOR_MONITORS_AIO
        else:
            required_monitors = constants.MIN_STOR_MONITORS_MULTINODE
        quorum_names = []
        inventory_monitor_names = []

        # first check that the monitors are available in sysinv
        monitor_list = db_api.ceph_mon_get_list()
        for mon in monitor_list:
            ihost = db_api.ihost_get(mon['forihostid'])
            host_action = ihost['ihost_action'] or ""
            locking = (host_action.startswith(constants.LOCK_ACTION) or
                       host_action.startswith(constants.FORCE_LOCK_ACTION))
            if (ihost['administrative'] == constants.ADMIN_UNLOCKED and
               ihost['operational'] == constants.OPERATIONAL_ENABLED and
               not locking):
                num_inv_monitors += 1
                inventory_monitor_names.append(ihost['hostname'])

        LOG.info("Active ceph monitors in inventory = %s" % str(inventory_monitor_names))

        # check that the cluster is actually operational.
        # if we can get the monitor quorum from ceph, then
        # the cluster is truly operational
        if num_inv_monitors >= required_monitors:
            try:
                quorum_names = self._osd_quorum_names()
            except Exception:
                # if the cluster is not responding to requests
                # we set the quorum_names to an empty list , indicating a problem
                quorum_names = []
                LOG.error("Ceph cluster not responding to requests.")

        LOG.info("Active ceph monitors in ceph cluster = %s" % str(quorum_names))

        # There may be cases where a host is in an unlocked-available state,
        # but the monitor is down due to crashes or manual removal.
        # For such cases, we determine the list of active ceph monitors to be
        # the intersection of the sysinv reported unlocked-available monitor
        # hosts and the monitors reported in the quorum via the ceph API.
        active_monitors = list(set(inventory_monitor_names) & set(quorum_names))
        num_active_monitors = len(active_monitors)
        if (num_inv_monitors and num_active_monitors == 0 and
                cutils.is_initial_config_complete() and
                not cutils.is_aio_system(db_api)):
            # The active controller always has a monitor.
            # We are on standard or storage, initial configuration
            # was completed and Ceph is down so we can't check if
            # it is working. Assume it is.
            num_active_monitors = 1
        LOG.info("Active ceph monitors = %s" % str(active_monitors))

        return num_active_monitors, required_monitors, active_monitors

    def list_osd_pools(self):
        """List all osd pools."""
        try:
            pools = self._ceph_api.osd_pool_ls()
        except Exception as e:
            LOG.error("Exception in ls osd pool: {}".format(e))
        return pools

    def get_osd_crushmap(self, crushmap_bin_file):
        try:
            self._ceph_api.osd_crushmap_get(crushmap_bin_file)
        except Exception as e:
            LOG.error("Exception in get osd crushmap: {}".format(e))

    def set_osd_crushmap(self, crushmap_bin_file):
        try:
            self._ceph_api.osd_crushmap_set(crushmap_bin_file)
        except Exception as e:
            LOG.error("Exception in set osd crushmap: {}".format(e))

    def compile_osd_crushmap(self, crushmap_txt, crushmap_bin):
        try:
            self._ceph_api.osd_crushmap_compile(crushmap_txt, crushmap_bin)
        except Exception as e:
            LOG.error("Exception in compile crushmap: {}".format(e))

    def decompile_osd_crushmap(self, crushmap_bin, crushmap_txt):
        try:
            self._ceph_api.osd_crushmap_decompile(crushmap_bin, crushmap_txt)
        except Exception as e:
            LOG.error("Exception in decompile crushmap: {}".format(e))


def fix_crushmap(dbapi=None):
    """ Set Ceph's CRUSH Map based on storage model """
    def _create_crushmap_flag_file():
        try:
            open(crushmap_flag_file, "w").close()
        except IOError as e:
            LOG.warn(('Failed to create flag file: {}. '
                       'Reason: {}').format(crushmap_flag_file, e))

    if not dbapi:
        dbapi = pecan.request.dbapi
    crushmap_flag_file = os.path.join(constants.SYSINV_CONFIG_PATH,
                                      constants.CEPH_CRUSH_MAP_APPLIED)

    if not os.path.isfile(crushmap_flag_file):
        _operator = CephApiOperator()
        if not cutils.is_aio_system(dbapi):
            # At least two monitors have to be running on a standard deployment,
            # otherwise don't even try to load the crushmap.
            active_mons, required_mons, __ = _operator.get_monitors_status(dbapi)
            if required_mons > active_mons:
                LOG.info("Not enough monitors yet available to fix crushmap.")
                return False

        # For AIO system, crushmap should be already loaded through puppet.
        # If it was loaded, set the crushmap flag to avoid loading it twice.
        default_ceph_tier_name = constants.SB_TIER_DEFAULT_NAMES[
                constants.SB_TIER_TYPE_CEPH] + constants.CEPH_CRUSH_TIER_SUFFIX
        rule_is_present, __, __ = _operator._crush_rule_status(default_ceph_tier_name)
        if rule_is_present:
            _create_crushmap_flag_file()
            return False

        # For AIO system, crushmap should alreadby be loaded through
        # puppet. If for any reason it is not, as a precaution we set
        # the crushmap here.

        # Check if a backup crushmap exists. If it does, that means
        # it is during restore. We need to restore the backup crushmap
        # instead of generating it. For non-AIO system, it is stored in
        # /opt/platform/sysinv which is a drbd fs. For AIO systems because
        # when unlocking controller-0 for the first time, the crushmap is
        # set thru ceph puppet when /opt/platform is not mounted yet, we
        # store the crushmap in /etc/sysinv.

        if cutils.is_aio_system(dbapi):
            backup = os.path.join(constants.CEPH_CRUSH_MAP_BACKUP_DIR_FOR_AIO,
                                  constants.CEPH_CRUSH_MAP_BACKUP)
        else:
            backup = os.path.join(constants.SYSINV_CONFIG_PATH,
                                  constants.CEPH_CRUSH_MAP_BACKUP)
        crushmap_bin = "/etc/sysinv/crushmap.bin"
        if os.path.exists(backup):
            shutil.copyfile(backup, crushmap_bin)
        else:
            stor_model = get_ceph_storage_model(dbapi)
            if stor_model == constants.CEPH_AIO_SX_MODEL:
                crushmap_txt = "/etc/sysinv/crushmap-aio-sx.txt"
            elif stor_model == constants.CEPH_CONTROLLER_MODEL:
                crushmap_txt = "/etc/sysinv/crushmap-controller-model.txt"
            elif stor_model == constants.CEPH_STORAGE_MODEL:
                crushmap_txt = "/etc/sysinv/crushmap-storage-model.txt"
            else:
                reason = "Error: Undefined ceph storage model %s" % stor_model
                raise exception.CephCrushMapNotApplied(reason=reason)
            LOG.info("Updating crushmap with: %s" % crushmap_txt)

            # Compile crushmap
            _operator.compile_osd_crushmap(crushmap_txt, crushmap_bin)
        # Set crushmap
        _operator.set_osd_crushmap(crushmap_bin)

        if os.path.exists(backup):
            os.remove(backup)

        _create_crushmap_flag_file()

        return True
    return False


def get_ceph_storage_model(dbapi=None):

    if not dbapi:
        dbapi = pecan.request.dbapi

    if cutils.is_aio_simplex_system(dbapi):
        return constants.CEPH_AIO_SX_MODEL

    if cutils.is_aio_duplex_system(dbapi):
        return constants.CEPH_CONTROLLER_MODEL

    is_storage_model = False
    is_controller_model = False

    monitor_list = dbapi.ceph_mon_get_list()
    for mon in monitor_list:
        ihost = dbapi.ihost_get(mon['forihostid'])
        if ihost.personality == constants.WORKER:
            # 3rd monitor is on a compute node, so OSDs are on controller
            is_controller_model = True
        elif ihost.personality == constants.STORAGE:
            # 3rd monitor is on storage-0, so OSDs are also on storage nodes
            is_storage_model = True

    # Check any storage nodes are provisioned
    if not is_storage_model:
        if dbapi.ihost_get_by_personality(constants.STORAGE):
            is_storage_model = True

    # There are cases where we delete the monitor on worker node and have not
    # yet assigned it to another worker. In this case check if any OSDs have
    # been configured on controller nodes.
    if not is_storage_model:
        controller_hosts = dbapi.ihost_get_by_personality(constants.CONTROLLER)
        for chost in controller_hosts:
            istors = dbapi.istor_get_by_ihost(chost['uuid'])
            if len(istors):
                is_controller_model = True
                break

    if is_storage_model and is_controller_model:
        # Both types should not be true at the same time, but we should log a
        # message for debug purposes
        # TODO(sdinescu): Improve error message
        LOG.error("Wrong ceph storage type. Bad configuration.")
        return constants.CEPH_STORAGE_MODEL
    elif is_storage_model:
        return constants.CEPH_STORAGE_MODEL
    elif is_controller_model:
        return constants.CEPH_CONTROLLER_MODEL
    else:
        # This case is for the install stage where the decision
        # to configure OSDs on controller or storage nodes is not
        # clear (before adding a monitor on a compute or before
        # configuring the first storage node)
        return constants.CEPH_UNDEFINED_MODEL
