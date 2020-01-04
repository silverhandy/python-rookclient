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

""" System Inventory Ceph Utilities and Helper functions. """


import os
import uuid
import copy
import tsconfig.tsconfig as tsc
from requests.exceptions import RequestException
from requests.exceptions import ReadTimeout

#from cephclient import wrapper as ceph
from rookclient import ceph as rook_ceph
from fm_api import fm_api
from oslo_log import log as logging
from sysinv._i18n import _
from sysinv.common import constants
from sysinv.common import exception
from sysinv.common import utils as cutils
from sysinv.openstack.common import uuidutils
from sysinv.common.storage_backend_conf import StorageBackendConfig

from sysinv.openstack.common import rpc
from sysinv.openstack.common.rpc.common import CommonRpcContext

LOG = logging.getLogger(__name__)
CEPH_POOLS = copy.deepcopy(constants.CEPH_POOLS)

class CephOperator(object):
    """Class to encapsulate Ceph operations for System Inventory
       Methods on object-based storage devices (OSDs).
    """

    def __init__(self, db_api):
        self._db_api = db_api
        self._ceph_api = rook_ceph.RookCephApi(
            constants.K8S_ROOK_CEPH_NAMESPACE_DEFAULT)
        self._db_cluster = None
        self._db_primary_tier = None
        self._cluster_name = 'ceph_cluster'
        self._init_db_cluster_and_tier()

    def _init_db_cluster_and_tier(self):
        try:
            self._db_cluster = self._db_api.clusters_get_all(
                type=constants.CINDER_BACKEND_CEPH)[0]
            if not self.cluster_ceph_uuid:
                # Retrieve ceph cluster fsid and update database
                fsid = self._get_fsid()
                if uuidutils.is_uuid_like(fsid):
                    LOG.info("Update cluster record: fsid=%s." % fsid)
                    self._db_cluster.cluster_uuid = fsid
                    self._db_api.cluster_update(
                        self.cluster_db_uuid,
                        {'cluster_uuid': fsid})
            self._db_primary_tier = self._db_api.storage_tier_get_all(
                name=constants.SB_TIER_DEFAULT_NAMES[
                    constants.SB_TIER_TYPE_CEPH])[0]
        except IndexError:
            # No existing DB record for the cluster, try to create one
            self._create_db_ceph_cluster()

    def _create_db_ceph_cluster(self):
        # Make sure the system has been configured
        try:
            isystem = self._db_api.isystem_get_one()
        except exception.NotFound:
            LOG.info('System is not configured. Cannot create Cluster '
                     'DB entry')
            return

        # Try to use ceph cluster fsid
        fsid = self._get_fsid()
        LOG.info("Create new ceph cluster record: fsid=%s." % fsid)
        # Create the default primary cluster
        self._db_cluster = self._db_api.cluster_create(
            {'uuid': fsid if uuidutils.is_uuid_like(fsid) else str(uuid.uuid4()),
             'cluster_uuid': fsid,
             'type': constants.CINDER_BACKEND_CEPH,
             'name': self._cluster_name,
             'system_id': isystem.id})

        # Create the default primary ceph storage tier
        LOG.info("Create primary ceph tier record.")
        self._db_primary_tier = self._db_api.storage_tier_create(
            {'forclusterid': self.cluster_id,
             'name': constants.SB_TIER_DEFAULT_NAMES[constants.SB_TIER_TYPE_CEPH],
             'type': constants.SB_TIER_TYPE_CEPH,
             'status': constants.SB_TIER_STATUS_DEFINED,
             'capabilities': {}})

    def ceph_status_ok(self):
        rc = True

        try:
            ceph_health = self._ceph_api.ceph_health()
            if ceph_health != constants.CEPH_HEALTH_OK:
                rc = False
        except Exception as e:
            rc = False
            LOG.warn("ceph status exception: %s " % e)
        
        return rc

    def update_ceph_cluster(self, host):
        if not self._db_cluster:
            self._init_db_cluster_and_tier()
        elif not self.cluster_ceph_uuid:
            fsid = self._get_fsid()
            if uuidutils.is_uuid_like(fsid):
                self._db_api.cluster_update(
                    self.cluster_db_uuid,
                    {'cluster_uuid': fsid})
                self._db_cluster.cluster_uuid = fsid

        self.assign_host_to_peer_group(host)

    def get_ceph_cluster_info_availability(self):
        # TODO(CephPoolsDecouple): rework
        # Check if the ceph cluster is ready to return statistics
        storage_hosts = self._db_api.ihost_get_by_personality(
            constants.STORAGE)

        is_aio = tsc.system_type == constants.TIS_AIO_BUILD

        if not storage_hosts and is_aio:
            storage_hosts = self._db_api.ihost_get_by_personality(
                constants.CONTROLLER)

        # If there is no storage node present, ceph usage
        # information is not relevant
        if not storage_hosts:
            return False

        # At least one storage node must be in available state
        for host in storage_hosts:
            if host['availability'] == constants.AVAILABILITY_AVAILABLE:
                break
        else:
            # No storage node is available
            return False
        return True

    def have_ceph_monitor_access(self, timeout=5):
        """ Verify that ceph monitor access will not timeout.

        :param timeout: Time in seconds to wait for the REST API request to
            respond.
        """
        available_mons = 0
        monitors = self._db_api.ceph_mon_get_list()
        for m in monitors:
            try:
                ihost = self._db_api.ihost_get_by_hostname(m.hostname)
            except exception.NodeNotFound:
                LOG.error("Monitor host %s not found" % m.hostname)
                continue

            if (ihost['administrative'] == constants.ADMIN_UNLOCKED and
                    ihost['operational'] == constants.OPERATIONAL_ENABLED):
                available_mons += 1

        # Avoid calling the ceph rest_api until we have a minimum configuration
        check_access = False
        if cutils.is_aio_system(self._db_api) and available_mons > 0:
            # one monitor: need it available
            check_access = True
        elif available_mons > 1:
            # three monitors: need two available
            check_access = True

        LOG.debug("Checking ceph monitors. Available: %s. Check cluster: "
                 "access %s" % (available_mons, check_access))
        if check_access:
            return True if self._get_fsid(timeout) else False
        return False

    def remove_ceph_monitor(self, hostname, timeout=None):
        try:
            if self._ceph_api.mon_remove(hostname, timeout) == False:
                LOG.error("Remove monitor %s error" % hostname)
        except Exception as e:
            LOG.error("Exception in removing monitor: {}".format(e))

    def get_ceph_tiers_size(self):
        try:
            tiers_size = self._ceph_api.get_tiers_size()
            if tiers_size is None:
                LOG.error("Get tiers size error")
                return None
        except Exception as e:
            LOG.error("Exception in getting tiers size: {}".format(e))
        
        LOG.debug("Ceph cluster tiers (size in GB): %s" % str(tiers_size))
        return tiers_size

    def get_ceph_primary_tier_size(self):
        primary_tier_name = constants.SB_TIER_DEFAULT_NAMES[
            constants.SB_TIER_TYPE_CEPH] + constants.CEPH_CRUSH_TIER_SUFFIX

        tiers_size = self.get_ceph_tiers_size()
        primary_tier_size = tiers_size.get(primary_tier_name, 0)
        LOG.debug("Ceph cluster primary tier size: %s GB" %
            str(primary_tier_size))
        return primary_tier_size

    def get_pools_df_stats(self):
        try:
            ceph_df = self._ceph_api.ceph_df()
            if ceph_df is None:
                LOG.error("Get ceph df error")
        except Exception as e:
            LOG.error("Exception in getting ceph df: {}".format(e))

        return ceph_df["pools"]

    def get_cluster_df_stats(self, timeout=10):
        try:
            ceph_df = self._ceph_api.ceph_df(timeout)
            if ceph_df is None:
                LOG.error("Get ceph df error")
        except Exception as e:
            LOG.error("Exception in getting ceph df: {}".format(e))

        return ceph_df["stats"]

    def delete_osd_pool(self, pool):
        try:
            self._ceph_api.osd_pool_delete(pool)
        except Exception as e:
            LOG.error("Exception in deleting osd pool: {}".format(e))

    def list_osd_pools(self):
        try:
            pool_ls = self._ceph_api.osd_pool_ls()
            if pool_ls is None:
                LOG.error("List osd pools error")
        except Exception as e:
            LOG.error("Exception in listing osd pools: {}".format(e))

        return pool_ls

    def osd_get_pool_quota(self, pool):
        try:
            pool_quotas = self._ceph_api.osd_pool_get_quota(pool)
            if not pool_quotas:
                LOG.error("Get osd pool quota error")
        except Exception as e:
            LOG.error("Exception in getting osd pool quota: {}".format(e))
        
        return {"max_objects": pool_quotas["quota_max_objects"],
                "max_bytes": pool_quotas["quota_max_bytes"]}


    def set_osd_pool_quota(self, pool, max_bytes=0, max_objects=0):
        """Set the quota for an OSD pool
        Setting max_bytes or max_objects to 0 will disable that quota param
        :param pool:         OSD pool
        :param max_bytes:    maximum bytes for OSD pool
        :param max_objects:  maximum objects for OSD pool
        """
        prev_quota = self.osd_get_pool_quota(pool)
        if prev_quota["max_bytes"] != max_bytes:
            self._ceph_api.osd_pool_set_quota(pool, 'max_bytes', max_bytes)
            LOG.info(_("Set OSD pool quota: "
                       "pool={}, max_bytes={}").format(pool, max_bytes))
        if prev_quota["max_objects"] != max_objects:
            self._ceph_api.osd_pool_set_quota(pool, 'max_objects', max_objects)
            LOG.info(_("Set OSD pool quota: "
                       "pool={}, max_objects={}").format(pool, max_objects))


    def get_osd_tree(self):
        try:
            osd_tree = self._ceph_api.osd_tree()
            if osd_tree is None:
                LOG.error("Get ceph df error")
                return None, None
        except Exception as e:
            LOG.error("Exception in getting ceph df: {}".format(e))

        return osd_tree['nodes'], osd_tree['stray']

    def set_osd_down(self, osdid):
        try:
            self._ceph_api.osd_down(osdid)
        except Exception as e:
            LOG.error("Exception in setting osd down: {}".format(e))
        LOG.info("Set OSD %d to down state.", osdid)

    def mark_osd_down(self, osdid):
        # set
        to_mark_osd_down = False
        nodes, stray = self.get_osd_tree()
        if nodes and stray:
            osdid_str = "osd." + str(osdid)
            for entry in nodes + stray:
                if entry['name'] == osdid_str:
                    if entry['status'] == 'up':
                        LOG.info("OSD %s is still up. Mark it down.", osdid_str)
                        to_mark_osd_down = True
                    break

        if to_mark_osd_down:
            self.set_osd_down(osdid)

    def osd_remove_crush_auth(self, osdid):
        # set
        osdid_str = "osd." + str(osdid)
        try:
            self._ceph_api.osd_crush_remove(osdid_str)
        except Exception as e:
            LOG.error("Exception in osd cursh remove: {}".format(e))
        LOG.info("Remove OSD %d CRUSH.", osdid)

        try:
            self._ceph_api.auth_del(osdid_str)
        except Exception as e:
            LOG.error("Exception in deleting auth osd: {}".format(e))
        LOG.info("Delete OSD %d Auth.", osdid)


    def get_pools_values(self):
        """Create or resize all of the osd pools as needed
        """
        default_quota_map = {'cinder': constants.CEPH_POOL_VOLUMES_QUOTA_GIB,
                             'glance': constants.CEPH_POOL_IMAGES_QUOTA_GIB,
                             'ephemeral': constants.CEPH_POOL_EPHEMERAL_QUOTA_GIB,
                             'object': constants.CEPH_POOL_OBJECT_GATEWAY_QUOTA_GIB,
                             'kube': constants.CEPH_POOL_KUBE_QUOTA_GIB}

        storage_ceph = StorageBackendConfig.get_configured_backend_conf(
            self._db_api,
            constants.CINDER_BACKEND_CEPH
        )

        quotas = []
        for p in ['cinder', 'glance', 'ephemeral', 'object', 'kube']:
            quota_attr = p + '_pool_gib'
            quota_val = getattr(storage_ceph, quota_attr)

            if quota_val is None:
                quota_val = default_quota_map[p]
                self._db_api.storage_ceph_update(storage_ceph.uuid,
                                                 {quota_attr: quota_val})

            quotas.append(quota_val)

        LOG.debug("Pool Quotas: %s" % quotas)
        return tuple(quotas)

    def set_quota_gib(self, pool_name):
        quota_gib_value = None
        cinder_pool_gib, glance_pool_gib, ephemeral_pool_gib, \
            object_pool_gib, kube_pool_gib = self.get_pools_values()

        if pool_name.find(constants.CEPH_POOL_VOLUMES_NAME) != -1:
            quota_gib_value = cinder_pool_gib
        elif pool_name.find(constants.CEPH_POOL_KUBE_NAME) != -1:
            quota_gib_value = kube_pool_gib
        elif pool_name.find(constants.CEPH_POOL_IMAGES_NAME) != -1:
            quota_gib_value = glance_pool_gib
        elif pool_name.find(constants.CEPH_POOL_EPHEMERAL_NAME) != -1:
            quota_gib_value = ephemeral_pool_gib
        elif pool_name.find(constants.CEPH_POOL_OBJECT_GATEWAY_NAME_JEWEL) != -1 or \
                 pool_name.find(constants.CEPH_POOL_OBJECT_GATEWAY_NAME_HAMMER) != -1:
            quota_gib_value = object_pool_gib
        else:
            quota_gib_value = 0

        return quota_gib_value

    def get_pools_config(self):
        for pool in CEPH_POOLS:
            # Here it is okay for object pool name is either
            # constants.CEPH_POOL_OBJECT_GATEWAY_NAME_JEWEL or
            # constants.CEPH_POOL_OBJECT_GATEWAY_NAME_HAMMER
            pool['quota_gib'] = self.set_quota_gib(pool['pool_name'])
        return CEPH_POOLS

    def get_ceph_object_pool_name(self):
        try:
            pg_num = self._ceph_api.osd_pool_get(
                constants.CEPH_POOL_OBJECT_GATEWAY_NAME_JEWEL,
                "pg_num")
            if pg_num is not None:
                return constants.CEPH_POOL_OBJECT_GATEWAY_NAME_JEWEL
        except Exception as e:
            LOG.error("Exception in getting pool var pg_num: {}".format(e))

        try:
            pg_num = self._ceph_api.osd_pool_get(
                constants.CEPH_POOL_OBJECT_GATEWAY_NAME_HAMMER,
                "pg_num")
            if pg_num is not None:
                return constants.CEPH_POOL_OBJECT_GATEWAY_NAME_HAMMER
        except Exception as e:
            LOG.error("Exception in getting pool var pg_num: {}".format(e))

        return None


    def _get_fsid(self, timeout=10):
        try:
            fsid = self._ceph_api.fsid(timeout)
        except Exception as e:
            LOG.warn("ceph_api.fsid failed: " + str(e))
            return None

        return str(fsid.strip())
