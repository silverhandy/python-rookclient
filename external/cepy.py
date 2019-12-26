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


from rookclient import ceph as rook_ceph

import os
import uuid
import copy
import tsconfig.tsconfig as tsc
from requests.exceptions import RequestException
from requests.exceptions import ReadTimeout

from cephclient import wrapper as ceph
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
        self._ceph_api = rook_ceph.RookCephApi()
        self._ceph_ns = 'rook-ceph'
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
            ceph_health = self._ceph_api.ceph_health(self._ceph_ns)
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
            if self._ceph_api.mon_remove(hostname, self._ceph_ns):
                LOG.error("Remove monitor error")
        except Exception as e:
            LOG.error("Exception in removing monitor: {}".format(e))

    def get_ceph_primary_tier_size(self):
        pass

    def get_ceph_tiers_size(self):
        pass

    def get_pools_df_stats(self):
        pass

    def get_cluster_df_stats(self, timeout=10):
        pass

    def delete_osd_pool(self, pool_name):
        pass

    def list_osd_pools(self):
        pass

    def osd_get_pool_quota(self, pool_name):
        pass

    def set_osd_pool_quota(self, pool, max_bytes=0, max_objects=0):
        pass

    def mark_osd_down(self, osdid):
        pass

    def osd_remove_crush_auth(self, osdid):
        pass

    def osd_remove(self, *args, **kwargs):
        pass

    def get_pools_config(self):
        pass

    def get_ceph_object_pool_name(self):
        pass

    def _get_fsid(self, timeout=10):
        pass