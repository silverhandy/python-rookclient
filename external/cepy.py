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

class CephOperator(object):
    """Class to encapsulate Ceph operations for System Inventory
       Methods on object-based storage devices (OSDs).
    """

    def __init__(self, db_api):
        self._db_api = db_api
        self._ceph_api = rook_ceph.RookCephApi()
        self._db_cluster = None
        self._db_primary_tier = None
        self._cluster_name = 'ceph_cluster'
        self._init_db_cluster_and_tier()

    def ceph_status_ok(self):
        return True

    def update_ceph_cluster(self, host):
        pass

    def get_ceph_cluster_info_availability(self):
        return True

    def have_ceph_monitor_access(self, timeout=5):
        return True

    def remove_ceph_monitor(self, hostname, timeout=None):
        self._ceph_api.mon_remove(hostname)

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
