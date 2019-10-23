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

"""
A rook-api python interface that handles rook yaml & REST calls and response.
"""

class RookClient(object):

	def __init__(self, **params):
		"""
        Initialize the class, get the necessary parameters
        """
        self.user_agent = 'python-rookclient'

        self.params = params

    def get_overrides(self):
    	"""
    	Implement the function to override pararmeters in ceph-cluster helm
    	chart.
    	"""
    	pass


