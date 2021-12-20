#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'phongphamhong'

# !/usr/bin/python
#
# Copyright 11/9/18 Phong Pham Hong <phongbro1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from airflow.hooks.base import BaseHook

try:
    import pydoop.hdfs as hdfs
    load_hdfs = True
except ImportError:
    load_hdfs = False


class HDFSClientHook(BaseHook):
    """
    Wrapper Pydoop library
    Documents: https://crs4.github.io/pydoop/
    """

    def __init__(self,
                 connection_id=None,
                 **kwargs
                 ):
        if load_hdfs is False:
            raise ModuleNotFoundError("Cannot import module pydoop")
        self.connection_id = connection_id

    def get_hook(self):
        """
        Documents: https://crs4.github.io/pydoop/api_docs/hdfs_api.html
        Returns: pydoop.hdfs
        """
        if self.connection_id is None:
            return hdfs
        else:
            con = BaseHook.get_conn(self.connection_id)
            groups = con.extras.get("groups")
            return hdfs(host=con.host, port=con.port, user=con.user, groups=groups)

    def get_records(self, sql):
        return None

    def get_pandas_df(self, sql):
        return None

    def run(self, sql):
        return None
