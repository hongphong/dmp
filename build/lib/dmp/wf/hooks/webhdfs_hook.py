#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'phongphamhong'

import copy

from dmp.helper.utils import convert_int
from dmp.helper.log import logger
# !/usr/bin/python
#
# Copyright 11/9/18 Phong Pham Hong <phongbro1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from airflow.hooks.base_hook import BaseHook

try:
    from hdfs import Client, InsecureClient, client

    load_hdfs = True
except ImportError:
    load_hdfs = False


class WebClientHook(BaseHook):
    """
    Wrapper hdfs library: https://pypi.org/project/hdfs/
    You can define standby_host on extra field (airflow connection)
    """

    def __init__(self,
                 connection_id='web_hdfs_default',
                 mode="insecure",
                 proxy=None,
                 **kwargs
                 ):
        """
        Args:
            connection_id: connection defined on airflow connection
            mode: insecure or other
            proxy: proxy config. ex: {
                 “http”: “http://10.10.10.10:8000”,
                 “https”: “http://10.10.10.10:8000”,
            }
            **kwargs: params for Client, InsecureClient
        """
        if load_hdfs is False:
            raise ModuleNotFoundError("Cannot import module hdfs")
        self.default_connection = connection_id
        self.hdfs_params = copy.copy(kwargs)
        self.proxy = proxy
        self.mode = mode

    def check_is_active(self, host):
        import requests
        try:
            link = "%s%s" % (host, client._Request.webhdfs_prefix)
            logger.info("check node is standby or active: %s" % link)
            r = requests.get(url=link, proxies=self.proxy)
            if r.status_code >= 400:
                logger.warn("Node :%s is standby" % host)
                return False
            return True
        except BaseException as e:
            logger.error("Error when check host standby or not. host: %s message: %s" % (host, e))
            return False

    def correct_host(self, host, port=None):
        if not host.startswith("http://") and not host.startswith("https://"):
            host = "http://%s" % host
        if convert_int(host.split(":")[-1]) <= 0:
            if port:
                host = "%s:%s" % (host, port)
            else:
                host = "%s:50070"
        return host

    def get_hook(self):
        """
        More documents: https://hdfscli.readthedocs.io/en/latest/quickstart.html
        Returns: InsecureClient or Client
        """
        conn = BaseHook.get_connection(self.default_connection)
        host = self.correct_host(conn.host, conn.port)
        other_host = conn.extra_dejson.get("standby_host")
        is_active = False
        if self.check_is_active(host):
            logger.info("host: %s is active" % host)
            is_active = True
        elif (other_host != None):
            other_host = self.correct_host(other_host, conn.port)
            if self.check_is_active(other_host):
                host = other_host
                is_active = True
        if is_active is not True:
            raise ConnectionError("Host: %s are not active" % [host, other_host])

        if self.mode == "insecure":
            logger.info("Create client with insecure mode: %s" % host)
            client = InsecureClient(host, **self.hdfs_params)
        else:
            logger.info("Create client with mode: %s" % host)
            client = Client(host, **self.hdfs_params)
        return client

    def get_records(self, sql):
        return None

    def get_pandas_df(self, sql):
        return None

    def run(self, sql):
        return None
