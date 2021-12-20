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
# # set enviroment in dev mode
from airflow.providers.oracle.hooks.oracle import OracleHook
from dmp.wf.hooks.dbi_hook import CustomDbApiHook
from urllib.parse import quote_plus

OracleHook.__bases__ = (CustomDbApiHook,)


class CusOracleHook(OracleHook):

    def get_uri(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        extra = conn.extra_dejson
        login = ''
        if conn.login:
            login = f'{quote_plus(conn.login)}:{quote_plus(conn.password)}@'
        host = conn.host
        if conn.port is not None:
            host += f':{conn.port}'
        uri = f'{conn.conn_type}://{login}{host}/'
        if conn.schema:
            uri += conn.schema
        list_extra = []
        sid = conn.extra_dejson.get('sid')
        service_name = conn.extra_dejson.get('service_name')
        if sid:
            list_extra.append("sid=%s" % sid)
        elif service_name:
            list_extra.append("service_name=%s" % service_name)
        if extra.get('extra_uri'):
            list_extra.append(extra.get('extra_uri'))
        if list_extra:
            uri+=("?"+"&".join(list_extra))
        return uri
