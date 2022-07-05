#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'phongphamhong'


# !/usr/bin/python
#
# Copyright Phong Pham Hong <phongpham1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

def load_factory(connect_type, connection_id, object_class=None):
    if callable(object_class):
        return object_class(connection_id)
    if 'jdbc' in connect_type:
        from dmp.wf.hooks.jdbc_hook import JdbcHook
        object_class = JdbcHook
    elif 'postgres' in connect_type:
        from dmp.wf.hooks.postgre_hook import PostgresHook
        object_class = PostgresHook
    elif 'mysql' in connect_type or 'mariadb' in connect_type:
        from dmp.wf.hooks.mysql_hook import MySqlHook
        object_class = MySqlHook
    elif 'mongodb' in connect_type:
        from dmp.wf.hooks.mongodb_hook import CusMongoHook
        object_class = CusMongoHook
    elif 'oracle' in connect_type:
        from dmp.wf.hooks.oracle_hook import CusOracleHook
        object_class = CusOracleHook
    elif 'clickhouse' in connect_type:
        from dmp.wf.hooks.clickhouse_hook import CusClickhouseHook
        object_class = CusClickhouseHook

    if object_class:
        return object_class(connection_id)
    else:
        raise Exception("Not support for connection type [%s]" % connect_type)
