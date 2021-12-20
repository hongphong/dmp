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

from dmp.wf.hooks.dbi_hook import CustomDbApiHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from dmp.helper.log import logger
from contextlib import closing
import sys

JdbcHook.__bases__ = (CustomDbApiHook,)


def __get_records_dicts(self, sql, parameters=None):
    """
    Executes the sql and returns a set of records.

    Result will be return with format: list[dict]

        Parameters:
            sql: str or list, the sql statement to be executed
                (str) or a list of sql statements to execute
            parameters: mappint or iterable, the parameters to
                render the SQL query with.

    """
    if sys.version_info[0] < 3:
        sql = sql.encode('utf-8')

    with closing(self.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            if parameters is not None:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)
            columns = [column[0] for column in cur.description]
            for r in cur.fetchall():
                yield dict(zip(columns, r))
    logger.info("Close connection!")


JdbcHook.get_records_dicts = __get_records_dicts
