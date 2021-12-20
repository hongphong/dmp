# !/usr/bin/python
#
# Copyright Phong Pham Hong <phongpham1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
__author__ = 'phongphamhong'

from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.utils import db
import urllib
from dmp.helper.log import logger


class WfConnection:

    def __init__(self, wf_connection_id):
        self.wf_connection_id = wf_connection_id

    @classmethod
    def init_wf_connection(cls, connection_id, host, login, password, extra={},
                           connection_type="other",
                           description="",
                           schema=None,
                           port=None,
                           uri=None,
                           encode=True
                           ):
        import json
        from airflow.models.connection import Connection
        if encode:
            password = urllib.parse.quote(password)
            login = urllib.parse.quote(login)
        c = Connection(
            conn_id=connection_id,
            conn_type=connection_type,
            description=description,
            host=host,
            login=login,
            password=password,
            extra=json.dumps(extra),
            schema=schema if schema else "",
            port=port,
            uri=uri
        )
        return c

    @classmethod
    def save_wf_connection(cls, connection_id, host, login, password, extra={}, connection_type="other",
                           description="", schema=None,
                           port=None,
                           uri=None, encode=False):
        session = db.settings.Session()
        try:

            c = cls.init_wf_connection(
                connection_id=connection_id,
                connection_type=connection_type,
                host=host,
                login=login,
                password=password,
                extra=extra,
                description=description,
                schema=schema,
                port=port,
                uri=uri,
                encode=encode
            )
            session.add(c)
            session.commit()
        except BaseException as e:
            session.rollback()
            logger.error("Cannot insert connection: %s" % connection_id)
            logger.exception(e)
            return False
        logger.info("You have added connection: [%s] = [%s] " % (connection_id, c.get_uri()))
        return True

    @classmethod
    def remove_wf_connection(cls, connection_id):
        session = db.settings.Session()
        try:
            session.query(Connection).filter(Connection.conn_id == connection_id).delete()
            session.commit()
        except BaseException as e:
            session.rollback()
            logger.error("Cannot delete connection: %s" % connection_id)
            logger.exception(e)
            return False
        logger.info("You have deleted connection: [%s]" % (connection_id))
        return True

    @classmethod
    def get_wf_list_connection(cls):

        session = db.settings.Session()
        return session.query(Connection).all()

    def get_wf_connection(self):
        return BaseHook.get_connection(self.wf_connection_id)

    def get_wf_db(self, object_class=None):
        connect = self.get_wf_connection()
        connect_type = [k.strip() for k in connect.conn_type.lower().split("+")]

        if object_class:
            return object_class(self.wf_connection_id)
        if 'jdbc' in connect_type:
            from dmp.wf.hooks.jdbc_hook import JdbcHook
            object_class = JdbcHook
        elif 'postgres' in connect_type:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            object_class = PostgresHook
        elif 'mysql' in connect_type or 'mariadb' in connect_type:
            from airflow.providers.mysql.hooks.mysql import MySqlHook
            object_class = MySqlHook
        elif 'mongodb' in connect_type:
            from dmp.wf.hooks.mongodb_hook import CusMongoHook
            object_class = CusMongoHook
        elif 'oracle' in connect_type:
            from dmp.wf.hooks.oracle_hook import CusOracleHook
            object_class = CusOracleHook
        if object_class:
            return object_class(self.wf_connection_id)
        else:
            raise Exception("Not support for connection type [%s]" % connect_type)

    def create_engine_by_uri(self, uri="", create_engine_params=None):
        from sqlalchemy import create_engine
        if create_engine_params is not None and isinstance(create_engine_params, dict):
            db_connection = create_engine(uri, **create_engine_params)
        else:
            db_connection = create_engine(uri)
        return db_connection
