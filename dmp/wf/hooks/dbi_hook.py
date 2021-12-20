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
import sys
from contextlib import closing
from dmp.helper.log import logger
from airflow.hooks.dbapi import DbApiHook
from urllib.parse import quote_plus


class CustomDbApiHook(DbApiHook):
    def get_records_dicts(self, sql, parameters=None):
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
                for r in cur:
                    yield dict(zip(columns, r))
        logger.info("Close connection!")

    def get_schema(self, sql, parameters=None, type="sqlalchemy"):
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
                logger.info("Close connection getting description!")
                return self.convert_description_to_schema(cur.description, type)

    def get_description(self, sql, parameters=None):
        """
        get description of sql query
        list description: name,type_code,display_size,internal_size,precision,scale,null_ok

            Params:
                sql:
                parameters:
                return:
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')

        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.description

    def convert_description_to_schema(self, description, type="sqlalchemy"):
        if type == "sqlalchemy":
            import sqlalchemy.sql.sqltypes as types
            rs = []
            for item in description:
                t = item[1].__name__.lower()
                if t.find("number") >= 0:
                    rs.append(types.NUMERIC)
                elif t.find("char") >= 0:
                    rs.append(types.NVARCHAR)
                elif t.find("varchar") >= 0:
                    rs.append(types.VARCHAR)
                elif t.find("clob") >= 0:
                    rs.append(types.CLOB)
                elif t.find("long") >= 0:
                    rs.append(types.BIGINT)
                elif t.find("int") >= 0:
                    rs.append(types.INTEGER)
                elif t.find("blob") >= 0:
                    rs.append(types.BLOB)
                elif t.find("double") >= 0:
                    rs.append(types.NUMERIC)
                elif t.find("datetime") >= 0:
                    rs.append(types.DATETIME)
                elif t.find("date") >= 0:
                    rs.append(types.DATE)
                elif t.find("timestamp") >= 0:
                    rs.append(types.TIMESTAMP)
                elif t.find("time") >= 0:
                    rs.append(types.TIME)
                elif t.find("decimal") >= 0:
                    rs.append(types.DECIMAL)
                elif t.find("float") >= 0:
                    rs.append(types.FLOAT)
                elif t.find("real") >= 0:
                    rs.append(types.REAL)
                elif t.find("text") >= 0:
                    rs.append(types.TEXT)
                elif t.find("varbinary") >= 0:
                    rs.append(types.VARBINARY)
                elif t.find("binary") >= 0:
                    rs.append(types.BINARY)
                elif t.find("json") >= 0:
                    rs.append(types.JSON)
                elif t.find("bigint") >= 0:
                    rs.append(types.BIGINT)
                elif t.find("numeric") >= 0:
                    rs.append(types.NUMERIC)
                else:
                    rs.append(types.STRINGTYPE)
            return rs
        elif type == "spark":
            import pyspark.sql.types as types
            from pyspark.sql.types import StructField, StructType
            rs = StructType()

            def ai_by_human_number(description):
                scale = description[5]
                size = description[2]
                name = description[1].__name__.lower()
                if (scale == 0 and (size is None or size >= 10)):
                    return types.LongType()
                elif (scale == 0 and size > 0 and size <= 9):
                    return types.IntegerType()
                elif (scale < 0 and (name.startswith("id_") or name.startswith("_id"))):
                    return types.LongType()
                else:
                    return types.DoubleType()

            for item in description:
                t = item[1].__name__.lower()
                col = item[0]
                if t.find("number") >= 0:
                    rs.add(StructField(col, ai_by_human_number(item)))
                elif t.find("char") >= 0:
                    rs.add(StructField(col, types.StringType()))
                elif t.find("varchar") >= 0:
                    rs.add(StructField(col, types.StringType()))
                elif t.find("clob") >= 0:
                    rs.add(StructField(col, types.StringType()))
                elif t.find("long") >= 0:
                    rs.add(StructField(col, types.LongType()))
                elif t.find("int") >= 0:
                    rs.add(StructField(col, types.IntegerType()))
                elif t.find("blob") >= 0:
                    rs.add(StructField(col, types.StringType()))
                elif t.find("double") >= 0:
                    rs.add(StructField(col, types.DoubleType()))
                elif t.find("datetime") >= 0:
                    rs.add(StructField(col, types.DateType()))
                elif t.find("date") >= 0:
                    rs.add(StructField(col, types.DateType()))
                elif t.find("timestamp") >= 0:
                    rs.add(StructField(col, types.TimestampType()))
                elif t.find("time") >= 0:
                    rs.add(StructField(col, types.TimestampType()))
                elif t.find("decimal") >= 0:
                    rs.add(StructField(col, types.DecimalType()))
                elif t.find("float") >= 0:
                    rs.add(StructField(col, types.FloatType()))
                elif t.find("real") >= 0:
                    rs.add(StructField(col, types.DoubleType()))
                elif t.find("text") >= 0:
                    rs.add(StructField(col, types.StringType()))
                elif t.find("varbinary") >= 0:
                    rs.add(StructField(col, types.BinaryType()))
                elif t.find("binary") >= 0:
                    rs.add(StructField(col, types.BinaryType()))
                elif t.find("json") >= 0:
                    rs.add(StructField(col, types.StringType()))
                elif t.find("bigint") >= 0:
                    rs.add(StructField(col, types.LongType()))
                elif t.find("numeric") >= 0:
                    rs.add(StructField(col, ai_by_human_number(item)))
                else:
                    rs.add(StructField(col, types.StringType()))
            return rs

        else:
            raise ValueError("type: %s is not allowed" % type)

    def get_uri(self) -> str:
        """
                Extract the URI from the connection.

                :return: the extracted uri.
                """
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
            uri += ("?" + "&".join(list_extra))
        return uri

