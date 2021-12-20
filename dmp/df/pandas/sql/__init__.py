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

from dmp.df.pandas import PandasPipeline
from dmp.helper.log import logger


class PdSql(PandasPipeline):
    """
    Extent PandasPipeline with some methods for SQL
    """

    def read_sql_by_pk(self, sql, pk_column=None, replace_sql={}, start=0, end=1000, step=10000, chunk_size=None,
                       callable_each_batch=None,
                       **read_sql):
        """
        Read data from sql and split batch by pk_column.
        Args:
            sql: table you want to extract data or a query with template: select * from {table_name} where {pk} >= {start} and {pk} < {end} <and your query>
            replace_sql: params for query
                        replace params on SQL string with specific variables. For example:
                        - sql: select * from example where a > {date_string}
                        - replace_params: {'date_string': '1970-01-01'}
            pk_column: primary key column
            start: start from value
            end: end from value
            step: the increase size from start
            chunk_size: chunk_size to split dataframe into iterator
            callable_each_batch: function executed on each batch
            **read_sql: params for PandasPipeline.read_sql
        Returns:
        """
        if not str(sql).lstrip("(").startswith("select "):
            sql_query = "select * from {table_name} where {pk} >= {start} and {pk} < {end}"
        else:
            sql_query = sql
        min_p = start
        counter = 0
        sql_params = replace_sql.copy()
        while True:
            max_p = min_p + step
            max_p = min(max_p, end) if end else max_p
            sql_params.update({
                "table_name": sql,
                "pk": pk_column,
                "start": min_p,
                "end": max_p
            })
            data = self.read_sql(sql=sql_query, replace_params=sql_params, chunk_size=chunk_size, **read_sql)

            if not chunk_size:
                count = data.num_rows()
                counter += count
                yield data
            else:
                for df in data:
                    yield df
            if callable(callable_each_batch):
                callable_each_batch(counter)
            min_p += step
            if max_p >= end:
                logger.info("Stop get data because reach end value")
                break
