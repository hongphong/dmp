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
from dmp.helper.utils import convert_int


class PdMongodb(PandasPipeline):
    """
    Base on PandasPipeline and add some methods to work with mongodb
    """

    def read_mongodb(self, collection, database=None, query={}, limit=None, query_params={},
                     return_cursor=False,
                     chunk_size=None,
                     process_item=None,
                     read_dict_params={}):
        """
                read data from mongodb
                Args:
                    collection: collection of mongodb
                    database: database of mongodb
                    query: query to get data
                    limit: limit when query mongodb
                    query_params: query params for mongodb
                    return_cursor: return query cursor if True
                    chunk_size: get data from cursor and split batch
                    process_item: callable
                                    function for process each row before loading to dataframe
                    read_dict_params: params for read_dict method

                Returns:

                """
        db = self.db_wf_connection.get_wf_db()
        data = db.get_collection(mongo_collection=collection, mongo_db=database).find(query, **query_params.copy())
        if limit:
            data = data.limit(limit)
        if return_cursor:
            return data
        if convert_int(chunk_size) > 0:
            return PandasPipeline.read_batch_inter(data, batch_size=chunk_size,
                                                   read_dict_params=read_dict_params.copy(), process_item=process_item)
        else:
            df = PandasPipeline.read_batch(data, read_dict_params=read_dict_params.copy(), process_item=process_item)
            return self.update_df(df)
