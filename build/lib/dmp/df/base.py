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

from dmp.helper.connection import WfConnection
from dmp.helper.log import logger


class Pipeline():
    MODE_APPEND = "append"
    MODE_OVERWRITE = "replace"

    def __init__(self, df=None, **kwargs):
        self._df_result = df
        self._db_sql_engine = None
        self._db_wf_connection = None

    @property
    def df(self):
        """
        Get final dataframe result
        Returns:
        """
        raise NotImplementedError()

    def init_new_df(self):
        raise NotImplementedError()

    @classmethod
    def connect_db(cls, wf_connection_id="", connection_engine=None):
        """
        Connect database with connection_id of database on Workflow or sqlalchemy connection_engine
        Args:
            wf_connection_id: connection_id of database on Workflow
            connection_engine: sqlalchemy connection engine
        Returns:

        """
        self = cls()
        if not self._db_sql_engine:
            try:
                if connection_engine:
                    self._db_sql_engine = connection_engine
                else:
                    self._db_wf_connection = WfConnection(wf_connection_id)
                    try:
                        self._db_sql_engine = self._db_wf_connection.get_wf_db().get_sqlalchemy_engine()
                    except BaseException as e:
                        self._db_sql_engine = None
            except BaseException as e:
                logger.exception(str(e))
                raise Exception("init database engine fail because: %s" % str(e))
        return self

    @property
    def db_sql_engine(self):
        """
        Return database engine.
        To init db_sql_engine, you need to run connect_db function first
        Returns:
        """
        if self._db_sql_engine:
            return self._db_sql_engine
        raise Exception(
            "You need to run connect_db function first to init database engine before get db_sql_engine property.\n"
            " Example: PandasHook.connect_db(wf_connection_id or db_sql_engine).db_sql_engine")

    @property
    def db_wf_connection(self)->WfConnection:
        """
        Return workflow connection database.
        To init db_sql_engine, you need to run connect_db function first
        Returns:
        """
        if self._db_wf_connection:
            return self._db_wf_connection
        raise Exception(
            "You need to run connect_db function first to init database engine before get db_sql_engine property.\n"
            " Example: PandasHook.connect_db(wf_connection_id or db_sql_engine).db_sql_engine")

    def reset_df(self):
        """
        Reset final dataframe result
        Returns:
        """
        self._df_result = self.init_new_df()
        return self

    def update_df(self, df):
        """
        Update final dataframe with new df
        Returns:
        """
        self._df_result = df
        return self

    def read_sql(self, sql: any, log: bool = True,
                 replace_params: dict = {}, **kwargs):
        """
        Extract data from database by sql query
        You can extract from multi queries and the final result is union all from result of each query
        Args:
            sql: Using SQl to get data from database. Sql param may be a list in case extract from multi queries.
                 Result dataframe will be concatenated into one dataframe
            log: log info queries or not
            replace_params: replace params on SQL string with specific variables. For example:
                - sql: select * from example where a > {date_string}
                - replace_params: {'date_string': '1970-01-01'}
        Returns:

        """
        raise NotImplementedError()

    def read_sql_table(self, table: any):
        """
        Extract data from database by table name
        You can extract from multi queries and the final result is union all from result of each query
        Args:
            table: extract from one or a list of table
        Returns:

        """
        raise NotImplementedError()

    def save_to_db_sql(self,
                   table: str,
                   to_wf_connection_id: str = "",
                   to_connection_engine=None,
                   mode=MODE_APPEND,
                   schema: str = "",
                   index: bool = False,
                   write_params: dict = None,
                   sql_before=None,
                   sql_after=None,
                   ):

        """
        Save dataframe to database
        Args:
            table: output table name
            to_wf_connection_id: output connection database id (defined on workflow)
            to_connection_engine: output connection engine
                if to_wf_connection_id and to_connection_engine are missing, current database will be used
            mode: append or replace target table
            schema: schema for target table
            index: insert index of dataframe into target table or not
            write_params: write params for pandas to_sql function
            sql_before: execute a sql query before save data
            sql_after: execute a sql query after save data
        Returns:

        """
        raise NotImplementedError()

    @classmethod
    def read_dict(cls, data, from_dict_params={}):
        """
        Create a dataframe with data is list of dictionaries
        Args:
            data: list of dictionaries
            mode: append or replace current dataframe that stored on property with result dataframe
            concat_params:
            from_dict_params:

        Returns:

        """
        raise NotImplementedError()

    @classmethod
    def read_df(cls, df):
        """
        Load data from a dataframe and stored into a property
        Args:
            df:
            mode:
            concat_params:

        Returns:

        """
        self = cls()
        self._df_result = df
        return self

    @classmethod
    def read_csv(cls, filepath_or_buffer, **read_params):
        raise NotImplementedError()

    @classmethod
    def read_excel(cls, io, **read_params):
        raise NotImplementedError()

    @classmethod
    def read_json(cls, path_or_buf, **read_params):
        raise NotImplementedError()

    def concat(self, df, inplace=False, **kwargs):
        """
        Concat current dataframe that stored on property with new dataframe
        Args:
            df: dataframe will be concat with current dataframe
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
            **kwargs: params of pandas concat function
        Returns:

        """
        raise NotImplementedError()

    def rename_cols(self, columns: dict = None, inplace: bool = True):
        """
        Rename columns of dataframe
        Args:
            columns: (dict) define columns will be rename
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        raise NotImplementedError()

    def normalize_cols(self, columns: list = None, inplace: bool = True):
        """
        Normalize column name of dataframe with removing accents, symbols and lowercase
        Args:
            columns: (list)
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        raise NotImplementedError()

    def rmv_cols(self, columns: any, inplace: bool = True, **remove_kwargs):
        """
        Remove some columns of dataframe
        Args:
            columns: Columns will be removed
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
            **remove_kwargs: params for drop function of pandas
        Returns:

        """
        raise NotImplementedError()

    def cast_cols(self, columns, inplace=False):
        """
        Convert type for each column
        You can pass params for convert func: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_numeric.html
        Args:
             cast_column:  format: {
                "column_name": ("int", {"errors": "raise", "downcast": 'float'})
             }.
                errors {‘ignore’, ‘raise’, ‘coerce’}, default ‘raise’}
                    - If ‘raise’, then invalid parsing will raise an exception.
                    - If ‘coerce’, then invalid parsing will be set as NaN.
                    - If ‘ignore’, then invalid parsing will return the input.
                downcast{‘integer’, ‘signed’, ‘unsigned’, ‘float’}, default None
                    -If not None, and if the data has been successfully cast to a numerical dtype (or if the data was numeric to begin with),
                    downcast that resulting data to the smallest numerical dtype possible according to the following rules:
                    -‘integer’ or ‘signed’: smallest signed int dtype (min.: np.int8)
                    -‘unsigned’: smallest unsigned int dtype (min.: np.uint8)
                    -‘float’: smallest float dtype (min.: np.float32)
                As this behaviour is separate from the core conversion to numeric values, any errors raised during the downcasting will be surfaced regardless of the value of the ‘errors’ input.
                In addition, downcasting will only occur if the size of the resulting data’s dtype is strictly larger than the dtype it is to be cast to, so if none of the dtypes checked satisfy that specification, no downcasting will be performed on the data.
                value type must be ['int', 'int32', 'int64', 'float', 'float32', 'float64', 'datetime','timedelta']
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        raise NotImplementedError()

    def add_cols(self, columns: dict, inplace: bool = False, **kwargs):
        """
        Add new columns
        Args:
            columns: Add new columns. Example format: {
                "new_column": lambda df: df["datetime"].dt.date. Value can be a callable function or other value
             }
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
            **kwargs:
        Returns:
        """
        raise NotImplementedError()

    def select_cols(self, columns, inplace=False):
        """
        Select some fields of dataframe
        Other fields will be removed from Dataframe if inplace is True
        Args:
            columns: Columns will be selected
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        raise NotImplementedError()

    def update_by(self, replace: dict, inplace=False):
        """
        Replace given values in Dataframe with new value.
        Args:
            replace: replace params
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        raise NotImplementedError()

    def delete_by(self, condition: any, inplace=False):
        """
        Remove data of dataframe with given condition
        Args:
            condition: Condition for removing data of dataframe
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        raise NotImplementedError()

    def filter_by(self, condition: any, inplace=False):
        """
        Only get data of dataframe with given condition
        Args:
            condition: Condition for filtering data of dataframe
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        raise NotImplementedError()

    def alert(self, condition: any):
        """
        Alert data with condition
        Args:
            condition: Condition for filtering data of dataframe
        Returns:
        """
        raise NotImplementedError()

    def show(self, number=100, pretty=False):
        raise NotImplementedError()

    def show_boxplot(self, column, **kwargs):
        """
        Draw boxplot with seaborn for one or many column
        Args:
            column: column of dataframe
            **kwargs:
        Returns:
        """
        raise NotImplementedError()

    def show_ts_data(self, date_column: str, agg_method: dict,
                     date_type: str = 'D',
                     fill_missing=True,
                     x_day_interval=5,
                     figsize=(30, 15),
                     plot_args=None
                     ):
        """
        Visualize timeseries data with given date and value column
        Args:
            date_column: date column
            agg_method: methods used for aggregating data
            date_type: group by DateOffset: https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
            fill_missing: auto fill missing date with zero values
            x_day_interval: group x-label with interval day
            figsize: size of chart
            plot_args: params for plot function
        Returns:
        """
        raise NotImplementedError()

    def show_profiling(self, columns=None, sample=10000, return_prof=False):
        """
        Show data profiling by pandas_profiling library
        Args:
            columns: columns will be profiled
            sample: number of rows to profiling. If sample is None, get all rows
        Returns:
        """
        raise NotImplementedError()

    def detect_invalid_type(self, column: str, value_type: str, index_column: str = None):
        """
        Detect wrong value type in one column
        Args:
            column: column that need to be investigated
            value_type: value type of column.
                        List value type ['int', 'int32', 'int64', 'float', 'float32', 'float64', 'datetime','timedelta']
            index_column: index column of dataframe
        Returns: pd.Dataframe
        """
        raise NotImplementedError()

    def detect_missing_ts(self, date_column: str, date_type: str = 'D'):
        """
        Detect missing datetime on a timeseries column
        Args:
            date_column: timeseries column
            date_type: DAILY, WEEKLY, MONTHLY, YEARLY
        Returns:

        """
        raise NotImplementedError()
