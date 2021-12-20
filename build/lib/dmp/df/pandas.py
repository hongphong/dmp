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

import pandas
import pandas as pd
import pandas_flavor as pf

from dmp.df.base import Pipeline
from dmp.df.exceptions import *
from dmp.helper.log import logger
from dmp.helper.sql import replace_template

try:
    import janitor
except BaseException:
    pass


@pf.register_dataframe_accessor('dmp')
class PandasPipeline(Pipeline):
    """
    Library working with pandas
    You can use pyjanitor to work with pandas
    Docs: https://pyjanitor.readthedocs.io/index.html

    .. code-block:: python
        df = PandasPipeline.read_dict(...).df
        # using janitor
        df
    Example Pyjanitor:
    https://github.com/pyjanitor-devs/pyjanitor/tree/dev/examples
    """

    MODE_APPEND = "append"
    MODE_OVERWRITE = "replace"
    PLOTTING_MATPLOTLIB = 'matplotlib'
    PLOTTING_PLOTLY = 'plotly'
    MAX_SAMPLE = 100000
    pd.options.plotting.backend = PLOTTING_MATPLOTLIB
    DEFAULT_BATCH_SIZE = 10000

    @property
    def df(self):
        """
        Get final dataframe result
        Returns:
        """
        if not isinstance(self._df_result, pd.DataFrame):
            raise DfNotInitialized()
        return self._df_result

    def read_sql(self, sql: any, log: bool = True,
                 replace_params: dict = {}, concat_params={}, pandas_read_params={}, inplace=True):
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
            concat_params: concat params for pandas concat function. Used when extract from multi queries
            pandas_read_params: params for read_sql function of pandas
        Returns:

        """
        if type(sql) is str:
            sql = replace_template(sql, replace_params)
            sql = sql.strip().rstrip(";")
            if log:
                logger.info("Run query:\n %s" % sql)
            df = pandas.read_sql(sql, con=self.db_sql_engine, **pandas_read_params)
            return self.update_df(df) if inplace else df
        elif type(sql) is list:
            df = pandas.DataFrame()
            for s in sql:
                df = pandas.concat([df, self.extract_sql(s, log, replace_params, **pandas_read_params)],
                                   **concat_params)
            return self.update_df(df) if inplace else df
        raise Exception("Your sql is invalid")

    def read_sql_table(self, table: any, concat_params={}, pandas_read_params={}, inplace=True):
        """
        Extract data from database by table name
        You can extract from multi queries and the final result is union all from result of each query
        Args:
            table: extract from one or a list of table
            concat_params: concat params for pandas concat function. Used when extract from multi queries
            pandas_read_params: params for read_sql function of pandas
        Returns:

        """
        if type(table) is str:
            logger.info("Extract data from table:\n %s" % table)
            df = pandas.read_sql_table(table, con=self.db_sql_engine, **pandas_read_params)
            return self.update_df(df) if inplace else df
        elif type(table) is list:
            df = pandas.DataFrame()
            for s in table:
                df = pandas.concat(self.extract_sql_table(s, **concat_params, **pandas_read_params))
            return self.update_df(df) if inplace else df

        raise Exception("Your table is invalid")

    def read_mongodb(self, collection, database=None, query={}, return_cursor=False, inplace=True,
                     chunk_size=DEFAULT_BATCH_SIZE, checkpoint=None, limit=None, query_params={},
                     read_dict_params={}):
        """
        read data from mongodb
        Args:
            collection: collection of mongodb
            database: database of mongodb
            query: query to get data
            return_cursor: return query cursor if True
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
            chunk_size: get data from cursor and split batch
            checkpoint: function callable for each batch. Using this function if you want to save database or file from each batch
            query_params:

        Returns:

        """
        db = self.db_wf_connection.get_wf_db()
        data = db.get_collection(mongo_collection=collection, mongo_db=database).find(query, **query_params.copy())
        if limit:
            data = data.limit(limit)
        if return_cursor:
            return data
        if callable(checkpoint):
            return PandasPipeline.read_batch(data, batch_size=chunk_size, checkpoint=checkpoint, from_dict_params=read_dict_params.copy())
        else:
            df = PandasPipeline.read_batch(data, batch_size=chunk_size, inplace=False, from_dict_params=read_dict_params.copy())
        return (self.update_df(df) if inplace else df)

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
        Save data to database (support only some database that supported by SQLAlchemy)
        Args:
            table: table will be inserted
            to_wf_connection_id: connection that defined on Workflow
            to_connection_engine: connection engine if you are not using workflow
        Returns:

        """
        from dmp.helper.connection import WfConnection
        table = table.strip()
        schema = schema.strip()
        pd_df = self.df
        if table.find('.') > 0:
            schema = table.split('.')[0]
            table = table.split('.')[1]
        if to_wf_connection_id.strip() != "":
            db_sql_engine = WfConnection(
                to_wf_connection_id if to_wf_connection_id else self.connection_id).get_wf_db().get_sqlalchemy_engine()
            logger.info(
                'Insert data to db: %s database/schema: %s, table: %s by Pandas engine' % (
                    db_sql_engine.url.host, schema, table))
        elif to_connection_engine:
            db_sql_engine = to_connection_engine
        else:
            db_sql_engine = self.db_sql_engine

        if type(sql_before) is str and sql_before.strip() != "":
            sql_before = sql_before.strip().rstrip(";")
            sql_before = sql_before.split(';\n')
            logger.info("Execute sql before insert into database")
            for k in sql_before:
                if k.strip() != '':
                    logger.info("Query execute before insert: %s" % k)
                    db_sql_engine.execute(statement=k)
        elif callable(sql_before):
            logger.info("Execute function before insert into database")
            sql_before(db_sql_engine)
        logger.info("Start insert into database by Pandas...")
        write_kwargs = write_params if type(write_params) is dict else {}
        if schema is not None and isinstance(schema, str) and schema != "":
            write_kwargs['schema'] = schema
        logger.info("table: %s, db: %s", table, schema)
        pd_df.to_sql(name=table,
                     con=db_sql_engine,
                     if_exists=mode,
                     index=index,
                     **write_kwargs
                     )
        if type(sql_after) is str and sql_after.strip() != "":
            sql_after = sql_after.strip().rstrip(";")
            sql_after = sql_after.split(';\n')
            logger.info("Execute sql after insert into database")
            for k in sql_after:
                if k.strip() != '':
                    logger.info("Query execute after insert: %s" % k)
                    db_sql_engine.execute(sql=k)
        elif callable(sql_after):
            logger.info("Execute function after insert into database")
            sql_after(db_sql_engine)
        return True

    @classmethod
    def read_dict(cls, data, from_dict_params={}, inplace=False):
        """
        Create a dataframe with data is list of dictionaries
        Args:
            data: list of dictionaries
            mode: append or replace current dataframe that stored on property with result dataframe
            concat_params:
            from_dict_params:

        Returns:

        """
        self = cls()
        df = pd.DataFrame.from_dict(data, **from_dict_params)
        return self.update_df(df) if inplace else df

    @classmethod
    def read_batch(cls, data, batch_size=DEFAULT_BATCH_SIZE, from_dict_params={}, checkpoint=None, inplace=False):
        """
        Create a dataframe with data is list of dictionaries
        Args:
            data: list data
            mode: append or replace current dataframe that stored on property with result dataframe
            concat_params: concat each dataframe from each batch and return union dataframe
            from_dict_params:
            checkpoint: function callable for each batch. Using this function if you want to save database or file from each batch
        Returns:

        """
        self = cls()
        from dmp.helper.utils import generate_batches
        df = pd.DataFrame([])
        n = 0
        for batch in generate_batches(data, batch_size_limit=batch_size):
            new_df = self.read_dict(batch, inplace=False, from_dict_params=from_dict_params)
            if callable(checkpoint):
                checkpoint(new_df)
            else:
                df = pd.concat([df, new_df])
            n += len(batch)
            logger.info("Loaded data from batch with number items: %s" % n)
        return self if callable(checkpoint) else (self.update_df(df) if inplace else df)

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
        self.update_df(df)
        return self

    @classmethod
    def read_csv(cls, filepath_or_buffer, **read_params):
        self = cls()
        self._df_result = pd.read_csv(filepath_or_buffer, **read_params)
        return self

    @classmethod
    def read_excel(cls, io, **read_params):
        self = cls()
        read_params['engine'] = read_params.get('engine', 'openpyxl')
        self._df_result = pd.read_excel(io, **read_params)
        return self

    @classmethod
    def read_json(cls, path_or_buf, **read_params):
        self = cls()
        self._df_result = pd.read_json(path_or_buf, **read_params)
        return self

    @pf.register_dataframe_method
    def rename_cols(self, columns: dict = None, inplace: bool = True):
        """
        Rename columns of dataframe
        Args:
            columns: (dict) define columns will be rename
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        df = self.df if isinstance(self, PandasPipeline) else self
        if type(columns) is dict and columns:
            if isinstance(self, PandasPipeline) and inplace:
                self._df_result = df.rename(columns=columns)
            else:
                return df.rename(columns=columns, inplace=inplace)
        return self

    @pf.register_dataframe_method
    def normalize_cols(self, columns: list = None, inplace: bool = True):
        """
        Normalize column name of dataframe with removing accents, symbols and lowercase
        Args:
            columns: (list)
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        from dmp.helper.sql import clean_col_name
        df = self.df if isinstance(self, PandasPipeline) else self
        if type(columns) is list and columns:
            new_cols = [clean_col_name(col) for col in df.columns if col in columns]
        else:
            new_cols = [clean_col_name(col) for col in df.columns]

        if inplace:
            df.columns = new_cols
            if isinstance(self, PandasPipeline):
                self._df_result = df
            return self
        else:
            new_df = df.copy()
            new_df.columns = new_cols
            return new_df

    @pf.register_dataframe_method
    def rmv_cols(self, columns: any, inplace: bool = False, **remove_kwargs):
        """
        Remove some columns of dataframe
        Args:
            columns: Columns will be removed
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
            **remove_kwargs: params for drop function of pandas
        Returns:

        """
        df = self.df if isinstance(self, PandasPipeline) else self
        if isinstance(self, PandasPipeline) and inplace:
            df.drop(columns, inplace=True, axis=remove_kwargs.get('axis', 1), **remove_kwargs)
            return self
        else:
            return df.drop(columns, inplace=inplace, axis=remove_kwargs.get('axis', 1), **remove_kwargs)

    @pf.register_dataframe_method
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
        input_df = self.df if isinstance(self, PandasPipeline) else self
        if not inplace:
            df = input_df.copy()
        else:
            df = input_df

        for column, config in columns.items():
            config = config if type(config) is tuple or type(config) is list else [config]
            d_type = config[0]
            params = config[1] if len(config) > 1 else {}
            d_type = d_type.strip()
            if d_type in ['int', 'int32', 'int64', 'float', 'float32', 'float64']:
                df[column] = pandas.to_numeric(df[column], **params)
            elif d_type == 'datetime':
                df[column] = pandas.to_datetime(df[column], **params)
            elif d_type == 'timedelta':
                df[column] = pandas.to_timedelta(df[column], **params)
            elif d_type in ['str', 'string']:
                df[column] = df[column].astype(str)
            elif d_type == 'pickle':
                df[column] = pandas.to_pickle(df[column], **params)

        if isinstance(self, PandasPipeline) and inplace:
            self._df_result = df
            return self
        else:
            return df

    @pf.register_dataframe_method
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
        if isinstance(self, PandasPipeline):
            df = self.df if inplace is False else self.df
        else:
            df = self
        df = df.copy() if not inplace else df
        for k, config in columns.items():
            if callable(config):
                df[k] = config(df)
            else:
                df[k] = config
        return self if (isinstance(self, PandasPipeline) and inplace is True) else df

    @pf.register_dataframe_method
    def select_cols(self, columns, inplace=False):
        """
        Select some fields of dataframe
        Other fields will be removed from Dataframe if inplace is True
        Args:
            columns: Columns will be selected. Example: ['test'] or string 'testA, testB'
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        df = self.df if isinstance(self, PandasPipeline) else self
        columns = [k.strip() for k in columns.split(',')] if type(columns) is str else columns
        if '*' in columns:
            columns = df.columns
        if isinstance(self, PandasPipeline) and inplace:
            self._df_result = df[columns]
            return self
        else:
            if inplace:
                return df.rmv_cols(columns=[k for k in df.columns if k not in columns], inplace=True)
            return df[columns]

    @pf.register_dataframe_method
    def update_by(self, replace: dict, inplace=False):
        """
        Replace given values in Dataframe with new value.

        Example
        -------
        .. code-block:: python
        replace = {
                                "column_name": (df['ColumnA'] > 0, 100)
                            }
        or using callback
        replace = {
                                "column_name": (lambda df: df['columnA'] > 100, 100)
                            }
        Args:
            replace: replace params
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        df = self.df if isinstance(self, PandasPipeline) else self
        df = df.copy() if not inplace else df
        for column, config in replace.items():
            if len(config) < 2:
                raise Exception("new value for column: %s is missing" % column)
            if column not in df.columns:
                raise Exception("Column [%s] is not exist" % column)
            condition = config[0]
            if callable(condition):
                condition = condition(df)
            new_value = config[1]
            df.loc[condition, column] = new_value

        if isinstance(self, PandasPipeline) and inplace:
            self._df_result = df
            return self
        else:
            return df

    @pf.register_dataframe_method
    def delete_by(self, condition: any, inplace=False):
        """
        Remove data of dataframe with given condition

        Example
        -------
        .. code-block:: python
        condition = df['ColumnA'] > 0
        or using callback
        replace = lambda df: df['columnA'] > 100
        Args:
            condition: Condition for removing data of dataframe
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        df = self.df if isinstance(self, PandasPipeline) else self

        if isinstance(self, PandasPipeline) and inplace:
            self._df_result = df.drop(df[(condition(df) if callable(condition) else condition)].index, inplace=False)
            return self
        else:
            return df.drop(df[(condition(df) if callable(condition) else condition)].index, inplace=inplace)

    @pf.register_dataframe_method
    def filter_by(self, condition: any, inplace=False):
        """
        Only get data of dataframe with given condition

        Example
        -------
        .. code-block:: python
        condition = df['ColumnA'] > 0
        or using callback
        replace = lambda df: df['columnA'] > 100
        Args:
            condition: Condition for filtering data of dataframe
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        df = self.df if isinstance(self, PandasPipeline) else self
        if callable(condition):
            df = df[condition(df)]
        else:
            df = df[condition]
        if isinstance(self, PandasPipeline) and inplace:
            self._df_result = df
            return self
        else:
            return df

    @pf.register_dataframe_method
    def query_by(self, query: str, inplace=False):
        """
        Using query string to filter data
        Ex: query('columnA > 1')
        Args:
            query: (str) Query string to filter data
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        df = self.df if isinstance(self, PandasPipeline) else self
        df = df.query(query)
        if isinstance(self, PandasPipeline) and inplace:
            self._df_result = df
            return self
        else:
            return df

    @pf.register_dataframe_method
    def query_sql(self, sql, temp_table, inplace=False):
        """
        Query data by sql language

        Example
        -------

        .. code-block:: python
            df = df.query_by('select * from temp1', 'temp1')
            Documents: https://github.com/zbrookle/dataframe_sql
        Args:
            sql: query string to working with dataframe
            temp_table: name of table that will be registered as temp table
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.

        Returns:

        """
        df = self.df if isinstance(self, PandasPipeline) else self
        from dataframe_sql import register_temp_table, query
        register_temp_table(df, temp_table)
        new_df = query(sql)
        if isinstance(self, PandasPipeline) and inplace:
            self._df_result = new_df
            return self
        else:
            return new_df

    @pf.register_dataframe_method
    def alert(self, condition: any):
        """
        Alert data with condition
        Args:
            condition: Condition for filtering data of dataframe
        Returns:
        """
        if type(condition) is str:
            df = self.query_by(condition)
        else:
            df = self.filter_by(condition=condition, inplace=False)
        if not df.empty:
            print('Unexpected data:\n%s\n%s' % (
                df.head(10), ('Condition is: %s' % condition) if type(condition) is str else ''))
            raise Exception("Your data have some unexpected data")

    @pf.register_dataframe_method
    def set_plotting_backend(self, default=PLOTTING_PLOTLY):
        """
        Set default plotting engine
        Args:
            default: engine for plotting: plotly, holoviews, matplotlib
        Returns:
        """
        logger.info("Set plotting backend to: [%s]" % default)
        df = self.df if isinstance(self, PandasPipeline) else self
        pd.options.plotting.backend = default
        return df

    @pf.register_dataframe_method
    def show(self, number=100, pretty=False):
        df = self.df if isinstance(self, PandasPipeline) else self
        if not pretty:
            print("\n\n ", df.head(number))
        else:
            pd.set_option('display.max_colwidth', None)
            print(df.head(number))

    @pf.register_dataframe_method
    def show_boxplot(self, column, **kwargs):
        """
        Draw boxplot with seaborn for one or many column
        Args:
            column: column of dataframe
            **kwargs:
        Returns:
        """
        df = self.df if isinstance(self, PandasPipeline) else self
        import seaborn as sns
        if type(column) is list:
            for col in column:
                sns.boxplot(df[col], **kwargs)
            return
        return sns.boxplot(df[column], **kwargs)

    @pf.register_dataframe_method
    def seaborn(self, function, **kwargs):
        """
        Call seaborn function by string
        Example
        -------
        .. code-block:: python
        df.seaborn('barplot',y='model',x='hp')

        Args:
            function:
            **kwargs:
        Returns:
        """
        import seaborn as sns
        df = self.df if isinstance(self, PandasPipeline) else self
        return getattr(sns, function)(data=df, **kwargs)

    @pf.register_dataframe_method
    def show_ts_data(self, date_column: str, agg_method: dict,
                     date_type: str = 'D',
                     fill_missing=True,
                     fill_missing_na=None,
                     width=700,
                     height=400,
                     plot_args=None,
                     return_df=False
                     ):
        """
        Visualize timeseries data with given date and value column

        Example:
        -------
        .. code-block:: python
        <df or pipeline instance>.show_ts_data(date_column='datetime', agg_method={
                    'test': ['min']
        }, fill_missing_na={
            'test_min': 0,
            'test_max': 0,
        })
        Args:
            date_column: date column
            agg_method: methods used for aggregating data
            date_type: group by DateOffset: https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
            fill_missing: auto fill missing date with zero values
            fill_missing_na: fill na value for missing date
            x_day_interval: group x-label with interval day
            figsize: size of chart
            plot_args: params for plot function

        Returns:
        """
        df = self.df if isinstance(self, PandasPipeline) else self
        df = df.copy()
        date_col = 'group_date'
        if date_type not in ['D', 'M', 'Y', 'H', 'T', 'A', 'Q', 'B', 'W']:
            raise Exception('DayOffset [%s] is not supported' % date_type)

        def convert_date(df, date_column, date_type=date_type):
            df[date_col] = df[date_column].dt.to_period(date_type)
            return df

        df = convert_date(df, date_column, date_type)

        if agg_method is not None:
            df = df.groupby(date_col).agg(agg_method)
        else:
            df = df.groupby(date_col).sum()
        df.reset_index(inplace=True)
        df.normalize_cols(inplace=True)

        # fill missing day
        if fill_missing:
            min_d = str(df[date_col].min())
            max_d = str(df[date_col].max())
            if date_type.upper() == 'W':
                min_d = min_d.split('/')[0]
                max_d = max_d.split('/')[0]
            fill_df = pd.DataFrame(pd.date_range(str(min_d), str(max_d), freq=date_type), columns=[date_col])
            fill_df = convert_date(df=fill_df, date_column=date_col, date_type=date_type)
            new_df = pd.merge(fill_df, df, how='outer', left_on=date_col, right_on=date_col)
            cols = new_df.dtypes.to_dict()
            fill_na = {}
            for c, v in cols.items():
                v = str(v)
                if v.find('int') >= 0 or v.find('float') >= 0:
                    fill_na[c] = 0
                elif v.find('object') >= 0:
                    fill_na[c] = ''
            if fill_missing_na and type(fill_missing_na) is dict:
                fill_na.update(fill_missing_na)
            new_df.fillna(value=fill_na, inplace=True)
            df = new_df

        df = df.sort_values([date_col], ascending=True)
        if type(plot_args) is not dict:
            plot_args = {}
        plot_args['kind'] = plot_args.get('kind', 'line')
        df.normalize_cols(inplace=True)
        cols = list(df.columns)
        cols.remove(date_col)
        df[date_col] = pd.to_datetime(df[date_col].astype(str))
        old_backend = pd.options.plotting.backend
        if old_backend == PandasPipeline.PLOTTING_PLOTLY:
            import plotly.graph_objects as go
            fig = go.Figure()
            fig.update_layout(width=width, height=height)
        elif old_backend == PandasPipeline.PLOTTING_MATPLOTLIB:
            import matplotlib
            px = 1 / matplotlib.rcParams['figure.dpi']
            matplotlib.rcParams['figure.figsize'] = [width * px, height * px]
        plot = df.plot(x=date_col, y=cols, **plot_args)
        if return_df:
            return df
        else:
            return plot

    @pf.register_dataframe_method
    def detect_invalid_type(self, column: str, value_type: str, index_column: str = None):
        """
        Detect wrong value type in one column

        Example:
        -------
        .. code-block:: python
        <df or pipeline instance>.detect_invalid_type(columns='test', value_type='int32')

        Args:
            column: column that need to be investigated
            value_type: value type of column.
                        List value type ['int', 'int32', 'int64', 'float', 'float32', 'float64', 'datetime','timedelta']
            index_column: index column of dataframe

        Returns: pd.Dataframe
        """
        logger.info("Detect invalid value in column [%s] with type [%s]" % (column, value_type))
        select_column = [column]
        if index_column:
            select_column.insert(0, index_column)
        df = self.df if isinstance(self, PandasPipeline) else self
        df = df[select_column]
        df['origin_%s' % column] = df[column]
        df = df.cast_cols(columns={
            column: (value_type, {"errors": "coerce"})
        }, inplace=False)
        if index_column:
            df = df.set_index(index_column)
        return df[df.isnull().any(axis=1)]

    @pf.register_dataframe_method
    def detect_missing_ts(self, date_column: str, date_type: str = 'D'):
        """
        Detect missing datetime on a timeseries column
        Args:
            date_column: timeseries column
            date_type: DAILY, WEEKLY, MONTHLY, YEARLY
        Returns:

        """
        df = self.df if isinstance(self, PandasPipeline) else self
        df = df.copy()
        date_col = 'group_date'
        if date_type not in ['D', 'M', 'Y', 'H', 'T', 'A', 'Q', 'B', 'W']:
            raise Exception('DayOffset [%s] is not supported' % date_type)

        def convert_date(df, date_column, date_type=date_type):
            df[date_col] = df[date_column].dt.to_period(date_type)
            return df

        df = convert_date(df, date_column, date_type)
        df = df.groupby(date_col).count()

        df.reset_index(inplace=True)

        min_d = str(df[date_col].min())
        max_d = str(df[date_col].max())
        if date_type.upper() == 'W':
            min_d = min_d.split('/')[0]
            max_d = max_d.split('/')[0]
        logger.info("Start fill missing from %s to %s" % (min_d, max_d))
        fill_df = pd.DataFrame(pd.date_range(str(min_d), str(max_d), freq=date_type), columns=[date_col])
        fill_df = convert_date(df=fill_df, date_column=date_col, date_type=date_type)
        new_df = pd.merge(fill_df, df, how='outer', left_on=date_col, right_on=date_col)

        new_df = new_df[new_df[date_column].isna()][date_col]
        print("The datetime are missing:")
        print('\n', new_df.head(100))
        return new_df

    @pf.register_dataframe_method
    def open_profiling_tool(self, columns=None, sample=MAX_SAMPLE, return_prof=False):
        """
        Show data profiling by pandas_profiling library

        Example:
        -------
        .. code-block:: python
        <df or pipeline instance>.show_profiling(columns=['test'])

        Args:
            columns: columns will be profiled
            sample: number of rows to profiling. If sample is None, get all rows

        Returns:
        """
        from pandas_profiling import ProfileReport
        df = self.df if isinstance(self, PandasPipeline) else self
        try:
            if columns is None:
                columns = df.columns
        except BaseException:
            columns = df.columns

        sample = min(sample, len(df.index))
        prof = ProfileReport(df[columns].sample(sample) if sample > 0 else df[columns])
        if return_prof:
            return prof
        prof.to_widgets()

    @pf.register_dataframe_method
    def open_pivot_tool(self, sample=MAX_SAMPLE):
        """
        Pivot Tool with interactive GUI
        Docs: https://pivottable.js.org/examples/
        Returns:
        """
        from pivottablejs import pivot_ui
        df = self.df if isinstance(self, PandasPipeline) else self
        sample = min(sample, len(df.index))
        return pivot_ui(df.sample(sample))

    @pf.register_dataframe_method
    def open_dtale_tool(self, sample=MAX_SAMPLE, **kwargs):
        """
        Open dtale tool
        Docs: https://github.com/man-group/dtale
        Args:
            sample:  number of rows to profiling. If sample is None, get all rows
        Returns:
        """
        import dtale
        import dtale.app as dtale_app
        df = self.df if isinstance(self, PandasPipeline) else self
        dtale_app.JUPYTER_SERVER_PROXY = True
        sample = min(sample, len(df.index))
        d = dtale.show(df.sample(sample), **kwargs)
        return d

    @pf.register_dataframe_method
    def open_table_tool(self, sample=MAX_SAMPLE, show_toolbar=True, **kwargs):
        """
        Show table with interactive
        Args:
            sample: number of rows to profiling. If sample is None, get all rows
        Returns:
        """
        import qgrid
        df = self.df if isinstance(self, PandasPipeline) else self
        sample = min(sample, len(df.index))
        qg = qgrid.show_grid(df.sample(sample), show_toolbar=show_toolbar, **kwargs)
        return qg

    @pf.register_dataframe_method
    def export_df_table_tool(self, table_instance, inplace=False):
        """
        Get df result after change from open_table_tool

        Example
        -------

        .. code-block:: python
            pandas_pipeline = PandasPipeline().read_dict([{"test":1}])
            grid = pandas_pipeline.open_table_tool()
            df_update = pandas_pipeline.export_df_table_tool(grid)

        Args:
            table_instance:
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        new_df = table_instance.get_changed_df()
        if isinstance(self, PandasPipeline) and inplace:
            self._df_result = new_df
            return self
        else:
            return new_df
