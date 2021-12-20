# !/usr/bin/python
#
# Copyright Phong Pham Hong <phongpham1805@gmail.com>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
__author__ = "phongphamhong"

import copy
import os

import pyspark
import pyspark.sql.functions as spark_functions
import pyspark.sql.types as spark_type
from pyspark import SparkConf
from pyspark.sql import SparkSession

from dmp.df.base import Pipeline, WfConnection
from dmp.df.exceptions import *
from dmp.helper.log import logger
from dmp.helper.sql import replace_template
from dmp.helper.utils import generate_batches


class SparkPipeline(Pipeline):
    _instance = {}
    __app_name = ""
    __master = ""
    __mode = ""
    __ENV = {}
    __auto_find_pyspark = False
    __load_config = {}
    __enable_hive_support = True

    MODE_APPEND = "append"
    MODE_OVERWRITE = "overwrite"
    MODE_APPEND_PARTITION = "append_partition"
    MODE_OVERWRITE_PARTITION = "overwrite_partition"
    MODE_TRUNCATE = "truncate"
    DEFAULT_BATCH_SIZE = 100000

    def get_app_name(self):
        return self.__app_name

    def get_master(self):
        return self.__master

    def get_mode(self):
        return self.__mode

    def get_list_instance(self):
        return self._instance

    def get_conn(self):
        return self.session

    def get_config(self):
        return self.__load_config

    def get_env_vars(self):
        return self.__load_config.get("env_vars", {})

    def get_spark_config(self):
        return self.__load_config.get("conf", {})

    def get_py_files(self):
        return self.__load_config.get("py_files", [])

    def __init__(self, wf_config_connection_id: str = "",
                 app_name: str = "spark_pipeline",
                 master: str = "local[4]",
                 mode: str = "",
                 spark_conf: dict = {},
                 env_vars: dict = {},
                 py_files: list = [],
                 auto_find_pyspark: bool = False,
                 enable_hive_support: bool = True):
        """
        Create app spark context

        args:
                    wf_config_connection_id: id of connection
                    app_name: spark app name
                    master: local or YARN
                    mode: mode run on YARN
                    spark_conf: overwrite some default configs of spark.
                                Format: Example:
                                   {
                                        "spark.jars.packages": "mysql:mysql-connector-java:5.1.47,org.elasticsearch:elasticsearch-hadoop:6.4.2",
                                        "spark.dynamicAllocation.enabled": "true",
                                        "spark.dynamicAllocation.maxExecutors": 5,
                                        "spark.executor.cores": 4,
                                        "spark.executor.memory": "2G",
                                        "spark.shuffle.service.enabled": "true",
                                        "spark.files.overwrite": "true",
                                        "spark.sql.warehouse.dir": "/apps/spark/warehouse",
                                        "spark.sql.catalogImplementation": "hive",
                                        "spark.sql.sources.partitionOverwriteMode": "dynamic",
                                        "hive.exec.dynamic.partition.mode": "nonstrict",
                                        "spark.sql.caseSensitive": "true"
                                   }
                    env_vars: add some env variables. Example: {
                        "SPARK_HOME": "/usr/hdp/current/spark2/"
                    }
                    add_py_files: Add some python file path when submit job. Example: ["test.py"]}
                    auto_find_pyspark: True or False. If true, automatic find pyspark environment by findpspark package

        """

        if not pyspark:
            raise ImportError("PySpark not installed. Please install pyspark through pip or download package manually.")
        self.__app_name = app_name.strip()
        self.__master = master.strip()
        self.__mode = mode.strip()
        custom_config = copy.copy(spark_conf if isinstance(spark_conf, dict) else {})
        env_vars = copy.copy(env_vars if isinstance(env_vars, dict) else {})
        add_py_files = py_files if isinstance(py_files, list) else []
        self.__auto_find_pyspark = auto_find_pyspark if isinstance(auto_find_pyspark, bool) else False
        self.__enable_hive_support = enable_hive_support if isinstance(enable_hive_support, bool) else False
        self.__load_config = {}
        connection_id = "" if wf_config_connection_id is None else wf_config_connection_id
        try:
            if connection_id:
                cn = WfConnection(connection_id).get_wf_connection()
                if cn.extra_dejson:
                    self.__load_config = cn.extra_dejson
                else:
                    logger.warn("Config for sparkContext of SparkHook is not set. Using default config")

        except BaseException as e:
            logger.warn("Connection SparkHook is not set. Using default config")
            self.__load_config = {}

        if self.__app_name != "":
            self.__load_config["app_name"] = self.__app_name.strip()
        else:
            self.__app_name = self.__load_config.get("app_name", "Zuka_etl_spark")
        if self.__master != "":
            self.__load_config["master"] = self.__master
        else:
            self.__master = self.__load_config.get("master", "local[*]")
        if self.__mode != "":
            self.__load_config["mode"] = self.__mode
        else:
            self.__mode = self.__load_config.get("mode", "")
        if custom_config:
            sp_cf = self.__load_config.get("conf", {})
            if not isinstance(sp_cf, dict):
                sp_cf = {}
                self.__load_config["conf"] = sp_cf
            self.__load_config["conf"].update(custom_config)
        if env_vars:
            env = self.__load_config.get("env_vars", {})
            if not isinstance(env, dict):
                env = {}
                self.__load_config["env_vars"] = env
            self.__load_config["env_vars"].update(custom_config)
        if add_py_files:
            add = self.__load_config.get("py_files", [])
            if not isinstance(add, list):
                self.__load_config["py_files"] = []
            self.__load_config["py_files"] = add_py_files

        return super(SparkPipeline, self).__init__()

    def connect_db(self, wf_connection_id=""):
        """
        Connect database with connection_id of database on Workflow or sqlalchemy connection_engine
        Args:
            wf_connection_id: connection_id of database on Workflow
            connection_engine: sqlalchemy connection engine
        Returns:

        """
        if not self._db_sql_engine:
            try:
                self._db_wf_connection = WfConnection(wf_connection_id)
                self._db_sql_engine = self._db_wf_connection.get_wf_db().get_sqlalchemy_engine()
            except BaseException as e:
                logger.exception(str(e))
                raise Exception("init database engine fail because: %s" % str(e))
        return self

    @property
    def session(self) -> SparkSession:
        """
        create a Spark Context by SparkSession

            return: pyspark.sql.session.SparkSession

        """
        if not isinstance(SparkPipeline._instance, dict):
            SparkPipeline._instance = {}
        ins = SparkPipeline._instance.get("_spark_session")
        if (not ins or not self.is_active):
            # import os global variable
            try:
                for k, v in self.get_env_vars().items():
                    logger.info("SET VARIABLE: %s:%s" % (k, v))
                    os.environ[k] = v
            except BaseException as e:
                logger.info("Cannot import variable spark because:\n%s" % logger.exception(e))
                pass

            logger.info("[Spark] Create main context ...")

            if self.__auto_find_pyspark:
                logger.info("Enabled auto find pyspark...")
                import findspark
                findspark.init()
            ss = SparkSession.builder.appName(self.__app_name)
            conf = SparkConf()
            logger.info("[Spark] set master: %s" % self.__master)
            conf.setMaster(self.__master)
            def_conf = copy.deepcopy(self.get_spark_config())
            if self.__mode:
                def_conf["spark.submit.deployMode"] = self.__mode

            if def_conf:
                for c, v in def_conf.items():
                    logger.info("[Spark] set config: %s: %s" % (c, v))
                    conf = conf.set(c, v)
            if self.__enable_hive_support:
                logger.info("[Spark] enable HiveSupport: true")
                SparkPipeline._instance["_spark_session"] = ss.config(conf=conf).enableHiveSupport().getOrCreate()
            else:
                SparkPipeline._instance["_spark_session"] = ss.config(conf=conf).getOrCreate()
            py_file = self.get_py_files()
            if py_file:
                logger.info("[Spark] start add file: %s" % py_file)
                for f in py_file:
                    SparkPipeline._instance["_spark_session"].sparkContext.addPyFile(f)
            return SparkPipeline._instance["_spark_session"]
        return ins

    @property
    def df(self):
        """
        Get final dataframe result
        Returns:
        """
        if not isinstance(self._df_result, pyspark.sql.DataFrame):
            raise DfNotInitialized()
        return self._df_result

    @property
    def sc(self):
        """
        get spark context from SparkSession

            return: pyspark.context.SparkHook

        """
        return self.session.sparkContext

    @property
    def is_active(self) -> bool:
        """
        check spark_context is running or not

            return:

        """
        import traceback
        try:
            spark = SparkPipeline._instance.get("_spark_session")
            if spark:
                spark.sparkContext.sparkUser()
                return True
            else:
                logger.info("SparkHook have not been initialized")
        except BaseException as e:
            logger.error("SparkHook is stopped. Error:\n %s" % traceback.format_exc())
        return False

    @property
    def spark_funcs(self):
        return spark_functions

    @property
    def spark_dtypes(self):
        """
        return spark data type

            return: pyspark.sql.types

        """
        return spark_type

    @staticmethod
    def replace_template(text: str, params: dict) -> str:
        return replace_template(text, params)

    def run_sql(self, sql: str, log: bool = True, multi: bool = False, replace_params: dict = {}, inplace=False):
        """
        run pyspark sql by sqlContext
            sql:
            return:
        """
        sql = self.replace_template(sql, replace_params)
        sql = sql.strip().rstrip(";")
        if not multi:
            if log:
                logger.info(
                    "Start query sql by sqlContext\n:-----------------------------\n%s\n-----------------------" % sql)
            df = self.session.sql(sql)
            return self.update_df(df) if inplace else df
        for k in sql.split(";"):
            if k.strip():
                if log:
                    logger.info("Run query:\n %s" % k)
                self.session.sql(k)
        return True

    @classmethod
    def stop(cls):
        import traceback
        self = cls()
        try:
            logger.info("[Spark] Stop sparkSession, SparkHook and clear instances...")
            # self.session.sc.stop()
            ins = SparkPipeline._instance.get("_spark_session")
            if ins:
                logger.info("Stop spark context of SparkHook...")
                ins.sparkContext.stop()
                try:
                    logger.info("Stop spark session of SparkHook...")
                    ins.stop()
                except BaseException as e:
                    pass
            SparkPipeline._instance = {}
            SparkPipeline._instance.clear()
            self._instance = {}
            return True
        except BaseException as e:
            logger.error("Error when clear spark object: %s \n trace: " % (traceback.format_exc()))
            return False

    def read_sql(self, sql: any, log: bool = True,
                 replace_params: dict = {}, concat_params={}, spark_options={}, inplace=False):
        """
        Extract data from database by sql query
        You can extract from multi queries and the final result is union all from result of each query
        Args:
            sql: Using SQl to get data from database. Sql param may be a list in case extract from multi queries.
                 Result dataframe will be concatenated into one dataframe
            log: log info queries or not
            replace_params: replace params on SQL string with specific variables. For example:
                - sql: select * from example where a > {date_string}
                - replace_params: {"date_string": "1970-01-01"}
            concat_params: concat params for pandas concat function. Used when extract from multi queries
            spark_options: options for spark when read data from jdbc
        Returns:

        """
        config = self.db_wf_connection.get_wf_connection()

        def connect_sql(table):
            session = self.session
            table = table.strip().rstrip(";")
            if table.strip()[:1] != "(":
                table = "(%s) s" % table
            table = self.replace_template(table, replace_params)
            df_load = (session.read.format("jdbc").option("url",
                                                          config.host)
                       .option("dbtable", table))
            if config.login:
                df_load = df_load.option("user", config.login)
            if config.password:
                df_load = df_load.option("password", config.password)
            df_load = df_load.option("driver", config.extra_dejson.get("extra__jdbc__drv_clsname")) \
                .option("isolationLevel", "READ_UNCOMMITTED")
            if isinstance(spark_options, dict) and spark_options:
                for k, v in spark_options.items():
                    df_load = df_load.option(k, v)
            return df_load.load()

        if type(sql) is str:
            df = connect_sql(sql)
            return self.update_df(df) if inplace else df
        elif type(sql) is list:
            df = self.session.createDataFrame([])
            for s in sql:
                df = df.unionAll(connect_sql(s))
            return self.update_df(df) if inplace else df

        raise Exception("Your sql is invalid")

    def read_dict(self, data, schema=None, inplace=False):
        """
        Create a dataframe with data is list of dictionaries
        Args:
            data: list of dictionaries
            mode: append or replace current dataframe that stored on property with result dataframe
            concat_params:
            from_dict_params:

        Returns:

        """
        if schema is not None:
            df_temp = self.session.createDataFrame(data, schema)
        else:
            df_temp = self.session.createDataFrame(data)
        return self.update_df(df_temp) if inplace else df_temp

    def read_df(self, df):
        """
        Load data from a dataframe and stored into a property
        Args:
            df:
            mode:
            concat_params:

        Returns:

        """
        self.update_df(df)
        return self

    def read_batch(self, data, checkpoint_func=None, batch_size=DEFAULT_BATCH_SIZE,
                   schema=None, inplace=False
                   ):
        """
        Create dataframe with list of data: [{"id" : 1,"name": "phong"},{"id" : 2,"name": "phong"}]

            iter: list data
            checkpoint_func: function process a dataframe. Format: checkpoint_func(index_batch, df_batch)
            return_func: function output data: Format: return_func(spark_session)
            batch_size: data will split into batches, this is config of size of each batch
            schema: Spark Schema DF of Spark
            kwargs:
            return:

        """

        f = generate_batches(data, batch_size_limit=batch_size, remove_none=True)
        sp = self.session
        n = 0
        total = 0
        df = self.session.createDataFrame([])
        for batch in f:
            df_temp = self.read_dict(batch, sp, schema, inplace=False)
            logger.info("Process checkpoint from index: %s" % n)
            if callable(checkpoint_func):
                checkpoint_func(n, df_temp)
            else:
                df = df.unionByName(df_temp)
            total += len(batch)
            logger.info("Import to df: %s done!" % total)
            n += 1
        return self.update_df(df) if inplace else df

    def read_csv(self, file_path, options_csv: dict = {}, inplace=False):
        default_option = {"delimiter": ";",
                          "header": True,
                          "encoding": "utf-8",
                          "escape": "",
                          "multiLine": True}
        default_option.update(options_csv)
        df = self.session.read
        for k, v in default_option.items():
            df = df.option(k, v)
        df = df.csv(file_path)
        return self.update_df(df) if inplace else df

    def rename_cols(self, columns: dict = None, inplace: bool = False):
        """
        Rename columns of dataframe
        Args:
            columns: (dict) define columns will be rename
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """

        if type(columns) is dict and columns:
            df = self.df
            for k, v in columns.items():
                df = df.withColumnRenamed(k, v)
            if inplace:
                self._df_result = df
            else:
                return df
        return self

    def normalize_cols(self, columns: list = None, inplace: bool = False):
        """
        Normalize column name of dataframe with removing accents, symbols and lowercase
        Args:
            columns: (list)
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        from dmp.helper.sql import clean_col_name
        df = self.df
        columns = columns if type(columns) is dict else df.columns
        for k in columns:
            n_col = clean_col_name(k)
            df = df.withColumnRenamed(k, n_col)
        if inplace:
            self._df_result = df
        else:
            return df

    def rmv_cols(self, columns: any, inplace: bool = False):
        """
        Remove some columns of dataframe
        Args:
            columns: Columns will be removed
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:

        """
        df = self.df.drop(columns)
        if inplace:
            self._df_result = df
            return self
        else:
            return df

    def cast_cols(self, columns, inplace=False, lowercase_columns=True):
        """
        Convert type for each column
        Args:
             cast_column:  format: {
                "column_name": "type of value : int, str,..."
             }.
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
            lowercase_columns: lowercase all column name
        Returns:

        """
        df = self.df
        cols = df.columns
        columns = {k.lower(): v.lower() for k, v in columns.items()}
        select = []
        for k in cols:
            l_k = k.lower()
            if columns.get(l_k):
                v = columns.get(l_k)
                select.append("CAST(`%s` as `%s`) as %s" % (k, v, l_k if lowercase_columns else k))
            else:
                select.append("`%s` as `%s`" % (k, l_k if lowercase_columns else k))
        logger.info("Columns will be casted: %s" % columns)
        return df.selectExpr(*select)

    def add_cols(self, columns: dict, inplace: bool = False, **kwargs):
        """
        Add new columns
        Args:
            columns: Add new columns. Example format: {
                "new_column": "<new value>" or lambda df: df["old_column"]. Value can be a callable function or other value
             }
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
            **kwargs:
        Returns:
        """

        df = self.df
        for k, config in columns.items():
            if callable(config):
                df[k] = config(df)
            else:
                df[k] = self.spark_funcs.exp(config)
        return self.update_df(df) if inplace else df

    def select_cols(self, expr: str, inplace=False):
        """
        Select some fields of dataframe
        Other fields will be removed from Dataframe if inplace is True
        Args:
            columns: using expr sql to select column. ex: colA,colB...
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        df = self.df.selectExpr(expr)
        return self.update_df(df) if inplace else df

    def update_by(self, replace: dict, inplace=False):
        """
        Update value by a condition
        Args:
            replace: using expre sql to update value. ex: "colA": "if(colA=1, 1,0)"
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:

        """
        df = self.df
        for col, expr in replace.items():
            df = df.withColumn(col, self.spark_funcs.expr(expr))
        return self.update_df(df) if inplace else df

    def filter_by(self, condition: str, inplace=False):
        """
        Only get data of dataframe with given condition
        Args:
            condition: Condition for filtering data of dataframe. ex: gender != "M"
            inplace: False: means the result would be stored in a new DataFrame instead of the original one.
        Returns:
        """
        df = self.df.filter(condition)
        return self.update_df(df) if inplace else df

    def show(self, **kwargs):
        return self.df.show(**kwargs)

    def save_to_db_sql(self, table: str,
                       to_wf_connection_id: str = "",
                       mode: str = MODE_APPEND,
                       lowercase_columns: bool = True,
                       options: dict = {},
                       empty_error: bool = False,
                       sql_before="",
                       sql_after="",
                       ) -> bool:
        """
        Insert df to sql: mysql, oracle... by jdbc and spark
            table: table name
            connection_id: airflow connection id
            pd_df: pandas dataframe
            mode: append or overwrite
            lower_columns: lowercase all column names
            return:

        """

        table = table.strip()

        if table == "":
            raise Exception("You have to pass output table.")
        spark_df = self.df
        if empty_error:
            count = spark_df.count()
            if count > 0:
                logger.info("Total data on DataFrame: %s" % count)
            else:
                raise DfEmptyException()

        if to_wf_connection_id.strip() != "":
            config = WfConnection.get_wf_db(to_wf_connection_id if to_wf_connection_id else self.connection_id)
            logger.info(
                "Insert data to db: %s database/schema: %s, table: %s by Pandas engine" % (
                    config.host, table))
        else:
            config = self.db_wf_connection

        if mode == SparkPipeline.MODE_TRUNCATE:
            mode = SparkPipeline.MODE_OVERWRITE
            options["truncate"] = True

        db_sql_engine = config.get_sqlalchemy_engine()

        if type(sql_before) is str and sql_before.strip() != "":
            sql_before = sql_before.strip().rstrip(";")
            sql_before = sql_before.split(";\n")
            logger.info("Execute sql before insert into database")
            for k in sql_before:
                if k.strip() != "":
                    logger.info("query: %s" % k)
                    db_sql_engine.execute(sql=k, autocommit=True)
        elif callable(sql_before):
            logger.info("Execute function before insert into database")
            sql_before(db_sql_engine)

        writer = spark_df.write.format("jdbc").mode(mode).option("url", config.host) \
            .option("dbtable", table) \
            .option("user", config.login) \
            .option("password", config.password) \
            .option("driver", config.extra_dejson.get("extra__jdbc__drv_clsname"))
        if isinstance(options, dict) and options:
            for k, v in options.items():
                writer = writer.option(k, v)
        logger.info("Start insert by spark to host: [%s] - table: [%s] with mode: [%s]" % (config.host, table, mode))
        writer.save()
        logger.info("Insert by spark to host: [%s] - table: [%s] done!" % (config.host, table))

        if type(sql_after) is str and sql_after.strip() != "":
            sql_after = sql_after.strip().rstrip(";")
            sql_after = sql_after.split(";\n")
            logger.info("Execute sql after insert into database")
            for k in sql_after:
                if k.strip() != "":
                    logger.info("query: %s" % k)
                    db_sql_engine.execute(sql=k, autocommit=True)
        elif callable(sql_after):
            logger.info("Execute function after insert into database")
            sql_after(db_sql_engine)
        return True

    def save_to_hive(self, table: str, mode: str = MODE_APPEND,
                     format: str = "orc",
                     lowercase_columns: bool = True,
                     options: dict = {},
                     cast_columns: dict = {},
                     transform_columns: dict = {},
                     partition_by: list = [],
                     empty_error: bool = False, ) -> bool:
        """
        Insert df to hive table
            :param table: hive table name
            :param mode: overwrite or append
            :param format: orc, parquet, csv..
            :param lowercase_columns: lower all columns or not
            :param options: options for spark writer
            :param partition_by: list fields will be used to partition
            :param kwargs:
            :return:
        """
        spark_df = self.df
        if empty_error:
            count = spark_df.take(1)
            if count:
                logger.info("Checking Your DataFrame is empty or not: pass!")
            else:
                raise DfEmptyException()
        spark_df = self.cast_cols(spark_df, cast_columns, lowercase_columns)
        writer = spark_df.write
        table = table.strip()
        if transform_columns:
            spark_df = self.trans_cols(spark_df, transform_columns)
        if mode in [self.MODE_APPEND_PARTITION,
                    self.MODE_OVERWRITE_PARTITION,
                    self.MODE_APPEND]:
            try:
                cols = [k[0] for k in SparkPipeline().run_sql("show columns in %s" % table, log=False).collect()]
                logger.info("Columns will be inserted: %s" % cols)
                writer = spark_df.select(*cols).write
                if isinstance(options, dict) and options:
                    for k, v in options.items():
                        writer = writer.option(k, v)
            except pyspark.sql.utils.AnalysisException as e:
                t = table.split(".")
                t = t[0] if len(t) == 1 else t[1]
                if (str(e).find("Table or view [%s] not found" % t) >= 0):
                    mode = self.MODE_OVERWRITE
                    writer = spark_df.write
                else:
                    raise pyspark.sql.utils.AnalysisException(e, e)

        if mode in [self.MODE_APPEND_PARTITION,
                    self.MODE_OVERWRITE_PARTITION
                    ]:
            c = {
                "hive.exec.dynamic.partition.mode": "nonstrict",
                "spark.sql.sources.partitionOverwriteMode": "dynamic"
            }
            logger.info("""Check spark.conf must have config: %s to prevent loss data (replace entire table)""" % c)
            conf = SparkPipeline().session.sparkContext.getConf()
            for k, v in c.items():
                if conf.get(k) != v:
                    raise SystemError("You must config SparkConf with %s=%s" % (k, v))
            logger.info("Start insert hive table with mode partition: {%s} - {%s}" % (mode, table))
            writer.format(format).insertInto(table, overwrite=mode == self.MODE_OVERWRITE_PARTITION)
            logger.info("Insert hive table with mode partition: {%s} - {%s} done!" % (mode, table))
        elif mode in [self.MODE_OVERWRITE, self.MODE_APPEND]:
            writer = writer.mode(mode).format(format)
            logger.info("Start insert hive table by spark - table:{%s} {%s}" % (mode, table))
            writer.saveAsTable(table, partitionBy=partition_by)
            logger.info("Insert hive table: table:{%s} {%s} done!" % (mode, table))
        else:
            raise ValueError("mode: %s is invalid" % mode)
        return True
