""""""
__author__ = 'phongpham1805@gmail.com'

import logging

from airflow.models import BaseOperator

from dmp.helper.exceptions import AirflowException
from dmp.helper.exceptions import BeforeRunException
from dmp.helper.sql import replace_template
from dmp.helper.time_utils import convert_from_unixtime, current_time_local, convert_unixtime, \
    current_unixtime

logger = logging.getLogger(__name__)


class EtlOperator(BaseOperator):

    def __init__(self, before_run_fn=None, *args, **kwargs):
        self.before_run_fn = before_run_fn
        return super(EtlOperator, self).__init__(*args, **kwargs)

    def parsing_context(self, context):
        dag_start_unixtime = self.dag.start_date.timestamp() if self.dag.start_date else current_unixtime()
        dag_end_unixtime = self.dag.end_date.timestamp() if self.dag.end_date else convert_unixtime(
            current_time_local(end_day=True))
        dag_start_time = convert_from_unixtime(dag_start_unixtime)
        dag_end_time = convert_from_unixtime(dag_end_unixtime)
        params = dict()
        current_time = current_time_local(reset_time=True)
        params.update({
            "start_time": current_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "start_date": current_time.strftime("%Y-%m-%d"),
            "today_date": current_time.strftime("%Y-%m-%d"),
            "today_time": current_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "dag_start_time": dag_start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "dag_end_time": dag_end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "dag_start_date": dag_start_time.strftime("%Y-%m-%d"),
            "dag_end_date": dag_end_time.strftime("%Y-%m-%d"),
            "dag_start_date_dim": int(dag_start_time.strftime("%Y%m%d")),
            "dag_end_date_dim": int(dag_end_time.strftime("%Y%m%d")),
            "dag_start_unixtime": dag_start_unixtime,
            "dag_end_unixtime": dag_end_unixtime
        })
        try:
            key_from_ct = ['ds', 'next_ds', 'prev_ds', 'ds_nodash', 'yesterday_ds', 'tomorrow_ds',
                           'end_date', 'latest_date']
            build_params = {('dag_ctx_%s' % k): context.get(k, '') for k in key_from_ct}
            execution_date = context['execution_date'].timestamp()
            next_execution_date = context['next_execution_date'].timestamp()
            build_params.update({
                'dag_ctx_execution_unixtime': execution_date,
                'dag_ctx_next_execution_unixtime': next_execution_date,
                'dag_ctx_execution_time': convert_from_unixtime(execution_date).strftime("%Y-%m-%d %H:%M:%S"),
                'dag_ctx_execution_date': convert_from_unixtime(execution_date).strftime("%Y-%m-%d"),
                'dag_ctx_execution_dim_date': int(convert_from_unixtime(execution_date).strftime("%Y%m%d")),
                'dag_ctx_next_execution_time': convert_from_unixtime(next_execution_date).strftime("%Y-%m-%d %H:%M:%S"),
                'dag_ctx_next_execution_date': convert_from_unixtime(next_execution_date).strftime("%Y-%m-%d"),
                'dag_ctx_next_execution_dim_date': int(convert_from_unixtime(next_execution_date).strftime("%Y%m%d"))
            })
            params.update(build_params)
        except BaseException as e:
            logger.error(
                "Error when set some default values from ariflow_context: %s.\nSkip setting these values..." % e)
        return params

    def run(self, context):
        pass

    def execute(self, context):
        if callable(self.before_run_fn):
            logger.info("Run before_run function before execute main method")
            if not self.before_run_fn(self):
                raise BeforeRunException()
        return self.run(context=context)


class SqlEtlByPandasOperator(EtlOperator):
    """Process"""

    def __init__(self,
                 from_connection_id: str,
                 from_sql: str,
                 to_connection_id: str,
                 to_table: str,
                 extract_sql_params: dict = None,
                 mode: str = "append",
                 transform_func: callable = None,
                 load_func: callable = None,
                 sql_before: str = None,
                 sql_after: str = None,
                 write_params: dict = None,
                 *args,
                 **kwargs):
        """
        Args:
            src_connection_id:
            src_schema:
            src_sql:
            dest_connection_id:
            dest_schema:
            dest_table_name:
            db_mode:
                'replace' / 'append'
            transform_fn:
                apply transformation on dataframe
            sql_before:
            sql_after:
            *args:
            **kwargs:
        """
        self.src_connection_id = from_connection_id
        self.src_sql = from_sql
        self.dest_connection_id = to_connection_id
        self.dest_table_name = to_table
        self.db_mode = mode
        self.transform_func = transform_func
        self.load_fn = load_func
        self.sql_before = sql_before
        self.sql_after = sql_after
        self.sql_params = extract_sql_params
        self.kwargs = kwargs
        self.write_params = write_params
        super(SqlEtlByPandasOperator, self).__init__(before_run_fn=kwargs.get('before_run_func'), *args, **kwargs)

    def run(self, context):
        logger.info("Read using Pandas Engine table=%s schema=%s from connection=%s",
                    self.src_sql, self.src_schema, self.src_connection_id)
        from dmp.df.pandas import PandasPipeline
        pd_engine = PandasPipeline().connect_db(wf_connection_id=self.src_connection_id)
        read_kwargs = {}
        if self.src_schema:
            read_kwargs['schema'] = self.src_schema
        sql_params = self.parsing_context(context)
        sql_params.update(self.sql_params)

        logger.info("SQL Params: %s" % sql_params)
        df = pd_engine.read_sql(sql=self.src_sql,
                                replace_params=sql_params,
                                **read_kwargs).df

        if callable(self.transform_func):
            logger.info("Start run transform data...")
            df = self.transform_func(df)

        logger.info("Start insert into database by Pandas table=%s schema=%s from connection=%s",
                    self.dest_table_name, self.dest_schema, self.dest_connection_id)
        if not callable(self.load_fn):
            pd_engine.read_df(df=df).save_to_db(
                table=self.dest_table_name,
                to_wf_connection_id=self.dest_connection_id,
                mode=self.db_mode,
                sql_before=self.sql_before,
                sql_after=self.sql_after,
                **(self.write_params if type(self.write_params) is dict else {})
            )
        else:
            logger.info("Run your custom Load Function...")
            self.load_fn(df, self)


class SqlEtlBySparkOperator(EtlOperator):

    def __init__(
        self,
        from_sql,  # type: any,
        from_connection_id: str,
        from_spark_conf_id: str = "spark_connection_default",
        to_connection_id: str = "",
        to_table: str = "",
        mode: str = "append",
        spark_conf: dict = {},
        transform_func=None,  # type: any
        load_func=None,  # type: Callable,
        from_sql_params: dict = {},
        empty_error=False,
        debug_dataframe=False,
        *args,
        **kwargs
    ):
        """
            Operator ETL with input is SQL Query
            Using PySpark as execution engine with JDBC connection
            Providing options for detect changing data with storing last change time on metadata:

                from_sql:List or Str. Query SQL query that used for extract data
                from_connection_id: JDBC connection for data source, defined on Airflow
                from_spark_conf: spark config that is defined on Airflow
                spark_conf: list params of SparkHook
                transform: callable or list of SparkSQL (only for engine is spark)
                load_func: callable. Function that will be used for saving data
                from_sql_params: dict. params that will be used for replacing variable on sql query
                provide_context:
                templates_dict:
                templates_exts:
                args:
                kwargs:
        """
        self.from_sql = from_sql
        self.from_spark_conf_id = from_spark_conf_id
        self.spark_conf = spark_conf
        self.from_connection_id = from_connection_id
        self.transform_func = transform_func
        self.load_func = load_func
        self.from_sql_params = from_sql_params
        self.to_connection_id = to_connection_id
        self.to_table = to_table
        self.to_mode = mode
        self.empty_error = empty_error
        self.debug_dataframe = debug_dataframe
        return super(SqlEtlBySparkOperator, self).__init__(before_run_fn=kwargs.get('before_run_func'), *args, **kwargs)

    def __extract(self):
        sql_extract = [k.strip() for k in self.from_sql.split(";\n") if k.strip() != '']
        if len(sql_extract) == 0:
            raise AirflowException("Your extraction sql query is empty.")
        list_df = []
        for index, sql in enumerate(sql_extract):
            if sql.strip() == '':
                continue
            sql = replace_template(sql, self.params)
            logger.info("[Extract] SQL query will be used for extracting: %s" % sql)
            df = self.spark_hook.read_jdbc(table=sql,
                                           connection_id=self.from_connection_id,
                                           replace_sql_params=self.from_sql_params
                                           )
            if self.debug_dataframe:
                df.show(n=20, truncate=False)
            logger.info("[Extract] Create temp view with name: TABLE_TEMP_%s" % index)
            df.createOrReplaceTempView("TABLE_TEMP_%s" % index)
            list_df.append(df)
        return list_df

    def __transform(self, extract_value):
        if isinstance(self.transform_func, list):
            logger.info("[Transform] Run transformation with SQL query and SparkSQL")
            r = None
            for sql in self.transform_func:
                sql = replace_template(sql, self.params)
                r = self.spark_hook.run_sql(sql=sql, replace_params=self.from_sql_params)
                if self.debug_dataframe:
                    r.show(n=20, truncate=False)
            return r
        elif isinstance(self.transform_func, str) and self.transform_func != '':
            query_list = self.transform_func.split(";\n")
            r = None
            for sql in query_list:
                sql = replace_template(sql, self.params)
                r = self.spark_hook.run_sql(sql=sql, replace_params=self.from_sql_params)
                if self.debug_dataframe:
                    r.show(n=20, truncate=False)
            return r
        elif callable(self.transform_func):
            logger.info("Start run transform data...")
            return self.transform_func(extract_value)
        return extract_value

    def __load(self, transform_value):
        if isinstance(self.load_func, str):
            logger.info("[Load] Run Load by SparkSQL String")
            return self.spark_hook.run_sql(sql=self.load_func)
        elif callable(self.load_func):
            return self.load_func(transform_value)
        elif self.to_connection_id and self.to_table:
            logger.info("[Load] Save data to table: [%s]" % self.to_table)
            return self.spark_hook.save_to_db(table=self.to_table,
                                              connection_id=self.to_connection_id,
                                              empty_error=self.empty_error,
                                              spark_df=transform_value[-1] if type(
                                                  transform_value) is list and transform_value else transform_value,
                                              mode=self.to_mode)
        else:
            logger.info("[Load] Skip Load step because output table and connection database are empty...")

    def run(self, context):
        from dmp.wf.hooks.spark_hook import SparkHook
        self.spark_hook = SparkHook(connection_id=self.from_spark_conf_id, **self.spark_conf)
        self.params = self.parsing_context(context)
        self.params.update(self.from_sql_params if type(self.from_sql_params) is dict else {})
        ex = self.__extract()
        tra = self.__transform(ex)
        load = self.__load(tra)
        return load
