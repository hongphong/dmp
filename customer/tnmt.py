import ftplib
from dmp.helper.sql import clean_col_name
from dmp.helper.utils import convert_float, convert_time, convert_int, current_time_local
from datetime import timedelta
import time
from dmp.helper.log import logger
from airflow.providers.ftp.hooks.ftp import FTPHook
from dmp.df.pandas import PandasPipeline
import os


class SyncData:
    TEMP = "/tmp/sync_tnmt/"
    FTP_PATH = "/home/dmp/demo_ftp/"
    TABLE_INSERT = "fact_tnmt_station_metrics"
    CONNECTION_FTP = "test_ftp"
    CONNECTION_DATABASE = "test_database"
    IS_TEST = False

    def __init__(self, station_code, ftp_connection=CONNECTION_FTP,
                 output_table=TABLE_INSERT,
                 output_conn_database_id=CONNECTION_DATABASE,
                 is_dev=True
                 ):
        self.station_code = station_code
        self.ftp_conn = ftp_connection
        self.output_table = output_table
        self.output_conn_database_id = output_conn_database_id
        self.database_engine = WfConnection(self.output_conn_database_id).get_wf_db().get_sqlalchemy_engine()
        self.is_dev = is_dev
        if not station_code:
            raise Exception("You have to set station code")

    def read_file(self, file):
        cols = ["name", "metric", "unit", "time"]
        with open(file, "r") as f:
            content = f.read()
            content = content.split("\n")
            data = []
            for k in content:
                if k == '':
                    continue
                k = k.split('\t')
                k[0] = clean_col_name(k[0])
                k[3] = convert_time(k[3], format='%Y%m%d%H%M%S')
                del k[4]
                k[1] = None if k[1] == '' else convert_float(k[1])
                d = {"station_code": self.station_code}
                d.update(dict(zip(cols, k)))
                d["version"] = convert_int(time.time())
                data.append(d)
            PandasPipeline.read_dict(data).save_to_db_sql(
                table=self.output_table,
                to_connection_engine=SyncData.CONNECTION_DATABASE,
                mode="append",
            )

    def build_path(self):
        current = current_time_local()
        preview = current - timedelta(days=1)
        for k in [preview, current]:
            yield "{station_code}/{year}/{month}/{day}".format(station_code=self.station_code,
                                                               year=str(k.year).zfill(2),
                                                               month=str(k.month).zfill(2), day=str(k.day).zfill(2))

    def consumer_file(self):
        # get from ftp
        logger.info("Start consume file from ftp: [%s]" % self.ftp_conn)
        path = self.build_path()
        conn = FTPHook.get_hook(self.ftp_conn)
        local_path = SyncData.TEMP + self.station_code
        logger.info("Consume data into local path: %s" % local_path)
        if not os.path.isdir(local_path):
            from pathlib import Path
            Path(local_path).mkdir(parents=True)

        for k in path:
            logger.info("Start consume from path: [%s]" % (SyncData.FTP_PATH + k))
            try:
                parent = SyncData.FTP_PATH + k
                list_files = conn.list_directory(parent)
                if not self.is_dev:
                    list_files = [k for k in list_files if k.find("_done") < 0]
                [conn.retrieve_file(parent + "/" + f, local_path + "/" + f) for f in list_files]
                if not self.is_dev:
                    [conn.rename(parent + "/" + f, parent + "/" + f + "_done") for f in list_files]
            except ftplib.error_perm as e:
                logger.error("Fail to prcess path: %s. code: [%s]" % (k, e))

        logger.info("Start read data from local")
        for file in os.listdir(local_path):
            fullp = local_path + "/" + file
            try:
                self.read_file(fullp)
                os.remove(fullp)
            except BaseException as e:
                logger.error("Error when process file: %s. Error: %s" % (fullp, e))

    @staticmethod
    def start(station_code, loop_after=20, **kwargs):
        while True:
            SyncData(station_code, **kwargs).consumer_file()
            try:
                logger.info("Start loop after %ss" % loop_after)
            except BaseException as e:
                logger.error("Error: %s" % e)
                logger.exception(e)
            time.sleep(loop_after)


if __name__ == "__main__":
    from dmp.helper.connection import WfConnection

    SyncData.start("HN_GLHN_KHINVC")
