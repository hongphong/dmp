from airflow.providers.mongo.hooks.mongo import MongoHook


class CusMongoHook(MongoHook):

    def __init__(self, conn_id: str = MongoHook.default_conn_name, *args, **kwargs) -> None:
        super().__init__()
        self.mongo_conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson.copy()
        self.client = None

        srv = self.extras.pop('srv', False)
        scheme = 'mongodb+srv' if srv else 'mongodb'
        extra_uri = self.extras.get("extra_uri")
        self.uri = '{scheme}://{creds}{host}{port}/{database}{extra_uri}'.format(
            scheme=scheme,
            creds=f'{self.connection.login}:{self.connection.password}@' if self.connection.login else '',
            host=self.connection.host,
            port='' if self.connection.port is None else f':{self.connection.port}',
            database=self.connection.schema,
            extra_uri=("?%s" % extra_uri) if extra_uri else ""
        )

    def get_list_database(self):
        return self.get_conn().list_databases()

    def get_list_collection(self, database):
        return self.get_conn()[database].list_collection_names()
