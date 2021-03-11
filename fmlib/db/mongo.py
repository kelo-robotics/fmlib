import logging

from pymodm import connect
from pymodm import connection
from pymongo.errors import ServerSelectionTimeoutError
from ropod.utils.logging.counter import ContextFilter


class MongoStore:

    def __init__(self, db_name, port=27017, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.logger.addFilter(ContextFilter())
        self.db_name = db_name
        self.port = port
        self.ip = kwargs.get('ip', 'localhost')
        self._connected = False
        self._connection_timeout = kwargs.get('connectTimeoutMS', 30) * 1000
        self.alias = kwargs.get("alias", "default")
        self.user = kwargs.get("user")
        self.pwd = kwargs.get("pwd")

        self.connect()

    def connect(self):
        if self._connected:
            return

        if self.user:
            # Connect with authentication
            connection_str = "mongodb://%s:%s@%s:%s/%s?authSource=admin" % (self.user, self.pwd, self.ip, self.port, self.db_name)
        else:
            connection_str = "mongodb://%s:%s/%s" % (self.ip, self.port, self.db_name)

        try:
            # Default timeout is 30s
            connect(connection_str, alias=self.alias, serverSelectionTimeoutMS=self._connection_timeout)
            self._connected = True
            self.logger.info("Connecting to %s on port %s", self.db_name, self.port)
        except ServerSelectionTimeoutError as err:
            self.logger.critical("Cannot connect to MongoDB", exc_info=True)
            self._connected = False
            return


    @property
    def connected(self):
        if not self._connected:
            self.connect()

        return self._connected


class MongoStoreInterface:

    def __init__(self, mongo_store=None):
        self.logger = logging.getLogger(__name__)
        self._store = mongo_store

    @property
    def db_name(self):
        return self._store.db_name

    @property
    def port(self):
        return self._store.port

    @property
    def user(self):
        return self._store.user

    @property
    def pwd(self):
        return self._store.pwd

    def save(self, model):
        if self._store.connected:
            try:
                model.save()
            except ServerSelectionTimeoutError as err:
                self.logger.error(err)

    def archive(self, model):
        if self._store.connected:
            try:
                model.archive()
            except ServerSelectionTimeoutError as err:
                self.logger.error(err)

    def update(self, model, **kwargs):
        if self._store.connected:
            try:
                model.update(**kwargs)
            except ServerSelectionTimeoutError as err:
                self.logger.error(err)

    def insert(self, collection, file_data):
        if self._store.connected:
            try:
                print(connection._get_db(alias=self._store.alias).client)
                db = connection._get_db(alias=self._store.alias).client[self._store.db_name]
                db[collection].insert(file_data)
            except ServerSelectionTimeoutError as err:
                self.logger.error(err)

    def clean(self):
        if self._store.connected:
            try:
                connection._get_db(alias=self._store.alias).client.drop_database(self._store.db_name)
            except ServerSelectionTimeoutError as err:
                self.logger.error(err)
