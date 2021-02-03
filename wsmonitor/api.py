import psycopg2
import psycopg2.pool
import threading
import datetime
import re
import json
import os
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
import kafka.errors


#
# Errors
#
class DatabaseError(Exception):
    pass


class KafkaError(Exception):
    pass


#
# Database management API
#
def _cast_metric(value, cur):
    """
    Cast a metric from postgres to python.

    Return (timestamp, response_time, return_code, regex_check) tuple.
    """

    if value is None:
        return None

    m = re.match(r'\("(.+)",(.+),(.+),(.*)\)', value)

    if m:
        # datetime.fromisoformat can't handle time zones without minutes field,
        # hence the +':00' :
        ts = datetime.datetime.fromisoformat(m.group(1) + ':00')
        response_time = int(m.group(2))
        return_code = int(m.group(3))
        if m.group(4) == '':
            regex_check = None
        elif m.group(4) == 'f':
            regex_check = False
        elif m.group(4) == 't':
            regex_check = True
        else:
            raise psycopg2.InterfaceError(
                f"bad metric for regex_check: {value}")
    else:
        raise psycopg2.InterfaceError(f"bad metric representation: {value}")
    return (ts, response_time, return_code, regex_check)


_connection_pool = None  # singleton connection pool instance
_connection_pool_lock = threading.Lock()


class MetricConnectionPool(psycopg2.pool.ThreadedConnectionPool):
    """
    Metric types-aware threaded connection pool.

    Needed to prevent to have to register types each time we need a connection.

    Unlike ThreadedConnectionPool, pool will grow as long as concurrent
    connections are needed.
    """
    def __init__(self, *args, **kwargs):
        self.type_registered = False
        self.metric_oid = None
        self.array_oid = None
        super().__init__(2, 2, *args, **kwargs)

    @staticmethod
    def _get_metric_type(cursor):
        cursor.execute("SELECT oid, typarray FROM pg_type "
                       "WHERE typname = 'metric'")
        return cursor.fetchone()

    @staticmethod
    def _create_metric_type(cursor):
        try:
            cursor.execute("CREATE TYPE metric AS (time_stamp TIMESTAMP(0) "
                           "WITH TIME ZONE, response_time INTEGER, "
                           "return_code INTEGER, regex_check BOOL)")
        except Exception:
            pass

    def _register_metric_types(self, conn):
        """
        Create psycopg2 types for metrics and arrays of metrics.
        """
        if self.metric_oid is None:
            cursor = conn.cursor()
            res = self._get_metric_type(cursor)
            if res is None:
                self._create_metric_type(cursor)
                res = self._get_metric_type(cursor)
            self.metric_oid, self.array_oid = res

        if self.metric_oid is not None:
            metric_type = psycopg2.extensions.new_type((self.metric_oid,),
                                                       "metric", _cast_metric)
            array_type = psycopg2.extensions.new_array_type((self.array_oid,),
                                                            'metric[]',
                                                            metric_type)
            psycopg2.extensions.register_type(metric_type)
            psycopg2.extensions.register_type(array_type)

            self.type_registered = True

    def _connect(self, key=None):
        conn = super()._connect(key)
        conn.set_session(autocommit=True)
        if not self.type_registered:
            self._register_metric_types(conn)
        return conn

    def getconn(self, key=None):
        try:
            conn = super().getconn(key)
        except psycopg2.pool.PoolError as e:
            # ugly hack to deal with psycopg2 pool implementation
            # TODO: propose upstream a better way to deal with growing pools
            # TODO: check how this is done in psycopg3
            if str(e) == 'connection pool exhausted':
                # prevent pool exhaustion :
                _connection_pool.maxconn += 1
                # prevent closing connections :
                _connection_pool.minconn = _connection_pool.maxconn
                conn = super().getconn(key)
            else:
                raise
        return conn


def _get_db_connection(config):
    """
    Return psycopg2 connection object to database.
    """

    global _connection_pool

    # create connection pool as needed
    acquired = _connection_pool_lock.acquire(timeout=20)
    if not acquired:
        raise DatabaseError(f"Can't acquire lock to connect to database")
    if _connection_pool is None:
        try:
            _connection_pool = MetricConnectionPool(
                host=config['database-host'],
                port=config['database-port'],
                user=config['database-user'],
                password=config['database-password'],
                database=config['database-name'])
        except Exception as e:
            _connection_pool_lock.release()
            raise DatabaseError(f"Can't connect to database : {e}")
    _connection_pool_lock.release()

    conn = _connection_pool.getconn()

    return conn


def _release_db_connection(conn):
    """
    Put back a connection to the pool
    """
    _connection_pool.putconn(conn)


def database_init(config):
    """
    Reset and initialize postgres database.

    This is not threadsafe : connection pool will be reset.
    """
    global _connection_pool
    global _connection_pool_lock

    conn = _get_db_connection(config)
    cursor = conn.cursor()

    acquired = _connection_pool_lock.acquire(timeout=20)
    if not acquired:
        raise DatabaseError(f"Can't acquire lock to connect to database")

    # Reset database if it exists already
    cursor.execute('DROP TABLE IF EXISTS websites;')
    cursor.execute('DROP TYPE IF EXISTS metric;')
    _release_db_connection(conn)
    _connection_pool.closeall()

    # reset connection pool to reflect new metrics and metric arrays OID
    _connection_pool = None
    _connection_pool_lock.release()

    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE websites (id SERIAL PRIMARY KEY, url VARCHAR,'
                   'regexp text, metrics metric[])')
    _release_db_connection(conn)


def database_add_metric(config, url_id, timestamp, response_time, return_code,
                        regex_check):
    """
    Register a new metric event to postgres database.
    """
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.execute('UPDATE websites set metrics = '
                   'array_append(metrics, (%s, %s, %s, %s)::metric) '
                   'WHERE id=%s;',
                   (timestamp, response_time, return_code,
                    regex_check, url_id))
    conn.commit()
    _release_db_connection(conn)


#
# Kafka message queue API
#

_kafka_producer = None  # singleton producer instance
_kafka_producer_lock = threading.Lock()


def _kafka_init_topic(config):
    """
    Create kafka topic on borker
    """

    admin = KafkaAdminClient(bootstrap_servers=config['kafka-broker'],
                             security_protocol="SASL_SSL",
                             ssl_cafile=_get_kafka_ca_file(config),
                             sasl_mechanism="PLAIN",
                             sasl_plain_username=config['kafka-user'],
                             sasl_plain_password=config['kafka-password'])
    topic = NewTopic(name=config['kafka-topic'],
                     num_partitions=1,
                     replication_factor=1)

    try:
        admin.create_topics([topic])
    except kafka.errors.TopicAlreadyExistsError:
        pass
    except Exception:
        raise

    admin.close()


def _get_kafka_ca_file(config):
    if config['kafka-ca-file'] is None:
        return None
    else:
        return os.path.expanduser(config['kafka-ca-file'])


def _get_kafka_producer(config):
    """
    Get a kafka producer for our topic.
    """
    global _kafka_producer

    acquired = _kafka_producer_lock.acquire(timeout=20)
    if not acquired:
        raise KafkaError(f"Can't acquire lock to kafka producer")

    if _kafka_producer is None:
        _kafka_init_topic(config)

        _kafka_producer = KafkaProducer(
            bootstrap_servers=config['kafka-broker'],
            security_protocol="SASL_SSL",
            ssl_cafile=_get_kafka_ca_file(config),
            sasl_mechanism="PLAIN",
            sasl_plain_username=config['kafka-user'],
            sasl_plain_password=config['kafka-password'])

    _kafka_producer_lock.release()
    return _kafka_producer


def kafka_send_metric(config, url_id, timestamp, response_time, return_code,
                      regex_check):
    """
    Send a metric to kafka broker
    """
    p = _get_kafka_producer(config)
    metric = (url_id, timestamp.isoformat(' ', timespec='seconds'),
              response_time, return_code, regex_check)
    msg = bytes(json.dumps(metric).encode('utf-8'))
    p.send(config['kafka-topic'], msg)


def kafka_get_metrics(config):
    """
    Generator that wait for a metric to be published on topic.

    Yield tuple (url_id, timestamp, response_time, return_code, regex_check)
    """
    _kafka_init_topic(config)
    consumer = KafkaConsumer(config['kafka-topic'],
                             bootstrap_servers=config['kafka-broker'],
                             auto_offset_reset='earliest',
                             security_protocol="SASL_SSL",
                             ssl_cafile=_get_kafka_ca_file(config),
                             sasl_mechanism="PLAIN",
                             sasl_plain_username=config['kafka-user'],
                             sasl_plain_password=config['kafka-password'])
    for msg in consumer:
        metric = json.loads(msg.value)
        yield metric


#
# URL management API
#
def url_add(config, url, regexp=None):
    """
    Add an URL to the watchlist, optionnaly with a regular expression.

    Return id of inserted element.
    """
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO websites (url, regexp, metrics) '
                   "VALUES (%s, %s, '{}') RETURNING id;", (url, regexp))
    _id = cursor.fetchone()[0]
    _release_db_connection(conn)
    return _id


def url_remove(config, url_id):
    """
    Remove watched url from database.
    """
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM websites WHERE id = %s;', (url_id,))
    _release_db_connection(conn)


def url_list(config):
    """
    Get list of monitored urls.

    Return an iterator over results.
    """
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.arraysize = 100
    cursor.execute('SELECT id, url, regexp from websites;')
    while True:
        res = cursor.fetchmany()
        if not res:
            break
        for url in res:
            yield url
    _release_db_connection(conn)


def url_status(config, ids=None):
    """
    Get metrics for all monitored URLs, or only those in given ids.

    Return an iterator over results.
    """
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.arraysize = 100
    if ids is None:
        sql = 'SELECT id, url, metrics FROM websites;'
    else:
        sql = cursor.mogrify('SELECT id, url, metrics FROM websites '
                             'WHERE id IN %s;', (tuple(ids),))
    cursor.execute(sql)
    while True:
        res = cursor.fetchmany()
        if not res:
            break
        for s in res:
            yield s
    _release_db_connection(conn)
