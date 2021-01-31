import psycopg2
import psycopg2.pool
import threading
import datetime
import re


#
# Errors
#
class DatabaseError(Exception):
    pass


#
# Database management API
#
def _cast_metric(value, cur):
    """
    Cast a metric from postgres to python.

    Return (timestamp, response_time, return_code, regexp_check) tuple.
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
            regexp_check = None
        elif m.group(4) == 'f':
            regexp_check = False
        elif m.group(4) == 't':
            regexp_check = True
        else:
            raise psycopg2.InterfaceError(
                f"bad metric for regexp_check: {value}")
    else:
        raise psycopg2.InterfaceError(f"bad metric representation: {value}")

    return (ts, response_time, return_code, regexp_check)


_connection_pool = None
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

    def _register_metric_types(self, conn):
        """
        Create psycopg2 types for metrics and arrays of metrics.
        """
        if self.metric_oid is None:
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT oid, typarray FROM pg_type "
                    "WHERE typname = 'metric'")
                self.metric_oid, self.array_oid = cursor.fetchone()
            except Exception:
                # metric type is not defined yet... nevermind.
                pass

        if self.metric_oid is not None:
            metric_type = psycopg2.extensions.new_type((self.metric_oid,),
                                                       "metric", _cast_metric)
            psycopg2.extensions.register_type(metric_type)
            array_type = psycopg2.extensions.new_array_type((self.array_oid,),
                                                            'metric[]',
                                                            metric_type)
            psycopg2.extensions.register_type(array_type)

            self.type_registered = True

    def _connect(self, key=None):
        conn = super()._connect(key)
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
                # prevent pool exhaustion "
                _connection_pool.maxconn += 1
                # prevent closing connections :
                _connection_pool.minconn = _connection_pool.maxconn
                conn = _connection_pool.getconn()
            else:
                raise
        return conn


def _get_db_connection(config):
    """
    Return psycopg2 connection object to database.

    TODO: Use connection pools
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

    schema = [
        '''CREATE TYPE metric AS (time_stamp TIMESTAMP(0) WITH TIME ZONE,
                                  response_time INTEGER, return_code INTEGER,
                                  regex_check BOOL);''',
        '''CREATE TABLE websites (id SERIAL PRIMARY KEY, url VARCHAR,
                                  regexp text, metrics metric[]);'''
    ]
    conn = _get_db_connection(config)
    cursor = conn.cursor()

    acquired = _connection_pool_lock.acquire(timeout=20)
    if not acquired:
        raise DatabaseError(f"Can't acquire lock to connect to database")

    # Reset database if it exists already
    try:
        cursor.execute('DROP TABLE websites;')
        cursor.execute('DROP TYPE metric;')
        conn.commit()
    except Exception:
        pass
    _release_db_connection(conn)
    _connection_pool.closeall()

    # reset connection pool to reflect new metrics and metric arrays OID
    _connection_pool = None
    _connection_pool_lock.release()

    conn = _get_db_connection(config)
    cursor = conn.cursor()
    for sql in schema:
        cursor.execute(sql)
    conn.commit()
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
    conn.commit()
    _release_db_connection(conn)
    return _id


def url_remove(config, url_id):
    """
    Remove watched url from database.
    """
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM websites WHERE id = %s;', (url_id,))
    conn.commit()
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
