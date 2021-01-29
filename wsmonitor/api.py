import psycopg2
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

    if value is None:
        return None

    m = re.match(r'\("(.+)",(.+),(.+),(.*)\)', value)

    if m:
        # nb: datetime can't handle time zones without minutes field,
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


def _get_db_connection(config):
    try:
        conn = psycopg2.connect(host=config['database-host'],
                                port=config['database-port'],
                                user=config['database-user'],
                                password=config['database-password'],
                                database=config['database-name'])
    except Exception as e:
        raise DatabaseError(f"Can't connect to database : {e}")

    # create psycopg2 array type for metrics
    metric_oid = array_oid = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT oid, typarray FROM pg_type WHERE typname = 'metric'")
        metric_oid, array_oid = cursor.fetchone()
    except Exception:
        # metric type is not definied yet... nevermind.
        pass

    if metric_oid is not None:
        metric_type = psycopg2.extensions.new_type((metric_oid,),
                                                   "metric", _cast_metric)
        psycopg2.extensions.register_type(metric_type)
        array_type = psycopg2.extensions.new_array_type((array_oid,),
                                                        'metric[]',
                                                        metric_type)
        psycopg2.extensions.register_type(array_type)

    return conn


def database_init(config):
    schema = [
        '''CREATE TYPE metric AS (time_stamp TIMESTAMP(0) WITH TIME ZONE,
                                  response_time INTEGER, return_code INTEGER,
                                  regex_check BOOL);''',
        '''CREATE TABLE websites (id SERIAL PRIMARY KEY, url VARCHAR,
                                  regexp text, metrics metric[]);'''
    ]
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    for sql in schema:
        try:
            cursor.execute(sql)
        except psycopg2.errors.DuplicateObject:
            # keep going if schema exists already
            conn.commit()
    conn.commit()
    conn.close()


def database_reset(config):

    conn = _get_db_connection(config)
    cursor = conn.cursor()
    try:
        cursor.execute('DROP TABLE websites;')
        cursor.execute('DROP TYPE metric;')
        conn.commit()
    except Exception:
        pass
    conn.close()


def database_add_metric(config, url_id, timestamp, response_time, return_code,
                        regex_check):
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.execute('UPDATE websites set metrics = '
                   'array_append(metrics, (%s, %s, %s, %s)::metric) '
                   'WHERE id=%s;',
                   (timestamp, response_time, return_code,
                    regex_check, url_id))
    conn.commit()
    conn.close()


#
# URL management API
#
def url_add(config, url, regexp=None):
    """
    Add an URL to the watchlist, optionnaly with a regular expression
    Return id of inserted element
    """
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO websites (url, regexp, metrics) '
                   "VALUES (%s, %s, '{}') RETURNING id;", (url, regexp))
    _id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return _id


def url_remove(config, url):
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM websites WHERE url = %s;', (url,))
    conn.commit()
    conn.close()


def url_list(config):
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
    conn.close()


def url_status(config, ids=None):
    """
    Get metrics for all monitored URLs, or only those in given ids.
    """
    conn = _get_db_connection(config)
    cursor = conn.cursor()
    cursor.arraysize = 100
    if ids is None:
        sql = 'SELECT id, url, metrics from websites;'
    else:
        sql = cursor.mogrify('SELECT id, url, metrics from websites '
                             'WHERE id IN %s;', (tuple(ids),))
    cursor.execute(sql)
    while True:
        res = cursor.fetchmany()
        if not res:
            break
        for s in res:
            yield s
    conn.close()
