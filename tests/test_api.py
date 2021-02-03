from wsmonitor import api
from wsmonitor.checker import check_url
import time
import threading


# TODO: proper tests should use proper kafka and posgresql containers
config = {
    'database-host': 'wsmon-pg-praksys-41e8.aivencloud.com',
    'database-name': 'wsmonitor',
    'database-password': 'fjm6xcctiilz2yn2',
    'database-port': 15383,
    'database-user': 'avnadmin',
    'kafka-broker': 'wsmon-kafka-praksys-41e8.aivencloud.com:15396',
    'kafka-user': 'avnadmin',
    'kafka-password': 'sbrnbs6a90rdniip',
    'kafka-topic': 'wsmonitor',
    'kafka-ca-file': '~/.config/wsmonitor/ca.crt',
    'pretty-print': False
    }


def test_init():
    api.database_init(config)


def test_url_add_remove():
    api.database_init(config)
    url = 'foo://some url'
    regexp = 'some regexp'
    url_id = api.url_add(config, url, regexp)
    res = [(url[1], url[2]) for url in api.url_list(config)]
    assert res[0] == (url, regexp)
    api.url_remove(config, url_id)
    res = [url for url in api.url_list(config)]
    assert res == []


cases = [('https://help.aiven.io/en/', None, 200, None),
         ('https://help.aiven.io/en/articles/'
          '4332080-public-access-for-kafka-in-a-vpc',
          'Kafka service is running .* a VPC', 200, True),
         ('https://aiven.io/', None, 200, None),
         ('https://aiven.io/', 'free software conspiracy', 200, False),
         ('http://bad.domain.namez', None, 503, None),
         ('!@#$', None, -1, None),
         ('http://www.google.com/should-404', None, 404, None),
         ('https://www.google.fr/search?q='+'403'*1024, None, 403, None)]


def test_url_check():
    api.database_init(config)

    checks = {}

    # check each URL once
    for url, regexp, return_code, regexp_check in cases:
        _id = api.url_add(config, url, regexp)

        t = threading.Thread(target=check_url, daemon=False,
                             kwargs={'url_id': _id, 'url': url,
                                     'regexp': regexp, 'timeout': 30})
        checks[_id] = (t, url, return_code, regexp_check)
        t.start()

    for t in checks.values():
        t[0].join()

    time.sleep(10)

    # gather results
    for _id, url, metrics in api.url_status(config, checks.keys()):
        print(metrics)
        _, _, return_code, regex_check = metrics[-1]
        assert url == checks[_id][1]
        assert return_code == checks[_id][2]
        assert regex_check == checks[_id][3]
