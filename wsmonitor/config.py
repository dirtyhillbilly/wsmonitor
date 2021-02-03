# App configuration

import json
import os


# Application name
APP_NAME = 'wsmonitor'

VERSION = '0.0.1'

_config_dir = os.environ.get('WSMONITORDIR',
                             os.path.expanduser(f'~/.config/{APP_NAME}'))

CONFIG_DIR = os.path.expandvars(_config_dir)
CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')

# only these keys are allowed
config = {

    # database connection parameters
    'database-host': None,
    'database-port': None,
    'database-name': 'wsmonitor',
    'database-user': None,
    'database-password': None,

    # kafka parameters
    'kafka-broker': None,
    'kafka-user': None,
    'kafka-password': None,
    'kafka-ca-file': None,

    # global parameters
    'pretty-print': False,
    }


def save():
    """Save config to file"""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4, sort_keys=True)


os.makedirs(CONFIG_DIR, exist_ok=True)

if os.path.exists(CONFIG_FILE):
    # load config if it exists
    with open(CONFIG_FILE, 'r') as f:
        config.update(json.load(f))
else:
    # save default config
    save()
