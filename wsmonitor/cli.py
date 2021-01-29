import click
import pprint
from collections import namedtuple
from wsmonitor import api
from wsmonitor.config import config as _config, save as save_config

Context = namedtuple('Context', ['pretty_print', 'verbose'])


def _print(context, *args):
    if context.pretty_print:
        pprint.pprint(*args, indent=4)
    else:
        print(*args)


@click.group('cli')
@click.version_option()
@click.option('--pretty-print', '-p', is_flag=True,
              default=_config['pretty-print'], help='Pretty-print results')
@click.option('--verbose', '-v', is_flag=True,
              type=click.BOOL, help='Enable verbose operations.')
@click.pass_context
def cli(ctx, **kwargs):
    ctx.obj = Context(**kwargs)


#
# 'database' group
#
@cli.group('database')
def url():
    """Manage database backend"""


@url.command('init')
@click.pass_obj
def database_init(context):
    """
    Initialize database.
    """
    api.database_init(_config)


@url.command('reset')
@click.pass_obj
def database_reset(context):
    """
    Empty database.
    """
    api.database_reset(_config)


#
# 'url' group
#
@cli.group('url')
def url():
    """Manage watched URLs"""


@url.command('add')
@click.argument('url')
@click.argument('regexp', required=False)
@click.pass_obj
def url_add(context, url, regexp):
    """
    Add a new URL to watched list.
    """
    api.url_add(_config, url=url, regexp=regexp)


@url.command('remove')
@click.argument('url')
@click.pass_obj
def url_remove(context, url):
    """
    Remove an URL from watched list.
    """
    api.url_remove(_config, url=url)


@url.command('list')
@click.pass_obj
def url_list(context):
    """
    Print watched URLs.
    """
    res = [url for url in api.url_list(_config)]
    _print(context, res)


@url.command('status')
@click.pass_obj
def url_status(context):
    """
    Print status for watched URLs.
    """
    res = [status for status in api.url_status(_config)]
    _print(context, res)


#
# 'config' group
#
@cli.group('config')
def config():
    """Configuration"""


@config.command('get')
@click.pass_obj
def config_get(context):
    """Print configuration"""
    _print(context, _config)


@config.command('set')
@click.argument('key_val', nargs=-1,
                metavar='[KEY VALUE] ...')
def config_set(key_val):
    """Update wsmonitor configuration.

    Set configuration variable KEY to VALUE.
    """
    keys = key_val[0::2]
    values = key_val[1::2]
    for k, v in zip(keys, values):
        if k not in _config:
            print('Unknown key {}'.format(k))
            continue
        if k == 'pretty-print':
            v = True if v.lower() in ['on', 'true', 'yes'] else False
        _config[k] = v
    save_config()


if __name__ == '__main__':
    cli()
