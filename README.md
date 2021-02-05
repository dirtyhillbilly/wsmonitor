<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Install](#install)
    - [Requirements](#requirements)
- [Running daemons](#running-daemons)
- [Command line interface](#command-line-interface)
    - [Commands](#commands)
        - [`url`](#url)
            - [`add`](#add)
            - [`remove`](#remove)
            - [`list`](#list)
            - [`status`](#status)
        - [`database`](#database)
        - [`config`](#config)
    - [Configuration parameters](#configuration-parameters)
- [Database schema](#database-schema)
- [TODO](#todo)

<!-- markdown-toc end -->
# Install #

## Requirements ##

You can install required dependencies to your virtual environement using :

    pip install -r requirements.txt

For testing purpose, from source root directory, you can install `wsmonitor` dependencies and endpoints with :

	pip install -e .

## Configuration ##

For now, files in `data` directories should be copied to `$HOME/.config/wsmonitor`.

# Running daemons #

In the virtual environement, besides the cli, two programs are available :
- `wsmonitor-checker-daemon` : this daemon scans the database for URLs to monitor, produces metric and report to kafka queue ;
- `wsmonitor-dbupdate-daemon` : this daemon consumes kafka events and feed postgresql database.

# Command line interface #

`wsmonitor` comes with a CLI for the common management tasks. Basic usage is :

`wsmonitor [OPTIONS] COMMAND [ARGS]...`

`wsmonitor --help` gives extended usage informations.

## Commands ##

`wsmonitor` has three major modes : `database`, `url` and `config`

### `url` ###

`url` mode manages monitored URLs. Subcommands are `add`, `remove`, `list` and `status`.

#### `add` ####

`wsmonitor url add [OPTIONS] URL [REGEXP]`

Add an URL to watch list. Optional REGEXP will be matched with content.

#### `remove` ####

`wsmonitor url remove [OPTIONS] URL`

Remove an URL from watch list.

#### `list` ####

`wsmonitor url list [OPTIONS]`

Get a list of all monitored URLs.

#### `status` ####

`wsmonitor url status [OPTIONS]`

Print status of all monitored URLs.

### `database` ###

In `database` mode, wsmonitor can initialize or reset database.

### `config` ###

`config` mode allows getting and modifying `wsmonitor` [configuration parameters](#configuration-parameters).
E.g., current configuration can be viewed with :

`wsmonitor -p config get`

## Configuration parameters ##

Configuration file `config.json`, is a JSON object with the following fields:

- `database-host` :
- `database-name` :
- `database-password` :
- `database-port` :
- `database-user`:
- `kafka-broker` :
- `kafka-user` :
- `kafka-password`:
- `kafka-topic` :
- `kafka-ca-file` : "~/.config/wsmonitor/ca.crt"
- `pretty-print` : Whether to pretty-print results. Valid values are `true` or `false`.

`wsmonitor` daemons and command line interface will look for `config.json` file in `$WSMONITORDIR` or `$HOME/.config/wsmonitor/`

# Database schema #

A `metric` type is defined as :

    TYPE metric AS (time_stamp TIMESTAMP(0) WITH TIME ZONE,
    				response_time INTEGER, return_code INTEGER,
    				regex_check BOOL);

Table `websites` holds all the metrics, and is defined by :

    CREATE TABLE websites (id SERIAL PRIMARY KEY, url VARCHAR,
                           regexp text, metrics metric[])

# TODO #

* [ ] Use timelapsedb, as it is available on aiven.io platform
* [ ] Use a kafka topic to report completion in tests (c.f. `test_api.py`)
* [ ] This document lacks a ton of informations to get wsmonitor run seamlessly
* [ ] systemd services to start checker and dbupdate daemons
* [ ] Package data are cumbersome as implemented. Use setuptools.config.
