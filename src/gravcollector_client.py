# -*- coding: utf-8 -*-

import sys
import argparse
import logging
import json
from pathlib import Path

from serial.tools.list_ports import comports

from src import __description__, __version__
from src.client import run_client

LOG = logging.getLogger()
_stream_handler = logging.StreamHandler(sys.stderr)
_stream_handler.setFormatter(logging.Formatter(fmt='%(asctime)s - %(levelname)s :: %(message)s'))
LOG.addHandler(_stream_handler)


def parse_args(argv=None):
    args = argv or sys.argv[1:]

    parser = argparse.ArgumentParser(prog=sys.argv[0],
                                     description=__description__,
                                     allow_abbrev=True)

    parser.add_argument('config', nargs='?', type=argparse.FileType('r'))
    parser.add_argument('-V', '--version', action='version', version=__version__)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--sensor-name', action='store')
    parser.add_argument('-d', '--device', action='store')
    parser.add_argument('--sensor-type', action='store')
    parser.add_argument('--host', action='store')
    parser.add_argument('--apikey', action='store')
    parser.add_argument('--meterini', action='store', type=Path)

    return parser.parse_args(args)


def entry_point():
    args = parse_args()
    if args.debug:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.WARNING)

    cli_config = {k: v for k, v in vars(args).items() if v is not None}
    if args.config:
        LOG.info("Loading JSON config from file")
        cfg: dict = json.load(args.config)

        cfg.update(cli_config)
        LOG.info(f'Using sensor name: {cfg["sensor_name"]}')
        LOG.info(f'Merged config: {cfg}')
        LOG.critical("Starting GravityCollector Client")
        return sys.exit(run_client(**cfg))

    if args.device is None:
        LOG.warning('No device specified')
        LOG.info('Available comports:')
        for port in comports():
            print(port.device)
        return
    else:
        sys.exit(run_client(**cli_config))


if __name__ == '__main__':
    entry_point()
