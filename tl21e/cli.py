#!/usr/bin/env python3
# Copyright (c) 2021 TIANLAN.tech
# SPDX-License-Identifier: Apache-2.0

# Language: Python

__all__ = 'main', 'exec'

import os
import asyncio
import logging

from io import FileIO
from pathlib import Path
from rich.logging import RichHandler

from .backend import Process


async def main():
    from argparse import ArgumentParser, FileType

    obj = ArgumentParser()
    obj.add_argument('-e', '--engine', type=str,
                     choices=['paddle', 'aliyun', 'tencent'],
                     default='tencent', help='speech recognition engine')
    obj.add_argument('-f', '--config', type=str,
                     metavar='file',
                     default='config.yaml', help='select config file')
    obj.add_argument('-v', '--verbose', dest='debug',
                     action='store_true',
                     default=True, help='increase logging verbosity')
    obj.add_argument('-q', '--quite', dest='quite',
                     action='store_true',
                     default=False, help='decrease logging verbosity')

    subobj = obj.add_subparsers(dest='method', required=True)
    applet = [subobj.add_parser(app) for app in ('clean', 'import', 'config',
                                                 'match', 'export', 'report')]

    applet[1].add_argument('file', type=FileIO, nargs='*')
    applet[4].add_argument('-o', '--output', type=FileType('wb'),
                           metavar='file', help='output file')
    applet[5].add_argument('-o', '--output', type=FileType('wb'),
                           metavar='file', help='output file')

    args = obj.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    level = logging.WARNING if args.quite else level
    logging.basicConfig(level=level, format='%(message)s',
                        datefmt='[%H:%M:%S]', handlers=[RichHandler()])

    _log = logging.getLogger('main')
    _log.debug(args)

    _mod = Process(Path(args.config))

    match args.method:
        case 'repl':
            await _mod.load()
            _log.info('Enter REPL')
        case 'clean':
            _log.info('Enter Clean')
            await _mod.clean()
        case 'import':
            await _mod.load()
            _log.info('Enter Import')
            await _mod.append(*args.file)
            await _mod.dump()
        case 'match':
            await _mod.load()
            _log.info('Enter Match')
            await _mod.match(args.engine)
        case 'export':
            await _mod.load()
            _log.info('Enter Export')
            await _mod.export()
        case _:
            _log.error('Unknow Method {}'.format(args.method))
            return -1

    _log.info('Done (PASS, rc=0)')


def exec():
    asyncio.run(main())


if __name__ == '__main__':
    asyncio.run(main())
