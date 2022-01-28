#!/usr/bin/env python3
# Copyright (c) 2021 TIANLAN.tech
# SPDX-License-Identifier: Apache-2.0

# Language: Python

__all__ = 'main',

import os
import asyncio
import logging

from io import FileIO
from rich.logging import RichHandler

from backend import Process


async def main():
    from argparse import ArgumentParser, FileType

    obj = ArgumentParser()
    obj.add_argument('-e', '--engine', type=str,
                     metavar='engine', choices=['paddle', 'aliyun', 'tencent'],
                     default='paddle', help='speech recognition engine')
    obj.add_argument('-f', '--config', type=str,
                     metavar='file',
                     default='config.yaml', help='select config file')
    obj.add_argument('-v', '--verbose', dest='debug',
                     action='store_true',
                     default=False, help='increase logging verbosity')
    obj.add_argument('-q', '--quite', dest='quite',
                     action='store_true',
                     default=False, help='decrease logging verbosity')

    subobj = obj.add_subparsers(dest='method')
    applet = [subobj.add_parser(app) for app in ('clean', 'import', 'replay',
                                                 'match', 'export', 'report')]

    applet[1].add_argument('file', type=FileIO, nargs='+')
    applet[4].add_argument('-o', '--output', type=FileType('wb'),
                           metavar='file', help='output file')
    applet[5].add_argument('-o', '--output', type=FileType('wb'),
                           metavar='file', help='output file')

    args = obj.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    level = logging.WARNING if args.quite else level
    logging.basicConfig(level=level, format='%(message)s',
                        datefmt='[%X]', handlers=[RichHandler()])

    _log = logging.getLogger('main')
    _log.debug(args)

    if not os.path.isfile(args.config):
        os.system('touch {}'.format(args.config))
    _mod = Process(FileIO(args.config, 'rb'))

    match args.method:
        case None:
            _log.debug('Enter REPL')
        case 'clean':
            pass
        case 'import':
            await _mod.append(*args.file)
        case '_':
            pass

    _log.debug('Exited')

if __name__ == '__main__':
    asyncio.run(main())
