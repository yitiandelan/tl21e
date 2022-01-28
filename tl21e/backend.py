# Copyright (c) 2021 TIANLAN.tech
# SPDX-License-Identifier: Apache-2.0

# Language: Python

__all__ = 'Process',

import os
import re
import asyncio
import logging
import yaml

from io import FileIO
from uuid import uuid4
from shutil import rmtree
from subprocess import PIPE


class Process(object):
    def __init__(self, config: FileIO) -> None:
        super().__init__()

        self._log = logging.getLogger('backend')
        self._cwd = '/home/tianlan/.cache/tl21e'

        self._cfn = config
        self._cfg: dict[str, str] = {}

        self.fileset: dict[int, dict[str, str | bool]] = {}

        if not os.path.exists(self._cwd):
            os.mkdir(self._cwd)

    async def match(self, **kwds):
        if not self._cfg:
            raise

    async def clean(self, **kwds):
        rmtree(self._cwd, ignore_errors=True)
        self.__init__(self._cfn)

    async def append(self, *file: FileIO, **kwds):
        for fp in file:
            fp.close()

        _obj: dict[int, dict[str, str]] = {}
        _pid = await asyncio.subprocess.create_subprocess_exec('sha1sum', '--tag',
                                                               *(fp.name for fp in file),
                                                               *(fp['path'] for fp in self.fileset.values()),
                                                               stdout=PIPE)
        assert await _pid.wait() == 0, _pid.returncode
        _ans = await _pid.stdout.read()

        for s in _ans.split(b'\n'):
            t = re.fullmatch(r'SHA1 \((?P<path>\S+)\) = (?P<sha1>[\da-f]{40})',
                             s.decode())
            if not t:
                continue
            elif _obj or self.fileset:
                n = max(*_obj, *self.fileset) + 1
            else:
                n = 0

            t = t.groupdict()
            # overwrite
            for k, v in self.fileset.items():
                if t['path'] != v['path']:
                    continue
                n = k
            _obj.setdefault(n, t)

        for k, v in _obj.items():
            fn = os.path.join(self._cwd, v['sha1'])
            if os.path.exists(fn):
                _obj[k]['hashed'] = True
                continue
            _cmds: list[tuple] = []
            match v['path'].split('.')[-1]:
                case 'wav' | 'mp3':
                    _cmds.append(('ffmpeg', '-i', os.path.abspath(v['path']), '-f', 'ffmetadata',
                                  '-v', '0', '-y', '{}.metadata'.format(fn)))
                    _cmds.append(('ffmpeg', '-i', os.path.abspath(v['path']), '-f', 'wav',
                                  '-ar', '16000', '-ac', '1', '-report', '-v', '0', '-y', fn))
                case 'md' | 'fountain':
                    _cmds.append(('cp', '-f', os.path.abspath(v['path']), fn))
                case '_':
                    continue

            for _cmd in _cmds:
                _pid = await asyncio.subprocess.create_subprocess_exec(*_cmd, stdout=PIPE, cwd=self._cwd)
                await _pid.wait()
                assert await _pid.wait() == 0, _pid.returncode

            if not os.path.exists(fn):
                self._log.error('Can\'t HASHED for: {}'.format(v['path']))
            _obj[k]['hashed'] = True

        self.fileset.update(_obj)
        self._log.debug(self.fileset)

    async def load(self, **kwds):
        self._cfg = yaml.safe_load(self._cfn)
        self._cfn.close()

        if not isinstance(self._cfg, (dict,)):
            self._cfg = dict(name='normal', media=[],
                             uuid=uuid4().hex)

        for k, v in enumerate(self._cfg.get('media', ())):
            fp = v.split(':')
            if not os.path.isfile(fp[0]):
                continue
            self.fileset.setdefault(k, dict(path=fp[0]))
            if len(fp) == 1 or not os.path.isfile(fp[1]):
                continue
            sha1 = os.path.basename(fp[1]).split('.')[0]
            if len(sha1) != 40:
                continue
            self.fileset[k]['sha1'] = sha1
            self.fileset[k]['hashed'] = True

        self._cfg.pop('media')
        self._log.debug(self.fileset)

    async def dump(self, **kwds):
        def gen_fileset():
            t = list(self.fileset.keys())
            t.sort()
            yield from (self.fileset[k] for k in t)

        _obj = dict(media=['{}:{}'.format(v['path'], os.path.join(self._cwd, v['sha1']))
                           for v in gen_fileset()])
        _obj.update(self._cfg)
        _cfg = yaml.safe_dump(_obj)

        with open(self._cfn.name, 'w') as fp:
            fp.write(_cfg)

    async def export(self, **kwds):
        if not self._cfg:
            raise

    async def report(self, **kwds):
        if not self._cfg:
            raise
