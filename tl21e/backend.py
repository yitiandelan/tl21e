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
from subprocess import PIPE


class Process(object):
    def __init__(self, config: FileIO) -> None:
        super().__init__()

        self._log = logging.getLogger('backend')
        self._cwd = '/home/tianlan/.cache/tl21e'

        self._cfn = config
        self._cfg = yaml.safe_load(self._cfn)
        self._cfn.close()

        if not isinstance(self._cfg, (dict)):
            self._cfg = dict(name='normal', media=[],
                             uuid=uuid4().hex)

        self.fileset: dict[int, dict[str, str | bool]] = {}

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

        if not os.path.exists(self._cwd):
            os.mkdir(self._cwd)

    async def match(self, **kwds):
        if not self._cfg:
            raise

    async def clean(self, **kwds):
        pass

    async def append(self, *file: FileIO, **kwds):
        for fp in file:
            fp.close()

        _ext = {'mp3': '.wav', 'wav': '.wav',
                'md': '.md', 'fountain': '.md'}
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
            elif _obj:
                n = max(_obj) + 1
            elif self.fileset:
                n = max(self.fileset) + 1
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
            fn += _ext.get(v['path'].split('.')[-1], '')
            if os.path.exists(fn):
                _obj[k]['hashed'] = True
                continue
            match fn.split('.')[-1]:
                case 'wav':
                    _cmd = ('ffmpeg', '-i', os.path.abspath(v['path']),
                            '-ar', '16000', '-ac', '1', '-report', '-v', '0', '-y', fn)
                case 'md':
                    _cmd = ('cp', '-f', os.path.abspath(v['path']), fn)
                case '_':
                    continue

            _task = await asyncio.subprocess.create_subprocess_exec(*_cmd, stdout=PIPE, cwd=self._cwd)
            await _task.wait()

            if not os.path.exists(fn):
                self._log.error('Can\'t HASHED for: {}'.format(v['path']))
            _obj[k]['hashed'] = True

        self.fileset.update(_obj)
        self._log.debug(self.fileset)

    async def export(self, **kwds):
        if not self._cfg:
            raise

    async def report(self, **kwds):
        if not self._cfg:
            raise
