# Copyright (c) 2021 TIANLAN.tech
# SPDX-License-Identifier: Apache-2.0

# Language: Python

__all__ = 'Process',

import os
import re
import asyncio
import logging
import yaml
import json

from io import FileIO, TextIOWrapper
from os import PathLike
from uuid import uuid4
from shutil import rmtree
from asyncio import Queue
from asyncio.exceptions import CancelledError
from subprocess import PIPE
from tempfile import TemporaryDirectory
from base64 import b64encode
from thefuzz import fuzz, process
from pypinyin import lazy_pinyin
from itertools import chain

from format import Fountain

try:
    from tencentcloud.common import credential
    from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
    from tencentcloud.asr.v20190614 import asr_client, models
except ImportError:
    pass


class ASRClient(object):
    def __init__(self, engine: str = 'paddle') -> None:
        self._cfg: dict[str, str] = {}
        self._eng = engine
        self._log = logging.getLogger('asr_client')
        self._pool = Queue()
        self._text = Queue()
        self._task: list[asyncio.subprocess.Process] = []

        fn = os.path.join('/home/tianlan/.config', 'tl21e', 'config.json')

        if os.path.exists(fn):
            fp = FileIO(fn, 'rb')
            self._cfg = json.load(TextIOWrapper(fp, 'utf-8'))

        self._cfg.setdefault('aliyun', {})
        self._cfg.setdefault('tencent', {})

        if engine == 'tencent':
            _key = [self._cfg.get(engine, {}).get(k, None)
                    for k in ('SecretId', 'SecretKey')]
            if None in _key:
                raise BaseException('Can\'t Find API Keys: {}'.format(fn))
            cred = credential.Credential(*_key)
            self._eng = asr_client.AsrClient(cred, '')

    async def append(self, file: PathLike[str], **kwds):
        from auditok import load, AudioRegion

        async def request():
            if self._pool.empty():
                return
            try:
                args = await self._pool.get()
                uid, offset, waveform = args
                req = models.SentenceRecognitionRequest()
                req._deserialize(dict(SubServiceType=2,
                                      ProjectId=0,
                                      EngSerViceType='16k_zh',
                                      SourceType=1,
                                      Data=waveform,
                                      DataLen=len(waveform),
                                      VoiceFormat='wav',
                                      UsrAudioKey='test',
                                      WordInfo=1,
                                      ConvertNumMode=0,
                                      FilterPunc=2))
                resp = self._eng.SentenceRecognition(req)
                # self._log.info(resp.to_json_string())
            except TencentCloudSDKException as err:
                self._log.error(err)
                return
            except CancelledError:
                return
            except BaseException as err:
                self._log.error(err)
                return

            _ans = [dict(word=ws.Word,
                         timecode=(ws.StartTime+offset, ws.EndTime+offset))
                    for ws in resp.WordList]

            await self._text.put((uid, _ans))
            await request()

        async def process(fp: PathLike[str], ep: Queue):
            _raw = load(fp, audio_format='wav', sr=16000, ch=1)
            _ans: list[dict] = []

            if _raw.duration < 59:
                _wav = {0: dict(offset=0, scene=_raw)}
            else:
                _wav, n = {}, 0
                for k, v in enumerate(_raw.split(max_dur=20)):
                    if v.meta['end'] - n > 50:
                        _wav[k] = dict(offset=int(n*1000),
                                       scene=_raw.seconds[n:v.meta['start']])
                        n = v.meta['start']
                    if v.meta['end'] - n < 45:
                        continue
                if k not in _wav:
                    _wav[k] = dict(offset=int(n*1000),
                                   scene=_raw.seconds[n:v.meta['end']])
            for k, v in _wav.items():
                self._log.debug(v['scene'])
                with TemporaryDirectory() as d:
                    fn = os.path.join(d, 'abc')
                    v['scene'].save(fn, audio_format='wav')
                    bs = b64encode(FileIO(fn, 'rb').readall())
                await self._pool.put((k, v['offset'], bs.decode('utf-8')))

            for k, v in [await self._text.get() for _ in _wav]:
                _ans += v

            assert _ans
            await ep.put(_ans)

        _obj = Queue()
        asyncio.gather(process(file, _obj), *(request() for _ in range(2)))
        return _obj

    async def split(self, **kwds):
        pass

    async def load(self, **kwds):
        pass

    async def close(self, **kwds):
        pass


class Process(object):
    def __init__(self, config: FileIO, engine: str = 'paddle') -> None:
        super().__init__()

        self._log = logging.getLogger('backend')
        self._cwd = '/home/tianlan/.cache/tl21e'

        self._cfn = config
        self._cfg: dict[str, str] = {}
        self._asr = None
        self._eng = engine
        self._map: dict[int, tuple] = {}

        self.fileset: dict[int, dict[str, str | bool]] = {}

        if not os.path.exists(self._cwd):
            os.mkdir(self._cwd)

    async def match(self, engine: str = '', **kwds):
        if not self._cfg:
            raise
        # overwrite
        if engine:
            self._eng = engine

        if not self.fileset:
            self._log.error('Empty Fileset!')
            return -1

        for k, v in self.fileset.items():
            fn = os.path.join(self._cwd, '{}.json'.format(v['sha1']))
            if os.path.exists(fn) and os.path.getsize(fn):
                continue
            self._log.info('Enter Pre-treat')
            try:
                await self.pretreat()
            except BaseException as err:
                self._log.error(err)
                return -1

            if os.path.exists(fn) and os.path.getsize(fn):
                continue
            self._log.error(v)
            return -1

        _obj: dict[str, dict] = {}
        _raw: dict[int, dict] = {}

        for k, v in self.fileset.items():
            fn = os.path.join(self._cwd, '{}.json'.format(v['sha1']))
            with FileIO(fn, 'rb') as fp:
                f0 = lambda x: int(x) if isinstance(x, str) and x.isdigit() else x
                f1 = lambda x: x if not isinstance(x, dict) else {f0(k): v for k, v in x.items()}
                _raw[k] = json.load(fp, object_hook=f1)
            if 'scene' not in _raw[k]:
                continue
            if _obj:
                raise

            _obj = _raw[k]['scene'][0]
            _obj = dict(title=_obj['title'],
                        match={}, track={},
                        lines={int(k): v for k, v in _obj['lines'].items()},
                        speaker={k: v for k, v in enumerate(set(v['name'] for v in _obj['lines'].values()))})
            _raw[k]['scene'][0] = _obj

        _cfg = (len(_obj['speaker']), len(_raw) - 1)
        self._log.info(dict(speakers=_cfg[0],
                            audio_track=_cfg[1]))

        def gen_track(s: int, lpf=False, within=0, before=0):
            if within or before:
                for d in gen_track(s=s, lpf=lpf):
                    if max(d['timecode']) < within:
                        continue
                    if before and min(d['timecode']) > before:
                        continue
                    yield d
                return

            ws: list[dict] = _raw[s]['asr']
            def tc(x: dict): return min(*x.get('timecode', (-1)))
            ws.sort(key=tc)
            if lpf:
                s, t0, t1 = '', 0, 0
                for k, w in enumerate(ws):
                    if k == 0:
                        t1 = min(w['timecode'])
                    if min(w['timecode']) != t1:
                        yield dict(word=s, timecode=(t0, t1))
                        s, t0 = '', min(w['timecode'])
                    s += w['word']
                    t1 = max(w['timecode'])
                yield dict(word=s, timecode=(t0, t1))
            else:
                yield from ws

        def gen_speaker():
            yield from _obj['speaker'].items()

        def gen_lines(*s: int):
            dst = list(_obj['speaker'][n] for n in s)
            for n in range(max(_obj['lines'])+1):
                if _obj['lines'][n]['name'] not in dst:
                    continue
                yield n, _obj['lines'][n]['name'], _obj['lines'][n]['word']

        _tmp: dict[int, list] = {}
        # enter speaker match base track
        for _t, _v in _raw.items():
            if 'asr' not in _v:
                continue

            self._log.info('Start Match Audio Track {}'.format(_t))
            line = lazy_pinyin([n['word'] for n in _v['asr']])
            line = ' '.join(line)

            for k, v in _obj['speaker'].items():
                cc = [[] if _obj['lines'][n]['name'] != v else _obj['lines'][n]['word']
                      for n in range(max(_obj['lines'])+1)]
                for c in cc:
                    if len(c) == 0:
                        continue
                    c = lazy_pinyin(c)
                    c = ' '.join(c)
                    t = fuzz.partial_ratio(c, line)

                    if t < 95:
                        continue
                    elif len(c) < 10:
                        continue

                    self._log.debug(dict(uid=k,
                                         name=v,
                                         size=len(c),
                                         ratio=t))
                    _tmp.setdefault(_t, [])
                    _tmp[_t].append(k)

        # update result
        for k in _raw:
            if not _tmp.get(k):
                continue
            v = {k: tuple(set(_tmp[k]))}
            self._map.update(v)
        self._log.info('Match Result {}'.format(self._map))

        _tmp: dict[int, list] = {}
        # enter speaker match base self
        for k, v in gen_speaker():
            if k in tuple(chain(*self._map.values())):
                continue
            self._log.info('Start Match Speaker {} ({})'.format(k, v))

            for _t, _v in _raw.items():
                if 'asr' not in _v:
                    continue
                dd = [p.get('word', '') for p in gen_track(_t, True)]
                dd = ' '.join(lazy_pinyin(dd))

                cc = list(p[2] for p in gen_lines(*self._map.get(_t, ())))
                cc = list(chain(*cc))
                cc = ' '.join(lazy_pinyin(cc))
                r0 = fuzz.ratio(dd, cc)

                cc = list(p[2] for p in gen_lines(*self._map.get(_t, ()), k))
                cc = list(chain(*cc))
                cc = ' '.join(lazy_pinyin(cc))
                r1 = fuzz.ratio(dd, cc)

                self._log.debug(dict(uid=k,
                                     name=v,
                                     track=_t,
                                     diff=r0-r1))
                _tmp.setdefault(k, [])
                _tmp[k].append((_t, r0-r1))

        # update result
        if len(_tmp) == 1:
            for k, v in _tmp.items():
                v = min(v, key=lambda x: x[1])[0]
                break
            _tmp = {v: tuple((*self._map.get(v, ()), k))}
            self._map.update(_tmp)
            self._log.info('Auto Select Track {}'.format(v))
        self._log.info('Match Result {}'.format(self._map))

        # check
        for k, v in gen_speaker():
            if k in tuple(chain(*self._map.values())):
                continue
            self._log.error('Can\'t Match Speaker {} ({})'.format(k, v))
            return -1

        for k, v in self._map.items():
            _obj['track'][k] = self.fileset[k]
            _obj['track'][k]['speaker'] = v

        self._log.info('Start Match Script {} ({})'.format(0, _obj['title']))
        _tmp: dict[int, list] = {k: 0 for k in _raw}

        def get_track(n):
            for s in _obj['speaker']:
                if n != _obj['speaker'][s]:
                    continue
                for t in self._map:
                    if s not in self._map[t]:
                        continue
                    return t
            raise

        for k, n, w in gen_lines(*chain(*self._map.values())):
            self._log.info('{} ({}) [cyan]{}[/cyan]'.format(n, k, ' '.join(w)),
                           extra=dict(markup=True))

            n = get_track(n)
            _c = {k: ' '.join(lazy_pinyin(v))
                  for k, v in enumerate(w)}

            _sop = _tmp[n]
            _eop = len(''.join(_c.values()).split()) * int(1000 * 0.6)
            _eop = max(16*1000, _eop) + _sop

            _s = [p for p in gen_track(n, within=_sop, before=_eop)]
            _s = {k: v for k, v in enumerate(_s)}
            _d = {k: ' '.join(lazy_pinyin(_s[k]['word']))
                  for k in range(max(_s)+1)}

            def gen_table(ref: str = '', start: int = 0):
                if start not in _d:
                    return
                t, c, g = len(ref.split()), 0, []
                for k in range(start, max(_d)+1):
                    g.append(k)
                    c += len(_d[k].split())
                    if c < (t - 2):
                        continue
                    elif c > (t + 6):
                        break
                    yield g
                yield from gen_table(ref=ref, start=start+1)

            _ans: dict[int, list] = {}
            for i, c in _c.items():
                for g in gen_table(c):
                    d = ' '.join(_d[p] for p in g)
                    t = process.extractOne(d, _c)
                    if not t:
                        continue
                    elif t[2] != i:
                        continue
                    elif t[1] < 90:
                        continue
                    _ans.setdefault(i, [])
                    _ans[i].append((t[1], g[0], g[-1]))
            for i in range(max(_c)+1):
                if i not in _ans:
                    continue
                _ans[i] = max(_ans[i])

            if _ans:
                _ans = {k: (min(_s[v[1]]['timecode']), max(_s[v[2]]['timecode']))
                        for k, v in _ans.items()}
                _obj['lines'][k]['track'] = n
                _obj['lines'][k]['timecode'] = _ans
                _tmp[n] = max(_ans[max(_ans)]) - 1000
            else:
                self._log.error(
                    'Can\'t Match Script {} Line ({})'.format(0, k))
                pass

        fn = os.path.abspath('results.json')
        with FileIO(fn, 'wb') as fp:
            json.dump({0: _obj},
                      TextIOWrapper(fp, 'utf-8'), ensure_ascii=False)
        self._log.info('Write JSON: {}'.format(fn))
        self._log.info('Finish Match Script')
        return 0

    async def pretreat(self, **kwds):
        if not self._cfg:
            raise
        _eng = self._eng
        self._log.info('Select ASR Engine: {}'.format(_eng))
        self._asr = ASRClient(_eng)
        await self._asr.load()

        for k, v in self.fileset.items():
            self._log.debug(v)
            match v['path'].split('.')[-1]:
                case 'wav' | 'mp3':
                    fn = os.path.join(self._cwd, '{}.json'.format(v['sha1']))
                    if os.path.exists(fn) and os.path.getsize(fn):
                        continue
                    fp = FileIO(fn, 'wb')
                    try:
                        if not isinstance(self._asr, (ASRClient, )):
                            raise
                        _obj = await self._asr.append(os.path.join(self._cwd, v['sha1']))
                        _ans = await _obj.get()
                        json.dump(dict(asr=_ans),
                                  TextIOWrapper(fp, 'utf-8'), ensure_ascii=False)
                        self._log.info('Write JSON: {}'.format(fn))
                    except BaseException as err:
                        self._log.error(err)
                    finally:
                        fp.close()
                    continue
                case 'md' | 'fountain':
                    fn = os.path.join(self._cwd, '{}.json'.format(v['sha1']))
                    if os.path.exists(fn) and os.path.getsize(fn):
                        continue
                    fp = FileIO(fn, 'wb')
                    try:
                        _doc = Fountain(os.path.join(self._cwd, v['sha1']))
                        json.dump(dict(heads=_doc.heads, scene=_doc.scene),
                                  TextIOWrapper(fp, 'utf-8'), ensure_ascii=False)
                        self._log.info('Write JSON: {}'.format(fn))
                    except BaseException as err:
                        self._log.error(err)
                    finally:
                        fp.close()
                    continue
                case '_':
                    continue

        await self._asr.close()

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

        if not isinstance(self._cfg, (dict, )):
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
        # overwrite
        self._eng = self._cfg.get('engine', self._eng)

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
