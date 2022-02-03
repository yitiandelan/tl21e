# Copyright (c) 2021 TIANLAN.tech
# SPDX-License-Identifier: Apache-2.0

# Language: Python

__all__ = 'Fountain',

import os
import re
from io import FileIO
from tatsu import compile


class Fountain(object):
    EBNF = '''
    @@grammar::FOUNTAIN
    start = {head} {scene}+ $;
    scene = scene_name scene_body ;
    scene_name = &'.' /.+/ ;
    scene_body = {speaker_name speaker_body}+ ;
    words = /[^.\n]+/ ;
    speaker_name = &'@' words | {} ;
    speaker_body = words ;
    head = /[A-Z][a-z ]+: ?/ /\S+/ ;
    '''

    def __init__(self, path=''):
        self.model = compile(self.EBNF)

        self.heads = {}
        self.nrows = {}
        self.scene = {}

        if os.path.isfile(path):
            self.text = FileIO(path, 'r')
            self.parse()
        else:
            self.text = None

    def parse(self, text: str = ''):
        if text:
            _txt = text
        elif isinstance(self.text, (FileIO,)):
            _txt = self.text.readall().decode()
        else:
            raise TypeError

        _obj = self.model.parse(_txt)
        _del = r'[\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\uff01\u2026]'

        self.heads = {re.sub(r': ?', '', k): v
                      for k, v in _obj[0]}
        self.scene = {k: {'title': re.sub('^\.', '', v[0]),
                          'lines': {i: {'name': b[0][1:] if b[0] else None,
                                        'word': list(filter(None, re.split(_del, b[1])))}
                                    for i, b in enumerate(v[1])}}
                      for k, v in enumerate(_obj[1])}

        return self
