'''
MIT License

Copyright (c) 2021 Caio Alexandre

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import typing

from .errors import SchemaError


__all__ = (
    'SQLType',
    'Integer',
    'String',
    'Json'
)


class SQLType:
    python: typing.Any = None

    def __str__(self):
        return self.__class__.__name__

    def to_sql(self) -> str:
        raise NotImplementedError()


class Integer(SQLType):
    python = int

    def __init__(self, *, big: bool = False, small: bool = False, auto_increment: bool = False):
        self.big = big
        self.small = small
        self.auto_increment = auto_increment

        if big and small:
            raise SchemaError('Integer column type cannot be both big and small')

    def to_sql(self):
        if self.auto_increment:
            if self.big:
                return 'BIGSERIAL'
            if self.small:
                return 'SMALLSERIAL'
            return 'SERIAL'

        if self.big:
            return 'BIGINT'
        if self.small:
            return 'SMALLINT'
        return 'INTEGER'


class String(SQLType):
    python = str

    def __init__(self, *, length: int = None, fixed: bool = False):
        if fixed and not length:
            raise SchemaError('Cannot have fixed string with no length')

        self.length = length
        self.fixed = fixed

    def to_sql(self):
        if not self.length:
            return 'TEXT'

        if self.fixed:
            return f'CHAR({self.length})'

        return f'VARCHAR({self.length})'


class Json(SQLType):
    python = [list, dict]

    def to_sql(self):
        return 'JSONB'
