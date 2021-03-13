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

from .types import SQLType, String


__all__ = ('Column',)


class Column:
    def __init__(self, name: str, column_type: SQLType, *, index: bool = False,
                 primary_key: bool = False, nullable: bool = False, unique: bool = False,
                 default: typing.Any = None):

        if isinstance(column_type, type):
            column_type = column_type()

        if not isinstance(column_type, SQLType):
            # TODO: Add error message.
            raise TypeError()

        self.name = name
        self.column_type = column_type

        self.index = index
        self.primary_key = primary_key
        self.nullable = nullable
        self.unique = unique
        self.default = default

        # to be filled later.
        self.index_name = None

    def __repr__(self):
        return '<Column name={0.name!r} type={0.column_type}>'.format(self)

    def to_sql(self) -> str:
        builder = []

        builder.append(self.name)
        builder.append(self.column_type.to_sql())

        default = self.default

        if default:
            builder.append('DEFAULT')

            if isinstance(default, str) and isinstance(self.column_type, String):
                builder.append("'%s'" % default)
            elif isinstance(default, bool):
                builder.append(str(default).upper())
            else:
                builder.append('(%s)' % default)
        elif self.unique:
            builder.append('UNIQUE')

        if not self.nullable:
            builder.append('NOT NULL')

        return ' '.join(builder)
