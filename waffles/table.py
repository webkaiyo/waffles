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

import asyncpg

from .column import Column
from .row import Row
from .errors import *


class Table:
    def __init__(self, name: str, columns: list[Column], *, pool: asyncpg.Pool):
        self.name = name

        self._pool = pool
        self._columns = {column.name: column for column in columns}

        for column in columns:
            if column.index:
                value.index_name = '%s_%s_idx' % (name, column.name)

    def __repr__(self):
        return '<Table name={0.name!r} columns={0.columns}>'.format(self)

    @property
    def columns(self) -> list[Column]:
        return list(self._columns.values())

    async def add(self, **values) -> Row:
        for name, value in values.items():
            if name not in self._columns:
                raise IncorrectColumn()

            column = self._columns[name]
            column_type = column.column_type

            if not isinstance(value, column_type.python):
                raise TypeError()

        sql = _insert_row_table_query(self.name, list(values.keys()))

        async with self._pool.acquire() as conn:
            record = await conn.fetchrow(sql, *values.values())

        return Row(record=record)


def _create_table_query(name: str, columns: list[Column], exists_ok: bool):
    statements = []
    builder = ['CREATE TABLE']

    if exists_ok:
        builder.append('IF NOT EXISTS')

    builder.append(name)

    column_creations = []
    primary_keys = []

    for column in columns:
        column_creations.append(column.to_sql())

        if column.primary_key:
            primary_keys.append(column.name)

    if primary_keys:
        column_creations.append('PRIMARY KEY (%s)' % ', '.join(primary_keys))

    builder.append('(%s)' % ', '.join(column_creations))
    statements.append(' '.join(builder) + ';')

    for column in columns:
        if column.index:
            sql = 'CREATE INDEX IF NOT EXISTS {1.index_name} ON {0} ({1.name});'
            statements.append(sql.format(name, column))

    return '\n'.join(statements)


def _drop_table_query(name: str, exists_ok: bool):
    builder = ['DROP TABLE']

    if exists_ok:
        builder.append('IF EXISTS')

    builder.append(name)
    return ' '.join(builder)


def _insert_row_table_query(name: str, columns: list[str]):
    builder = ['INSERT INTO']

    builder.append(name)
    builder.append('(%s)' % ', '.join(columns))

    arguments = [f'${i + 1}' for i in range(len(columns))]

    builder.append('VALUES (%s)' % ', '.join(arguments))
    builder.append('RETURNING *;')

    return ' '.join(builder)
