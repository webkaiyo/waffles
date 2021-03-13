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

import asyncpg

from .column import Column
from .table import Table, _create_table_query, _drop_table_query
from .errors import *


__all__ = ('Database',)


class Database:
    def __init__(self, *, pool: asyncpg.Pool):
        self._pool = pool
        self._tables = {}

    def __repr__(self):
        return '<Database>'

    @property
    def tables(self) -> list[Table]:
        return list(self._tables.values())

    def get_table(self, name: str) -> Table:
        return self._tables.get(name)

    async def create_table(self, name: str, columns: list[Column], *, exists_ok: bool = True) -> Table:
        sql = _create_table_query(name, columns, exists_ok)

        async with self._pool.acquire() as conn:
            await conn.execute(sql)

        table = Table(name, columns, pool=self._pool)
        self._tables[name] = table
        return table

    async def drop_table(self, name: str, *, exists_ok: bool = True):
        if name not in self._tables:
            raise TableDoesNotExist()

        sql = _drop_table_query(name, exists_ok)

        async with self._pool.acquire() as conn:
            await conn.execute(sql)

        del self._tables[name]
