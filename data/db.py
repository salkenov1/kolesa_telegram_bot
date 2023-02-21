import asyncio
import ujson
import logging
import asyncpg
from asyncpg import Pool
from typing import Tuple, Union, List

from .schemas import Car


class NotFound(Exception):
    pass


class InsertFailed(Exception):
    pass


class UpdateFailed(Exception):
    pass


class QueryError(Exception):
    pass


class DB:
    """
    Класс для работы с запросами в БД postgres
    """

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url

        event_loop = asyncio.get_event_loop()
        self._connection_pool: Pool = event_loop.run_until_complete(
            asyncpg.create_pool(db_url, init=self._init_connection)
        )

    @staticmethod
    async def _init_connection(conn: asyncpg.Connection):
        """
        Подключение к бд postgres
        """
        await conn.set_type_codec(
            'jsonb',
            encoder=ujson.dumps,
            decoder=ujson.loads,
            schema='pg_catalog'
        )

    async def _insert_into(self, tablename: str, **kwargs) -> dict:
        """
        Добавление row в таблицу tablename
        Связка данных 
            kwargs => {column_name: value}

        Возвращает добавленную row в виде dict

        При ошибке выбрасывает InsertFailed
        """

        columns = '"' + '", "'.join(list(kwargs.keys())) + '"'
        values = list(kwargs.values())
        injection = ', '.join([f"${i}" for i in range(1, len(values)+1)])

        query = f"""
            INSERT INTO "{tablename}" ({columns})
            VALUES ({injection}) RETURNING *;
        """

        try:
            async with self._connection_pool.acquire() as connection:
                result = await connection.fetchrow(query, *values)

            return dict(result)
        except Exception as e:
            module = e.__class__.__module__
            if module is None or module == str.__class__.__module__:
                errmsg = e.__class__.__name__
            errmsg = module + '.' + e.__class__.__name__

            logging.error("InsertFailed: %s >> %s\n%s %s\n",
                          errmsg, str(e), query, values)

            raise InsertFailed()

    async def _select_all(self, tablename: str, cols: List[str] = []) -> dict:
        """
        Поиск row в таблице tablename

        Связка данных 
            kwargs => {column_name: value}

        Возвращает row в виде dict

        Выбрасывает NotFound при ошибке
        """
        

        query = f"""
            SELECT DISTINCT {'*' if cols == [] else ','.join(cols)} from "{tablename}" 
        """
        print(query)
        try:
            async with self._connection_pool.acquire() as connection:
                result = await connection.fetch(query)
            if not result:
                return None
                
            return list(result)
        except Exception as e:
            raise NotFound()


    async def _select_where(self, tablename: str, cols: List[str] = [], **kwargs) -> dict:
        """
        Поиск row в таблице tablename

        Связка данных 
            kwargs => {column_name: value}

        Возвращает row в виде dict

        Выбрасывает NotFound при ошибке
        """
        columns = list(kwargs.keys())
        values = list(kwargs.values())
        injection = ' AND '.join(
            [f"{v} = ${idx+1}" for idx, v in enumerate(columns)])

        print(columns)
        print(values)
        print(injection)
        

        query = f"""
            SELECT {'*' if cols == [] else ','.join(cols)} from "{tablename}" 
            WHERE ({injection});
        """

        try:
            async with self._connection_pool.acquire() as connection:
                result = await connection.fetchrow(query, *values)
                
            return dict(result)
        except Exception as e:
            module = e.__class__.__module__
            if module is None or module == str.__class__.__module__:
                errmsg = e.__class__.__name__
            errmsg = module + '.' + e.__class__.__name__

            logging.error("CustomQueryFailed: %s >> %s\n%s",
                          errmsg, str(e), query)
            raise NotFound()

    async def _update(self, tablename: str, condition: dict, **kwargs) -> None:
        """
        Обновление данных в таблице tablename с поиском по condition

        Связка данных 
            condition => {column_name: value}
            kwargs => {column_name: value}
        """

        # Нельзя менять create_date
        kwargs.pop("create_date", None)
        if len(kwargs) == 0:
            return
        # Нет смысла обновлять
        elif len(kwargs) == 1 and "update_date" in kwargs:
            return

        columns = list(kwargs.keys())
        cond_columns = list(condition.keys())
        values = list(kwargs.values()) + list(condition.values())

        injection_set = ' ,'.join(
            [f"{v} = ${idx+1}" for idx, v in enumerate(columns)])
        injection_where = ' AND '.join(
            [f"{v} = ${len(columns) + idx+1}" for idx, v in enumerate(cond_columns)])

        query = f"""
            UPDATE "{tablename}"
            SET {injection_set}
            WHERE ({injection_where});
        """

        try:
            async with self._connection_pool.acquire() as connection:
                await connection.execute(query, *values)

        except Exception as e:
            module = e.__class__.__module__
            if module is None or module == str.__class__.__module__:
                errmsg = e.__class__.__name__
            errmsg = module + '.' + e.__class__.__name__

            logging.error("UpdateFailed: %s >> %s\n%s %s\n",
                          errmsg, str(e), query, values)

            raise UpdateFailed()

    async def _custom_query(self, query: str) -> Union[list, None]:
        """
        Исполнение кастомного query

        Возвращает результат в виде list
        """
        try:
            async with self._connection_pool.acquire() as connection:
                result = await connection.fetch(query)

            if not result:
                return None

            return list(result)
        except Exception as e:
            module = e.__class__.__module__
            if module is None or module == str.__class__.__module__:
                errmsg = e.__class__.__name__
            errmsg = module + '.' + e.__class__.__name__

            logging.error("CustomQueryFailed: %s >> %s\n%s",
                          errmsg, str(e), query)

            raise QueryError()

    async def get_brands(self) -> None:
        res = await self._select_all(
            'cars',
            cols=['brand']
        )
        return res

    async def get_brands_by_brand(self, brand: int) -> None:
        res = await self._select_where(
            'cars',
            cols=['brand'],
            brand=brand
        )
        return res