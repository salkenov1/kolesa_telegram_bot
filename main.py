import logging
import timeit
import requests
import json


import conf

from io import BytesIO

from aiogram import Bot, Dispatcher, executor
from aiogram.utils.exceptions import NetworkError

from data import DB


db = DB(conf.POSTGRES_URL)

bot = Bot(token=conf.BOT_TOKEN)
dp = Dispatcher(bot)


def run_bot():
    from handlers import common

    # filters.regist_filters(dp)

    common.regist_handlers(dp)

    try:
        executor.start_polling(dp)  # skip_updates
    except NetworkError as e:
        module = e.__class__.__module__
        if module is None or module == str.__class__.__module__:
            errmsg = e.__class__.__name__
        errmsg = module + '.' + e.__class__.__name__

        logging.error("ClientConnectorError: %s >> %s", errmsg, str(e))

        raise e


if __name__ == '__main__':
    run_bot()
