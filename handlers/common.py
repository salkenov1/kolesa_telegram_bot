import timeit
import logging

from aiogram import types, Dispatcher

from main import db, bot
from data import NotFound


async def main_handler(message: types.Message):
    text = """
        Привет, мудило, пиши команды:
            /город - чтобы найти объявы по городу
            /марка - чтобы найти объявы по маркам автомобиля
            /цена - чтобы найти объявы по цене
    """
    await message.answer(text)

async def offers_by_brand(message: types.Message):

    if message.get_args() == '':
        cars = await db.get_brands()
    else:
        cars = await db.get_brands_by_brand(message.text.split(' ')[1])

    print(cars)
    for i, car in enumerate(cars):
        if i > 5:
            break
        print(car['brand'])


def regist_handlers(dp: Dispatcher):
    dp.register_message_handler(main_handler, lambda m: m.get_command(
        pure=True) in [None, 'start'], content_types=[types.ContentType.TEXT])
    dp.register_message_handler(offers_by_brand, lambda m: m.get_command(
        pure=True) in [None, 'марка'], content_types=[types.ContentType.TEXT])
