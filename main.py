import asyncio
import aiohttp
import json
import os
import time # 
import sqlite3
from datetime import date
import threading
import datetime

# Твои комментарии полностью удаляю, чтобы не мешались с моими

# Глобальные переменные плохи тем, что они являются по сути указателями на ячейку в памяти
# Значения под переменными не копируются в область видимости функции,
# а используются разными функциями напрямую.
#
# Это приводит к тому, что если у тебя есть, например, несколько потоков, то они могут в в один и тот же момент времени
# записать в глобальную переменную какие-то данные. Это состояние называется data race.
# Оно может возникнуть и в случае, если ты используешь корутины.

row_data = []
amount_r = 0

# Не имеет смысл объявлять global: left_itr и так объявлена на глобальном уровне
global left_itr
left_itr = 20000

async def find_product_by_id(number_of_requests):
    global amount_r

    # Из-за этого полотна код плохо читается. Заходя в функцию я хочу понять, что она делает,
    # а не читать километры заголовка запроса. Если юзаешь глобальные переменные, наиболее логично
    # было бы вынести переменную headers туда, поскольку она не переписывается на протяжении всей программы
    headers = {
            'Host': 'api.kazanexpress.ru',
            'Sec-Ch-Ua': '"Chromium";v="103", ".Not/A)Brand";v="99"',
            'Accept-Language': 'ru-RU',
            'Sec-Ch-Ua-Mobile': '?0',
            'Authorization': 'Basic a2F6YW5leHByZXNzLWN1c3RvbWVyOmN1c3RvbWVyU2VjcmV0S2V5',
            'Content-Type': 'application/json',  # если стоит content-type, сервер ожидает этот контент в теле запроса
            'Accept': 'application/json',        # не вижу, чтобы ты пересылал json
            'X-Iid': 'ccc494fd-21aa-40eb-9228-ce7a98459b4b',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36',
            'Access-Content-Allow-Origin': '*',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Origin': 'https://kazanexpress.ru',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://kazanexpress.ru/',
            # 'Accept-Encoding': 'gzip, deflate',
            }

    async with aiohttp.ClientSession() as session: 
        tasks = []
        for i in range(number_of_requests):
            # У тебя здесь i принимает значение числа, и ты можешь его использовать вместо amount_r
            tasks.append(asyncio.create_task(session.get(url = f'https://api.kazanexpress.ru/api/v2/product/{amount_r}',headers = headers)))
            # При этом инкремент будет не нужен, так как в процессе итерации i всегда прибавляет +1
            amount_r = amount_r + 1
        responses = await asyncio.gather(*tasks)
        # Ты каждый раз возвращаешь из генератора СПИСОК, а не значение в списке
        # То, что ты хочешь сделать, называется yield from ...
        yield [await r.text(encoding='UTF-8') for r in responses]

#запрос 3000


def parse_product():
    # У тебя реально база называется "времядата.db"? :)
    conn = sqlite3.connect(str(datetime.datetime.now().strftime('%Y-%m-%d-%S'))+'.db')
    cur = conn.cursor()

    # Вложенные функции объявляются наверху
    # Иначе непонятно где они заканчиваются
    def make_data_base():
        print('Создание базы данных')
        '''Создает базу данных'''
        cur.execute("""CREATE TABLE IF NOT EXISTS products(
            id INT UNIQUE,
            название  TEXT,
            продажи INT,
            рейтинг TEXT,
            отзывы TEXT,
            категория TEXT,
            is_eco TEXT,
            производитель TEXT,
            наличие TEXT,
            атрибуты TEXT,
            описание TEXT,
            цена TEXT,
            время_парсинга TEXT);""")
        conn.commit()

    make_data_base()
    global data

    # Этот цикл никогда не завершится или завершится только потому, что интерпретатор питона делали умные люди
    # По идее программа будет вечно проверять что длина row_data больше нуля, даже после того, как проверка дала False
    while True:
        if len(row_data) > 0:
            items = row_data.pop(0) 
            for i in items:
                try:
                    # Для вот этой портянки можно составить словарь соответствия твоих ключей и ключей в ответе сервера
                    # Можно добавить только те ключи, которые отличаются
                    # Тогда можно будет обойтись без переменных и получать значения так:
                    #   divergent_keys = {
                    #       "id": "item_id"
                    #       ...
                    #   }
                    #   def get_entry(divergent_keys: dict, item: dict) -> Any:
                    #       for k, v in item.items():
                    #           if k in divergent_keys:
                    #               proper_key = dk[k]
                    #               return {proper_key: v}
                    #           return {k: v}
                    #
                    # На выходе собираешь словари в список:
                    #   results = []
                    #   for i in items:
                    #       results.append(
                    #           {**get_entry(divergent_keys, i)}
                    #       )
                    # Это примерная логика, не учитывающая специальных кейсов, просто чтобы ты понял направление ветра
                    # А еще лучше будет сделать dataclasses. Погугли

                    item = json.loads(i)['payload']['data'] 
                    title = item.get('title')
                    #print(title) 
                    category = item.get('category').get('title')
                    attributes = item.get('attributes')
                    item_id = item.get('id')
                    amount_sells = item.get('ordersAmount')
                    rating = item.get('rating')
                    reviewsQuantity = item.get('reviewsAmount')
                    is_eco = item.get('isEco') 
                    description = item.get('description')
                    photos = item.get('photos')
                    photo_links = []
                    seller = item.get('seller').get('title')
                    for photo_item in photos: 
                        for k,photo_sub in photo_item.items():
                            if type(photo_sub) == dict:
                                photo_links.append(photo_sub.get(max(photo_sub.keys()))) # Добавляет фото криво нужно исправить
                    sub_items = json.loads(i)['payload']['data']['skuList']
                    for sub_item in sub_items:  
                        Id = sub_item.get('id')
                        price = str(sub_item.get('purchasePrice'))
                        avaiable_amount = sub_item.get('availableAmount')

                        # Здесь как раз тебе бы пригодился словарь соответствий, о котором я написал выше
                        # И да, абсолютно нечитабельный код. Тебе нужно разбивать длинные конструкции на абзацы с идентацией
                        # Скачай себе pycharm или vscode. Они это автоматом делают
                        j = {'title':title,'itemid':item_id,'amount_sells':amount_sells,'rating':rating,
                                    'reviews':reviewsQuantity,'is_eco':is_eco,'Id':Id,'avaiable_amount':avaiable_amount,'price':price,
                                    'description':description,'attributes':attributes,'category':category,'seller':seller, 'date':str(datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')),}
                        cur.execute("""INSERT INTO products(id,название,продажи,рейтинг,отзывы,категория,is_eco,производитель,наличие,описание,атрибуты,цена,время_парсинга)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",(j.get('Id'),j.get('title'),j.get('amount_sells'),j.get('rating'),j.get('reviews'),j.get('category'),j.get('is_eco'),
                                            j.get('seller'),j.get('avaiable_amount'),j.get('description'),str(j.get('attributes')),j.get('price'),j.get('date')))
                        conn.commit()
                        

                        '''
                        data.append({
                                    'title':title,'itemid':item_id,'amount_sells':amount_sells,'rating':rating,
                                    'reviews':reviewsQuantity,'is_eco':is_eco,'id':Id,'avaiable_amount':avaiable_amount,'price':price,
                                    'description':description,'attributes':attributes,'category':category,'seller':seller, 'date':str(datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')),
                                    })
                        '''
                # Нехорошо. Если ты захочешь остановить программу через ctrl+c в терминале
                # То из-за этой строки эксепшн KeyboardInterrupt не приведет к завершению программы
                # Как и впрочем SIGTERM сигнал может не сработать. и в итоге у тебя бесконечный цикл, который не завершается
                # может так в памяти и застрять. Опять же, возможно так не будет, потому что питон написан умными людьми,
                # которые не дадут тебе выстрелить в ногу.
                except Exception as ex:
                    pass
               


async def start_itr(number_of_r_for_itr,number_of_itr,left_itr):
    # Вообще за логикой этой функции проследить крайне трудно. из-за обилия того,
    # что с чем сравнивается в такой простой вроде бы задаче, совершенно непонятно, что происходит
    # и надо сидеть ковыряться
    try:
        global amount_r
        for j in range(number_of_itr):
            async for i in find_product_by_id(number_of_r_for_itr):
                row_data.append(i)
                if amount_r > number_of_r_for_itr * number_of_itr:
                    # Если это выход из программы, то можно оставить просто пустой return или return None
                    return True
            left_itr = left_itr - 1
            global data
            print("\n" * 20)
            print('запросов сделанно:',amount_r )
            print('Осталось итераций:',left_itr)
            # time.sleep(2) блокирует всю программу и не дает выполняться асинхронно.
            # для асинхронных функций нужно использовать asyncio.sleep()
            time.sleep(2)
        
    except Exception as ex:
        # У этого блока своя область видимости и по идее тут тоже надо объявлять `global amount_r`
        print(f'Exeption is :{ex}')
        amount_r = amount_r - number_of_r_for_itr
        print('сон на минутку')
        time.sleep(10)

        # Функции можно дать аргументы по умолчанию и перезапустить ее еще раз вместо того, чтобы сбрасывать какие-то переменные
        await start_itr(number_of_r_for_itr,number_of_itr,left_itr)





'''
def insert_into_db():
    'Вставляет данные в базу данных'
    for i in data:
        cur.execute("""INSERT INTO products(id,название,продажи,рейтинг,отзывы,категория,is_eco,производитель,наличие,описание,атрибуты,цена,время_парсинга)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",(i.get('id'),i.get('title'),i.get('amount_sells'),i.get('rating'),i.get('reviews'),i.get('category'),i.get('is_eco'),
                                            i.get('seller'),i.get('avaiable_amount'),i.get('description'),str(i.get('attributes')),i.get('price'),i.get('date')))
    conn.commit()
    print('Данные вставлены в базу данных')
    print('Количество продуктов: ',len(data))    

'''

async def main():
    # последовательная активация функций
    N  = 100 # Количество запросов подряд
    itr = 20000 # Количество итераций
    parse_thread = threading.Thread(target= parse_product)  # Поток для обработки данных
    parse_thread.start() # Запускаем поток
    start_timestamp = time.time() # Время начала парсинга
    await start_itr(N,itr,itr) # Запускаем парсер
    task_time = round(time.time() - start_timestamp, 2) # Время выполнения парсинга
    rps = round(N / task_time, 1) # Количество запросов в секунду
    print(
        f"| Requests: {N * itr}; Total time: {task_time} s; RPS: {rps}. |\n" 
    )
    #insert_into_db()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()  
    loop.run_until_complete(main())
    loop.close()