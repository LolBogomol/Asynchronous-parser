import asyncio
import aiohttp
import json
import os
import time # 
import sqlite3
from datetime import date
import threading
import datetime



row_data = [] #Сырая дата которая попадает в поток и обрабатывается 
amount_r = 0 # Нужно для отслежинвания количества запросов 
global left_itr
left_itr = 20000 # Количество оставшихся итераций 
#today = date.today(today.strftime("%d/%m/%Y - %H:%M"))

async def find_product_by_id(number_of_requests):
    '''Получает данные о продуктах по идентификатору'''
    global amount_r 
    headers = {
            'Host': 'api.kazanexpress.ru',
            'Sec-Ch-Ua': '"Chromium";v="103", ".Not/A)Brand";v="99"',
            'Accept-Language': 'ru-RU',
            'Sec-Ch-Ua-Mobile': '?0',
            'Authorization': 'Basic a2F6YW5leHByZXNzLWN1c3RvbWVyOmN1c3RvbWVyU2VjcmV0S2V5',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
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
        tasks = [] #собирает все запросы т
        for i in range(number_of_requests):
            tasks.append(asyncio.create_task(session.get(url = f'https://api.kazanexpress.ru/api/v2/product/{amount_r}',headers = headers))) # Добавления новой задачи
            amount_r = amount_r + 1
        # Асиннхронная активация всех собранных задач
        responses = await asyncio.gather(*tasks)
        yield [await r.text(encoding='UTF-8') for r in responses] # возвращяет один ответ за раз





#запрос 3000


def parse_product():
    conn = sqlite3.connect(str(datetime.datetime.now().strftime('%Y-%m-%d-%S'))+'.db')
    cur = conn.cursor()
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

    '''Cам парсер, он проеверет row_data на наличие инфы и в случае если она есть то обрабатывает её
      работет в втором потоке.
      А ну ещё говнокодом заполняет базу
     '''
    make_data_base()
    global data
    while True:
        if len(row_data) > 0:
            items = row_data.pop(0) 
            for i in items:
                try:
                    item = json.loads(i)['payload']['data'] 
                    title = item.get('title')
                    #print(title) 
                    category = item.get('category').get('title')
                    attributes = item.get('attributes')
                    item_id = item.get('id')
                    amount_sells = item.get('ordersAmount')#Общие продажи карточки 
                    rating = item.get('rating')# рейтинг 
                    reviewsQuantity = item.get('reviewsAmount')# количество отзывов
                    is_eco = item.get('isEco') 
                    description = item.get('description') # описание
                    photos = item.get('photos') # список фотографий
                    photo_links = []
                    seller = item.get('seller').get('title') # продавец
                    for photo_item in photos: 
                        for k,photo_sub in photo_item.items():
                            if type(photo_sub) == dict:
                                photo_links.append(photo_sub.get(max(photo_sub.keys()))) # Добавляет фото криво нужно исправить
                    sub_items = json.loads(i)['payload']['data']['skuList']
                    for sub_item in sub_items:  
                        Id = sub_item.get('id')
                        price = str(sub_item.get('purchasePrice'))
                        avaiable_amount = sub_item.get('availableAmount')
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
                except Exception as ex:
                    pass
               


async def start_itr(number_of_r_for_itr,number_of_itr,left_itr):
    '''Запускает парсер и обработку данных'''
    try:
        global amount_r
        for j in range(number_of_itr):
            async for i in find_product_by_id(number_of_r_for_itr):
                row_data.append(i)
                if amount_r > number_of_r_for_itr * number_of_itr:
                    return True
            left_itr = left_itr - 1
            global data
            print("\n" * 20)
            print('запросов сделанно:',amount_r )
            print('Осталось итераций:',left_itr)
            time.sleep(2)
        
    except Exception as ex:
        print(f'Exeption is :{ex}')
        amount_r = amount_r - number_of_r_for_itr # возвращает на предыдущую итерацию
        print('сон на минутку')
        time.sleep(10)
        await start_itr(number_of_r_for_itr,number_of_itr,left_itr) # пробуем еще раз





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