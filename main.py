# -*- coding: utf-8 -*-
# https://github.com/KazakovDenis
import certifi
from datetime import datetime
# from multiprocessing import Pool
from random import uniform, choice
from time import sleep
from scraping import *
from data import *
from parts import *


# TODO:
# добавил брут-форс артикулов запчастей, запись логов, отлов ускользнувших исключений

# глобально объявляем данные, необходимые для избежания бана
global useragent_list, proxy_list

# получаем генератор списка юзер-агентов из файла
with open("in/useragents.txt", "r") as f:
    useragent_list = f.read().split('\n')

# получаем генератор списка прокси из файла (прокси проверять отдельно!)
with open("in/proxylist.txt", "r") as f:
    proxy_list = f.read().split('\n')


def write_log(msg):
    with open('out/log.txt', 'a') as log:
        log.write(msg + '\n' + '-----------' + '\n')


def write_not_parsed(url):
    """ Записываем страницы, которые по какой-то причине пришлось пропустить """
    with open('out/not_parsed.txt', 'a', newline='') as f:
        f.write(url + '\n')


def get_response(url, useragent={'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0'},
                 proxy={'http': 'http://41.89.171.220:8080', 'https': 'http://41.89.171.220:8080'}):
    """ Получаем ответ страницы по урл с паузой. Ожидаем <Response [200]>"""
    print('\nConnecting to', url)
    timeout = uniform(30, 40)
    try:
        response = requests.get(url, headers=useragent, proxies=proxy, timeout=timeout, verify=certifi.where())
        # делаем паузу
        pause = uniform(3, 6)
        sleep(pause)
        return response
    # в случае бана пробуем снова с бОльшей паузой
    except requests.exceptions.RequestException as e:
        print(f'get_response error: {url}.')
        write_log(f'get_response error at {url}: {e} ')
        useragent = {'User-Agent': choice(useragent_list)}
        proxy = {'http': choice(proxy_list), 'https': choice(proxy_list)}
        write_not_parsed(url)    # записываем страницу с ошибкой
        pause = uniform(10, 15)
        sleep(pause)
        get_response(url, useragent=useragent, proxy=proxy)
    except Exception as e:
        # непредусмотренные ошибки
        write_not_parsed(url)
        write_log(f'Another get_response error at {url}: {e} ')


def time_decorator(func):
    """ Декоратор считает время выполнения всего скрипта """
    def wrapper():
        # начинаем отсчёт времени работы модуля
        start = datetime.now()
        print(f'Parsing started at {start}')
        func()
        # считаем и выводим затраченное время
        period = datetime.now() - start
        print('The End. Time wasted: ', str(period))
        write_log(f'The End. Time wasted: {str(period)}')
    return wrapper


def parse_page(url, category):
    try:
        if not url:
            return
        # извлекаем всю информацию о товаре
        product = extract_product_info(url)  # product[0] = product_id
        # если запись уже существует, пропускаем
        query = f'SELECT Код FROM Berg WHERE Код = {product[0]}'
        if execute_sql(query):
            return
        # добавляем значения в таблицу
        product = (category,) + product
        values = convert_to_string(*product)
        query = f'INSERT INTO Berg VALUES ({values})'
        execute_sql(query)
    except Exception as e:
        write_log(f'parse_page error at {url}: {e}')


def parse_category(link, category):
    response = get_response(link)
    # получаем количество страниц в категории / подкатегории
    try:
        last_page = get_last_page(response)
    except:
        last_page = 1

    # парсим категорию постранично
    current_page = 1
    while current_page <= last_page:
        # если в категории всего 1 страница, лишнего запроса не делаем
        if current_page != 1:
            url_constructor = [link, '&page=', str(current_page)]
            next_url = ''.join(url_constructor)
            response = get_response(next_url)

        # получаем ссылки на страницы товаров
        product_links = get_product_links(response)
        # парсим страницы товаров
        [parse_page(url, category) for url in product_links]
        current_page += 1


@time_decorator
def main():
    # создаём таблицу в БД
    query = f"""CREATE TABLE IF NOT EXISTS Berg
            (Категория TEXT, Код INTEGER PRIMARY KEY, Артикул TEXT, Бренд TEXT, Наименование TEXT, 
            Изображения TEXT, Упаковка TEXT, Описание TEXT, Замены TEXT, OEM TEXT)"""
    execute_sql(query)
    # вход
    url = 'https://berg.ru/products'
    response = get_response(url)
    # получаем ссылки на категории товаров
    category_links = get_category_links(response)
    category_counter = 0

    # заходим поочерёдно в каждую категорию
    for link in category_links:
        try:
            response = get_response(link)
            soup = BeautifulSoup(response.text, 'lxml')
            category = soup.find('title').text.strip().split(' / ')[-2]  # название категории
        except:
            continue

        # проверяем категорию на наличие подкатегорий
        subcategories = look_for_sub(soup)
        if subcategories:   # парсим найденные подкатегории
            [parse_category(sub_link, category) for sub_link in subcategories]
        else:               # парсим категорию
            parse_category(link, category)
        category_counter += 1
        # записываем в лог
        write_log(f'{category_counter}. {category} parsed.')
    else:
        # первый этап парсинга окончен
        write_log('CONGRATULATIONS! Accessories has been parsed. Lets try to parse parts!')

    # # парсим запасные части и материалы для ТО
    # while True:
    #     url = next(get_parts_url())
    #     category = 'Запасные частии и расходники'
    #     if not url:
    #         break
    #     parse_page(url, category)
    #
    # # второй этап парсинга окончен
    # write_log('CONGRATULATIONS! Parts has been parsed.  Lets try to parse pages we could not parse earlier!')
    #
    # # повторно парсим страницы, на которых возникали ошибки
    # with open('out/not_parsed.txt', 'r') as f:
    #     while True:
    #         url = f.readline().strip()
    #         category = ''
    #         if not url:
    #             break
    #         parse_page(url, category)


if __name__ == '__main__':
    main()
