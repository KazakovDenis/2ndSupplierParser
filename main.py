# -*- coding: utf-8 -*-
# https://github.com/KazakovDenis
import requests
from bs4 import BeautifulSoup
import lxml
import csv
import certifi
import numba
from datetime import datetime
# from multiprocessing import Pool
from random import uniform, choice
from time import sleep
import data as db

# добавил выгрузку превью, убрал повторы фото
# исправил перезапись ОЕМ при повторении ключей в словаре (заменил на список)

# глобально объявляем данные, необходимые для избежания бана
global useragent_list, proxy_list

# получаем генератор списка юзер-агентов из файла
with open("useragents.txt", "r") as f:
    useragent_list = f.read().split('\n')

# получаем генератор списка прокси из файла (прокси проверять отдельно!)
with open("proxylist.txt", "r") as f:
    proxy_list = f.read().split('\n')


def extract_product_info(url):
    product_id = url.split('/')[-1]
    product_info = []
    product = {product_id: product_info}
    try:
        html = get_response(url).text
    except Exception as e:
        print('Exception:', e, '\n----------------------')

    soup = BeautifulSoup(html, 'lxml')
    img_list = []

    try:
        # извлекаем главное фото
        preview_photo = 'https:' + soup.find('div', class_='preview__item').find('a').get('href')
        img_list.append(preview_photo)
        # излекаем остальные фото
        lis = soup.find('div', class_='photo_gallery').find('ul').find_all('li')
        for li in lis:
            src = li.find('img').get('data-src')
            img_url = 'https:' + src
            img_list.append(img_url)
    except:
        pass

    img_list = set(img_list)
    img = {'Изображения': img_list}
    product_info.append(img)

    div_info = soup.find('div', class_='additional_info').find_all('div', class_='group')
    general_info_div, criterion_div, replacement_div, OEMs_div = False, False, False, False

    for div in div_info:
        if 'Общая информация' in str(div):
            general_info_div = div
        elif 'Критерии' in str(div):
            criterion_div = div
        elif 'Замены' in str(div):
            replacement_div = div
        elif 'Конструкционные номера' in str(div):
            OEMs_div = div

    if general_info_div:
        general_info = {}
        general_info_rows = general_info_div.find_all('div', class_='row')

        for row in general_info_rows:
            key = row.find('div', class_='title_col').text.strip()
            value = row.find('div', class_='value_col').text.strip()
            general_info[key] = value

        product_info.append(general_info)

    if criterion_div:
        criterion = {}
        criterion_rows = criterion_div.find_all('div', class_='row')

        for row in criterion_rows:
            key = row.find('div', class_='title_col').text.strip()
            value = row.find('div', class_='value_col').text.strip()
            criterion[key] = value

        product_info.append(criterion)

    # извлекаем замены
    if replacement_div:
        prereplacement = []
        replacement = {'Замены': prereplacement}
        replacement_rows = replacement_div.find_all('div', class_='row')

        for row in replacement_rows:
            key = row.find('div', class_='title_col').text.strip()
            value = row.find('div', class_='value_col').text.strip()
            # пишем в кортеж, добавляем в список, т.к. могут повторяться ключи
            pairs = ' - '.join((key, value))
            prereplacement.append(pairs)

        product_info.append(replacement)

    # извлекаем конструкционные номера
    if OEMs_div:
        PreOEMs = []
        OEMs = {'Конструкционные номера': PreOEMs}
        OEMs_rows = OEMs_div.find_all('div', class_='row')

        for row in OEMs_rows:
            key = row.find('div', class_='title_col').text.strip()
            value = row.find('div', class_='value_col').text.strip()
            # пишем в кортеж, добавляем в список, т.к. могут повторяться марки машин
            pairs = ' - '.join((key, value))
            PreOEMs.append(pairs)

        product_info.append(OEMs)

    return product


def write_not_parsed(url):
    """ Записываем необработанные страницы """
    with open('not_parsed.txt', 'a', newline='') as f:
        f.write(url + '\n')


def write_csv(dictionary):
    # """ Записываем извлечённые данные в csv """
    # product_values = list(dictionary.values())  # получаем значения словаря product, т.е. список product_info
    # fieldnames = []
    #
    # image_dict = product_values[0]  # получаем первый элемент списка, т.е. словарь с изображениями
    # image = list(image_dict.keys())[0]  # получаем первый ключ этого словаря
    # fieldnames.append(image)
    #
    # general_info_dict = product_values[1]  # получаем словарь с общей информацией
    # for key in list(general_info_dict.keys()):  # добавляем каждый ключ словаря в fieldnames
    #     fieldnames.append(key)
    #
    # criterion_dict = product_values[2]  # получаем словарь с параметрами товара
    # for key in list(criterion_dict.keys()):  # добавляем каждый ключ словаря в fieldnames
    #     fieldnames.append(key)
    #
    # replacement_dict = product_values[3]  # получаем словарь с заменами
    # replacement = list(replacement_dict.keys())[0]  # получаем первый ключ этого словаря
    # fieldnames.append(replacement)
    #
    # OEMs_dict = product_values[4]  # получаем словарь с конструкционными номерами
    # OEMs = list(OEMs_dict.keys())[0]  # получаем первый ключ этого словаря
    # fieldnames.append(OEMs)
    #
    # with open('berg.csv', 'a', newline='') as csv_file:
    #     writer = csv.DictWriter(csv_file, delimiter=';', fieldnames=fieldnames)
    #     writer.writeheader()
    #     for row in dictionary:
    #         writer.writerow(row)
    with open('berg.csv', 'a', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=';')
        writer.writerow(dictionary.items())


def get_response(url, useragent={'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0'},
                 proxy={'http': 'http://51.15.193.253:3128', 'https': 'http://51.15.193.253:3128'}):
    """ Получаем ответ страницы по урл с паузой. Ожидаем <Response [200]>"""
    print('\nTrying to parse', url)
    timeout = uniform(30, 40)
    try:
        response = requests.get(url, headers=useragent, proxies=proxy, timeout=timeout, verify=certifi.where())
        # делаем паузу
        pause = uniform(3, 6)
        sleep(pause)
        return response
    # в случае бана пробуем снова с бОльшей паузой
    except requests.exceptions.RequestException as e:
        print(f'Error for {url}. More info:', e)
        useragent = {'User-Agent': choice(useragent_list)}
        proxy = {'http': choice(proxy_list), 'https': choice(proxy_list)}
        write_not_parsed(url)    # записываем страницу с ошибкой
        pause = uniform(10, 15)
        sleep(pause)
        get_response(url, useragent=useragent, proxy=proxy)
    except Exception as e:
        # непредусмотренные ошибки
        write_not_parsed(url)
        print(f'Another Error for {url} in get_response():', e)


def get_category_links(response):
    """ Находим на странице входа ссылки на категории (кроме запчастей). Ожидаем список со ссылками """
    soup = BeautifulSoup(response.text, 'lxml')
    links = []
    # получаем все теги <ul> на странице с указанным классом
    uls = soup.find_all('ul', class_='catalog__list')
    for ul in uls:
        # получаем все теги <li> внутри каждого <ul>
        lis = soup.find_all('li', class_='item')
        for li in lis[1:]:
            # получаем атрибут href тега <a> внутри каждого <li>
            a = li.find('a').get('href')
            link = 'https://berg.ru' + a + '?products-view-type=short&offers-view-type=short'
            links.append(link)

    return links


def get_product_links(response):
    """ Ищет ссылки на товары. Возвращает список со ссылками или пустой список """
    soup = BeautifulSoup(response.text, 'lxml')
    links = []
    # получаем все теги <a> на странице с указанным классом
    try:
        tags_a = soup.find_all('a', class_='part_description__link')
        for a in tags_a:
            link = a.get('href')
            links.append(link)
    except:
        print(f'No links at page: {response.url}')
    links = list(map(lambda x: 'https://berg.ru' + x, links))
    return links


def get_last_page(response):
    """ Находит количество страниц в категории. Возвращает int или None """
    soup = BeautifulSoup(response.text, 'lxml')
    try:
        pagination = soup.find('div', class_='paginator').find('ul')
        last_page = int(pagination.find_all('li')[-1].find('a').text.strip())
        print(f'Страниц в текущей категории:', last_page)
    except:
        print(f'В текущей категории 1 страница')
    return last_page


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
    return wrapper


@time_decorator
# @jit(parallel=True)
def main():
    # вход
    url = 'https://berg.ru/products'
    response = get_response(url)
    # получаем ссылки на категории товаров
    category_links = get_category_links(response)

    query = f"""
            CREATE TABLE IF NOT EXISTS goods (
            id INTEGER,
            articul TEXT,
            title TEXT,
            brand TEXT,
            image TEXT );

            COMMIT;"""
    db.write_to_db(query)

    # заходим в каждую категорию
    for link in category_links[:1]:
        response = get_response(link)

        # получаем количество страниц в категории
        try:
            last_page = get_last_page(response)
        except:
            last_page = 1

        last_page = 2
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
            for url in product_links[:1]:
                product = extract_product_info(url)
                print(product)

                product_id = list(product.keys())[0]
                articul = product[product_id][1]['Артикул']
                title = product[product_id][1]['Наименование']
                brand = product[product_id][1]['Бренд']
                image = product[product_id][0]['Изображения']

                query = f"""
                INSERT INTO goods (id, articul, title, brand, image)  
                VALUES({product_id}, {articul}, {title}, {brand}, {image});
                COMMIT;
                """
                db.write_to_db(query)

                # пишем результат парсинга в csv
                # write_csv(product)

                # ------------------------------ для теста
                # product_id = list(product.keys())[0]
                # print('ID товара: ', product_id)
                # for row in product[product_id]:
                #     for value in row.items():
                #         print(f'{value[0]}: {value[1]}')
                # ------------------------------ конец теста
            current_page += 1


if __name__ == '__main__':
    main()

