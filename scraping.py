# -*- coding: utf-8 -*-
# https://github.com/KazakovDenis
import requests
import lxml
from bs4 import BeautifulSoup
from main import write_not_parsed
from main import get_response
from data import execute_sql
from main import write_log


def get_category_links(response):
    """ Находим на странице входа ссылки на категории (кроме запчастей). Ожидаем список со ссылками """
    links = []
    try:
        soup = BeautifulSoup(response.text, 'lxml')
        # получаем все теги <ul> на странице с указанным классом
        uls = soup.find_all('ul', class_='catalog__list')
        for ul in uls:
            # получаем все теги <li> внутри каждого <ul>
            lis = ul.find_all('li', class_='item')
            for li in lis[1:]:
                # получаем атрибут href тега <a> внутри каждого <li>
                a = li.find('a').get('href')
                link = 'https://berg.ru' + a + '?products-view-type=short&offers-view-type=short'
                links.append(link)
    except:
        write_log('get_category_links error: None argument')
    return links


def get_product_links(response):
    """ Ищет ссылки на товары. Возвращает список со ссылками или пустой список """
    links = []
    # получаем все теги <a> на странице с указанным классом
    try:
        soup = BeautifulSoup(response.text, 'lxml')
        tags_a = soup.find_all('a', class_='part_description__link')
        for a in tags_a:
            link = a.get('href')
            links.append(link)
        links = list(map(lambda x: 'https://berg.ru' + x, links))
    except:
        return links
    return links


def look_for_sub(soup):
    try:
        lis = soup.find('li', class_='active').find('ul').find_all('li')
        links = ['https://berg.ru' + li.find('a').get('href') for li in lis]
        return links
    except:
        return


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


def extract_product_info(url):
    """ Extracts all information about a product in a tuple """
    try:
        product_id = url.split('/')[-1]
        html = get_response(url).text
        soup = BeautifulSoup(html, 'lxml')
    except Exception as e:
        print('Exception:', e, '\n----------------------')
        write_not_parsed(url)
        return

    # извлекаем изображения
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
    images = set(img_list)

    # находим блоки с информацией о товаре
    div_info = soup.find('div', class_='additional_info').find_all('div', class_='group')
    for div in div_info:
        if 'Общая информация' in str(div):
            general_info_div = div
        elif 'Критерии' in str(div):
            description_div = div
        elif 'Замены' in str(div):
            replacement_div = div
        elif 'Конструкционные номера' in str(div):
            oem_div = div

    # извлекаем основную информацию о товаре
    try:
        general_info_rows = general_info_div.find_all('div', class_='row')[:3]
        articul = general_info_rows[0].find('div', class_='value_col').text.strip()
        brand = general_info_rows[1].find('div', class_='value_col').text.strip()
        title = general_info_rows[2].find('div', class_='value_col').text.strip()
    except:
        write_not_parsed(url)
        return

    # извлекаем данные об упаковке
    try:
        package = []
        package_rows = general_info_div.find_all('div', class_='row')[3:]

        for row in package_rows:
            key = row.find('div', class_='title_col').text.strip()
            value = row.find('div', class_='value_col').text.strip()
            pairs = ' - '.join((key, value))
            package.append(pairs)

        package = ', '.join(package[:])
    except:
        package = ''

    # извлекаем описание (из блока критерии)
    try:
        description = []
        description_rows = description_div.find_all('div', class_='row')
        for row in description_rows:
            key = row.find('div', class_='title_col').text.strip()
            value = row.find('div', class_='value_col').text.strip()
            pairs = ' - '.join((key, value))
            description.append(pairs)
        description = ', '.join(description[:])
    except:
        description = ''

    # извлекаем замены
    try:
        replacement = []
        replacement_rows = replacement_div.find_all('div', class_='row')
        for row in replacement_rows:
            key = row.find('div', class_='title_col').text.strip()
            value = row.find('div', class_='value_col').text.strip()
            # пишем в кортеж, добавляем в список, т.к. могут повторяться марки машин
            pairs = ' - '.join((key, value))
            replacement.append(pairs)
    except:
        replacement = ''

    # извлекаем конструкционные номера
    try:
        oem = []
        oem_rows = oem_div.find_all('div', class_='row')
        for row in oem_rows:
            key = row.find('div', class_='title_col').text.strip()
            value = row.find('div', class_='value_col').text.strip()
            # пишем в кортеж, добавляем в список, т.к. могут повторяться марки машин
            pairs = ' - '.join((key, value))
            oem.append(pairs)
    except:
        oem = ''

    product = (product_id, articul, brand, title, images, package, description, replacement, oem)
    return product
