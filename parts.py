# -*- coding: utf-8 -*-
# https://github.com/KazakovDenis

""" Брут-форс: пишем в txt все id товаров, не совпадающие с имеющимися в базе, для чего:
 - берём диапазон от 1 до 999 999 999
 - берём товар с минимальным id
 - запускаем запись всех id по порядку
 - по достижении id, пропускаем этот номер и присваиваем диапазону следующую границу в виде следующего id """
# from data import execute_sql
# from main import get_response


def get_ids():
    """ Получаем id всех товаров из базы данных, выдаём поштучно """
    try:
        query = f'SELECT Код FROM Berg'
        id_list = [int(ids[0]) for ids in execute_sql(query)]
        id_list.sort()
        for i in id_list:
            yield i
    except StopIteration:
        return


def filter_and_write(ids):
    """ Пишем все возможные значения за исключением тех id, что были в базе """
    next_id = next(ids)
    with open('out/entire_ids.txt', 'w') as f:
        for unparsed in range(2*(10**8)):
            if unparsed != next_id:
                f.write(str(unparsed) + '\n')
            else:
                try:
                    next_id = next(ids)
                except:
                    continue


def check_and_write():
    """ Проверяем на отклик страницы и предполгаемыми id """
    try:
        with open('out/entire_ids.txt', 'r') as f:
            while True:
                article = f.readline().strip()
                if not article:
                    break
                url = 'https://berg.ru/article/' + article
                if get_response(url).status_code == '200':
                    with open('out/parts_urls.txt', 'a') as g:
                        g.write(url + '\n')
    except StopIteration:
        return


def collect_parts_urls():
    """ На выходе получаем список готовых к парсингу url запчастей в parts_urls.txt """
    ids = get_ids()
    filter_and_write(ids)
    check_and_write()


def get_parts_url():
    """" Поштучно выдаём url """
    try:
        with open('out/parts_urls.txt', 'r') as f:
            while True:
                url = f.readline().strip()
                if not url:
                    break
            yield url
    except StopIteration:
        return
