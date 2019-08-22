import sqlite3


def execute_sql(query, params=()):
    print('Подключение к базе...')
    try:
        # Подключаемся к БД (если её нет - создаём), выполняем запрос и подтверждаем изменения
        with sqlite3.connect('products') as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(query, params)
            cnxn.commit()
            print('Запрос выполнен')
    except sqlite3.Error as e:
        print('Ошибка БД: ' + str(e))


query = 'CREATE TABLE IF NOT EXISTS goods (id INTEGER PRIMARY KEY, articul TEXT, title TEXT, brand TEXT, images TEXT)'
execute_sql(query)

columns = ('id', 'articul', 'title', 'brand', 'image')
joined_columns = ', '.join(columns)

product_id = 1772314
articul = 'T51565'
title = 'Акк'
brand = 'Барс 60 Ач Обр.'
image = ['http://berg.ru/1267.jpg', 'http://berg.ru/973.jpg']

values = (str(product_id), articul, title, brand, ' '.join(image))

query = f'INSERT INTO goods ({joined_columns}) VALUES {values}'
print(query)
execute_sql(query)

