import sqlite3
# from main import write_log


def execute_sql(query, params=()):
    """ Выполняет sql-запрос и возвращает запрошенные данные """
    try:
        # Подключаемся к БД (если её нет - создаём), выполняем запрос и подтверждаем изменения
        with sqlite3.connect('out/berg') as cnxn:
            data = []
            cursor = cnxn.cursor()
            cursor.execute(query, params)
            data = cursor.fetchall()
            cnxn.commit()
    except sqlite3.Error as e:
        print('Ошибка БД: ' + str(e))
        write_log(f'execute_sql error: {query}')
    return data


def convert_to_string(*args, value=True):
    """ Подготавливает данные к подстановке в sql-запрос """
    args = list(args)
    for i in range(len(args)):
        if type(args[i]) == list or type(args[i]) == tuple or type(args[i]) == set:
            args[i] = ', '.join(args[i])
    string = ', '.join([f'"{str(arg)}"' if value else str(arg) for arg in args])
    return string
