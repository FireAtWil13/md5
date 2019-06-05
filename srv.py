import flask
import requests
import json
import uuid
import sqlite3
import hashlib
from threading import Thread

app = flask.Flask(__name__)
conn = sqlite3.connect('mysqlite.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS requests (ID TEXT, URI TEXT, MD5 TEXT, EMAIL TEXT, RESPONSE_CODE INT, STATUS TEXT)''')
insert = '''INSERT INTO requests VALUES (?,?,?,?,?,?)'''
update = '''UPDATE requests SET MD5 = ?, RESPONSE_CODE = ?, STATUS = ? WHERE ID = ?'''
conn.commit()
conn.close()


def to_json(data):
    """Генерация JSON для ответа"""
    return json.dumps(data) + "\n"


def resp(code, data):
    """Формирование ответа"""
    return flask.Response(
        status=code,
        mimetype="application/json",
        response=to_json(data)
    )


def send_mail(uid):
    """Отправка письма"""
    pass


def md5(url, uid, conn, cur):
    """Расчет MD5 блоками, запуск отправки письма"""
    md5 = hashlib.md5()
    with requests.get(url, stream=True) as r:       # расчет хэш-функции пфайла по блокам
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                md5.update(chunk)
    cur.execute(update, (md5.hexdigest(), 200, 'done', uid))
    conn.commit()
    send_mail(uid)                                  # отпрвка письма


def task_check(url, uid, email='NULL'):
    """Проверка доступности файла, запуск расчета MD5 или сообщение об ошибке"""
    resp = requests.get(url).status_code
    conn = sqlite3.connect('mysqlite.db')
    cur = conn.cursor()
    if resp == 200:                                 # проверка что URL правильный и там есть файл
        cur.execute(insert, (uid, url, 'NULL', email, 202, 'running'))
        conn.commit()
        md5(url, uid, conn, cur)                    # запуск расчета хэша
    else:
        cur.execute(insert, (uid, url, 'NULL', email, 500, 'unsuccessfull'))
        conn.commit()
        conn.close()


@app.route('/submit', methods=['POST'])
def submit_file():
    """Обработка ПОСТ запроса"""
    id = str(uuid.uuid1())
    if 'url' in flask.request.form:                 # проверяем наличие URL в параметрах
        if 'email' in flask.request.form:
            t = Thread(target=task_check, args=(flask.request.form['url'], id, flask.request.form['email']))
            t.start()                               # запускаем поток по обрабоке файла, вернув ИД запроса
            return resp(200, {'id': id})
        else:
            t = Thread(target=task_check, args=(flask.request.form['url'], id))
            t.start()
            return resp(200, {'id': id})
    else:
        return resp(404, {'status': 'URL not found'})


@app.route('/check', methods=['GET'])
def check():
    """Обработка ГЕТ запроса"""
    if flask.request.args.get('id'):            # проверка на наличие параметра ИД
        conn = sqlite3.connect('mysqlite.db')
        cur = conn.cursor()
        cur.execute('''select * from requests where id = ? ''', (flask.request.args.get('id'),))
        rows = cur.fetchall()
        if len(rows):                           # если что-то с таким ИД присутствует в базе возвращаем результат
            if rows[0][4] == 200:               # если процедура завершена возвращаем хэш
                return resp(rows[0][4], {'md5': rows[0][2], 'status': rows[0][5], 'url': rows[0][1]})
            else:
                return resp(rows[0][4], {'status': rows[0][5]})
    return resp(404, {'status': 'not found'})



if __name__ == '__main__':
    app.debug = True  # enables auto reload during development
    app.run()