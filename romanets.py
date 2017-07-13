import requests
import json
import random
import string
import sys
import sqlite3
import itertools
import pytz
import time
from datetime import datetime, timedelta
from dateutil import parser as parser
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

API_URL = 'https://api.intra.42.fr/v2'
API_REQUEST_TOKEN_URL = 'https://api.intra.42.fr/oauth/token'

uid = os.environ.get("UID")
secret = os.environ.get("SECTET")
name_bd = "cheater.db"

class IntraAPI:

    def __init__(self, client_id, client_secret):
        self.__client_id         = client_id
        self.__client_secret     = client_secret
        self.__client            = None
        self._init_app()
        self._set_OAuth_session()
        self._get_token()

    def _init_app(self):
        self.__app = BackendApplicationClient(client_id=self.__client_id)

    def _set_OAuth_session(self):
        self.__oauth = OAuth2Session(client=self.__app)

    def _get_token(self):
        self.__token = self.__oauth.fetch_token(
                    token_url=API_REQUEST_TOKEN_URL,
                    client_id=self.__client_id,
                    client_secret=self.__client_secret
                )

    def get_client(self):
        if self.__client is None:
            self.__client = OAuth2Session(
                    self.__client_id,
                    token=self.__token
                    )
        return self.__client

""" Клас для роботи з таблицею юзерів в БД. Для роботи потрібно в конструктор надіслали назву БД. Зв'язок з БД встановлюється при створенні об'єкта. """
class DBUsers:

	def __init__(self, db_name):
		self.con = sqlite3.connect(db_name, timeout=15)
		self.c = self.con.cursor()
		self.c.execute("CREATE TABLE IF NOT EXISTS users(id INT, login TEXT)")
		self.con.commit()

	def __del__(self):
		self.con.commit()
		self.con.close()

	""" Метод для запису студентів в БД (його id та login) """
	def put_data(self, id, login):
		self.c.execute("INSERT INTO users (id, login) VALUES (?,?)", (id, login))
		self.con.commit()

	""" Метод для отримання id студента з БД по його логіну. Якщо такого студента немає, то повернеться None. """
	def get_id(self, name):
		self.c.execute("SELECT id FROM users WHERE login = ?", (name,))
		res = self.c.fetchone()
		if res:
			return res[0]
		return None

	""" Метод для отримання логіна студента по його id. Якщо такого студента немає, то повернеться None. """
	def get_login(self, id):
		self.c.execute("SELECT login FROM users WHERE id = ?", (id,))
		res = self.c.fetchone()
		if res:
			return res[0]
		return None

	""" Метод для отрмання всіх студентів по нашому кампусу, які є в БД """
	def get_all_users(self):
		self.c.execute("SELECT id FROM users")
		all_users = self.c.fetchall()
		return all_users


""" Функції, яка отримує всіх студентів по нашому кампусу і фільтрує по даті активності (якщо він був хоч раз залогінений після 1 березня 2017 (date_check), то він активний). """
def put_fresh_user_to_BD(client, name_bd):
	db = DBUsers(name_bd)
	date_check = datetime(2017, 3, 1, tzinfo=pytz.utc)
	for page in itertools.count(1):
		user_data = client.get("https://api.intra.42.fr/v2/campus/8/users?page[number]=" + str(page) + "&page[size]=100").json()
		if not user_data:
			break
		for data in user_data:
			location_data = client.get("https://api.intra.42.fr/v2/campus/8/locations?filter[user_id]=" + str(data['id'])).json()
			if (location_data):
				date_user = parser.parse(location_data[0]['begin_at'])
				if (date_user > date_check):
					check = db.get_id(data['login'])
					if not check:
						db.put_data(data['id'], data['login'])

""" Клас для роботи з таблицею scales (перевірок) в БД. Для роботи потрібно в конструктор надіслали назву БД. Зв'язок з БД встановлюється при створенні об'єкта. """
class DBScale:

	def __init__(self, db_name):
		self.con = sqlite3.connect(db_name, timeout=15)
		self.c = self.con.cursor()
		self.c.execute("CREATE TABLE IF NOT EXISTS scales(scale_id INT, corrector_id INT, correcteds_id TEXT, begin_at DATE, updated_at DATE, finish INT, is_checked INT)")
		self.con.commit()

	def __del__(self):
		self.con.commit()
		self.con.close()

	""" Метод для запису данних в БД. Записується id перевірки (scale_id), id перевіряючого (corrector_id), id перевіряючих (correcteds_id, їх може бути декілька, пишуться id через кому), формальна дата перевірки (begin_at),
фактична дата перевірки (updated_at), чи закінчена перевірка (updated_at) та чи перевірено було цю перевірку нашим скриптом (is_checked, по дефолту 0, змінюється в іншій частині програми) """
	def put_data(self, scale_id, corrector_id, correcteds_id, begin_at, updated_at, finish):
		self.c.execute("INSERT INTO scales (scale_id, corrector_id, correcteds_id, begin_at, updated_at, finish, is_checked) VALUES (?,?,?,?,?,?,?)", (scale_id, corrector_id, correcteds_id, begin_at, updated_at, finish, 0))
		self.con.commit()

	""" Метод для отримання даних про перевірку по її id з БД. Якщо її немає в БД, то повернеться None """
	def get_scale(self, scale_id):
		self.c.execute("SELECT * FROM scales WHERE scale_id = ?", (scale_id,))
		scale = self.c.fetchall()
		if (scale):
			return (scale[0])
		return None

	""" Метод для оновлення інфо в БД після проставлення оцінки. """
	def update_scale(self, scale_id, updated_at, finish):
		self.c.execute("UPDATE scales SET updated_at = ?, finish = ? WHERE scale_id = ?", (updated_at, finish, scale_id,))
		self.con.commit()

	def del_scale(self, scale_id):
		self.c.execute("DELETE FROM scales WHERE scale_id = ?", (scale_id,))
		self.con.commit()

""" Функція яка отримує всі перевірки за останні 30 хв, знаходить збіг по студентам з нашого кампусу і записує це в БД.
Якщо запис існує, то оновлює значення updated_at після отримання оцінки (після першого оновлення більше не змінюється для того, щоб зафіксувати саме час виставлення оцінки). """
def scales(client, name_bd):
	add_scale = DBScale(name_bd)
	users = DBUsers(name_bd)
	for page in itertools.count(1):
		scales = client.get("https://api.intra.42.fr/v2/scale_teams?page[number]=" + str(page) + "&page[size]=100&range[begin_at]=" + str(datetime.utcnow() - timedelta(minutes=30)) + "," + str(datetime.utcnow())).json()
		if not scales:
			break
		for scale in scales:
			user = users.get_login(scale['corrector']['id'])
			if user and not scale['truant']:
				correcteds = []
				for corrected in scale['correcteds']:
					correcteds.append(str(corrected['id']))
				finish = 1
				if(scale['final_mark'] is None):
					finish = 0
				check = add_scale.get_scale(scale['id'])
				if check and not bool(check[5]):
					add_scale.update_scale(scale['id'], parser.parse(scale['updated_at']), finish)
				elif not check:
					add_scale.put_data(scale['id'], scale['corrector']['id'], ','.join(correcteds), parser.parse(scale['begin_at']), parser.parse(scale['updated_at']), finish)
			if scale['truant']:
				add_scale.del_scale(scale['id'])

""" Клас для роботи з таблицею locations (часом коли студент був залогінений в кластері) в БД. Для роботи потрібно в конструктор надіслали назву БД. Зв'язок з БД встановлюється при створенні об'єкта. """
class DBLocation:

	def __init__(self, db_name):
		self.con = sqlite3.connect(db_name, timeout=15)
		self.c = self.con.cursor()
		self.c.execute("CREATE TABLE IF NOT EXISTS locations(id INT, location_id INT, begin_at date, end_at date)")
		self.con.commit()

	def __del__(self):
		self.con.commit()
		self.con.close()

	def get_location(self, id, location_id):
		self.c.execute("SELECT * FROM locations WHERE id = ? AND location_id = ?", (id, location_id,))
		location = self.c.fetchall()
		if location:
			return location[0]
		else:
			return None

	def put_location(self, id, location_id, begin_at, end_at):
		self.c.execute("INSERT INTO locations (id, location_id, begin_at, end_at) VALUES (?,?,?,?)", (id, location_id, begin_at, end_at,))
		self.con.commit()

	def update_location(self, location_id, end_at):
		self.c.execute("UPDATE locations SET end_at = ? WHERE location_id = ?", (end_at, location_id,))
		self.con.commit()

""" Функція яка отримує всі location по нашому кампусу за останній день. Якщо є нові - додає. Якщо вже існує, але немає end_at, то перевіряє і оновлює. """
def locations(client, name_bd):
	add_location = DBLocation(name_bd)
	for page in itertools.count(1):
		locations = client.get("https://api.intra.42.fr/v2/campus/8/locations?page[number]=" + str(page) + "&page[size]=100&range[begin_at]=" + str(datetime.utcnow() - timedelta(days=1)) + "," + str(datetime.utcnow())).json()
		if not locations:
			break
		for location in locations:
			check = add_location.get_location(location['user']['id'], location['id'])
			if check:
				if not check[3]:
					add_location.update_location(location['id'], location['end_at'])
			else:
				end = None
				if location['end_at']:
					end = parser.parse(location['end_at'])
				add_location.put_location(location['user']['id'], location['id'], parser.parse(location['begin_at']), end)

if __name__ == '__main__':
	print ("start")
	cl = IntraAPI(uid, secret)
	put_fresh_user_to_BD(cl.get_client(), name_bd)
	while True:
		try:
			client = cl.get_client()
			locations(client, name_bd)
			scales(client, name_bd)
			print (str(datetime.now()))
			time.sleep(900)
		except Exception as e:
			continue
	print ("Something went wrong")
