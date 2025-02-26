import psycopg2
import requests
import os
import time
import sys
import json
import csv
import re
import logging
from datetime import datetime, date, time, timedelta
from requests.exceptions import ConnectionError
from time import sleep
from transform_data_mod import transform
from glob import glob


# Настройка логирования
logger = logging.getLogger(__name__)
fileHandler = logging.FileHandler("logs/" + f"{date.today()}.log", mode="a", encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s | %(name)s | %(levelname)s: %(message)s",
    handlers=[fileHandler]
)

# Проверка файлов с логами и удаление логов, записанных более 3-х дней назад
files_log = [fl for fl in glob("logs/" + "*") if re.search(r"logs\\\d{4}-\d{2}-\d{2}\.log", fl)]

for fl in files_log:
    file_name = re.search(r"\d{4}-\d{2}-\d{2}", fl).group()
    if date.fromisoformat(file_name) <= date.today() - timedelta(days=2):
        os.remove(fl)


# Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
if sys.version_info < (3,):
    def u(x):
        try:
            return x.encode("utf8")
        except UnicodeDecodeError:
            return x
else:
    def u(x):
        if type(x) == type(b''):
            return x.decode('utf8')
        else:
            return x


class Extract:
    # Формирование заголовка HTTP запроса
    @classmethod
    def create_http_header(cls, ReportsURL, token, clientLogin):
        try:
            cls.ReportsURL = ReportsURL
            cls.headers = {
                # OAuth-токен
                "Authorization": "Bearer " + token,
                # Логин клиента
                "Client-Login": clientLogin,
                # Язык ответных сообщений
                "Accept-Language": "en",
                # Режим формирования отчета: online, offline или auto.
                "processingMode": "auto",
                # Формат денежных значений в отчете
                "returnMoneyInMicros": "false",
                # Не выводить в отчете строку с названием отчета и диапазоном дат
                "skipReportHeader": "true",
                # Не выводить в отчете строку с названиями полей.
                "skipColumnHeader": "false",
                # Не выводить в отчете строку с количеством строк статистики.
                "skipReportSummary": "true"
            }
        except TypeError as err:
            logger.critical(f"Указаны неверные значения: {err}")
            print(f"Указаны неверные значения: {err}")


    # Формирование тела запроса. По умолчанию статистика выгружается за 2 последних дня без учета текущего (date_start и date_end).
    @classmethod
    def create_http_body(cls, date_start=(date.today() - timedelta(days=2)).strftime("%Y-%m-%d"), date_end=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d")):
        try:
            # Указание диапазона дат для выгрузки данных из API Яндекс.Директ
            if re.match(r"\d{4}-\d{2}-\d{2}", date_start):
                cls.date_start = date_start
                cls.date_end = date_end

        except ValueError as err:
            logger.critical(f"Указаны некорректные значения дат: {err}")
            print(f"Указаны некорректные значения дат: {err}")

        else:
            # Создание тела запроса
            cls.body = {
                "params": {
                    # Критерии отбора данных для отчета. В нашем примере - по дате
                    "SelectionCriteria": {
                        "DateFrom": cls.date_start,
                        "DateTo": cls.date_end
                    },
                    # Указываем идентификаторы целей Метрики и модель атрибуции (установлена последний переход из Яндекс.Директ).
                    "Goals": ["<id>", "<id>", "<id>", "<id>", "<id>", "<id>"],
                    "AttributionModels": ["LYDC"],
                    # Необходимые поля в отчете
                        "FieldNames": [
                            "Date",
                            "CampaignId",
                            "CampaignName",
                            "CampaignType",
                            "AdGroupId",
                            "AdGroupName",
                            "AdId",
                            "AdFormat",
                            "CriterionId",
                            "Criterion",
                            "CriterionType",
                            "Impressions",
                            "Clicks",
                            "Cost",
                            "AdNetworkType",
                            "Placement",
                            "AvgImpressionPosition",
                            "AvgClickPosition",
                            "Slot",
                            "BounceRate",
                            "AvgPageviews",
                            "Conversions",
                            "Age",
                            "Gender",
                            "Device",
                            "MobilePlatform",
                            "LocationOfPresenceId",
                            "LocationOfPresenceName",
                            "TargetingLocationId",
                            "TargetingLocationName",
                            "RlAdjustmentId"
                    ],
                    "ReportName": u("Request1" + cls.date_start + "_" + cls.date_end),
                    "Page": {
                        "Limit": 10000000
                    },
                    "ReportType": "CUSTOM_REPORT",
                    "DateRangeType": "CUSTOM_DATE",
                    "Format": "TSV",
                    "IncludeVAT": "YES",
                }
            }

            # Преобразование в JSON формат
            cls.body_json = json.dumps(cls.body, indent=4)

    # Цикл передачи запроса и обработка ответа от сервера
    @classmethod
    def post_request(cls):

        while True:
            try:
                cls.req = requests.post(cls.ReportsURL, cls.body_json, headers=cls.headers)
                cls.req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
                if cls.req.status_code == 400:
                    logger.error("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    logger.error("RequestId: {}".format(cls.req.headers.get("RequestId", False)))
                    logger.error("JSON-код запроса: {}".format(u(cls.body_json)))
                    logger.error("JSON-код ответа сервера: \n{}".format(u(cls.req.json())))
                    break
                elif cls.req.status_code == 200:
                    logger.info("Отчет создан успешно")
                    print("Отчет создан успешно")
                    logger.info("RequestId: {}".format(cls.req.headers.get("RequestId", False)))
                    break
                elif cls.req.status_code == 201:
                    logger.info("Отчет успешно поставлен в очередь в режиме офлайн")
                    print("Отчет успешно поставлен в очередь в режиме офлайн")
                    retryIn = int(cls.req.headers.get("retryIn", 60))
                    logger.info("Повторная отправка запроса через {} секунд".format('60'))
                    logger.info("RequestId: {}".format(cls.req.headers.get("RequestId", False)))
                    sleep(60)
                elif cls.req.status_code == 202:
                    logger.info("Отчет формируется в режиме офлайн")
                    print("Отчет формируется в режиме офлайн")
                    retryIn = int(req.headers.get("retryIn", 60))
                    logger.info("Повторная отправка запроса через {} секунд".format(retryIn))
                    logger.info("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 500:
                    logger.error("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    logger.error("RequestId: {}".format(req.headers.get("RequestId", False)))
                    logger.error("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 502:
                    logger.error("Время формирования отчета превысило серверное ограничение.")
                    print("Время формирования отчета превысило серверное ограничение.")
                    logger.error("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    logger.error("JSON-код запроса: {}".format(cls.body_json))
                    logger.error("RequestId: {}".format(req.headers.get("RequestId", False)))
                    logger.error("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break

                else:
                    logger.error("Произошла непредвиденная ошибка")
                    print("Произошла непредвиденная ошибка")
                    logger.error("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    logger.error("JSON-код запроса: {}".format(cls.body_json))
                    logger.error("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break

            # Обработка ошибки, если не удалось соединиться с сервером API Директа
            except ConnectionError:
                # В данном случае мы рекомендуем повторить запрос позднее
                logger.error("Произошла ошибка соединения с сервером API")
                print("Произошла ошибка соединения с сервером API")
                # Принудительный выход из цикла
                break

                # Если возникла какая-либо другая ошибка
            except:
                # В данном случае мы рекомендуем проанализировать действия приложения
                logger.error("Произошла непредвиденная ошибка")
                print("Произошла непредвиденная ошибка")
                # Принудительный выход из цикла
                break

        # Разбиваем полученные строки (для последующей обработки) и возвращаем итератор
        cls.extracted_data = (row.split("\t") for row in cls.req.text.split("\n"))
        return cls.extracted_data


class Load:
    __instance = None

    # Шаблон singleton для создания объекта Load (подключение к БД)
    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    # Конструктор объекта класса
    def __init__(self, host, port, database, user, password, autocommit=False):
        try:
            self.connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )

            if autocommit is True:
                self.connection.autocommit = True


        except Exception as err:
            logger.error(f"Ошибка подключения к БД: {err}. Проверьте указанные доступы к БД.")
            print(f"Ошибка подключения к БД: {err}. Проверьте указанные доступы к БД.")

    # Создание таблицы
    def create_table(self):
        try:
            self.__query = """
            create table if not exists ya_direct (
                id serial primary key,
                Date date,
                CampaignId varchar,
                CampaignName varchar,
                CampaignType varchar,
                AdGroupId int,
                AdGroupName varchar,
                AdId int,
                AdFormat varchar,
                CriterionId int,
                Criterion varchar,
                CriterionType varchar,
                Impressions smallint,
                Clicks smallint,
                Cost numeric,
                AdNetworkType varchar,
                Placement varchar,
                AvgImpressionPosition decimal,
                AvgClickPosition decimal,
                Slot varchar,
                BounceRate decimal,
                AvgPageviews decimal,
                        form_order_callback smallint,       # Указываем свое название конверсии (цель Метрики)
                        call smallint,      # Указываем свое название конверсии (цель Метрики)
                        form_callback smallint,     # Указываем свое название конверсии (цель Метрики)
                        purchase smallint,      # Указываем свое название конверсии (цель Метрики)
                        form_call_manager smallint,     # Указываем свое название конверсии (цель Метрики)
                        form_under_search smallint,     # Указываем свое название конверсии (цель Метрики)
                Age varchar,
                Gender varchar,
                Device varchar,
                MobilePlatform varchar,
                LocationOfPresenceId varchar,
                LocationOfPresenceName varchar,
                TargetingLocationId varchar,
                TargetingLocationName varchar,
                RlAdjustmentId varchar,
                TargetingCategory varchar
            );
                        """

            with self.connection.cursor() as cursor:
                cursor.execute(self.__query)

                if self.connection.autocommit is False:
                    self.connection.commit()

                logger.info("Таблица ya_direct создана")
                cursor.close()

        except Exception as err:
            logger.error(f"Ошибка при создании таблицы: {err}. Проверьте правильность SQL-скрипта и указанных типов данных.")
            print(f"Ошибка при создании таблицы: {err}. Проверьте правильность SQL-скрипта и указанных типов данных.")

    def insert_values(self, rows):
        self.__query = """
            insert into ya_direct(
                        Date,
                        CampaignId,
                        CampaignName,
                        CampaignType,
                        AdGroupId,
                        AdGroupName,
                        AdId,
                        AdFormat,
                        CriterionId,
                        Criterion,
                        CriterionType,
                        Impressions,
                        Clicks,
                        cost,
                        AdNetworkType,
                        Placement,
                        AvgImpressionPosition,
                        AvgClickPosition,
                        Slot,
                        BounceRate,
                        AvgPageviews,
                        form_order_callback,
                        call,
                        form_callback,
                        purchase,
                        form_call_manager,
                        form_under_search,
                        Age,
                        Gender,
                        Device,
                        MobilePlatform,
                        LocationOfPresenceId,
                        LocationOfPresenceName,
                        TargetingLocationId,
                        TargetingLocationName,
                        RlAdjustmentId,
                        TargetingCategory
                    )
                        values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

        with self.connection.cursor() as cursor:
            logger.info("Начало загрузки данных в БД")
            for row in rows:

                try:
                    cursor.execute(self.__query, (row))

                    if self.connection.autocommit is False:
                        self.connection.commit()
                    print(row)

                except Exception as err:
                    logger.error(f"Ошибка при загрузке строки в БД: {err}, строка {row} пропущена")
                    print(f"Ошибка при загрузке строки в БД: {err}, строка {row} пропущена")

            logger.info("Данные загружены в БД")
            print("Данные загружены в БД")
            cursor.close()

    def truncate_table(self):
        self.__query = """truncate table ya_direct"""
        with self.connection.cursor() as cursor:
            cursor.execute(self.__query)

            if self.connection.autocommit is False:
                self.connection.commit()

    def close_connection(self):
        self.connection.close()


# Формирование HTTP-запроса
# Указываем персональные данные для подключения
Extract.create_http_header(ReportsURL = "https://api.direct.yandex.com/json/v5/reports", token = "Идентификатор_токена", clientLogin = "Логин_аккаунта_Яндекс.Директ")
#Указываем диапазон дат выгрузки
Extract.create_http_body('start_date', 'end_date')

# Проверка и преобразование данных для последующей загрузки в БД
header, transformed_data = transform(Extract.post_request())


# Создание объекта класса Load для подключения к БД
db = Load(host="хост", port=5432, database="База данных", user="Пользователь", password="Пароль")

# Создание таблицы, если она не существует
db.create_table()
# Вставка преобразованных значений в БД
db.insert_values(transformed_data)
#db.truncate_table()

# Закрытие соединения с БД
db.close_connection()