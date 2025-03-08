import psycopg2
import requests
import os
import time
import sys
import json
import csv
import re
import logging
import configparser
from datetime import datetime, date, time, timedelta
from requests.exceptions import ConnectionError
from time import sleep
from transform_data_mod import transform
from db_pg_connection import Load
from glob import glob

config = configparser.ConfigParser()
config.read('config.ini')

# Настройка логирования
logger = logging.getLogger(__name__)
fileHandler = logging.FileHandler(config['LOGS']['file_path'] + f"{date.today()}.log", mode="a", encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s | %(name)s | %(levelname)s: %(message)s",
    handlers=[fileHandler]
)

# Проверка файлов с логами и удаление логов, записанных более 3-х дней назад
files_log = [fl for fl in glob(config['LOGS']['file_path'] + "*") if re.search(r"logs\\\d{4}-\d{2}-\d{2}\.log", fl)]

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
                # OAuth-токен. Слово "Bearer" обязательно к использованию
                "Authorization": "Bearer " + token,
                # Логин клиента рекламного агентства
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


    # Формирование тела запроса
    @classmethod
    def create_http_body(cls, date_start=(date.today() - timedelta(days=2)).strftime("%Y-%m-%d"), date_end=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d")):
        try:
            # Указание диапазона дат для выгрузки данных из API Яндекс.Директ
            if re.match(r"\d{4}-\d{2}-\d{2}", date_start) and re.match(r"\d{4}-\d{2}-\d{2}", date_end):
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
                    #
                    "Goals": ["101103598", "129037972", "235883055", "235893048", "274250920", "283275292"],
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
                    # print("Содержание отчета: \n{}".format(u(req.text)))
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


        cls.extracted_data = (row.split("\t") for row in cls.req.text.split("\n"))
        return cls.extracted_data


# Формирование HTTP-запроса
Extract.create_http_header(ReportsURL = config['HTTP']['REPORTS_URL'], token = config['HTTP']['TOKEN'], clientLogin = config['HTTP']['CLIENTLOGIN'])
Extract.create_http_body('2025-02-08')

# Проверка и преобразование данных для последующей загрузки в БД
header, transformed_data = transform(Extract.post_request())

# Создание объекта класса Load (созданного модуля) для подключения к БД
db = Load(host=config['Database']['HOST'], port=config['Database']['PORT'], database=config['Database']['DATABASE'], user=config['Database']['USER'], password=config['Database']['PASSWORD'])

# Создание таблицы, если она не существует
db.create_table()
# Вставка преобразованных значений в БД
db.post(transformed_data)
#db.truncate_table()

# Закрытие соединения с БД
db.close_connection()