import logging
import psycopg2
from datetime import datetime, date, time, timedelta

# Настройка логирования
logger = logging.getLogger(__name__)
fileHandler = logging.FileHandler("logs/" + f"{date.today()}.log", mode="a", encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="%(asctime)s | %(name)s | %(levelname)s: %(message)s",
    handlers=[fileHandler]
)

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
                        form_order_callback smallint,
                        call smallint,
                        form_callback smallint,
                        purchase smallint,
                        form_call_manager smallint,
                        form_under_search smallint,
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

    def post(self, rows):
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