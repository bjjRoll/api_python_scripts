import os
from dotenv import load_dotenv
import importlib
import pandas as pd
from datetime import date
import logging
import yaml
import traceback
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

Base = declarative_base()


class ParsedData(Base):
    __tablename__ = 'parsed_data'

    id = Column(Integer, primary_key=True)
    identifier = Column(String, index=True)
    platform = Column(String, index=True)
    link = Column(String)
    name = Column(String)
    price = Column(String)
    description = Column(Text)
    region = Column(String)
    date_of_publication = Column(String)
    status = Column(String)
    object_type = Column(String)
    additional_information = Column(Text)
    upload_date = Column(DateTime)


STANDARD_COLUMNS = [
    'идентификатор',
    'площадка',
    'ссылка',
    'название',
    'цена',
    'описание',
    'регион',
    'дата публикации',
    'статус',
    'тип объекта',
    'дополнительная информация'
]


def load_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)


def run_parser(parser_module):
    try:
        module = importlib.import_module(f'src.parsers.{parser_module}')
        result = module.run_parser()
        if isinstance(result, pd.DataFrame):
            for col in STANDARD_COLUMNS:
                if col not in result.columns:
                    result[col] = None

            result = result[STANDARD_COLUMNS]

            logger.info(f"Парсер {parser_module} вернул DataFrame с {len(result)} записями")
            return result
        else:
            logger.error(f"Парсер {parser_module} вернул неверный тип данных: {type(result)}")
            return None
    except Exception as e:
        logger.error(f"Ошибка при выполнении парсера {parser_module}: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def update_excel(new_data, filename='result.xlsx'):
    try:
        if os.path.exists(filename):
            existing_data = pd.read_excel(filename)
            logger.info(f"Загружено {len(existing_data)} существующих записей из {filename}")
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)

            logger.info(f"Количество строк до удаления дубликатов: {len(combined_data)}")
            combined_data.drop_duplicates(subset=['площадка', 'идентификатор'], keep='last', inplace=True)
            logger.info(f"Количество строк после удаления дубликатов: {len(combined_data)}")
        else:
            combined_data = new_data
            logger.info(f"Создан новый файл {filename}")

        combined_data['дата выгрузки'] = pd.to_datetime(combined_data['дата выгрузки'])
        combined_data.sort_values('дата выгрузки', ascending=False, inplace=True)

        logger.info(f"Колонки перед сохранением: {combined_data.columns.tolist()}")
        logger.info(f"Количество строк перед сохранением: {len(combined_data)}")

        combined_data.to_excel(filename, index=False)
        logger.info(f"Сохранено {len(combined_data)} записей в файл {filename}")
    except Exception as e:
        logger.error(f"Ошибка при обновлении Excel файла: {str(e)}")
        logger.error(traceback.format_exc())


def main():
    config = load_config()
    all_data = []

    for parser in config['parsers']:
        logger.info(f"Запуск парсера: {parser}")
        parser_data = run_parser(parser)
        if parser_data is not None and not parser_data.empty:
            current_date = date.today()
            parser_data['дата выгрузки'] = current_date.strftime('%d.%m.%Y')
            all_data.append(parser_data)
            logger.info(f"Добавлено {len(parser_data)} записей от парсера {parser}")
        else:
            logger.warning(f"Парсер {parser} не вернул данных")

    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        logger.info(f"Всего собрано {len(combined_data)} записей")
        update_excel(combined_data)

        try:
            db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            engine = create_engine(db_url)
            Base.metadata.create_all(engine)

            Session = sessionmaker(bind=engine)
            session = Session()

            for _, row in combined_data.iterrows():
                data_dict = {
                    'identifier': row['идентификатор'],
                    'platform': row['площадка'],
                    'link': row['ссылка'],
                    'name': row['название'],
                    'price': row['цена'],
                    'description': row['описание'],
                    'region': row['регион'],
                    'date_of_publication': row['дата публикации'],
                    'status': row['статус'],
                    'object_type': row['тип объекта'],
                    'additional_information': row['дополнительная информация'],
                    'upload_date': row['дата выгрузки']
                }
                parsed_data = ParsedData(**data_dict)
                session.merge(parsed_data)

            session.commit()
            logger.info("Данные успешно добавлены в базу данных")
        except Exception as e:
            logger.error(f"Ошибка при работе с базой данных: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            session.close()
    else:
        logger.warning("Нет данных для сохранения")


if __name__ == "__main__":
    main()
