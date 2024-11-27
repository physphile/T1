from io import BytesIO
import pandas
from fastapi_sqlalchemy import db
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from custom_logger import log
import logging

TABLE_NAME = "fuzzy"
logger = log(name="backend", level=logging.DEBUG, log_folder_path="backend/logs")


def create_virtual_table(csvs: list[bytes]):
    # engine = create_engine("sqlite+pysqlite:///database.sqlite", echo=True)
    # print("Engine created")

    # conn = engine.connect()
    # print("Engine connection established")
    try:
        db.session.execute(
            text(
                f"CREATE VIRTUAL TABLE IF NOT EXISTS {TABLE_NAME} USING fts4({','.join(get_table_headers())});"
            )
        )
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

    logger.info("Virtual table created")

    for index, csv in enumerate(csvs):
        logger.debug(f"Processing file #{index + 1}...")
        dataframe = pandas.read_csv(BytesIO(bytearray(csv)), low_memory=False)
        if_exists = "replace" if index == 0 else "append"
        try:
            dataframe.to_sql(TABLE_NAME, db.session.connection(), if_exists=if_exists, index=False)
        except OperationalError as e:
            logger.critical(e)
            raise HTTPException(status_code=422)
    
        logger.debug(f"File #{index + 1} processed")

    logger.info("Изменения сохранены")


def get_table_headers():
    try:
        result = db.session.execute(text(f"PRAGMA table_info({TABLE_NAME});"))
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

    return list(map(lambda row: row[1], result))


def fuzzy_group():
    try:
        result = db.session.execute(
            text(
                f'''select t1.*,
                    t2.Stdname
                    from {TABLE_NAME} t1
                    inner join
                    (
                    select client_fio_full as stdName, snd, rn
                    from
                    (
                        select client_fio_full, soundex(client_fio_full) snd,
                        row_number() over(partition by soundex(client_fio_full)
                                            order by soundex(client_fio_full)) rn
                        from {TABLE_NAME}
                    ) d
                    where rn = 1
                    ) t2
                    on soundex(t1.client_fio_full) = t2.snd'''
            )
        )

        # result = db.session.execute(
        #     text(
        #         f"SELECT * FROM {TABLE_NAME} LIMIT 1;"
        #     )
        # )
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

    return result.mappings().all()[:100]
