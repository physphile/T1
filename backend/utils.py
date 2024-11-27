from io import BytesIO
import pandas
from fastapi_sqlalchemy import db
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from custom_logger import log
import logging

TABLE_NAME = "fuzzy"
logger = log(name="backend", level=logging.DEBUG, log_folder_path="logs")


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


def fuzzy_group(reference_columns):
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
        #         f'''WITH NormalizedRecords AS (
        #             SELECT 
        #                 id,
        #                 client_fio_full,
        #                 client_bday,
        #                 LOWER(REPLACE(REPLACE(client_fio_full, ' ', ''), '-', '')) AS normalized_name,
        #                 LOWER(SUBSTRING_INDEX(client_fio_full, ' ', 1)) AS first_part,
        #                 LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(client_fio_full, ' ', 2), ' ', -1)) AS middle_part,
        #                 LOWER(SUBSTRING_INDEX(client_fio_full, ' ', -1)) AS last_part
        #             FROM 
        #                 users_table
        #         ),
        #         SimilarityGroups AS (
        #             SELECT 
        #                 client_bday,
        #                 normalized_name,
        #                 (
        #                     SELECT client_fio_full 
        #                     FROM NormalizedRecords n2 
        #                     WHERE 
        #                         n2.client_bday = n1.client_bday AND
        #                         (
        #                             levenshtein(n1.normalized_name, n2.normalized_name) <= 2 OR
        #                             LOWER(n1.client_fio_full) LIKE '%' || LOWER(n2.client_fio_full) || '%' OR
        #                             LOWER(n2.client_fio_full) LIKE '%' || LOWER(n1.client_fio_full) || '%' OR
        #                             (
        #                                 n1.first_part = n2.first_part AND 
        #                                 (n1.last_part = n2.last_part OR n1.middle_part = n2.middle_part)
        #                             )
        #                         )
        #                     LIMIT 1
        #                 ) AS representative_name,
        #                 ARRAY_AGG(id) AS group_ids,
        #                 ARRAY_AGG(client_fio_full) AS group_names
        #             FROM 
        #                 users_table u1
        #             JOIN 
        #                 NormalizedRecords n1 ON u1.id = n1.id
        #             GROUP BY 
        #                 client_bday, normalized_name
        #         )
        #         SELECT 
        #             JSON_AGG(
        #                 JSON_BUILD_OBJECT(
        #                     'client_fio_full', representative_name,
        #                     'date_birth', client_bday,
        #                     'group_size', group_size,
        #                     'group_ids', group_ids,
        #                     'group_names', group_names,
        #                     'properties', (
        #                         SELECT JSON_AGG(
        #                             JSON_BUILD_OBJECT(
        #                                 'id', id,
        #                                 'client_fio_full', client_fio_full,
        #                                 'properties', (
        #                                     SELECT json_object_agg(key, value)
        #                                     FROM (
        #                                         SELECT 
        #                                             key, 
        #                                             value 
        #                                         FROM jsonb_each(to_jsonb(u.*)) 
        #                                         WHERE 
        #                                             key NOT IN ('id', 'client_fio_full', 'client_bday') AND
        #                                             (
        #                                                 (value IS NOT NULL AND value::text != '') OR
        #                                                 jsonb_typeof(value) = 'number' OR
        #                                                 jsonb_typeof(value) = 'boolean'
        #                                             )
        #                                     ) AS extra_properties
        #                                 )
        #                             )
        #                         )
        #                         FROM users_table u
        #                         WHERE u.id = ANY(group_ids)
        #                     )
        #                 )
        #             ) AS grouped_users
        #         FROM 
        #             SimilarityGroups
        #         '''
        #     )
        # )

        # result = db.session.execute(
        #     text(
        #         f"SELECT * FROM {TABLE_NAME} LIMIT 1;"
        #     )
        # )
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

    return result.mappings().all()[:100]
