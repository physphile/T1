import logging
from io import BytesIO

import pandas
from custom_logger import log
from fastapi import HTTPException
from fastapi_sqlalchemy import db
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

TABLE_NAME = "fuzzy"
logger = log(name="backend", level=logging.DEBUG, log_folder_path="logs")


def create_virtual_table(csvs: list[bytes]):
    # engine = create_engine("sqlite+pysqlite:///database.sqlite", echo=True)
    # print("Engine created")

    # conn = engine.connect()
    # print("Engine connection established")
    try:
        db.session.execute(
            text(f"CREATE EXTENSION IF NOT EXISTS fuzzystrmatch SCHEMA public;")
        )
        db.session.execute(
            text(f"ALTER TABLE fuzzy ADD COLUMN IF NOT EXISTS group_id INTEGER;")
        )
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

    for index, csv in enumerate(csvs):
        logger.debug(f"Processing file #{index + 1}...")
        dataframe = pandas.read_csv(
            BytesIO(bytearray(csv)), low_memory=False, encoding="utf-8"
        )
        if_exists = "replace" if index == 0 else "append"
        try:
            dataframe.to_sql(
                TABLE_NAME, db.session.connection(), if_exists=if_exists, index=False
            )
        except OperationalError as e:
            logger.critical(e)
            raise HTTPException(status_code=422)

        logger.debug(f"File #{index + 1} processed")

    logger.info("Изменения сохранены")


def get_table_headers():
    try:
        result = db.session.execute(
            text(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='{TABLE_NAME}';"
            )
        )
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

    return list(result.mappings().fetchall())


def fuzzy_group(reference_columns):
    try:
        # Группировка всего и вся
        db.session.execute(
            text(
                f"""
                    WITH NormalizedRecords AS (
                        SELECT 
                            client_id,
                            client_fio_full,
                            client_bday,
                            LOWER(REPLACE(REPLACE(client_fio_full, ' ', ''), '-', '')) AS normalized_name,
                            LOWER(SPLIT_PART(client_fio_full, ' ', 1)) AS first_part,
                            LOWER(SPLIT_PART(client_fio_full, ' ', 2)) AS middle_part,
                            LOWER(SPLIT_PART(client_fio_full, ' ', -1)) AS last_part
                        FROM 
                            fuzzy
                    ),
                    RepresentativeNames AS (
                        SELECT DISTINCT ON (n1.client_bday, n1.normalized_name)
                            n1.client_bday,
                            n1.normalized_name,
                            n2.client_fio_full AS representative_name
                        FROM 
                            NormalizedRecords n1
                        JOIN 
                            NormalizedRecords n2 
                            ON n1.client_bday = n2.client_bday
                            AND (
                                levenshtein(n1.normalized_name, n2.normalized_name) <= 2 OR
                                LOWER(n1.client_fio_full) LIKE '%' || LOWER(n2.client_fio_full) || '%' OR
                                LOWER(n2.client_fio_full) LIKE '%' || LOWER(n1.client_fio_full) || '%' OR
                                (
                                    n1.first_part = n2.first_part AND 
                                    (n1.last_part = n2.last_part OR n1.middle_part = n2.middle_part)
                                )
                            )
                        ORDER BY 
                            n1.client_bday, n1.normalized_name, levenshtein(n1.normalized_name, n2.normalized_name)
                    ),
                    SimilarityGroups AS (
                        SELECT 
                            n1.client_bday,
                            r.representative_name,
                            ARRAY_AGG(n1.client_id) AS group_ids,
                            ARRAY_AGG(n1.client_fio_full) AS group_names
                        FROM 
                            NormalizedRecords n1
                        JOIN 
                            RepresentativeNames r 
                            ON n1.client_bday = r.client_bday AND n1.normalized_name = r.normalized_name
                        GROUP BY 
                            n1.client_bday, r.representative_name
                    ),
                    AssignGroupIDs AS (
                        SELECT 
                            unnest(group_ids) AS client_id,
                            ROW_NUMBER() OVER () AS group_id
                        FROM 
                            SimilarityGroups
                    )
                    UPDATE 
                        fuzzy f
                    SET 
                        group_id = g.group_id
                    FROM 
                        AssignGroupIDs g
                    WHERE 
                        f.client_id = g.client_id;
            """
            )
        )

        # Вот эта срань выдаст список словариков
        # result = db.session.execute(
        #     text(
        #         f'''WITH NormalizedRecords AS (
        #             SELECT
        #                 client_id,
        #                 client_fio_full,
        #                 client_bday,
        #                 LOWER(REPLACE(REPLACE(client_fio_full, ' ', ''), '-', '')) AS normalized_name,
        #                 LOWER(SPLIT_PART(client_fio_full, ' ', 1)) AS first_part,
        #                 LOWER(SPLIT_PART(client_fio_full, ' ', 2)) AS middle_part,
        #                 LOWER(SPLIT_PART(client_fio_full, ' ', -1)) AS last_part
        #             FROM
        #                 fuzzy
        #         ),
        #         RepresentativeNames AS (
        #             SELECT DISTINCT ON (n1.client_bday, n1.normalized_name)
        #                 n1.client_bday,
        #                 n1.normalized_name,
        #                 n2.client_fio_full AS representative_name
        #             FROM
        #                 NormalizedRecords n1
        #             JOIN
        #                 NormalizedRecords n2
        #                 ON n1.client_bday = n2.client_bday
        #                 AND (
        #                     levenshtein(n1.normalized_name, n2.normalized_name) <= 2 OR
        #                     LOWER(n1.client_fio_full) LIKE '%' || LOWER(n2.client_fio_full) || '%' OR
        #                     LOWER(n2.client_fio_full) LIKE '%' || LOWER(n1.client_fio_full) || '%' OR
        #                     (
        #                         n1.first_part = n2.first_part AND
        #                         (n1.last_part = n2.last_part OR n1.middle_part = n2.middle_part)
        #                     )
        #                 )
        #             ORDER BY
        #                 n1.client_bday, n1.normalized_name, levenshtein(n1.normalized_name, n2.normalized_name)
        #         ),
        #         SimilarityGroups AS (
        #             SELECT
        #                 n1.client_bday,
        #                 r.representative_name,
        #                 ARRAY_AGG(n1.client_id) AS group_ids,
        #                 ARRAY_AGG(n1.client_fio_full) AS group_names
        #             FROM
        #                 NormalizedRecords n1
        #             JOIN
        #                 RepresentativeNames r
        #                 ON n1.client_bday = r.client_bday AND n1.normalized_name = r.normalized_name
        #             GROUP BY
        #                 n1.client_bday, r.representative_name
        #         )
        #         SELECT
        #             JSON_AGG(
        #                 JSON_BUILD_OBJECT(
        #                     'client_fio_full', representative_name,
        #                     'date_birth', client_bday,
        #                     'group_size', array_length(group_ids, 1),
        #                     'group_ids', group_ids,
        #                     'group_names', group_names,
        #                     'properties', (
        #                         SELECT JSON_AGG(
        #                             JSON_BUILD_OBJECT(
        #                                 'id', client_id,
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
        #                         FROM fuzzy u
        #                         WHERE u.client_id = ANY(group_ids)
        #                     )
        #                 )
        #             ) AS grouped_users
        #         FROM
        #             SimilarityGroups;
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
def create_golden_table(reference_columns):
    try:
        result = db.session.execute(
                text(
                    f'''CREATE TABLE IF NOT EXISTS GOLDEN_TABLE (LIKE {TABLE_NAME} INCLUDING ALL);'''))
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

def frequence_analisys_column(reference_column):
    try:
        result = db.session.execute(
            text(
                f'''DO $$
DECLARE
    column_record RECORD;  
    most_frequent_value TEXT; 
BEGIN
    FOR column_record IN 
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'DM_INFRA_LOGS' AND TABLE_NAME = 'incident_hint'
    LOOP
        EXECUTE format('
            SELECT %I 
            FROM "DM_INFRA_LOGS".incident_hint 
            GROUP BY %I 
            HAVING COUNT(*) = (
                SELECT COUNT(*) 
                FROM "DM_INFRA_LOGS".incident_hint 
                GROUP BY %I 
                ORDER BY COUNT(*) DESC 
                LIMIT 1
            )', column_record.column_name, column_record.column_name, column_record.column_name)
        INTO most_frequent_value;

        RAISE NOTICE column_record.column_name, most_frequent_value;
    END LOOP;
END $$;'''))
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

    return result.mappings().all()[:100]
