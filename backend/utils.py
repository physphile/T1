import logging
from io import BytesIO

import pandas
from settings import get_settings
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


def fuzzy_group(columns):
    try:
        cores = 8
        set_cores_sql = f"SET max_parallel_workers_per_gather TO {cores};"

        normalized_columns = [
            f"LOWER(REPLACE(REPLACE(COALESCE({col}, ''), ' ', ''), '-', '')) AS normalized_{col}" for col in columns
        ]
        levenshtein_conditions = [
            f"(LENGTH(n1.normalized_{col}) - LENGTH(n2.normalized_{col}) <= 2 AND levenshtein(n1.normalized_{col}, n2.normalized_{col}) <= 2)" for col in columns
        ]
        substring_conditions = [
            f"(n1.normalized_{col} LIKE '%' || n2.normalized_{col} || '%' OR n2.normalized_{col} LIKE '%' || n1.normalized_{col} || '%')" for col in columns
        ]
        index_name = f"idx_fuzzy_{'_'.join(columns)}"  # Generate a unique index name based on columns
        create_index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON fuzzy({', '.join(columns)});"

        sql_query = f"""
            -- Step 1: Add dynamic index for faster JOIN and filtering
            {set_cores_sql}

            {create_index_sql}

            -- Step 2: Temporary table for normalized data
            CREATE TEMP TABLE temp_normalized_records AS
            SELECT
                client_id,
                {', '.join(columns)},
                {', '.join(normalized_columns)} -- Pre-normalized columns
            FROM
                fuzzy;

            -- Step 3: Calculate representative names
            CREATE TEMP TABLE temp_representative_names AS
            SELECT DISTINCT ON ({', '.join([f'n1.{col}' for col in columns])})
                n1.client_id AS client_id,
                n2.client_id AS representative_client_id
            FROM
                temp_normalized_records n1
            JOIN
                temp_normalized_records n2
            ON
                n1.client_id <> n2.client_id -- Avoid self-joins
                AND (
                    { ' OR '.join(levenshtein_conditions) } -- Levenshtein conditions
                    OR
                    { ' OR '.join(substring_conditions) }  -- Substring matching
                )
            ORDER BY
                {', '.join([f'n1.{col}' for col in columns])},
                n2.client_id;

            -- Step 4: Group clients by similarity
            CREATE TEMP TABLE temp_similarity_groups AS
            SELECT
                r.representative_client_id,
                ARRAY_AGG(n1.client_id) AS group_ids,
                ARRAY_AGG({ ' || \' \' || '.join([f"COALESCE(n1.{col}, '')" for col in columns]) }) AS group_values
            FROM
                temp_normalized_records n1
            JOIN
                temp_representative_names r
            ON
                n1.client_id = r.client_id
            GROUP BY
                r.representative_client_id;

            -- Step 5: Assign group IDs and update original table
            WITH AssignGroupIDs AS (
                SELECT
                    unnest(group_ids) AS client_id,
                    ROW_NUMBER() OVER () AS group_id
                FROM
                    temp_similarity_groups
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
        # Это неоптимизированный запрос
        # normalized_columns = []
        # levenshtein_conditions = []
        # substring_conditions = []

        # for col in reference_columns:
        #     normalized_columns.append(f"LOWER(REPLACE(REPLACE(COALESCE({col}, ''), ' ', ''), '-', '')) AS normalized_{col}")
        #     levenshtein_conditions.append(f"(n1.{col} IS NOT NULL AND n2.{col} IS NOT NULL AND levenshtein(LOWER(n1.{col}), LOWER(n2.{col})) <= 2)")
        #     substring_conditions.append(f"(n1.{col} IS NOT NULL AND n2.{col} IS NOT NULL AND LOWER(n1.{col}) LIKE '%' || LOWER(n2.{col}) || '%')")
        #     substring_conditions.append(f"(n1.{col} IS NOT NULL AND n2.{col} IS NOT NULL AND LOWER(n2.{col}) LIKE '%' || LOWER(n1.{col}) || '%')")


        # normalized_columns_sql = ", ".join(normalized_columns)
        # levenshtein_sql = " OR ".join(levenshtein_conditions)
        # substring_sql = " OR ".join(substring_conditions)

        # sql_query = f"""
        #     -- Create index on client_id for faster JOIN operations
        #     CREATE INDEX IF NOT EXISTS idx_fuzzy_client_id ON fuzzy(client_id);

        #     WITH NormalizedRecords AS (
        #         SELECT 
        #             client_id,
        #             {', '.join(reference_columns)},
        #             {normalized_columns_sql}  -- Dynamically generated normalized columns with NULL handling
        #         FROM 
        #             fuzzy
        #     ),
        #     RepresentativeNames AS (
        #         SELECT DISTINCT ON ({', '.join(reference_columns)})
        #             {', '.join(["n1."+col for col in reference_columns])},
        #             n2.client_id AS representative_client_id
        #         FROM 
        #             NormalizedRecords AS n1  -- Declare n1 explicitly
        #         JOIN 
        #             NormalizedRecords AS n2  -- Declare n2 explicitly
        #         ON n1.client_id <> n2.client_id AND  -- Prevent self-joining
        #         (
        #             ({levenshtein_sql}) OR 
        #             ({substring_sql})
        #         )
        #         ORDER BY 
        #             {', '.join(reference_columns)}, 
        #             {levenshtein_sql}
        #     ),
        #     SimilarityGroups AS (
        #         SELECT 
        #             {', '.join(["n1."+col for col in reference_columns])},
        #             r.representative_client_id AS representative_client_id,
        #             ARRAY_AGG(n1.client_id) AS group_ids,
        #             ARRAY_AGG({ " || ' ' || ".join([f"COALESCE(n1.{col}, '')" for col in reference_columns]) }) AS group_values
        #         FROM 
        #             NormalizedRecords AS n1  -- Declare n1 explicitly
        #         JOIN 
        #             RepresentativeNames AS r 
        #         ON n1.client_id = r.representative_client_id
        #         GROUP BY 
        #             {', '.join(["n1."+col for col in reference_columns])}, r.representative_client_id
        #     ),
        #     AssignGroupIDs AS (
        #         SELECT 
        #             unnest(group_ids) AS client_id,
        #             ROW_NUMBER() OVER () AS group_id
        #         FROM 
        #             SimilarityGroups
        #     )
        #     UPDATE 
        #         fuzzy AS f
        #     SET 
        #         group_id = g.group_id
        #     FROM 
        #         AssignGroupIDs AS g
        #     WHERE 
        #         f.client_id = g.client_id;
        #     """
        # db.session.execute(text(sql_query))
        # Вот эта срань сделает группировку по ФИО и ДР
        # db.session.execute(
        #     text(
        #         f"""
        #             WITH NormalizedRecords AS (
        #                 SELECT
        #                     client_id,
        #                     client_fio_full,
        #                     client_bday,
        #                     LOWER(REPLACE(REPLACE(client_fio_full, ' ', ''), '-', '')) AS normalized_name,
        #                     LOWER(SPLIT_PART(client_fio_full, ' ', 1)) AS first_part,
        #                     LOWER(SPLIT_PART(client_fio_full, ' ', 2)) AS middle_part,
        #                     LOWER(SPLIT_PART(client_fio_full, ' ', -1)) AS last_part
        #                 FROM
        #                     fuzzy
        #             ),
        #             RepresentativeNames AS (
        #                 SELECT DISTINCT ON (n1.client_bday, n1.normalized_name)
        #                     n1.client_bday,
        #                     n1.normalized_name,
        #                     n2.client_fio_full AS representative_name
        #                 FROM
        #                     NormalizedRecords n1
        #                 JOIN
        #                     NormalizedRecords n2
        #                     ON n1.client_bday = n2.client_bday
        #                     AND (
        #                         levenshtein(n1.normalized_name, n2.normalized_name) <= 2 OR
        #                         LOWER(n1.client_fio_full) LIKE '%' || LOWER(n2.client_fio_full) || '%' OR
        #                         LOWER(n2.client_fio_full) LIKE '%' || LOWER(n1.client_fio_full) || '%' OR
        #                         (
        #                             n1.first_part = n2.first_part AND
        #                             (n1.last_part = n2.last_part OR n1.middle_part = n2.middle_part)
        #                         )
        #                     )
        #                 ORDER BY
        #                     n1.client_bday, n1.normalized_name, levenshtein(n1.normalized_name, n2.normalized_name)
        #             ),
        #             SimilarityGroups AS (
        #                 SELECT
        #                     n1.client_bday,
        #                     r.representative_name,
        #                     ARRAY_AGG(n1.client_id) AS group_ids,
        #                     ARRAY_AGG(n1.client_fio_full) AS group_names
        #                 FROM
        #                     NormalizedRecords n1
        #                 JOIN
        #                     RepresentativeNames r
        #                     ON n1.client_bday = r.client_bday AND n1.normalized_name = r.normalized_name
        #                 GROUP BY
        #                     n1.client_bday, r.representative_name
        #             ),
        #             AssignGroupIDs AS (
        #                 SELECT
        #                     unnest(group_ids) AS client_id,
        #                     ROW_NUMBER() OVER () AS group_id
        #                 FROM
        #                     SimilarityGroups
        #             )
        #             UPDATE
        #                 fuzzy f
        #             SET
        #                 group_id = g.group_id
        #             FROM
        #                 AssignGroupIDs g
        #             WHERE
        #                 f.client_id = g.client_id;
        #     """
        #     )
        # )

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
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)
    print(sql_query)
    return sql_query


def create_golden_table(reference_columns):
    try:
        result = db.session.execute(
                text(
                    f'''CREATE TABLE IF NOT EXISTS GOLDEN_TABLE (LIKE {TABLE_NAME} INCLUDING ALL);'''))
    except OperationalError as e:
        logger.critical(e)
        raise HTTPException(status_code=422)

def frequence_analisys_column(reference_column):
    try: #тут не дм инфра логс, просто тестилось на нем хддд
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

