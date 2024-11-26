from io import BytesIO
import pandas

from sqlalchemy import create_engine, text

TABLE_NAME = "fuzzy"


def create_virtual_table(csvs: list[bytes]):
    engine = create_engine("sqlite+pysqlite:///database.sqlite", echo=True)
    print("Engine created")

    conn = engine.connect()
    print("Engine connection established")

    conn.execute(
        text(
            f"CREATE VIRTUAL TABLE {TABLE_NAME} USING fts4({','.join(get_table_headers())});"
        )
    )
    print("Virtual table created")

    for index, csv in enumerate(csvs):
        print(f"Processing file #{index + 1}...")
        dataframe = pandas.read_csv(BytesIO(bytearray(csv)), low_memory=False)
        if_exists = "replace" if index == 0 else "append"
        dataframe.to_sql(TABLE_NAME, conn, if_exists=if_exists, index=False)
        print(f"File #{index + 1} processed")

    conn.commit()
    print("Изменения сохранены")


def get_table_headers():
    engine = create_engine("sqlite+pysqlite:///database.sqlite", echo=True)
    print("Engine created")

    conn = engine.connect()
    print("Engine connection established")

    result = conn.execute(text(f"PRAGMA table_info({TABLE_NAME});"))
    return list(map(lambda row: row[1], result))


def fuzzy_group():
    engine = create_engine("sqlite+pysqlite:///database.sqlite", echo=True)
    print("Engine created")

    conn = engine.connect()
    print("Engine connection established")

    result = conn.execute(
        text(
            f"SELECT * FROM {TABLE_NAME} WHERE client_fio_full MATCH 'Евдунов Меджид Кузарович';"
        )
    )
    return list(result)
