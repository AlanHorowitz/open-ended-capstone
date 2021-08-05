from util.sqltypes import Table
from typing import List

import mysql.connector
from mysql.connector import connect

ms_connection = connect(
        host="localhost",
        user="alan",
        password="alan",
        database="edw",
        charset="utf8",
    )

def run_sql(conn, create_sql: str) -> None:
    cur = conn.cursor()
    cur.execute(create_sql)
    conn.commit()
    cur.close()

def create_and_copy_warehouse_tables(pg_conn, tables : List[Table]):
    for table in tables:
        drop = "DROP TABLE IF EXISTS " + table.get_name() + ";"
        run_sql(ms_connection, drop)
        run_sql(ms_connection, table.get_create_sql_mysql())
        # simple low tech read/write


        src_cursor: cursor = src_conn.cursor(name="pgread")
    src_cursor.arraysize = BATCH_SIZE

    trg_cursor: cursor = trg_conn.cursor()
    from_timestamp = etl_history_get_last_update(trg_cursor, table.get_name())

    src_cursor.execute(
        f"SELECT {column_names} from {table_name} "
        f"WHERE {table.get_updated_at()} > %s;",
        (from_timestamp,),
    )

    while True:

        r = src_cursor.fetchmany()
        if len(r) == 0:
            break

        trg_cursor.executemany(
            f"REPLACE INTO {table_name} ({column_names}) values ({values_substitutions})",
            r,
        )

        # updates registers 2 affected rows (DELETE/INSERT)
        n_inserts += 2 * len(r) - trg_cursor.rowcount
        n_updates += trg_cursor.rowcount - len(r)
        trg_conn.commit()
