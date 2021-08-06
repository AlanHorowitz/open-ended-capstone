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
        table_name = table.get_name()
        column_names = ",".join(table.get_column_names())
        values_substitutions = ",".join(["%s"] * len(table.get_column_names()))     
        drop = "DROP TABLE IF EXISTS " + table_name + ";"
        run_sql(ms_connection, drop)
        run_sql(ms_connection, table.get_create_sql_mysql())

        # simple low tech read/write

        src_cursor = pg_conn.cursor()
        trg_cursor = ms_connection.cursor()
        
        src_cursor.execute(
            f"SELECT {column_names} from {table_name};")

        r = src_cursor.fetchall()
        if len(r) == 0:
            break
        
        trg_cursor.executemany(
            f"INSERT INTO {table_name} ({column_names}) values ({values_substitutions})",
            r,
        )
        ms_connection.commit()
