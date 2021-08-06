from util.sqltypes import Table
from typing import List
import os 

import mysql.connector
from mysql.connector import connect
from sqlalchemy import create_engine, event

import pandas as pd

os.environ['DATA_GENERATOR_DB'] = 'postgres'
os.environ['DATA_GENERATOR_HOST'] = '172.17.0.2'
os.environ['DATA_GENERATOR_PORT'] = '5432'
os.environ['DATA_GENERATOR_USER'] = 'postgres'
os.environ['DATA_GENERATOR_PASSWORD'] = 'postgres'
os.environ['DATA_GENERATOR_SCHEMA'] = 'test'

postgres_str = ('postgresql+psycopg2://{username}:{password}@{ipaddress}:{port}/{dbname}'
.format(username=os.environ['DATA_GENERATOR_USER'],
password=os.environ['DATA_GENERATOR_PASSWORD'],
ipaddress=os.environ['DATA_GENERATOR_HOST'],
port=os.environ['DATA_GENERATOR_PORT'],
dbname=os.environ['DATA_GENERATOR_PASSWORD'] ))

cnx = create_engine(postgres_str)

@event.listens_for(cnx, "connect", insert=True)    
def set_search_path(dbapi_connection, connection_record):
    print("ANHANHANH" * 10)
    existing_autocommit = dbapi_connection.autocommit
    dbapi_connection.autocommit = True
    cursor = dbapi_connection.cursor()
    cursor.execute("SET SESSION search_path='test'")
    cursor.close()
    dbapi_connection.autocommit = existing_autocommit


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

def write_parquet_warehouse_tables(tables : List[Table]):    

    for table in tables:
        table_name = table.get_name()
        column_names = ",".join(table.get_column_names())
                
        sql = f"SELECT {column_names} from {table_name};"
                
        df = pd.read_sql_query(sql, con=cnx)
        print(df.head(1))

    
    
