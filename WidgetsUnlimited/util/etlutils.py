import random
from typing import Tuple, Dict, List
from datetime import datetime

from psycopg2.extras import DictCursor, DictRow
from psycopg2.extensions import connection, cursor

from util.sqltypes import Table, Column

# ETL history table records a row for each extract.  The prior to_timestamp is used
# to limit source system reads to unseen records.

ETL_HISTORY_CREATE_MYSQL = """
CREATE TABLE IF NOT EXISTS etl_history
( id INT NOT NULL AUTO_INCREMENT,
  table_name VARCHAR(80),
  from_timestamp TIMESTAMP(6), 
  to_timestamp TIMESTAMP(6),
  n_inserts INT,
  n_updates INT,
  PRIMARY KEY (id)
);
"""

DATE_LOW = datetime(1980, 1, 1)  # timestamp preceding all simulations


def create_table(conn: connection, create_sql: str) -> None:

    cur: cursor = conn.cursor()
    cur.execute(create_sql)
    conn.commit()
    cur.close()


def create_source_tables(source_connection: connection, tables: List[Table]) -> None:
    """Create tables for postgres source system."""

    for table in tables:
        create_table(source_connection, table.get_create_sql_postgres())


def create_target_tables(target_connection: connection, tables: List[Table]) -> None:
    """Create tables for mySQL target system.  Include etl_history table."""

    for table in tables:
        create_table(target_connection, table.get_create_sql_mysql())

    create_table(target_connection, ETL_HISTORY_CREATE_MYSQL)


def load_source_table(
    conn: connection,
    table: Table,
    n_inserts: int,
    n_updates: int,
    timestamp: datetime = datetime.now(),
) -> Tuple[int, int]:
    """
    Insert and update the given numbers of sythesized records to a table.

    For update, a random sample of n_updates keys is generated and the corresponding records 
    read.  A random selection of one the string columns of the table is written back to the table with 
    '_UPD' appended. 

    For insert, n_insert dummy records are written to the table. The primary key is a sequence of
    incrementing integers, starting at the prior maximum value + 1.

    Args:
        conn: a psycopg2 db connection.
        table: a RetailDW.sqltypes.Table object to be loaded.
        n_inserts: quantity to insert.
        n_updates: quantity to update

    Returns:
        A tuple, (n_inserted, n_updated), representing the number of rows inserted and updated. 
        In the future these may differ from the input values.
    """
    cur: cursor = conn.cursor(cursor_factory=DictCursor)
    table.preload(cur)

    table_name = table.get_name()
    primary_key_column = table.get_primary_key()
    updated_at_column = table.get_updated_at()
    column_names = ",".join(table.get_column_names())  # for SELECT statements

    cur.execute(f"SELECT COUNT(*), MAX({primary_key_column}) from {table_name};")
    result: DictRow = cur.fetchone()
    row_count = result[0]
    next_key = 1 if result[1] == None else result[1] + 1

    if n_updates > 0:

        n_updates = min(n_updates, row_count)
        update_keys = ",".join(
            [str(i) for i in random.sample(range(1, next_key), n_updates)]
        )
        cur.execute(
            f"SELECT {column_names} from {table_name}"
            f" WHERE {primary_key_column} IN ({update_keys});"
        )

        result = cur.fetchall()

        for r in result:
            key_value = r[primary_key_column]
            update_column = table.get_update_column().get_name()
            update_column_value = r[update_column]
            cur.execute(
                f"UPDATE {table_name}"
                f" SET {update_column} = concat('{update_column_value}', '_UPD'),"
                f" {updated_at_column} = %s"
                f" WHERE {primary_key_column} = {key_value}",
                [timestamp],
            )

        conn.commit()

    if n_inserts > 0:

        insert_records = []
        for pk in range(next_key, next_key + n_inserts):
            insert_records.append(table.getNewRow(pk, timestamp))

        values_substitutions = ",".join(
            ["%s"] * n_inserts
        )  # each %s holds one tuple row

        cur.execute(
            f"INSERT INTO {table_name} ({column_names}) values {values_substitutions}",
            insert_records,
        )

        conn.commit()

    table.postload()

    return n_inserts, n_updates


def etl_history_get_last_update(trg_cursor: cursor, table_name: str) -> datetime:
    """
    Return the latest updated timestamp seen for the table.
    """
    trg_cursor.execute(
        "SELECT MAX(to_timestamp) FROM etl_history WHERE table_name = %s;",
        (table_name,),
    )
    from_time = trg_cursor.fetchone()[0]
    return from_time if from_time else DATE_LOW


def etl_history_insert(
    trg_cursor: cursor,
    table_name: str,
    n_inserts: int,
    n_updates: int,
    from_timestamp: datetime,
    to_timestamp: datetime,
) -> None:

    trg_cursor.execute(
        "INSERT INTO etl_history "
        "(table_name, n_inserts, n_updates,from_timestamp, to_timestamp) "
        "VALUES (%s, %s, %s, %s, %s);",
        (table_name, n_inserts, n_updates, from_timestamp, to_timestamp),
    )


def extract_table_to_target(
    src_conn: connection, trg_conn: connection, table: Table
) -> Tuple[int, int, datetime, datetime]:
    """
    Extract records more recent than the prior update from the source system and UPSERT to the target system.

    Records are read, written and commited in batches of 1000.  The upsert is performed unsing MySQL REPLACE. 
    Insert and update counts are derived from the input record count and row return count. 

    Args:
        conn: a mysql.connector db connection.
        table: a RetailDW.sqltypes.Table object to be loaded.
        
    Returns:
        A tuple, (n_inserted,  # records inserted
                  n_updated,   # records updated
                  from_time,   #   
                  to_time      # from_time < update times of records processed <= to_time  
    """
    BATCH_SIZE = 1000

    n_inserts = 0
    n_updates = 0

    table_name = table.get_name()
    column_names = ",".join(table.get_column_names())  # for SELECT statements
    values_substitutions = ",".join(["%s"] * len(table.get_column_names()))

    # use server-side cursor with batch size
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

    # Populate new row of ETL_history
    trg_cursor.execute(f"SELECT MAX({table.get_updated_at()}) FROM {table_name};")
    to_timestamp = trg_cursor.fetchone()[0]
    etl_history_insert(
        trg_cursor, table.get_name(), n_inserts, n_updates, from_timestamp, to_timestamp
    )

    trg_conn.commit()

    src_cursor.close()
    trg_cursor.close()

    return (n_inserts, n_updates, from_timestamp, to_timestamp)
