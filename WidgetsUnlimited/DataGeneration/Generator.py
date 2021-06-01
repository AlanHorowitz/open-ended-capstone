from re import U
from .OperationalSystem import OperationalSystem
from typing import List, Tuple
from collections import namedtuple
from util.sqltypes import Table, Column
from datetime import datetime
import random

import psycopg2
from psycopg2.extras import DictCursor, DictRow
from psycopg2.extensions import connection, cursor
from util.etlutils import create_table


GeneratorItem = namedtuple("GeneratorItem", ["table_object", "n_inserts", "n_updates"])

class Generator():

    def __init__(self) -> None:
        self.connection: connection = psycopg2.connect(
        "dbname=retaildw host=172.17.0.1 user=user1 password=user1"
        )
        self.cur: cursor = self.connection.cursor(cursor_factory=DictCursor)
        self.cur.execute("CREATE SCHEMA GENERATOR;")
        self.cur.execute("SET SEARCH_PATH TO GENERATOR;")
        self.connection.commit()
        pass

    def run(self, batch : List[GeneratorItem]) -> None:
        for b in batch:
            table : Table = b.table_object
            opSystem : OperationalSystem = table.getOperationalSystem()
            i_rows, u_rows  = self.generate(b.table_object, b.n_inserts, b.n_updates, datetime.now())
            print(b)
            opSystem.insert(table, i_rows)
            opSystem.update(table, u_rows)
            # opSystem open
            # while get next update (generator function) looping factor
                # save local
                # call opsystemUpdate
            # while get next insert (generator function)
                # save local
                # call opsystemInsert
            # opsystem close
        pass

    def close():
        pass

    def add_tables(self, tables : List[Table]) -> None:
        # create all the postgres tables
        for table in tables:
            create_table(self.connection, table.get_create_sql_postgres())  

    def generate(self,        
        table: Table,
        n_inserts: int,
        n_updates: int,
        timestamp: datetime = datetime.now(),
    ) -> List[DictRow]:
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

        conn = self.connection
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

        update_records : List[DictRow] = []

        if n_updates > 0:

            n_updates = min(n_updates, row_count)
            update_keys = ",".join(
                [str(i) for i in random.sample(range(1, next_key), n_updates)]
            )
            cur.execute(
                f"SELECT {column_names} from {table_name}"
                f" WHERE {primary_key_column} IN ({update_keys});"
            )

            update_records = cur.fetchall()

            for r in update_records:
                key_value = r[primary_key_column]
                update_column = table.get_update_column().get_name()
                r[update_column] = r[update_column] + "_UPD"
                cur.execute(
                    f"UPDATE {table_name}"
                    f" SET {update_column} = %s,"
                    f" {updated_at_column} = %s"
                    f" WHERE {primary_key_column} = %s",
                    [r[update_column], timestamp, r[primary_key_column]],
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

        return insert_records, update_records

