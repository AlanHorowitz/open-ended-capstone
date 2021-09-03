from typing import List
from util.sqltypes import Table
from datetime import datetime
from .table_transaction import TableTransaction
import random
import os

import psycopg2
from psycopg2.extras import DictCursor, DictRow
from psycopg2.extensions import connection, cursor


class DataGenerator:
    def __init__(self) -> None:
        self.connection: connection = psycopg2.connect(
            dbname=os.environ["DATA_GENERATOR_DB"],
            host=os.environ["DATA_GENERATOR_HOST"],
            port=os.environ["DATA_GENERATOR_PORT"],
            user=os.environ["DATA_GENERATOR_USER"],
            password=os.environ["DATA_GENERATOR_PASSWORD"],
        )
        schema = os.environ["DATA_GENERATOR_SCHEMA"]
        self.cur: cursor = self.connection.cursor(cursor_factory=DictCursor)
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        self.cur.execute(f"SET SEARCH_PATH TO {schema};")
        self.connection.commit()

    def close():
        pass

    def get_connection(self):
        return self.connection

    def add_tables(self, tables: List[Table]) -> None:
        for table in tables:
            self.cur.execute(f"DROP TABLE IF EXISTS {table.get_name()};")
            self.cur.execute(table.get_create_sql_postgres())
            self.connection.commit()

    def generate(self, table_transaction: TableTransaction, batch_id: int = 0) -> List[DictRow]:
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
        table = table_transaction.table
        n_inserts = table_transaction.n_inserts
        n_updates = table_transaction.n_updates
        link_parent = table_transaction.link_parent
        timestamp = datetime.now()

        if link_parent and not table.has_parent():
            raise Exception(f"Invalid Request. No parent for table {table.get_name()}")

        table.preload(cur)

        table_name = table.get_name()
        primary_key_column = table.get_primary_key()
        updated_at_column = table.get_updated_at()
        column_names = ",".join(table.get_column_names())  # for SELECT statements

        cur.execute(f"SELECT COUNT(*), MAX({primary_key_column}) from {table_name};")
        result: DictRow = cur.fetchone()
        row_count = result[0]
        next_key = 1 if result[1] == None else result[1] + 1

        update_records: List[DictRow] = []
        insert_records: List[DictRow] = []

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
                    f" {updated_at_column} = %s,"
                    f" batch_id = %s"
                    f" WHERE {primary_key_column} = %s",
                    [r[update_column], timestamp, batch_id, r[primary_key_column]],
                )

            conn.commit()

        if n_inserts > 0:
            # if there is a parent_link, read the keys from the parent table
            # modified already in the current batch. Insert n_insert records per parent key,
            # using parent_key attribute of column (foreign key v. parent key)

            # Generate n_inserts inserts
            if not link_parent:

                for pk in range(next_key, next_key + n_inserts):
                    insert_records.append(
                        table.getNewRow(
                            primary_key=pk,
                            parent_key=None,
                            batch_id=batch_id,
                            timestamp=timestamp,
                        )
                    )

                values_substitutions = ",".join(
                    ["%s"] * n_inserts
                )  # each %s holds one tuple row

                cur.execute(
                    f"INSERT INTO {table_name} ({column_names}) values {values_substitutions}",
                    insert_records,
                )

            # Generate n_inserts per parent record in batch
            else:
                if table.has_parent():
                    cur.execute(
                        f"SELECT {table.get_parent_key()}"
                        f" FROM   {table.get_parent_table()}"
                        f" WHERE batch_id = {batch_id};"
                    )
                    linked_rs = cur.fetchall()
                    if len(linked_rs) == 0:
                        raise Exception(
                            "Invalid request.  No parent records discovered in batch"
                        )
                else:
                    raise Exception("Invalid request.  Table has no parent")

                for row in linked_rs:
                    for _ in range(n_inserts):
                        insert_records.append(
                            table.getNewRow(
                                primary_key=next_key,
                                parent_key=row[0],
                                batch_id=batch_id,
                                timestamp=timestamp,
                            )
                        )
                        next_key += 1

                values_substitutions = ",".join(
                    ["%s"] * (n_inserts * len(linked_rs))
                )  # each %s holds one tuple row

                cur.execute(
                    f"INSERT INTO {table_name} ({column_names}) values {values_substitutions}",
                    insert_records,
                )
            conn.commit()

        table.postload()

        # TODO clean batch_id, not for downstream use.
        return insert_records, update_records
