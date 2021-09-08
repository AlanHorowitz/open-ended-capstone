from typing import List, Tuple, Dict
from model.metadata import Table, XrefTableData
from datetime import datetime
import random
import os

import psycopg2
from psycopg2.extras import DictCursor, DictRow
from psycopg2.extensions import connection, cursor

DEFAULT_INSERT_VALUES: Dict[str, object] = {
    "INTEGER": 98,
    "VARCHAR": "AAA",
    "FLOAT": 5.0,
    "DATE": "2021-02-11 12:52:47",
    "BOOLEAN": True,
    "TIMESTAMP": datetime(2020, 11, 11),
}


class GeneratorRequest:
    """ A structure of options passed to DataGenerator.generate """
    def __init__(
            self,
            table: Table,                   # Table to be generated
            n_inserts: int = 0,             # number of inserts to generate
            n_updates: int = 0,             # number of updates to generate
            link_parent: bool = False,      # If true, use n_inserts to describe how many records to insert
                                            # per parent key inserted in the same batch.
    ) -> None:
        self.table = table
        self.n_inserts = n_inserts
        self.n_updates = n_updates
        self.link_parent = link_parent


class DataGenerator:
    """
    The DataGenerator synthesizes sample data for tables described by Table and Column classes
    in WidgetsUnlimited.tables, and is tightly integrated with these classes' methods.
    It is invoked via its generate method with a GeneratorRequest and a batch_id (see below).  The cumulative product
    of the create_only is stored in postgresql.

    The DataGenerator supports tables linked together as a data model via foreign keys.
    In particular the following behaviors are supported:

        - Generate a table record with a random reference to an existing foreign key
        - Generate a table reference or references to a specific foreign key (parent/child create_only)
    """

    def __init__(self) -> None:
        """Initialize connection to dedicated schema in postgresql"""
        self._connection: connection = psycopg2.connect(
            dbname=os.environ["DATA_GENERATOR_DB"],
            host=os.environ["DATA_GENERATOR_HOST"],
            port=os.environ["DATA_GENERATOR_PORT"],
            user=os.environ["DATA_GENERATOR_USER"],
            password=os.environ["DATA_GENERATOR_PASSWORD"],
        )
        schema = os.environ["DATA_GENERATOR_SCHEMA"]
        self.cur: cursor = self._connection.cursor(cursor_factory=DictCursor)
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        self.cur.execute(f"SET SEARCH_PATH TO {schema};")
        self._connection.commit()

    def get_connection(self):
        return self._connection

    def add_tables(self, tables: List[Table]) -> None:
        """Create new postgresql tables"""
        for table in tables:
            self.cur.execute(f"DROP TABLE IF EXISTS {table.get_name()};")
            self.cur.execute(table.get_create_sql_postgres())
            self._connection.commit()

    def generate(
            self, generator_request: GeneratorRequest, batch_id: int = 0
    ) -> Tuple[List[Tuple], List[DictRow]]:
        """
        Insert and update the given numbers of synthesized records for a table.

        :param generator_request: class structure for generation options:
           - table - table object
           - n_inserts - number of records to insert
           - n_updates - number of (previously inserted) records to update
           - link_parent boolean - If true, use n_inserts to describe how many records to insert per parent key inserted
             in same batch.

        :param batch_id: Identifier used to group together multiple calls to generate, distinguishing current from prior
        transactions.

        :return: insert_records, update_records - lists of generated input and update rows, respectively.
        The psycopg2 DictRow class allows columns to be accessed directly by name.

        For update, a random sample of n_updates keys is generated and the corresponding records
        read.  A random selection of one the updatable string columns (as indicated in the metadata)
        is written back to the table with '_UPD' appended.

        For insert, n_insert dummy records are written to the table. The primary key is a sequence of
        incrementing integers, starting at the prior maximum value + 1.

        Foreign key references are resolved by random selection from previously generated keys in the
        referenced tables, unless link_parent=True, in which case, they are correlated with parent keys
        that are included in the current batch.
        """

        conn = self._connection
        cur: cursor = conn.cursor(cursor_factory=DictCursor)
        table = generator_request.table
        n_inserts = generator_request.n_inserts
        n_updates = generator_request.n_updates
        link_parent = generator_request.link_parent
        timestamp = datetime.now()

        if link_parent and not table.has_parent():
            raise Exception(f"Invalid Request. No parent for table {table.get_name()}")

        table_name = table.get_name()
        primary_key_column = table.get_primary_key()
        updated_at_column = table.get_updated_at()
        column_names = ",".join(table.get_column_names())

        cur.execute(f"SELECT COUNT(*), MAX({primary_key_column}) from {table_name};")
        result: DictRow = cur.fetchone()
        row_count = result[0]
        next_primary_key = 1 if result[1] is None else result[1] + 1

        update_records: List[DictRow] = []
        insert_records: List[Tuple] = []

        if n_updates > 0:

            n_updates = min(n_updates, row_count)
            update_keys = ",".join(
                [str(i) for i in random.sample(range(1, next_primary_key), n_updates)]
            )
            cur.execute(
                f"SELECT {column_names} from {table_name}"
                f" WHERE {primary_key_column} IN ({update_keys});"
            )

            update_records = cur.fetchall()

            for r in update_records:
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

            # for each cross referenced table, prefetch the needed columns and store
            # state data in XrefTableData helper object
            xref_dict: Dict[str, XrefTableData] = table.get_xref_dict()
            for xref_table_name, xref_data in xref_dict.items():
                xref_column_names = ",".join(xref_data.column_list)
                cur.execute(f"SELECT {xref_column_names} from {xref_table_name};")
                xref_data.result_set = cur.fetchall()
                xref_data.num_rows = len(xref_data.result_set)

            if not link_parent:

                for pk in range(next_primary_key, next_primary_key + n_inserts):
                    insert_records.append(
                        _get_new_row(
                            table=table,
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

            # if there is a parent_link, read the keys from the parent table that were inserted in
            # the current batch. Insert n_insert records per parent key in one operation.
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
                            _get_new_row(
                                table=table,
                                primary_key=next_primary_key,
                                parent_key=row[0],
                                batch_id=batch_id,
                                timestamp=timestamp,
                            )
                        )
                        next_primary_key += 1

                values_substitutions = ",".join(
                    ["%s"] * (n_inserts * len(linked_rs))
                )  # each %s holds one tuple row

                cur.execute(
                    f"INSERT INTO {table_name} ({column_names}) values {values_substitutions}",
                    insert_records,
                )

                """ Clear references in XrefTableData helper objects """
                for table_data in xref_dict.values():
                    table_data.result_set = []
                    table_data.num_rows = 0
                    table_data.next_random_row = 0

            conn.commit()

        return insert_records, update_records


def _get_new_row(
        table: Table,
        primary_key: int,
        parent_key: int = None,
        batch_id: int = None,
        timestamp: datetime = datetime.now(),
) -> Tuple:
    """
    Make a new row for the generator.  Substitutes cross references and default values from the column metadata.
    Plan to be extended to use value ranges and pick lists.

    :param primary_key:
    :param parent_key:
    :param batch_id:  batch identifier
    :param timestamp: insert/update time for record
    :return: a tuple, in column order, suitable for insertion
    """

    d: List[object] = []

    xref_dict: Dict[str, XrefTableData] = table.get_xref_dict()
    for table_data in xref_dict.values():
        table_data.next_random_row = random.randint(0, table_data.num_rows - 1)

    for col in table.get_columns():
        if col.has_default():
            d.append(col.get_default())
        elif col.is_primary_key():
            d.append(primary_key)
        elif col.is_batch_id():
            d.append(batch_id)
        elif col.is_inserted_at() or col.is_updated_at():
            d.append(timestamp)
        elif col.is_xref():
            xref_table = col.get_xref_table()
            row = xref_dict[xref_table].next_random_row
            value = xref_dict[xref_table].result_set[row][col.get_xref_column()]
            d.append(value)
        elif col.is_parent_key() and parent_key is not None:
            d.append(parent_key)
        else:
            d.append(DEFAULT_INSERT_VALUES[col.get_type()])

    return tuple(d)



