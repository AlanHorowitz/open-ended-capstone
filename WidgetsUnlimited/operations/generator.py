from typing import List, Tuple, Dict
from model.metadata import Table, XrefTableData
from datetime import datetime
import random
from random import choice
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
    """A structure of options passed to DataGenerator.generate"""

    def __init__(
            self,
            table: Table,  # Table to be generated
            n_inserts: int = 0,  # number of inserts to generate
            n_updates: int = 0,  # number of updates to generate
            link_parent: bool = False,  # If true, use n_inserts to describe how many records to insert
            # per parent key inserted in the same batch.
    ) -> None:
        self.table = table
        self.n_inserts = n_inserts
        self.n_updates = n_updates
        self.link_parent = link_parent


class DataGenerator:
    """
    The DataGenerator synthesizes sample data for entities modeled by Table classes in package
    WidgetsUnlimited.model. When processing a GeneratorRequest it applies inputs and updates to a
    postgres database (maintaining cumulative state) and returns inputs and updates to the caller,
    which are then routed to the associated operational system.

    Example:
         generator = DataGenerator()
         request = GeneratorRequest(PRODUCT, n_inserts=50, n_updates=5)
         insert, updates = generator.generate(request, 1)

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
        Synthesize insert and update records for a table. apply these changes to the generator postgres and return
        to the caller for routing to an operational system.

        :param generator_request: class structure for generation options:
           - table - table object
           - n_inserts - number of records to insert
           - n_updates - number of (previously inserted) records to update
           - link_parent boolean - If true, use n_inserts to describe how many records to insert per parent key inserted
             in same batch.

        :param batch_id: Identifier used to group together multiple calls to generate, distinguishing current from prior
        generator_requests.

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
        bridge = table.get_bridge()

        bridge_table = bridge.bridge_table if bridge else None
        bridge_table_name = bridge_table.get_name() if bridge else None
        bridge_column_names = f"{table.get_primary_key()}, {bridge.partner_key}, " \
                              f"{bridge_table.get_updated_at()}, batch_id" if bridge else None

        cur.execute(f"SELECT COUNT(*), MAX({primary_key_column}) from {table_name};")
        result: DictRow = cur.fetchone()
        row_count = result[0]
        next_primary_key = 1 if result[1] is None else result[1] + 1

        update_rows: List[DictRow] = []
        insert_rows: List[Tuple] = []

        # for each cross referenced table, prefetch the needed columns and store
        # state data in XrefTableData helper object
        xref_dict: Dict[str, XrefTableData] = table.get_xref_dict()
        for xref_table_name, xref_data in xref_dict.items():
            xref_column_names = ",".join(xref_data.column_list)
            cur.execute(f"SELECT {xref_column_names} from {xref_table_name};")
            xref_data.result_set = cur.fetchall()
            xref_data.num_rows = len(xref_data.result_set)

        if n_updates > 0:

            n_updates = min(n_updates, row_count)
            update_keys = ",".join(
                [str(i) for i in random.sample(range(1, next_primary_key), n_updates)]
            )
            cur.execute(
                f"SELECT {column_names} from {table_name}"
                f" WHERE {primary_key_column} IN ({update_keys});"
            )

            update_rows = cur.fetchall()

            for u_row in update_rows:
                update_column = table.get_update_column().get_name()
                u_row[update_column] = u_row[update_column] + "_UPD"
                cur.execute(
                    f"UPDATE {table_name}"
                    f" SET {update_column} = %s,"
                    f" {updated_at_column} = %s,"
                    f" batch_id = %s"
                    f" WHERE {primary_key_column} = %s",
                    [u_row[update_column], timestamp, batch_id, u_row[primary_key_column]],
                )

                if bridge:

                    partner_rows = xref_dict[bridge.partner_table].result_set
                    bridge_rows = xref_dict[bridge_table_name].result_set
                    if choice([True, False]):
                        # True: get a row from bridge and delete it (where PK) and delete it.
                        # Loop through bridge_table, delete the first one that exists
                        for b_row in bridge_rows:
                            if b_row[primary_key_column] == u_row[primary_key_column]:
                                # delete where primary_key and partner_key
                                break
                    else:
                        # False: select a supplier not already connected with pkey and add
                        # Collect all the suppliers already in the bridge table.  Then take the first supplier found
                        # that is not in the list

                        new_partner_key = None
                        partner_keys = [b_row[bridge.partner_key] for b_row in bridge_rows
                                        if b_row[primary_key_column] == u_row[primary_key_column]]

                        for p_row in partner_rows:
                            if p_row[bridge.partner_key] not in partner_keys:
                                new_partner_key = p_row[bridge.partner_key]
                                break
                        if new_partner_key:
                            new_row = [(u_row[primary_key_column], new_partner_key,
                                        timestamp, batch_id)]
                            _insert_rows(cur, bridge_table_name, bridge_column_names, new_row)

            print(f"DataGenerator: {len(update_rows)} records updated for {table_name}")

            conn.commit()

        if n_inserts > 0:

            if not link_parent:
                for pk in range(next_primary_key, next_primary_key + n_inserts):
                    insert_rows.append(
                        _create_new_row(
                            table=table,
                            primary_key=pk,
                            parent_key=None,
                            batch_id=batch_id,
                            timestamp=timestamp,
                        )
                    )
                insert_count = _insert_rows(cur, table_name, column_names, insert_rows)

            # if there is a parent_link, read the keys from the parent table that were inserted in
            # the current batch. Insert n_insert records per parent key in one operation.
            else:
                if table.has_parent():
                    cur.execute(
                        f"SELECT {table.get_parent_key()}"
                        f" FROM   {table.get_parent_table()}"
                        f" WHERE batch_id = {batch_id};"
                    )
                    parent_rows = cur.fetchall()
                    if len(parent_rows) == 0:
                        raise Exception(
                            "Invalid request.  No parent records discovered in batch"
                        )
                else:
                    raise Exception("Invalid request.  Table has no parent")

                for p_row in parent_rows:
                    for _ in range(n_inserts):
                        insert_rows.append(
                            _create_new_row(
                                table=table,
                                primary_key=next_primary_key,
                                parent_key=p_row[0],
                                batch_id=batch_id,
                                timestamp=timestamp,
                            )
                        )
                        next_primary_key += 1

                insert_count = _insert_rows(cur, table_name, column_names, insert_rows)

            """ Add bridge table entries """
            if bridge:
                partner_rows = xref_dict[bridge.partner_table].result_set
                if len(partner_rows) >= bridge.inserts:
                    bridge_insert_rows = []
                    for i in bridge_insert_rows:
                        u_row = random.sample(range(len(partner_rows)), k=bridge.inserts)
                        for n in u_row:
                            insert_rows.append((i[0], partner_rows[n][bridge.partner_key],
                                                timestamp, batch_id))

                    bridge_count = _insert_rows(cur, bridge_table_name, bridge_column_names, bridge_insert_rows)
                    print(f"DataGenerator: {bridge_count} records inserted for {bridge_table_name}")

            print(f"DataGenerator: {insert_count} records inserted for {table_name}")
            conn.commit()

        return insert_rows, update_rows


def _create_new_row(
        table: Table,
        primary_key: int,
        parent_key: int = None,
        batch_id: int = None,
        timestamp: datetime = datetime.now(),
) -> Tuple:
    """
    Create a new row for a table.  Substitute cross references and set default values using the column metadata.

    :param table: Table metadata object
    :param primary_key: value of primary key
    :param parent_key:  value of parent key (where table has parent)
    :param batch_id:  batch identifier
    :param timestamp: insert/update time for record
    :return: a tuple representing a row, in column order, suitable for database insertion
    """

    row = []

    xref_dict: Dict[str, XrefTableData] = table.get_xref_dict()
    for table_data in xref_dict.values():
        table_data.next_random_row = -1 if table_data.num_rows == 0 else random.randint(0, table_data.num_rows - 1)

    for col in table.get_columns():
        if col.has_default():
            row.append(col.get_default())
        elif col.is_primary_key():
            row.append(primary_key)
        elif col.is_batch_id():
            row.append(batch_id)
        elif col.is_inserted_at() or col.is_updated_at():
            row.append(timestamp)
        elif col.is_xref():
            xref_table = col.get_xref_table()
            xref_row = xref_dict[xref_table].next_random_row
            if xref_row == -1:
                value = -1
            else:
                value = xref_dict[xref_table].result_set[xref_row][col.get_xref_column()]
            row.append(value)
        elif col.is_parent_key() and parent_key is not None:
            row.append(parent_key)
        else:
            row.append(DEFAULT_INSERT_VALUES[col.get_type()])

    return tuple(row)


def _insert_rows(cur, table_name, column_names, rows) -> int:
    values_substitutions = ",".join(["%s"] * len(rows))
    cur.execute(
        f"INSERT INTO {table_name} ({column_names}) values {values_substitutions}",
        rows
    )
    return cur.rowcount
