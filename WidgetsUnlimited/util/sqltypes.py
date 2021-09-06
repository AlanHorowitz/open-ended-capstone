import random
from typing import List, Tuple, Dict, Any
from psycopg2.extensions import cursor

from datetime import datetime

DEFAULT_INSERT_VALUES: Dict[str, object] = {
    "INTEGER": 98,
    "VARCHAR": "AAA",
    "FLOAT": 5.0,
    "DATE": "2021-02-11 12:52:47",
    "BOOLEAN": True,
    "TIMESTAMP": datetime(2020, 11, 11),
}


class Column:
    """Database column metadata used for DDL and data generation"""
    def __init__(
        self,
        column_name: str,               # column name
        column_type: str,               # (INTEGER, VARCHAR, FLOAT, DATE, BOOLEAN, TIMESTAMP)
        column_length= None,            # optional column length (e.g. 200 for VARCHAR(200))
        primary_key: bool = False,      # column is primary key
        inserted_at: bool = False,      # column is the inserted_at column
        updated_at: bool = False,       # column is the updated_at column
        batch_id: bool = False,         # column is batch_id column
        update: bool = False,           # column is eligible for update by generator
        xref_table: str = "",           # name of foreign reference table
        xref_column: str = "",          # column in reference table to use for foreign key
        parent_table: str = "",         # parent table with one to many relationship with child table
        parent_key: str = "",           # primary key in parent table
        default: Any = None,            # default value for column
    ):

        self._name = column_name
        self._type = column_type
        self._length = column_length
        self._primary_key = primary_key
        self._inserted_at = inserted_at
        self._updated_at = updated_at
        self._batch_id = batch_id
        self._update = update
        self._xref_table = xref_table
        self._xref_column = xref_column
        self._parent_table = parent_table
        self._parent_key = parent_key
        self._default = default

    def get_definition_text(self, db_types_dict) -> str:
        """
        Get database-specific DDL text for the column to be used in CREATE TABLE.  Optionally include a length
        parameter (e.g. my_column VARCHAR(200) or my_column VARCHAR).

        :param db_types_dict: Mapping from Column.column_type to tuple (db-specific SQL type, optional type length)
        :return: Native DDL text (postgres or mysql) for column.

        """
        length_parameter = ""
        length = self.get_type_length()
        db_type = db_types_dict[self.get_type()][0]
        db_default_length = db_types_dict[self.get_type()][1]

        # when explicit or default length found, append within parentheses
        if length or db_default_length:
            length_parameter = "(" + (str(length) if length else db_default_length) + ")"

        return self.get_name() + " " + db_type + length_parameter

    def get_name(self) -> str:
        return self._name

    def get_type(self) -> str:
        return self._type

    def get_type_length(self) -> str:
        return self._length

    def is_primary_key(self) -> bool:
        return self._primary_key

    def is_inserted_at(self) -> bool:
        return self._inserted_at

    def is_updated_at(self) -> bool:
        return self._updated_at

    def is_batch_id(self) -> bool:
        return self._batch_id

    def get_xref_table(self) -> str:
        return self._xref_table

    def get_xref_column(self) -> str:
        return self._xref_column

    def is_xref(self) -> bool:
        return self._xref_table != "" and self._xref_column != ""

    def get_parent_table(self) -> str:
        return self._parent_table

    def get_parent_key(self) -> str:
        return self._parent_key

    def is_parent_key(self) -> bool:
        return self._parent_table != "" and self._parent_key != ""

    def get_default(self) -> Any:
        return self._default

    def has_default(self) -> bool:
        return self._default is not None

    def can_update(self) -> bool:
        return self._update


class Table:
    """Database Table metadata used for DDL and data generation"""
# Note: Source system tables in RetailDW must have a single column integer primary key
# and at least one VARCHAR column.
    def __init__(self, name: str, *columns: Column, create_only=False, batch_id=True):
        """
        Initialize a Table object:
            - Prepare the class for use by the DataGenerator
            - Enforce constraints on Column attributes



        :param name: Table name
        :param columns: Column objects comprising Table
        :param create_only: Use Table only for create table and metadata, not data generation
        :param batch_id: Include a batch_id column with table
        """

        self._name = name
        self._create_only = create_only
        self._batchId = batch_id
        self._columns = [col for col in columns]
        if self._batchId:
            self._columns.append(Column("batch_id", "INTEGER", batch_id=True))
        primary_keys = [col.get_name() for col in columns if col.is_primary_key()]
        inserted_ats = [col.get_name() for col in columns if col.is_inserted_at()]
        updated_ats = [col.get_name() for col in columns if col.is_updated_at()]
        parent_keys = [col for col in columns if col.is_parent_key()]

        if len(primary_keys) != 1:
            raise Exception("Generator requires exactly one primary key.")
        self._primary_key = primary_keys[0]

        if not self._create_only:
            if (len(inserted_ats), len(updated_ats)) != (1, 1):
                raise Exception(
                    "Generator requires exactly one inserted_at and updated_at column"
                )

            self._inserted_at = inserted_ats[0]
            self._updated_at = updated_ats[0]

            if len(parent_keys) > 1:
                raise Exception("Generator may accept at most one parent key")
            elif len(parent_keys) == 1:
                self._parent_key = parent_keys[0].get_parent_key()
                self._parent_table = parent_keys[0].get_parent_table()
            else:
                self._parent_key = ""
                self._parent_table = ""

            self._update_columns = [
                col for col in columns if col.can_update()
            ]  # restrict to VARCHAR update
            if len(self._update_columns) == 0:
                raise Exception("Need at least one VARCHAR for update")

            self._init_xref_dict()



    #
    #  Database table creation methods
    #
    def get_create_sql_mysql(self) -> str:
        """ Returns SQL to create table for this class in postgresql """

        # type mappings and default lengths for mysql
        mysql_types_dict = {
            "INTEGER": ("INT", None),
            "VARCHAR": ("VARCHAR", "80"),
            "FLOAT": ("DOUBLE", None),
            "DATE": ("DATE", None),
            "BOOLEAN": ("TINYINT", "1"),
            "TIMESTAMP": ("TIMESTAMP", "6"),
        }
        return self.get_create_sql(mysql_types_dict)

    def get_create_sql_postgres(self) -> str:
        """ Returns SQL to create table for this class in postgresql """

        # type mappings and default lengths for postgres
        postgres_types_dict = {
            "INTEGER": ("INTEGER", None),
            "VARCHAR": ("VARCHAR", "80"),
            "FLOAT": ("FLOAT", "11"),
            "DATE": ("DATE", None),
            "BOOLEAN": ("BOOLEAN", None),
            "TIMESTAMP": ("TIMESTAMP", None),
        }
        return self.get_create_sql(postgres_types_dict)

    def get_create_sql(self, definition_dict):
        """
        Compose the text for CREATE TABLE to be executed on postgresql or mysql

        :param definition_dict: Mappings from Column.column_type to native SQL name plus optional column length
        :return CREATE TABLE statement in print friendly format.
        """

        create_table = f"CREATE TABLE IF NOT EXISTS {self.get_name()} ( \n"
        columns = "\n".join([col.get_definition_text(definition_dict) + "," for col in self.get_columns()])
        primary_key = f"\nPRIMARY KEY ({self.get_primary_key()}));"

        return create_table + columns + primary_key

    #
    # Generation lifecycle methods
    #
    def pre_load(self, cur: cursor) -> None:
        """Load foreign key tables for valid references when generating records.  Assume
        these tables fit in memory for now.  Update the xrefDict with result set and count.
        """
        for table_name, table_data in self._xrefDict.items():
            column_names = ",".join(table_data.column_list)
            cur.execute(f"SELECT {column_names} from {table_name};")
            table_data.result_set = cur.fetchall()
            table_data.num_rows = len(table_data.result_set)

    def post_load(self) -> None:
        """Clear references to xref result set"""
        for table_data in self._xrefDict.values():
            table_data.result_set = []
            table_data.num_rows = 0
            table_data.next_random_row = 0

    def get_new_row(
        self,
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
        self._setXrefTableRows()

        for col in self.get_columns():
            if col.has_default():
                d.append(col.get_default())
            elif col.is_primary_key():
                d.append(primary_key)
            elif col.is_batch_id():
                d.append(batch_id)
            elif col.is_inserted_at() or col.is_updated_at():
                d.append(timestamp)
            elif col.is_xref():
                d.append(self._getXrefValue(col))
            elif col.is_parent_key() and parent_key is not None:
                d.append(parent_key)
            else:
                d.append(DEFAULT_INSERT_VALUES[col.get_type()])

        return tuple(d)

    class XrefTableData:
        """helper object for xref data"""

        def __init__(self):
            self.column_list = []
            self.result_set = []
            self.next_random_row = 0
            self.num_rows = 0

    def _init_xref_dict(self) -> None:
        """ Create a dictionary mapping xref table names to helper object used to manage cross table lookups """

        xref_dict: Dict[str, Table.XrefTableData] = {}

        for col in self.get_columns():
            if col.is_xref():
                xref_table, xref_column = col.get_xref_table(), col.get_xref_column()
                if xref_table in xref_dict:
                    xref_dict[xref_table].column_list.append(xref_column)
                else:
                    xref_dict[xref_table] = Table.XrefTableData()
                    xref_dict[xref_table].column_list.append(xref_column)

        self._xrefDict = xref_dict


    def _setXrefTableRows(self):
        """Update the Xref dictionary with the current random rows for each table."""
        for table_data in self._xrefDict.values():
            table_data.next_random_row = random.randint(0, table_data.num_rows - 1)

    def _getXrefValue(self, col: Column) -> str:
        """Return the column's value from the current random DictRow."""

        row = self._xrefDict[col._xref_table].next_random_row
        value = self._xrefDict[col._xref_table].result_set[row][col._xref_column]

        return value

    #
    # Aggregate column methods
    #
    def get_columns(self) -> List[Column]:
        return self._columns

    def get_column_names(self) -> List[str]:
        return [col.get_name() for col in self._columns]

    def get_update_column(self) -> Column:
        """Return a random eligible update column."""
        i = random.randint(0, len(self._update_columns) - 1)
        return self._update_columns[i]

    def get_column_pandas_types(self) -> Dict[str, str]:
        """ Return a dictionary of column names and associated panda type for Dataframe.astype()"""

        pd_types = {
            "INTEGER": "int64",
            "VARCHAR": "string",
            "FLOAT": "float64",
            "DATE": "datetime64[ns]",
            "BOOLEAN": "bool",
            "TIMESTAMP": "datetime64[ns]",
        }
        return {col.get_name(): pd_types[col.get_type()] for col in self._columns}

#
# Other getters
#
    def get_name(self) -> str:
        return self._name

    def get_primary_key(self) -> str:
        return self._primary_key

    def get_updated_at(self) -> str:
        return self._updated_at

    def get_parent_key(self) -> str:
        return self._parent_key

    def get_parent_table(self) -> str:
        return self._parent_table

    def has_parent(self) -> bool:
        return self._parent_key != "" and self._parent_table != ""


