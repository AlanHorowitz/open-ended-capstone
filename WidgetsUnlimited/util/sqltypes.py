import random
from typing import List, Tuple, Dict, Optional, Any
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
    """Database Column metadata used for DDL and data generation"""
    def __init__(
        self,
        column_name: str,               # column name
        column_type: str,               # (INTEGER, VARCHAR, FLOAT, DATE, BOOLEAN, TIMESTAMP)
        column_type_length=None,        # optional column length (e.g. 200 for VARCHAR(200)
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
        self._type_length = column_type_length
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

    def get_column_definition(self, definition_dict) -> str:
        """
        Get database specific DDL text for column, optionally including a length

        :param definition_dict: Mappings from Column.column_type to native SQL name plus optional column length
        :return: Native DDL text (postgres or mysql) for column definition, with length appended if specified or
        if default is indicated for the type
        """
        length_suffix = ""
        type_length = self.get_type_length()
        type_keyword = definition_dict[self.get_type()][0]
        type_length_default = definition_dict[self.get_type()][1]

        # when explicit or default length found, append within parentheses
        if type_length or type_length_default:
            length_suffix = "(" + (str(type_length) if type_length else type_length_default) + ")"

        return type_keyword + length_suffix

    def get_name(self) -> str:
        return self._name

    def get_type(self) -> str:
        return self._type

    def get_type_length(self) -> str:
        return self._type_length

    def is_primary_key(self) -> bool:
        return self._primary_key

    def is_inserted_at(self) -> bool:
        return self._inserted_at

    def isUpdatedAt(self) -> bool:
        return self._updated_at

    def isBatchId(self) -> bool:
        return self._batch_id

    def get_xref_table(self) -> str:
        return self._xref_table

    def get_xref_column(self) -> str:
        return self._xref_column

    def isXref(self) -> bool:
        return self._xref_table != "" and self._xref_column != ""

    def get_parent_table(self) -> str:
        return self._parent_table

    def get_parent_key(self) -> str:
        return self._parent_key

    def isParentKey(self) -> bool:
        return self._parent_table != "" and self._parent_key != ""

    def get_default(self) -> Any:
        return self._default

    def hasDefault(self) -> bool:
        return self._default != None

    def canUpdate(self) -> bool:
        return self._update


class Table:
    """Database Table metadata used for DDL and data generation"""
# Note: Source system tables in RetailDW must have a single column integer primary key
# and at least one VARCHAR column.
    def __init__(self, name: str, *columns: Column, generation=True, batch_id=True):
        """

        :param name:
        :param columns:
        :param generation:
        :param batch_id:
        """

        self._name = name
        self._generation = generation
        self._batchId = batch_id
        self._columns = [col for col in columns]
        if self._batchId:
            self._columns.append(Column("batch_id", "INTEGER", batch_id=True))
        primary_keys = [col.get_name() for col in columns if col.is_primary_key()]
        inserted_ats = [col.get_name() for col in columns if col.is_inserted_at()]
        updated_ats = [col.get_name() for col in columns if col.isUpdatedAt()]
        parent_keys = [col for col in columns if col.isParentKey()]

        if len(primary_keys) != 1:
            raise Exception("Generator requires exactly one primary key.")
        self._primary_key = primary_keys[0]

        if self._generation:
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
                col for col in columns if col.canUpdate()
            ]  # restrict to VARCHAR update
            if len(self._update_columns) == 0:
                raise Exception("Need at least one VARCHAR for update")

            self._xrefDict: Dict[str, Table.XrefTableData] = Table._init_xref_dict(
                self._columns
            )

    #
    #  Database table creation class methods
    #
    def get_create_sql_mysql(self) -> str:
        """ Returns SQL to create table for this class in postgresql """

        mysql_dict = {
            "INTEGER": ("INT", None),
            "VARCHAR": ("VARCHAR", "80"),
            "FLOAT": ("DOUBLE", None),
            "DATE": ("DATE", None),
            "BOOLEAN": ("TINYINT", "1"),
            "TIMESTAMP": ("TIMESTAMP", "6"),
        }
        return self.get_create_sql(mysql_dict)

    def get_create_sql_postgres(self) -> str:
        """ Returns SQL to create table for this class in postgresql """

        postgres_dict = {
            "INTEGER": ("INTEGER", None),
            "VARCHAR": ("VARCHAR", "80"),
            "FLOAT": ("FLOAT", "11"),
            "DATE": ("DATE", None),
            "BOOLEAN": ("BOOLEAN", None),
            "TIMESTAMP": ("TIMESTAMP", None),
        }
        return self.get_create_sql(postgres_dict)

    def get_create_sql(self, definition_dict):
        """
        Compose the text for CREATE TABLE to be executed on postgresql or mysql

        :param definition_dict: Mappings from Column.column_type to native SQL name plus optional column length
        :return CREATE TABLE statement in print friendly format.
        """

        create_table = f"CREATE TABLE IF NOT EXISTS {self.get_name()} ( \n"
        columns = "\n".join(
            [
                f"{col.get_name()} {col.get_column_definition(definition_dict)},"
                for col in self.get_columns()
            ]
        )
        primary_key = f"\nPRIMARY KEY ({self.get_primary_key()}));"

        return create_table + columns + primary_key

    #
    # Generation lifecycle class methods
    #
    def preload(self, cur: cursor) -> None:
        """Load foreign key tables for valid references when generating records.  Assume
        these tables fit in memory for now.  Update the xrefDict with result set and count.
        """
        for table_name, table_data in self._xrefDict.items():
            column_names = ",".join(table_data._column_list)
            cur.execute(f"SELECT {column_names} from {table_name};")
            table_data._result_set = cur.fetchall()
            table_data._num_rows = len(table_data._result_set)

    def postload(self) -> None:
        """Clear references to xref result set"""
        for table_data in self._xrefDict.values():
            table_data._result_set = []
            table_data._num_rows = 0
            table_data._next_random_row = 0

    def getNewRow(
        self,
        primary_key: int,
        parent_key: int = None,
        batch_id: int = None,
        timestamp: datetime = datetime.now(),
    ) -> Tuple:

        d: List[object] = []
        self._setXrefTableRows()

        for col in self.get_columns():
            if col.hasDefault():
                d.append(col.get_default())
            elif col.is_primary_key():
                d.append(primary_key)
            elif col.isBatchId():
                d.append(batch_id)
            elif col.is_inserted_at() or col.isUpdatedAt():
                d.append(timestamp)
            elif col.isXref():
                d.append(self._getXrefValue(col))
            elif col.isParentKey() and parent_key is not None:
                d.append(parent_key)
            else:
                d.append(DEFAULT_INSERT_VALUES[col.get_type()])

        return tuple(d)

    class XrefTableData:
        """helper object for xref data"""

        def __init__(
                self, column_list=[], result_set=[], next_random_row=0, num_rows=0
        ):
            self._column_list = column_list
            self._result_set = result_set
            self._next_random_row = next_random_row
            self._num_rows = num_rows

    @staticmethod
    def _init_xref_dict(columns: List[Column]) -> Dict[str, XrefTableData]:

        xref_dict: Dict[str, Table.XrefTableData] = {}
        for col in columns:
            if col.isXref():
                xref_table, xref_column = col.get_xref_table(), col.get_xref_column()
                if xref_table in xref_dict:
                    xref_dict[xref_table]._column_list.append(xref_column)
                else:
                    xref_dict[xref_table] = Table.XrefTableData(
                        column_list=[xref_column]
                    )
        return xref_dict


    def _setXrefTableRows(self):
        """Update the Xref dictionary with the current random rows for each table."""
        for table_data in self._xrefDict.values():
            table_data._next_random_row = random.randint(0, table_data._num_rows - 1)

    def _getXrefValue(self, col: Column) -> str:
        """Return the column's value from the current random DictRow."""

        row = self._xrefDict[col._xref_table]._next_random_row
        value = self._xrefDict[col._xref_table]._result_set[row][col._xref_column]

        return value



    def get_column_names(self) -> List[str]:
        """Return a complete list of Column names for the table."""
        return [col.get_name() for col in self._columns]

    def get_column_pandas_types(self) -> Dict[str, str]:
        """Return a complete list of Column names for the table."""

        pd_types = {
            "INTEGER": "int64",
            "VARCHAR": "string",
            "FLOAT": "float64",
            "DATE": "datetime64[ns]",
            "BOOLEAN": "bool",
            "TIMESTAMP": "datetime64[ns]",
        }

        return {col.get_name(): pd_types[col.get_type()] for col in self._columns}

    def get_update_column(self) -> Column:
        """Return a random eligible update column."""
        i = random.randint(0, len(self._update_columns) - 1)
        return self._update_columns[i]

    def get_name(self) -> str:

        return self._name

    def get_columns(self) -> List[Column]:
        """Return a complete list of Column objects for the table."""

        return self._columns

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


