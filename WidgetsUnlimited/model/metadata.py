import random
from typing import List, Dict, Any


class Column:
    """Database column metadata used for DDL and data generation"""
    def __init__(
        self,
        column_name: str,               # sql name of the column
        column_type: str,               # sql type (INTEGER, VARCHAR, FLOAT, DATE, BOOLEAN, TIMESTAMP)
        column_length= None,            # optional column length (e.g. 200 for VARCHAR(200))
        primary_key: bool = False,      # column is primary key
        inserted_at: bool = False,      # column is the inserted_at column
        updated_at: bool = False,       # column is the updated_at column
        batch_id: bool = False,         # column is batch_id column
        update: bool = False,           # column is eligible for update by generator
        xref_table: str = "",           # name of foreign reference table
        xref_column: str = "",          # referenced column in referenced table
        parent_table: str = "",         # parent table from which to populate column
        parent_key: str = "",           # column within parent table (key) to populate column
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

    def get_create_sql_text(self, db_types_dict) -> str:
        """
        Get database-specific DDL text to be used in CREATE TABLE.  Optionally include a length
        parameter (e.g. my_column VARCHAR(200) or my_column VARCHAR).

        :param db_types_dict: Mapping from Column.column_type to tuple (db-specific SQL type, optional type length)
        :return: Native DDL text (postgres or mysql) for column.

        """
        length_parameter = ""
        length = self.get_type_length()
        db_type = db_types_dict[self.get_type()][0]
        db_default_length = db_types_dict[self.get_type()][1]

        # when length attribute or default length found, append within parentheses
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
    """Database Table metadata (schema) used for DDL and data generation"""

    def __init__(self, name: str, *columns: Column, create_only=False, batch_id=True):
        """
        Prepare the Table class for use by the DataGenerator

            - Enforce constraints on Column attributes
            - Initial xref dictionary

        :param name: sql name of the table
        :param columns: ordered Column objects comprising Table
        :param create_only: Use Table only for create table and metadata, not data generation
        :param batch_id: Append a batch_id column to columns
        """

        self._name = name
        self._create_only = create_only
        self._batch_id = batch_id
        self._columns = [col for col in columns]
        if self._batch_id:
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
            ]
            if len(self._update_columns) == 0:
                raise Exception("Table requires at least one update column")

            self._init_xref_dict()

    def _init_xref_dict(self) -> None:
        """ Create a dictionary mapping xref table names to helper objects used to manage cross table lookups """

        xref_dict: Dict[str, XrefTableData] = {}

        for col in self.get_columns():
            if col.is_xref():
                xref_table, xref_column = col.get_xref_table(), col.get_xref_column()
                if xref_table in xref_dict:
                    xref_dict[xref_table].column_list.append(xref_column)
                else:
                    xref_dict[xref_table] = XrefTableData()
                    xref_dict[xref_table].column_list.append(xref_column)

        self._xref_dict = xref_dict

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
        columns = "\n".join([col.get_create_sql_text(definition_dict) + "," for col in self.get_columns()])
        primary_key = f"\nPRIMARY KEY ({self.get_primary_key()}));"

        return create_table + columns + primary_key

    def get_columns(self) -> List[Column]:
        """Return column objects"""

        return self._columns

    def get_column_names(self) -> List[str]:
        """Return column names"""

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

    def get_xref_dict(self) -> Dict:
        return self._xref_dict


class XrefTableData:
    """helper object for xref data"""

    def __init__(self):
        self.column_list = []
        self.result_set = []
        self.next_random_row = 0
        self.num_rows = 0
