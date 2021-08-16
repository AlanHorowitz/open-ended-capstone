import random
from typing import List, Tuple, Dict, Optional, Any
from psycopg2.extensions import cursor

from datetime import datetime
from collections import namedtuple

DEFAULT_INSERT_VALUES: Dict[str, object] = {
    "INTEGER": 98,
    "VARCHAR": "AAA",
    "FLOAT": 5.0,
    "REAL": 5.0,
    "DATE": "2021-02-11 12:52:47",
    "TINYINT": 0,
    "BOOLEAN": True,
    "TIMESTAMP" : datetime(2020,11,11),
}


class Column:
    """Database Column metadata"""

    def __init__(
        self,
        column_name: str,
        column_type: str,
        column_type_length = None,
        isPrimaryKey: bool = False,
        isInsertedAt: bool = False,
        isUpdatedAt: bool = False,
        isBatchId: bool = False,
        isUpdateable: bool = False,
        xref_table: str = "",
        xref_column: str = "", 
        parent_table: str = "",
        parent_key: str = "", 
        default: Any = None         
    ):

        self._name = column_name
        self._type = column_type
        self._type_length = column_type_length
        self._isPrimaryKey = isPrimaryKey
        self._isInsertedAt = isInsertedAt
        self._isUpdatedAt = isUpdatedAt
        self._isBatchId = isBatchId
        self._isUpdateable = isUpdateable
        self._xref_table = xref_table
        self._xref_column = xref_column
        self._parent_table = parent_table
        self._parent_key = parent_key
        self._default = default

    @staticmethod
    def make_type(type, type_len, type_dict) -> str:
        
        suffix = ""
        type_trans = type_dict[type][0]
        type_len_default = type_dict[type][1]

        if type_len or type_len_default:
            suffix = "(" + (str(type_len) if type_len else type_len_default) + ")"

        return type_trans + suffix

    def get_column_def_mysql(self) -> str:

        mysql_dict = {'INTEGER'   : ('INT', None),
                      'VARCHAR'   : ('VARCHAR', '80'),
                      'FLOAT'     : ('DOUBLE', None),
                      'DATE'      : ('DATE', None),
                      'BOOLEAN'   : ('TINYINT', '1'),
                      'TIMESTAMP' : ('TIMESTAMP', '6')}

        return Column.make_type(self.get_type(), self.get_type_length(), mysql_dict)
        
                                
    def get_column_def_postgres(self) -> str:
       
        postgres_dict = {'INTEGER'   : ('INTEGER', None),
                         'VARCHAR'   : ('VARCHAR', '80'),
                         'FLOAT'     : ('FLOAT', '11'),
                         'DATE'      : ('DATE', None),
                         'BOOLEAN'   : ('BOOLEAN', None),
                         'TIMESTAMP' : ('TIMESTAMP', None)}

        return Column.make_type(self.get_type(), self.get_type_length(), postgres_dict)


    def get_name(self) -> str:

        return self._name

    def get_type(self) -> str:

        return self._type

    def get_type_length(self) -> str:

        return self._type_length

    def isPrimaryKey(self) -> bool:

        return self._isPrimaryKey
   
    def isInsertedAt(self) -> bool:

        return self._isInsertedAt

    def isUpdatedAt(self) -> bool:

        return self._isUpdatedAt

    def isBatchId(self) -> bool:

        return self._isBatchId

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

        return self.__default

    def hasDefault(self) -> bool:

        return self._default != None

    def isUpdateable(self) -> bool:

        return self._isUpdateable

class Table:
    """Database Table metadata"""

    class XrefTableData:
        """ helper object for xref data """
        def __init__(
            self, column_list=[], result_set=[], next_random_row=0, num_rows=0
        ):
            self._column_list = column_list
            self._result_set = result_set
            self._next_random_row = next_random_row
            self._num_rows = num_rows

    @staticmethod
    def _initXrefDict(columns: List[Column]) -> Dict[str, XrefTableData]:

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

    def __init__(
        self,
        name: str,        
        *columns: Column,
        generation = True
    ):
        """Instatiate a table metadata object.

        Note: Source system tables in RetailDW must have a single column integer primary key
        and at least one VARCHAR column.
        """

        self._name = name        
        self._generation = generation
        self._columns = [col for col in columns]
        self._columns.append(Column("batch_id", "INTEGER", isBatchId=True)) 
        primary_keys = [col.get_name() for col in columns if col.isPrimaryKey()]
        inserted_ats = [col.get_name() for col in columns if col.isInsertedAt()]
        updated_ats = [col.get_name() for col in columns if col.isUpdatedAt()]
        parent_keys = [col for col in columns if col.isParentKey()]

        if len(primary_keys) != 1:
            raise Exception(
                    "Generator requires exactly one primary key."
                )
        self._primary_key = primary_keys[0]
        
        if self._generation == True:
            if (len(inserted_ats), len(updated_ats)) != (1, 1):
                raise Exception(
                    "Generator requires exactly one inserted_at and updated_at column"
                )                    
            
            self._inserted_at = inserted_ats[0]
            self._updated_at = updated_ats[0]
            
            if len(parent_keys) > 1:
                raise Exception(
                    "Generator may accept at most one parent key")
            elif len(parent_keys) == 1:
                self._parent_key = parent_keys[0].get_parent_key()
                self._parent_table = parent_keys[0].get_parent_table()
            else:
                self._parent_key = "" 
                self._parent_table = ""

            self._update_columns = [
                col for col in columns if col.isUpdateable()
            ]  # restrict to VARCHAR update
            if len(self._update_columns) == 0:
                raise Exception("Need at least one VARCHAR for update")

            self._xrefDict: Dict[str, Table.XrefTableData] = Table._initXrefDict(
                self._columns
            )

        self.operationalSystem = None

    def preload(self, cur: cursor) -> None:
        """ Load foreign key tables for valid references when generating records.  Assume 
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

    def getNewRow(self, primary_key: int, 
                        parent_key: int = None,
                        batch_id: int = None,
                        timestamp: datetime = datetime.now()) -> Tuple:

        d: List[object] = []
        self._setXrefTableRows()

        for col in self.get_columns():
            if col.hasDefault():
                d.append(col.get_default())
            elif col.isPrimaryKey():
                d.append(primary_key)
            elif col.isBatchId():
                d.append(batch_id)
            elif col.isInsertedAt() or col.isUpdatedAt():
                d.append(timestamp)
            elif col.isXref():
                d.append(self._getXrefValue(col))
            elif col.isParentKey() and parent_key is not None:
                d.append(parent_key)
            else:
                d.append(DEFAULT_INSERT_VALUES[col.get_type()])

        return tuple(d)

    def _setXrefTableRows(self):
        """Update the Xref dictionary with the current random rows for each table."""
        for table_data in self._xrefDict.values():
            table_data._next_random_row = random.randint(0, table_data._num_rows - 1)

    def _getXrefValue(self, col: Column) -> str:
        """ Return the column's value from the current random DictRow."""

        row = self._xrefDict[col._xref_table]._next_random_row
        value = self._xrefDict[col._xref_table]._result_set[row][col._xref_column]

        return value

    def get_name(self) -> str:

        return self._name

    def get_columns(self) -> List[Column]:
        """ Return a complete list of Column objects for the table."""

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

    def get_create_sql_mysql(self) -> str:
                
        create_table = "CREATE TABLE IF NOT EXISTS {} ( \n".format(self.get_name()) 
        columns = "\n".join(["{} {},".format(col.get_name(), col.get_column_def_mysql())
                                             for col in self.get_columns()])
        primary_key = "\nPRIMARY KEY ({}));".format(self.get_primary_key())
        return create_table + columns + primary_key


    def get_create_sql_postgres(self) -> str:

        create_table = "CREATE TABLE IF NOT EXISTS {} ( \n".format(self.get_name()) 
        columns = "\n".join(["{} {},".format(col.get_name(), col.get_column_def_postgres())
                                             for col in self.get_columns()])
        primary_key = "\nPRIMARY KEY ({}));".format(self.get_primary_key())
        return create_table + columns + primary_key

    def get_column_names(self) -> List[str]:
        """ Return a complete list of Column names for the table."""
        return [col.get_name() for col in self._columns]

    def get_update_column(self) -> Column:
        """ Return a random eligible update column."""
        i = random.randint(0, len(self._update_columns) - 1)
        return self._update_columns[i]

    def setOperationalSystem(self, op: object) -> None:
        """ Associate table with host operational system."""
        self.operationalSystem = op

    def getOperationalSystem(self) -> object:
        """ Return operational system hosting table."""
        return self.operationalSystem
