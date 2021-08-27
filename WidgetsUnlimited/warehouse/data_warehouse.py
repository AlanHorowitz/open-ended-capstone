from .util import clean_stage_dir, get_stage_dir
from typing import List
from util.sqltypes import Column, Table
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable
import os
import pandas as pd

class DataWarehouse:

    def direct_extract(self, connection, batch_id):
        self.write_parquet_warehouse_tables(connection, batch_id, 
        [CustomerTable(),
        CustomerAddressTable()]
        )

    def transform_load(self, batch_id):
        pass

    def write_parquet_warehouse_tables(self, connection, batch_id: int, tables : List[Table]):    

        pd_types = { 'INTEGER' : 'int64',
                'VARCHAR'   : 'string' ,
                'FLOAT'     : 'float64',
                'DATE'      : 'datetime64[ns]',
                'BOOLEAN'   : 'bool',
                'TIMESTAMP' : 'datetime64[ns]'
                }

        clean_stage_dir(batch_id)

        for table in tables:
            table_name = table.get_name()
            column_names = ",".join(table.get_column_names())
                    
            sql = f"SELECT {column_names} from {table_name} WHERE batch_id = {batch_id};"

            cur = connection.cursor()
            cur.execute(sql)

            result = cur.fetchall()
                    
            df = pd.DataFrame(result, columns=table.get_column_names())

            dtype1 = {col.get_name() : pd_types[col.get_type()] for col in table.get_columns()}        
            df = df.astype(dtype1)         
            df.to_parquet(os.path.join(get_stage_dir(batch_id),f"{table_name}.pr"), compression='gzip')
            
