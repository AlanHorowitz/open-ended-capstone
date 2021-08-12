# read incremental customer and customer addresses
# get unique customer keys
# pull dim for existing customers
# compute new_customers, allocate new surrogate keys, build new dim record
# update existing record (type 2 SCD comes later)

from typing import Tuple
import pandas as pd

customer_dim_columns = []
customer_dim_types = {}

customer_stage_columns = []
customer_stage_types = {}

customer_address_stage_columns = []
customer_address_stage_types = {}
con = None

def create_customer_dim_table():
    pass

def update_customer_dim(batch_id):
    """
    Assumes that all dependant ingested, stage data has been persisted in the sepcified location.
    This persistence is handled by different code
    """
    customer_stage_df = pd.read_parquet('stage/b' + str(batch_id) + '/customer.pq')
    customer_stage_df = customer_stage_df.astype(customer_stage_types)
    customer_address_stage_df = pd.read_parquet('stage/b' + str(batch_id) + '/customer_address.pq')
    customer_address_stage_df = customer_address_stage_df.astype(customer_address_stage_types)
    customer_dim_df = pd.read_sql_query("select * from customer_dim", con, index_col='customer_key')

    customer_keys =  get_customer_keys_incremental(customer_stage_df, customer_address_stage_df)
    new_keys, updates = get_new_keys_and_updates(customer_keys, customer_dim_df)

    build_new_dimension(new_keys, customer_stage_df, customer_address_stage_df)
    build_update_dimension(updates, customer_stage_df, customer_address_stage_df)

def get_customer_keys_incremental(customer : pd.DataFrame, 
                                  customer_address : pd.DataFrame) -> pd.Series:
    customer_keys = customer['customer_id'].append(customer_address['customer_id']).drop_duplicates()
    customer_keys.name = 'customer_key'
    return customer_keys

# customer keys must be unique
def get_new_keys_and_updates(customer_keys : pd.Series,
                             customer_dim : pd.DataFrame)-> Tuple[pd.Series, pd.DataFrame]:

    merged = pd.merge(customer_keys, customer_dim, on='customer_key', how='left')
    new_mask = merged['surrogate_key'].isna()
    new_keys = merged[new_mask]['customer_key']
    updates = merged[~new_mask]
    return new_keys, updates

# combine customer and customer address into new customer_dim entry
def build_new_dimension(new_keys, customer, customer_address):
    # if multiple addresses of a type come in, take the most recent date

    customer_dim_insert = pd.DataFrame([], columns=[], index='customer_key')
    # straight copy
    customer_dim_insert['customer_key'] = customer['customer_id']
    customer_dim_insert['name'] = customer['customer_name']
    

    pass

def build_update_dimension(updates, customer, customer_address):
    
    pass
