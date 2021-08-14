# read incremental customer and customer addresses
# get unique customer keys
# pull dim for existing customers
# compute new_customers, allocate new surrogate keys, build new dim record
# update existing record (type 2 SCD comes later)

from typing import Tuple
import pandas as pd
from datetime import date

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
             
def decode_referral(s):

    referrals = {'OA' : 'Online Advertising',
                 'AM' : 'Affiliate Marketing',
                 ''   : 'None'}
    return referrals.get(s.strip().upper(),'Unknown')

def parse_address(s : str) -> pd.Series:
    """ Parse the address and return a series correctly labeled.
    For our purposes the address is a string with the format
    name\nstreet_address\ncity, state zip
    """
    name, street_number, rest = s.split("\n")
    city, rest = rest.split(',')
    state, zip = rest.strip().split()

    return pd.Series({'name' : name,
                      'street_number' : street_number,
                      'city' : city,
                      'state' : state,
                      'zip' : zip
                     })



# combine customer and customer address into new customer_dim entry
def build_new_dimension(new_keys, customer, customer_address):
    # Todo if multiple addresses of a type come in, take the most recent date

    # align two inputs and outputs by customer_id, renamed customer key in dimension
    customer = customer[customer['customer_id'].isin(new_keys.values)]
    customer = customer.set_index('customer_id')
    customer_address = customer_address[customer_address['customer_id'].isin(new_keys.values)]
    customer_address = customer_address.set_index('customer_id', drop=False)
    customer_dim_insert = pd.DataFrame([], columns=[], index=new_keys)

    # straight copy
    # customer_dim_insert['customer_key'] = customer['customer_id']
    customer_dim_insert['name'] = customer['customer_name']

    customer_dim_insert['referral_type'] = \
    customer['customer_referral_type'].map(decode_referral)

    # customer_address    
    
    billing = customer_address[customer_address.customer_address_type == 'B']\
        ['customer_address'].apply(parse_address)
    
    shipping = customer_address[customer_address.customer_address_type == 'S']\
        ['customer_address'].apply(parse_address)

    if billing.size != 0:
        customer_dim_insert['billing_name'] = billing['name']
        customer_dim_insert['billing_street_number'] = billing['street_number']
        customer_dim_insert['billing_city'] = billing['city']
        customer_dim_insert['billing_state'] = billing['state']
        customer_dim_insert['billing_zip'] = billing['zip']

    if shipping.size != 0:
        customer_dim_insert['shipping_name'] = shipping['name']
        customer_dim_insert['shipping_street_number'] = shipping['street_number']
        customer_dim_insert['shipping_city'] = shipping['city']
        customer_dim_insert['shipping_state'] = shipping['state']
        customer_dim_insert['shipping_zip'] = shipping['zip']

    next_surrogate_key = 1  # update after successful insert
    num_inserts = customer_dim_insert.shape[0]

    customer_dim_insert['id'] = (range(next_surrogate_key, num_inserts)) 
    customer_dim_insert['effective_date'] = [date(2020,10,10)] 
    customer_dim_insert['expiration_date'] = None 
    customer_dim_insert['is_current_row'] = 'Y'
    return customer_dim_insert

def build_update_dimension(updates, customer, customer_address):
    """
    Seems like we would need to do the same transforms for the update keys.


    """
    pass
