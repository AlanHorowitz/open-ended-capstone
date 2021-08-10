# read incremental customer and customer addresses
# get unique customer keys
# pull dim for existing customers
# compute new_customers, allocate new surrogate keys, build new dim record
# update existing record (type 2 SCD comes later)

import pandas as pd

def get_customer_keys_incremental(customer : pd.DataFrame, 
                                  customer_address : pd.DataFrame) -> pd.Series:
    return customer['customer_id'].append(customer_address['customer_id']).unique()

# I could get new and update off single call
def get_updates_from_customer_dim(customer_keys : pd.Series,
                                  customer_dim : pd.DataFrame) -> pd.DataFrame:
    return pd.merge(customer_keys, customer_dim, on='customer_key', how='left')
