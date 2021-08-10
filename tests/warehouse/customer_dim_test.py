from .context import get_customer_keys_incremental
from .context import get_new_keys_and_updates

import pandas as pd

cust_dim_cols = ['surrogate_key', 'customer_key', 'customer_name', 'customer_address_id', 'customer_address']

def test_get_customer_keys_incremental():
    customers = [[1, 'cust1'], [2, 'cust1'],
                 [3, 'cust1'], [4, 'cust1']]

    customer_addresses = [[1, 'custaddress1', 2],
                          [3, 'custaddress3', 2], 
                          [5, 'custaddress5', 4], 
                          [7, 'custaddress7', 4], 
                          [9, 'custaddress9', 6]]

    pd_cust = pd.DataFrame(customers, columns=['customer_id', 'name'])
    pd_cust_address = pd.DataFrame(customer_addresses, columns=['address_id', 'name', 'customer_id'])

    ser = get_customer_keys_incremental(pd_cust, pd_cust_address)
    assert sorted(ser.tolist()) == [1,2,3,4,6]

# all updates
def test_get_new_keys_and_updates_1():
    
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_2', 'cust_key_3', 'cust_key_4'], name='customer_key')

    new, update_df = get_new_keys_and_updates(incremental_keys, df_cust_dim) 

    assert sorted(new.to_list()) == ['cust_key_4']
    assert update_df.size == 10   # 5 * 2
    update_df.set_index('customer_key', inplace=True)
    assert update_df.at['cust_key_2', 'customer_name'] == 'name_2'
    assert update_df.at['cust_key_3', 'customer_address'] == 'address_3'

# all new
def test_get_new_keys_and_updates_2():
    
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_2', 'cust_key_3', 'cust_key_4'], name='customer_key')

    new, update_df = get_new_keys_and_updates(incremental_keys, df_cust_dim) 

    assert sorted(new.to_list()) == ['cust_key_4']
    assert update_df.size == 10   # 5 * 2
    update_df.set_index('customer_key', inplace=True)
    assert update_df.at['cust_key_2', 'customer_name'] == 'name_2'
    assert update_df.at['cust_key_3', 'customer_address'] == 'address_3'

# mix of updates and new
def test_get_new_keys_and_updates_3():
    
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_2', 'cust_key_3', 'cust_key_4'], name='customer_key')

    new, update_df = get_new_keys_and_updates(incremental_keys, df_cust_dim) 

    assert sorted(new.to_list()) == ['cust_key_4']
    assert update_df.size == 10   # 5 * 2
    update_df.set_index('customer_key', inplace=True)
    assert update_df.at['cust_key_2', 'customer_name'] == 'name_2'
    assert update_df.at['cust_key_3', 'customer_address'] == 'address_3'