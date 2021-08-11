from .context import (
    get_customer_keys_incremental,
    get_new_keys_and_updates,
    build_new_dimension,
    customer_dim_columns,
    customer_dim_types,
    customer_stage_columns,
    customer_stage_types,
    customer_address_stage_columns,
    customer_address_stage_types,
    CustomerTable,
    CustomerAddressTable
)

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
    incremental_keys = pd.Series(['cust_key_1', 'cust_key_2', 'cust_key_3'],
        name='customer_key')

    new, update_df = get_new_keys_and_updates(incremental_keys, df_cust_dim) 

    assert sorted(new.to_list()) == []
    assert update_df.size == 15     
    update_df.set_index('customer_key', inplace=True)
    assert update_df.at['cust_key_1', 'customer_name'] == 'name_1'
    assert update_df.at['cust_key_3', 'customer_address'] == 'address_3'

# all new
def test_get_new_keys_and_updates_2():
    
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_4', 'cust_key_5', 'cust_key_6'], name='customer_key')

    new, update_df = get_new_keys_and_updates(incremental_keys, df_cust_dim) 

    assert sorted(new.to_list()) == ['cust_key_4', 'cust_key_5', 'cust_key_6']
    assert update_df.size == 0  
    
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

# Since this is for actual structured object, use full column list
def test_build_new_dimension():

    customer_table_cols = CustomerTable().get_column_names
    customer_address_table_cols = CustomerAddressTable().get_column_names

    new_keys = pd.Series(['cust_key_2', 'cust_key_3', 'cust_key_4'], name='customer_key')
    customer_stage_df = pd.DataFrame(columns=customer_table_cols)
    customer_address_stage_df = pd.DataFrame(columns=customer_address_table_cols)

    build_new_dimension(new_keys, customer_stage_df, customer_address_stage_df)
