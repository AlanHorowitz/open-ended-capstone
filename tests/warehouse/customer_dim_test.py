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
    CustomerAddressTable,
    parse_address
)

import pandas as pd
from datetime import date, datetime
from math import isnan

# simplify test with simple representative structure
cust_dim_cols = ['surrogate_key', 'customer_key', 'customer_name', 'customer_address_id', 'customer_address']

# union of customer_id from two tables
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

    incremental_keys = get_customer_keys_incremental(pd_cust, pd_cust_address)
    assert sorted(incremental_keys.tolist()) == [1,2,3,4,6]

# only updates
def test_get_new_keys_and_updates_1():
    
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_1', 'cust_key_2', 'cust_key_3'],
        name='customer_key')

    new_keys, updates_df = get_new_keys_and_updates(incremental_keys, df_cust_dim) 

    assert sorted(new_keys.to_list()) == []
    assert updates_df.size == 15     
    updates_df.set_index('customer_key', inplace=True)
    assert updates_df.at['cust_key_1', 'customer_name'] == 'name_1'
    assert updates_df.at['cust_key_3', 'customer_address'] == 'address_3'

#only new
def test_get_new_keys_and_updates_2():
    
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_4', 'cust_key_5', 'cust_key_6'], name='customer_key')

    new_keys, updates_df = get_new_keys_and_updates(incremental_keys, df_cust_dim) 

    assert sorted(new_keys.to_list()) == ['cust_key_4', 'cust_key_5', 'cust_key_6']
    assert updates_df.size == 0  
    
# mix of updates and new
def test_get_new_keys_and_updates_3():
    
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_2', 'cust_key_3', 'cust_key_4'], name='customer_key')

    new_keys, updates_df = get_new_keys_and_updates(incremental_keys, df_cust_dim) 

    assert sorted(new_keys.to_list()) == ['cust_key_4']
    assert updates_df.size == 10   # 5 * 2
    updates_df.set_index('customer_key', inplace=True)
    assert updates_df.at['cust_key_2', 'customer_name'] == 'name_2'
    assert updates_df.at['cust_key_3', 'customer_address'] == 'address_3'

TEST_BILLING_ADDRESS = "First Middle Last\n123 Snickersnack Lane\nBrooklyn, NY 11229"
TEST_SHIPPING_ADDRESS = "First Middle Last\n15 Jones Boulevard\nFair Lawn,NJ 07410"

# test change
def test_parse_address():
    
    ser = parse_address(TEST_BILLING_ADDRESS)
    assert(ser['name'] == "First Middle Last")
    assert(ser['street_number'] == "123 Snickersnack Lane") 
    assert(ser['city'] == "Brooklyn")
    assert(ser['state'] == "NY") 
    assert(ser['zip'] == "11229")

# test billing/shipping address parsing
def test_build_new_dimension_1():

    day = date(2020,10,10)
    dt_tm = datetime.now()
    tba = TEST_BILLING_ADDRESS
    tsa = TEST_SHIPPING_ADDRESS

    customer_stage_data  = \
                {"customer_id" : [1,3,4,5],
                "customer_name" : ['c1', 'c3', 'c4', 'c5'],
                "customer_user_id" : ['XXX','XXX', 'XXX','XXX'],
                "customer_password" : ['XXX','XXX', 'XXX','XXX'],
                "customer_email" : ['aaa@gmail.com', 'bbb@gmail.com', 'ccc@gmail.com', 'ddd@gmail.com'],
                "customer_user_id" : ['u1','u2', 'u3','u4'],
                "customer_referral_type" : [None, ' ','OA','AM'],
                "customer_sex" : ['F','f', 'M',None],            
                "customer_date_of_birth" : [day, day, day, day],
                "customer_loyalty_number" : [1001,1002,1003,1004],
                "customer_credit_card_number" : ['123', '123', '123', '123'],
                "customer_is_preferred" : [True, False, True, True],
                "customer_is_active" : [True, True, True, True],
                "customer_inserted_at" : [dt_tm, dt_tm, dt_tm, dt_tm],
                "customer_updated_at" :  [dt_tm, dt_tm, dt_tm, dt_tm],
                "batch_id" : [1,1,1,1] }
    # new customers 3 & 5 have billing and shipping; customer 4, billing only,
    customer_stage_address_data = \
        {"customer_id" : [1,2,3,3,4,5,5],
         "customer_address_id" : [1,2,3,4,5,6,7],
         "customer_address" : [tsa, tba, tsa, tba, tba, tsa, tba],
         "customer_address_type" : ['S','B', 'S','B','B','S','B'],             
         "customer_inserted_at" : [dt_tm] * 7,
         "customer_updated_at" :  [dt_tm] * 7,
         "batch_id" : [1,1,1,1,1,1,1] }

    new_keys = pd.Series([3,4,5], name='customer_key')

    customer_stage_df = pd.DataFrame(customer_stage_data)
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)

    inserts = build_new_dimension(new_keys, customer_stage_df, customer_address_stage_df)
    assert(inserts.shape[0] == 3)
    assert(inserts.at[3,'name'] == 'c3')
    assert(inserts.at[3,'billing_state'] == 'NY')
    assert(inserts.at[3,'shipping_state'] == 'NJ')
    assert(inserts.at[4,'billing_city'] == 'Brooklyn')
    assert(isnan(inserts.at[4,'shipping_city']))  #Format checking later

# isolated referral_type parsing
def test_build_new_dimension_2():
    customer_stage_data  = \
                {"customer_id" : [1,2,3,4],
                "customer_name" : ['c1', 'c2', 'c3', 'c4'],
                "customer_referral_type" : ['ll', ' ','OA','AM']}

    customer_stage_address_data = \
        {"customer_id" : [],
         "customer_address_id" : [],
         "customer_address" : [] ,
         "customer_address_type" : [],             
         "customer_inserted_at" : [],
         "customer_updated_at" :  [],
         "batch_id" : [] }

    new_keys = pd.Series([1,2,3,4], name='customer_key')
    customer_stage_df = pd.DataFrame(customer_stage_data)
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)

    inserts = build_new_dimension(new_keys, customer_stage_df, customer_address_stage_df)

    assert(inserts.shape[0] == 4)
    assert(inserts.at[1,'referral_type'] == 'Unknown')
    assert(inserts.at[2,'referral_type'] == 'None')
    assert(inserts.at[3,'referral_type'] == 'Online Advertising')
    assert(inserts.at[4,'referral_type'] == 'Affiliate Marketing')
    

