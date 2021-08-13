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

from datetime import date, datetime
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

def test_parse_address():

    s = "First Middle Last\n123 Snickersnack Lane\nBrooklyn, NY 11229"
    ser = parse_address(s)
    assert(ser['name'] == "First Middle Last")
    assert(ser['street_number'] == "123 Snickersnack Lane") 
    assert(ser['city'] == "Brooklyn")
    assert(ser['state'] == "NY") 
    assert(ser['zip'] == "11229")

# Since this is for actual structured object, use full column list
# def test_build_new_dimension():

#     customer_table_cols = CustomerTable().get_column_names
#     customer_address_table_cols = CustomerAddressTable().get_column_names

#     day = date(2020,10,10)
#     dt_tm = datetime.now()
#     customer_stage_data  = \
#                 {"customer_id" : [1,3,4,5],
#                 "customer_name" : ['c1', 'c3', 'c4', 'c5'],
#                 "customer_user_id" : ['XXX','XXX', 'XXX','XXX'],
#                 "customer_password" : ['XXX','XXX', 'XXX','XXX'],
#                 "customer_email" : ['aaa@gmail.com', 'bbb@gmail.com', 'ccc@gmail.com', 'ddd@gmail.com'],
#                 "customer_user_id" : ['u1','u2', 'u3','u4'],
#                 "customer_referral_type" : [None, None,'OnlineAd',None],
#                 "customer_sex" : ['F','f', 'M',None],            
#                 "customer_date_of_birth" : [day, day, day, day],
#                 "customer_loyalty_number" : [1001,1002,1003,1004],
#                 "customer_credit_card_number" : ['123', '123', '123', '123'],
#                 "customer_is_preferred" : [True, False, True, True],
#                 "customer_is_active" : [True, True, True, True],
#                 "customer_inserted_at" : [dt_tm, dt_tm, dt_tm, dt_tm],
#                 "customer_updated_at" :  [dt_tm, dt_tm, dt_tm, dt_tm],
#                 "batch_id" : [1,1,1,1] }

#     customer_stage_address_data = \
#         {"customer_id" : [1,2,3,3,4,5,5],
#          "customer_address_id" : [1,2,3,4,5,6,7],
#          "customer_address" : ['XXX','XXX', 'XXX','XXX','XXX','XXX','XXX'],
#          "customer_address_type" : ['S','B', 'S','B','B','S','B'],             
#          "customer_inserted_at" : [dt_tm, dt_tm, dt_tm, dt_tm, dt_tm, dt_tm, dt_tm],
#          "customer_updated_at" :  [dt_tm, dt_tm, dt_tm, dt_tm, dt_tm, dt_tm, dt_tm],
#          "batch_id" : [1,1,1,1,1,1,1] }

#     new_keys = pd.Series([3,4,5], name='customer_key')


#     # I need dataframes that have 
#     #  - customer_ids 1,3,4,5 (1 should be ignored)
#     #  - customer address 1,2, 2x3, 1x4, 2x5 (#4 has only billing address)

#     customer_stage_df = pd.DataFrame(customer_stage_data)
#     customer_address_stage_df = pd.DataFrame(customer_stage_address_data)

#     build_new_dimension(new_keys, customer_stage_df, customer_address_stage_df)
#     # assert desired columns
#     # assert desired rows

