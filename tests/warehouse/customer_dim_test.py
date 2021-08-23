from numpy.lib.twodim_base import triu_indices_from
from pandas.core.frame import DataFrame
from .context import CustomerDimension, CustomerAddressTable, CustomerTable   

import pandas as pd
from datetime import date, datetime
import pytest

# simplify test with simple representative structure
cust_dim_cols = ['surrogate_key', 'customer_key', 'customer_name', 'customer_address_id', 'customer_address']

dim_records = {
    "surrogate_key"    : [1,2],
    "effective_date"   : [date(2020,10,10), date(2020,10,10)],
    "expiration_date"   : [date(2099,12,31),date(2099,12,31)],
    "is_current_row"   : [True, True],
    "customer_key"     : [45,46],
    "name"             : ["Ellen Woods", "Henry Higgins"],
    "user_id"          : ["Ellen456", "Henry123"],
    "password"         : ["XG8yL89BB6T", "XP7ne9Bl9S"],
    "email"            : ["ellen@supermail.com", "henry@hotmail"],
    "referral_type"    : ["Affiliate Marketing", "Online Advertising"],
    "sex"              : ["F", "M"],
    "date_of_birth"    : [date(1994,8,12), date(1996,5,28)],
    "age_cohort"       : ["N/A", "N/A"],
    "loyalty_number"   : [1234, 1235],
    "credit_card_number" : ['12345678', '99999999'],
    "is_preferred" : [True, True],
    "is_active" : [True, True],
    "activation_date"  : [date(2020,10,10), date(2020,10,10)],
    "deactivation_date" :  [date(2099,12,31),date(2099,12,31)],
    "start_date"        : [date(2020,10,10), date(2020,10,10)],
    "last_update_date"  : [date(2020,10,10), date(2020,10,10)],
    "billing_name"      : ["Ellen Woods", "Henry Higgins"],
    "billing_street"    : ["123 Clarkstown Road", "33 Oak Street"],
    "billing_city"      : ["Moorestown", "Kenosha"],
    "billing_state"     : ["NJ", "WI"],
    "billing_zip"       : ["12345", "77777"],
    "shipping_name"      : ["Fred Johnson", None],
    "shipping_street"    : ["77 Eagle Avenue", None],
    "shipping_city"      : ["St. Joseph", None],
    "shipping_state"     : ["TN", None],
    "shipping_zip"       : ["54321", None]
    } 

@pytest.fixture
def base_dimension_record_45():
    
    customer_dim = pd.DataFrame(dim_records)
    customer_dim = customer_dim.set_index('customer_key', drop=False)      
    yield pd.DataFrame(customer_dim.loc[45:45]) # slice for dataframe

@pytest.fixture
def base_dimension_records_all():
    
    customer_dim = pd.DataFrame(dim_records)
    customer_dim = customer_dim.set_index('customer_key', drop=False)      
    yield customer_dim

# union of customer_id from two tables
def test_get_customer_keys_incremental():
    customers = [[1, 'cust1'], [2, 'cust1'],
                 [3, 'cust1'], [4, 'cust1']]

    customer_addresses = [[1, 'custaddress1', 2],
                          [3, 'custaddress3', 2], 
                          [5, 'custaddress5', 4], 
                          [7, 'custaddress7', 4], 
                          [9, 'custaddress9', 6]]

    c = CustomerDimension()
    pd_cust = pd.DataFrame(customers, columns=['customer_id', 'name'])
    pd_cust_address = pd.DataFrame(customer_addresses, columns=['address_id', 'name', 'customer_id'])

    incremental_keys = c.get_customer_keys_incremental(pd_cust, pd_cust_address)
    assert sorted(incremental_keys.tolist()) == [1,2,3,4,6]

# only updates
def test_get_new_keys_1():

    c = CustomerDimension()

    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_1', 'cust_key_2', 'cust_key_3'],
        name='customer_key')

    new_keys = c.get_new_keys(incremental_keys, df_cust_dim) 

    assert sorted(new_keys.to_list()) == []
    
#only new
def test_get_new_keys_2():
    
    c = CustomerDimension()
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_4', 'cust_key_5', 'cust_key_6'], name='customer_key')

    new_keys = c.get_new_keys(incremental_keys, df_cust_dim) 

    assert sorted(new_keys.to_list()) == ['cust_key_4', 'cust_key_5', 'cust_key_6']
        
# mix of updates and new
def test_get_new_keys_3():
    
    c = CustomerDimension()
    cust_dim_data = [[1, 'cust_key_1', 'name_1', 'cust_addr_key_1', 'address_1'],
                     [2, 'cust_key_2', 'name_2', 'cust_addr_key_2', 'address_2'],
                     [3, 'cust_key_3', 'name_3', 'cust_addr_key_3', 'address_3']]

    df_cust_dim = pd.DataFrame(cust_dim_data, columns=cust_dim_cols)
    incremental_keys = pd.Series(['cust_key_2', 'cust_key_3', 'cust_key_4'], name='customer_key')

    new_keys = c.get_new_keys(incremental_keys, df_cust_dim) 

    assert sorted(new_keys.to_list()) == ['cust_key_4']
    
TEST_BILLING_ADDRESS = "First Middle Last\n123 Snickersnack Lane\nBrooklyn, NY 11229"
TEST_SHIPPING_ADDRESS = "First Middle Last\n15 Jones Boulevard\nFair Lawn,NJ 07410"

# test change
def test_parse_address():
    
    c = CustomerDimension(None)
    ser = c.parse_address(TEST_BILLING_ADDRESS)
    assert(ser['name'] == "First Middle Last")
    assert(ser['street_number'] == "123 Snickersnack Lane") 
    assert(ser['city'] == "Brooklyn")
    assert(ser['state'] == "NY") 
    assert(ser['zip'] == "11229")

# test billing/shipping address parsing
# new dimension tessts require complete customers and customer addresses
def test_build_new_dimension_1():

    c = CustomerDimension(None)
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
         "customer_address_inserted_at" : [dt_tm] * 7,
         "customer_address_updated_at" :  [dt_tm] * 7,
         "batch_id" : [1,1,1,1,1,1,1] }

    new_keys = pd.Series([3,4,5], name='customer_key')

    customer_stage_df = pd.DataFrame(customer_stage_data)
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)

    inserts = c.build_new_dimension(new_keys, customer_stage_df, customer_address_stage_df)
    assert(inserts.shape[0] == 3)
    assert(inserts.at[3,'name'] == 'c3')
    assert(inserts.at[3,'billing_state'] == 'NY')
    assert(inserts.at[3,'shipping_state'] == 'NJ')
    assert(inserts.at[4,'billing_city'] == 'Brooklyn')
    # assert(pd.isna(inserts.at[4,'shipping_city']))
    assert(inserts.at[4,'shipping_city'] == 'N/A')

# isolated referral_type parsing
def test_build_new_dimension_2():
    c = CustomerDimension(None)
    customer_table = CustomerTable()
    customer_stage_data  = \
                {"customer_id" : [1,2,3,4],
                "customer_name" : ['c1', 'c2', 'c3', 'c4'],
                "customer_referral_type" : ['ll', ' ','OA','AM'],
                "customer_user_id" : [pd.NA] * 4,
                "customer_password" : [pd.NA] * 4,
                "customer_email" : [pd.NA] * 4,
                "customer_user_id" : [pd.NA] * 4,     
                "customer_sex" : [pd.NA] * 4,
                "customer_date_of_birth" : [pd.NA] * 4,
                "customer_loyalty_number" : [0] * 4,
                "customer_credit_card_number" :[pd.NA] * 4,
                "customer_is_preferred" :[True] * 4,
                "customer_is_active" : [True] * 4,
                "customer_inserted_at" : [pd.NA] * 4,
                "customer_updated_at" :  [pd.NA] * 4,
                "batch_id" : [1,1,1,1] }

    

    customer_stage_address_data = \
        {"customer_id" : [1],
         "customer_address_id" : [1],
         "customer_address" : [None] ,
         "customer_address_type" : [None],             
         "customer_address_inserted_at" : [None],
         "customer_address_updated_at" :  [None],
         "batch_id" : [1] }

    new_keys = pd.Series([1,2,3,4], name='customer_key')
    customer_stage_df = pd.DataFrame(customer_stage_data, columns=customer_table.get_column_names())
    customer_stage_df = customer_stage_df.astype(customer_table.get_column_pandas_types())
    
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)

    inserts = c.build_new_dimension(new_keys, customer_stage_df, customer_address_stage_df)

    assert(inserts.shape[0] == 4)
    assert(inserts.at[1,'referral_type'] == 'Unknown')
    assert(inserts.at[2,'referral_type'] == 'None')
    assert(inserts.at[3,'referral_type'] == 'Online Advertising')
    assert(inserts.at[4,'referral_type'] == 'Affiliate Marketing')
        
    customer_stage_df = customer_stage_df.set_index('customer_id', drop=False)
    customer_address_stage_df = customer_address_stage_df.set_index('customer_id', drop=False)
                
    inserts = c.customer_transform(customer_stage_df, customer_address_stage_df)

    assert(inserts.shape[0] == 4)
    assert(inserts.at[1,'referral_type'] == 'Unknown')
    assert(inserts.at[2,'referral_type'] == 'None')
    assert(inserts.at[3,'referral_type'] == 'Online Advertising')
    assert(inserts.at[4,'referral_type'] == 'Affiliate Marketing')

def test_update_customer_only(base_dimension_record_45):

    c = CustomerDimension(None)
    test_time = datetime.now()

    customer_stage_data  = \
        {"customer_id" : [45],                  
        "customer_email" : ["ellen123@gmail.com"],
        "customer_updated_at" : test_time,
        "customer_credit_card_number" : None,
        "batch_id" : [1] }

    customer_stage_address_data = \
        {"customer_id" : [],
         "customer_address_id" : [],
         "customer_address" : [] ,
         "customer_address_type" : [],             
         "customer_address_inserted_at" : [],
         "customer_address_updated_at" :  [],
         "batch_id" : [] }

    customer_stage_df = pd.DataFrame(customer_stage_data)
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)
    
    customer_dim = c.build_update_dimension(base_dimension_record_45, customer_stage_df, customer_address_stage_df)

    assert customer_dim.shape[0]  == 1
    assert customer_dim.loc[45, 'credit_card_number'] == '12345678' # None value unchanged
    assert customer_dim.loc[45, 'email'] == "ellen123@gmail.com"    # Changed value picked uo
    assert customer_dim.loc[45, 'user_id'] == "Ellen456"            # missing value unchanged 

def test_update_customer_address_only(base_dimension_record_45):
    c = CustomerDimension(None)
    test_time = datetime.now()

    customer_stage_data  = \
        {"customer_id" : [],
        "customer_updated_at" : [],
        "customer_credit_card_number" : [],
        "batch_id" : [] }

    customer_stage_address_data = \
        {"customer_id" : [45],
         "customer_address_id" : [45],
         "customer_address" : ["Fred Johnson\n77 Eagle Avenue\nSt. Joseph, TN 54322"], #change last digit of zip
         "customer_address_type" : ['S'],             
         "customer_address_inserted_at" : [date(2020,10,10)],
         "customer_address_updated_at" :  [test_time],
         "batch_id" : [1] }
    
    customer_stage_df = pd.DataFrame(customer_stage_data)
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)
    
    customer_dim = c.build_update_dimension(base_dimension_record_45, customer_stage_df, customer_address_stage_df)
    assert customer_dim.shape[0]  == 1
    assert customer_dim.loc[45, 'shipping_zip'] == '54322' 
    assert customer_dim.loc[45, 'billing_zip'] == '12345' 
    assert customer_dim.loc[45, 'user_id'] == "Ellen456"             
    

def test_deactivate(base_dimension_record_45):

    c = CustomerDimension(None)
    test_time = datetime.now()

    customer_stage_data  = \
        {"customer_id" : [45],                          
        "customer_updated_at" : test_time,
        "customer_is_active" : False,
        "batch_id" : [1] }

    customer_stage_address_data = \
        {"customer_id" : [],
         "customer_address_id" : [],
         "customer_address" : [] ,
         "customer_address_type" : [],             
         "customer_address_inserted_at" : [],
         "customer_address_updated_at" :  [],
         "batch_id" : [] }

    customer_stage_df = pd.DataFrame(customer_stage_data)
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)
    
    customer_dim = c.build_update_dimension(base_dimension_record_45, customer_stage_df, customer_address_stage_df)

    assert customer_dim.shape[0]  == 1
    assert customer_dim.loc[45, 'is_active'] == False
    assert customer_dim.loc[45, 'deactivation_date'] == test_time 

def test_activate(base_dimension_record_45):

    old_dim = base_dimension_record_45.copy()
    old_dim['is_active'] = False
    old_dim['deactivation_date'] = date(2020,10,15)

    c = CustomerDimension(None)
    test_time = datetime.now()

    customer_stage_data  = \
        {"customer_id" : [45],                          
        "customer_updated_at" : test_time,
        "customer_is_active" : True,
        "batch_id" : [1] }

    customer_stage_address_data = \
        {"customer_id" : [],
         "customer_address_id" : [],
         "customer_address" : [] ,
         "customer_address_type" : [],             
         "customer_address_inserted_at" : [],
         "customer_address_updated_at" :  [],
         "batch_id" : [] }

    customer_stage_df = pd.DataFrame(customer_stage_data)
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)
    
    customer_dim = c.build_update_dimension(old_dim, customer_stage_df, customer_address_stage_df)

    assert customer_dim.shape[0]  == 1
    assert customer_dim.loc[45, 'is_active'] == True
    assert customer_dim.loc[45, 'activation_date'] == test_time 
    assert customer_dim.loc[45, 'deactivation_date'] == date(2099,12,31) 


def test_customer_transform():
    pass


def test_update_billing_and_shipping():
    pass

# mix behaviors across two records
def test_update_all(base_dimension_records_all):

    c = CustomerDimension(None)
    test_time = datetime.now()

    customer_stage_data  = \
        {"customer_id" : [45, 46],                  
        "customer_email" : ["ellen123@gmail.com", None],
        "customer_updated_at" : [test_time, test_time],
        "customer_credit_card_number" : [None, '88888888'],
        "batch_id" : [1,1] }

    customer_stage_address_data = \
        {"customer_id" : [45, 46, 46],
         "customer_address_id" : [1,2,3],
         "customer_address" :  ["Fred Johnson\n77 Eagle Avenue\nSt. Joseph, TN 54322",
                                "Henry Higgins\n44 Pine Street\nKenosha, WI 77777",
                                "Mary Higgins\n44 Pine Street\nKenosha, WI 77777"],
         "customer_address_type" : ['S', 'B', 'S' ],             
         "customer_address_inserted_at" : [test_time, test_time, test_time],
         "customer_address_updated_at" :  [test_time, test_time, test_time],
         "batch_id" : [1,1,1] }

    customer_stage_df = pd.DataFrame(customer_stage_data)
    customer_address_stage_df = pd.DataFrame(customer_stage_address_data)
    
    customer_dim = c.build_update_dimension(base_dimension_records_all, customer_stage_df, customer_address_stage_df)

    assert customer_dim.shape[0]  == 2
    assert customer_dim.loc[45, 'shipping_zip'] == '54322' 
    assert customer_dim.loc[45, 'billing_zip'] == '12345' 
    assert customer_dim.loc[45, 'email'] == 'ellen123@gmail.com'   
    assert customer_dim.loc[45, 'user_id'] == "Ellen456"
    assert customer_dim.loc[45, 'credit_card_number'] == "12345678"

    assert customer_dim.loc[46, 'shipping_name'] == 'Mary Higgins' 
    assert customer_dim.loc[46, 'billing_address'] == '44 Pine Street' 
    assert customer_dim.loc[46, 'email'] == 'henry@hotmail.com'  
    assert customer_dim.loc[46, 'credit_card_number'] == "88888888"