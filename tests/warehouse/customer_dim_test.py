from .context import get_customer_keys_incremental

import pandas as pd

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