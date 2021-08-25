# read incremental customer and customer addresses
# get unique customer keys
# pull dim for existing customers
# compute new_customers, allocate new surrogate keys, build new dim record
# update existing record (type 2 SCD comes later)

from typing import Tuple, Dict
import pandas as pd
from datetime import date

from pandas.core.frame import DataFrame, Index
from .util import STAGE_DIRECTORY_PREFIX, clean_stage_dir, get_stage_dir
from tables.customer_dim import CustomerDimTable
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable


customer_dim_columns = []
customer_dim_types = {}

customer_stage_columns = []
customer_stage_types = CustomerTable().get_column_pandas_types()

customer_address_stage_columns = []
customer_address_stage_types = CustomerAddressTable().get_column_pandas_types()

customer_to_customer_dim_mappings = {
        'customer_key'  : 'customer_id',
        'name'          : 'customer_name',
        'user_id'       : 'customer_user_id',
        'password'      : 'customer_password',
        'email'         : 'customer_email',
        'referral_type' : 'customer_referral_type', 
        'sex'           : 'customer_sex',
        'date_of_birth' : 'customer_date_of_birth',
        'loyalty_number': 'customer_loyalty_number',
        'credit_card_number' : 'customer_credit_card_number',
        'is_preferred'  : 'customer_is_preferred',
        'is_active'     : 'customer_is_active'       
    }

billing_to_customer_dim_mappings = {
        'billing_name'  : 'name',
        'billing_street_number' : 'street_number',
        'billing_city'  : 'city',
        'billing_state' : 'state',
        'billing_zip'   : 'zip'
    }

shipping_to_customer_dim_mappings = {
        'shipping_name'  : 'name',
        'shipping_street_number' : 'street_number',
        'shipping_city'  : 'city',
        'shipping_state' : 'state',
        'shipping_zip'   : 'zip'
    }

class CustomerDimension():

    def __init__(self, connection=None):
        self._connection = connection
        self._customer_dim_table = CustomerDimTable()
        if connection:
            self._create_table()       
           
    def process_update(self, batch_id):
        self._load_incrementals(batch_id)
        self._update_customer_dim()        
        # self._cleanup(batch_id)

    def _create_table(self):        
        cur = self._connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self._customer_dim_table.get_name()};")
        cur.execute(self._customer_dim_table.get_create_sql_mysql())

    def _cleanup(self, batch_id):
        pass

    def _load_incrementals(self, batch_id):
        # get files
        self._customer_stage_df = pd.read_parquet(get_stage_dir(batch_id) + '/customer.pr')
        self._customer_stage_df = self._customer_stage_df.astype(customer_stage_types)
        self._customer_address_stage_df = pd.read_parquet(get_stage_dir(batch_id) + '/customer_address.pr')
        self._customer_address_stage_df = self._customer_address_stage_df.astype(customer_address_stage_types)


    def _load_customer_dim(self, customer_keys):
        keys_list = ",".join([str(k) for k in customer_keys])
        ready_query = f"select * from customer_dim where customer_key in ({keys_list});"                    
        customer_dim = pd.read_sql_query(ready_query, self._connection)
        return customer_dim

        
    def persist(self, customer_dim : pd.DataFrame, operation : str) -> None:
        if customer_dim.shape[0] > 0:
            table = self._customer_dim_table
            table_name = table.get_name()
            column_names = ",".join(table.get_column_names())  # for SELECT statements
            values_substitutions = ",".join(["%s"] * len(table.get_column_names()))
            cur = self._connection.cursor()
            rows = customer_dim.to_numpy().tolist()
            if operation == "REPLACE":
                print('row length', len(rows))
                print('row width', len(rows[0]), 'col count', len(table.get_column_names()))
                print(column_names)
                print(rows[0])                
            cur.executemany(
                f"{operation} INTO {table_name} ({column_names}) values ({values_substitutions})", rows)
            self._connection.commit()

    def _update_customer_dim(self):
        """
        Assumes that all dependent ingested, stage data has been persisted in the specified location.
        This persistence is handled by different code
        """
        incremental_keys = self.get_incremental_keys(self._customer_stage_df, self._customer_address_stage_df)
        if incremental_keys.size == 0:
            return
        prior_cust_dim = self._load_customer_dim(incremental_keys)
        new_keys = self.get_new_keys(incremental_keys, prior_cust_dim)

        cust_dim_insert = self.build_new_dimension(new_keys, self._customer_stage_df, self._customer_address_stage_df)    
        cust_dim_update = self.build_update_dimension(prior_cust_dim, self._customer_stage_df, self._customer_address_stage_df)

        self.persist(cust_dim_insert, "INSERT")        
        self.persist(cust_dim_update, "REPLACE")        

    def get_incremental_keys(self, customer : pd.DataFrame, 
                                    customer_address : pd.DataFrame) -> pd.Series:
        customer_keys = customer['customer_id'].append(customer_address['customer_id']).drop_duplicates()
        customer_keys.name = 'customer_key'
        return customer_keys

    # customer keys must be unique
    def get_new_keys(self, 
                                customer_keys : pd.Series,
                                customer_dim : pd.DataFrame)-> pd.Index:
        
        merged = pd.merge(customer_keys, customer_dim, on='customer_key', how='left')
        new_mask = merged['surrogate_key'].isna()
        new_keys = pd.Index(merged[new_mask]['customer_key'])
        return new_keys

    @staticmethod
    def get_address_defaults() -> Dict[str,str]:
        return { col : "N/A" for col in 
            ['billing_name', 'billing_street_number', 'billing_city', 
             'billing_state', 'billing_zip',
             'shipping_name', 'shipping_street_number', 'shipping_city',
             'shipping_state', 'shipping_zip' ] }

    @staticmethod            
    def decode_referral(s):
        
        referrals = {'OA' : 'Online Advertising',
                    'AM' : 'Affiliate Marketing',
                    ''   : 'None'}
        return referrals.get(s.strip().upper(),'Unknown')

    @staticmethod
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
    
    @staticmethod
    def customer_transform(customer : DataFrame, 
                           customer_address: DataFrame) -> DataFrame:
        """ Common transformations from ingest formats to customer_dim schema.
        Logic specific to inputs and updates occur outside.  Presents dim table ready for 
        further processing.
        Customer and customer address are already reduced to new or update keys.
        Name mappings and special treatments.
        """
               
        customer_dim = pd.DataFrame([], 
            columns=CustomerDimTable().get_column_names())

        update_dates = pd.DataFrame([], columns=["billing", "shipping", "customer"])

        for k,v in customer_to_customer_dim_mappings.items():
            if v in customer:                
                customer_dim[k] = customer[v]

        # todo check column exists with empty referral
        if 'customer_referral_type' in customer.columns:
            customer_dim.referral_type = customer['customer_referral_type']\
            .map(CustomerDimension.decode_referral)                
         
        is_billing = customer_address['customer_address_type'] == 'B'  
        is_shipping = customer_address['customer_address_type'] == 'S'  
        
        billing = customer_address[is_billing]\
            ['customer_address'].apply(CustomerDimension.parse_address)        
        
        if billing.size != 0:
            update_dates['billing'] = customer_address.loc[is_billing, 'customer_address_updated_at']
            for k,v in billing_to_customer_dim_mappings.items():
                customer_dim[k] = billing[v]            
        
        shipping = customer_address[is_shipping]\
            ['customer_address'].apply(CustomerDimension.parse_address)

        if shipping.size != 0:
            update_dates['shipping'] = customer_address.loc[is_shipping, 'customer_address_updated_at']
            for k,v in shipping_to_customer_dim_mappings.items():
                customer_dim[k] = shipping[v]  

        update_dates['customer'] = customer['customer_updated_at']  #required column
        customer_dim['last_update_date'] = update_dates.T.max()
        print("+" * 10)
        print('billing:', billing)
        print('shipping:', shipping)
        print('customer:', customer['customer_updated_at'])

        print('update_dates billing:', update_dates['billing'])
        print('update_dates shipping:', update_dates['shipping'])
        print('update_dates customer:', update_dates['customer'])

        print('last_update_date:', customer_dim['last_update_date'])
        print("+" * 10)
        return customer_dim
   
    # new customer_dim entry for an unseen natural key
    def build_new_dimension(self, new_keys, customer, customer_address):
        if len(new_keys) == 0:
            return pd.DataFrame([])

        new_customers = customer['customer_id'].isin(new_keys.values)
        new_customer_addresses = customer_address['customer_id'].isin(new_keys.values)
        
        customer = customer[new_customers]
        customer_address = customer_address[new_customer_addresses]

        customer = customer.set_index('customer_id', drop=False)
        customer_address = customer_address.set_index('customer_id', drop=False)        
                
        customer_dim = CustomerDimension.customer_transform(customer, customer_address)
        
        customer_dim['age_cohort'] = 'N/A'  # TODO derived column

        customer_dim['activation_date'] = customer['customer_inserted_at']
        customer_dim['deactivation_date'] = date(2099,12,31) 
        customer_dim['start_date'] = customer['customer_inserted_at']
         
        next_surrogate_key = 1  # update after successful insert
        num_inserts = customer_dim.shape[0]

        customer_dim['surrogate_key'] = range(next_surrogate_key, next_surrogate_key+num_inserts)
        customer_dim['effective_date'] = date(2020,10,10)
        customer_dim['expiration_date'] = date(2099,12,31) 
        customer_dim['is_current_row'] = 'Y'    

        customer_dim = pd.DataFrame(customer_dim, columns=self._customer_dim_table.get_column_names())    
        customer_dim = customer_dim.astype(self._customer_dim_table.get_column_pandas_types())
        customer_dim = customer_dim.fillna(CustomerDimension.get_address_defaults())
        
        return customer_dim

    def build_update_dimension(self, prior_customer_dim, customer, customer_address):    

        if prior_customer_dim.shape[0] == 0:
            return pd.DataFrame([])
       
        prior_customer_dim = prior_customer_dim.set_index('customer_key', drop=False)
        update_keys = prior_customer_dim.index
         
        customer = customer[customer['customer_id'].isin(update_keys.values)]
        customer = customer.set_index('customer_id', drop=False)
        customer_address = customer_address[customer_address['customer_id'].isin(update_keys.values)]
        customer_address = customer_address.set_index('customer_id', drop=False)
      
        customer_dim = CustomerDimension.customer_transform(customer, customer_address)        

        if 'customer_is_active' in customer.columns:
            was_activated = (customer['customer_is_active'] == True) & \
                            (prior_customer_dim['is_active'] == False)
            was_deactivated = (customer['customer_is_active'] == False) & \
                            (prior_customer_dim['is_active'] == True) 

            prior_customer_dim.loc[was_activated, 'activation_date'] = customer['customer_updated_at']
            prior_customer_dim.loc[was_activated, 'deactivation_date'] = date(2099,12,31)        
            prior_customer_dim.loc[was_deactivated, 'deactivation_date'] = customer['customer_updated_at']

        mask = customer_dim.notnull()
        for col in customer_dim.columns:            
            prior_customer_dim.loc[mask[col], col] = customer_dim[col]
       
        customer_dim = pd.DataFrame(prior_customer_dim, columns=self._customer_dim_table.get_column_names())    
        date_cols = [col.get_name() for col in self._customer_dim_table.get_columns() 
        if col.get_type() == 'DATE'] 
        for d in date_cols:
            print(d, customer_dim[d])

        customer_dim = customer_dim.astype(self._customer_dim_table.get_column_pandas_types())

        return customer_dim