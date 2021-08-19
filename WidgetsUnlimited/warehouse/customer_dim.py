# read incremental customer and customer addresses
# get unique customer keys
# pull dim for existing customers
# compute new_customers, allocate new surrogate keys, build new dim record
# update existing record (type 2 SCD comes later)

from typing import Tuple
import pandas as pd
from datetime import date

from pandas.core.frame import DataFrame
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
        self._address_defaults = { col : "N/A" for col in 
            ['billing_name', 'billing_street_number', 'billing_city', 
             'billing_state', 'billing_zip',
             'shipping_name', 'shipping_street_number', 'shipping_city',
             'shipping_state', 'shipping_zip' ] }
    
    def process_update(self, batch_id):
        self._load_dataframes(batch_id)
        self._update_customer_dim()        
        # self._cleanup(batch_id)

    def _create_table(self):        
        cur = self._connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self._customer_dim_table.get_name()};")
        cur.execute(self._customer_dim_table.get_create_sql_mysql())

    def _cleanup(self, batch_id):
        pass

    def _load_dataframes(self, batch_id):
        # get files
        self._customer_stage_df = pd.read_parquet(get_stage_dir(batch_id) + '/customer.pr')
        self._customer_stage_df = self._customer_stage_df.astype(customer_stage_types)
        self._customer_address_stage_df = pd.read_parquet(get_stage_dir(batch_id) + '/customer_address.pr')
        self._customer_address_stage_df = self._customer_address_stage_df.astype(customer_address_stage_types)
        self._customer_dim_df = pd.read_sql_query("select * from customer_dim", self._connection, index_col='customer_key')
        print(self._customer_stage_df.dtypes)
        print("+" *25)
        print(self._customer_address_stage_df.dtypes)
        print("+" *25)
        print(self._customer_dim_df.dtypes)
        print("+" *25)

        print("size", self._customer_dim_df.size )

    def persist_new(self, customer_dim : pd.DataFrame):
        table = self._customer_dim_table
        table_name = table.get_name()
        column_names = ",".join(table.get_column_names())  # for SELECT statements
        values_substitutions = ",".join(["%s"] * len(table.get_column_names()))
        cur = self._connection.cursor()
        rows = customer_dim.to_numpy().tolist()
        print(rows[0])
        print(type(rows[0][2]))
        cur.executemany(
            f"INSERT INTO {table_name} ({column_names}) values ({values_substitutions})", rows)
        self._connection.commit()

    def create_customer_dim_table():
        pass

    def _update_customer_dim(self):
        """
        Assumes that all dependant ingested, stage data has been persisted in the specified location.
        This persistence is handled by different code
        """
        customer_keys =  self.get_customer_keys_incremental(self._customer_stage_df, self._customer_address_stage_df)
        if customer_keys.size == 0:
            return
        new_keys, updates = self.get_new_keys_and_updates(customer_keys, self._customer_dim_df)
        new_cust = self.build_new_dimension(new_keys, self._customer_stage_df, self._customer_address_stage_df)    
        update_cust = self.build_update_dimension(updates, self._customer_stage_df, self._customer_address_stage_df)
        self.persist_new(new_cust)        
        # persist(update_cust)

    def get_customer_keys_incremental(self, customer : pd.DataFrame, 
                                    customer_address : pd.DataFrame) -> pd.Series:
        customer_keys = customer['customer_id'].append(customer_address['customer_id']).drop_duplicates()
        customer_keys.name = 'customer_key'
        return customer_keys

    # customer keys must be unique
    def get_new_keys_and_updates(self, 
                                customer_keys : pd.Series,
                                customer_dim : pd.DataFrame)-> Tuple[pd.Series, pd.DataFrame]:

        merged = pd.merge(customer_keys, customer_dim, on='customer_key', how='left')
        new_mask = merged['surrogate_key'].isna()
        new_keys = merged[new_mask]['customer_key']
        updates = merged[~new_mask]
        return new_keys, updates

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
    def customer_transform(customer : DataFrame, customer_address: DataFrame) -> DataFrame:
        
        customer_dim = pd.DataFrame()
        mask = pd.notnull(customer)

        for k,v in customer_to_customer_dim_mappings.items():
            customer_dim[k] = customer[v][mask[v]]

        customer_dim.referral_type = customer_dim.referral_type.map(CustomerDimension.decode_referral)
                
        # customer_address - mask not needed because screening condition is sufficient    
        
        billing = customer_address[customer_address.customer_address_type == 'B']\
            ['customer_address'].apply(CustomerDimension.parse_address)        
        
        if billing.size != 0:
            for k,v in billing_to_customer_dim_mappings.items():
                customer_dim[k] = billing[v]            
        
        shipping = customer_address[customer_address.customer_address_type == 'S']\
            ['customer_address'].apply(CustomerDimension.parse_address)

        if shipping.size != 0:
            for k,v in shipping_to_customer_dim_mappings.items():
                customer_dim[k] = shipping[v]           

        return customer_dim

    # new customer_dim entry for an unseen natural key
    def build_new_dimension(self, new_keys, customer, customer_address):
        # Todo if multiple addresses of a type come in, take the most recent date

        # align two inputs and outputs by customer_id, renamed customer key in dimension
        customer = customer[customer['customer_id'].isin(new_keys.values)]
        customer = customer.set_index('customer_id', drop=False)
        customer_address = customer_address[customer_address['customer_id'].isin(new_keys.values)]
        customer_address = customer_address.set_index('customer_id', drop=False)
        # customer_dim_insert = pd.DataFrame([], 
        #                       columns=self._customer_dim_table.get_column_names())                             
        pandas_types = self._customer_dim_table.get_column_pandas_types()
        print("pandas_data_types", pandas_types)
        
        # customer_dim_insert = customer_dim_insert.astype(pandas_types)
        # customer_dim_insert.set_index(new_keys, inplace=True)
        # straight copy        

        # inherit the types from customer/customer address -- conform at end
        # customer_id is the alignment index
        
        customer_dim_insert = CustomerDimension.customer_transform(customer, customer_address)
        
        customer_dim_insert['age_cohort'] = 'n/a'
        customer_dim_insert['activation_date'] = customer['customer_inserted_at']
        customer_dim_insert['deactivation_date'] = date(2099,12,31) 
        customer_dim_insert['start_date'] = customer['customer_inserted_at']
        customer_dim_insert['last_update_date'] = customer['customer_inserted_at']
         
        next_surrogate_key = 1  # update after successful insert
        num_inserts = customer_dim_insert.shape[0]

        customer_dim_insert['surrogate_key'] = range(next_surrogate_key, next_surrogate_key+num_inserts)
        customer_dim_insert['effective_date'] = date(2020,10,10)
        customer_dim_insert['expiration_date'] = date(2099,12,31) 
        customer_dim_insert['is_current_row'] = 'Y'    
        customer_dim_insert = pd.DataFrame(customer_dim_insert, columns=self._customer_dim_table.get_column_names())    
        customer_dim_insert = customer_dim_insert.astype(pandas_types)
        customer_dim_insert = customer_dim_insert.fillna(self._address_defaults)
        return customer_dim_insert

    def build_update_dimension(self, updates_dim, customer, customer_address):
        """
        Seems like we would need to do the same transforms for the update keys.
        """
        updates_dim = updates_dim.set_index('customer_key', drop=False)
        update_keys = updates_dim.index
        # build updates_incremental

        customer = customer[customer['customer_id'].isin(update_keys.values)]
        customer = customer.set_index('customer_id', drop=False)
        customer_address = customer_address[customer_address['customer_id'].isin(update_keys.values)]
        customer_address = customer_address.set_index('customer_id', drop=False)

        # update will have at least one of customer, billing address or shipping address, but
        # may not have all the columns.

        customer_incremental = pd.DataFrame()
        customer_incremental['customer_key'] = pd.notnull(customer['customer_id'])
        customer_incremental['name'] = pd.notnull(customer['customer_name'])
        customer_incremental['user_id'] = pd.notnull(customer['customer_user_id'])
        customer_incremental['password'] = pd.notnull(customer['customer_password'])
        customer_incremental['email'] = pd.notnull(customer['customer_email'])       

        customer_incremental['referral_type'] = pd.notnull(customer['customer_referral_type'])
        customer_incremental['referral_type'] = \
        customer_incremental['referral_type'].map(CustomerDimension.decode_referral)