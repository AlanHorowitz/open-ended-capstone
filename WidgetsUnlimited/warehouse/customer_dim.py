# read incremental customer and customer addresses
# get unique customer keys
# pull dim for existing customers
# compute new_customers, allocate new surrogate keys, build new dim record
# update existing record (type 2 SCD comes later)

from typing import Tuple
import pandas as pd
from datetime import date
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

class CustomerDimension():

    def __init__(self, connection=None):
        self._connection = connection
        self._customer_dim_table = CustomerDimTable()
        if connection:
            self._create_table()
    
    def process_update(self, batch_id):
        self._load_dataframes(batch_id)
        new_df, update_df = self._update_customer_dim()
        return new_df, update_df
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

        return new_cust, updates
        # persist(new_cust)
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
        customer_dim_insert = pd.DataFrame([])
        # customer_dim_insert = customer_dim_insert.astype(pandas_types)
        # customer_dim_insert.set_index(new_keys, inplace=True)
        # straight copy        
       
        customer_dim_insert['customer_key'] = customer['customer_id']
        customer_dim_insert['name'] = customer['customer_name']
        customer_dim_insert['user_id'] = customer['customer_user_id']
        customer_dim_insert['password'] = customer['customer_password']
        customer_dim_insert['email'] = customer['customer_email']        

        customer_dim_insert['referral_type'] = \
        customer['customer_referral_type'].map(CustomerDimension.decode_referral)

        customer_dim_insert['sex'] = customer['customer_sex']
        customer_dim_insert['date_of_birth'] = customer['customer_date_of_birth']
        customer_dim_insert['age_cohort'] = 'n/a'

        customer_dim_insert['loyalty_number'] = customer['customer_loyalty_number']
        customer_dim_insert['credit_card_number'] = customer['customer_credit_card_number']
        customer_dim_insert['is_preferred'] = customer['customer_is_preferred']
        customer_dim_insert['is_active'] = customer['customer_is_active']

        customer_dim_insert['activation_date'] = customer['customer_inserted_at']
        customer_dim_insert['deactivation_date'] = None
        customer_dim_insert['start_date'] = customer['customer_inserted_at']
        customer_dim_insert['last_update_date'] = customer['customer_inserted_at']

        # customer_address    
        
        billing = customer_address[customer_address.customer_address_type == 'B']\
            ['customer_address'].apply(CustomerDimension.parse_address)        
        
        if billing.size != 0:
            customer_dim_insert['billing_name'] = billing['name']
            customer_dim_insert['billing_street_number'] = billing['street_number']
            customer_dim_insert['billing_city'] = billing['city']
            customer_dim_insert['billing_state'] = billing['state']
            customer_dim_insert['billing_zip'] = billing['zip']

        shipping = customer_address[customer_address.customer_address_type == 'S']\
            ['customer_address'].apply(CustomerDimension.parse_address)

        if shipping.size != 0:
            customer_dim_insert['shipping_name'] = shipping['name']
            customer_dim_insert['shipping_street_number'] = shipping['street_number']
            customer_dim_insert['shipping_city'] = shipping['city']
            customer_dim_insert['shipping_state'] = shipping['state']
            customer_dim_insert['shipping_zip'] = shipping['zip']

        next_surrogate_key = 1  # update after successful insert
        num_inserts = customer_dim_insert.shape[0]

        customer_dim_insert['surrogate_key'] = range(next_surrogate_key, next_surrogate_key+num_inserts)
        customer_dim_insert['effective_date'] = date(2020,10,10)
        customer_dim_insert['expiration_date'] = None 
        customer_dim_insert['is_current_row'] = 'Y'    
        customer_dim_insert = pd.DataFrame(customer_dim_insert, columns=self._customer_dim_table.get_column_names())    
        customer_dim_insert = customer_dim_insert.astype(pandas_types)
        return customer_dim_insert

    def build_update_dimension(self, updates, customer, customer_address):
        """
        Seems like we would need to do the same transforms for the update keys.


        """
        pass
