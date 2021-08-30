# read incremental customer and customer addresses
# get unique customer keys
# pull dim for existing customers
# compute new_customers, allocate new surrogate keys, build new dim record
# update existing record (type 2 SCD comes later)
# May be called in a series of updates.
# Take ingested input from source systems and construct updated customer dimension table in data 
# warehouse.

from typing import Tuple, Dict
import pandas as pd
from datetime import date

from pandas.core.frame import DataFrame, Index
from .util import get_stage_dir, get_new_keys, read_stage
from tables.customer_dim import CustomerDimTable
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable

customer_stage_columns = []
customer_stage_types = CustomerTable().get_column_pandas_types()

customer_address_stage_columns = []
customer_address_stage_types = CustomerAddressTable().get_column_pandas_types()

customer_dim_to_customer_mappings = {
    "customer_key": "customer_id",
    "name": "customer_name",
    "user_id": "customer_user_id",
    "password": "customer_password",
    "email": "customer_email",
    "referral_type": "customer_referral_type",
    "sex": "customer_sex",
    "date_of_birth": "customer_date_of_birth",
    "loyalty_number": "customer_loyalty_number",
    "credit_card_number": "customer_credit_card_number",
    "is_preferred": "customer_is_preferred",
    "is_active": "customer_is_active",
}

billing_to_customer_dim_mappings = {
    "billing_name": "name",
    "billing_street_number": "street_number",
    "billing_city": "city",
    "billing_state": "state",
    "billing_zip": "zip",
}

shipping_to_customer_dim_mappings = {
    "shipping_name": "name",
    "shipping_street_number": "street_number",
    "shipping_city": "city",
    "shipping_state": "state",
    "shipping_zip": "zip",
}


class CustomerDimension:
    """Provide methods of transforming source to dimension
    
    customer 
    customer_address
    customer_dim

    incremental_keys
    undate_keys
    new_keys

    """

    def __init__(self, connection=None):

        self._connection = connection
        self._dimension_table = CustomerDimTable()
        self._next_surrogate_key = 1
        if connection:
            self._create_dimension()

    def process_update(self, batch_id: int) -> None:
        """
        Process all the steps to change the customer_dimension table.

        1) Load customer and customer_address from stage area to dataframes
        2) Compute batch keys
        3) Load customer_dim from database to dataframe for batch keys
        4) Compute keys for insert and update records
        5) Compute input and update customer_dim records from customer, customer_address and customer_dim
        6) Persist inputs and updates to customer_dim data warehouse

        :param batch_id:
        :return:
        """

        customer, customer_address = read_stage(batch_id, [CustomerTable(), CustomerAddressTable()])
        incremental_keys = customer.index.union(customer_address.index).unique()
        if incremental_keys.size == 0:
            return

        prior_customer_dim = self._read_dimension(incremental_keys)
        update_keys = prior_customer_dim.index
        new_keys = incremental_keys.difference(update_keys)

        inserts = self._build_new_dimension(new_keys, customer, customer_address)
        updates = self._build_update_dimension(update_keys, prior_customer_dim, customer, customer_address)

        self._write_dimension(inserts, "INSERT")
        self._next_surrogate_key += inserts.shape[0]
        self._write_dimension(updates, "REPLACE")

    def _create_dimension(self):
        cur = self._connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self._dimension_table.get_name()};")
        cur.execute(self._dimension_table.get_create_sql_mysql())

    def _read_dimension(self, customer_keys):
        keys_list = ",".join([str(k) for k in customer_keys])
        ready_query = f"select * from customer_dim where customer_key in ({keys_list});"
        customer_dim = pd.read_sql_query(ready_query, self._connection)
        customer_dim = customer_dim.set_index("customer_key", drop=False)
        return customer_dim

    def _write_dimension(self, customer_dim: pd.DataFrame, operation: str) -> None:
        if customer_dim.shape[0] > 0:
            table = self._dimension_table
            table_name = table.get_name()
            column_names = ",".join(table.get_column_names())  # for SELECT statements
            values_substitutions = ",".join(["%s"] * len(table.get_column_names()))
            cur = self._connection.cursor()
            rows = customer_dim.to_numpy().tolist()

            cur.executemany(
                f"{operation} INTO {table_name} ({column_names}) values ({values_substitutions})",
                rows,
            )
            self._connection.commit()

    @staticmethod
    def get_address_defaults() -> Dict[str, str]:
        return {
            col: "N/A"
            for col in [
                "billing_name",
                "billing_street_number",
                "billing_city",
                "billing_state",
                "billing_zip",
                "shipping_name",
                "shipping_street_number",
                "shipping_city",
                "shipping_state",
                "shipping_zip",
            ]
        }

    @staticmethod
    def decode_referral(s):

        referrals = {
            "OA": "Online Advertising",
            "AM": "Affiliate Marketing",
            "": "None",
        }
        return referrals.get(s.strip().upper(), "Unknown")

    @staticmethod
    def parse_address(s: str) -> pd.Series:
        """Parse the address and return a series correctly labeled.
        For our purposes the address is a string with the format
        name\nstreet_address\ncity, state zip
        """
        name, street_number, rest = s.split("\n")
        city, rest = rest.split(",")
        state, zip = rest.strip().split()

        return pd.Series(
            {
                "name": name,
                "street_number": street_number,
                "city": city,
                "state": state,
                "zip": zip,
            }
        )

    @staticmethod
    def customer_transform(
            customer: DataFrame, customer_address: DataFrame
    ) -> DataFrame:
        """Common transformations from ingest formats to customer_dim schema.
        Logic specific to inputs and updates occur outside.  Presents dim table ready for
        further processing.
        Customer and customer address are already reduced to new or update keys.
        Name mappings and special treatments.
        """
        union_index = customer.index.union(customer_address.index).unique()
        customer_dim = pd.DataFrame(
            [], columns=CustomerDimTable().get_column_names(), index=union_index
        )

        update_dates = pd.DataFrame(
            [], columns=["billing", "shipping", "customer"], index=union_index
        )

        for k, v in customer_dim_to_customer_mappings.items():
            if v in customer:
                customer_dim[k] = customer[v]

        # todo check column exists with empty referral
        if "customer_referral_type" in customer.columns:
            customer_dim.referral_type = customer["customer_referral_type"].map(
                CustomerDimension.decode_referral
            )

        is_billing = customer_address["customer_address_type"] == "B"
        is_shipping = customer_address["customer_address_type"] == "S"

        billing = customer_address[is_billing]["customer_address"].apply(
            CustomerDimension.parse_address
        )

        if billing.size != 0:
            update_dates["billing"] = customer_address.loc[
                is_billing, "customer_address_updated_at"
            ]
            for k, v in billing_to_customer_dim_mappings.items():
                customer_dim[k] = billing[v]

        shipping = customer_address[is_shipping]["customer_address"].apply(
            CustomerDimension.parse_address
        )

        if shipping.size != 0:
            update_dates["shipping"] = customer_address.loc[
                is_shipping, "customer_address_updated_at"
            ]
            for k, v in shipping_to_customer_dim_mappings.items():
                customer_dim[k] = shipping[v]

        update_dates["customer"] = customer["customer_updated_at"]  # required column
        customer_dim["last_update_date"] = update_dates.T.max()

        return customer_dim

    # new customer_dim entry for an unseen natural key
    def _build_new_dimension(self, new_keys, customer, customer_address):

        if len(new_keys) == 0:
            return pd.DataFrame([])

        customer = customer.loc[new_keys]
        customer_address = customer_address.loc[new_keys]

        customer_dim = CustomerDimension.customer_transform(customer, customer_address)

        customer_dim["age_cohort"] = "N/A"  # TODO derived column

        customer_dim["activation_date"] = customer["customer_inserted_at"]
        customer_dim["deactivation_date"] = date(2099, 12, 31)
        customer_dim["start_date"] = customer["customer_inserted_at"]

        next_surrogate_key = self._next_surrogate_key
        num_inserts = customer_dim.shape[0]

        customer_dim["surrogate_key"] = range(
            next_surrogate_key, next_surrogate_key + num_inserts
        )
        customer_dim["effective_date"] = date(2020, 10, 10)
        customer_dim["expiration_date"] = date(2099, 12, 31)
        customer_dim["is_current_row"] = "Y"

        customer_dim = pd.DataFrame(
            customer_dim, columns=self._dimension_table.get_column_names()
        )
        customer_dim = customer_dim.astype(
            self._dimension_table.get_column_pandas_types()
        )
        customer_dim = customer_dim.fillna(CustomerDimension.get_address_defaults())

        return customer_dim

    def _build_update_dimension(self, update_keys: Index, prior_customer_dim, customer, customer_address):

        if prior_customer_dim.shape[0] == 0:
            return pd.DataFrame([])

        customer = customer.loc[update_keys.intersection(customer.index)]
        customer_address = customer_address.loc[update_keys.intersection(customer_address.index)]

        customer_dim = CustomerDimension.customer_transform(customer, customer_address)

        customer = customer.reindex(update_keys)

        if "customer_is_active" in customer.columns:
            was_activated = (customer["customer_is_active"] == True) & (
                    prior_customer_dim["is_active"] == False
            )
            was_deactivated = (customer["customer_is_active"] == False) & (
                    prior_customer_dim["is_active"] == True
            )

            prior_customer_dim.loc[was_activated, "activation_date"] = customer[
                "customer_updated_at"
            ]
            prior_customer_dim.loc[was_activated, "deactivation_date"] = date(
                2099, 12, 31
            )
            prior_customer_dim.loc[was_deactivated, "deactivation_date"] = customer[
                "customer_updated_at"
            ]

        mask = customer_dim.notnull()
        for col in customer_dim.columns:
            prior_customer_dim.loc[mask[col], col] = customer_dim[col]

        customer_dim = pd.DataFrame(
            prior_customer_dim, columns=self._dimension_table.get_column_names()
        )

        customer_dim = customer_dim.astype(
            self._dimension_table.get_column_pandas_types()
        )

        return customer_dim
