from typing import Tuple, Dict
import pandas as pd
from datetime import date

from pandas.core.frame import DataFrame, Series, Index
from .warehouse_util import read_stage
from model.customer_dim import CustomerDimTable
from model.customer import CustomerTable
from model.customer_address import CustomerAddressTable

# transformation mappings
customer_dim_to_customer_mapping = {
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

billing_to_customer_dim_mapping = {
    "billing_name": "name",
    "billing_street_number": "street_number",
    "billing_city": "city",
    "billing_state": "state",
    "billing_zip": "zip",
}

shipping_to_customer_dim_mapping = {
    "shipping_name": "name",
    "shipping_street_number": "street_number",
    "shipping_city": "city",
    "shipping_state": "state",
    "shipping_zip": "zip",
}


class CustomerDimensionProcessor:
    """
    Transform the customer_dimension table in the mySQL star schema.

    The process_update method is called when a batch of incremental updates of source data is
    available in data warehouse staging area.

    The following naming conventions are used in the class:

    pandas DataFrames

    customer - staged customer data
    customer_address - staged customer address data
    customer_dim - mySQL star schema customer_dimension data

    pandas Indexes

    incremental_keys - All customer ids (natural key) present in batch
    update_keys - Customer ids already in the star schema
    new_keys - Customer ids not yet in the star schema
    """

    def __init__(self, connection=None):
        """
        Initialize CustomerDimensionProcessor
        :param connection: mySQL connection created by the warehouse.  None is used for test.
        """

        self._connection = connection
        self._dimension_table = CustomerDimTable()
        self._next_surrogate_key = 1
        if connection:
            self._create_dimension()

    def process_update(self, batch_id: int) -> None:
        """
        Perform the following steps to update the customer_dimension table for an ETL batch

        1) Read customer and customer_address files from stage area into dataframes
        2) Compute incremental_keys
        3) Load customer_dim from mySQL to dataframe for incremental_keys
        4) Compute input_keys and update_keys
        5) Compute input and update customer_dim records using transformations taking customer, customer_address
        and customer_dim as inputs
        6) Write inputs and updates to customer_dimension table in mySQL
        :param batch_id: Identifier for ETL process
        :return:None
        """

        customer, customer_address = read_stage(
            batch_id, [CustomerTable(), CustomerAddressTable()]
        )
        incremental_keys = customer.index.union(customer_address.index).unique()
        print(
            f"CustomerDimensionProcessor: {len(incremental_keys)} unique customer ids detected",
            end=" ",
        )
        if incremental_keys.size == 0:
            print()  # Add newline to 0 keys message
            return

        prior_customer_dim = self._read_dimension("customer_key", incremental_keys)
        update_keys = prior_customer_dim.index
        new_keys = incremental_keys.difference(update_keys)
        print(f"(New: {len(new_keys)})", end=" ")
        print(f"(Updated: {len(update_keys)})")

        inserts = self._build_new_dimension(new_keys, customer, customer_address)
        updates = self._build_update_dimension(
            update_keys, prior_customer_dim, customer, customer_address
        )

        self._write_dimension(inserts, "INSERT")
        self._next_surrogate_key += inserts.shape[0]
        self._write_dimension(updates, "REPLACE")

        print(
            f"CustomerDimensionProcessor: {self._count_dimension()} total rows in customer_dim table"
        )

    def _create_dimension(self):
        """Create an empty customer_dimension on warehouse initialization."""

        cur = self._connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self._dimension_table.get_name()};")
        cur.execute(self._dimension_table.get_create_sql_mysql())

    def _read_dimension(self, key_name: str, key_values: Index) -> DataFrame:
        """
        Read rows from the customer_dimension table using a key filter

        :param key_name: Name of the filter key
        :param key_values: Filter values
        :return: A dataframe of the result set, indexed by the key column
        """

        table_name = self._dimension_table.get_name()
        key_values_list = ",".join([str(k) for k in key_values])
        query = f"SELECT * FROM {table_name} WHERE {key_name} IN ({key_values_list});"
        dimension_df = pd.read_sql_query(query, self._connection)
        dimension_df = dimension_df.set_index(key_name, drop=False)

        return dimension_df

    def _write_dimension(self, customer_dim: DataFrame, operation: str) -> None:
        """
        Write a dataframe containing inserts or updates to the mySQL customer_dimension table.
        Convert the dataframe to a python list and use mysql-connector-python for bulk execution call.

        :param customer_dim: dataframe conforming to customer_dim schema
        :param operation: INSERT/REPLACE -- mirror mySQL verbs for insert/upsert
        :return: None
        """
        if customer_dim.shape[0] > 0:
            table = self._dimension_table
            table_name = table.get_name()
            column_names = ",".join(table.get_column_names())
            values_substitutions = ",".join(["%s"] * len(table.get_column_names()))
            cur = self._connection.cursor()
            rows = customer_dim.to_numpy().tolist()

            cur.executemany(
                f"{operation} INTO {table_name} ({column_names}) values ({values_substitutions})",
                rows,
            )

            if operation == "INSERT":
                operation_text = "inserts"
                rows_affected = cur.rowcount
            else:
                operation_text = "updates"
                rows_affected = cur.rowcount // 2
            print(
                f"CustomerDimensionProcessor: {rows_affected} {operation_text} written to {table_name} table"
            )

            self._connection.commit()

    def _count_dimension(self) -> int:
        """Return number of rows in dimension table"""
        table_name = self._dimension_table.get_name()
        cur = self._connection.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cur.fetchone()[0]

    @staticmethod
    def get_address_defaults() -> Dict[str, str]:
        """Build a dictionary used to set missing address items to N/A"""
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
    def decode_referral_type(s) -> str:
        """Translate referral_type code to text"""
        referrals = {
            "OA": "Online Advertising",
            "AM": "Affiliate Marketing",
            "": "None",
        }
        return referrals.get(s.strip().upper(), "Unknown")

    @staticmethod
    def parse_address(s: str) -> Series:
        """
        Parse the customer_address column and return a series of correctly labeled component fields.
        The address is a string with the expected format: name\nstreet_address\ncity, state zip
        """
        name, street_number, rest = s.split("\n")
        city, rest = rest.split(",")
        state, zip_code = rest.strip().split()

        return pd.Series(
            {
                "name": name,
                "street_number": street_number,
                "city": city,
                "state": state,
                "zip": zip_code,
            }
        )

    @staticmethod
    def customer_transform(
        customer: DataFrame, customer_address: DataFrame
    ) -> DataFrame:
        """
        Common transformations that apply to both inserts and updates.

        - simple column to column mapping
        - decoding of customer_referral_type
        - parsing of billing and customer addresses
        - resolution of last_update_date column

        :param customer: customer stage data, indexed with new_keys or update_keys
        :param customer_address: customer address stage data, indexed with new_keys or update_keys
        :return: a customer_dim dataframe with incremental changes. Values not included in the stage data
        are returned as null.

        """

        # make empty customer_dim dataframe indexed by target keys
        union_index = customer.index.union(customer_address.index).unique()
        customer_dim = pd.DataFrame(
            [], columns=CustomerDimTable().get_column_names(), index=union_index
        )

        # work area to compute latest of three possibly existing dates
        update_dates = pd.DataFrame(
            [], columns=["billing", "shipping", "customer"], index=union_index
        )

        # simple copies from customer to customer_dim
        for k, v in customer_dim_to_customer_mapping.items():
            if v in customer:
                customer_dim[k] = customer[v]

        if "customer_referral_type" in customer.columns:
            customer_dim.referral_type = customer["customer_referral_type"].map(
                CustomerDimensionProcessor.decode_referral_type
            )

        # Collect billing and shipping information where available
        is_billing = customer_address["customer_address_type"] == "B"
        is_shipping = customer_address["customer_address_type"] == "S"

        billing = customer_address[is_billing]["customer_address"].apply(
            CustomerDimensionProcessor.parse_address
        )

        if billing.size != 0:
            update_dates["billing"] = customer_address.loc[
                is_billing, "customer_address_updated_at"
            ]
            for k, v in billing_to_customer_dim_mapping.items():
                customer_dim[k] = billing[v]

        shipping = customer_address[is_shipping]["customer_address"].apply(
            CustomerDimensionProcessor.parse_address
        )

        if shipping.size != 0:
            update_dates["shipping"] = customer_address.loc[
                is_shipping, "customer_address_updated_at"
            ]
            for k, v in shipping_to_customer_dim_mapping.items():
                customer_dim[k] = shipping[v]

        # set last_update_date to latest of three dates
        update_dates["customer"] = customer["customer_updated_at"]  # required column
        customer_dim["last_update_date"] = update_dates.T.max()

        return customer_dim

    def _build_new_dimension(
        self, new_keys: Index, customer: DataFrame, customer_address: DataFrame
    ) -> DataFrame:
        """
        Create and initialize new records for customer_dimension table in star schema

        :param new_keys: keys for records in batch not yet in star schema
        :param customer: staged customer data
        :param customer_address: staged customer address data
        :return: a customer_dim dataframe ready to be written to mySQL
        """

        if len(new_keys) == 0:
            return pd.DataFrame([])

        # restrict stage date to new_keys
        customer = customer.loc[new_keys.intersection(customer.index)]
        customer_address = customer_address.loc[
            new_keys.intersection(customer_address.index)
        ]

        # apply common transformation
        customer_dim = CustomerDimensionProcessor.customer_transform(
            customer, customer_address
        )

        # assign surrogate keys
        next_surrogate_key = self._next_surrogate_key
        num_inserts = customer_dim.shape[0]
        customer_dim["surrogate_key"] = range(
            next_surrogate_key, next_surrogate_key + num_inserts
        )

        # initialize values for new records
        customer_dim["age_cohort"] = "N/A"
        customer_dim["activation_date"] = customer["customer_inserted_at"]
        customer_dim["deactivation_date"] = date(2099, 12, 31)
        customer_dim["start_date"] = customer["customer_inserted_at"]
        customer_dim["effective_date"] = date(2020, 10, 10)
        customer_dim["expiration_date"] = date(2099, 12, 31)
        customer_dim["is_current_row"] = "Y"

        # conform output types
        customer_dim = customer_dim.astype(
            self._dimension_table.get_column_pandas_types()
        )

        # apply default values
        customer_dim = customer_dim.fillna(
            CustomerDimensionProcessor.get_address_defaults()
        )

        return customer_dim

    def _build_update_dimension(
        self,
        update_keys: Index,
        prior_customer_dim: DataFrame,
        customer: DataFrame,
        customer_address: DataFrame,
    ) -> DataFrame:
        """
        Update existing records in customer_dimension table star schema

        :param update_keys: keys for records in batch already in star schema
        :param prior_customer_dim:
        :param customer: staged customer data
        :param customer_address: staged customer address data
        :return: a customer_dim dataframe ready to be written to mySQL
        """

        if prior_customer_dim.shape[0] == 0:
            return pd.DataFrame([])

        # restrict stage date to update_keys
        customer = customer.loc[update_keys.intersection(customer.index)]
        customer_address = customer_address.loc[
            update_keys.intersection(customer_address.index)
        ]

        # apply common transformation
        customer_dim = CustomerDimensionProcessor.customer_transform(
            customer, customer_address
        )

        # reset activation status and dates when change detected
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

        # copy all non-null values from customer_dim over prior_customer_dim (old values not
        # appearing in the incremental batch are preserved)
        mask = customer_dim.notnull()
        for col in customer_dim.columns:
            prior_customer_dim.loc[mask[col], col] = customer_dim[col]

        # make copy of result
        update_dim = pd.DataFrame(
            prior_customer_dim, columns=self._dimension_table.get_column_names()
        )

        # conform output types
        update_dim = update_dim.astype(self._dimension_table.get_column_pandas_types())

        return update_dim
