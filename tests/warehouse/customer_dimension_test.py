from datetime import date, datetime
import pandas as pd
import pytest

from WidgetsUnlimited.warehouse.util import get_new_keys
from .context import CustomerDimensionProcessor, CustomerTable

# subset of customer_dim columns for testing
customer_dim_cols = [
    "surrogate_key",
    "customer_key",
    "customer_name",
    "customer_address_id",
    "customer_address",
]

dim_records = {
    "surrogate_key": [1, 2],
    "effective_date": [date(2020, 10, 10), date(2020, 10, 10)],
    "expiration_date": [date(2099, 12, 31), date(2099, 12, 31)],
    "is_current_row": [True, True],
    "customer_key": [45, 46],
    "name": ["Ellen Woods", "Henry Higgins"],
    "user_id": ["Ellen456", "Henry123"],
    "password": ["XG8yL89BB6T", "XP7ne9Bl9S"],
    "email": ["ellen@supermail.com", "henry@hotmail.com"],
    "referral_type": ["Affiliate Marketing", "Online Advertising"],
    "sex": ["F", "M"],
    "date_of_birth": [date(1994, 8, 12), date(1996, 5, 28)],
    "age_cohort": ["N/A", "N/A"],
    "loyalty_number": [1234, 1235],
    "credit_card_number": ["12345678", "99999999"],
    "is_preferred": [True, True],
    "is_active": [True, True],
    "activation_date": [date(2020, 10, 10), date(2020, 10, 10)],
    "deactivation_date": [date(2099, 12, 31), date(2099, 12, 31)],
    "start_date": [date(2020, 10, 10), date(2020, 10, 10)],
    "last_update_date": [date(2020, 10, 10), date(2020, 10, 10)],
    "billing_name": ["Ellen Woods", "Henry Higgins"],
    "billing_street": ["123 Clarkstown Road", "33 Oak Street"],
    "billing_city": ["Moorestown", "Kenosha"],
    "billing_state": ["NJ", "WI"],
    "billing_zip": ["12345", "77777"],
    "shipping_name": ["Fred Johnson", None],
    "shipping_street": ["77 Eagle Avenue", None],
    "shipping_city": ["St. Joseph", None],
    "shipping_state": ["TN", None],
    "shipping_zip": ["54321", None],
}

TEST_BILLING_ADDRESS = "First Middle Last\n123 Snickersnack Lane\nBrooklyn, NY 11229"
TEST_SHIPPING_ADDRESS = "First Middle Last\n15 Jones Boulevard\nFair Lawn,NJ 07410"


@pytest.fixture
def base_dimension_record_45():

    customer_dim = pd.DataFrame(dim_records)
    customer_dim = customer_dim.set_index("customer_key", drop=False)
    yield pd.DataFrame(customer_dim.loc[45:45])  # slice for dataframe


@pytest.fixture
def base_dimension_records_all():

    customer_dim = pd.DataFrame(dim_records)
    customer_dim = customer_dim.set_index("customer_key", drop=False)
    yield customer_dim


# def test_get_incremental_keys():
#
#     customer_data = [[1, "cust1"], [2, "cust1"], [3, "cust1"], [4, "cust1"]]
#
#     customer_address_data = [
#         [1, "custaddress1", 2],
#         [3, "custaddress3", 2],
#         [5, "custaddress5", 4],
#         [7, "custaddress7", 4],
#         [9, "custaddress9", 6],
#     ]
#
#     c = CustomerDimension()
#
#     customer = pd.DataFrame(customer_data, columns=["customer_id", "name"])
#     customer_address = pd.DataFrame(
#         customer_address_data, columns=["address_id", "name", "customer_id"]
#     )
#
#     incremental_keys = c._get_incremental_keys(customer, customer_address)
#     assert sorted(incremental_keys.tolist()) == [1, 2, 3, 4, 6]
#

def test_get_new_keys():

    c = CustomerDimensionProcessor()

    customer_dim_data = [
        [1, "cust_key_1", "name_1", "cust_addr_key_1", "address_1"],
        [2, "cust_key_2", "name_2", "cust_addr_key_2", "address_2"],
        [3, "cust_key_3", "name_3", "cust_addr_key_3", "address_3"],
    ]

    customer_dim = pd.DataFrame(customer_dim_data, columns=customer_dim_cols)

    # updates only
    incremental_keys = pd.Series(
        ["cust_key_1", "cust_key_2", "cust_key_3"], name="customer_key"
    )

    new_keys = get_new_keys(incremental_keys, customer_dim, 'customer_key')
    assert len(new_keys) == 0

    # new only
    incremental_keys = pd.Series(
        ["cust_key_4", "cust_key_5", "cust_key_6"], name="customer_key"
    )

    new_keys = get_new_keys(incremental_keys, customer_dim, 'customer_key')
    assert sorted(new_keys.to_list()) == ["cust_key_4", "cust_key_5", "cust_key_6"]

    # mix of updates and new
    incremental_keys = pd.Series(
        ["cust_key_2", "cust_key_3", "cust_key_4"], name="customer_key"
    )

    new_keys = get_new_keys(incremental_keys, customer_dim, 'customer_key')
    assert sorted(new_keys.to_list()) == ["cust_key_4"]


def test_parse_address():

    c = CustomerDimensionProcessor(None)
    address = c.parse_address(TEST_BILLING_ADDRESS)
    assert address["name"] == "First Middle Last"
    assert address["street_number"] == "123 Snickersnack Lane"
    assert address["city"] == "Brooklyn"
    assert address["state"] == "NY"
    assert address["zip"] == "11229"


# Build new customer_dim from customers and addresses.
# Customers 3 & 5 have billing and shipping; customer 4, billing only,
def test_build_new_dimension():

    c = CustomerDimensionProcessor(None)

    _day = date(2020, 10, 10)
    _date = datetime.now()
    tba = TEST_BILLING_ADDRESS
    tsa = TEST_SHIPPING_ADDRESS

    customer_data = {
        "customer_id": [1, 3, 4, 5],
        "customer_name": ["c1", "c3", "c4", "c5"],
        "customer_user_id": ["XXX"] * 4,
        "customer_password": ["XXX"] * 4,
        "customer_email": [
            "aaa@gmail.com",
            "bbb@gmail.com",
            "ccc@gmail.com",
            "ddd@gmail.com",
        ],
        "customer_user_id": ["u1", "u2", "u3", "u4"],
        "customer_referral_type": [None, " ", "OA", "AM"],
        "customer_sex": ["F", "f", "M", None],
        "customer_date_of_birth": [_day] * 4,
        "customer_loyalty_number": [1001, 1002, 1003, 1004],
        "customer_credit_card_number": ["123"] * 4,
        "customer_is_preferred": [True, False, True, True],
        "customer_is_active": [True] * 4,
        "customer_inserted_at": [_date] * 4,
        "customer_updated_at": [_date] * 4,
        "batch_id": [1] * 4,
    }
    customer_address_data = {
        "customer_id": [1, 2, 3, 3, 4, 5, 5],
        "customer_address_id": [1, 2, 3, 4, 5, 6, 7],
        "customer_address": [tsa, tba, tsa, tba, tba, tsa, tba],
        "customer_address_type": ["S", "B", "S", "B", "B", "S", "B"],
        "customer_address_inserted_at": [_date] * 7,
        "customer_address_updated_at": [_date] * 7,
        "batch_id": [1] * 7,
    }

    new_keys = pd.Index([3, 4, 5])

    customer = pd.DataFrame(customer_data).set_index('customer_id', drop=False)
    customer_address = pd.DataFrame(customer_address_data).set_index('customer_id', drop=False)

    inserts = c._build_new_dimension(new_keys, customer, customer_address)

    assert inserts.shape[0] == 3
    assert inserts.at[3, "name"] == "c3"
    assert inserts.at[3, "billing_state"] == "NY"
    assert inserts.at[3, "shipping_state"] == "NJ"
    assert inserts.at[4, "billing_city"] == "Brooklyn"
    assert inserts.at[4, "shipping_city"] == "N/A"


def test_transform_referral_type():

    customer_data = {
        "customer_id": [1, 2, 3, 4],
        "customer_name": ["c1", "c2", "c3", "c4"],
        "customer_referral_type": ["ll", " ", "OA", "AM"],
        "customer_user_id": [pd.NA] * 4,
        "customer_password": [pd.NA] * 4,
        "customer_email": [pd.NA] * 4,
        "customer_user_id": [pd.NA] * 4,
        "customer_sex": [pd.NA] * 4,
        "customer_date_of_birth": [pd.NA] * 4,
        "customer_loyalty_number": [0] * 4,
        "customer_credit_card_number": [pd.NA] * 4,
        "customer_is_preferred": [True] * 4,
        "customer_is_active": [True] * 4,
        "customer_inserted_at": [pd.NA] * 4,
        "customer_updated_at": [pd.NA] * 4,
        "batch_id": [1, 1, 1, 1],
    }

    customer_address_data = {
        "customer_id": [1],
        "customer_address_id": [1],
        "customer_address": [None],
        "customer_address_type": [None],
        "customer_address_inserted_at": [None],
        "customer_address_updated_at": [None],
        "batch_id": [1],
    }

    customer = pd.DataFrame(customer_data)
    customer = customer.set_index("customer_id", drop=False)
    customer_address = pd.DataFrame(customer_address_data)
    customer_address = customer_address.set_index("customer_id", drop=False)

    customer_dim = CustomerDimensionProcessor.customer_transform(customer, customer_address)

    assert customer_dim.shape[0] == 4
    assert customer_dim.at[1, "referral_type"] == "Unknown"
    assert customer_dim.at[2, "referral_type"] == "None"
    assert customer_dim.at[3, "referral_type"] == "Online Advertising"
    assert customer_dim.at[4, "referral_type"] == "Affiliate Marketing"


def test_update_customer_only(base_dimension_record_45):

    c = CustomerDimensionProcessor(None)
    test_time = datetime.now()

    customer_data = {
        "customer_id": [45],
        "customer_email": ["ellen123@gmail.com"],
        "customer_updated_at": test_time,
        "customer_credit_card_number": None,
        "batch_id": [1],
    }

    customer_address_data = {
        "customer_id": [],
        "customer_address_id": [],
        "customer_address": [],
        "customer_address_type": [],
        "customer_address_inserted_at": [],
        "customer_address_updated_at": [],
        "batch_id": [],
    }

    customer = pd.DataFrame(customer_data).set_index('customer_id', drop=False)
    customer_address = pd.DataFrame(customer_address_data).set_index('customer_id', drop=False)

    customer_dim = c._build_update_dimension(base_dimension_record_45.index, base_dimension_record_45,
                                             customer, customer_address)

    assert customer_dim.shape[0] == 1
    assert customer_dim.loc[45, "credit_card_number"] == "12345678"
    assert customer_dim.loc[45, "email"] == "ellen123@gmail.com"
    assert customer_dim.loc[45, "user_id"] == "Ellen456"


def test_update_customer_address_only(base_dimension_record_45):
    c = CustomerDimensionProcessor(None)
    test_time = datetime.now()

    customer_data = {
        "customer_id": [],
        "customer_updated_at": [],
        "customer_credit_card_number": [],
        "batch_id": [],
    }

    customer_address_data = {
        "customer_id": [45],
        "customer_address_id": [45],
        "customer_address": [
            "Fred Johnson\n77 Eagle Avenue\nSt. Joseph, TN 54322"
        ],  # change last digit of zip
        "customer_address_type": ["S"],
        "customer_address_inserted_at": [date(2020, 10, 10)],
        "customer_address_updated_at": [test_time],
        "batch_id": [1],
    }

    customer = pd.DataFrame(customer_data).set_index('customer_id', drop=False)
    customer_address = pd.DataFrame(customer_address_data).set_index('customer_id', drop=False)

    customer_dim = c._build_update_dimension(
        base_dimension_record_45.index, base_dimension_record_45, customer, customer_address
    )
    assert customer_dim.shape[0] == 1
    assert customer_dim.loc[45, "shipping_zip"] == "54322"
    assert customer_dim.loc[45, "billing_zip"] == "12345"
    assert customer_dim.loc[45, "user_id"] == "Ellen456"


def test_deactivate(base_dimension_record_45):

    c = CustomerDimensionProcessor(None)
    test_time = datetime.now()

    customer_data = {
        "customer_id": [45],
        "customer_updated_at": test_time,
        "customer_is_active": False,
        "batch_id": [1],
    }

    customer_address_data = {
        "customer_id": [],
        "customer_address_id": [],
        "customer_address": [],
        "customer_address_type": [],
        "customer_address_inserted_at": [],
        "customer_address_updated_at": [],
        "batch_id": [],
    }

    customer = pd.DataFrame(customer_data).set_index('customer_id', drop=False)
    customer_address = pd.DataFrame(customer_address_data).set_index('customer_id', drop=False)

    customer_dim = c._build_update_dimension(
        base_dimension_record_45.index, base_dimension_record_45, customer, customer_address
    )

    assert customer_dim.shape[0] == 1
    assert customer_dim.loc[45, "is_active"] == False
    assert customer_dim.loc[45, "deactivation_date"] == test_time


def test_activate(base_dimension_record_45):

    old_dim = base_dimension_record_45.copy()
    old_dim["is_active"] = False
    old_dim["deactivation_date"] = date(2020, 10, 15)

    c = CustomerDimensionProcessor(None)
    test_time = datetime.now()

    customer_data = {
        "customer_id": [45],
        "customer_updated_at": test_time,
        "customer_is_active": True,
        "batch_id": [1],
    }

    customer_address_data = {
        "customer_id": [],
        "customer_address_id": [],
        "customer_address": [],
        "customer_address_type": [],
        "customer_address_inserted_at": [],
        "customer_address_updated_at": [],
        "batch_id": [],
    }

    customer = pd.DataFrame(customer_data).set_index('customer_id', drop=False)
    customer_address = pd.DataFrame(customer_address_data).set_index('customer_id', drop=False)

    customer_dim = c._build_update_dimension(
        old_dim.index, old_dim, customer, customer_address
    )

    assert customer_dim.shape[0] == 1
    assert customer_dim.loc[45, "is_active"] == True
    assert customer_dim.loc[45, "activation_date"] == test_time
    assert customer_dim.loc[45, "deactivation_date"] == date(2099, 12, 31)


def test_customer_transform():
    pass


def test_latest_update():
    pass


# mix behaviors across two records
def test_update_all(base_dimension_records_all):

    c = CustomerDimensionProcessor(None)
    test_time = datetime.now()

    customer_data = {
        "customer_id": [45, 46],
        "customer_email": ["ellen123@gmail.com", None],
        "customer_updated_at": [test_time, test_time],
        "customer_credit_card_number": [None, "88888888"],
        "batch_id": [1, 1],
    }

    customer_address_data = {
        "customer_id": [45, 46, 46],
        "customer_address_id": [1, 2, 3],
        "customer_address": [
            "Fred Johnson\n77 Eagle Avenue\nSt. Joseph, TN 54322",
            "Henry Higgins\n44 Pine Street\nKenosha, WI 77777",
            "Mary Higgins\n44 Pine Street\nKenosha, WI 77777",
        ],
        "customer_address_type": ["S", "B", "S"],
        "customer_address_inserted_at": [test_time, test_time, test_time],
        "customer_address_updated_at": [test_time, test_time, test_time],
        "batch_id": [1, 1, 1],
    }

    customer = pd.DataFrame(customer_data).set_index('customer_id', drop=False)
    customer_address_stage_df = pd.DataFrame(customer_address_data).set_index('customer_id', drop=False)

    customer_dim = c._build_update_dimension(
        base_dimension_records_all.index, base_dimension_records_all, customer, customer_address_stage_df
    )

    assert customer_dim.shape[0] == 2
    assert customer_dim.loc[45, "shipping_zip"] == "54322"
    assert customer_dim.loc[45, "billing_zip"] == "12345"
    assert customer_dim.loc[45, "email"] == "ellen123@gmail.com"
    assert customer_dim.loc[45, "user_id"] == "Ellen456"
    assert customer_dim.loc[45, "credit_card_number"] == "12345678"

    assert customer_dim.loc[46, "shipping_name"] == "Mary Higgins"
    assert customer_dim.loc[46, "billing_street_number"] == "44 Pine Street"
    assert customer_dim.loc[46, "email"] == "henry@hotmail.com"
    assert customer_dim.loc[46, "credit_card_number"] == "88888888"


# change only address on record 45
def test_update_all_2(base_dimension_records_all):

    c = CustomerDimensionProcessor(None)
    test_time = datetime.now()

    customer_data = {
        "customer_id": [46],
        "customer_email": [None],
        "customer_updated_at": [test_time],
        "customer_credit_card_number": ["88888888"],
        "batch_id": [1],
    }

    customer_address_data = {
        "customer_id": [45, 46, 46],
        "customer_address_id": [1, 2, 3],
        "customer_address": [
            "Fred Johnson\n77 Eagle Avenue\nSt. Joseph, TN 54322",
            "Henry Higgins\n44 Pine Street\nKenosha, WI 77777",
            "Mary Higgins\n44 Pine Street\nKenosha, WI 77777",
        ],
        "customer_address_type": ["S", "B", "S"],
        "customer_address_inserted_at": [test_time, test_time, test_time],
        "customer_address_updated_at": [test_time, test_time, test_time],
        "batch_id": [1, 1, 1],
    }

    customer = pd.DataFrame(customer_data).set_index('customer_id', drop=False)
    customer_address = pd.DataFrame(customer_address_data).set_index('customer_id', drop=False)

    customer_dim = c._build_update_dimension(
        base_dimension_records_all.index, base_dimension_records_all, customer, customer_address
    )

    assert customer_dim.shape[0] == 2
    assert customer_dim.loc[45, "shipping_zip"] == "54322"
    assert customer_dim.loc[45, "billing_zip"] == "12345"
    assert customer_dim.loc[45, "credit_card_number"] == "12345678"

    assert customer_dim.loc[46, "shipping_name"] == "Mary Higgins"
    assert customer_dim.loc[46, "billing_street_number"] == "44 Pine Street"
    assert customer_dim.loc[46, "email"] == "henry@hotmail.com"
    assert customer_dim.loc[46, "credit_card_number"] == "88888888"

