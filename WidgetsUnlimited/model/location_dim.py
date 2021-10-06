from .metadata import Table, Column


class LocationDimTable(Table):
    NAME = "location_dim"

    def __init__(self):
        super().__init__(
            LocationDimTable.NAME,
            Column("surrogate_key", "INTEGER", primary_key=True),
            Column("region_name", "VARCHAR"),
            Column("location_name", "VARCHAR"),
            Column("number_of_customers", "INTEGER"),
            Column("number_of_stores", "INTEGER"),
            Column("square_footage_of_stores", "FLOAT"),
            create_only=True,
            batch_id=False,
        )

    def get_header_columns(self):
        return [col.get_name() for col in self.get_columns()[0:2]]

    @staticmethod
    def get_location_data():
        # Use 15 locations in 6 regions
        # Each location covers a list of zip code ranges

        location_data = {
            1: ('LOC1',   'REG1', ((0, 9999), (11000, 19999))),
            2: ('LOC2',   'REG1', ((10000, 10999), (20000, 23999))),
            3: ('LOC3',   'REG1', ((24000, 27999),)),
            4: ('LOC4',   'REG2', ((28000, 29999), (40000, 42999))),
            5: ('LOC5',   'REG2', ((30000, 34999), (36000, 37999), (39000, 39999))),
            6: ('LOC6',   'REG3', ((35000, 35999), (38000, 38999))),
            7: ('LOC7',   'REG3', ((43000, 49999),)),
            8: ('LOC8',   'REG3', ((50000, 54999),)),
            9: ('LOC9',   'REG4', ((55000, 59999),)),
            10: ('LOC10', 'REG5', ((60000, 64999),)),
            11: ('LOC11', 'REG5', ((65000, 69999),)),
            12: ('LOC12', 'REG5', ((70000, 71999), (73000, 79999))),
            13: ('LOC13', 'REG5', ((72000, 72999), (80000, 87999))),
            14: ('LOC14', 'REG6', ((88000, 92999),)),
            15: ('LOC15', 'REG6', ((93000, 99999),)),
        }

        return location_data

    @staticmethod
    def get_location_from_zip(zip: int) -> int:
        return 0

