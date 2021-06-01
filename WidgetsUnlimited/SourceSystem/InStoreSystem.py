from util.sqltypes import Table, Column
from typing import List
from .OperationalSystem import OperationalSystem

class InStoreOperationalSystem(OperationalSystem):
    def __init__(self) -> None:
        # open connection to postgres
        pass

    def add_tables(tables : List[Table]) -> None:
        # create all the kafka topics
        pass

    def open(table : Table) -> None:
        # get kafka producer
        pass

    def close(table):
        # flush and close the producer
        pass

    def insert(table, records):
        pass

    def update(table, records):
        pass