from util.sqltypes import Table
from typing import List
from .OperationalSystem import operationalSystem

class InventorySystem(operationalSystem):
    def __init__(self) -> None:
        # open connection to postgres
        pass

    def add_tables(tables : List[Table]) -> None:
        # create all the postgres tables
        pass

    def open(table : Table) -> None:
        pass

    def close(table):
        # extract table to csv and write to disk
        # How to do ftp in docker image (in my python image it's bad enough)
        # but how to do it from a postgres image.
        pass

    def insert(table, records):
        pass

    def update(table, records):
        pass