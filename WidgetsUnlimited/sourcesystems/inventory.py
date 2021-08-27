from util.sqltypes import Table
from typing import List

from .generator import DataGenerator
from .base import BaseSystem


class InventorySystem(BaseSystem):
    def __init__(self, data_generator) -> None:
        super().__init__(data_generator)         

    def add_tables(self, tables : List[Table]) -> None:
        # create all the postgres tables
        super().add_tables(tables)


    def open(self, table : Table) -> None:
        pass

    def close(self, table):
        # extract table to csv and write to disk
        # How to do ftp in docker image (in my python image it's bad enough)
        # but how to do it from a postgres image.
        pass

    def insert(self, table, records):
        pass

    def update(self, table, records):
        pass