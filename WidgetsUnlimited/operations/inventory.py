from util.sqltypes import Table
from typing import List

from .generator import DataGenerator
from .base import BaseSystem


class InventorySystem:
    def __init__(self) -> None:
        pass

    def add_tables(self, tables: List[Table]) -> None:
        # create all the postgres tables
        pass

    def open(self, table: Table) -> None:
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
