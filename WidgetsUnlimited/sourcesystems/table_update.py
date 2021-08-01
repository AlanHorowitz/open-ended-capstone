from util.sqltypes import Table

class TableUpdate():
    def __init__(self, table : Table, n_inserts : int, n_updates : int,
    batch_id: int = None, link_parent: bool = False) -> None:
        self.table = table
        self.n_inserts = n_inserts
        self.n_updates = n_updates
        self.batch_id = batch_id
        self.link_parent = link_parent

    