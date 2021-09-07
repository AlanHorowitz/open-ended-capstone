from model.sqltypes import Table


class TableTransaction:
    def __init__(
        self,
        table: Table,
        n_inserts: int = 0,
        n_updates: int = 0,
        link_parent: bool = False,
    ) -> None:
        self.table = table
        self.n_inserts = n_inserts
        self.n_updates = n_updates
        self.link_parent = link_parent
