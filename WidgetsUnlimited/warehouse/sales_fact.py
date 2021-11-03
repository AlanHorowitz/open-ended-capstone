from .dimension_processor import DimensionProcessor
from WidgetsUnlimited.model.sales_fact import SalesFactTable


class SalesFactsProcessor(DimensionProcessor):
    pass

    def __init__(self, connection=None):

        super().__init__(connection, SalesFactTable())

    def process_update(self, batch_id: int) -> None:
        pass