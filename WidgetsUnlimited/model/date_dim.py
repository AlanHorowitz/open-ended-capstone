from .metadata import Table, Column


class DateDimTable(Table):

    NAME = "date_dim"

    def __init__(self):
        super().__init__(
            DateDimTable.NAME,
            # dimension control header columns
            Column("surrogate_key", "INTEGER", primary_key=True),  # surrogate key
            Column("date", "DATE"),

            Column("date_text_description", "VARCHAR"),
            Column("day_name", "VARCHAR"),
            Column("month_name", "VARCHAR"),
            Column("year_name", "VARCHAR"),
            Column("day_number_in_month", "INTEGER"),
            Column("day_number_in_year", "INTEGER"),

            Column("weekday_indicator", "VARCHAR"),
            Column("holiday_indicator", "VARCHAR"),

            create_only=True,
            batch_id=False,
        )

    def get_header_columns(self):
        return [col.get_name() for col in self.get_columns()[0:2]]
