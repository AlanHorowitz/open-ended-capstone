"""A simulated data warehouse for the retail domain.

The data from 3 different source systems representing e-commerce, in-store sales,
and inventory will be consolidated into a data warehouse.  
"""
import os
from logging.config import fileConfig
from logging import LogRecord, setLogRecordFactory


class WidgetsLogRecord(LogRecord):

    def __init__(self, name, level, pathname, lineno, msg, args, exc_info, func=None, sinfo=None, **kwargs):

        self.short_name = name.replace('WidgetsUnlimited.', 'wu.', 1).\
            replace('warehouse.', 'wh.', 1).\
            replace('operations.', 'op.', 1)
        super().__init__(name, level, pathname, lineno, msg, args, exc_info, func, sinfo, **kwargs)


setLogRecordFactory(WidgetsLogRecord)
conf = os.environ.get("WIDGETS_LOG_CONF")
if conf:
    fileConfig(conf)
else:
    print("Error no log configuration found")
    exit(1)