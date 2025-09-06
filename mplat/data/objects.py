from abc import ABC, abstractmethod
from io import StringIO
import pandas as pd
from datetime import datetime as dt
from mplat.data.handlers import DataObjectHandler


class DataObject(ABC):
    """Abstract base class for all data objects."""
    def __init__(self, name, handler: DataObjectHandler):
        self.name = name
        self.handler = handler

    @abstractmethod
    def read(self, *args, **kwargs):
        pass


class CSVDataObject(DataObject):
    """Concrete implementation of DataObject for CSV files."""
    def __init__(self, name, data, handler: DataObjectHandler):
        super().__init__(name, handler)
        self.handler.write(name, data = StringIO(data.to_csv(index=False)).getvalue())

    def read(self, *args, **kwargs):
        data = self.handler.read(*args, **kwargs)
        if data:
            return pd.read_csv(StringIO(data))
        return None
    

class DataSource(ABC):
    """Abstract base class for all data sources."""
    @abstractmethod
    def fetch(self, *args, **kwargs):
        pass