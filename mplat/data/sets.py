from abc import ABC, abstractmethod
import boto3
from botocore.exceptions import ClientError
from mplat.data.handlers import DataObjectHandler, AWSDataObjectHandler


class DataObject(ABC):
    """Abstract base class for all data objects."""
    def __init__(self, handler: DataObjectHandler):
        self.handler = handler
        self.metadata = {}
        self.url = ""


class Dataset(ABC):
    """Abstract base class for all datasets."""
    def __init__(self, handler: DataObjectHandler):
        self.handler = handler
        self.data = {}

    # @abstractmethod
    # def create(self, *args, **kwargs):
    #     pass