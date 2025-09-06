import os
import logging
import datetime as dt
from io import StringIO
from abc import ABC, abstractmethod
import pandas as pd
import boto3
from botocore.exceptions import ClientError


class DataObjectHandler(ABC):
    """Abstract base class for data object handlers."""
    @abstractmethod
    def read(self, *args, **kwargs):
        pass

    @abstractmethod
    def write(self, *args, **kwargs):
        pass
    
    @abstractmethod
    def move(self, *args, **kwargs):
        pass

    @abstractmethod
    def copy(self, *args, **kwargs):
        pass    

    @abstractmethod
    def remove(self, *args, **kwargs):
        pass


class AWSDataObjectHandler(DataObjectHandler):
    """Concrete implementation of DataObjectHandler for AWS S3."""
    def __init__(self, bucket_name: str):
        self.metadata = {
            "bucket_name": ""
        }
        self.s3 = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.bucket = self.s3.Bucket(bucket_name)

    def read(self, key):
        try:
            obj = self.s3.Object(self.bucket_name, key)
            data = obj.get()['Body'].read().decode('utf-8')
            return data
        except ClientError as e:
            logging.error(e)
            return None

    def write(self, key, data):
        try:
            obj = self.s3.Object(self.bucket_name, key)
            obj.put(Body=data)
            return True
        except ClientError as e:
            logging.error(e)
            return False

    def move(self, source_key, dest_key):
        try:
            copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
            self.s3.Object(self.bucket_name, dest_key).copy(copy_source)
            self.s3.Object(self.bucket_name, source_key).delete()
            return True
        except ClientError as e:
            logging.error(e)
            return False

    def copy(self, source_key, dest_key):
        try:
            copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
            self.s3.Object(self.bucket_name, dest_key).copy(copy_source)
            return True
        except ClientError as e:
            logging.error(e)
            return False

    def remove(self, key):
        try:
            obj = self.s3.Object(self.bucket_name, key)
            obj.delete()
            return True
        except ClientError as e:
            logging.error(e)
            return False