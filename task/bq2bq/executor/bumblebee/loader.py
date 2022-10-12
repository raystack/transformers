from bumblebee.bigquery_service import BigqueryService
from datetime import datetime

from abc import ABC
from abc import abstractmethod
from bumblebee.config import LoadMethod

class BaseLoader(ABC):

    @abstractmethod
    def load(self, query):
        pass


class PartitionLoader(BaseLoader):

    def __init__(self, bigquery_service, destination: str, load_method: LoadMethod, partition: datetime, allow_field_addition=False):
        self.bigquery_service = bigquery_service
        self.destination_name = destination
        self.load_method = load_method
        self.partition_date = partition
        self.allow_field_addition = allow_field_addition

    def load(self, query):
        partition_date_str = self.partition_date.strftime("%Y%m%d")
        load_destination = "{}${}".format(self.destination_name, partition_date_str)
        write_disposition = self.load_method.write_disposition
        allow_field_addition = self.allow_field_addition
        return self.bigquery_service.transform_load(query=query,
                                                    write_disposition=write_disposition,
                                                    destination_table=load_destination,
                                                    allow_field_addition=allow_field_addition)


class TableLoader(BaseLoader):

    def __init__(self, bigquery_service, destination: str, load_method: LoadMethod, allow_field_addition=False):
        self.bigquery_service = bigquery_service
        self.full_table_name = destination
        self.load_method = load_method
        self.allow_field_addition = allow_field_addition

    def load(self, query):
        return self.bigquery_service.transform_load(query=query,
                                                    write_disposition=self.load_method.write_disposition,
                                                    destination_table=self.full_table_name,
                                                    allow_field_addition=self.allow_field_addition)


class DMLLoader(BaseLoader):
    def __init__(self,bigquery_service: BigqueryService, destination: str):
        self.bigquery_service = bigquery_service
        self.full_table_name = destination

    def load(self,query):
        return self.bigquery_service.execute_query(query)
