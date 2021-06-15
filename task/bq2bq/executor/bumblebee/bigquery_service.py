import json
import sys
from abc import ABC, abstractmethod

import google as google
from google.api_core.exceptions import BadRequest, Forbidden
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig, CreateDisposition
from google.cloud.bigquery.schema import _parse_schema_resource
from google.cloud.bigquery.table import TimePartitioningType, TimePartitioning, TableReference, Table
from google.cloud.exceptions import GoogleCloudError

from bumblebee.config import TaskConfigFromEnv
from bumblebee.log import get_logger

logger = get_logger(__name__)


class BaseBigqueryService(ABC):

    @abstractmethod
    def execute_query(self, query):
        pass

    @abstractmethod
    def transform_load(self,
                       query,
                       source_project_id=None,
                       destination_table=None,
                       write_disposition=None,
                       create_disposition=CreateDisposition.CREATE_NEVER):
        pass

    @abstractmethod
    def create_table(self, full_table_name, schema_file,
                     partitioning_type=TimePartitioningType.DAY,
                     partitioning_field=None):
        pass

    @abstractmethod
    def delete_table(self, full_table_name):
        pass

    @abstractmethod
    def get_table(self, full_table_name):
        pass


class BigqueryService(BaseBigqueryService):

    def __init__(self, client, labels, writer):
        """

        :rtype:
        """
        self.client = client
        self.labels = labels
        self.writer = writer

    def execute_query(self, query):
        query_job_config = QueryJobConfig()
        query_job_config.use_legacy_sql = False
        query_job_config.labels = self.labels

        if query is None or len(query) == 0:
            raise ValueError("query must not be Empty")

        logger.info("executing query")
        query_job = self.client.query(query=query,
                                      job_config=query_job_config)
        logger.info("Job {} is initially in state {} of {} project".format(query_job.job_id, query_job.state,
                                                                           query_job.project))
        try:
            result = query_job.result()
        except (GoogleCloudError, Forbidden, BadRequest) as ex:
            self.writer.write("error", ex.message)
            logger.error(ex)
            sys.exit(1)

        logger.info("Job {} is finally in state {} of {} project".format(query_job.job_id, query_job.state,
                                                                         query_job.project))
        logger.info("Bytes processed: {}, Affected Rows: {}, Bytes billed: {}".format(query_job.estimated_bytes_processed,
                                                               query_job.num_dml_affected_rows,
                                                               query_job.total_bytes_billed))
        logger.info("Job labels {}".format(query_job._configuration.labels))
        return result

    def transform_load(self,
                       query,
                       source_project_id=None,
                       destination_table=None,
                       write_disposition=None,
                       create_disposition=CreateDisposition.CREATE_NEVER):
        if query is None or len(query) == 0:
            raise ValueError("query must not be Empty")

        query_job_config = QueryJobConfig()
        query_job_config.create_disposition = create_disposition
        query_job_config.write_disposition = write_disposition
        query_job_config.use_legacy_sql = False
        query_job_config.labels = self.labels

        if destination_table is not None:
            table_ref = TableReference.from_string(destination_table)
            query_job_config.destination = table_ref

        logger.info("transform load")
        query_job = self.client.query(query=query, job_config=query_job_config, project=source_project_id)
        logger.info("Job {} is initially in state {} of {} project".format(query_job.job_id, query_job.state,
                                                                           query_job.project))

        try:
            result = query_job.result()
        except (GoogleCloudError, Forbidden, BadRequest) as ex:
            self.writer.write("error", ex.message)
            logger.error(ex)
            sys.exit(1)

        logger.info("Job {} is finally in state {} of {} project".format(query_job.job_id, query_job.state,
                                                                           query_job.project))
        logger.info("Bytes processed: {}, Stats: {} {}".format(query_job.estimated_bytes_processed,
                                                               query_job.num_dml_affected_rows,
                                                               query_job.total_bytes_billed))
        logger.info("Job labels {}".format(query_job._configuration.labels))
        return result

    def create_table(self, full_table_name, schema_file,
                     partitioning_type=TimePartitioningType.DAY,
                     partitioning_field=None):
        with open(schema_file, 'r') as file:
            schema_json = json.load(file)
        table_schema = _parse_schema_resource({'fields': schema_json})

        table_ref = TableReference.from_string(full_table_name)

        bigquery_table = bigquery.Table(table_ref, table_schema)
        bigquery_table.time_partitioning = TimePartitioning(type_=partitioning_type,
                                                            field=partitioning_field)

        self.client.create_table(bigquery_table)

    def delete_table(self, full_table_name):
        table_ref = TableReference.from_string(full_table_name)
        self.client.delete_table(bigquery.Table(table_ref))

    def get_table(self, full_table_name):
        table_ref = TableReference.from_string(full_table_name)
        return self.client.get_table(table_ref)


def create_bigquery_service(task_config: TaskConfigFromEnv, labels, writer):

    if writer is None:
        writer = writer.StdWriter()

    SCOPE = ('https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive')
    credentials, _ = google.auth.default(scopes=SCOPE)
    client = bigquery.Client(project=task_config.destination_project, credentials=credentials)
    bigquery_service = BigqueryService(client, labels, writer)
    return bigquery_service


class DummyService(BaseBigqueryService):

    def execute_query(self, query):
        logger.info("execute query : {}".format(query))
        return []

    def transform_load(self, query, source_project_id=None, destination_table=None, write_disposition=None,
                       create_disposition=CreateDisposition.CREATE_NEVER):
        log = """ transform and load with config :
        {}
        {}
        {}
        {}""".format(query, source_project_id, destination_table, write_disposition)
        logger.info(log)

    def create_table(self, full_table_name, schema_file, partitioning_type=TimePartitioningType.DAY,
                     partitioning_field=None):
        log = """ create table with config :
        {}
        {}
        {}
        {}""".format(full_table_name, schema_file, partitioning_type, partitioning_field)
        logger.info(log)

    def delete_table(self, full_table_name):
        logger.info("delete table: {}".format(full_table_name))

    def get_table(self, full_table_name):
        return Table.from_string(full_table_name)
