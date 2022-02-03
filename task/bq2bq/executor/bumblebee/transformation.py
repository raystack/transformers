import asyncio
import concurrent
import math
from datetime import datetime, timedelta
from typing import List, TypeVar

from bumblebee.bigquery_service import BigqueryService
from bumblebee.config import LoadMethod, TaskConfig
from bumblebee.datehelper import localise_datetime
from bumblebee.loader import BaseLoader, TableLoader, DMLLoader, PartitionLoader
from bumblebee.log import get_logger
from bumblebee.query import Query, DestinationParameter, WindowParameter, ExecutionParameter, MergeReplaceQuery
from bumblebee.window import WindowFactory, Window, CustomWindow
from concurrent.futures import ThreadPoolExecutor
from google.cloud.bigquery.table import TimePartitioningType

logger = get_logger(__name__)

OPTIMUS_QUERY_BREAK_MARKER = "--*--optimus-break-marker--*--"


class Transformation:
    def __init__(self,
                 bigquery_service: BigqueryService,
                 task_config: TaskConfig,
                 sql_query: str,
                 spillover_query: str,
                 dstart: datetime,
                 dend: datetime,
                 execution_time: datetime,
                 dry_run: bool):
        self.bigquery_service = bigquery_service
        self.task_config = task_config
        self.sql_query = sql_query
        self.dstart = dstart
        self.dend = dend
        self.execution_time = execution_time
        self.dry_run = dry_run

    def transform(self):
        self.task_config.print()

        localised_dstart = localise_datetime(self.dstart, self.task_config.timezone)
        localised_dend = localise_datetime(self.dend, self.task_config.timezone)
        localised_execution_time = localise_datetime(self.execution_time, self.task_config.timezone)
        logger.info("localized dstart: {}".format(localised_dstart))
        logger.info("localized dend: {}".format(localised_dend))
        logger.info("localized execution time: {}".format(localised_execution_time))

        if self.task_config.load_method is LoadMethod.MERGE:
            task = DMLBasedTransformation(self.bigquery_service,
                                          self.task_config,
                                          self.sql_query,
                                          localised_dstart,
                                          localised_dend,
                                          self.dry_run,
                                          localised_execution_time)
            task.execute()
        elif self.task_config.load_method is LoadMethod.APPEND:
            transformation = TableTransformation(self.bigquery_service,
                                                 self.task_config,
                                                 self.sql_query,
                                                 localised_dstart,
                                                 localised_dend,
                                                 self.dry_run,
                                                 localised_execution_time)
            transformation.transform()
        elif self.task_config.load_method is LoadMethod.REPLACE:
            # query bq and check if table is partitioned
            bq_destination_table = self.bigquery_service.get_table(self.task_config.destination_table)
            if bq_destination_table.time_partitioning is None:
                task_queries = self.sql_query.split(OPTIMUS_QUERY_BREAK_MARKER)
                transformation = TableTransformation(self.bigquery_service,
                                                     self.task_config,
                                                     task_queries[0],
                                                     self.dstart,
                                                     self.dend,
                                                     self.dry_run,
                                                     localised_execution_time)
            elif bq_destination_table.partitioning_type == "DAY":
                partition_strategy = timedelta(days=1)

                # queries where source data/partition directly map with destination partitions
                transformation = MultiPartitionTransformation(self.bigquery_service,
                                                              self.task_config,
                                                              self.sql_query,
                                                              self.dstart, self.dend,
                                                              self.dry_run,
                                                              localised_execution_time,
                                                              partition_strategy,
                                                              self.task_config.concurrency)
            else:
                raise Exception("unable to generate a transformation for request, unsupported partition strategy")
            transformation.transform()
        elif self.task_config.load_method is LoadMethod.REPLACE_MERGE:
            # query bq and check if table is partitioned
            bq_destination_table = self.bigquery_service.get_table(self.task_config.destination_table)
            if bq_destination_table.time_partitioning is None and bq_destination_table.range_partitioning is None:
                transformation = TableTransformation(self.bigquery_service,
                                                     self.task_config,
                                                     self.sql_query,
                                                     localised_dstart,
                                                     localised_dend,
                                                     self.dry_run,
                                                     localised_execution_time)
            else:
                partition_type = bq_destination_table.time_partitioning if bq_destination_table.time_partitioning is not None \
                    else bq_destination_table.range_partitioning

                # ingestion time partitioned has field as None
                if partition_type.field is None and self.task_config.filter_expression is None:
                    raise Exception("partition filter is required for tables partitioned with INGESTION TIME, "
                                    "for eg: date(`_PARTITIONTIME`) >= date('{{.DSTART}}') AND date(`_PARTITIONTIME`) < date('{{.DEND}}')"
                                    "")

                partition_column_name = partition_type.field
                partition_column_type = "DATE"

                table_columns = []
                for field in bq_destination_table.schema:
                    table_columns.append(field.name)
                    if field.name == partition_column_name:
                        partition_column_type = field.field_type

                logger.info("table columns: {}, partitioned on: {}".format(table_columns, partition_column_name,
                                                                           partition_column_type))

                transformation = MergeReplaceTransformation(self.bigquery_service,
                                                            self.task_config,
                                                            self.sql_query,
                                                            table_columns,
                                                            partition_column_name,
                                                            partition_column_type,
                                                            self.dry_run,
                                                            localised_execution_time
                                                            )

            transformation.transform()
        elif self.task_config.load_method is LoadMethod.REPLACE_ALL:
            # query bq and check if table is partitioned
            bq_destination_table = self.bigquery_service.get_table(self.task_config.destination_table)
            if bq_destination_table.time_partitioning is None and bq_destination_table.range_partitioning is None:
                task_queries = self.sql_query.split(OPTIMUS_QUERY_BREAK_MARKER)
                transformation = TableTransformation(self.bigquery_service,
                                                     self.task_config,
                                                     task_queries[0],
                                                     self.dstart,
                                                     self.dend,
                                                     self.dry_run,
                                                     localised_execution_time)
            else:
                # queries where source data/partition map with start date as destination partition
                transformation = SinglePartitionTransformation(self.bigquery_service,
                                                              self.task_config,
                                                              self.sql_query,
                                                              self.dstart, self.dend,
                                                              self.dry_run,
                                                              localised_execution_time,)
            transformation.transform()
        else:
            raise Exception("unsupported load method {}".format(self.task_config.load_method))


class DMLBasedTransformation:
    def __init__(self, bigquery_service,
                 task_config: TaskConfig,
                 task_query: str,
                 dstart: datetime,
                 dend: datetime,
                 dry_run: bool,
                 execution_time: datetime):
        self.loader = DMLLoader(bigquery_service, task_config.destination_table)
        self.query = task_query
        self.dry_run = dry_run
        self.destination_table = task_config.destination_table
        self.execution_time = execution_time
        self.window = CustomWindow(dstart, dend)

    def execute(self):
        logger.info("starting DML transformation job")

        execution_parameter = ExecutionParameter(self.execution_time)
        destination_parameter = DestinationParameter(self.destination_table)
        window_parameter = WindowParameter(self.window)

        query_parameters = [destination_parameter, window_parameter, execution_parameter]

        query = Query(self.query)
        for parameter in query_parameters:
            query = query.apply_parameter(parameter)
        query.print_with_logger(logger)

        result = None

        if not self.dry_run:
            result = self.loader.load(query)

        logger.info(result)
        logger.info("finished")


class TableTransformation:
    """
    Query transformation effects whole non partitioned table
    """

    def __init__(self, bigquery_service: BigqueryService,
                 task_config: TaskConfig,
                 task_query: str,
                 dstart: datetime,
                 dend: datetime,
                 dry_run: bool,
                 execution_time: datetime):
        self.bigquery_service = bigquery_service
        self.task_config = task_config
        self.task_query = task_query
        self.dry_run = dry_run
        self.window = CustomWindow(dstart, dend)
        self.execution_time = execution_time

    def transform(self):
        loader = TableLoader(self.bigquery_service, self.task_config.destination_table, self.task_config.load_method)
        logger.info("create transformation for table")

        task = PartitionTransformation(self.task_config,
                                       loader,
                                       self.task_query,
                                       self.window,
                                       self.dry_run,
                                       self.execution_time)
        task.execute()


class SinglePartitionTransformation:
    """
    Query transformation effects only a single partition

    queries like aggregate where source partitions don't
    directly map to destination partitions
    """

    def __init__(self, bigquery_service: BigqueryService,
                 task_config: TaskConfig,
                 task_query: str,
                 dstart: datetime,
                 dend: datetime,
                 dry_run: bool,
                 execution_time: datetime):
        self.bigquery_service = bigquery_service
        self.task_config = task_config
        self.task_query = task_query

        self.dry_run = dry_run
        self.window = CustomWindow(dstart, dend)
        self.execution_time = execution_time

    def transform(self):
        destination_partition = self.window.start
        loader = PartitionLoader(self.bigquery_service, self.task_config.destination_table,
                                 self.task_config.load_method, destination_partition)
        logger.info("create transformation for partition: {}".format(destination_partition))

        task = PartitionTransformation(self.task_config,
                                       loader,
                                       self.task_query,
                                       self.window,
                                       self.dry_run,
                                       self.execution_time)
        task.execute()


class PartitionTransformation:
    def __init__(self,
                 task_config: TaskConfig,
                 loader: BaseLoader,
                 query: str,
                 window: Window,
                 dry_run: bool,
                 execution_time: datetime):
        self.dry_run = dry_run
        self.loader = loader

        destination_parameter = DestinationParameter(task_config.destination_table)
        window_parameter = WindowParameter(window)
        execution_parameter = ExecutionParameter(execution_time)

        self.query = Query(query).apply_parameter(window_parameter).apply_parameter(
            execution_parameter).apply_parameter(destination_parameter)

    def execute(self):
        logger.info("start transformation job")
        self.query.print_with_logger(logger)

        result = None
        if not self.dry_run:
            result = self.loader.load(self.query)

        logger.info(result)
        logger.info("finished")

    async def async_execute(self):
        self.execute()


class MergeReplaceTransformation:
    """
    Query replaces the effected partitions. Partitions can be derived either via a filter expression
    or executing a query to a temporary table and running a distinct query.
    - Using a filter expression is cheaper and faster
    - Auto partition resolution actually consumes double the size of requested query
    """

    def __init__(self, bigquery_service: BigqueryService,
                 task_config: TaskConfig,
                 task_query: str,
                 table_columns: list,
                 partition_column_name: str,
                 partition_column_type: str,
                 dry_run: bool,
                 execution_time: datetime
                 ):
        self.loader = DMLLoader(bigquery_service, task_config.destination_table)
        self.task_query = task_query
        self.task_config = task_config
        self.dry_run = dry_run
        self.execution_time = execution_time
        self.filter_expression = task_config.filter_expression
        self.table_columns = table_columns
        self.partition_column_name = partition_column_name
        self.partition_column_type = partition_column_type

    def transform(self):
        execution_parameter = ExecutionParameter(self.execution_time)
        destination_parameter = DestinationParameter(self.task_config.destination_table)

        query_parameters = [destination_parameter, execution_parameter]

        query = Query(self.task_query)
        for parameter in query_parameters:
            query = query.apply_parameter(parameter)
        query.print_with_logger(logger)

        if self.filter_expression is not None:
            logger.info("running with filter expression set {}".format(self.filter_expression))
            query = MergeReplaceQuery(query).from_filter(self.task_config.destination_table, self.table_columns,
                                                         self.table_columns, self.filter_expression)
        else:
            logger.info("running with auto partition filter")
            query = MergeReplaceQuery(query).auto(self.task_config.destination_table, self.table_columns,
                                                  self.table_columns, self.partition_column_name,
                                                  self.partition_column_type)
        query.print_with_logger(logger)

        result = None
        if not self.dry_run:
            result = self.loader.load(query)

        logger.info("finished {}".format(result.total_rows))


class MultiPartitionTransformation:
    """
    Query transformation effects multiple partitions

    queries where source data/partition directly map with destination partitions
    """

    def __init__(self, bigquery_service: BigqueryService,
                 task_config: TaskConfig,
                 task_query: str,
                 dstart: datetime,
                 dend: datetime,
                 dry_run: bool,
                 execution_time: datetime,
                 partition_delta: timedelta,
                 concurrency: int
                 ):
        self.bigquery_service = bigquery_service
        self.task_query = task_query
        self.task_config = task_config
        self.dry_run = dry_run
        self.execution_time = execution_time
        self.window = CustomWindow(dstart, dend)
        self.concurrency = concurrency
        self.partition_delta = partition_delta

    def transform(self):
        datetime_list = []
        execute_for = self.window.start

        # tables are partitioned for day
        # iterate from start to end for each partition
        while execute_for < self.window.end:
            datetime_list.append(execute_for)
            execute_for += self.partition_delta

        # break query file
        task_queries = self.task_query.split(OPTIMUS_QUERY_BREAK_MARKER)
        if len(task_queries) < len(datetime_list):
            raise Exception(
                "query needs to be broken using {}, {} query found, needed {}\n{}".format(OPTIMUS_QUERY_BREAK_MARKER,
                                                                                          len(task_queries),
                                                                                          len(datetime_list),
                                                                                          self.task_query))

        tasks = []
        query_index = 0
        for partition_time in datetime_list:
            task_window = WindowFactory.create_window_with_time(partition_time, partition_time + self.partition_delta)
            destination_partition = task_window.end - self.partition_delta

            logger.info("create transformation for partition: {}".format(destination_partition))
            task_loader = PartitionLoader(self.bigquery_service, self.task_config.destination_table,
                                          self.task_config.load_method, destination_partition)

            task = PartitionTransformation(self.task_config,
                                           task_loader,
                                           task_queries[query_index],
                                           task_window,
                                           self.dry_run,
                                           self.execution_time)
            tasks.append(task)
            query_index += 1

        executor = ConcurrentTaskExecutor(self.concurrency)
        executor.execute(tasks)


class LegacySpilloverTransformation:
    """
    Query transformation effects multiple partitions
    """

    def __init__(self,
                 bigquery_service: BigqueryService,
                 task_config: TaskConfig,
                 sql_query: str,
                 spillover_query: str,
                 start_time: datetime,
                 dry_run: bool,
                 execution_time: datetime):
        self.bigquery_service = bigquery_service
        self.task_config = task_config
        self.sql_query = sql_query
        self.spillover_query = spillover_query
        self.dry_run = dry_run
        self.start_time = start_time
        self.execution_time = execution_time

        self.concurrency = self.task_config.concurrency

    def transform(self):
        datetime_list = []
        default_datetime = [self.start_time]
        datetime_list.extend(default_datetime)

        if self.task_config.use_spillover:
            spillover = SpilloverDatetimes(self.bigquery_service,
                                           self.spillover_query,
                                           self.task_config,
                                           self.start_time,
                                           self.dry_run,
                                           self.execution_time)
            spillover_datetimes = spillover.collect_datetimes()
            datetime_list.extend(spillover_datetimes)

        datetime_list = distinct_list(datetime_list)

        tasks = []
        for partition_time in datetime_list:
            logger.info("create transformation for partition: {}".format(partition_time))
            loader = PartitionLoader(self.bigquery_service, self.task_config.destination_table,
                                     self.task_config.load_method, partition_time)

            task = PartitionTransformation(self.task_config,
                                           loader,
                                           self.sql_query,
                                           self.window,
                                           self.dry_run,
                                           self.execution_time)
            tasks.append(task)

        executor = ConcurrentTaskExecutor(self.concurrency)
        executor.execute(tasks)


class SpilloverDatetimes:
    def __init__(self, bigquery_service: BigqueryService,
                 query: str,
                 task_config: TaskConfig,
                 dstart: datetime,
                 dend: datetime,
                 dry_run: bool,
                 execution_time: datetime):
        self.bigquery_service = bigquery_service
        self.query = query
        self.timezone = task_config.timezone
        self.execution_time = execution_time
        self.dry_run = dry_run
        self.destination_table = task_config.destination_table
        self.window = CustomWindow(dstart, dend)

    def collect_datetimes(self):
        window_parameter = WindowParameter(self.window)
        execution_parameter = ExecutionParameter(self.execution_time)
        destination_parameter = DestinationParameter(self.destination_table)

        query = Query(self.query)
        query = query.apply_parameter(window_parameter).apply_parameter(execution_parameter).apply_parameter(
            destination_parameter)
        query.print()

        results = None
        if not self.dry_run:
            results = self.bigquery_service.execute_query(query)

        dates = [row[0] for row in results]
        datetimes = [datetime.combine(d, datetime.min.time()) for d in dates]
        localised_datetimes = [localise_datetime(dtime, self.timezone) for dtime in datetimes]
        return localised_datetimes


T = TypeVar('T')


class ConcurrentTaskExecutor:
    def __init__(self, concurrency: int):
        self.concurrency = concurrency

    def execute(self, tasks: List[T]):
        if tasks is not None and len(tasks) > 0:
            self._concurrent_execute_task(tasks, self.concurrency)

    def execute_task(self, task):
        return task.execute()

    # TODO: future should check for task exception
    def _concurrent_execute_task(self, tasks, concurrency: int):

        with ThreadPoolExecutor(concurrency) as executor:
            futures = {executor.submit(self.execute_task, task): task for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                future.result()



def distinct_list(a_list: List) -> List:
    d = dict()
    for item in a_list:
        d[item] = 0

    result = []
    for key in d.keys():
        result.append(key)

    return result


def split_list(a_list: List, size: int) -> List[List]:
    l = a_list.copy()

    z = []
    by = int(math.ceil(len(l) / size))

    for i in range(by):
        x = []
        for j in range(size):
            if len(l) > 0:
                a = l.pop()
                x.append(a)
        z.append(x)
    return z
