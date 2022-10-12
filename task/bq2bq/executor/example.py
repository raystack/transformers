from bumblebee.bq2bq import bq2bq
from datetime import datetime, timezone
import os

execution_date = datetime.utcnow()


def use_spillover():
    bq2bq(
        "./samples/tasks/legacy/use_spillover/properties.cfg",
        "./samples/tasks/legacy/use_spillover/query.sql",
        "./samples/tasks/legacy/use_spillover/spillover_date.sql",
        datetime(2019, 5, 6), datetime(2019, 5, 7), execution_date)


def not_use_spillover():
    bq2bq(
        "./samples/tasks/legacy/not_use_spillover/properties.cfg",
        "./samples/tasks/legacy/not_use_spillover/query.sql",
        "./samples/tasks/legacy/not_use_spillover/spillover_date.sql",
        datetime(2019, 5, 6), datetime(2019, 5, 7), execution_date)


def partition_by_column():
    bq2bq(
        "./samples/tasks/partition_by_column/properties.cfg",
        "./samples/tasks/partition_by_column/query.sql",
        "./samples/tasks/partition_by_column/spillover_date.sql",
        datetime(2019, 3, 24), datetime(2019, 3, 25, 4), execution_date)


def partition_by_ingestiontime():
    bq2bq(
        "./samples/tasks/partition_by_ingestiontime/properties.cfg",
        "./samples/tasks/partition_by_ingestiontime/query.sql",
        "./samples/tasks/partition_by_ingestiontime/spillover_date.sql",
        datetime(2019, 3, 21), datetime(2019, 3, 22, 4), execution_date)


def dml():
    bq2bq(
        "./samples/tasks/dml/properties.cfg",
        "./samples/tasks/dml/query.sql",
        None,
        datetime(2019, 4, 9),
        datetime(2020, 4, 10),
        execution_date,
        False)


def dml_dry_run():
    bq2bq(
        "./samples/tasks/dml/properties.cfg",
        "./samples/tasks/dml/query.sql",
        None,
        datetime(2019, 4, 9),
        datetime(2020, 4, 10),
        execution_date,
        True)


def select_append_query():
    bq2bq(
        "./samples/tasks/select/select/properties.cfg",
        "./samples/tasks/select/select/query.sql",
        None,
        datetime(2020, 5, 25, 4),
        datetime(2020, 5, 26, 4),
        execution_date,
        False,
        {
            "deployment": "local-test-run",
            "org": "local",
            "landscape": "test",
            "environment": "local"
        }
    )


def delete_from_query():
    bq2bq(
        "./samples/tasks/delete/properties.cfg",
        "./samples/tasks/delete/query.sql",
        None,
        datetime(2020, 5, 25, 4),
        datetime(2020, 5, 26, 4),
        execution_date,
        True,
        {
            "deployment": "local-test-run",
            "org": "local",
            "landscape": "test",
            "environment": "local"
        }
    )


def query_script():
    bq2bq(
        "samples/tasks/select/script/properties.cfg",
        "samples/tasks/select/script/query.sql",
        None,
        datetime(2020, 7, 8),
        datetime(2020, 7, 9, 6),
        execution_date,
        False
    )


def drop_table():
    bq2bq(
        "./samples/tasks/drop/properties.cfg",
        "./samples/tasks/drop/query.sql",
        None,
        datetime(2020, 7, 8),
        datetime(2020, 7, 9, 6),
        execution_date,
        False
    )


def non_partitioned_append():
    bq2bq(
        "./samples/tasks/non_partitioned_append/properties.cfg",
        "./samples/tasks/non_partitioned_append/query.sql",
        None,
        datetime(2020, 7, 8, 6),
        datetime(2020, 7, 9, 6),
        execution_date,
        False
    )


def partition_by_column_load_timestamp():
    bq2bq(
        "./samples/tasks/partition_by_column_load_timestamp/properties.cfg",
        "./samples/tasks/partition_by_column_load_timestamp/query.sql",
        None,
        datetime(2020, 7, 8, 6),
        datetime(2020, 7, 9, 6),
        execution_date,
        False
    )


def weekly_partitioned():
    bq2bq(
        "./samples/tasks/weekly_partitioned/properties.cfg",
        "./samples/tasks/weekly_partitioned/query.sql",
        None,
        datetime(2020, 8, 25, 6),
        datetime(2020, 9, 1, 6),
        execution_date,
        False
    )


def select_federated_table_from_gsheet():
    bq2bq(
        "./samples/tasks/select/federated_table/properties.cfg",
        "./samples/tasks/select/federated_table/query.sql",
        None,
        datetime(2019, 12, 1, 19),
        datetime(2019, 12, 2, 19),
        execution_date,
        False
    )


def partition_append():
    bq2bq(
        "./samples/tasks/partition_append/properties.cfg",
        "./samples/tasks/partition_append/query.sql",
        None,
        datetime(2020, 8, 25, 6),
        datetime(2020, 8, 26, 6),
        execution_date,
        False
    )


def replace_merge():
    bq2bq(
        "samples/tasks/replace_merge/auto/properties.cfg",
        "samples/tasks/replace_merge/auto/query.sql",
        None,
        datetime(2020, 12, 5, 1),
        datetime(2020, 12, 5, 1),
        execution_date,
        False
    )


def replace_all():
    bq2bq(
        "samples/tasks/replace_all/basic/properties.cfg",
        "samples/tasks/replace_all/basic/query.sql",
        None,
        datetime(2021, 9, 1, 1),
        datetime(2021, 9, 30, 1),
        execution_date,
        False
    )


def allow_field_addition():
    bq2bq(
        DEFAULT_PATH + "/samples/tasks/allow_field_addition/basic/properties.cfg",
        DEFAULT_PATH + "/samples/tasks/allow_field_addition/basic/query.sql",
        None,
        datetime(2021, 9, 1, 1),
        datetime(2021, 9, 2, 1),
        execution_date,
        False
    )


DEFAULT_PATH = os.path.dirname(os.path.realpath(__file__))
allow_field_addition()