#!/usr/bin/env python3

import json
import os
from bumblebee import bq2bq, log
from bumblebee.config import AppConfig
from bumblebee.handler import BigqueryJobHandler
import pathlib

if __name__ == "__main__":
    logger = log.get_logger(__name__)
    job_handler = BigqueryJobHandler()

    app_config = AppConfig()
    xcom_data = {'execution_time': app_config.execution_time.strftime('%Y-%m-%dT%H:%M:%S.%f')}
    logger.info("prepared xcom data: {} at: {}".format(xcom_data, app_config.xcom_path))

    bq2bq.bq2bq(
        None,
        app_config.sql_file,
        app_config.spillover_sql_file,
        app_config.dstart,
        app_config.dend,
        app_config.execution_time,
        app_config.dry_run,
        app_config.job_labels,
        app_config.xcom_path,
        on_finished_job = job_handler.handle_finished_job,
    )

    xcom_data['monitoring'] = {
        'slot_millis': job_handler.get_sum_slot_millis(),
        'total_bytes_processed': job_handler.get_sum_total_bytes_processed()
    }

    pathlib.Path(os.path.dirname(app_config.xcom_path)).mkdir(parents=True, exist_ok=True)
    # will be returned by xcom operator
    with open(app_config.xcom_path, 'w') as the_file:
        json.dump(xcom_data, the_file)
