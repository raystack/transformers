from datetime import datetime

from bumblebee.bigquery_service import create_bigquery_service, DummyService
from bumblebee.config import TaskConfigFromEnv, TaskConfigFromFile
from bumblebee.config import TaskFiles
from bumblebee.filesystem import FileSystem
from bumblebee.log import get_logger
from bumblebee.transformation import Transformation
from bumblebee.version import VERSION
from bumblebee.writer import JsonWriter

logger = get_logger(__name__)


def bq2bq(properties_file: str,
          query_file: str,
          spillover_query_file: str,
          dstart: datetime,
          dend: datetime,
          execution_time: datetime,
          dry_run: bool = False,
          labels: dict = {},
          output_on: str = './return.json',
          on_finished_job = None,
          ):

    logger.info("Using bumblebee version: {}".format(VERSION))

    job_labels = base_job_Labels.copy()
    job_labels.update(labels)
    writer = JsonWriter(output_on)

    task_files = TaskFiles(FileSystem(), [query_file, spillover_query_file, properties_file])
    if task_files.properties_cfg is not None:
        task_config = TaskConfigFromFile(task_files.properties_cfg)
    else:
        task_config = TaskConfigFromEnv()

    bigquery_service = DummyService()
    if not dry_run:
        bigquery_service = create_bigquery_service(task_config, job_labels, writer, on_finished_job=on_finished_job)

    transformation = Transformation(bigquery_service,
                                    task_config,
                                    task_files.query,
                                    task_files.spillover_query,
                                    dstart,
                                    dend,
                                    execution_time,
                                    dry_run)
    transformation.transform()


base_job_Labels = {
    "lifecycle": "process",
    "component": "worker",
    "alias": "bumblebee"
}
