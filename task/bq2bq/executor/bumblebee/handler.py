import signal
import sys
from bumblebee.log import get_logger

logger = get_logger(__name__)

class BigqueryJobHandler:
    def __init__(self) -> None:
        self._sum_slot_millis = 0
        self._sum_total_bytes_processed = 0

    def handle_job_finish(self, job) -> None:
        self._sum_slot_millis += job.slot_millis
        self._sum_total_bytes_processed += job.total_bytes_processed

    def handle_job_cancelled(self, client, job):
        c = client
        job_id = job.job_id
        def handler(signum, frame):
            c.cancel_job(job_id)
            logger.info(f"{job_id} successfully cancelled")
            sys.exit(1)

        signal.signal(signal.SIGTERM, handler)

    def get_sum_slot_millis(self) -> int:
        return self._sum_slot_millis

    def get_sum_total_bytes_processed(self) -> int:
        return self._sum_total_bytes_processed
