class BigqueryJobHandler:
    def __init__(self) -> None:
        self._sum_slot_millis = 0
        self._sum_total_bytes_processed = 0

    def handle_finished_job(self, job) -> None:
        self._sum_slot_millis += job.slot_millis
        self._sum_total_bytes_processed += job.total_bytes_processed

    def get_sum_slot_millis(self) -> int:
        return self._sum_slot_millis

    def get_sum_total_bytes_processed(self) -> int:
        return self._sum_total_bytes_processed
