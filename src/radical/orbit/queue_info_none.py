"""No-op QueueInfo backend for systems without a recognised batch scheduler."""

from .queue_info import QueueInfo


class QueueInfoNone(QueueInfo):
    """Returns empty data for every query. Used on dev laptops / CI."""

    backend_name = 'none'

    def _collect_info(self):
        return {'queues': {}}

    def _collect_jobs(self, queue, user):
        return {'jobs': []}

    def _collect_all_user_jobs(self, user):
        return {'jobs': []}

    def _collect_allocations(self, user):
        return {'allocations': []}
