from pynamodb_async.connection.base import AsyncConnection
from pynamodb.connection.table import TableConnection as PynamoDBTableConnection


class TableConnection(PynamoDBTableConnection):
    def __init__(self, table_name, region=None, host=None, session_cls=None, request_timeout_seconds=None,
                 max_retry_attempts=None, base_backoff_ms=None):

        super(TableConnection, self).__init__(table_name, region, host, session_cls, request_timeout_seconds,
                                              max_retry_attempts, base_backoff_ms)

        self.connection = AsyncConnection(region=region, host=host, session_cls=session_cls,
                                          request_timeout_seconds=request_timeout_seconds,
                                          max_retry_attempts=max_retry_attempts,
                                          base_backoff_ms=base_backoff_ms)
