from pynamodb.models import Model as PynamoDBModel

from pynamodb_async.connection.table import TableConnection


class Model(PynamoDBModel):
    # TODO Check CRUD first
    @classmethod
    def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None):
        """
        :param wait: argument for making this method identical to PynamoDB, but not-used variable
        :param read_capacity_units: Sets the read capacity units for this table
        :param write_capacity_units: Sets the write capacity units for this table
        """
        print("Hello World!")
        super(Model, cls).create_table(wait=False,
                                       read_capacity_units=read_capacity_units,
                                       write_capacity_units=write_capacity_units)

    @classmethod
    def _get_connection(cls):
        """
        Returns a (cached) connection
        """
        if not hasattr(cls, "Meta") or cls.Meta.table_name is None:
            raise AttributeError(
                """As of v1.0 PynamoDB Models require a `Meta` class.
                See https://pynamodb.readthedocs.io/en/latest/release_notes.html"""
            )
        if cls._connection is None:
            cls._connection = TableConnection(cls.Meta.table_name,
                                              region=cls.Meta.region,
                                              host=cls.Meta.host,
                                              session_cls=cls.Meta.session_cls,
                                              request_timeout_seconds=cls.Meta.request_timeout_seconds,
                                              max_retry_attempts=cls.Meta.max_retry_attempts,
                                              base_backoff_ms=cls.Meta.base_backoff_ms)
        return cls._connection
